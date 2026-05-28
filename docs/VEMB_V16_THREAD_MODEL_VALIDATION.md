# VEMB V16 新线程模型验证与优化设计总结

## Proxy/SuperNode 线程模型架构图

```text
                                   TCP Clients
                                        |
                                        v
                         +-------------------------------+
                         | accept/control main thread    |
                         | - accept TCP fd               |
                         | - HELLO/WELCOME               |
                         | - channel lifecycle           |
                         +-------------------------------+
                                        |
                                        v
             +-------------------------------------------------------+
             |               proxy I/O worker pool                   |
             |                                                       |
             |  worker 0   worker 1   ...   worker N-1              |
             |  --------   --------         ----------               |
             |  epoll fd   epoll fd         epoll fd                 |
             |  parse req  parse req        parse req                |
             |  dispatch   dispatch         dispatch                 |
             |  drain cmpl drain cmpl       drain cmpl               |
             |  write resp write resp       write resp               |
             +-------------------------------------------------------+
                        |                 |                 |
                        | SPSC shard/job  | SPSC shard/job  |
                        | queue           | queue           |
                        v                 v                 v
             +-------------------------------------------------------+
             |              supernode worker pool                    |
             |                                                       |
             |  worker 0   worker 1   ...   worker M-1              |
             |  --------   --------         ----------               |
             |  poll job   poll job         poll job                 |
             |  load vec   load vec         load vec                 |
             |  VEMB/VADD  VEMB/VADD        VEMB/VADD                |
             |  publish completion via completion ring               |
             +-------------------------------------------------------+
                        ^                 ^                 ^
                        | completion ring | completion ring |
                        +-----------------+-----------------+

  Linux pooled mode extras:
  - proxy I/O worker uses epoll for multi-fd management
  - slow TCP response switches to per-channel backlog + EPOLLOUT flush
  - completion/job wakeup uses worker-local eventfd with arm/disarm semantics
```

## 背景

本轮工作围绕 `vemb_v16_server` 的新线程模型展开，目标是在保持 VEMB/VADD 语义正确的前提下，解决高连接数下的线程膨胀、per-channel 队列扫描成本、慢客户端拖累正常请求、以及 TCP 多连接管理效率不足等问题。

主验证配置如下：

```bash
./src/vemb_v16_server \
  --transport tcp \
  --tcp-host 127.0.0.1 \
  --tcp-port 6391 \
  --proxy-io-threads 8 \
  --supernode-workers 16 \
  --vector-region /vemb_v16_vectors \
  --warm-backend shm \
  --dim 300 \
  --max-vectors 131072 \
  --loglevel notice
```

此外，本文还补充了三组 `vemb-supernode-read` 线程模型对照数据：

- 旧模型：`--proxy-io-threads 0 --supernode-workers 0`
- 中间态：`--proxy-io-threads 0 --supernode-workers 16`
- 放大池化：`--proxy-io-threads 16 --supernode-workers 32`

## 本次优化设计点

### 1. SuperNode 执行从 per-channel thread 收敛为固定 worker 池

- 新增 `--supernode-workers N`，支持将 SuperNode 执行从每个 channel 一个线程，收敛为固定数量的 worker 池。
- 同一 channel 仍保持单 worker 消费，避免立即引入 MPSC 队列和 response reorder。
- `--supernode-workers 0` 或不传时，保留旧 per-channel SuperNode thread 模型，便于 A/B 对照。

### 2. TCP proxy 处理从 per-channel thread 收敛为固定 I/O worker 池

- 新增 `--proxy-io-threads N`，将 TCP channel thread 收敛为固定 `proxy I/O worker` 池。
- accept/control main thread 只负责 accept、HELLO/WELCOME、channel 生命周期管理。
- `proxy I/O worker` 负责 frame parse、job dispatch、completion drain、response write。
- `--proxy-io-threads 0` 或不传时，保留旧 per-channel proxy thread 模型。

### 3. Linux 下引入 epoll 化多 fd 管理

- Linux 下每个 `proxy I/O worker` 拥有独立 `epoll_fd`，统一管理多个 TCP fd。
- 非 Linux 平台继续 fallback 到 `poll`，保持实现兼容性。
- channel close 时等待 worker 从 epoll 中摘除 fd，避免 fd close/reuse 与 epoll 注册表交叉。

### 4. 引入 slow client backpressure 隔离

- 当 TCP response 写不动时，channel 不再同步阻塞 worker，而是进入 per-channel backlog。
- 通过 `EPOLLOUT` 事件继续 flush backlog，避免慢连接长期占住同一个 `proxy I/O worker`。
- 设计目标是将慢连接的影响限制在自身 channel，不放大到同 worker 上的其他请求。

### 5. 引入 completion/job queue 唤醒机制

- Linux 下 `proxy I/O worker` 使用 worker-local `eventfd` 接收 completion 唤醒，减少 idle scan。
- pooled SuperNode worker 也使用 worker-local `eventfd` 做 job queue 唤醒，降低 shard queue 和 per-channel ring 的空转扫描成本。
- 唤醒采用 arm/disarm 语义，避免无谓的频繁通知。

### 6. 队列结构从 per-channel 收敛到 per-worker/per-shard

- 当 `--proxy-io-threads N` 与 `--supernode-workers M` 同时启用时，TCP VEMB/VADD 从 per-channel `job ring` 收敛为 `proxy_io_worker -> supernode_worker` 的 SPSC shard queue。
- 该设计降低了高连接数场景下的线程数量、queue 数量、内存占用和 cache 压力。
- 当前 VADD 仍沿用 full-vector payload shard queue，优先获取 pooled 线程模型收益，后续再考虑 staged payload 或小 descriptor 化。

### 7. 保留可观测性与兼容回退能力

- stats 同时保留 per-channel 路径上的请求、completion、response 等统计，便于定位瓶颈。
- `proxy-io-threads=0` 与 `supernode-workers=0` 均可回退到旧模型，降低演进风险。

## 验证项一：Slow Client Backpressure

### 测试目标

验证慢客户端在长时间不读取响应时，是否仍会拖慢同一 `proxy I/O worker` 上的正常请求。

### 测试结论

验证结果表明，新线程模型下的 slow client backpressure 已生效。慢连接在多个压力档位下不会显著影响 probe 请求的成功率和延迟表现。

### 结果汇总

| Server proxy-io-threads | Bench proxy-io-threads | slow_ops | probe_ops | stall_ms | slow 结果 | probe 结果 | probe 延迟 |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| 1 | 1 | 8192 | 1000 | 2000 | 100% 成功 | 100% 成功 | p99 = 0.240ms, max = 0.325ms |
| 8 | 8 | 8192 | 1000 | 2000 | 100% 成功 | 100% 成功 | p99 = 0.165ms, max = 0.233ms |
| 8 | 8 | 4096 | 2000 | 5000 | 100% 成功 | 100% 成功 | p99 = 0.078ms, max = 0.211ms |
| 8 | 8 | 16384 | 2000 | 5000 | 部分失败 | 100% 成功 | p99 = 0.213ms, max = 0.306ms |

### 关键观察

- `slow_ops=4096, stall_ms=5000` 是当前最稳定的一组正例。
- `slow_ops=16384, stall_ms=5000` 时，probe 仍然 100% 成功且低延迟，说明隔离仍成立。
- 该组 slow 失败的原因是 slow channel 自身命中 TCP response backlog 上限后被保护性断开，不是隔离失效。
- 因此，当前边界主要体现在 slow channel backlog 容量，而不是线程模型本身。

## 验证项二：`vemb_v16_bench` 吞吐测试

### 测试参数

- `transport=tcp`
- `host=127.0.0.1`
- `port=6391`
- `dim=300`
- `prefill=65536`
- `ops/thread=200000`
- `pipeline=16`
- `timeout-ms=30000`
- `threads=4,8,16,32`
- `--no-pin`

### 2.1 `vemb-supernode-read`

命令：

```bash
./benchmark/vemb_v16_bench \
  --transport tcp \
  --host 127.0.0.1 \
  --port 6391 \
  --mode vemb-supernode-read \
  --dim 300 \
  --prefill 65536 \
  --ops 200000 \
  --threads 4,8,16,32 \
  --pipeline 16 \
  --timeout-ms 30000 \
  --no-pin
```

结果汇总：

| Threads | Requests | QPS | Fail |
| ---: | ---: | ---: | ---: |
| 4 | 800,000 | 708,807.12 | 0 |
| 8 | 1,600,000 | 1,118,341.60 | 0 |
| 16 | 3,200,000 | 1,414,048.48 | 0 |
| 32 | 6,400,000 | 1,918,553.44 | 0 |

结论：

- 32 线程下 `vemb-supernode-read` 达到约 `1.92M QPS`。
- 所有线程档位均 `fail=0`，吞吐随并发上升保持良好扩展。

### 2.2 `mixed-80r20w`

命令：

```bash
./benchmark/vemb_v16_bench \
  --transport tcp \
  --host 127.0.0.1 \
  --port 6391 \
  --mode mixed-80r20w \
  --dim 300 \
  --prefill 65536 \
  --ops 200000 \
  --threads 4,8,16,32 \
  --pipeline 16 \
  --timeout-ms 30000 \
  --no-pin
```

本次记录包含两轮运行，结果基本一致。

结果区间：

| Threads | QPS 区间 | Fail |
| ---: | ---: | ---: |
| 4 | 689,175.98 - 727,205.87 | 0 |
| 8 | 1,081,942.42 - 1,104,866.00 | 0 |
| 16 | 1,418,848.27 - 1,524,972.01 | 0 |
| 32 | 1,824,299.31 - 1,843,665.09 | 0 |

结论：

- 32 线程下混合读写吞吐稳定在约 `1.82M - 1.84M QPS`。
- 两轮结果接近，说明性能表现稳定。
- 在 20% 写入占比下，系统仍保持良好的并发扩展能力。

### 2.3 三组线程模型对照：`vemb-supernode-read`

为更清楚地分析线程模型优化收益，本轮进一步补充了三组服务端配置对照：

1. 旧模型：`--proxy-io-threads 0 --supernode-workers 0`
2. 中间态：`--proxy-io-threads 0 --supernode-workers 16`
3. 放大池化：`--proxy-io-threads 16 --supernode-workers 32`

统一 benchmark 命令：

```bash
./benchmark/vemb_v16_bench \
  --transport tcp \
  --host 127.0.0.1 \
  --port 6391 \
  --mode vemb-supernode-read \
  --dim 300 \
  --prefill 65536 \
  --ops 200000 \
  --threads 4,8,16,32,64 \
  --pipeline 16 \
  --timeout-ms 30000 \
  --no-pin
```

结果汇总：

| Threads | 旧模型 `0/0` | 中间态 `0/16` | 放大池化 `16/32` |
| ---: | ---: | ---: | ---: |
| 4 | 452,872.49 | 677,723.76 | 645,822.55 |
| 8 | 920,905.41 | 997,025.11 | 1,024,226.42 |
| 16 | 1,539,796.95 | 1,830,148.36 | 2,446,995.59 |
| 32 | 2,486,399.27 | 2,346,365.57 | 2,326,425.49 |
| 64 | N/A | 2,238,428.73 | 3,386,415.44 |

线程档位对比：

| Threads | 最优配置 | 现象 | 说明 |
| ---: | --- | --- | --- |
| 4 | 中间态 `0/16` | `0/16` 略高于 `16/32`，显著高于 `0/0` | 低并发下先收敛 SuperNode worker 已能带来收益，旧模型线程管理开销更明显 |
| 8 | 放大池化 `16/32` | `16/32` 与 `0/16` 接近，均高于 `0/0` | 中低并发阶段，池化模型整体更高效 |
| 16 | 放大池化 `16/32` | `16/32` 明显领先 | 增大 proxy/supernode worker 数量后，池化模型扩展优势开始释放 |
| 32 | 旧模型 `0/0` | 三组接近，`0/0` 略高 | 旧模型仍可依赖更多 per-channel 线程堆高吞吐，但优势已经收窄 |
| 64 | 放大池化 `16/32` | `16/32` 大幅领先，`0/16` 反而回落 | `0/16` 已受固定 SuperNode worker 数量限制，`16/32` 仍可继续扩展 |

关键观察：

- 旧模型 `0/0` 在 32 线程前扩展性较强，本质上仍依赖 per-channel proxy thread 与 per-channel SuperNode thread 的高并发度。
- 中间态 `0/16` 在 4/8/16 线程下明显优于旧模型，说明先将 SuperNode 执行收敛到固定 worker 池后，低到中等并发区间的线程管理和执行效率更好。
- 中间态 `0/16` 在 32 线程附近开始接近饱和，64 线程时 QPS 从 `2.35M` 回落到 `2.24M`，说明 `supernode-workers=16` 已成为主要瓶颈。
- 放大池化 `16/32` 在 16 线程和 64 线程下表现最好，尤其 64 线程达到 `3.39M QPS`，相较中间态 `0/16` 提升约 `51%`，说明放大 worker 池规模后，新线程模型仍具备继续扩展的空间。
- 放大池化 `16/32` 在 32 线程与中间态 `0/16` 基本持平，说明在该并发档位上系统处于扩展拐点附近，额外 worker 数量尚未完全转化为吞吐优势。

阶段性结论：

- 旧模型的优势主要体现在高并发下可以通过更多 per-channel 线程直接堆出峰值吞吐，但代价是线程膨胀、可控性弱，且不具备 pooled proxy I/O 路径下的 slow-client 隔离收益。
- 中间态 `0/16` 证明仅收敛 SuperNode worker 池，就能在中低并发区间获得可观收益，但高并发下会受限于固定 worker 数量。
- 放大池化 `16/32` 证明新线程模型不是天然吞吐吃亏，而是其上限与 `proxy I/O worker` / `supernode worker` 配置规模强相关；在 worker 数量足够时，池化模型同样可以取得更高峰值吞吐。

## 稳定性信号

从 bench 输出可以看到以下稳定性信号：

- 所有 `vemb_v16_bench` 档位均 `ok=100%`、`fail=0`。
- `published == completed == response_publish`。
- `request_publish_spins=0`。
- `response_empty_polls=0`。
- `vemb_ring=0`。
- `vadd_ring=0`。
- `response_ring=0`。
- `completion_ring=0`。

这些信号说明：

- 请求发布、SuperNode 执行、completion 回传、response 发布链路闭环正常。
- 当前实现下未出现 ring 满、明显自旋或空轮询问题。
- 新线程模型在高并发下运行稳定。

## 当前边界

- 已验证的稳定 slow-client 档位为 `slow_ops=4096, probe_ops=2000, stall_ms=5000`。
- `slow_ops=16384, stall_ms=5000` 会触发 slow channel 的 backlog 保护性断开。
- 当前瓶颈主要在 slow channel 的 TCP response backlog 容量，而不是线程模型本身的隔离能力。

## 最终结论

本轮验证可以得出以下结论：

1. 新线程模型下，slow client backpressure 隔离已经生效，慢连接不会显著影响正常请求延迟。
2. 在主验证配置 `--proxy-io-threads 8 --supernode-workers 16` 下，`vemb-supernode-read` 在 TCP 模式下最高达到约 `1.92M QPS`。
3. 在三组线程模型对照中，`vemb-supernode-read` 的最高结果达到约 `3.39M QPS`，出现在放大池化配置 `--proxy-io-threads 16 --supernode-workers 32`、`threads=64` 下。
4. `mixed-80r20w` 在 TCP 模式下稳定达到约 `1.82M - 1.84M QPS`。
5. 所有高并发吞吐测试均无失败、无 ring full，说明 `proxy I/O worker + supernode worker` 模型已经具备较好的稳定性与扩展性。
6. 当前边界主要体现在 slow channel 的 TCP response backlog 容量，以及高并发下 worker 池规模对吞吐上限的影响，而不是线程模型本身的隔离能力。

## 可直接引用的阶段性总结

> 在 `proxy I/O worker + supernode worker` 新线程模型下，VEMB V16 已验证 slow client backpressure 隔离生效；同时在 TCP 模式下，`vemb-supernode-read` 在放大池化配置下吞吐可达约 `3.39M QPS`，`mixed-80r20w` 吞吐稳定在约 `1.82M-1.84M QPS`，并在多线程高并发测试中保持 0 失败、0 ring full。
