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

此外，本文还补充了三组 `vemb-supernode-read` 历史线程模型对照数据：

- 旧模型：`--proxy-io-threads 0 --supernode-workers 0`
- 中间态：`--proxy-io-threads 0 --supernode-workers 16`
- 放大池化：`--proxy-io-threads 16 --supernode-workers 32`

## 本次优化设计点

### 1. SuperNode 执行从 per-channel thread 收敛为固定 worker 池

- 新增 `--supernode-workers N`，支持将 SuperNode 执行从每个 channel 一个线程，收敛为固定数量的 worker 池。
- 同一 channel/connection 的请求在源头进入同一个统一 job shard queue，连接内保持 FIFO；不同 channel 之间仍可交错执行，不引入 response reorder buffer。
- 当前实现已将 `--supernode-workers` 作为必选主路径配置，`0` 不再作为可运行回退模式；本文中的 `0` 仅保留历史对照意义。

### 2. TCP proxy 处理从 per-channel thread 收敛为固定 I/O worker 池

- 新增 `--proxy-io-threads N`，将 TCP channel thread 收敛为固定 `proxy I/O worker` 池。
- accept/control main thread 只负责 accept、HELLO/WELCOME、channel 生命周期管理。
- `proxy I/O worker` 负责 TCP frame parse、SHM request ring poll、job dispatch、completion drain、response write。
- 当前实现已将 `--proxy-io-threads` 作为必选主路径配置，`0` 不再作为可运行回退模式；本文中的 `0` 仅保留历史对照意义。

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
- pooled SuperNode worker 也使用 worker-local `eventfd` 做 job queue 唤醒，降低 shard queue 的空转扫描成本。
- 唤醒采用 arm/disarm 语义，避免无谓的频繁通知。

### 6. 队列结构从 per-channel 收敛到 per-worker/per-shard

- 当前 TCP / SHM 的 `PING/VEMB/VADD/VSIM` 主路径均已收敛为 `proxy_io_worker -> supernode_worker` 的统一 SPSC job shard queue。
- 该设计降低了高连接数场景下的线程数量、queue 数量、内存占用和 cache 压力。
- VADD/VSIM inline 仍把 full-vector payload 放入 job slot，优先获取 pooled 线程模型收益，后续再考虑 staged payload 或小 descriptor 化。

### 7. 保留可观测性与历史对照能力

- stats 同时保留 per-channel 路径上的请求、completion、response 等统计，便于定位瓶颈。
- `proxy-io-threads=0` 与 `supernode-workers=0` 已不再作为运行模式保留；旧模型仅保留在文档和历史对照数据中。

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

### 2.4 SHM 场景补充对照：`docs/bench_log`

为补充验证 `SHM + per-channel` 与 `SHM + pooled` 两种线程模型的差异，本轮又整理了 `docs/bench_log` 中的对照结果。统一配置为：

```text
transport=aeron
dim=300
prefill=65536
ops/thread=200000
pipeline=16
pin=no
threads=4,8,16,32,64
```

#### 2.4.1 `vemb-supernode-read`

| Threads | per-channel QPS | pooled QPS | 变化 |
| ---: | ---: | ---: | ---: |
| 4 | 5,764,304.15 | 5,866,760.82 | `+1.8%` |
| 8 | 5,053,078.60 | 4,913,201.63 | `-2.8%` |
| 16 | 6,681,219.64 | 6,550,002.24 | `-2.0%` |
| 32 | 6,586,852.53 | 6,664,796.64 | `+1.2%` |

#### 2.4.2 `mixed-80r20w`

| Threads | per-channel QPS | pooled QPS | 变化 |
| ---: | ---: | ---: | ---: |
| 4 | 4,891,607.15 | 4,579,285.21 | `-6.4%` |
| 8 | 5,209,320.59 | 5,214,604.47 | `+0.1%` |
| 16 | 6,877,517.45 | 6,766,011.11 | `-1.6%` |
| 32 | 7,252,304.21 | 7,178,422.28 | `-1.0%` |
| 64 | 1,015,835.71 | 7,074,032.88 | `+596.4%` |

#### 2.4.3 关键观察

- 在 `4/8/16/32` 线程区间，`per-channel` 与 `pooled` 的差距总体不大，说明当前系统的主要瓶颈仍在 SuperNode 热路径，而不是线程模型本身。
- `request_publish_spins=0`，且所有 `ring full` 计数均为 `0`，说明请求/响应队列容量不是主要限制因素。
- `bitmap_lock_avg_ns`、`bitmap_unlock_avg_ns`、`table_lookup_avg_ns` 会随着并发上升而明显变大，说明并发扩展压力主要来自 bitmap 争用与表查找成本。
- 在 `64` 线程的 `mixed-80r20w` 场景下，`per-channel` 吞吐掉到约 `1.02M QPS`，而 `pooled` 仍能维持约 `7.07M QPS`，差距接近 `7x`。
- 这说明旧的 `per-channel` 线程模型在高并发下会被线程膨胀、调度开销和 cache footprint 明显拖累，而池化模型可以更稳定地控制执行资源，维持可用吞吐。

#### 2.4.4 关键指标对比（64 线程）

`vemb-supernode-read`：

| 指标 | per-channel | pooled | 变化 |
| --- | ---: | ---: | ---: |
| `QPS` | 6,586,852.53 | 6,664,796.64 | `+1.2%` |
| `response_empty_polls` | 28,779,827,790 | 28,590,394,452 | `-0.7%` |
| `table_lookup_avg_ns` | 1,967.4 | 1,876.4 | `-4.6%` |
| `bitmap_lock_avg_ns` | 683.9 | 683.0 | `-0.1%` |
| `bitmap_unlock_avg_ns` | 226.0 | 240.4 | `+6.4%` |
| `vector_load_avg_ns` | 49.3 | 48.2 | `-2.2%` |
| `completion_publish_avg_ns` | 103.9 | 102.9 | `-1.0%` |

`mixed-80r20w`：

| 指标 | per-channel | pooled | 变化 |
| --- | ---: | ---: | ---: |
| `QPS` | 1,015,835.71 | 7,074,032.88 | `+596.4%` |
| `response_empty_polls` | 195,735,733,981 | 87,470,592,523 | `-55.3%` |
| `table_lookup_avg_ns` | 2,341.8 | 2,404.2 | `+2.7%` |
| `bitmap_lock_avg_ns` | 497.1 | 459.7 | `-7.5%` |
| `bitmap_unlock_avg_ns` | 203.8 | 173.7 | `-14.8%` |
| `vector_load_avg_ns` | 461.0 | 55.7 | `-87.9%` |
| `completion_publish_avg_ns` | 105.1 | 105.3 | `+0.2%` |

#### 2.4.5 指标分析结论

- `vemb-supernode-read` 在 64 线程下，两种模型的 `QPS` 仍然接近，说明纯读路径的主要限制依然是 SuperNode 热路径本身，线程模型只带来小幅波动。
- `response_empty_polls` 在两种模式下都非常高，说明 client 端大部分时间仍在等待 response；但 pooled 在 `mixed-80r20w` 下明显更低，表明池化模型在高并发读写混合场景下更能减少整体等待时间。
- `table_lookup_avg_ns` 在 `mixed-80r20w` 下略有上升，说明池化模型并没有消除 lookup 成本，真正的收益主要来自执行资源收敛和更稳定的调度，而不是更快的查表本身。
- `bitmap_lock_avg_ns` 和 `bitmap_unlock_avg_ns` 变化不大，说明 bitmap 争用仍然存在，而且已经接近该 workload 下的主瓶颈之一。
- `vector_load_avg_ns` 在 `mixed-80r20w` 下从 `461.0ns` 降到 `55.7ns`，这是最显著的变化，说明 per-channel 模型在 64 线程时已经出现明显的调度/采样失真或局部拥塞，而 pooled 模型保持了更稳定的热路径表现。
- 综合来看，`64` 线程的结果证明：在低到中等并发下，`per-channel` 和 `pooled` 差距有限，但当并发继续拉高时，`pooled` 能显著抑制等待和资源抖动，因而更适合作为长期主路径。

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

## Pooled 收敛落地计划

基于上述验证结果，后续代码收敛方向应明确为：`pooled` 作为唯一长期主路径，`per-channel` 仅作为过渡期兼容逻辑，并按阶段逐步删除。

### 目标状态

```text
accept/control thread
  -> proxy I/O worker pool
  -> proxy_worker x supernode_worker shard queues
  -> supernode worker pool
  -> per-channel completion/backlog
```

这里需要保留 `per-channel` 的只有 completion / response backlog / channel lifecycle，因为 response ordering、slow-client 隔离和 channel close 仍以 channel 为边界；需要删除的是 `per-channel proxy thread`、`per-channel supernode thread`、以及它们对应的双路径分支。

### 阶段一：先统一运行时到 pooled 主路径

1. 将 `--proxy-io-threads` 与 `--supernode-workers` 变为默认启用配置，不再以 `0` 作为正常运行模式。
2. `proxy-io-threads=0`、`supernode-workers=0` 改为非法参数，避免线上实例继续退回旧模型。
3. TCP 场景要求必须启用 `proxy I/O worker pool`；SuperNode 场景要求必须启用 `supernode worker pool`。
4. 保留现有 `per-channel completion ring` 与 `TCP backlog` 机制，确保 slow-client 隔离能力不回退。

### 阶段二：删除 per-channel SuperNode 线程分支（已完成）

1. channel 级 `supernode_thread` 的创建、join 与 close 分支已移除。
2. 所有请求现已统一由 `supernode worker pool` 消费。
3. 主请求分发已不再依赖 channel-local job ring fallback，统一收敛到 shard queue。

### 阶段三：删除 per-channel proxy thread 主路径（已完成）

1. TCP 场景已彻底移除 `channel_thread_main()` 分支，只保留 `proxy I/O worker pool`。
2. SHM 场景已并入同一组 proxy worker：
   - worker 负责固定 channel 子集的 request ring 轮询；
   - worker 负责 dispatch、completion drain、response publish；
   - TCP fd 继续由 epoll/poll 管理，SHM ring 使用 bounded scan。
3. 当前 proxy 侧已收敛为 `accept/control thread + proxy worker pool` 两层结构。

### 阶段四：队列结构收敛到 shard queue（已完成）

1. 主请求路径现已只保留 `proxy->job_shard_queues[proxy_worker][supernode_worker]`。
2. 同一 channel/connection 的 `PING/VEMB/VADD/VSIM` 共用同一个源头队列，因此连接内 FIFO；不同 channel 之间允许交错。
3. `completion_ring` 继续保持 per-channel，用于 response 回写、slow-client 隔离和 close 协议；当前没有 response reorder buffer。
4. 协议校验失败的 `error_response` 仍是异常直返路径，不收纳进 job FIFO。
5. 统计项已同步调整为以 `job_shard` 深度与 ring-full 为主，不再把 per-channel job ring 作为长期观测对象。

### 阶段五：文档、配置与测试收口（进行中）

已完成：

1. 文档中已移除 `0/0`、`0/16` 作为正常配置的写法，仅保留历史对照意义。
2. stats / bench 输出口径已从早期读写分离标签收敛为 `job_shard`。
3. pooled-only 的最小 TCP / SHM smoke 已补齐，当前已确认：
   - TCP `vadd`
   - TCP `vemb-inline-vector`
   - SHM `vadd`
   - SHM `vemb-supernode-read`
   - SHM `mixed-80r20w`

剩余项：

1. benchmark 收敛为 pooled worker sizing 对照，例如 `4/8`、`8/16`、`16/32`、`32/64`。
2. 回归项固定覆盖：
   - TCP `vemb-inline-vector`
   - TCP `mixed-80r20w`
   - TCP slow-client bench
   - SHM `vemb-supernode-read`
   - SHM `mixed-80r20w`
3. 后续新增或更新 benchmark 记录时，统一使用 `job_shard` 统计标签。

推荐的 pooled-only 回归命令模板：

```bash
# TCP pooled smoke
./benchmark/vemb_v16_bench \
  --transport tcp \
  --host 127.0.0.1 \
  --port 6391 \
  --mode vemb-inline-vector \
  --dim 300 \
  --prefill 8 \
  --ops 20 \
  --threads 1 \
  --pipeline 2 \
  --timeout-ms 3000 \
  --no-pin

# SHM pooled smoke
./benchmark/vemb_v16_bench \
  --transport aeron \
  --socket /tmp/vemb_v16.sock \
  --mode mixed-80r20w \
  --dim 300 \
  --prefill 8 \
  --ops 20 \
  --threads 1 \
  --pipeline 2 \
  --timeout-ms 3000 \
  --no-pin
```

### 实施顺序回顾

本轮实际收敛按“先统一运行时行为，再删除遗留结构”的顺序推进：

1. 先禁止 `0` worker，并让新进程默认启动 pooled workers。
2. 再删除 `per-channel supernode thread` 分支。
3. 接着把 TCP proxy 完全锁定到 pooled path。
4. 最后移除 SHM/TCP 的 per-channel proxy thread 与 per-channel job ring 遗留。

这样每一步都有独立验证边界，也让回归出现波动时能快速定位到具体阶段。

### 当前收敛进展

截至当前代码收敛，已完成以下事项：

1. `proxy-io-threads` / `supernode-workers` 默认启用，并禁止以 `0` 回退到旧模型。
2. `per-channel supernode thread`、`per-channel proxy thread` 已从主运行路径移除。
3. 主请求分发已统一收敛到 `proxy_worker x supernode_worker job shard queue`，不再走 channel-local job ring fallback。
4. SHM pooled 路径的剩余阻塞点已定位并修复：`proxy_io_channel_acquire()` 原先错误地只允许 `TCP channel` 进入 pooled proxy worker，导致 SHM request ring 无法被轮询；修复后 SHM 与 TCP 均走同一套 pooled worker 调度。
5. 统计口径已与新架构对齐：早期读写分离的 queue depth 标签现已收敛为 `job_shard`，避免把 pooled shard queue 误读为 per-channel job ring 或读写两条独立执行队列。

当前剩余工作重点已从“删除旧路径”转向“补足 pooled-only 回归覆盖与继续清理遗留辅助分支”。

### 当前代码架构现状

以下描述仅对应当前代码主路径，用于说明现状实现；前文中的历史线程模型、历史 benchmark 数据与对照结论保持原样，不在这里改写。

#### 1. 顶层 ownership

- `vemb_v16_server` 负责顶层启动、参数解析和生命周期管理。
- `server` 会先创建 `vemb_v16_storage_ctx`，再创建 `vemb_v16_proxy`，并在退出时显式销毁两者。
- `storage` 现已从 `proxy` 内部析出，`proxy` 不再负责 `warm provider` / `tlc` 的创建和销毁。

#### 2. 线程模型

- 当前运行时已经收敛为 pooled-only：
  - `accept/control main thread`
  - `proxy I/O worker pool`
  - `supernode worker pool`
- `--proxy-io-threads` 与 `--supernode-workers` 必须大于 `0`；`0` 不再表示可运行模式。
- `per-channel proxy thread` 与 `per-channel supernode thread` 已退出主运行路径。

#### 3. 数据与请求流

当前主链路可概括为：

```text
client (SHM/TCP)
  -> channel
  -> proxy I/O worker
  -> proxy_worker x supernode_worker job shard queue
  -> supernode worker
  -> per-channel completion ring
  -> proxy I/O worker
  -> client
```

- `channel` 当前主要承载连接状态、request/response ring、completion ring 和 close/backlog 边界。
- 主请求分发已经统一走 `proxy->job_shard_queues[proxy_worker][supernode_worker]`。
- `completion_ring` 仍保持 per-channel，以维持 response 回写、slow-client 隔离和 channel close 协议简单性；response ordering 由连接内 FIFO 的执行源头保证，不靠 completion reorder。

#### 4. 模块职责边界

- `proxy`
  - 负责 accept、HELLO/WELCOME、channel 生命周期管理
  - 负责 TCP frame parse / SHM request ring poll
  - 负责将请求发布到 job shard queue
  - 负责 drain completion 并回写 response
- `storage`
  - 持有 `warm_provider`
  - 持有 `tlc`
  - 持有 `vector_region` 及相关 warm/vector 元数据
  - 对外提供 channel desc 填充、stats 访问、inline vector slice 等公共接口
- `supernode`
  - 负责消费 `PING/VEMB/VADD/VSIM` job shard
  - 负责执行 TLC 读写与向量加载
  - 负责将 completion 发布回 channel completion ring

#### 5. 当前 ownership 边界状态

- `server owns storage lifecycle`
- `proxy` 当前仅持有 `storage *`，并优先通过 `storage` 公共接口读取元数据，而不是直接中继 `warm provider` / `tlc` 细节。
- `supernode` 仍然直接消费 `storage->tlc` 的执行面能力；这一层暂未继续抽象收口。

因此，当前代码现状可以概括为：

> VEMB V16 已经收敛为 `proxy I/O worker pool + supernode worker pool` 的 pooled-only 主架构；`server` 持有 storage 生命周期，`proxy` 负责接入与调度，`supernode` 负责 TLC 执行，per-channel 设计仅保留在 completion/backlog/channel lifecycle 这些必须以 channel 为边界的部分。

## 最终结论

本轮验证可以得出以下结论：

1. 新线程模型下，slow client backpressure 隔离已经生效，慢连接不会显著影响正常请求延迟。
2. 在主验证配置 `--proxy-io-threads 8 --supernode-workers 16` 下，`vemb-supernode-read` 在 TCP 模式下最高达到约 `1.92M QPS`。
3. 在三组线程模型对照中，`vemb-supernode-read` 的最高结果达到约 `3.39M QPS`，出现在放大池化配置 `--proxy-io-threads 16 --supernode-workers 32`、`threads=64` 下。
4. `mixed-80r20w` 在 TCP 模式下稳定达到约 `1.82M - 1.84M QPS`。
5. 所有高并发吞吐测试均无失败、无 ring full，说明 `proxy I/O worker + supernode worker` 模型已经具备较好的稳定性与扩展性。
6. 当前边界主要体现在 slow channel 的 TCP response backlog 容量，以及高并发下 worker 池规模对吞吐上限的影响，而不是线程模型本身的隔离能力。
7. 在 SHM 场景的补充对照中，`4/8/16/32` 线程下 `per-channel` 与 `pooled` 大体接近，但 `64` 线程时 `pooled` 明显优于 `per-channel`，说明线程池化在高并发场景下更具可扩展性。

## 可直接引用的阶段性总结

> 在 `proxy I/O worker + supernode worker` 新线程模型下，VEMB V16 已验证 slow client backpressure 隔离生效；同时在 TCP 模式下，`vemb-supernode-read` 在放大池化配置下吞吐可达约 `3.39M QPS`，`mixed-80r20w` 吞吐稳定在约 `1.82M-1.84M QPS`，并在多线程高并发测试中保持 0 失败、0 ring full。
