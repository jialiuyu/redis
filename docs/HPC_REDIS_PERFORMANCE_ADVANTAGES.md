# hpc-redis 相比原生 Redis 的性能优势总结

日期：2026-06-29

## 1. 核心判断

`hpc-redis` 的性能优势不是来自对 Redis 通用命令路径的小幅优化，而是为固定形态的向量读写和相似度计算重做了一条专用数据面：

```text
client / benchmark
  -> TCP or Aeron/SHM channel
  -> proxy I/O worker pool
  -> SuperNode worker pool
  -> TLC local storage / UB warm payload / remote meta
  -> completion / response
```

bench 支持的口径:

| 口径 | 当前 benchmark mode | data op | TCP 是否 vector-inline 返回 | 说明 |
| --- | --- | --- | --- | --- |
| 写入 | `vadd` | `VEMB_V16_OP_VADD` | 否 | request 携带 inline vector，写入 TLC warm slot，response 返回 handle/状态。 |
| 删除 | `vrem` | `VEMB_V16_OP_VREM` | 否 | 不携带 vector payload，不做维度 shape 检查，走 delete/tombstone/迁移语义。 |
| handle 读 | `vemb-handle` | `VEMB_V16_OP_VEMB_HANDLE` | 否 | Aeron/SHM 场景返回 `{region_id, offset, bytes}`，client 通过 mmap warm region 读取 payload。TCP 读模式不使用该语义。 |
| TCP inline 读 | `vemb-inline` | `VEMB_V16_OP_VEMB_INLINE` | 是 | SuperNode 在 completion 中保存 snapshot，TCP response frame 后追加 vector-inline payload。300 维 FP32 约 1200B。 |
| mixed 读写 | `mixed-80r20w` | TCP 读侧 `VEMB_INLINE`，写侧 `VADD` | TCP 下读侧是 vector-inline | 80% 读、20% 写。TCP mixed 的读侧返回 vector-inline payload；Aeron/SHM mixed 读侧仍可使用 handle/mmap 口径。 |
| inline VSIM | `vsim-inline` | `VEMB_V16_OP_VSIM_INLINE` | 否 | request 携带查询 vector，SuperNode 对已存 payload 计算 score。 |
| key-key VSIM | `vsim-key-key` | `VEMB_V16_OP_VSIM_KEY_KEY` | 否 | SuperNode 查两个 key 的 handle，必要时经 remote meta / UB lookup RPC 解析 key2。 |

当前 inline 语义的关键点：

1. `VEMB_V16_OP_VEMB_INLINE` 就是 response 携带 payload 的语义来源。
2. TCP vector-inline response 的判断口径只由 op、status 和 payload 长度决定。
3. TCP transport 根据 `resp->op == VEMB_V16_OP_VEMB_INLINE`、`status == OK`、`vector_bytes != 0` 编码额外 payload。
4. benchmark 在 TCP read mode 下只允许 `vemb-inline` 或 `mixed-80r20w`，避免把 handle-only QPS 误当成 vector-inline QPS。

## 2. 技术性能提升迭代与关键数据

可以把 hpc-redis 的性能演进分成两个阶段：

1. **Redis 内部增强阶段**：仍保留 Redis server / 模块 / 通用命令框架，只在 TLC/UB/SVE 上做加速，收益通常是 `1.1x - 1.7x`。
2. **VEMB V16 独立数据面阶段**：把 vector 读写/相似度从 Redis 通用执行面里拆出来，收益开始进入“数百万级 TCP QPS、数百万到千万级 SHM QPS”的量级。

### 2.1 阶段 A：Redis + TLC Module 的早期增益

`docs/QUICKSTART_UB_SVE.md` 记录了最早一批和 baseline Redis 的对比数据。它们仍然运行在 Redis server 形态内，因此能看到“保留 Redis 通用路径时，上限大概在哪里”：

| 场景 | Baseline Redis | TLC Module | 提升 |
| --- | ---: | ---: | ---: |
| 80R/20W 无 Pipeline | 123K QPS | 213K QPS | **1.73x** |
| 80R/20W `pipeline=16` | 747K QPS | 828K QPS | **1.11x** |
| 100% GET `pipeline=16` | 786K QPS | 889K QPS | **1.13x** |

这个阶段的意义不是绝对峰值，而是说明：

1. 即使 payload 已经是 1200B、并且底层用了 TLC/UB/SVE，只要命令执行仍挂在 Redis 通用命令框架上，收益会被 RESP、对象层和主线程执行面吃掉一大部分。
2. 无 pipeline 时收益更明显，说明底层存储访问优化是有效的；但一旦 pipeline 拉高，Redis 通用路径就更容易成为瓶颈。

### 2.2 阶段 B：VEMB V16 独立数据面起步

`docs/VEMB_V16_IMPLEMENTATION_TODO.md` 记录了 VEMB V16 的落地方向：不依赖 `RedisModule`、不走 `blocked-client`、不复用 `server.h` 主命令路径，而是独立做 `proxy -> SuperNode -> TLC` 数据面。

这里最重要的变化不是某一组局部 QPS，而是架构边界发生了变化：

```text
Redis/TLC 阶段:
  redis client
    -> RESP
    -> Redis command
    -> module callback
    -> TLC

VEMB V16 阶段:
  vemb bench / future CLI
    -> binary frame
    -> proxy
    -> SuperNode
    -> TLC/UB
```

这一步把“性能提升的来源”从底层存储优化，扩大成了“协议、调度、存储、返回路径”全链路重做。

### 2.3 阶段 C：线程模型从 per-channel 走向 pooled-only

`docs/VEMB_V16_THREAD_MODEL_VALIDATION.md` 给出了最关键的一组中期数据：同样的 TCP `vemb-supernode-read` 口径下，线程模型从旧的 per-channel 结构逐步收敛到 `proxy I/O worker pool + SuperNode worker pool`。

| Threads | 旧模型 `0/0` | 中间态 `0/16` | 放大池化 `16/32` |
| ---: | ---: | ---: | ---: |
| 4 | 452,872.49 | 677,723.76 | 645,822.55 |
| 8 | 920,905.41 | 997,025.11 | 1,024,226.42 |
| 16 | 1,539,796.95 | 1,830,148.36 | 2,446,995.59 |
| 32 | 2,486,399.27 | 2,346,365.57 | 2,326,425.49 |
| 64 | N/A | 2,238,428.73 | **3,386,415.44** |

这一阶段的数据说明两件事：

1. 旧模型并不是“完全跑不快”，它在某些并发档位甚至能靠线程堆出吞吐；问题是线程数随 channel 增长，长期不可控。
2. 当 `proxy-io-threads` 和 `supernode-workers` 放大到足够规模后，池化模型在 64 线程下可以把 TCP 读路径抬到 `3.39M QPS`，说明 pooled 模型不是性能妥协，而是更稳的长期主路径。

#### 2.3.1 hpc-redis 与原生 Redis 线程模型差异

核心区别是：原生 Redis 是 **主线程执行命令，I/O 线程辅助网络**；hpc-redis VEMB 是 **proxy I/O 线程做接入调度，SuperNode worker 线程并行执行向量读写/计算**。

| 维度 | 原生 Redis | hpc-redis VEMB |
| --- | --- | --- |
| 主线程职责 | event loop、命令执行、DB 修改、模块调用、reply 生命周期 | accept/control、server 生命周期、storage/proxy 初始化 |
| I/O 线程职责 | 读写 socket、解析部分输入，命令准备好后交回主线程 | TCP/Aeron fd/ring poll、frame parse、dispatch job、drain completion、write response |
| 业务执行线程 | 基本仍是主线程执行 `processCommand -> call -> cmd->proc` | `supernode worker pool` 执行 `VADD/VREM/VEMB/VSIM` |
| 并行粒度 | 网络 I/O 可并行，命令执行大体串行 | I/O 与 vector 执行都可并行 |
| 数据一致性来源 | 主线程串行执行天然简化一致性 | key meta shard lock、slot seqlock、owner generation、completion ring |
| 慢客户端 | Redis client output buffer / I/O 线程写出机制 | per-channel backlog + `EPOLLOUT`，慢连接隔离在 channel |
| 队列模型 | client 在主线程/I/O 线程之间转移 | `proxy_worker x supernode_worker` shard queue + per-channel completion |
| 适合场景 | 通用 KV/命令/模块语义 | 固定 vector workload，高并发 `VADD/VEMB/VSIM` |

#### 2.3.2 原生 Redis：I/O 可多线程，命令执行仍回主线

原生 Redis 的线程模型以主 event loop 为核心。即使启用 `io-threads`，I/O 线程的主要职责也是网络层：读 socket、解析输入、写 response，以及把准备好的 client 在 I/O 线程和主线程之间转移。

关键点是：I/O thread context 里遇到可执行命令时，不直接执行命令，而是把 client 标记为 `CLIENT_IO_PENDING_COMMAND` 并交回主线程。真正执行仍是：

```text
processInputBuffer()
  -> processCommandAndResetClient()
  -> processCommand()
  -> call()
  -> c->cmd->proc(c)
```

这个设计保留了 Redis 最核心的优点：绝大多数 DB/object/module 状态只在主执行线上修改，一致性简单，锁少，语义完整。但对 CPU-heavy command、module callback、复杂对象处理或 vector workload 来说，执行面不会因为 I/O thread 数量增加而线性扩展。

#### 2.3.3 hpc-redis VEMB：I/O 与执行面都拆成 worker pool

VEMB server 要求 `--proxy-io-threads >= 1` 和 `--supernode-workers >= 1`。它把接入、调度和向量执行拆成两组 worker：

```text
client
  -> proxy I/O worker
  -> shard job queue
  -> SuperNode worker
  -> TLC / UB / SVE
  -> completion ring
  -> proxy response
```

proxy I/O worker 负责连接和数据搬运，SuperNode worker 负责真正执行 `VADD/VREM/VEMB/VSIM`。因此 SuperNode worker 不是“网络辅助线程”，而是实际的数据面执行线程。

这种模型让 hpc-redis 能把固定 vector workload 分摊到多个执行 worker 上：

1. TCP/Aeron frame parse 和 response write 不占用 SuperNode compute/storage worker。
2. TLC lookup、warm slot 读写、SVE copy/cosine 可以在多个 SuperNode worker 上并行。
3. `proxy_worker x supernode_worker` shard queue 让请求从 I/O 面稳定进入执行面。
4. per-channel completion/backlog 保留 response ordering 和慢客户端隔离。

#### 2.3.4 本质收益：从网络并行变成执行并行

原生 Redis 的多线程主要缓解网络 I/O；hpc-redis 的多线程直接覆盖请求生命周期里的两段核心成本：

```text
I/O cost:
  accept/read/parse/write

Execution cost:
  lookup / slot validate / payload copy / SVE compute / completion publish
```

Redis 即使启用 I/O 线程，命令执行仍回到主线程，因此通用命令执行面不是横向扩展主路径。hpc-redis 则把 execution cost 放进 SuperNode worker pool，这也是它在固定 vector workload 下能获得巨大吞吐收益的关键。

#### 2.3.5 代价：hpc-redis 必须自己承担并发一致性

Redis 主线程串行执行的好处是简单：很多对象生命周期、dict 修改、module 回调天然在同一执行线上完成。hpc-redis 把执行面并行化后，就必须显式处理一致性：

1. key meta shard lock 串行化同 shard 写控制面。
2. slot seqlock 保证读到稳定 payload snapshot。
3. owner generation 防止 stale handle 读到复用后的 slot。
4. completion ring 保证结果按 channel 边界返回。
5. migration source fence / tombstone 防止扩容期间读旧 source location。

因此 hpc-redis 的线程模型不是“Redis 加几个线程”，而是把通用 Redis server 改造成专用 vector 数据面：I/O 面和执行面都并行，代价是自己维护更细粒度的一致性协议。

### 2.4 阶段 D：执行面继续放大，VSIM 进入 3M TCP 区间

`docs/vemb_v16_vsim_single_node_analysis.md` 展示了第二类关键迭代：在 `vsim-inline` workload 下，单纯增加 server 并行度就能带来大幅收益。

| Server | Client Threads | Pipeline | QPS | 相对前一阶段 |
| --- | ---: | ---: | ---: | ---: |
| `proxy=8, workers=32` | 64 | 32 | 1,688,354.42 | 1.000x |
| `proxy=16, workers=64` | 64 | 32 | 2,860,948.14 | **1.695x** |
| `proxy=16, workers=64` | 96 | 32 | **3,017,300.44** | **1.786x** vs S1 |

这组数据的含义很明确：在固定协议和 fixed-shape workload 下，瓶颈已经不在“客户端 pipeline 不够深”，而在“server 执行面并行度是否足够”。

### 2.5 阶段 E：Aeron/SHM 与最新 TCP vector-inline mixed 成熟

到 2026 年 6 月，已经能看到两条比较成熟的高吞吐路径：

| 路径 | workload | 最佳记录 | 口径说明 |
| --- | --- | ---: | --- |
| Aeron/SHM pooled | `mixed-80r20w` | **7,096,721.24 QPS** | 读侧可走 handle/mmap，不要求 TCP 回 1200B payload |
| TCP pooled 历史读口径 | `vemb-supernode-read` | **3,386,415.44 QPS** | 历史 mode，不返回 vector-inline payload |
| TCP pooled 当前 vector-inline | `mixed-80r20w` | **2,843,824.98 QPS** | 2026-07-03 最新 5 轮复测均值；TCP 读侧返回 1200B vector-inline payload |

这里最关键的是最后一行：当前 TCP mixed 已经不是 handle-only 历史口径，而是 vector-inline 口径。按 2026-07-03 最新 5 轮复测，仍能稳定达到约 `2.84M QPS`，并且 `fail=0`。单轮结果区间为 `2.83M - 2.85M QPS`，说明当前代码下这个口径是稳定的，而不是偶发高点。

```text
fail=0
response error count=0
not_found=0
stale_handle=0
remote_meta_stale=0
```

这说明 hpc-redis 的优势已经不只是“共享内存模式快”，而是即使走 TCP、即使读侧真的回 vector-inline payload，也仍然能维持接近 `2.85M QPS` 的数百万级吞吐。

### 2.6 迭代主线总结

把这些文档按时间和架构阶段串起来，性能提升主线可以概括为：

1. **先优化存储层**：TLC/UB/SVE 让 Redis 形态下拿到 `1.1x - 1.7x` 的真实收益。
2. **再重做协议和执行面**：VEMB V16 去掉 Redis 通用命令路径后，吞吐上限跨进 `1M - 3M+` 的 TCP 区间。
3. **再收敛线程模型**：pooled-only 把高并发下的线程膨胀、慢客户端拖累和 cache footprint 问题压住。
4. **最后补齐 vector-inline TCP 语义**：即使读侧返回 vector-inline payload，TCP mixed 在最新代码下仍能稳定在 `2.84M QPS` 左右。

### 2.7 UB 相关开发演进

UB 这条线的设计目标不是把 Redis object 直接搬进共享内存，而是把 vector workload 里最重、最稳定的 payload 单独抽出来，形成一套可 mmap、可定位、可迁移的 warm payload 平面。整体路线可以概括为：

1. **从 Redis 内部优化起步**：早期 TLC/UB/SVE 仍挂在 Redis/module 形态下，主要验证 packed payload、固定 stride、SVE copy/cosine 这类底层优化是否有效。
2. **metadata 和 payload 分离**：VEMB V16 后，key/hash/state/lock/migration 等控制结构留在 SuperNode/TLC 私有 metadata，UB warm region 只承载 packed vector bytes，避免跨进程共享复杂对象 ABI。
3. **用稳定 handle 描述 payload**：读写路径逐步收敛到 `{region_id, local_slot, offset, bytes, owner_generation}`。`region_id` 是协议和 client 可见的稳定身份，`region_index` 只是 server 本地数组下标，两者通过小映射衔接。
4. **从单 region 走向多 warm region**：一个 TLC 可以挂载多个 SHM/UB warm region，放置策略引入 hash ring、local weight、local/remote alloc、fallback 和 full 统计，方便在无 eviction 主测法下用足大容量 UB region。
5. **把 handle/mmap 做成读侧主语义之一**：Aeron/SHM 场景下，server 返回 handle，client mmap 对应 region 后本地读 payload；TCP vector-inline 场景则仍由 SuperNode 保存 snapshot，并通过 TCP response 返回 payload。
6. **为 scale-out 补 remote meta / UB lookup**：跨 owner VSIM 不传完整 Redis object，也不默认传完整 vector，而是通过 remote meta view、repair 和 UB lookup RPC 解析远端 handle；单 owner fast path 则跳过无意义 publish。
7. **为迁移和容量继续留接口**：source fence、tombstone、owner generation 和 migration snapshot 让 UB payload 在扩容/迁移时仍可被版本化校验；COLD / WARM-first overflow 还在继续完善，短期主线仍是稳定 warm payload 热路径。

这条路线的核心取舍是：UB 不承担通用数据库对象语义，只承担固定 vector payload 的共享、定位和版本化。控制面保持私有且 cache-friendly，数据面保持 packed 且可 mmap，这是 hpc-redis 后续继续扩展容量、跨 owner 查询和迁移能力的基础。

## 3. Server 读写路径高性能设计拆解

本节按 server 内部真实链路拆开看：从 proxy 接收请求，到 SuperNode 执行，再到 TLC/UB 读写 payload。整体收益来自多层小而明确的“少做事”：少解析、少对象、少锁、少线程、少复制、少跨层语义转换。

### 3.1 总体热路径短而固定

当前 VEMB server 主路径是固定的二进制数据面：

```text
TCP/Aeron request
  -> proxy channel
  -> shard job queue
  -> SuperNode worker
  -> vemb_v16_tlc facade
  -> tlc_core location cache / key meta / warm slot
  -> SHM/UB packed payload
  -> completion ring
  -> TCP/Aeron response
```

相对 Redis 通用命令路径，这条链路不需要经历：

```text
RESP parse
  -> command table dispatch
  -> argv robj/SDS 生命周期
  -> module API / command callback
  -> Redis dict/object encoding
  -> generic reply builder
```

固定协议和固定向量 shape 让 server 可以在早期就知道请求属于 `VADD/VREM/VEMB/VSIM` 哪条路径，后续字段和 payload 长度也都是直接可用的，不需要每次重新解释命令参数。

当前 TCP 已经完全收敛到 encode/decode only 的 compact protocol，不再保留旧的 `struct + memcpy` payload 模式，也不再需要 `--tcp-encoded-payloads` 之类的兼容开关。对于 `dim=300`、`key_len=10` 的当前主测口径：

1. `VEMB_INLINE` request 从旧 fixed request frame 的 `16736B` 降到 `66B`。
2. `VADD` request 从旧 fixed request frame 的 `16736B` 降到 `1270B`。
3. `VADD` response 从 `96B` 降到 `38B`。
4. `VEMB_INLINE` response 因为仍要返回 `1200B` vector payload，所以从 `1296B` 降到 `1266B`，节省主要集中在 metadata。

按 `mixed-80r20w` 的 `80% VEMB_INLINE + 20% VADD` 加权：

1. 平均 request frame 从 `16736B` 降到 `306.8B`，节省 `98.17%`。
2. 平均 response frame 从 `1056.0B` 降到 `1020.4B`，节省 `3.37%`。
3. 平均 round-trip bytes/op 从 `17792.0B` 降到 `1327.2B`，节省 `92.54%`。

也就是说，在 `25,600,000` 次请求的这组 benchmark 里，估算总线流量大约从 `455.48 GB` 降到 `33.98 GB`。这解释了为什么新协议的主要收益首先体现在 TCP request write path，而不是 vector-inline response path。

### 3.2 Proxy 只做连接、搬运和背压隔离

Proxy 的职责很窄：管理 channel 生命周期、读 TCP/Aeron 请求、把请求发布到 SuperNode shard queue、把 completion 编码回 client。它不做向量计算，不读写 TLC，不维护 Redis object，也不把请求转成通用命令对象。

这个拆分带来几个收益：

1. **I/O 与计算分离**：proxy I/O worker 不被 VSIM compute、TLC lookup、UB copy 占住；SuperNode worker 也不用直接处理 socket accept/read/write 的复杂状态。
2. **channel 状态局部化**：每个 channel 保留自己的 request/response/completion 边界，便于慢客户端、response backlog、active 状态和统计隔离。
3. **请求转发轻量化**：proxy 只把固定 job copy 到 shard queue，SuperNode 直接消费 job，不经过 Redis command framework。
4. **拓扑边界清晰**：client-side topology/consistent hash 决定目标 endpoint，proxy 不做二次 hash 或 fan-out，普通请求不会被拓扑控制逻辑污染。

Redis 的通用 server 则需要把 socket 输入解析为通用命令，再进入统一 command 执行框架。对固定 vector workload 来说，这些能力很强，但每次请求都要付出额外成本。

### 3.3 Pooled worker + shard queue 避免 per-channel thread 膨胀

当前 server 使用 `proxy I/O worker pool + SuperNode worker pool`。每个请求经 shard queue 进入 SuperNode worker，completion 再按 channel 返回。这个模型避免了“一个 channel 一个线程”的扩展问题。

高并发时它的性能收益主要来自：

1. **线程数量稳定**：线程数由 `--proxy-io-threads` 和 `--supernode-workers` 控制，不随 client/channel 线性增长。
2. **cache footprint 可控**：worker 长期处理一类任务，热代码、统计字段、queue 状态更容易留在 cache 中。
3. **queue 原语轻**：SPSC/ring/shard queue 的 memory order 和 cacheline 布局比通用锁队列更贴近这个数据面。
4. **batch poll 入口**：proxy 和 SuperNode drain queue 时具备批量处理入口，后续可以继续向 batch execute 演进。

这也是 SHM/Aeron `mixed-80r20w` 能在 pooled 模型下跑到约 `7.10M QPS` 的关键原因之一。它证明高并发下主路径不应该让 channel 数量决定线程数量。

### 3.4 TCP encode/decode 协议和 response writev 直接编码

TCP vector-inline read 由 `VEMB_V16_OP_VEMB_INLINE` 本身表示 response 后面追加 vector-inline payload。

请求和响应都只保留一种 TCP wire format：

1. request 按 op 精确编码，再 decode 回内部 `vemb_v16_req_t`
2. response 按 status/op 精确编码，`VEMB_INLINE` success 才追加 vector payload
3. `hdr.flags` 不再承载“选择哪一种 TCP payload 布局”的语义
4. 旧的 fixed request struct 直传、fixed response metadata 直传、`tcp_payloads_encoded` 分支都已经删除

这使 TCP response 编码更简单：

```text
resp.op == VEMB_INLINE && status == OK && vector_bytes > 0
  -> response frame + inline vector snapshot
else
  -> response frame only
```

`vemb_v16_net_writev_full()` 里的 `make_iov()` 只复制 `iovec` 描述符，并跳过空片段；真正的 header/response/vector bytes 不会被先拼成一个新的连续 buffer：

```c
static int make_iov(struct iovec *dst, const struct iovec *src, int iovcnt) {
    int out = 0;
    for (int i = 0; i < iovcnt; i++) {
        if (src[i].iov_len == 0) continue;
        if (!src[i].iov_base) return -1;
        dst[out++] = src[i];
    }
    return out > 0 ? out : -1;
}

while (curcnt > 0) {
    ssize_t r = writev(fd, cur, curcnt);
    if (r < 0 && errno == EINTR) continue;
    RETURN_IF(r <= 0, -1);
    advance_iov(&cur, &curcnt, (size_t)r);
}
```

这里的 zero-copy 指的是应用层避免把 frame 和 vector payload 再 `memcpy` 到一个临时发送 buffer；内核 socket 发送仍然会按 TCP 栈完成必要的数据拷贝和分片。

高性能点在于：

1. **语义单一**：client/server 围绕 op、status 和 payload length 做一致判断，口径更稳定。
2. **响应构造固定**：response header 和 payload 都是固定结构，TCP transport 可以用 `writev` 批量写 frame 和 inline payload。
3. **慢客户端隔离**：socket 写不动时进入 per-channel backlog，再通过 `EPOLLOUT` flush，不让慢连接长期占住 worker。
4. **handle-only 路径更轻**：Aeron/SHM `vemb-handle` 只返回 handle，client mmap payload，避免每次复制/返回 1200B vector。
5. **带宽收益集中在 request 侧**：`mixed-80r20w` 下平均 request frame 节省 `98.17%`，整体 round-trip bytes/op 节省 `92.54%`。

Redis 通用 reply builder 要处理多种 RESP 类型、bulk string 长度、client output buffer 策略和命令返回形态。VEMB 的 response 形态窄很多，因此可以做得更直。

### 3.5 SuperNode job handler 是固定 op 的小状态机

SuperNode 不进入 Redis command callback，而是直接执行固定 op handler：

```text
VEMB_HANDLE / VEMB_INLINE / VSIM_KEY_KEY -> vemb job handler
VADD / VREM / VSIM_INLINE                -> vadd job handler
```

好的设计点：

1. **早期 shape guard**：`VADD`、`VEMB_INLINE`、`VSIM_INLINE` 等需要 vector shape 的 op 统一检查 `dim/value_size`，失败直接 common finish。
2. **VREM 不伪装成 vector op**：删除不携带 payload，不做无意义的 shape 检查。
3. **completion 结构固定**：handle 字段通过统一 helper 填入 `{region_id, local_slot, offset, bytes, owner_generation}`。
4. **sampled timing**：只有采样请求才做细粒度耗时统计，避免观测逻辑污染每一条热路径。
5. **op 语义驱动 payload**：只有 `VEMB_INLINE` 需要 `snapshot_vemb_payload()`，其他 handle/score/status response 不复制完整 vector。

Redis 通用命令路径需要把 socket 输入解释成 RESP、构造 argv/robj/SDS、查 command table，再进入 `cmd->proc` 或 module callback，并按通用 reply 规则返回结果。VEMB SuperNode 则把 binary frame 里的 op 直接映射到少数固定处理路径：`VADD` 检查 shape 后写 warm slot，`VREM` 走 delete/tombstone，`VEMB_INLINE` 查 handle 并保存 payload snapshot，`VSIM_INLINE` / `VSIM_KEY_KEY` 进入固定 score 计算路径。

这相当于把 Redis 的“通用命令解释器”替换成了一个专用、可预测、分支很少的数据面状态机。

这里“专用、可预测、分支很少”的核心是：系统提前知道 workload 只服务 `VADD/VREM/VEMB/VSIM` 这类固定向量操作，所以不用保留 Redis 通用命令框架的弹性成本。这样 CPU 分支预测更友好，内存访问模式更稳定，reply 结构也更窄，吞吐上限就更容易拉高。

### 3.6 VEMB 读路径：先 cached handle，再稳定 payload snapshot

VEMB read 的快路径是：

```text
SuperNode
  -> vemb_v16_tlc_get_cached_handle()
  -> tlc_core_get_cached_warm_location()
  -> location_cache_peek()
  -> cache hit ? slot meta validate
               : key meta shard lock + full lookup
  -> handle response or inline payload snapshot
```

如果 cached handle 命中且 slot meta 校验通过，就不需要进入更重的 key meta shard 查找。这里的收益主要来自路径设计本身:

1. **读热路径不拿 key meta shard lock**：普通热读可以走 location cache + slot 校验。
2. **cache hit 后仍验证**：不会因为 cache 快就牺牲一致性，slot `state/write_seq/owner_generation/key_hash` 会继续防 stale。
3. **inline snapshot 只在 TCP vector-inline 场景发生**：handle/mmap 读不复制 payload，TCP inline 才复制到 completion。
4. **cache miss 才 full lookup**：只有 miss/stale 时退回 `vemb_v16_tlc_get_handle()`，避免每次都扫描/锁定完整 metadata。

Redis 的 GET/模块读路径通常围绕通用 dict、robj、SDS/module value 展开。VEMB read 面向固定 vector handle，把 lookup 和 payload 读取拆开，**热读成本更接近一次稳定 location 解析**。

#### 测试数据
这条读路径的另一个关键点，是把 key meta 控制面锁从“全局共享锁”缩成“按 hash 分片的 shard lock”。即使 cache miss 或 stale handle 需要退回 full lookup，竞争也只会集中在对应 shard，而不是把所有 key 的 metadata 读写都串到同一把锁上。

`2026-07-05` 按 [benchmark/test_host.md](/Users/szza/codespace/work/hpc-redis/benchmark/test_host.md) 的 `mixed-80r20w` TCP 基准，在 `192.168.90.111` 上对比了默认 `256` 分片和“退化成单锁”的 `1` 分片配置：

| `TLC_CORE_KEY_META_SHARDS` | 有效样本 | 平均 QPS | 结果 |
| --- | ---: | ---: | --- |
| `256` | `5` | `2,851,977.02` | 默认配置 |
| `1` | `5` | `866,119.37` | 退化为单个 key meta 锁 |

同一套 server/client 参数下，`256` 分片相对 `1` 分片的吞吐提升约 `3.29x`，QPS 降幅约 `69.63%`。这说明 VEMB 读路径里的“cache hit 尽量不拿锁”很重要，而即便进入 metadata 路径，**控制面锁的分片粒度本身也是决定 mixed 负载吞吐的核心因素**。

另外，`2026-07-05` 在同一台 `192.168.90.111` 上，针对 `TCP + vemb-inline + hot-key-id=1` 口径，额外测了 `location_cache` 对 **SuperNode 内部 primary lookup** 的收益。用 `TLC_CORE_DISABLE_LOCATION_CACHE=1` 关闭 cache 后，对比 `primary_lookup_avg_ns` 的 3 轮均值：

| 模式 | `primary_lookup_avg_ns` 均值 | 说明 |
| --- | ---: | --- |
| 默认 `location_cache` 开启 | `99.1ns` | `vemb_v16_tlc_get_cached_handle()` 可优先走 cached handle |
| `TLC_CORE_DISABLE_LOCATION_CACHE=1` | `105.3ns` | 回退到非 cache 口径 |

也就是说，在 `TCP` 模式下如果只看 `SuperNode` 内部 lookup 阶段，`location_cache` 带来的收益大约是 `6.2ns/op`，相对提升约 `6.26%`。这个数字明显小于端到端 QPS 提升，原因是 `TCP vector-inline` 路径里还包含 payload snapshot、response encode/write 等额外固定成本；但它仍然说明 **cached handle 先命中，再做稳定校验** 的设计，在热点读场景下可以稳定压低 metadata lookup 开销。

#### 3.6.1 proxy/supernode 消息瘦身：先缩 job/completion，再考虑 `slot-id`

这一节的详细设计已拆到独立文档：[docs/VEMB_PROXY_SUPERNODE_MESSAGE_SLIMMING.md](./VEMB_PROXY_SUPERNODE_MESSAGE_SLIMMING.md)。

这里保留结论版：

1. 当前 `proxy -> SuperNode -> proxy` 热路径的主要额外成本，来自 `job_shard_queue` 和 `completion_ring` 上“大而全结构体”的整 slot `memcpy`。
2. 优先级应该是**先把 read job 和 completion 按语义裁小**，减少 copy 体积和 cache footprint，而不是一开始就把 queue 改成 `slot-id` / pool 生命周期管理。
3. `job` 侧重点是把 `vemb-handle` / `vemb-inline` 从通用 `job_base` 中拆出来，去掉 `key2`、`topology_epoch`、`dim`、`vector_bytes` 等热读不需要的字段。
4. `completion` 侧重点是把 handle、inline payload、VSIM score 分开承载，避免所有 completion slot 都为少数语义背固定布局成本。
5. `2026-07-06` 新增的 UB backing 只落在 `job_pool->slots` 上；`job_shard_queue`、`job_return_queue`、`completion_ring` 和 `free_stack` 仍保留本地 heap/ring 语义，因此这轮 UB 改动的目的不是把整个 proxy/supernode 消息面共享化，而是验证“真实 job payload 存储移到 UB”本身的成本。

这轮实现边界可以概括为：

1. queue 上继续只搬小 `job_ref`
2. 真实 job payload 可选 heap-backed 或 UB-backed
3. slot 分配/释放所有权仍在 owner proxy worker

同一台 `192.168.90.111`、同一组 TCP `mixed-80r20w` 参数下，`shared-plane` 方案约为 `307w QPS`，而“本地 ring/free-stack + 仅 slot payload 可选 UB backing”的实现，在移除 read-pool 热路径诊断原子更新后已经回到 `346w ~ 349w QPS`。这说明当前阶段的主要收益仍然来自**消息瘦身和本地调度面保持轻量**，而不是“只要上 UB 就会更快”。

也就是说，更稳的演进顺序是：**先瘦消息，再评估 `slot-id`**。

#### 3.6.2 `job pool + slot_id`：保留小 queue payload，同时去掉每请求 `zmalloc/zfree`

`proxy -> SuperNode` 的消息瘦身第一阶段已经把大 job slot 改成了小 `job_desc { job_ptr }`，证明：

1. `job_shard_queue` 上的整块大 `memcpy` 确实可以拿掉
2. 同一 channel 的 mixed 顺序语义可以保持不变
3. 真实瓶颈会从 queue copy 前移到 payload slice/load 和固定分配成本

但第一阶段也引入了一个新的固定成本：**每请求 `zmalloc/zfree`**。在 `mixed-80r20w` 这类高频短生命周期 workload 下，这部分开销不再是边角料，而是会稳定出现在每一条 job 上。

因此第二阶段进一步把：

```text
job_desc { job_ptr }
```

改成：

```text
job_ref { proxy_worker_id, pool_id, slot_id, generation }
```

整体模型变成：

```text
proxy worker
  -> 从本地 job pool 取 slot
  -> 填充真实 job
  -> 向 shard queue 发布小 job_ref

supernode worker
  -> 消费 job_ref
  -> 用 (proxy_worker_id, pool_id, slot_id) 定位真实 job
  -> 执行
  -> 向 return queue 发布 slot_id

owner proxy worker
  -> drain return queue
  -> 回收 slot
```

这里最关键的设计点有三个：

1. **queue 仍然只搬小对象**：`job_shard_queue` 不回退到大 struct 值传递
2. **真实 job 不再走 heap**：改成 per-proxy-worker 本地 pool
3. **回收不做跨线程 free-list push**：SuperNode 只发 `job_return`，真正回收由 owner proxy worker 完成

这种做法把“消息瘦身”和“生命周期稳定存储”分开处理：

1. 小 queue payload 负责减少线程间 copy
2. pool slot 负责替代 heap alloc/free
3. return queue 负责把所有权重新交回 owner

#### 测试数据

`2026-07-06` 按 [benchmark/test_host.md](/Users/szza/codespace/work/hpc-redis/benchmark/test_host.md) 的 TCP `mixed-80r20w` 口径，在 `192.168.90.111` 上把三代实现放到同一条演进线上看：

1. **原始 pooled 基线**
   `job_shard_queue` 仍按大 job slot 传值，均值约 `2.84M QPS`
2. **`job_ptr descriptor`**
   ring 里改传小 descriptor，但真实 job 仍是每请求 `zmalloc/zfree`
3. **`job pool + slot_id`**
   ring 里传 `job_ref`，真实 job 改为 per-proxy-worker pool slot

对比基线：

| 版本 | 测试日期 | 有效样本 | 平均 QPS | 说明 |
| --- | --- | ---: | ---: | --- |
| 原始 pooled 基线 | `2026-07-03` | `5` | `2,843,824.98` | 旧实现仍按较大 job slot 过 ring，是本轮优化前的主基线 |
| `job_ptr descriptor` | `2026-07-06` 之前 | `3` | `2,944,068.16` | ring 里传小 descriptor，但真实 job 仍是每请求 `zmalloc/zfree` |
| `job pool + slot_id` | `2026-07-06` | `3` | `3,472,419.33` | ring 里传 `job_ref`，真实 job 落到 per-proxy-worker pool |

`job pool + slot_id` 版三轮原始结果：

| Run | QPS | Fail |
| --- | ---: | ---: |
| 1 | `3,514,139.84` | `0` |
| 2 | `3,459,060.78` | `0` |
| 3 | `3,444,057.37` | `0` |

相对 `job_ptr descriptor` 版：

1. 平均提升 `528,351.17 QPS`
2. 相对提升约 **17.95%**
3. 三轮 `fail=0`
4. `job_ring_vemb=0`、`job_ring_vadd=0`、`completion_ring=0`

相对更早的原始 `2.84M` pooled 基线：

1. 平均提升 `628,594.35 QPS`
2. 相对提升约 **22.10%**

也就是说，这次收益不是因为 queue 堵住后偶然解开，而是沿着同一条热路径连续吃掉了两块固定成本：

1. 先吃掉大 job slot `memcpy`
2. 再吃掉每请求 heap alloc/free

#### 数据分析

这组数据的意义比“又快了一点”更强，因为它回答了一个关键问题：**在小 descriptor 已经生效后，剩下的固定成本里谁更重。**

如果把三代实现放在一起看，阶段收益会更清楚：

1. 原始 pooled 基线 `2.84M QPS`
2. `job_ptr descriptor` 升到 `2.94M QPS`
3. `job pool + slot_id` 再升到 `3.47M QPS`

按均值拆开看：

1. **原始 pooled -> `job_ptr descriptor`**
   提升 `100,243.18 QPS`，约 **3.52%**
2. **`job_ptr descriptor` -> `job pool + slot_id`**
   提升 `528,351.17 QPS`，约 **17.95%**
3. **原始 pooled -> `job pool + slot_id`**
   总提升 `628,594.35 QPS`，约 **22.10%**

这说明两件事：

1. 第一阶段的小 descriptor 是有效的，但收益相对温和，主要是在消掉 queue 上的大 struct 搬运
2. 第二阶段的 pool/slot_id 收益更大，说明在 descriptor 版里，`zmalloc/zfree` 已经成为更显著的固定成本

这也解释了为什么单看 sampled timing：

1. `primary_lookup_avg_ns` 仍然大约在 `189ns - 195ns`
2. `payload_local_slice_avg_ns` 仍然大约在 `620ns - 630ns`
3. `completion_publish_avg_ns` 仍然只有 `27ns - 29ns`

也就是说：

1. `location_cache` / primary lookup 不是这次收益来源
2. completion ring 也不是这次收益来源
3. 主要改善的是 **job ingress 固定成本**

换一个更直观的说法：

```text
原始 pooled
  -> 大 job memcpy + 生命周期固定成本 都还在

job_ptr descriptor
  -> 吃掉大 job memcpy
  -> 但每请求 malloc/free 仍在

job pool + slot_id
  -> 再吃掉每请求 malloc/free
  -> payload slice/load 继续留在最显眼的位置
```

所以这组结果说明两件事：

1. `proxy -> SuperNode` 热路径优化不能只看 queue slot 大小，也要看真实 job 生命周期管理
2. 在 fixed-shape 高 QPS workload 下，**稳定对象池** 本身就是吞吐优化，而不只是工程整洁度优化
3. 如果只拿 `job_ptr descriptor` 和 pool 版相比，会低估这条优化链路相对最初 `2.84M` 基线的累计收益；完整口径下，这一轮累计提升已经超过 **22%**

#### 当前瓶颈位置

完成 `job pool + slot_id` 后，剩下更显眼的成本已经更集中在：

1. `VEMB_INLINE` 的 payload slice/load
2. 1200B vector-inline payload 的 TCP response 返回
3. completion side 仍然按固定 `vemb_v16_completion_t` slot 搬运

因此下一阶段如果继续追吞吐，优先级更合理的方向是：

1. 继续压 `payload_local_slice`
2. 评估 `completion_ring` 的 pool/ref 化
3. 再看 batch execute / batch slice 是否值得推进

这也意味着，`job pool + slot_id` 已经把 job ingress 的固定成本压到了一个更低的位置，后续优化空间会更多集中在 **真实 payload 路径**，而不是 queue 生命周期本身。

### 3.7 VADD 写路径：inline 写入、分片串行、same-key overwrite

VADD request 本身携带 inline vector payload，SuperNode 直接把 payload 写入 TLC warm slot：

```text
VADD
  -> key meta shard lock
  -> existing key ? warm_overwrite_location()
                  : warm_put() allocate slot
  -> SVE/streaming store payload
  -> publish slot meta READY + owner_generation
  -> location_cache_put()
  -> completion handle
```

这一节的核心不是单纯对比 Redis 的通用对象路径，而是 VADD 写侧如何让写入本身和后续读取都保持稳定：

1. **key meta shard 分片锁**：写控制面按 hash 分成多个 shard，不需要全局大锁，同一 key 的 location/tombstone/migration state 仍能串行更新。
2. **same-key overwrite 保持位置稳定**：已有 key 优先写回原 `{region_id, local_slot, offset}`，减少重新分配、重新 hash、handle 抖动和 remote meta churn。
3. **location_cache 写后更新**：写成功后直接更新当前 key 的 location，避免写侧和后续读侧看到不同口径，也让后续 VEMB read 更容易命中 cached handle。
4. **固定 payload 写入**：固定 1200B payload 直接写 packed warm arena，payload 更新和 metadata 发布分离。
5. **单 owner 跳过 remote meta publish**：`remote_meta_view_count <= 1` 时不做无意义异步 publish，mixed 写侧少一段后台队列成本。
6. **迁移逻辑可被 fast path 跳过**：非迁移状态下普通写不需要走 delta/outbox/fence 的重流程。

因此 VADD 的写路径重点是“稳定 location”：写入时按 key shard 收敛控制面并发，已有 key 尽量原地 overwrite，location 改变后及时刷新 cache。这样既减少写侧重新分配、handle 抖动和 remote meta churn，也为后续高命中、低抖动的读路径铺路。

### 3.8 VREM 删除路径保持写侧语义但不携带 payload

VREM 与 VADD 一样属于写侧拓扑/迁移语义，但它不携带 vector payload。当前设计把它和 VADD 放在同一类写侧完成路径里处理 topology epoch、ASK/MOVED、tombstone 和 delete info，但不做 vector shape 检查。

收益点：

1. **删除路径不做无用 payload 工作**：不分配、不复制、不检查 vector bytes。
2. **仍复用写侧一致性语义**：迁移、source fence、tombstone、not_found 语义和 VADD 保持一致。
3. **对 mixed 写侧统计友好**：VADD/VREM 都进入写侧计数和完成路径，但 payload 成本不同。

这类语义拆分能避免 server 为了复用代码而把所有 op 都当成“带 vector 的请求”处理。

### 3.9 TLC/UB：metadata 私有，payload packed 共享

TLC 的关键设计是把控制面 metadata 和大块用户 payload 分开：

```text
SuperNode private metadata:
  location cache
  key meta shards
  warm entries/hash table
  slot state/write_seq/generation
  migration/tombstone/source fence

SHM/UB warm payload:
  packed vector bytes
  region_id + offset + bytes
```

这带来几类收益：

1. **payload 不污染控制结构**：300 维 FP32 vector 是大块数据，放在 packed arena；hash/key/state/lock 留在本地 heap metadata。
2. **client 可 mmap 读 payload**：handle 口径下 client 直接通过 `{region_id, offset, bytes}` 定位，不要求 server 每次返回 vector-inline payload。
3. **UB region 可多块挂载**：一个 TLC 可挂载多个 warm region，按 hash ring 和 local weight 选择 region。
4. **`region_id` 对外稳定**：协议和 client 只看稳定 ID；server 内部用 `region_index` 访问数组，靠小映射衔接。
5. **shared allocator 支撑多 region 容量管理**：warm slot 分配与 region runtime 解耦，manifest 可以用足大 UB region，避免 cold layer 关闭时 eviction 造成 not_found。

Redis 的 value 通常不是裸 payload，而是对象化的通用内存结构：

```text
key
  -> redisObject
      -> type / encoding / refcount / lru
      -> ptr
      -> SDS / dict / listpack / skiplist / module value ...
```

这条链路既要承载 string/hash/zset/module 等类型语义，也要参与 Redis 的引用计数、释放、替换、淘汰和 module callback 等对象生命周期。TLC/UB 把 vector workload 最重的 payload 从通用对象系统里拿出来，改成 `key -> metadata -> {region_id, offset, bytes} -> packed vector bytes`。

### 3.10 并发一致性靠轻量 slot 版本 与 seqlock

VEMB 的读写一致性不是依赖 Redis object 引用计数或全局对象生命周期，而是 slot 级别的轻量版本：

```text
slot state
write_seq even/odd
owner_generation
key_hash / key fingerprint
region_id / local_slot / offset / bytes
```

读 payload 时先验证 slot 是 READY、`write_seq` 为稳定偶数、key/generation 匹配，再复制 payload，复制后再次验证 `write_seq` 未变化。写 payload 时通过状态和 seqlock 发布新版本。

这里的 seqlock 来自 sequence lock 思路，但当前用法不是完整传统读写锁，而是 **stable snapshot read**：

```text
writer:
  write_seq -> odd / writing
  write payload + slot meta
  write_seq -> next even / stable

reader:
  seq1 = write_seq
  if seq1 is odd: retry
  read slot meta + copy payload
  seq2 = write_seq
  success only if seq1 == seq2 and seq2 is even
```

因此它替代的是 warm payload slot 读写路径里按 slot 抢 bitmap lock 的那部分语义。读者不阻塞写者，写者也不等待读者；读者如果遇到写入中或前后版本变化，就丢弃本次 copy 并重试/失败。这个模型要求 payload 地址稳定、大小固定，并且 slot 生命周期由 `state / owner_generation / key_hash` 一起约束，所以很适合当前 packed vector slot。

它不等价于全局 rwlock，也没有替代所有 bitmap lock。`key_meta_locks` 仍用于 key meta shard 的写侧/迁移控制面串行化；seqlock 主要负责 warm slot payload 的无锁读校验和写入发布。

这个设计的好处：

1. **读路径可以无锁验证稳定快照**。
2. **旧 handle 会被 owner_generation 拦截**。
3. **半写 payload 不会被当成成功读返回**。
4. **slot 复用不需要暴露复杂对象生命周期给 client**。

此外，迁移场景中的 `CUTOVER/SOURCE_GC` source fence 和 tombstone 会阻断旧 source location，普通未迁移路径则通过 active counter 快速跳过额外检查。

### 3.11 VSIM 和 remote meta 让跨 owner 查询保持窄接口

`vsim-inline` 和 `vsim-key-key` 都在 SuperNode 内部直接拿 payload slice 做 SVE cosine，不进入 Redis Search/HNSW/filter 的通用路径。

对 key-key VSIM：

1. key1 owner SuperNode 先查本地 key1 handle。
2. key2 如果本地可见，直接读取 payload slice。
3. key2 如果属于远端 owner，通过 remote meta view 获取 `{region_id, offset, bytes}`。
4. remote meta stale 时可走 repair/UB lookup RPC fallback。

这个接口很窄：跨 owner 传递的是 handle metadata，而不是完整 Redis object、vector-inline payload 或通用查询计划。它保留了 scale-out 能力，但普通同 owner 快路径仍然很短。

### 3.12 低扰动统计让优化可以继续推进

当前 SuperNode timing 已经按 sampled request 统计，并拆出：

```text
primary_lookup
secondary_lookup
remote_meta_lookup
payload_local_slice
payload_remote_slice
compute
completion_publish
```

这不是直接的业务性能优化，但它避免了每请求都打时间戳的热路径污染，同时保留定位瓶颈的能力。对这种百万级 QPS 数据面来说，观测代码本身如果不采样，很容易变成新的瓶颈。

### 3.13 Bitmap CAS microbench

bitmap acquire 最原始的设计是 CAS：先读 bitmap word，确认目标 bit 仍为空，再通过 compare-exchange 抢占。后续在远端 aarch64 机器上做 hotspot microbench 后，生产路径优化为当前 `fetch_or` 实现；测试结果显示 `fetch_or` 在该场景下优于两种 CAS 版本：

| 场景 | CAS optimized | CAS bounded | fetch_or 当前实现 |
| --- | ---: | ---: | ---: |
| 8 线程 Hotspot | 2.01 Mops/s, 497 ns, 81.39% success | 2.34 Mops/s, 427 ns, 85.65% success | 2.48 Mops/s, 403 ns, 91.04% success |
| 16 线程 Hotspot | 2.28 Mops/s, 439 ns, 77.34% success | 2.69 Mops/s, 372 ns, 78.95% success | 3.08 Mops/s, 325 ns, 86.79% success |

结论: 按吞吐看，`fetch_or` 相对 CAS optimized 在 8 线程 hotspot 下提升约 **23.4%**，16 线程 hotspot 下提升约 **35.1%**；相对 CAS bounded 分别提升约 **6.0%** 和 **14.5%**。同时平均 acquire 延迟也从 CAS optimized 的 `497 ns / 439 ns` 降到 `403 ns / 325 ns`，success rate 分别提高到 `91.04%` 和 `86.79%`。

bitmap acquire 已从 CAS 路线收敛到当前 `fetch_or`，后续收益应来自减少 bitmap word 竞争，而不是退回 CAS acquire 原语。

### 3.14 对 Redis 的收益来源总结
因此，hpc-redis 的性能不是单点 trick，而是一整条 server 数据面把 Redis 的通用性成本系统性移除：proxy 只搬运，SuperNode 只执行固定 op，TLC 只管理 vector location 和一致性，UB 只承载 packed payload。

## 4. 已实现的主要收益点

| 层面 | 当前实现 | 相对原生 Redis 的收益 |
| --- | --- | --- |
| 协议 | VEMB V16 TCP 已收敛到 compact encode/decode-only binary frame：request 按 op 精确编码，response 按 status/op 精确编码。 | 避免 RESP 解析、命令查表、参数对象化和通用 reply 编码，同时去掉旧 `struct + memcpy` TCP wire 冗余。 |
| inline 语义 | `VEMB_INLINE` op 直接表示 TCP vector-inline response。 | 语义集中在 op/status/payload length 上，减少 client/server 口径不一致风险。 |
| 数据模型 | 固定 FP32 vector，payload 大小稳定，metadata 与 payload 分离。 | 避免 Redis object/SDS/listpack/HNSW 节点等通用结构成本。 |
| handle/mmap | Aeron/SHM `vemb-handle` 返回 `{region_id, offset, bytes}`，client mmap warm region 后本地读 payload。 | 非 TCP vector-inline 场景避免每次返回 1200B payload，网络和复制压力更小。 |
| TCP vector-inline | `vemb-inline` 返回 vector-inline payload，但仍使用专用 frame、SuperNode snapshot 和 TCP `writev` 编码。`mixed-80r20w` 最新 5 轮均值约 `2.84M QPS`。 | payload 仍要传输，收益小于 handle-only；优势主要来自少对象层、少 reply 构造和调度稳定性。 |
| 线程模型 | `proxy I/O worker pool + SuperNode worker pool`，accept/control 只处理连接生命周期。 | 避免 per-channel thread 膨胀，降低高并发调度和 cache footprint。 |
| 慢客户端隔离 | TCP 写不动时进入 per-channel backlog，通过 `EPOLLOUT` flush。 | 慢连接不长期占住 worker，降低尾延迟扩散。 |
| 队列 | proxy 到 SuperNode 使用 shard queue，completion 仍按 channel 边界保存。SPSC ring 采用 64B head/tail、batch poll、acquire/release memory order。 | 比通用锁队列更轻，跨线程转发成本可控。 |
| TLC warm region | 一个 TLC 可挂载多个 SHM/UB warm region，内部维护 `region_id -> region_index` 小映射。 | `region_id` 对外稳定，`region_index` 本地数组友好，避免把部署 ID 和运行时下标绑死。 |
| UB payload | warm data region 只存 packed vector bytes，key/hash/state/lock 留在 SuperNode 私有 metadata。 | 大 payload 可被 mmap/UB 共享，控制结构保持 cache-friendly，也避免跨进程 metadata ABI。 |
| region 放置 | warm region hash ring 支持 local weight、local/remote alloc、fallback 和 full 统计。 | 本地 UB region 优先，容量不足时可走下一个 region，便于无 eviction 容量配置。 |
| location cache | VEMB read 先走 cached handle，命中后仍通过 slot meta 校验。 | 热读不必总是进入 key meta shard lock，同时保留 stale handle 防护。 |
| slot seqlock | `state + write_seq + owner_generation` 校验 payload 版本，copy 前后双读 `write_seq`。 | 支持无锁读稳定 snapshot，避免半写 payload 和旧 handle 被误读。 |
| key meta shard | 256 个 key meta shard，写路径按 hash 分片串行化，维护 migration state、epoch、tombstone 和 location。`2026-07-05` 的 `mixed-80r20w` TCP 实测中，`256` 分片均值 `2.85M QPS`，退化到 `1` 分片只剩 `0.87M QPS`。 | 控制面锁粒度小，普通读写路径不需要全局 Redis dict/object 锁；相对单锁配置，当前分片设计带来约 `3.29x` 吞吐提升。 |
| VADD overwrite | existing key 尽量写回原 `{region_id, local_slot, offset}`。 | 减少重新分配、bucket scan 和 handle 抖动，mixed 写入路径更稳定。 |
| VREM | delete 走 key meta / tombstone / migration 语义，不再伪装成 vector-carrying op。 | 避免无意义的 shape 检查和错误 payload 假设。 |
| remote meta | remote meta view 支持异步 publish、repair 和 stale 校验；单 owner 时跳过 publish。 | 跨 owner VSIM 可查 remote handle，单机/单 owner fast path 不承担无用 publish 成本。 |
| migration fence | `CUTOVER` / `SOURCE_GC` source fence 和 tombstone 会阻止旧 source location 被读出。 | 扩容迁移期间避免 stale source 读，普通路径通过 active counter 快速跳过。 |
| timing | SuperNode timing 使用 `monotonic.h::elapsedNs`，并只在 sampled request 上计时。 | 降低观测代码对热路径的扰动，保留足够的瓶颈可见性。 |
| helper 收敛 | hash helper、bitmap acquire/release、TLC wrapper 调用已收敛。 | 减少重复实现和薄 wrapper，热路径更直接，后续阅读和优化成本更低。 |
| UB/SVE | UB mmap、固定 stride、SVE streaming load/store/cosine。 | 固定维度向量可利用更直接的内存访问和 SIMD 路径。 |
| 功能取舍 | VEMB/UB 路径不覆盖原生 Vector Sets 的所有通用能力，如复杂 filter、attribute、HNSW 参数等。 | 用更窄语义换更短热路径，这是性能收益的重要来源。 |

## 5. 后续继续优化的收益点

| 优先级 | 优化项 | 当前状态 | 预期收益 |
| --- | --- | --- | --- |
| P0 | TCP inline per-request allocation | 当前仍为 per-request `zmalloc + copy`；简单全局池已回退。 | 做 per-worker/per-channel cache 或生命周期内复用，降低 vector-inline CPU 成本和尾延迟。 |
| P0 | batch 指标补齐 | 已有 sampled timing，但 batch rounds、avg batch、HOT hit/miss、WARM probe、bitmap word conflict 仍不完整。 | 先量化瓶颈，避免盲调。 |
| P0 | completion ring 消息瘦身 | `job_shard_queue` 已完成 `job_ref + per-proxy-worker pool + return queue` 改造，但 `completion_ring` 仍按固定 `vemb_v16_completion_t` slot 传递。 | 继续压缩 `SuperNode -> proxy` 元数据搬运成本，评估 `completion_ref + pool slot` 或更瘦 completion layout。 |
| P0 | payload 路径继续减重 | `job ingress` 固定成本已经明显下降，当前 sampled timing 更显眼的是 `payload_local_slice` 和 TCP vector-inline payload 路径。 | 继续压 `VEMB_INLINE` payload slice/load 与 response payload 路径，把收益集中到真正的 1200B payload 热点。 |
| P1 | SuperNode batch execute | 当前主要是 batch poll，执行仍偏逐条。 | 按 op 分组后批量 lookup、批量 slot validate、批量 payload slice。 |
| P1 | bitmap word 分组 | acquire/release helper 已统一，竞争规避尚未做。 | 减少同一 atomic word 的反复争用。 |
| P1 | batch 内重复 key/slot 去重 | 尚未落地。 | 热点 key 场景减少重复 lookup、validate 和 copy。 |
| P1 | VEMB optimistic read | slot seqlock 已具备基础，读路径仍以现有 validate/copy 为主。 | 读多场景减少 bitmap/metadata 慢路径成本。 |
| P1 | HOT/WARM lookup 优化 | location cache 已是主路径，仍需更细 probe 统计、prefetch 或 tiny cache。 | 降低 table lookup 固定成本。 |
| P2 | VSIM norm 预计算 | 尚未落地。 | VADD 写入时维护 norm，VSIM 时少算一次 norm。 |
| P2 | remote meta lookup batch 化 | remote meta 和 UB RPC 已具备功能路径，批量化尚未做。 | 降低跨 owner VSIM 查询成本。 |
| P2 | COLD / WARM-first overflow 完整化 | cold layer 默认关闭，no-eviction manifest 是当前主测法。 | 提升容量和恢复能力，短期吞吐收益低于热路径优化。 |
