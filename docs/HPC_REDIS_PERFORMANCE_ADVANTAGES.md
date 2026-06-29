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

这条路径把 `VADD / VREM / VEMB / VSIM` 压缩为固定二进制协议、固定向量布局、固定内存访问模式和固定 worker 调度模型。相对原生 Redis，它绕开了 RESP array/bulk string 解析、Redis command framework、Redis object/SDS、RedisModule callback、通用 reply 编码、HNSW/filter/attribute 等复杂逻辑。

当前已经实现的收益主线可以概括为：

1. 用 VEMB V16 binary frame 减少通用协议解析和 reply 构造成本。
2. 用 op 语义直接表达读写口径，`VEMB_INLINE` 自身决定 TCP response 携带完整 vector payload。
3. 用 TLC metadata / payload 分离，把大块 vector bytes 放在 SHM/UB warm region 中。
4. 用 pooled proxy I/O worker 和 SuperNode worker 避免 per-channel thread 膨胀。
5. 用多 warm region、`region_id -> region_index` 映射、local weight 和 shared allocator 支撑 UB payload 放置。
6. 用 location cache、slot seqlock、owner generation 和 key meta shard 保持读写一致性。
7. 用 remote meta / UB lookup RPC 支撑跨 owner VSIM，单 owner fast path 跳过 remote meta publish。
8. 用 SVE copy / cosine、bitmap fetch_or 和 sampled timing 贴近固定 300 维 FP32 workload。

## 2. 当前协议与 benchmark 口径

当前 CLI/server 已经不再使用 `vemb-supernode-read`、`vemb-read-vector`、`vemb-inline-vector` 这些旧 mode 名称。历史文档中的这些数据只能作为旧口径参考，不能直接和当前 CLI mode 混用。

| 口径 | 当前 benchmark mode | data op | TCP 是否返回完整 vector | 说明 |
| --- | --- | --- | --- | --- |
| 写入 | `vadd` | `VEMB_V16_OP_VADD` | 否 | request 携带 inline vector，写入 TLC warm slot，response 返回 handle/状态。 |
| 删除 | `vrem` | `VEMB_V16_OP_VREM` | 否 | 不携带 vector payload，不做维度 shape 检查，走 delete/tombstone/迁移语义。 |
| handle 读 | `vemb-handle` | `VEMB_V16_OP_VEMB_HANDLE` | 否 | Aeron/SHM 场景返回 `{region_id, offset, bytes}`，client 通过 mmap warm region 读取 payload。TCP 读模式不使用该语义。 |
| TCP inline 读 | `vemb-inline` | `VEMB_V16_OP_VEMB_INLINE` | 是 | SuperNode 在 completion 中保存 snapshot，TCP response frame 后追加完整 vector bytes。300 维 FP32 约 1200B。 |
| mixed 读写 | `mixed-80r20w` | TCP 读侧 `VEMB_INLINE`，写侧 `VADD` | TCP 下读侧是 | 80% 读、20% 写。TCP mixed 的读侧返回完整 vector；Aeron/SHM mixed 读侧仍可使用 handle/mmap 口径。 |
| inline VSIM | `vsim-inline` | `VEMB_V16_OP_VSIM_INLINE` | 否 | request 携带查询 vector，SuperNode 对已存 payload 计算 score。 |
| key-key VSIM | `vsim-key-key` | `VEMB_V16_OP_VSIM_KEY_KEY` | 否 | SuperNode 查两个 key 的 handle，必要时经 remote meta / UB lookup RPC 解析 key2。 |

当前 inline 语义的关键点：

1. `VEMB_V16_OP_VEMB_INLINE` 就是 response 携带 payload 的语义来源。
2. 不再依赖单独的 `INLINE_VECTOR` request flag 或 net response flag。
3. TCP transport 只根据 `resp->op == VEMB_V16_OP_VEMB_INLINE`、`status == OK`、`vector_bytes != 0` 编码额外 payload。
4. benchmark 在 TCP read mode 下只允许 `vemb-inline` 或 `mixed-80r20w`，避免把 handle-only QPS 误当成 full-vector QPS。

## 3. 技术性能提升迭代与关键数据

这一部分只整理已经在 `docs/` 中留痕的性能迭代，不把不同口径硬拼成一条“绝对可比”的曲线。结论上可以把 hpc-redis 的性能演进分成两个阶段：

1. **Redis 内部增强阶段**：仍保留 Redis server / 模块 / 通用命令框架，只在 TLC/UB/SVE 上做加速，收益通常是 `1.1x - 1.7x`。
2. **VEMB V16 独立数据面阶段**：把 vector 读写/相似度从 Redis 通用执行面里拆出来，收益开始进入“数百万级 TCP QPS、数百万到千万级 SHM QPS”的量级。

### 3.1 阶段 A：Redis + TLC Module 的早期增益

`docs/QUICKSTART_UB_SVE.md` 记录了最早一批和 baseline Redis 的对比数据。它们仍然运行在 Redis server 形态内，因此能看到“保留 Redis 通用路径时，上限大概在哪里”：

| 场景 | Baseline Redis | TLC Module | 提升 |
| --- | ---: | ---: | ---: |
| 80R/20W 无 Pipeline | 123K QPS | 213K QPS | **1.73x** |
| 80R/20W `pipeline=16` | 747K QPS | 828K QPS | **1.11x** |
| 100% GET `pipeline=16` | 786K QPS | 889K QPS | **1.13x** |

这个阶段的意义不是绝对峰值，而是说明：

1. 即使 payload 已经是 1200B、并且底层用了 TLC/UB/SVE，只要命令执行仍挂在 Redis 通用命令框架上，收益会被 RESP、对象层和主线程执行面吃掉一大部分。
2. 无 pipeline 时收益更明显，说明底层存储访问优化是有效的；但一旦 pipeline 拉高，Redis 通用路径就更容易成为瓶颈。

### 3.2 阶段 B：VEMB V16 独立数据面起步

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

### 3.3 阶段 C：线程模型从 per-channel 走向 pooled-only

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

### 3.4 阶段 D：执行面继续放大，VSIM 进入 3M TCP 区间

`docs/vemb_v16_vsim_single_node_analysis.md` 展示了第二类关键迭代：在 `vsim-inline` workload 下，单纯增加 server 并行度就能带来大幅收益。

| Server | Client Threads | Pipeline | QPS | 相对前一阶段 |
| --- | ---: | ---: | ---: | ---: |
| `proxy=8, workers=32` | 64 | 32 | 1,688,354.42 | 1.000x |
| `proxy=16, workers=64` | 64 | 32 | 2,860,948.14 | **1.695x** |
| `proxy=16, workers=64` | 96 | 32 | **3,017,300.44** | **1.786x** vs S1 |

这组数据的含义很明确：在固定协议和 fixed-shape workload 下，瓶颈已经不在“客户端 pipeline 不够深”，而在“server 执行面并行度是否足够”。

### 3.5 阶段 E：Aeron/SHM 与最新 TCP full-vector mixed 成熟

到 2026 年 6 月，文档中已经能看到两条比较成熟的高吞吐路径：

| 路径 | workload | 最佳记录 | 文档来源 | 口径说明 |
| --- | --- | ---: | --- | --- |
| Aeron/SHM pooled | `mixed-80r20w` | **7,096,721.24 QPS** | `docs/VEMB_V16_SHM_MIXED_80R20W_BENCH.md` | 读侧可走 handle/mmap，不要求 TCP 回 1200B payload |
| TCP pooled 历史读口径 | `vemb-supernode-read` | **3,386,415.44 QPS** | `docs/VEMB_V16_THREAD_MODEL_VALIDATION.md` / `docs/HPC_REDIS_TCP_PROXY_SUPERNODE_REPORT.md` | 历史 mode，不返回完整 vector payload |
| TCP pooled 当前 full-vector | `mixed-80r20w` | **3,011,502.12 QPS** | `docs/VEMB_V16_SHM_MIXED_80R20W_BENCH.md` | TCP 读侧返回完整 1200B vector |

这里最关键的是最后一行：当前 TCP mixed 已经不是 handle-only 历史口径，而是 full-vector 口径，仍能稳定达到约 `3.01M QPS`，并且 clean run 指标为：

```text
fail=0
response error count=0
not_found=0
stale_handle=0
remote_meta_stale=0
```

这说明 hpc-redis 的优势已经不只是“共享内存模式快”，而是即使走 TCP、即使读侧真的回完整 vector payload，也仍然能维持数百万级吞吐。

### 3.6 迭代主线总结

把这些文档按时间和架构阶段串起来，性能提升主线可以概括为：

1. **先优化存储层**：TLC/UB/SVE 让 Redis 形态下拿到 `1.1x - 1.7x` 的真实收益。
2. **再重做协议和执行面**：VEMB V16 去掉 Redis 通用命令路径后，吞吐上限跨进 `1M - 3M+` 的 TCP 区间。
3. **再收敛线程模型**：pooled-only 把高并发下的线程膨胀、慢客户端拖累和 cache footprint 问题压住。
4. **最后补齐 full-vector TCP 语义**：即使读侧返回完整 payload，TCP mixed 仍能稳定在 `3.01M QPS` 左右。

## 4. 已实现的主要收益点

| 层面 | 当前实现 | 相对原生 Redis 的收益 |
| --- | --- | --- |
| 协议 | VEMB V16 固定 binary frame，请求直接携带 `op / key_hash / key / dim / vector`，响应直接携带 `handle / score / vector_bytes`。 | 避免 RESP 解析、命令查表、参数对象化和通用 reply 编码。 |
| inline 语义 | `VEMB_INLINE` op 直接表示 TCP full-vector response。 | 去掉重复 flag 判断，减少 client/server 口径不一致风险。 |
| 数据模型 | 固定 FP32 vector，payload 大小稳定，metadata 与 payload 分离。 | 避免 Redis object/SDS/listpack/HNSW 节点等通用结构成本。 |
| handle/mmap | Aeron/SHM `vemb-handle` 返回 `{region_id, offset, bytes}`，client mmap warm region 后本地读 payload。 | 非 TCP full-vector 场景避免每次返回 1200B payload，网络和复制压力更小。 |
| TCP full-vector | `vemb-inline` 返回完整 vector，但仍使用专用 frame、SuperNode snapshot 和 TCP `writev` 编码。 | payload 仍要传输，收益小于 handle-only；优势主要来自少对象层、少 reply 构造和调度稳定性。 |
| 线程模型 | `proxy I/O worker pool + SuperNode worker pool`，accept/control 只处理连接生命周期。 | 避免 per-channel thread 膨胀，降低高并发调度和 cache footprint。 |
| 慢客户端隔离 | TCP 写不动时进入 per-channel backlog，通过 `EPOLLOUT` flush。 | 慢连接不长期占住 worker，降低尾延迟扩散。 |
| 队列 | proxy 到 SuperNode 使用 shard queue，completion 仍按 channel 边界保存。SPSC ring 采用 64B head/tail、batch poll、acquire/release memory order。 | 比通用锁队列更轻，跨线程转发成本可控。 |
| TLC warm region | 一个 TLC 可挂载多个 SHM/UB warm region，内部维护 `region_id -> region_index` 小映射。 | `region_id` 对外稳定，`region_index` 本地数组友好，避免把部署 ID 和运行时下标绑死。 |
| UB payload | warm data region 只存 packed vector bytes，key/hash/state/lock 留在 SuperNode 私有 metadata。 | 大 payload 可被 mmap/UB 共享，控制结构保持 cache-friendly，也避免跨进程 metadata ABI。 |
| region 放置 | warm region hash ring 支持 local weight、local/remote alloc、fallback 和 full 统计。 | 本地 UB region 优先，容量不足时可走下一个 region，便于无 eviction 容量配置。 |
| location cache | VEMB read 先走 cached handle，命中后仍通过 slot meta 校验。 | 热读不必总是进入 key meta shard lock，同时保留 stale handle 防护。 |
| slot seqlock | `state + write_seq + owner_generation` 校验 payload 版本，copy 前后双读 `write_seq`。 | 支持无锁读稳定 snapshot，避免半写 payload 和旧 handle 被误读。 |
| key meta shard | 256 个 key meta shard，写路径按 hash 分片串行化，维护 migration state、epoch、tombstone 和 location。 | 控制面锁粒度小，普通读写路径不需要全局 Redis dict/object 锁。 |
| VADD overwrite | existing key 尽量写回原 `{region_id, local_slot, offset}`。 | 减少重新分配、bucket scan 和 handle 抖动，mixed 写入路径更稳定。 |
| VREM | delete 走 key meta / tombstone / migration 语义，不再伪装成 vector-carrying op。 | 避免无意义的 shape 检查和错误 payload 假设。 |
| remote meta | remote meta view 支持异步 publish、repair 和 stale 校验；单 owner 时跳过 publish。 | 跨 owner VSIM 可查 remote handle，单机/单 owner fast path 不承担无用 publish 成本。 |
| migration fence | `CUTOVER` / `SOURCE_GC` source fence 和 tombstone 会阻止旧 source location 被读出。 | 扩容迁移期间避免 stale source 读，普通路径通过 active counter 快速跳过。 |
| timing | SuperNode timing 使用 `monotonic.h::elapsedNs`，并只在 sampled request 上计时。 | 降低观测代码对热路径的扰动，保留足够的瓶颈可见性。 |
| helper 收敛 | hash helper、bitmap acquire/release、TLC wrapper 调用已收敛。 | 减少重复实现和薄 wrapper，热路径更直接，后续阅读和优化成本更低。 |
| UB/SVE | UB mmap、固定 stride、SVE streaming load/store/cosine。 | 固定维度向量可利用更直接的内存访问和 SIMD 路径。 |
| 功能取舍 | VEMB/UB 路径不覆盖原生 Vector Sets 的所有通用能力，如复杂 filter、attribute、HNSW 参数等。 | 用更窄语义换更短热路径，这是性能收益的重要来源。 |

## 5. Server 读写路径高性能设计拆解

本节按 server 内部真实链路拆开看：从 proxy 接收请求，到 SuperNode 执行，再到 TLC/UB 读写 payload。整体收益来自多层小而明确的“少做事”：少解析、少对象、少锁、少线程、少复制、少跨层语义转换。

### 5.1 总体热路径短而固定

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

固定协议和固定向量 shape 让 server 可以在早期就知道请求属于 `VADD/VREM/VEMB/VSIM` 哪条路径，后续结构体字段和 payload 长度也都是直接可用的，不需要每次重新解释命令参数。

### 5.2 Proxy 只做连接、搬运和背压隔离

Proxy 的职责很窄：管理 channel 生命周期、读 TCP/Aeron 请求、把请求发布到 SuperNode shard queue、把 completion 编码回 client。它不做向量计算，不读写 TLC，不维护 Redis object，也不把请求转成通用命令对象。

这个拆分带来几个收益：

1. **I/O 与计算分离**：proxy I/O worker 不被 VSIM compute、TLC lookup、UB copy 占住；SuperNode worker 也不用直接处理 socket accept/read/write 的复杂状态。
2. **channel 状态局部化**：每个 channel 保留自己的 request/response/completion 边界，便于慢客户端、response backlog、active 状态和统计隔离。
3. **请求转发轻量化**：proxy 只把固定 job copy 到 shard queue，SuperNode 直接消费 job，不经过 Redis command framework。
4. **拓扑边界清晰**：client-side topology/consistent hash 决定目标 endpoint，proxy 不做二次 hash 或 fan-out，普通请求不会被拓扑控制逻辑污染。

Redis 的通用 server 则需要把 socket 输入解析为通用命令，再进入统一 command 执行框架。对固定 vector workload 来说，这些能力很强，但每次请求都要付出额外成本。

### 5.3 Pooled worker + shard queue 避免 per-channel thread 膨胀

当前 server 使用 `proxy I/O worker pool + SuperNode worker pool`。每个请求经 shard queue 进入 SuperNode worker，completion 再按 channel 返回。这个模型避免了“一个 channel 一个线程”的扩展问题。

高并发时它的性能收益主要来自：

1. **线程数量稳定**：线程数由 `--proxy-io-threads` 和 `--supernode-workers` 控制，不随 client/channel 线性增长。
2. **cache footprint 可控**：worker 长期处理一类任务，热代码、统计字段、queue 状态更容易留在 cache 中。
3. **queue 原语轻**：SPSC/ring/shard queue 的 memory order 和 cacheline 布局比通用锁队列更贴近这个数据面。
4. **batch poll 入口**：proxy 和 SuperNode drain queue 时具备批量处理入口，后续可以继续向 batch execute 演进。

这也是 SHM/Aeron `mixed-80r20w` 能在 pooled 模型下跑到约 `7.10M QPS` 的关键原因之一。它证明高并发下主路径不应该让 channel 数量决定线程数量。

### 5.4 TCP response 用 op 语义和 writev 直接编码

TCP full-vector read 不再靠额外 `INLINE_VECTOR` flag 判断，而是 `VEMB_V16_OP_VEMB_INLINE` 本身表示 response 后面追加完整 vector payload。

这使 TCP response 编码更简单：

```text
resp.op == VEMB_INLINE && status == OK && vector_bytes > 0
  -> response frame + inline vector snapshot
else
  -> response frame only
```

高性能点在于：

1. **语义单一**：client/server 不需要同时判断 op、request flag、net flag，多一层组合就多一类错配风险。
2. **响应构造固定**：response header 和 payload 都是固定结构，TCP transport 可以用 `writev` 批量写 frame 和 inline payload。
3. **慢客户端隔离**：socket 写不动时进入 per-channel backlog，再通过 `EPOLLOUT` flush，不让慢连接长期占住 worker。
4. **handle-only 路径更轻**：Aeron/SHM `vemb-handle` 只返回 handle，client mmap payload，避免每次复制/返回 1200B vector。

Redis 通用 reply builder 要处理多种 RESP 类型、bulk string 长度、client output buffer 策略和命令返回形态。VEMB 的 response 形态窄很多，因此可以做得更直。

### 5.5 SuperNode job handler 是固定 op 的小状态机

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

这相当于把 Redis 的“通用命令解释器”替换成了一个专用、可预测、分支很少的数据面状态机。

### 5.6 VEMB 读路径：先 cached handle，再稳定 payload snapshot

VEMB read 的快路径是：

```text
SuperNode
  -> vemb_v16_tlc_get_cached_handle()
  -> tlc_core_get_cached_warm_location()
  -> location_cache_peek()
  -> slot meta validate
  -> handle response or inline payload snapshot
```

如果 cached handle 命中且 slot meta 校验通过，就不需要进入更重的 key meta shard 查找。这里的性能收益很直接：

1. **读热路径不拿 key meta shard lock**：普通热读可以走 location cache + slot 校验。
2. **cache hit 后仍验证**：不会因为 cache 快就牺牲一致性，slot `state/write_seq/owner_generation/key_hash` 会继续防 stale。
3. **inline snapshot 只在 TCP full-vector 场景发生**：handle/mmap 读不复制 payload，TCP inline 才复制到 completion。
4. **cache miss 才 full lookup**：只有 miss/stale 时退回 `vemb_v16_tlc_get_handle()`，避免每次都扫描/锁定完整 metadata。

Redis 的 GET/模块读路径通常围绕通用 dict、robj、SDS/module value 展开。VEMB read 面向固定 vector handle，把 lookup 和 payload 读取拆开，热读成本更接近一次稳定 location 解析。

### 5.7 VADD 写路径：inline 写入、分片串行、same-key overwrite

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

高性能点：

1. **没有 Redis object/SDS 分配链**：固定 1200B payload 直接写 packed warm arena。
2. **key meta shard 分片锁**：写控制面按 hash 分成多个 shard，不需要全局大锁。
3. **same-key overwrite 保持位置稳定**：已有 key 优先写回原 `{region_id, local_slot, offset}`，减少重新分配、重新 hash、handle 抖动和 remote meta churn。
4. **location_cache 写后更新**：写成功后直接更新读热路径需要的 location。
5. **单 owner 跳过 remote meta publish**：`remote_meta_view_count <= 1` 时不做无意义异步 publish，mixed 写侧少一段后台队列成本。
6. **迁移逻辑可被 fast path 跳过**：非迁移状态下普通写不需要走 delta/outbox/fence 的重流程。

Redis 写路径的优势是通用和完整，但也意味着 object 编码、dict 更新、module value 生命周期、可能的索引更新都在命令路径中。VADD 牺牲通用性，换成固定 vector slot 写入。

### 5.8 VREM 删除路径保持写侧语义但不携带 payload

VREM 与 VADD 一样属于写侧拓扑/迁移语义，但它不携带 vector payload。当前设计把它和 VADD 放在同一类写侧完成路径里处理 topology epoch、ASK/MOVED、tombstone 和 delete info，但不做 vector shape 检查。

收益点：

1. **删除路径不做无用 payload 工作**：不分配、不复制、不检查 vector bytes。
2. **仍复用写侧一致性语义**：迁移、source fence、tombstone、not_found 语义和 VADD 保持一致。
3. **对 mixed 写侧统计友好**：VADD/VREM 都进入写侧计数和完成路径，但 payload 成本不同。

这类语义拆分能避免 server 为了复用代码而把所有 op 都当成“带 vector 的请求”处理。

### 5.9 TLC/UB：metadata 私有，payload packed 共享

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
2. **client 可 mmap 读 payload**：handle 口径下 client 直接通过 `{region_id, offset, bytes}` 定位，不要求 server 每次返回完整 vector。
3. **UB region 可多块挂载**：一个 TLC 可挂载多个 warm region，按 hash ring 和 local weight 选择 region。
4. **`region_id` 对外稳定**：协议和 client 只看稳定 ID；server 内部用 `region_index` 访问数组，靠小映射衔接。
5. **shared allocator 支撑多 region 容量管理**：warm slot 分配与 region runtime 解耦，manifest 可以用足大 UB region，避免 cold layer 关闭时 eviction 造成 not_found。

Redis 的 value 通常是对象化的通用内存结构，既要承载类型语义，也要参与 Redis 对象生命周期。TLC/UB 把 vector workload 最重的 payload 从通用对象系统里拿了出来。

### 5.10 并发一致性靠轻量 slot 版本，而不是对象生命周期

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

### 5.11 VSIM 和 remote meta 让跨 owner 查询保持窄接口

`vsim-inline` 和 `vsim-key-key` 都在 SuperNode 内部直接拿 payload slice 做 SVE cosine，不进入 Redis Search/HNSW/filter 的通用路径。

对 key-key VSIM：

1. key1 owner SuperNode 先查本地 key1 handle。
2. key2 如果本地可见，直接读取 payload slice。
3. key2 如果属于远端 owner，通过 remote meta view 获取 `{region_id, offset, bytes}`。
4. remote meta stale 时可走 repair/UB lookup RPC fallback。

这个接口很窄：跨 owner 传递的是 handle metadata，而不是完整 Redis object、完整 vector payload 或通用查询计划。它保留了 scale-out 能力，但普通同 owner 快路径仍然很短。

### 5.12 低扰动统计让优化可以继续推进

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

### 5.13 对 Redis 的收益来源总结

把上述链路合在一起，hpc-redis 的巨大收益来自这些明确取舍：

| Redis 通用成本 | VEMB server 当前做法 | 收益来源 |
| --- | --- | --- |
| RESP 解析和通用命令分发 | 固定 VEMB V16 binary frame + op dispatch | 少解析、少分支、少对象化。 |
| Redis object/SDS/module value 生命周期 | fixed vector shape + packed warm payload | 少分配、少间接访问、cache 更稳定。 |
| 通用 reply builder | 固定 response struct，TCP inline 用 op 决定 payload | reply 编码更直，`writev` 可直接拼 frame/payload。 |
| 每连接/命令路径混合 I/O 与执行 | proxy I/O pool + SuperNode worker pool | I/O 和计算隔离，线程数稳定。 |
| 通用 dict/object 锁和生命周期语义 | key meta shard + slot seqlock + owner_generation | 锁粒度更小，读路径可无锁验证。 |
| 每次从 server 返回完整 value | handle/mmap 或按需 TCP inline snapshot | handle 口径避免 1200B payload 返回。 |
| 通用搜索/索引能力 | 直接 payload slice + SVE cosine | 固定场景下少走 HNSW/filter/attribute 通用逻辑。 |
| 跨节点通用对象同步 | remote meta handle + UB lookup RPC | 跨 owner 只传播定位元信息，普通快路径不变重。 |

因此，hpc-redis 的性能不是单点 trick，而是一整条 server 数据面把 Redis 的通用性成本系统性移除：proxy 只搬运，SuperNode 只执行固定 op，TLC 只管理 vector location 和一致性，UB/SHM 只承载 packed payload。

## 6. hpc-redis 与原生 Redis 线程模型差异

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

### 6.1 原生 Redis：I/O 可多线程，命令执行仍回主线

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

### 6.2 hpc-redis VEMB：I/O 与执行面都拆成 worker pool

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

### 6.3 本质收益：从网络并行变成执行并行

原生 Redis 的多线程主要缓解网络 I/O；hpc-redis 的多线程直接覆盖请求生命周期里的两段核心成本：

```text
I/O cost:
  accept/read/parse/write

Execution cost:
  lookup / slot validate / payload copy / SVE compute / completion publish
```

Redis 即使启用 I/O 线程，命令执行仍回到主线程，因此通用命令执行面不是横向扩展主路径。hpc-redis 则把 execution cost 放进 SuperNode worker pool，这也是它在固定 vector workload 下能获得巨大吞吐收益的关键。

### 6.4 代价：hpc-redis 必须自己承担并发一致性

Redis 主线程串行执行的好处是简单：很多对象生命周期、dict 修改、module 回调天然在同一执行线上完成。hpc-redis 把执行面并行化后，就必须显式处理一致性：

1. key meta shard lock 串行化同 shard 写控制面。
2. slot seqlock 保证读到稳定 payload snapshot。
3. owner generation 防止 stale handle 读到复用后的 slot。
4. completion ring 保证结果按 channel 边界返回。
5. migration source fence / tombstone 防止扩容期间读旧 source location。

因此 hpc-redis 的线程模型不是“Redis 加几个线程”，而是把通用 Redis server 改造成专用 vector 数据面：I/O 面和执行面都并行，代价是自己维护更细粒度的一致性协议。

## 7. TCP mode 下相对 Redis 提升更大的原因

这一节只讨论 **TCP 模式**，因为它最接近“真实跨机部署”的读法，也最容易和 Redis 网络模式放在一起理解。

### 7.1 先说明可比性边界

当前 `docs/` 里有两类数据：

1. **严格同类对照的旧数据**：`QUICKSTART_UB_SVE.md` 中 baseline Redis vs TLC module。
2. **更强但不完全同口径的新数据**：VEMB V16 standalone TCP `vemb-inline` / `mixed-80r20w` / `vsim-inline`。

因此，下面的判断要分成两层：

1. **严格证据**：Redis 形态下，单靠 TLC/UB/SVE，网络场景大约能得到 `1.11x - 1.73x`。
2. **架构推论 + 新数据支持**：当 VEMB V16 把 Redis 通用路径整体拿掉后，TCP 吞吐能进一步抬到 `3M` 左右，这部分收益主要来自数据面重做，而不是单点 SIMD 或缓存优化。

### 7.2 为什么 Redis 形态下提升有限

早期同机网络 benchmark 的结果是：

| 场景 | Baseline Redis | TLC Module | 提升 |
| --- | ---: | ---: | ---: |
| 80R/20W 无 Pipeline | 123K | 213K | 1.73x |
| 80R/20W `pipeline=16` | 747K | 828K | 1.11x |
| 100% GET `pipeline=16` | 786K | 889K | 1.13x |

这个现象说明，Redis 模式下真正被优化到的主要是“值的存储和访问”，但下面这些成本仍然还在：

1. `RESP` 解析和 bulk string 编码。
2. `processCommand -> call -> cmd->proc` 的通用命令执行框架。
3. `robj/SDS/module value` 的对象生命周期。
4. 主线程执行命令的串行模型。
5. 通用 reply builder 和 client 输出缓冲语义。

也就是说，TLC Module 阶段是在 **Redis 外壳不变** 的前提下优化“里面那一块存储引擎”，所以收益是真的，但上限有限。

### 7.3 为什么 VEMB V16 TCP 模式能明显拉开差距

VEMB V16 的 TCP 优势不是来自“把 Redis TCP 改得更快”，而是把 TCP 后面的整条命令执行链换掉了。

#### 7.3.1 协议层：少一次通用命令解释

Redis TCP 路径：

```text
socket
  -> RESP parse
  -> argv / bulk string
  -> command lookup
  -> command proc
```

VEMB TCP 路径：

```text
socket
  -> fixed binary frame
  -> op dispatch
  -> shard job
  -> SuperNode handler
```

这里的收益不是“省掉几十行代码”，而是省掉整套通用参数解释和对象化过程。对于 `dim=300`、`value_size=1200` 的固定 workload，二进制 frame 能让 payload 在进入 server 后几乎立刻变成可执行 job。

#### 7.3.2 执行层：Redis 是网络并行，VEMB 是执行并行

Redis 即使开了 `io-threads`，命令执行大体仍回主线程。VEMB V16 则把真正的业务成本放进 `supernode worker pool`：

```text
lookup
slot validate
payload copy / slice
SVE cosine
completion publish
```

这就是为什么 `vsim-inline` 的服务器并行度从 `8/32` 放大到 `16/64` 时，QPS 能从 `1.688M` 提到 `2.861M`，增幅约 **69.5%**。说明收益来源不是 TCP 调优本身，而是执行面可以真正横向扩展。

#### 7.3.3 数据模型：Redis 返回 value，VEMB 返回 handle 或定制 response

Redis 的 value 是通用对象；VEMB 的 vector payload 是固定大小的 packed bytes。这样一来：

1. 非 TCP full-vector 场景可以直接返回 handle，让 client mmap 读 payload。
2. TCP full-vector 场景也只需要固定 response header + payload snapshot，不需要通用 bulk string builder。
3. `VADD` / `VEMB` / `VSIM` 的返回结构都很窄，编码路径稳定。

这对 1200B vector 非常重要，因为回复路径如果还是通用对象编码，payload 越大，Redis 风格的通用回复开销就越明显。

#### 7.3.4 存储与一致性：为 fixed-shape payload 定制，而不是沿用对象生命周期

VEMB 用的是：

1. `location cache`
2. `key meta shard`
3. `slot state/write_seq/owner_generation`
4. `packed warm payload`

Redis 依赖的是更通用的对象和 DB 生命周期语义。前者的好处是：

1. 热读路径可以先走 cached handle，再做 slot 校验。
2. 写路径可以 same-key overwrite，减少位置抖动。
3. 读写一致性用 slot 版本就能解决，不必让每个 payload 都套进通用对象体系。

这让 VEMB 的每次请求更像“定位一段固定长度内存并校验版本”，而不是“取一个通用 value 对象并沿着通用命令语义走完全程”。

#### 7.3.5 功能面：有意放弃通用能力，换更短热路径

Redis 和原生 Vector Sets 需要为更广的功能面负责，例如：

1. 通用命令协议。
2. 更多返回形态。
3. 更完整的对象语义。
4. HNSW/filter/attribute 等通用向量检索能力。

VEMB 当前只服务固定 `VADD/VREM/VEMB/VSIM` 数据面。功能更窄，所以热路径才能更短。这不是“额外 bonus”，而是性能差距的主要来源之一。

### 7.4 为什么 TCP 模式仍然明显慢于 Aeron/SHM

即使 VEMB TCP 已经能到 `3.01M QPS`，它仍低于 SHM/Aeron `mixed-80r20w` 的 `7.10M QPS`。原因也很直接：

1. TCP 需要走 kernel socket stack。
2. TCP full-vector read 需要真的回传 1200B payload。
3. 当前 `snapshot_vemb_payload()` 仍有 per-request `zmalloc + copy` 成本。
4. 慢客户端/backpressure 处理是 TCP 必须承担的额外复杂性。

所以正确理解不是“TCP 不够好”，而是：

1. **对比 Redis TCP**：VEMB TCP 已经通过专用数据面拿到了大幅收益。
2. **对比 VEMB SHM/Aeron**：TCP 仍然要为网络协议和 payload 传输付出更多成本，这是物理边界，不是架构失误。

### 7.5 面向 TCP vs Redis 的结论

如果只看 `docs/` 里的证据，最稳妥的结论是：

1. 保留 Redis 外壳时，hpc-redis 能拿到 `1.1x - 1.7x` 的网络性能提升。
2. 拆掉 Redis 通用命令路径、改成 VEMB V16 独立数据面后，TCP 吞吐进入 `3M` 量级。
3. 这部分额外收益主要来自协议、执行面、对象模型、回复路径和功能面一起变窄，而不是单一的 SIMD 或内存优化。

## 8. 已有数据与结论边界

### 8.1 TCP + OBMM UB no-eviction mixed-80r20w

最新已记录的 clean run 使用单机 TCP、两个 8G OBMM warm region、一个 UB remote meta region：

```text
server: proxy-io-threads=16, supernode-workers=32, dim=300, max-vectors=131072
bench:  mixed-80r20w, prefill=65536, keyspace=65536, ops/thread=200000, threads=128, pipeline=32
```

结果：

```text
[done] mode=mixed-80r20w threads=128 ok=25600000 fail=0 qps=3011502.12 avg_thread_ns/op=331.7 read_bytes=24576000000
```

关键稳定性信号：

```text
response error count=0
not_found=0
evict_ok=0
evict_fail=0
stale_handle=0
remote_meta_stale=0
warm_alloc_local=65536
warm_overwrite=5120000
```

这个数据的含义是：在无 warm eviction、cold layer 关闭、TCP 读侧返回完整 vector 的口径下，当前 VEMB V16 主链路可以稳定跑到约 `3.01M QPS`，并且没有 response error、not_found、stale handle 或 remote meta stale。

### 8.2 TCP encoded payload mixed-80r20w

在同一类单机 TCP `mixed-80r20w` full-vector 口径下，bench 开启 `--tcp-encoded-payloads` 后，最近一次实测结果为：

```text
[done] mode=mixed-80r20w threads=128 ok=25600000 fail=0 qps=2746793.21 avg_thread_ns/op=363.7 read_bytes=24576000000
```

关键稳定性信号：

```text
request_publish_spins=0
response_empty_polls=0
not_found=0
job_ring_vemb=0
job_ring_vadd=0
response_ring=0
completion_ring=0
remote_meta_stale=0
```

对应 sampled stats：

```text
table_lookup_avg_ns=378.8
vector_load_avg_ns=559.3
completion_publish_avg_ns=28.9
```

和当前 raw TCP full-vector `mixed-80r20w` 参考值 `3,011,502.12 QPS` 对比，encoded payload 模式大约低 `8.8%`。这说明：

1. encode/decode 路径已经功能正确，可稳定完成 prefill 和 2560 万次混合读写。
2. 当前额外开销主要来自 TCP 边界的 request encode/response decode 与 proxy 侧 decode/encode，而不是 TLC correctness 或队列容量问题。
3. 现阶段 encoded 模式更适合作为协议兼容/可移植选项，而不是默认高性能路径。

### 8.3 Aeron/SHM mixed-80r20w handle/mmap 口径

SHM/Aeron 对照中，64 线程 `mixed-80r20w` 场景下：

| Threads | Requests | OK | Fail | QPS | Avg ns/op |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 800,000 | 800,000 | 0 | 4,503,764.33 | 220.3 |
| 8 | 1,600,000 | 1,600,000 | 0 | 5,414,712.47 | 184.4 |
| 16 | 3,200,000 | 3,200,000 | 0 | 5,925,714.07 | 168.5 |
| 32 | 6,400,000 | 6,400,000 | 0 | 7,067,416.95 | 141.2 |
| 64 | 12,800,000 | 12,800,000 | 0 | 7,096,721.24 | 140.7 |

该口径不通过 TCP 返回完整 vector payload，因此不能直接和 TCP full-vector QPS 对比。它证明的是 pooled worker、Aeron/SHM ring、handle/mmap payload 模型在本地共享内存读写下的上限。

### 8.4 历史 TCP thread model 数据

早期文档中的 TCP `vemb-supernode-read` 约 `3.39M QPS`、TCP `mixed-80r20w` 约 `1.82M - 1.84M QPS` 属于历史 mode/历史口径：

1. `vemb-supernode-read` 不是当前 benchmark mode。
2. `vemb-inline-vector` 已改为当前 `vemb-inline`。
3. TCP read path 当前用 `VEMB_INLINE` 明确表达 full-vector response。

这些历史数据仍可说明 proxy/SuperNode pooled thread model 有百万级到数百万级 QPS 能力，但不应作为当前 full-vector benchmark 的最终数据引用。

### 8.5 Bitmap CAS microbench

远端 aarch64 机器上的 bitmap microbench 显示，当前生产实现使用的 `fetch_or` 在 hotspot 场景下仍优于两种 CAS 实现：

| 场景 | CAS optimized | CAS bounded | fetch_or 当前实现 |
| --- | ---: | ---: | ---: |
| 8 线程 Hotspot | 2.01 Mops/s, 497 ns, 81.39% success | 2.34 Mops/s, 427 ns, 85.65% success | 2.48 Mops/s, 403 ns, 91.04% success |
| 16 线程 Hotspot | 2.28 Mops/s, 439 ns, 77.34% success | 2.69 Mops/s, 372 ns, 78.95% success | 3.08 Mops/s, 325 ns, 86.79% success |

结论仍然成立：生产 bitmap acquire 不建议改成 CAS。后续收益应来自减少 bitmap word 竞争，而不是替换 acquire 原语。

## 9. 当前瓶颈边界

绕过 Redis/RESP 后，瓶颈已经转移到 SuperNode 和 TLC 内部的逐条固定成本：

```text
job dispatch
  -> cached handle / full lookup
  -> slot meta validate
  -> payload slice or snapshot copy
  -> VSIM compute or VEMB response build
  -> completion publish
  -> TCP/Aeron response
```

当前需要区分几个边界：

1. TCP `vemb-inline` 和 TCP `mixed-80r20w` 读侧必须传输 1200B payload，收益天然低于 handle/mmap 口径。
2. `snapshot_vemb_payload()` 当前仍会为 inline response 做 per-request `zmalloc` 和 payload copy；简单全局槽池曾实测负收益，尚未落地替代方案。
3. no-eviction bench 依赖足够大的 warm region。当前 cold layer 默认关闭，如果 manifest 容量太小并触发 eviction，可能出现 `not_found` 和 `response error`。
4. 高并发 SHM/Aeron handle 口径下，`table_lookup_avg_ns` 和 `bitmap_lock_avg_ns` 会明显上升，说明 lookup 与 bitmap word 竞争仍是扩展性瓶颈。
5. sampled timing 已降低观测扰动，但更细的 batch、hot key、slot conflict 指标仍有补充价值。

## 10. 后续继续优化的收益点

| 优先级 | 优化项 | 当前状态 | 预期收益 |
| --- | --- | --- | --- |
| P0 | TCP inline per-request allocation | 当前仍为 per-request `zmalloc + copy`；简单全局池已回退。 | 做 per-worker/per-channel cache 或生命周期内复用，降低 full-vector CPU 成本和尾延迟。 |
| P0 | batch 指标补齐 | 已有 sampled timing，但 batch rounds、avg batch、HOT hit/miss、WARM probe、bitmap word conflict 仍不完整。 | 先量化瓶颈，避免盲调。 |
| P1 | SuperNode batch execute | 当前主要是 batch poll，执行仍偏逐条。 | 按 op 分组后批量 lookup、批量 slot validate、批量 payload slice。 |
| P1 | bitmap word 分组 | acquire/release helper 已统一，竞争规避尚未做。 | 减少同一 atomic word 的反复争用。 |
| P1 | batch 内重复 key/slot 去重 | 尚未落地。 | 热点 key 场景减少重复 lookup、validate 和 copy。 |
| P1 | VEMB optimistic read | slot seqlock 已具备基础，读路径仍以现有 validate/copy 为主。 | 读多场景减少 bitmap/metadata 慢路径成本。 |
| P1 | HOT/WARM lookup 优化 | location cache 已是主路径，仍需更细 probe 统计、prefetch 或 tiny cache。 | 降低 table lookup 固定成本。 |
| P2 | VSIM norm 预计算 | 尚未落地。 | VADD 写入时维护 norm，VSIM 时少算一次 norm。 |
| P2 | remote meta lookup batch 化 | remote meta 和 UB RPC 已具备功能路径，批量化尚未做。 | 降低跨 owner VSIM 查询成本。 |
| P2 | COLD / WARM-first overflow 完整化 | cold layer 默认关闭，no-eviction manifest 是当前主测法。 | 提升容量和恢复能力，短期吞吐收益低于热路径优化。 |

## 11. 推荐结论

当前 hpc-redis 相比原生 Redis 的性能优势可以归纳为：

1. **专用协议胜过通用 RESP**：固定 frame 减少解析、对象和 reply 编码成本。
2. **op 语义胜过额外 flag 组合**：`VEMB_INLINE` 直接定义 TCP full-vector response，client/server 更容易保持一致。
3. **TLC/UB payload 模型胜过 Redis object path**：payload packed 存放，metadata 私有且 cache-friendly。
4. **handle/mmap 胜过每次完整返回**：Aeron/SHM 场景收益最大，TCP full-vector 场景仍受 1200B payload 传输约束。
5. **pooled worker 胜过 per-channel thread**：高并发下调度、cache 和慢客户端隔离更稳定。
6. **当前瓶颈已从 Redis 路径转移到 lookup、slot validate、payload copy、completion publish 和 TCP payload 传输**。
7. **bitmap 不应 CAS 化**：现有 microbench 支持继续使用 `fetch_or`，优化重点是降低 word 竞争。

后续性能提升主线建议保持为：

```text
补齐 batch / conflict 指标
  -> inline payload per-worker 复用
  -> SuperNode batch execute
  -> bitmap word 分组 / 重复 key 去重
  -> VEMB optimistic read
  -> VSIM / remote meta / COLD overflow 继续完善
```
