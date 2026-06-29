# hpc-redis 相比原生 Redis 的性能优势总结

日期：2026-06-26

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

## 3. 已实现的主要收益点

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

## 4. Server 读写路径高性能设计拆解

本节按 server 内部真实链路拆开看：从 proxy 接收请求，到 SuperNode 执行，再到 TLC/UB 读写 payload。整体收益来自多层小而明确的“少做事”：少解析、少对象、少锁、少线程、少复制、少跨层语义转换。

### 4.1 总体热路径短而固定

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

### 4.2 Proxy 只做连接、搬运和背压隔离

Proxy 的职责很窄：管理 channel 生命周期、读 TCP/Aeron 请求、把请求发布到 SuperNode shard queue、把 completion 编码回 client。它不做向量计算，不读写 TLC，不维护 Redis object，也不把请求转成通用命令对象。

这个拆分带来几个收益：

1. **I/O 与计算分离**：proxy I/O worker 不被 VSIM compute、TLC lookup、UB copy 占住；SuperNode worker 也不用直接处理 socket accept/read/write 的复杂状态。
2. **channel 状态局部化**：每个 channel 保留自己的 request/response/completion 边界，便于慢客户端、response backlog、active 状态和统计隔离。
3. **请求转发轻量化**：proxy 只把固定 job copy 到 shard queue，SuperNode 直接消费 job，不经过 Redis command framework。
4. **拓扑边界清晰**：client-side topology/consistent hash 决定目标 endpoint，proxy 不做二次 hash 或 fan-out，普通请求不会被拓扑控制逻辑污染。

Redis 的通用 server 则需要把 socket 输入解析为通用命令，再进入统一 command 执行框架。对固定 vector workload 来说，这些能力很强，但每次请求都要付出额外成本。

### 4.3 Pooled worker + shard queue 避免 per-channel thread 膨胀

当前 server 使用 `proxy I/O worker pool + SuperNode worker pool`。每个请求经 shard queue 进入 SuperNode worker，completion 再按 channel 返回。这个模型避免了“一个 channel 一个线程”的扩展问题。

高并发时它的性能收益主要来自：

1. **线程数量稳定**：线程数由 `--proxy-io-threads` 和 `--supernode-workers` 控制，不随 client/channel 线性增长。
2. **cache footprint 可控**：worker 长期处理一类任务，热代码、统计字段、queue 状态更容易留在 cache 中。
3. **queue 原语轻**：SPSC/ring/shard queue 的 memory order 和 cacheline 布局比通用锁队列更贴近这个数据面。
4. **batch poll 入口**：proxy 和 SuperNode drain queue 时具备批量处理入口，后续可以继续向 batch execute 演进。

这也是 SHM/Aeron `mixed-80r20w` 能在 pooled 模型下跑到约 `7.10M QPS` 的关键原因之一。它证明高并发下主路径不应该让 channel 数量决定线程数量。

### 4.4 TCP response 用 op 语义和 writev 直接编码

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

### 4.5 SuperNode job handler 是固定 op 的小状态机

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

### 4.6 VEMB 读路径：先 cached handle，再稳定 payload snapshot

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

### 4.7 VADD 写路径：inline 写入、分片串行、same-key overwrite

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

### 4.8 VREM 删除路径保持写侧语义但不携带 payload

VREM 与 VADD 一样属于写侧拓扑/迁移语义，但它不携带 vector payload。当前设计把它和 VADD 放在同一类写侧完成路径里处理 topology epoch、ASK/MOVED、tombstone 和 delete info，但不做 vector shape 检查。

收益点：

1. **删除路径不做无用 payload 工作**：不分配、不复制、不检查 vector bytes。
2. **仍复用写侧一致性语义**：迁移、source fence、tombstone、not_found 语义和 VADD 保持一致。
3. **对 mixed 写侧统计友好**：VADD/VREM 都进入写侧计数和完成路径，但 payload 成本不同。

这类语义拆分能避免 server 为了复用代码而把所有 op 都当成“带 vector 的请求”处理。

### 4.9 TLC/UB：metadata 私有，payload packed 共享

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

### 4.10 并发一致性靠轻量 slot 版本，而不是对象生命周期

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

### 4.11 VSIM 和 remote meta 让跨 owner 查询保持窄接口

`vsim-inline` 和 `vsim-key-key` 都在 SuperNode 内部直接拿 payload slice 做 SVE cosine，不进入 Redis Search/HNSW/filter 的通用路径。

对 key-key VSIM：

1. key1 owner SuperNode 先查本地 key1 handle。
2. key2 如果本地可见，直接读取 payload slice。
3. key2 如果属于远端 owner，通过 remote meta view 获取 `{region_id, offset, bytes}`。
4. remote meta stale 时可走 repair/UB lookup RPC fallback。

这个接口很窄：跨 owner 传递的是 handle metadata，而不是完整 Redis object、完整 vector payload 或通用查询计划。它保留了 scale-out 能力，但普通同 owner 快路径仍然很短。

### 4.12 低扰动统计让优化可以继续推进

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

### 4.13 对 Redis 的收益来源总结

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

## 5. hpc-redis 与原生 Redis 线程模型差异

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

### 5.1 原生 Redis：I/O 可多线程，命令执行仍回主线

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

### 5.2 hpc-redis VEMB：I/O 与执行面都拆成 worker pool

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

### 5.3 本质收益：从网络并行变成执行并行

原生 Redis 的多线程主要缓解网络 I/O；hpc-redis 的多线程直接覆盖请求生命周期里的两段核心成本：

```text
I/O cost:
  accept/read/parse/write

Execution cost:
  lookup / slot validate / payload copy / SVE compute / completion publish
```

Redis 即使启用 I/O 线程，命令执行仍回到主线程，因此通用命令执行面不是横向扩展主路径。hpc-redis 则把 execution cost 放进 SuperNode worker pool，这也是它在固定 vector workload 下能获得巨大吞吐收益的关键。

### 5.4 代价：hpc-redis 必须自己承担并发一致性

Redis 主线程串行执行的好处是简单：很多对象生命周期、dict 修改、module 回调天然在同一执行线上完成。hpc-redis 把执行面并行化后，就必须显式处理一致性：

1. key meta shard lock 串行化同 shard 写控制面。
2. slot seqlock 保证读到稳定 payload snapshot。
3. owner generation 防止 stale handle 读到复用后的 slot。
4. completion ring 保证结果按 channel 边界返回。
5. migration source fence / tombstone 防止扩容期间读旧 source location。

因此 hpc-redis 的线程模型不是“Redis 加几个线程”，而是把通用 Redis server 改造成专用 vector 数据面：I/O 面和执行面都并行，代价是自己维护更细粒度的一致性协议。

## 6. 已有数据与结论边界

### 6.1 TCP + OBMM UB no-eviction mixed-80r20w

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

### 6.2 Aeron/SHM mixed-80r20w handle/mmap 口径

SHM/Aeron 对照中，64 线程 `mixed-80r20w` 场景下：

| Threads | Requests | OK | Fail | QPS | Avg ns/op |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 800,000 | 800,000 | 0 | 4,503,764.33 | 220.3 |
| 8 | 1,600,000 | 1,600,000 | 0 | 5,414,712.47 | 184.4 |
| 16 | 3,200,000 | 3,200,000 | 0 | 5,925,714.07 | 168.5 |
| 32 | 6,400,000 | 6,400,000 | 0 | 7,067,416.95 | 141.2 |
| 64 | 12,800,000 | 12,800,000 | 0 | 7,096,721.24 | 140.7 |

该口径不通过 TCP 返回完整 vector payload，因此不能直接和 TCP full-vector QPS 对比。它证明的是 pooled worker、Aeron/SHM ring、handle/mmap payload 模型在本地共享内存读写下的上限。

### 6.3 历史 TCP thread model 数据

早期文档中的 TCP `vemb-supernode-read` 约 `3.39M QPS`、TCP `mixed-80r20w` 约 `1.82M - 1.84M QPS` 属于历史 mode/历史口径：

1. `vemb-supernode-read` 不是当前 benchmark mode。
2. `vemb-inline-vector` 已改为当前 `vemb-inline`。
3. TCP read path 当前用 `VEMB_INLINE` 明确表达 full-vector response。

这些历史数据仍可说明 proxy/SuperNode pooled thread model 有百万级到数百万级 QPS 能力，但不应作为当前 full-vector benchmark 的最终数据引用。

### 6.4 Bitmap CAS microbench

远端 aarch64 机器上的 bitmap microbench 显示，当前生产实现使用的 `fetch_or` 在 hotspot 场景下仍优于两种 CAS 实现：

| 场景 | CAS optimized | CAS bounded | fetch_or 当前实现 |
| --- | ---: | ---: | ---: |
| 8 线程 Hotspot | 2.01 Mops/s, 497 ns, 81.39% success | 2.34 Mops/s, 427 ns, 85.65% success | 2.48 Mops/s, 403 ns, 91.04% success |
| 16 线程 Hotspot | 2.28 Mops/s, 439 ns, 77.34% success | 2.69 Mops/s, 372 ns, 78.95% success | 3.08 Mops/s, 325 ns, 86.79% success |

结论仍然成立：生产 bitmap acquire 不建议改成 CAS。后续收益应来自减少 bitmap word 竞争，而不是替换 acquire 原语。

## 7. 当前瓶颈边界

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

## 8. 后续继续优化的收益点

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

## 9. 推荐结论

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
