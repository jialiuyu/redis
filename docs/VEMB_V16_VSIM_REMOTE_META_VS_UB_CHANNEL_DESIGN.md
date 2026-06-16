# VEMB V16 VSIM Remote Meta 与 UB Channel Lookup 对比

日期：2026-06-12

## 背景

VSIM key-key 场景中，请求按 `key1` 路由到 `key1` 所属 SuperNode。
如果 `key2` 属于另一个 SuperNode，`key1` SuperNode 需要拿到 `key2`
的 vector handle，然后读取 `key2` payload 并计算 cosine。

当前有两个可选设计：

```text
方案 A: remote_meta fast path
  key1 owner 直接读取 key2 owner 发布在 UB 上的 remote_meta。

方案 B: UB 双 channel lookup RPC
  key1 owner 通过 UB request/response channel 向 key2 owner 查询 handle。
```

本文按读多写少场景分析，读写比例按“写:读 = 1:4”理解，即约 80% 读、
20% 写。如果实际含义是“读:写 = 1:4”，则结论需要重新评估。

## 方案 A：remote_meta fast path

### 路径

```text
key1 owner SuperNode
  -> local lookup key2
  -> miss
  -> 根据 key2 hash / owner_resolver 找到 key2 owner 的 remote_meta_view
  -> 读取 UB remote_meta bucket / entry
  -> 得到 candidate vector handle
  -> 后续用 slot_meta 校验 generation / write_seq
  -> 读取 key2 payload
  -> compute cosine
```

### 特点

- 不需要 key2 owner SuperNode CPU 参与。
- lookup 是少量 64B UB cacheline read。
- 适合读多写少，因为读路径最短。
- `remote_meta` 不是 truth source，只是 candidate locator。
- 后续支持淘汰后，必须通过 `slot_meta` 校验 handle 是否仍有效。

### 风险

- `remote_meta` 可能 stale。
- hash bucket 冲突会增加 probe 次数。
- UB CC/NC 可见性必须保证，否则会出现 lookup 读到旧 entry。
- remote_meta miss/stale 时需要 fallback，否则会把可恢复问题变成 NOT_FOUND。

## 方案 B：UB 双 Channel Lookup RPC

### 路径

```text
key1 owner SuperNode
  -> local lookup key2
  -> miss
  -> write lookup request 到 key2 owner 的 UB request channel

key2 owner SuperNode
  -> poll / dequeue request
  -> 用本地 TLC/private index lookup key2
  -> write response 返回 vector handle

key1 owner SuperNode
  -> poll / dequeue response
  -> 读取 key2 payload
  -> compute cosine
```

### 特点

- 返回的是 key2 owner 当前 private index 里的最新 handle。
- 对 overwrite、淘汰、slot reuse、slot pin/refcnt/lease 更友好。
- 适合做强一致 lookup、miss repair、stale repair。
- 后续如果 key2 owner 需要参与读前校验或保护 payload，RPC 模型更自然。

### 风险

- 每次远端 key2 lookup 都引入一次对端 SuperNode 执行路径。
- 包含 request write、对端 poll/dequeue、本地 lookup、response write、本端 poll/dequeue。
- 对读多场景来说，这是每个远端读请求的固定成本。
- 如果所有跨 SuperNode VSIM 都走 RPC，key2 owner 可能变成 lookup 热点。

## 现有延迟数据参考

当前文档中的相关数据：

```text
SHM/Aeron mixed-80r20w:
  peak QPS ~= 7.1M
  avg ns/op ~= 140ns
  vector_load_avg_ns ~= 48ns - 59ns
  completion_publish_avg_ns ~= 103ns - 105ns

高并发下 lookup:
  table_lookup_avg_ns ~= 1.8us - 2.4us

single-node vsim-inline:
  best QPS ~= 3.0M
  avg ns/op ~= 331ns
```

这些数据不是 UB channel lookup RPC 的直接拆解，但可以说明两个事实：

```text
1. 本地 SuperNode lookup 在高并发下可能达到 us 级。
2. completion publish / ring publish 本身不是零成本，约百 ns 量级。
```

因此，UB channel RPC 即使 request/response channel 很快，也至少多出：

```text
request write
remote poll/dequeue
remote local lookup
response write
local poll/dequeue
```

而 remote_meta fast path 通常只是：

```text
remote_meta bucket 64B read
remote_meta entry 64B read
slot_meta 64B read, if enabled
payload read
```

在 remote_meta 命中稳定、probe 短、UB 64B read 正常的情况下，方案 A
大概率低于方案 B。

## 读多写少下的判断

读多写少下，应该尽量缩短读路径，把复杂维护成本放到写路径或后台修复路径。

### 方案 A 更适合主路径

原因：

- 80% 读请求都会受益于更短 lookup。
- 写少意味着 remote_meta publish / overwrite 频率低。
- stale / collision / retry 出现概率相对可控。
- 不把每次远端读都转化为 key2 owner 的 CPU 调度。

### 方案 B 更适合 fallback

原因：

- remote_meta miss 时，owner private index 仍可能有最新 handle。
- remote_meta stale 时，RPC 可以拿到最新 handle 并顺手 repair。
- 淘汰、slot reuse、generation mismatch 时，RPC 可以作为强一致兜底。
- 后续如果引入 slot pin/refcnt/lease，RPC 可以扩展为读前授权。

## 推荐设计：混合模型

第一版建议：

```text
VSIM key1,key2:

1. key1 owner local lookup key2
   - hit: 直接读 payload

2. remote_meta lookup
   - hit: 得到 candidate handle
   - 用 slot_meta 校验 key_hash / owner_generation / write_seq
   - 校验通过: 读 payload

3. remote_meta miss / stale / busy / collision
   - 走 UB channel lookup RPC 到 key2 owner
   - key2 owner 用 private index 返回最新 handle
   - key1 owner 读 payload
   - 成功后可刷新 remote_meta
```

整体形态：

```text
remote_meta = fast path
UB channel RPC = correctness fallback + repair path
slot_meta = final validation authority
```

## 对内存淘汰的影响

引入 slot reuse 后，remote_meta 不能再被视为权威位置。

读路径必须变为：

```text
remote_meta handle
  -> slot_meta.owner_generation match
  -> slot_meta.key_hash / fingerprint match
  -> write_seq even/stable
  -> payload read
  -> write_seq still stable
```

如果校验失败：

```text
1. 认为 remote_meta stale。
2. lazy clear 或记录 stale counter。
3. fallback 到 UB channel RPC 查询 key2 owner。
4. RPC 成功后刷新 remote_meta。
```

这样可以避免 stale handle 读到被复用后的新 payload。

## 需要新增的统计

为了实测两个设计的差异，建议增加以下指标：

```text
remote_meta_lookup_count
remote_meta_lookup_ns
remote_meta_hit
remote_meta_miss
remote_meta_busy
remote_meta_stale
remote_meta_collision

slot_meta_verify_count
slot_meta_verify_ns
slot_meta_verify_ok
slot_meta_verify_generation_mismatch
slot_meta_verify_write_busy

ub_lookup_rpc_count
ub_lookup_rpc_ns
ub_lookup_rpc_timeout
ub_lookup_rpc_not_found
ub_lookup_rpc_repair_remote_meta

vsim_key2_source_local
vsim_key2_source_remote_meta
vsim_key2_source_rpc
```

关键对比口径：

```text
remote_meta_lookup_avg_ns
remote_meta_lookup_p99_ns
slot_meta_verify_avg_ns
ub_lookup_rpc_avg_ns
ub_lookup_rpc_p99_ns
vsim_job_total_avg_ns by key2_source
```

## 最终结论

在“写:读 = 1:4”的读多写少场景下：

```text
remote_meta fast path 更适合作为主路径。
UB 双 channel lookup RPC 更适合作为 fallback / repair / 强一致路径。
```

原因是 remote_meta 命中时不需要对端 SuperNode CPU 参与，读路径只包含少量
UB cacheline read；而 UB channel RPC 每次都会引入对端 poll/dequeue、本地
lookup 和 response publish，在读多场景下会成为持续成本。

推荐落地顺序：

```text
P0:
  保留 remote_meta fast path。
  增加 remote_meta lookup timing 和 hit/miss/stale stats。

P1:
  引入 slot_meta 校验，保证淘汰/slot reuse 后不会读错 payload。

P2:
  增加 UB channel lookup RPC 作为 remote_meta miss/stale fallback。

P3:
  用实际数据比较 remote_meta_lookup_avg_ns 与 ub_lookup_rpc_avg_ns。
  只有当 remote_meta tail latency 明显更差时，再考虑提升 RPC 占比。
```
