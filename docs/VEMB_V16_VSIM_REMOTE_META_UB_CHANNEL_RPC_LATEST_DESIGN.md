# VEMB V16 VSIM remote_meta 与 UB Channel RPC 最新设计

日期：2026-06-15

## 结论

最新设计采用混合模型：

```text
remote_meta:
  只服务 VSIM key-key 这类跨 owner 读取 key2 的场景。
  作为 fast path candidate directory，不作为权威索引。
  VADD 不同步等待 remote_meta 发布完成。

UB channel RPC:
  作为 remote_meta miss / stale / busy / evict miss 的 correctness fallback。
  由 key2 owner 使用本地 private TLC index 查询并返回结果。

slot_meta / owner private index:
  仍然是最终正确性来源。
```

读路径优先级：

```text
1. key1 owner 本地查 key2。
2. 若 key2 owner 不是本地，查 key2 owner 发布的 remote_meta。
3. remote_meta 命中后只得到 candidate vector handle。
4. 用 WARM slot_meta 校验 key_hash / owner_generation / write_seq / bytes。
5. 校验通过后读取 payload 并计算 VSIM。
6. remote_meta miss / stale / busy / set conflict miss 时，走 UB channel RPC 兜底。
```

## 边界

### remote_meta 仅给 VSIM 使用

`remote_meta` 不进入普通 `VEMB` 读路径，也不改变 `VADD` 主写路径。它只解决：

```text
VSIM key1 key2:
  请求按 key1 路由到 key1 owner。
  key1 owner 需要快速找到 key2 的远端 vector handle。
```

这能把 `remote_meta` 的复杂度限制在远端 VSIM 场景内，避免普通读写都背上远端目录维护成本。

### 不提供跨连接同 key 顺序语义

当前系统保证 connection/channel 级 FIFO；不同 connection 的相同 key 请求不会被 key-level lock 全局串行化。

因此 `remote_meta` 不能假设跨连接 VADD 与 VSIM 之间天然线性化。正确性必须依赖：

```text
1. payload 写入 happens-before handle publish。
2. handle 带 owner_generation。
3. reader 使用 slot_meta 验证 generation 和 write_seq。
4. stale 时 fallback RPC。
```

## remote_meta 放置位置

推荐每个 owner SuperNode 发布一块独立的 UB meta region：

```text
owner SuperNode N:
  private TLC index:
    owner 本地权威索引，不共享给其他节点直接写。

  remote_meta_N:
    owner 发布的只读目录。
    其他 SuperNode 通过 manifest attach。
    entry 指向 owner 管理的 WARM payload slot。
```

也就是说，`remote_meta` 不嵌入每个 WARM payload region 内部，也不做全局单表；它是按 owner 拆分的 published directory。

```text
key owner 决定:
  谁维护 private index。
  谁发布该 key 的 remote_meta entry。

payload region 决定:
  vector 实际存放在哪个 UB WARM region。
```

一个 owner 的 key 可能分布在多个 WARM UB region 中，`remote_meta` entry 通过 `region_id/local_slot/offset/owner_generation` 指向具体位置。

## remote_meta 结构

目标结构采用 set-associative cache，而不是当前 bucket + entry_index
的 open-address directory。

核心原因：

```text
1. remote_meta 是 best-effort fast path，不是 correctness source。
2. miss / evict / stale 都可以通过 UB channel RPC fallback 修复。
3. 读多写少场景更需要固定 probe 上限和 cacheline 可控访问。
4. set + way 不需要 tombstone/free-list，也不需要维护 probe chain。
```

推荐第一版参数：

```text
header: 64B
set_count: power-of-two
ways: 4 或 8
entry: 64B * set_count * ways
```

定位方式：

```text
set = mix(key_hash) & (set_count - 1)
ways = entries[set * ways ... set * ways + ways - 1]
```

每个 way 是一个完整 locator entry：

```text
entry:
  version            odd = writing, even = stable
  state              empty / valid / evicting
  flags
  key_hash
  key_fingerprint
  region_id
  local_slot
  owner_generation
  offset
  access_epoch 或 clock_bit
```

lookup 只扫描固定 ways：

```text
for way in set:
  read version
  if stable and key_hash/fingerprint match:
      return candidate handle
return NOT_FOUND
```

publish 先扫描同 set：

```text
1. 若同 key 已存在，update-in-place。
2. 若有 empty way，写入 empty way。
3. 若 set 满，按 CLOCK/epoch 选一个 way 替换。
```

不采用“纯 set”。VSIM 需要的是 locator，不只是 membership；纯 set 命中后仍需要另一张表拿 handle，最终会退化成 map。这里的 set-associative cache 是“set + ways + complete handle entry”。

## 与 WARM set-associative 的复用边界

`remote_meta` 可以复用 WARM 层的策略思想，但不直接复用 WARM payload slot
实现。

可以复用：

```text
1. key_hash -> set/way bounds 的小 helper。
2. CLOCK bit / access_epoch victim policy。
3. odd/even version 发布模式。
4. 64B cacheline 对齐习惯。
5. hit/miss/evict/stale 统计模型。
```

不复用：

```text
1. WARM slot state/write_seq/cold_state 状态机。
2. payload write / slot allocation / shared_allocator 逻辑。
3. WARM eviction committed 语义。
```

原因是 WARM cache 拥有 payload slot，`remote_meta` 只保存 candidate handle。
`remote_meta` evict 一个 entry 只会降低 fast path 命中率，不会删除 vector。

## remote_meta 内存复用策略

set-associative 版本不引入 tombstone/free-list。内存复用通过 way 内替换完成。

原因：

```text
1. 当前没有 delete 语义。
2. WARM 淘汰不会删除 key 的逻辑存在性，只会让旧 handle stale。
3. same-key overwrite 可以 update-in-place，不消耗新 entry。
4. set 内 conflict 可以直接替换某个 way。
5. tombstone/free-list 会引入 ABA、probe-chain 修复、并发 reader 可见性问题。
```

set 满时的策略：

```text
1. 优先替换 clock_bit=0 或 access_epoch 最旧的 way。
2. 若所有 way 都处于 writing/busy，返回 BUSY。
3. BUSY 不阻塞 VADD，记录 counter 和限频日志。
4. 被替换 key 后续 VSIM remote_meta miss，走 UB channel RPC 兜底。
```

entry 替换必须仍然使用 version odd/even 发布，reader 看到 writing 或 version
变化时返回 BUSY/NOT_FOUND，不能读半条 handle。

## VADD 异步发布 remote_meta

`VADD` 主路径只负责：

```text
1. 写 cold。
2. 写 WARM payload。
3. 发布 WARM slot_meta READY。
4. 返回 vector handle / completion。
```

`remote_meta` 发布改为异步：

```text
SuperNode VADD worker
  -> TLC put 成功
  -> 生成 remote_meta_publish_event
  -> 投递到本 owner 的 remote_meta publish queue
  -> VADD completion 不等待该事件完成

remote_meta publisher
  -> 消费 publish_event
  -> 写 remote_meta entry
  -> version odd/even 发布
```

异步发布要求：

```text
payload + slot_meta READY happens-before publish_event visible
publish_event visible happens-before remote_meta entry stable
```

队列建议为有界队列。队列满时允许丢弃事件或做 key_hash coalescing，但不能反向阻塞 VADD。丢弃的代价只是 remote_meta fast path 暂时 miss，VSIM 仍可通过 UB channel RPC 找到 key2。

建议新增统计：

```text
remote_meta_publish_async_enqueue
remote_meta_publish_async_drop
remote_meta_publish_async_coalesce
remote_meta_publish_ok
remote_meta_publish_busy
remote_meta_publish_insert
remote_meta_publish_update
remote_meta_publish_evict
remote_meta_publish_ns
```

## VSIM 远端读取流程

### Fast path

```text
key1 owner:
  local lookup key1
  resolve key2 owner
  if key2 owner == local:
      local lookup key2
  else:
      lookup remote_meta_owner[key2_owner]
      if remote_meta hit:
          build candidate handle
          validate WARM slot_meta
          if valid:
              read key2 payload
              compute cosine
```

`remote_meta` lookup 成功不代表 vector 可读，只代表拿到了 candidate。最终必须校验：

```text
region_id matches an attached WARM region
offset == local_slot * value_size
bytes == value_size
slot key_hash / fingerprint matches
owner_generation matches
write_seq is even and stable
state == READY
```

### Fallback path

```text
remote_meta miss / busy / stale / set conflict miss:
  key1 owner -> UB channel RPC -> key2 owner
  key2 owner local private index lookup key2
  key2 owner returns one of:
    HANDLE
    SNAPSHOT
    NOT_FOUND
    ERROR
  key1 owner consumes response
```

RPC 成功返回 handle 时，key1 owner 仍然执行 slot_meta 校验。校验失败可以重试一次 RPC；连续失败应返回 NOT_FOUND/ERROR 并打日志，不要猜测数据状态。

RPC 成功后可以触发 repair：

```text
if response.kind == HANDLE and slot_meta valid:
    async repair remote_meta entry for key2 owner
```

repair 也不应阻塞当前 VSIM completion。

## RPC 返回 HANDLE 还是 SNAPSHOT

RPC response 支持 optional payload 形态：

```text
HANDLE:
  返回 region_id/local_slot/offset/owner_generation。
  key1 owner 自己读取 UB payload。
  默认优先，响应小，适合正常 UB 可读场景。

SNAPSHOT:
  key2 owner 直接把 vector payload 放进 response。
  适合远端 payload 暂不可读、需要 owner 侧保护读、或调试 stale 问题。
  成本高，不进入 remote_meta。
```

推荐第一版：

```text
remote_meta:
  只存 HANDLE。

UB channel RPC:
  默认返回 HANDLE。
  协议保留 SNAPSHOT kind，必要时启用。
```

这和 TCP `vemb-inline-vector` 的 snapshot 语义不同。TCP inline vector 是为了避免 client 用旧 handle 再读 payload；VSIM 远端 RPC 的 snapshot 是 owner 参与读取时的可选兜底。

## UB Channel RPC 设计

每个 SuperNode 之间维护 request/response channel：

```text
SN A -> SN B lookup request channel
SN B -> SN A lookup response channel
```

请求字段：

```text
request_id
src_owner_id
dst_owner_id
op = LOOKUP_HANDLE
key_hash
key_len
key
flags
timeout_ns
```

响应字段：

```text
request_id
status = OK / NOT_FOUND / BUSY / ERROR
kind = HANDLE / SNAPSHOT
key_hash
region_id
local_slot
offset
bytes
owner_generation
snapshot_bytes
snapshot_payload optional
```

RPC owner 侧只查本地 private TLC index，不查其他节点的 remote_meta，避免 fallback 递归。

超时策略：

```text
1. request channel full: 记录 rpc_ring_full，短暂 retry，超过预算返回 BUSY/ERROR。
2. response timeout: 记录 rpc_timeout，当前 VSIM 返回 miss/error。
3. NOT_FOUND: 直接返回 key2 不存在，不再猜测 remote_meta。
```

## 与当前实现的关系

当前已具备的基础：

```text
1. remote_meta header/bucket/entry 固定 64B。
2. remote_meta publish 使用 version odd/even。
3. remote_meta lookup 返回 candidate handle。
4. VSIM key2 lookup 已有 local -> remote_meta 的路径。
5. stale handle 已通过 slot_meta 校验计数 remote_meta_stale。
6. manifest 支持 owner remote_meta views。
```

需要调整或新增：

```text
1. 将 remote_meta backend 从 bucket + entry_index 迁移为 set + ways。
2. 保留 64B entry 和 version odd/even 发布语义。
3. VADD remote_meta publish 从同步调用改为异步 publish queue。
4. remote_meta 失败不影响 VADD completion。
5. VSIM remote_meta miss/stale 后接 UB channel RPC fallback。
6. RPC response 支持 HANDLE，协议预留 SNAPSHOT。
7. RPC 成功后异步 repair remote_meta。
8. 增加 remote_meta publish / evict / rpc fallback / repair 统计和限频日志。
```

迁移时可以先保留 manifest 字段兼容含义：

```text
remote_meta_entries:
  解释为 set_count * ways。

remote_meta_buckets:
  过渡期可继续解析，但新实现应显式增加 remote_meta_sets / remote_meta_ways。
```

长期 manifest 推荐字段：

```text
remote_meta_sets
remote_meta_ways
remote_meta_backend
remote_meta_path
remote_meta_mmap_offset
```

## 统计与日志

建议保留当前 lookup timing，并补齐分类计数：

```text
remote_meta_lookup_count
remote_meta_lookup_hit
remote_meta_lookup_miss
remote_meta_lookup_busy
remote_meta_lookup_way_probe
remote_meta_lookup_set_conflict
remote_meta_stale
remote_meta_lookup_ns

remote_meta_publish_async_enqueue
remote_meta_publish_async_drop
remote_meta_publish_ok
remote_meta_publish_busy
remote_meta_publish_insert
remote_meta_publish_update
remote_meta_publish_evict
remote_meta_publish_ns

ub_lookup_rpc_count
ub_lookup_rpc_ok
ub_lookup_rpc_not_found
ub_lookup_rpc_busy
ub_lookup_rpc_timeout
ub_lookup_rpc_error
ub_lookup_rpc_handle
ub_lookup_rpc_snapshot
ub_lookup_rpc_ns

remote_meta_repair_enqueue
remote_meta_repair_ok
remote_meta_repair_drop
```

关键异常需要日志，但必须限频：

```text
remote_meta stale:
  key_hash, owner_id, region_id, local_slot, expected_generation, actual_generation, write_seq, state

remote_meta publish drop/busy/evict:
  owner_id, key_hash, queue_depth, set_count, ways, set_id, victim_way

ub rpc timeout:
  src_owner_id, dst_owner_id, request_id, key_hash, timeout_ns
```

## 实施顺序

建议按以下顺序落地：

```text
1. 抽取 set-associative 小 helper：
     key_hash -> set bounds
     CLOCK/epoch victim policy
2. 将 remote_meta backend 迁移为 set + ways + 64B entry。
3. 增加 remote_meta async publish queue。
4. 将 VADD remote_meta publish 改为 best-effort async。
5. 增加 UB lookup RPC 协议与本地 owner handler。
6. VSIM key2 lookup 接入 remote_meta -> RPC fallback。
7. 增加 repair remote_meta 异步事件。
8. 补齐统计和限频日志。
9. 用 bench 增加 vsim-key-key remote key2 场景验证：
     remote_meta hit
     remote_meta miss -> rpc
     remote_meta stale -> rpc
     remote_meta publish queue drop/coalesce
     remote_meta set conflict eviction
```

## 当前 TODO

日期：2026-06-16

当前第一阶段核心链路已经落地：

```text
1. remote_meta 已迁移为 set + ways + 64B entry。
2. remote_meta entry 读写使用 atomic field + odd/even version snapshot。
3. VADD remote_meta publish 已改为 best-effort async queue。
4. async publish queue 已去除 mutex/cond，改为 Aeron-style bounded sequence ring。
5. VSIM key2 lookup 已接入 remote_meta -> UB RPC fallback。
6. UB RPC 已使用 UB/shared ring poll 方式传输 request/response。
7. RPC 成功返回 HANDLE 后已支持异步 repair remote_meta。
8. remote_meta publish / evict / rpc fallback / repair 统计已补齐。
```

仍需继续收口的 TODO：

```text
P0. 明确 Aeron 口径：
    当前实现是 Aeron-style UB shared ring poll，不是官方 Aeron media driver。
    如果后续要求严格接入官方 Aeron，需要单独替换 transport/ring 管理层。

P0. 补 ring 初始化状态机：
    当前已避免 attach/restart 时误 reset live ring。
    后续建议增加 INITING/READY 或 magic-last 初始化协议，覆盖两个进程同时创建/初始化同一 ring 的极端场景。

P1. 恢复或替代 remote_meta publish coalesce：
    旧 mutex queue 可扫描 pending key 做合并。
    当前 lock-free ring 满时直接 drop，正确性依赖 UB RPC fallback，remote_meta_publish_async_coalesce 暂不会增长。
    后续可增加 lock-free side-cache 或 per-key latest slot 合并。

P1. 增强 UB RPC 等待模型：
    当前 request 发布到 UB ring 后，调用线程在 pending slot 上等待 response。
    listener 线程负责 poll inbound request 和 response ring。
    后续需要压测 CPU 占用、tail latency，并决定是否加入 backoff 参数或事件通知。

P1. 实现可选 SNAPSHOT response：
    当前协议预留 SNAPSHOT kind，默认只返回 HANDLE。
    若远端 payload 不可直接读、需要 owner 侧保护读，或调试 stale 问题，再启用 payload snapshot 返回。

P1. 补充故障与压力测试：
    - RPC ring full
    - response timeout
    - publisher queue full/drop
    - remote_meta stale 连续失败
    - supernode 同时 restart/attach ring
    - remote_meta hit/miss/stale/conflict 在真实 UB 设备上的长稳压测

P2. manifest 长期字段收口：
    过渡期 remote_meta_entries 仍可解释为 set_count * ways。
    后续应显式使用 remote_meta_sets / remote_meta_ways / remote_meta_backend。

P2. 代码提交边界收口：
    将 remote_meta、UB RPC、lock-free publish queue、测试用例整理为独立提交。
    避免和无关文档、实验代码或远端测试生成文件混在一起。
```

测试规则：

```text
1. 远端测试只同步代码相关必要文件，不同步文档，不同步二进制。
2. 远端测试生成的二进制和 Makefile.dep 测试后需要清理。
3. 如果测试遇到硬件、驱动、UB 设备、链路、机器状态等无法自行确认或解决的问题，必须明确报告，不允许硬猜结论。
```

## 不做的事

第一阶段明确不做：

```text
1. 不让 remote_meta 服务普通 VEMB。
2. 不让 VADD 等 remote_meta 发布完成。
3. 不引入 remote_meta tombstone/free-list 复用。
4. 不做全局 remote_meta 单表。
5. 不用 remote_meta 替代 owner private TLC index。
6. 不让 RPC fallback 再递归查询第三方 remote_meta。
7. 不直接复用 WARM payload slot 状态机实现 remote_meta。
```
