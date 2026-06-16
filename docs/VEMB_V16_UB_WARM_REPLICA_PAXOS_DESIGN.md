# VEMB V16 UB WARM Replica / Paxos 设计

日期：2026-06-12

## 核心结论

如果 UB region 在 server 重启后仍然保留数据，那么 WARM/UB 可以不只作为 cache，也可以演进为 primary storage。但这需要两个前提：

```text
1. UB WARM region 必须是 self-describing layout。
   server 重启后可以通过 slot_meta 重建 private index / remote_meta。

2. 如果允许淘汰或覆盖旧 slot，旧数据必须已经有可靠副本。
   副本可以是 COLD，也可以是 UB replica group 的多数派提交。
```

如果故障模型只覆盖 server 进程重启，而 UB region 不丢：

```text
self-describing slot_meta + restart rebuild
```

基本足够。

如果要覆盖：

```text
单台 SuperNode 挂掉
UB device 不可读
远端节点掉线
写入不能丢
多副本一致
```

则需要 Paxos/Raft 这类共识复制。

## 和 Hash Cache 文档的边界

本文只描述 UB WARM 的副本和一致性提交方向。slot layout、`slot_meta.state`、`owner_generation`、`write_seq`、set-associative placement、remote_meta locator 和淘汰基本流程见：

```text
docs/VEMB_V16_UB_WARM_HASH_CACHE_EVICTION_DESIGN.md
```

本文在其基础上，把 `cold_state` 抽象为更通用的 `durability_state`。

## durability_state

如果 WARM 仍作为 COLD-backed cache，`cold_state` 可以只表达是否已有 COLD 副本。

如果 UB WARM 要升级为 primary storage，建议使用更通用的 `durability_state`：

```text
DURABILITY_LOCAL_ONLY:
  只有当前 UB slot 一份。
  不允许被淘汰，除非业务允许丢。

DURABILITY_REPLICA_PENDING:
  正在复制到 backup UB region，或等待 Paxos/Raft commit。
  不允许被淘汰。

DURABILITY_REPLICATED_COMMITTED:
  已经被 replica group 多数派提交，或已有可靠 UB 副本。
  可以作为 eviction victim。

DURABILITY_COLD_COMMITTED:
  已经落 COLD。
  可以作为 eviction victim。
```

淘汰必须检查：

```text
slot_meta.state == READY
slot_meta.write_seq is stable even
can_evict(slot_meta.durability_state) == true
```

建议：

```text
can_evict(DURABILITY_REPLICATED_COMMITTED) = true
can_evict(DURABILITY_COLD_COMMITTED) = true
can_evict(DURABILITY_LOCAL_ONLY) = false
can_evict(DURABILITY_REPLICA_PENDING) = false
```

## Paxos / Raft 负责什么

Paxos/Raft 不直接替代 UB slot。它负责：

```text
哪些写入被提交
写入顺序是什么
哪些副本应该包含这条记录
故障后如何恢复一致状态
```

UB WARM slot 仍然负责保存 payload：

```text
Paxos/Raft commit log -> logical record order
UB WARM slot          -> payload bytes
slot_meta             -> physical slot state and durability state
remote_meta           -> candidate locator cache
```

## 写入提交流程

严格提交路径：

```text
1. proposer 选择 target slot。
2. 写 payload 到本地 primary UB slot，state = FILLING。
3. slot_meta.durability_state = DURABILITY_REPLICA_PENDING。
4. 向 replica group 发 Paxos/Raft proposal。
5. 多数派 accept/commit。
6. committed 后，各副本 apply 到自己的 UB slot。
7. primary slot_meta.durability_state = DURABILITY_REPLICATED_COMMITTED。
8. publish remote_meta candidate locator。
9. 返回 VADD 成功。
```

如果要降低延迟，可以做 optimistic path：

```text
1. 先写 primary UB WARM。
2. 返回 pending / optimistic success。
3. 后台 Paxos/Raft commit。
```

但 optimistic path 的语义更复杂。如果 primary 在 commit 前故障，未提交写入可能丢失或需要回滚。第一版建议使用严格提交路径。

## slot_meta 需要补充的字段

在 hash cache 文档的 `slot_meta` 基础上，replica/Paxos 模式建议补充：

```c
typedef struct vemb_v16_warm_replica_meta {
    _Atomic uint64_t log_index;
    _Atomic uint64_t commit_index;
    uint64_t term_or_ballot;
    uint32_t replica_group_id;
    _Atomic uint32_t durability_state;
    uint32_t reserved;
} vemb_v16_warm_replica_meta_t;
```

这些字段可以直接并入未来版 `slot_meta`，也可以放在独立 replica meta array 中。第一版为了控制 64B `slot_meta` cacheline，建议先放独立 array，避免破坏 hot read path。

## 重启恢复

server 重启后：

```text
1. mmap UB region。
2. 扫描 slot_meta，找到 READY 且 write_seq even 的 slots。
3. 读取本地 Paxos/Raft log，或从 peers 补齐 committed log。
4. 对比 slot_meta.log_index / owner_generation。
5. 对缺失或半写 slot 重新 apply committed log。
6. 重建 private index。
7. 重建 remote_meta。
```

如果发现：

```text
slot_meta.write_seq odd
state == FILLING / EVICTING
```

恢复时以 committed log 为准：

```text
committed -> replay/apply
not committed -> discard/free
```

## 淘汰与副本

在 UB primary + replica 模式下，WARM slot 淘汰不是简单删除数据，而是物理位置重用。

淘汰条件：

```text
state == READY
write_seq is stable even
durability_state == DURABILITY_REPLICATED_COMMITTED
```

淘汰后：

```text
1. old remote_meta 可以 stale。
2. old handle 会被 owner_generation/key_hash 校验挡住。
3. 如果需要重读 old key，从 committed log / replica group / rebuilt index 找到当前有效位置。
```

是否把 eviction 本身写入 Paxos/Raft log，取决于语义：

```text
local cache eviction:
  不必进入 log。
  只影响本地物理 slot，逻辑记录仍由 committed log / replica group 保证。

logical delete / compaction:
  必须进入 log。
  所有副本按同一顺序 apply。
```

## remote_meta 语义

remote_meta 在副本模式下仍然只是 candidate locator：

```text
key -> {region_id, local_slot, offset, owner_generation}
```

它不是一致性权威，不承诺一定最新。读路径必须继续校验：

```text
slot_meta.state == READY
slot_meta.owner_generation == handle.owner_generation
slot_meta.key_hash == lookup_key_hash
slot_meta.write_seq stable
```

校验失败：

```text
remote_meta stale -> retry / ask owner / rebuild locator
```

## 分阶段落地

```text
Phase 1:
  保持 COLD-backed cache。
  slot_meta self-describing，支持 restart rebuild。

Phase 2:
  引入 durability_state，但只映射 COLD_COMMITTED / PENDING。

Phase 3:
  加 UB replica async copy。
  durability_state 支持 REPLICA_PENDING / REPLICATED_COMMITTED。

Phase 4:
  引入 Paxos/Raft commit log。
  VADD 成功以 majority commit 为准。

Phase 5:
  恢复流程以 committed log 为准，支持 replay/apply。

Phase 6:
  区分 local physical eviction 和 logical delete / compaction。
```

## 需要补充的统计

```text
replica_propose
replica_commit
replica_apply
replica_commit_latency_ns
replica_replay
replica_repair
durability_local_only
durability_replica_pending
durability_replicated_committed
evict_skip_replica_pending
remote_meta_rebuild_after_replay
```
