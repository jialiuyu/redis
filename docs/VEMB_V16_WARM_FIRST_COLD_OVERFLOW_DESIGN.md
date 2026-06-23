# VEMB V16 WARM-first 与 COLD overflow 设计

日期：2026-06-16

## 结论

当前 `tlc_core_put_location()` 是 write-through 语义：

```text
VADD
  -> append COLD
  -> try WARM
  -> WARM 失败时返回 invalid warm handle，但 COLD 已有全量数据
```

下一阶段目标改为 WARM-first：

```text
VADD
  -> try WARM
  -> WARM 可放入：返回 WARM handle，不写 COLD
  -> WARM 全部无法放入：写 COLD overflow
```

这个模型下，COLD 不再是全量数据。第一阶段暂不解决完整容错恢复问题，后续再决定从 UB WARM region 恢复，还是从 COLD 层恢复，或引入 WARM replica/Paxos。

## 新写路径

推荐写路径：

```text
new key
  -> resolve WARM region order
  -> for each candidate region:
       set = mix(key_hash) % set_count
       scan fixed ways
       1. same-key overwrite
       2. FREE slot claim
       3. victim spill-to-COLD + overwrite
  -> if all WARM regions failed:
       append new key to COLD
       return invalid WARM handle / cold-only status
```

关键点：

```text
1. 普通 VADD 成功写入 WARM 时，不同步写 COLD。
2. WARM 满时优先淘汰 victim，不能直接让 WARM 停止复用。
3. victim 如果只存在于 WARM，需要先 spill 到 COLD，再覆盖 slot。
4. 如果 victim spill 失败，不能覆盖该 slot。
5. 如果所有 region 都无法 claim/free/evict，才把 new key 写入 COLD。
```

## slot 状态机

当前已有状态：

```text
FREE
FILLING
READY
EVICTING
```

WARM-first 下建议补充显式删除语义：

```text
DELETING
```

推荐状态迁移：

```text
FREE
  -> FILLING
  -> READY

READY(old key)
  -> EVICTING
  -> FILLING(new key)
  -> READY(new key)

READY(key)
  -> DELETING
  -> FREE
```

语义区别：

```text
EVICTING:
  cache replacement / victim 覆盖过渡态。
  旧 key 逻辑上仍存在，必要时先写 COLD，再复用 slot。

DELETING:
  逻辑删除 key 或显式释放 slot。
  必须配合 owner private index / COLD tombstone，否则旧数据可能被重新读回。

FREE:
  slot 无 owner，后续映射到该 set 的 key 可以 claim。
```

所有 owner 切换都必须递增 `owner_generation`：

```text
old handle: region_id=R, local_slot=S, owner_generation=10
new owner:  region_id=R, local_slot=S, owner_generation=11

reader 用 old handle 校验时 generation mismatch -> stale reject
```

## cold_state 语义

当前已有：

```text
COLD_NONE
COLD_PENDING
COLD_COMMITTED
```

WARM-first 后必须严格使用：

```text
COLD_NONE:
  当前 slot payload 只在 WARM 中有最新版本。
  不能无条件淘汰覆盖，除非先 spill 到 COLD。

COLD_PENDING:
  正在把 WARM victim 写入 COLD。
  reader 可以按 READY/write_seq 继续读稳定旧值，但 writer 不能覆盖。

COLD_COMMITTED:
  当前 slot payload 已有 COLD 副本。
  可以作为低成本 victim 覆盖。
```

写入来源对应状态：

```text
普通 VADD -> WARM:
  COLD_NONE

COLD read-through -> WARM:
  COLD_COMMITTED

same-key overwrite:
  如果不同时更新 COLD，必须变回 COLD_NONE。

victim spill-to-COLD 成功:
  victim 旧版本可视为 COLD_COMMITTED，然后 slot 可被新 key 覆盖。
```

## victim spill-to-COLD

当目标 set 满、没有 FREE slot 时：

```text
1. choose victim way
2. CAS READY -> EVICTING
3. 读取 stable slot_meta:
     key_hash
     key_fingerprint
     owner_generation
     bytes
     write_seq even
4. 读取 payload snapshot
5. 获取 victim 完整 key
6. append victim key + payload 到 COLD
7. owner_generation++
8. 写新 key metadata + payload
9. publish READY
```

如果任一步失败：

```text
1. 不覆盖旧 slot。
2. 尽量恢复 READY。
3. 记录 warm_eviction_fail。
4. 尝试下一个 region 或最终让 new key 进入 COLD overflow。
```

## victim key 来源

victim spill-to-COLD 需要完整 key。仅有 `key_hash/key_fingerprint` 不够，因为 COLD 精确 lookup、delete/tombstone 和冲突处理都需要：

```text
key_len
key bytes
key_hash
payload
```

有两个方案。

### 方案 A：per-slot side metadata

每个 WARM slot 旁边保存完整 key：

```c
typedef struct warm_slot_key_meta {
    _Atomic uint64_t version;
    uint64_t key_hash;
    uint64_t key_fingerprint;
    uint64_t owner_generation;
    uint32_t key_len;
    uint32_t flags;
    char key[128];
} warm_slot_key_meta_t;
```

大小估算：

```text
裸大小: 168B
按 64B 对齐: 192B / slot

现有 slot_meta: 64B / slot
合计 metadata: 256B / slot，不含 payload
```

优点：

```text
1. 实现简单。
2. victim spill 时直接读取 key。
3. 不依赖 owner private index 反查能力。
```

缺点：

```text
1. 每个 slot 多 192B，slot 数很大时成本高。
2. key 同时存在 owner index 和 side metadata，存在一致性维护成本。
```

### 方案 B：slot -> owner index 反向索引

不在 per-slot metadata 中保存 key，只保存反向引用：

```c
typedef struct warm_slot_reverse_ref {
    uint32_t index_id;
    uint32_t flags;
    uint64_t owner_generation;
} warm_slot_reverse_ref_t;
```

大小估算：

```text
约 16B / slot
```

淘汰时：

```text
slot -> reverse_ref -> owner index entry -> key/key_len/key_hash
```

优点：

```text
1. 内存开销远小于 192B/slot。
2. key 仍然归 owner private index 管理，长期架构更清晰。
```

缺点：

```text
1. 需要 owner index 支持 slot 反查 key。
2. 需要处理 index entry 生命周期。
3. 需要 generation 校验，避免 slot 复用后反查到旧 key。
4. delete / overwrite / eviction 都要同步维护反向引用。
```

建议：

```text
第一版:
  如果 slot 数量不大，先用 per-slot side metadata，尽快跑通 WARM-first + victim spill。

长期:
  改为 slot -> owner index entry 的反向索引，降低 metadata 开销。

超大 WARM:
  优先做反向索引，不建议承受 192B/slot 的 key copy。
```

## 不保存 key 的后果

如果 per-slot metadata 和反向索引都不保存完整 key，只保留 hash/fingerprint：

```text
1. victim 无法正确 append 到 COLD。
2. COLD 不能做严格 key lookup。
3. delete/tombstone 只能按 hash 操作，存在碰撞误删风险。
4. remote_meta / RPC fallback 只能做概率性校验，不满足精确语义。
```

因此 WARM-first + victim spill 需要满足以下二选一：

```text
1. slot side metadata 保存完整 key。
2. owner private index 支持从 slot/generation 反查完整 key。
```

## COLD 非全量后的读取语义

COLD 非全量后，读取路径需要区分：

```text
WARM hit:
  返回 WARM handle。

WARM miss + COLD hit:
  可以 read-through promote 到 WARM。

WARM miss + COLD miss:
  不能再等价于 key 不存在，除非 owner private index 也确认不存在。
```

因此后续需要 owner private index 成为权威 key directory：

```text
owner private index:
  key -> current location:
    WARM handle
    COLD offset
    deleted/tombstone
```

第一阶段可以暂时只保证 VADD 返回路径和 VSIM/RPC 的 WARM handle 路径，不把 COLD 非全量作为故障恢复依据。

## RPC 影响

当前 UB RPC 默认返回 HANDLE。WARM-first 后，如果 key 只在 COLD：

```text
LOOKUP_HANDLE
  -> WARM hit: return HANDLE
  -> WARM miss + COLD hit: return SNAPSHOT 或 promote 后 return HANDLE
  -> owner index says deleted/not found: return NOT_FOUND
```

协议里已预留：

```text
VEMB_V16_UB_LOOKUP_RPC_KIND_SNAPSHOT
```

建议后续启用 SNAPSHOT，作为 COLD-only key 的兜底返回方式。

## 需要修改的代码位置

核心修改点：

```text
src/tlc_core.c
  tlc_core_put_location:
    从 cold_append -> warm_put
    改为 warm_put -> cold_append overflow

  warm_try_fill_free:
    普通 VADD 写入时 cold_state = COLD_NONE

  warm_try_overwrite_same:
    若不更新 COLD，同 key覆盖后 cold_state = COLD_NONE

  warm_try_evict_and_fill:
    支持 WARM_ONLY victim spill-to-COLD 后覆盖

  cold_append:
    继续作为 overflow/victim spill 的 append API

src/vemb_v16_shared_allocator.h
  增加 DELETING 状态，或明确复用 EVICTING 但文档中区分语义

src/vemb_v16_protocol.h
  确认 SNAPSHOT payload 长度和响应大小限制

src/vemb_v16_ub_rpc.c
  后续支持 COLD-only key 返回 SNAPSHOT
```

## 测试计划

必须新增测试：

```text
1. WARM 未满:
     VADD 只写 WARM，不增加 write_throughs。

2. same-key overwrite:
     WARM slot 复用，owner_generation 不变。
     如果不更新 COLD，cold_state 变 COLD_NONE。

3. WARM set 满:
     victim spill-to-COLD 成功后，新 key 覆盖 victim slot。
     old handle 被 owner_generation 拒绝。
     victim 可从 COLD 找回。

4. COLD_COMMITTED victim:
     可以直接覆盖，或跳过重复 spill。

5. victim spill 失败:
     不覆盖旧 slot。
     new key 尝试其他 region，最终 cold overflow。

6. delete:
     READY -> DELETING -> FREE。
     old handle stale reject。
     后续同 set key 可 claim FREE slot。

7. COLD-only RPC:
     LOOKUP_HANDLE 对 COLD-only key 返回 SNAPSHOT 或 promote 后 HANDLE。
```

## 暂不解决的问题

第一阶段暂不解决：

```text
1. COLD 作为全量恢复源。
2. WARM region crash 后如何完整重建 owner index。
3. WARM replica / Paxos 容错。
4. 跨 SuperNode 的 delete/tombstone 广播。
5. 超大规模 slot 下 side metadata 的最终内存优化。
```

