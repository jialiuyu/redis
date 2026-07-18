# VEMB V16 TLC Local-Write Global-Read + Live Delta Atomic Migration + Cache 设计

日期：2026-07-17

## 0. 实现同步与断点

本文已同步到 2026-07-17 当前代码状态。本次提交仅标记并收口 `P0/P1` 范围；`P2` 及后续迁移主流程、routing shortlist、残留命名清理不在本提交完成范围内。

### 0.1 已完成

- normal write 已切到 local-only 语义
  - `warm_put()` 在存在 local region 时只写 local region
  - remote region 不再作为 normal write fallback
- `VSIM` 主读路径已删除 `UB lookup RPC fallback`
  - `vemb_v16_tlc_lookup_vsim_key2()` 现在是
    - `cached handle hint`
    - `full local/global warm lookup`
  - 不再走 `remote_meta stale -> RPC fallback -> repair`
- 普通 `VADD` 成功后的 remote meta publish enqueue 已删除
  - 迁移 delta publish 仍保留
- read cache 已明确降级成 hint-only
  - `tlc_core_get_warm_location()` 主路径不再先走 `location_cache_get + validate`
  - `tlc_core_get_cached_warm_location()` 现在只做 `location_cache_peek()`
  - correctness 仍由后续 `slot meta` 校验承担
- `slot meta` 已完成一轮轻量化
  - 删除了重复存储的 `region_id`
  - 删除了重复存储的 `local_slot`
  - 保留 `state / bytes / owner_generation / write_seq / key_hash / key_fingerprint`
- `benchmark/vemb_v16_tlc_ut` 已同步到新语义并通过
- `P0/P1` 断点已完成
  - `P0`: normal write local-only、删除 `UB lookup RPC fallback`、读主路径回到 owner-local/global warm deterministic probe
  - `P1`: read cache 降级为 hint-only，主读路径不再依赖 cache correctness

### 0.2 当前仍未完成

- 扩容/迁移主流程还没有按 `Local Write + Global Read + Live Delta Atomic Migration + cache` 完整收敛
- `remote_meta` / `lookup_rpc` / 相关统计和 wiring 仍有残留，但已不属于 P0/P1 correctness 主路径完成条件
- cache 命名还没完全统一成 `hint` 语义命名
- `location_cache_entry` 字段还没有继续压缩
- migration 期间的 `source + target pair probe` 还没正式收口

### 0.3 建议下一个断点起手

后续继续时，优先顺序建议是：

1. 清理 `remote_meta` / `lookup_rpc` 残留 wiring、统计和 bench 输出
2. 继续做扩容/迁移主流程的 `source + target pair probe` 和 routing/shortlist 收口
3. 最后再做 cache 命名和 entry 字段进一步压缩

本文基于前面的 `Local-Write Global-Read` 简化讨论，进一步收敛出一版更贴近当前约束的目标模型：

```text
正常态：
  只有 owner 能写自己的本地 warm region
  所有 node 都可以只读 owner 的 warm region

迁移态：
  仍保留 baseline + live delta + barrier + atomic cutover
  target 只允许通过迁移复制通道写自己的本地 warm region

读路径：
  保留一个轻量 cache / hint 层
  miss/stale 后回退到 owner-local deterministic probe
```

本文重点回答四个问题：

1. 这个目标模型和当前 `hpc-redis` 扩容迁移流程相比，哪些地方保留，哪些地方删除。
2. 删除 `UB lookup RPC fallback` 后，如何确定 `key -> owner -> region`。
3. `slot meta` 和 `warm cache` 能否继续做轻量化收缩。
4. `tlc_core_get_cached_warm_location()` 这类读路径 cache 在新模型里应当保留到什么程度。

## 1. 结论先行

推荐目标模型不是：

```text
Local-Write Global-Read + 短暂停写 final copy
```

而是：

```text
Local-Write Global-Read + Live Delta Atomic Migration + Cache
```

原因很直接：

- 如果要求在线写连续性，`短暂停写 final copy` 的 cutover 风险更高。
- 如果继续保留 live delta，迁移复杂度仍然存在，但可以把复杂度收缩到“迁移专用复制通道”里。
- 相比当前 `global-write/read`，仍然可以删除一整层“平时全局写、全局定位、全局修复”相关复杂度。

因此目标不是“把 TLC 缩成一个极薄 facade”，而是：

```text
删除平时 global write 需要的复杂度
保留 online atomic migration 必需的复杂度
把 read path cache 收缩成可选优化层
```

## 2. 核心规则

### 2.1 正常态规则

```text
Rule 1:
  任一时刻只有一个权威写 owner。

Rule 2:
  正常业务写只能落到 owner 的本地 warm region。

Rule 3:
  非 owner node 可以只读 owner 的 warm region，但不能平时写 remote region。

Rule 4:
  读路径返回的 handle/location 只是候选位置；
  最终 payload 访问仍必须经过 slot meta 校验。
```

### 2.2 迁移态规则

```text
Rule 5:
  target 在迁移阶段可以写自己的本地 warm region，
  但仅限 baseline/delta apply 这条迁移专用通道。

Rule 6:
  cutover 前，对外权威写 owner 仍然是 source。

Rule 7:
  cutover 后，权威写 owner 一次性切到 target。
```

### 2.3 cache 规则

```text
Rule 8:
  read cache/hint cache 不是 correctness 依赖层。

Rule 9:
  cache 命中时只返回候选 hint，
  不在 cache 层自己做 validate。

Rule 9.1:
  后续真正访问 payload 时，
  仍要靠 slot meta 校验确认 location 仍然有效。

Rule 10:
  cache miss/stale 不再走 UB lookup RPC fallback，
  而是回退到 owner-local deterministic probe。
```

## 3. 与当前流程的主要区别

## 3.1 当前流程的核心特征

当前设计同时包含三种复杂度：

- 平时允许较复杂的全局定位和 remote 路径
- 写后 remote meta publish / repair
- 迁移期间 baseline + live delta + barrier + cutover

这使 TLC 同时承担：

- local storage facade
- remote meta distribution
- lookup RPC fallback
- migration control plane

## 3.2 新模型保留的部分

以下能力继续保留：

- baseline copy
- live delta replay
- barrier / final fence
- atomic cutover
- source GC
- `ASK/MOVED` 或等价迁移期语义
- `owner_epoch` / `topology_epoch` / stale reject
- `write_seq` / `owner_generation`

也就是说，迁移协议没有被删掉。

## 3.3 新模型删除或弱化的部分

以下复杂度应当删除或明显弱化：

- `warm_put()` 的 remote warm allocation fallback
- 普通写路径成功后的 `remote meta publish / repair`
- `UB lookup RPC fallback`
- “key 在正常态可能落到任意 remote region”的设计前提
- 把 `remote_meta` 当作 correctness 必需层

更准确地说：

```text
当前：
  global-write/read + remote_meta/RPC + live migration

目标：
  local-write/global-read + owner-local probe + live migration
```

## 4. 新的 owner / region 确定方式

删除 `UB lookup RPC fallback` 后，定位不再依赖“远端 owner 帮你查一次 key->handle”，而是分成两步。

### 4.1 先确定 owner

owner 应由静态或低频刷新的 routing 决定：

```text
key
-> shard/range/hash
-> owner
```

实现上可以是：

- shard/range 到 owner 的直接映射
- 或低频刷新的 routing snapshot

这层应当完全脱离 RPC fallback。

### 4.2 再确定 region

确定 owner 后，读路径只在该 owner 的本地 region 集合中 probe：

```text
key
-> owner
-> owner_local_regions(owner)
-> deterministic probe
-> slot meta validate
-> location/handle
```

这里的关键前提是：

- 正常写只写 owner 本地 region
- 不再有 remote fallback
- 所以 key 的候选落点被收缩成 owner 的本地 region 集合

### 4.3 迁移期间的额外 probe

迁移期间可允许额外的小范围双边 probe：

```text
stable:
  probe owner local regions

migration before cutover:
  先 probe source owner local regions
  必要时再 probe target owner local regions

after cutover:
  probe target owner local regions
```

注意这不是重新引入通用 RPC fallback，而是把迁移态额外复杂度收敛为：

```text
source + target pair probe
```

## 5. 扩容迁移状态机

推荐状态机如下：

### 5.1 `prepare`

- 发布迁移计划
- 固定 `migration_epoch`
- 预留 `cutover_epoch`
- 刷新 routing snapshot
- target attach/mmap 自己会接管的本地 region
- 所有节点刷新 `owner -> local region shortlist`

### 5.2 `baseline_copy`

- source 扫描迁移范围
- 复制 stable snapshot 到 target 本地 warm region
- target apply 时做 stale/duplicate 拒绝

### 5.3 `live_delta_replay`

- source 继续接受在线写
- source 本地写成功后写 migration outbox
- delta 异步送到 target
- target 幂等 apply 到自己的本地 region

### 5.4 `barrier_catchup`

- source 发 barrier/checkpoint
- target 确认 baseline 之后的增量已经追到某个确定序号
- 迁移进度进入可 cutover 状态

### 5.5 `atomic_cutover`

- source 暂停迁移范围的新写
- flush 最后少量 delta
- target 确认 apply 完成
- bump `owner_epoch`
- 发布新 routing
- 新写开始直接进入 target 本地 region

### 5.6 `source_gc`

- source 标记 `source_cutover/source_gc`
- 旧 handle 继续通过 generation/stale 校验拒绝
- 安全窗口后清理 source 侧旧数据

## 6. slot meta 轻量化边界

## 6.1 可以轻量化，但不能清空

在新模型下，slot meta 不再需要承担那么多“全局不稳定落点”的语义，但仍然必须承担并发安全和最小身份校验。

推荐保留的最小语义：

- `write_seq`
- `owner_generation`
- `state`
- `key fingerprint`

如果需要，也可保留 `key_hash`，但可以评估是否降成更轻的 fingerprint 组合。

## 6.2 为什么这些字段还不能删

`write_seq` 不能删：

- global read 仍然存在
- 否则可能读到 torn payload

`owner_generation` 不能删：

- slot 会复用
- 旧 handle 可能误命中新 payload

`state` 不能删：

- 至少要区分 `empty/live/tombstone/source_gc`

`key fingerprint` 不能全删：

- deterministic probe 命中后仍需要最小身份校验

## 6.3 哪些迁移语义应移出 slot meta 主路径

迁移多状态不应继续膨胀到 slot meta 主路径里。

更推荐：

- routing / owner state 负责大方向迁移语义
- key meta 负责 source cutover / tombstone / migration state
- slot meta 只保留并发与最小 location 身份校验

## 7. warm cache 的收缩方向

## 7.1 可以明显更“清零”

相对 slot meta，warm cache 更适合大幅删减。

应删除或清空的部分：

- remote warm allocation fallback
- remote meta publish cache
- remote repair queue
- UB lookup RPC fallback cache
- 为上述机制服务的复杂 publisher/ring/access snapshot 依赖

收缩后的 warm cache 更像：

```text
owner local warm store
+ target shadow warm store during migration
+ thin owner->region routing cache
```

## 7.2 可以保留的 cache 类型

可以保留两类 cache：

- 薄的 routing cache
  - `owner -> local region shortlist`
- 薄的 read hint cache
  - `key -> last-known location hint`

但它们都不应再成为 correctness 依赖层。

## 8. `tlc_core_get_cached_warm_location()` 的收敛方案

## 8.1 当前语义

当前 `tlc_core_get_cached_warm_location()` 返回的是 cache 里的候选 `tlc_warm_location_t`，只做 `peek`，并不在 cache 层做 slot 校验；真正最终访问 payload 时，仍要靠后续 location/slot validate。

因此它本质上已经更像：

```text
read-path hint
```

而不是：

```text
authoritative lookup
```

## 8.2 目标语义

建议把这层 cache 明确收敛成轻量 hint cache：

```text
cache hit
  -> 给出 last-known location hint
  -> 后续 load path 继续做 slot validate

cache miss/stale
  -> 回退到 owner-local deterministic probe
```

建议语义上把它视作：

```text
try_get_cached_hint()
```

而不是一个保证有效的 location lookup。

## 8.3 建议保留的最小字段

对固定长度向量场景，cache entry 可收缩到：

- `key_hash`
- `key_len` 或短 fingerprint
- `region_index`
- `local_slot`
- `owner_generation`

可优先评估删除的字段：

- `offset`
  - 可由 `local_slot * value_size` 推导
- `bytes`
  - 固定 `1200B` 场景几乎冗余
- `region_id`
  - 若 `region_index` 稳定可单独保留其一

## 8.4 `peek/get` 的建议

当前代码已经收敛成：

- `peek`
  - 只取 hint，不做深校验
- 主读路径
  - 不再先查 cache 再 validate
  - 直接走 `hot/warm/cold` 主链

后续命名上建议继续向 hint 语义靠拢。

历史上曾经存在的更重语义是：

- `get`
  - 命中后做轻量 validate，失败直接 miss

这一层现在已经从主读路径中移除。

如果后续继续整理 API，建议保留两层语义，但实现上更薄：

- `peek`
  - 只取 hint，不做深校验
- `get`
  - 命中后做轻量 validate，失败直接 miss

如果后续读路径进一步统一，也可以考虑把两者最终合并成：

```text
try_get_cached_hint()
try_resolve_warm_location()
```

## 8.5 VSIM 场景的特殊处理

如果 `VSIM` 是低频场景，则不需要为了 `VSIM` 维护一条昂贵 cache correctness 路径。

推荐：

- 高频 `load/read` 可继续使用 hint cache
- `VSIM` 可直接走 owner-local deterministic probe
- 迁移期最多补 `source + target` 双边 probe

这样可以避免为低频场景保留完整 RPC fallback 体系。

## 9. 模块去留建议

### 9.1 应保留

- `tlc_core` 的 warm slot 并发校验骨架
- `key_meta_shards`
- baseline copy / delta outbox / barrier / cutover / source_gc
- supernode 写路径中的 `ASK/MOVED`、stale topology、source cutover 检查
- proxy 侧 migration control 骨架

### 9.2 应删除或弱化

- `warm_put()` remote fallback
- 普通写后的 remote meta publish / repair
- `UB lookup RPC fallback`
- 读路径对 `remote_meta` 的 correctness 依赖
- “key 正常态可能落在任意 remote region”的假设

### 9.3 可保留为可选优化层

- `remote_meta`
  - 若继续存在，应退化成性能优化层，而非 correctness 依赖层
- read hint cache
  - 只作为 last-known location hint

## 10. 推荐落地顺序

### P0

- 已完成：
  - 禁止 normal write remote fallback
  - 删除 `UB lookup RPC fallback`
  - 读路径切换为 owner-local deterministic probe 主路径

### P1

- 已完成：
  - 收缩 `tlc_core_get_cached_warm_location()` 到轻量 hint cache
- 仍未完成：
  - 收缩 `remote_meta` 到可选层并清理残留 wiring / stats / bench 输出
  - 统一 cache/hint 命名
  - 缩减 cache entry 字段

### P2

- 保留 live delta atomic migration
- 清理迁移以外的 global write 控制面残留
- 统一 routing snapshot 和 owner-region shortlist 刷新机制

## 11. 一句话总结

目标模型不是“去掉迁移复杂度”，而是：

```text
把复杂度重新分层

平时：
  Local Write + Global Read + thin cache + owner-local probe

迁移时：
  baseline + live delta + barrier + atomic cutover
```

因此最重要的收敛不是删掉 live migration，而是删掉那些“为了平时 global write 和 RPC fallback 存在”的复杂度。

## 12. 代码模块映射与改造清单

这一节把目标模型直接映射到当前代码结构，方便后续按模块推进。

### 12.1 `src/tlc_core.c` / `src/tlc_core.h`

`tlc_core` 是这次收敛的第一落点。

建议保留：

- warm region runtime
- slot meta 并发校验
- key meta shard
- `location_cache` 的轻量 hint 角色

建议修改：

- `warm_put()` 去掉 normal remote fallback，只保留 local put 和迁移专用 apply 入口
- `tlc_core_get_warm_location()` miss 后改走 owner-local deterministic probe 主路径
- `tlc_core_get_cached_warm_location()` 明确降级成 hint 接口
- `location_cache_get/peek` 的命名和语义向 hint cache 靠拢

建议评估删除：

- `warm_alloc_remote`
- `warm_alloc_fallback`
- 任何围绕“正常态 remote region 写入”的统计和分支

### 12.2 `src/vemb_v16_tlc.c` / `src/vemb_v16_tlc.h`

`vemb_v16_tlc` 当前承担了太多职责，是第二个需要收缩的模块。

建议保留：

- handle facade
- migration control 相关 API
- owner/routing 辅助
- 轻量 cache/hint 读入口

建议修改：

- `vemb_v16_tlc_get_cached_handle()` 语义改成“取 cached hint handle”
- 主读路径优先改为 owner-local deterministic probe
- `remote_meta_view_for_key()` 这类逻辑从 correctness 主路径降级

建议删除或弱化：

- `lookup_rpc`
- `lookup_rpc_runtime`
- `vemb_v16_tlc_lookup_rpc_local_handler()`
- `enqueue_remote_meta_publish()`
- `remote_meta_publisher_*`
- `vemb_v16_tlc_set_lookup_rpc()`
- 大部分 `ub_lookup_rpc_*` 和 `remote_meta_publish_*` 统计

建议保留为可选优化层：

- `remote_meta_views`
  - 若短期不删，可仅用于性能 hint，不再作为 miss 后 correctness fallback

### 12.3 `src/vemb_v16_supernode.c`

这是业务读写热路径的第三个关键模块。

建议保留：

- `ASK/MOVED`
- stale topology 检查
- source cutover / write blocked 检查
- `migration_delta_put_after_local_write()` 调用点

建议修改：

- 普通读路径不要再把 remote meta / lookup RPC 作为 correctness 依赖
- `VSIM` 路径可直接按低频 probe 模式走 owner-local shortlist
- cached handle miss 后直接走 deterministic probe，而不是远端 fallback

建议删除：

- 写成功后普通 remote meta publish enqueue

### 12.4 `src/vemb_v16_proxy.c` / `src/vemb_v16_storage.*`

这层主要承接迁移状态机和 range/key control。

建议保留：

- `migration_mark_migrating`
- `migration_barrier`
- `migration_mark_cutover`
- `migration_mark_source_gc`
- range control 版本
- migration outbox 和 baseline retry

建议修改：

- 明确“target 只通过迁移通道写本地 region”
- 普通数据面不要再借用这层做 global write 兜底
- routing snapshot / owner-local shortlist 刷新机制放到更明确的 control plane

### 12.5 `src/vemb_v16_ub_rpc.c`

这是最明确的删减对象之一。

建议删除或退场：

- `UB lookup RPC` request/response 主链路
- runtime install / swap lookup rpc 的 wiring

建议保留的唯一前提：

- 如果未来仍有独立非迁移控制 RPC 需求，再单独定义，不与 key location fallback 混用

### 12.6 `src/vemb_v16_remote_meta.c`

这个模块建议从 correctness 关键模块降级成可选优化模块。

短期方案：

- 保留结构体和 attach/init 以减少一次性改动面
- 但逐步把 lookup/publish 从主路径移出

长期方案：

- 若 owner-local deterministic probe 的性能足够，则整体下线

## 13. 结构体字段与统计项删改建议

### 13.1 `tlc_core_location_cache_entry`

当前字段偏重，建议目标收缩为：

- `key_hash`
- `key_len` 或短 fingerprint
- `region_index`
- `local_slot`
- `owner_generation`

优先评估删除：

- `offset`
- `bytes`
- `region_id`
- 全量 `key_words`

如果担心误判，可保留短 fingerprint，而不是完整 key words。

### 13.2 `vemb_v16_tlc_t`

建议逐步删除或置废弃状态的字段：

- `lookup_rpc`
- `lookup_rpc_arg`
- `lookup_rpc_runtime`
- `current_lookup_rpc_runtime`
- `remote_meta_publisher`
- `ub_lookup_rpc_next_request_id`
- `remote_meta_publish_*` 统计
- `ub_lookup_rpc_*` 统计

短期可保留但降级的字段：

- `remote_meta_view`
- `remote_meta_views`
- `remote_meta_view_count`

### 13.3 `tlc_core_stats` / server stats

建议后续同步收缩以下统计：

- `warm_alloc_remote`
- `warm_alloc_fallback`
- `remote_meta_publish_*`
- `ub_lookup_rpc_*`

建议增加的新统计：

- owner-local deterministic probe count
- owner-local deterministic probe hit
- migration pair probe count
- cached hint hit
- cached hint stale

## 14. 函数级别优先改造顺序

下面按“当前代码状态”重排后续顺序，尽量每一步都能保持系统可编译、可验证。

### Step 1

先做 `P1` 剩余清理：

- 删除 `lookup_rpc` / `lookup_rpc_runtime` 相关 wiring
- 删除 `ub_rpc` lookup fallback 残留入口
- 清理 `remote_meta_publish_*` / `ub_lookup_rpc_*` 统计和 bench 输出

### Step 2

再做 cache/hint 收口：

- 统一 `get_cached_handle()` / `location_cache_*` 的 hint 语义命名
- 缩减 `location_cache_entry` 字段
- 保持 miss/stale 后继续回退到 owner-local deterministic probe

### Step 3

再改迁移态读路径：

- 正式收口 migration 期间的 `source + target pair probe`
- 统一 cached miss/stale 的 fallback 分支
- 明确 routing snapshot / owner shortlist 刷新边界

### Step 4

最后做迁移主流程整体收敛：

- prepare / baseline / delta / barrier / cutover / source_gc 与当前实现重新对齐
- 清理迁移以外的 global write 控制面残留
- 补齐针对 pair probe / hint stale / source_gc 的测试与 smoke

## 15. 测试与 benchmark 调整建议

## 15.1 单元测试

建议新增或重写以下单测：

- owner-local deterministic probe 能命中 owner 本地 region
- cache hint stale 后能安全 miss 并回退
- migration before cutover 时 source/target 双边 probe 语义正确
- cutover 后 source handle 被 generation/source_gc 正确拒绝
- normal write 永远不进入 remote region

建议保留并继续扩展：

- `benchmark/vemb_v16_tlc_ut.c`
- `benchmark/vemb_v16_manifest_ut.c`
- `benchmark/vemb_v16_migration_outbox_ut.c`
- `benchmark/vemb_v16_migration_control_ut.c`

## 15.2 集成/烟测

建议补 smoke：

- 扩容期间持续 live write，cutover 后新写直接进入 target
- cutover 前读以 source owner 为主，必要时允许 source/target pair probe
- cutover 后客户端不再依赖迁移期 fallback
- source_gc 后旧 handle 读失败且无脏读

重点现有脚本：

- `benchmark/vemb_v16_scaleout_coordinated_server_smoke.sh`
- `benchmark/vemb_v16_scaleout_coordinated_live_write_smoke.sh`

这两套应增加：

- “没有 normal remote write fallback” 的断言
- “没有 UB lookup RPC fallback” 的断言
- “cached hint stale 回退成功” 的验证

## 15.3 benchmark 观察项

建议新增或重点观察：

- VADD 写侧 CPU 是否下降
- cached hint 命中率
- owner-local probe 次数和平均额外探测数
- VSIM 单次延迟变化
- cutover 窗口长度

建议从 bench 输出中逐步去掉：

- `remote_meta_publish_*`
- `ub_lookup_rpc_*`

## 16. 风险与注意事项

### 16.1 最大风险

最大的风险不是删 `UB lookup RPC fallback` 本身，而是：

- routing snapshot 不够稳定
- owner-local shortlist 维护不一致
- cache stale 后 fallback 没有完全覆盖

因此真正的 correctness 支点会变成：

- routing snapshot
- owner-local deterministic probe
- slot meta validation
- migration cutover/source_gc 语义

### 16.2 最容易混淆的边界

需要明确写进实现约束：

- “所有节点都能读全局 warm region”
  不等于
- “所有节点天然知道 key 的精确 location”

新模型能删除 RPC fallback，不是因为数据更可见，而是因为 key 落点空间更小、更稳定、更可推导。

### 16.3 关于 cache 的底线

无论 cache 怎么简化，都不要把它重新抬回 correctness 层。

正确边界应始终是：

```text
cache/hint
-> deterministic probe
-> slot meta validate
-> payload copy
```

而不是：

```text
cache
-> 直接信任 location
```
