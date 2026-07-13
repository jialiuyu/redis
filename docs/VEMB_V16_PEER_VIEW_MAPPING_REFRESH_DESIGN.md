# VEMB V16 Peer-View Mapping Refresh Design

## 背景

当前 scaleout 的纯动态 attach 方案依赖新节点通过 `channel_desc` 把自己的
UB path 直接暴露给旧节点，然后旧节点拿这些字面 path 做 runtime attach。

在真实机器上，这个假设不成立：

- 对端自视角 path 不等于本机访问该对端资源时应使用的 peer-view path
- 例如 `node1` 本地 warm region 可能是 `/dev/obmm_shmdev1`
- 但 `node0` 访问 `node1` 的同一块资源时，实际应使用
  `/dev/obmm_shmdev5`

这使得“纯动态 attach”在当前硬件环境下不稳定，尤其会体现在：

- 新 owner 的 warm region 可见性错误
- remote meta owner view attach 到错误的设备路径
- UB RPC ring 路径语义不一致，导致 timeout / unmatched response

## 目标

把“资源什么时候 attach”和“本机该 attach 到哪条 UB path”拆开：

- owner 发现与 topology 演进仍然可以动态进行
- 但 attach 所依赖的本机视角 path，由用户在扩容前显式下发
- 同一套控制面既支持扩容节点，也支持只扩容 UB 内存/region

## 核心思路

新增 `peer-view map` 控制面：

- 用户把本机视角的 peer 资源映射下发给节点
- 节点缓存这份 mapping
- 节点可选择立即 attach 缺失资源，或仅缓存等待后续 topology 触发

这里的 mapping 是“本机视角”的，而不是对端自报视角。

## 控制面

新增 `topology_ctl --apply-peer-view-map FILE`。

控制请求包含三类资源：

- `warm_regions`
- `remote_meta_views`
- `ub_rpc_peers`

并带一个总开关：

- `attach_now: true|false`

当 `attach_now: true` 时：

- 缓存 mapping
- 对当前尚未 attach 的资源立即 attach

当 `attach_now: false` 时：

- 只缓存 mapping
- 后续由 topology 扩容或单独刷新动作触发 attach

## YAML 形态

```yaml
expected_local_owner_id: 0
attach_now: true
ub_rpc_timeout_ms: 200

warm_regions:
  - region_id: 101
    provider: ub
    path: /dev/obmm_shmdev5
    mmap_offset: 0
    bytes: 67108864
    value_size: 64
    home_ub_node_id: 1
    weight: 1

remote_meta_views:
  - owner_id: 1
    provider: ub
    path: /dev/obmm_shmdev5
    mmap_offset: 268435456
    entries: 8192
    buckets: 16384

ub_rpc_peers:
  - owner_id: 1
    provider: ub
    request_path: /dev/obmm_shmdev2
    request_mmap_offset: 8388608
    response_path: /dev/obmm_shmdev8
    response_mmap_offset: 16777216
    inbound_request_path: /dev/obmm_shmdev6
    inbound_request_mmap_offset: 8388608
    outbound_response_path: /dev/obmm_shmdev4
    outbound_response_mmap_offset: 16777216
```

## 运行时行为

### 节点扩容

1. 向旧节点下发新 owner 的 peer-view map
2. 向新节点下发当前全局 owners 的 peer-view map
3. 旧节点/新节点缓存并按需 attach
4. 再发布 candidate topology

### 只扩容 UB 内存

1. 对现有 owner 下发新增 region 的 peer-view map
2. 节点缓存并 attach 新 region
3. 不要求新增 owner，也不要求 topology active owner 集合变化

正式脚本：

- `benchmark/vemb_v16_expand_ub_memory_2node.sh`

对应操作说明：

- `test_expand_ub_host.md`

## UB RPC 动态 attach 注意事项

peer-view map 不只是补 warm region / remote meta 的可见性，`ub_rpc_peers`
也必须一起补齐。否则即便旧节点已经能看到新 owner 的 warm region，
跨 owner 的 VSIM key2 fallback 仍可能因为缺少 UB RPC peer 而失败。

这次落地里还修复了一个运行时追加 UB RPC peer 的实现细节：

- `vemb_v16_ub_rpc_attach_peer()` 会先创建新的 RPC 实例，再销毁旧实例
- 如果旧实例销毁时无条件清空 TLC 上的 `lookup_rpc` 回调
- 就会把刚注册好的新 RPC 回调一并抹掉
- 结果是 runtime attach 看似成功，但后续 UB RPC fallback 实际失效

修复后的行为是：

- 只有当 TLC 当前仍然指向“将被销毁的旧 rpc 实例”时，才清空回调
- 如果 TLC 已经切到新实例，则保留新回调不动

对应代码位置：

- `src/vemb_v16_ub_rpc.c`
- `benchmark/vemb_v16_manifest_ut.c`

当前已补回归测试，覆盖：

- manifest 启动时先带一个已有 UB RPC peer
- 运行时通过 `--apply-peer-view-map` 再追加第二个 peer
- 校验旧 peer / 新 peer 都仍可见
- 校验 TLC 上的 `lookup_rpc` 仍然指向当前新的 `storage->ub_rpc`

## 首版实现约束

首版实现刻意保守：

- 允许新增 mapping
- 允许对“尚未 attach”的资源立即 attach
- 如果同一 `region_id` / `owner_id` 已存在但配置不同，直接返回错误
- 不支持在线替换已 attach 资源到底层新 path

这样可以先把控制面语义做稳，避免 live switch 过程中出现路径与元数据错配。

## 代码落点

- `src/vemb_v16_peer_view_map.h`
  - 新增 peer-view map 控制结构
- `benchmark/vemb_v16_topology_ctl.c`
  - 新增 `--apply-peer-view-map`
- `src/vemb_v16_storage.c`
  - 新增 mapping cache
  - 新增 `vemb_v16_storage_apply_peer_view_map`
  - topology attach 优先使用本地 mapping
- `src/vemb_v16_tcp_transport.c`
  - 新增 TCP 控制帧
- `src/vemb_v16_aeron_transport.c`
  - 新增 UDS/Aeron 控制 opcode

## 已知限制

1. 首版 `topology_ctl --apply-peer-view-map` 使用轻量 YAML 解析器，只支持当前文档中的字段子集。
2. 首版不支持在线替换已 attach 资源的 path，只支持新增/幂等刷新。
3. 对于 UB RPC，仍建议在变更前保留 reset 流程，避免设备侧残留 ring 状态干扰排障。

## 2026-07-09 回归风险记录

当前实现虽然解决了 peer-view path 语义不一致问题，但这轮改动已经不再是
"只影响扩容路径"。下面三个风险会外溢到正常非扩容路径，后续修复前需要
按 blocker 处理。

### 风险 1: 普通 topology_set 被隐式绑定 peer-view map 预热

当前 `vemb_v16_storage_topology_set()` 在处理新 owner endpoint 时，会先尝试
`storage_attach_peer_owner_from_mapping()`；如果本地还没有对应 owner 的
peer-view map，直接返回失败。

这意味着：

- 以前只需要发布 topology 的普通多 owner 场景
- 现在也被要求必须先做 peer-view map 下发
- 否则即使不是扩容迁移，只是正常 topology publish / refresh，也可能失败

NOTE:
当前实现下，刷新集群/刷新拓扑时，必须同时具备节点信息和内存信息
(`ub regions`)；仅下发节点信息而缺少对应 peer-view memory mapping，会导致
`topology_set` 在 attach peer owner 阶段失败。

对应代码：

- `src/vemb_v16_storage.c:2948`
- `src/vemb_v16_storage.c:2955`

### 风险 2: owner resolver 运行时重写与普通请求并发缺少同步

当前 `topology_set()` 会在持有 `storage->topology_lock` 时重写：

- `storage->owner_ring`
- `storage->owner_hash_node_count`
- `storage->owner_hash_nodes[]`

但正常请求路径里的 `storage_owner_resolver()` 读取这些字段时没有拿同一把锁，
也没有使用原子快照或 RCU 风格切换。

这意味着：

- 非扩容普通请求也可能与 topology publish 并发
- 读到半更新 ring / hash node 数据
- 产生错误 owner 路由，属于正常路径数据竞争

对应代码：

- `src/vemb_v16_storage.c:71`
- `src/vemb_v16_storage.c:2967`

### 风险 3: runtime attach 热替换 lookup_rpc / remote_meta 视图缺少并发保护

当前 peer-view map apply/runtime attach 会在线追加：

- warm region
- remote meta owner view
- UB RPC peer

其中 UB RPC peer 追加会重建 `storage->ub_rpc`，并重新设置 TLC 上的
`lookup_rpc` / `lookup_rpc_arg`；remote meta owner view 也会直接扩展
`tlc->remote_meta_view_count` 和数组内容。

但普通读路径会无锁读取这些状态：

- `lookup_vsim_key2_via_rpc()` 直接调用 `tlc->lookup_rpc(...)`
- `remote_meta_view_for_key()` / `remote_meta_view_for_owner()` 直接遍历
  `remote_meta_views[]`

这意味着：

- 即使没有进入扩容迁移，只要控制面在运行时做 peer-view attach
- 普通远端查找 / remote-meta fallback 也会暴露在竞态里
- 最坏可能出现 UAF、空回调窗口或读到部分更新视图

对应代码：

- `src/vemb_v16_storage.c:730`
- `src/vemb_v16_ub_rpc.c:1038`
- `src/vemb_v16_tlc.c:670`
- `src/vemb_v16_tlc.c:1420`

## 建议使用顺序

推荐扩容顺序：

1. 启动新节点
2. 向新节点下发当前全局 owners 的 peer-view map
3. 由旧节点使用 `--set-with-peer-view-map` 一次下发 peer-view map 与 candidate topology
4. 校验 runtime attach 日志
5. 等待 scaleout local done / full active cutover

对于“只扩 UB 内存、不扩节点”，推荐顺序是：

1. 节点继续按原 manifest 运行
2. 对新增本地 region 的 owner 下发本地 peer-view map
3. 对其他 owner 下发该 region 的 peer-view map
4. 校验 `runtime warm region attached` 日志
5. 确认 topology epoch / active owner 集合保持不变

## 2026-07-09 扩 UB 远端验证

基于：

- `node0=192.168.90.111`
- `node1=192.168.90.112`

使用：

- node1 本地新增 region path：`/dev/obmm_shmdev3`
- node0 对 owner1 新 region 的 peer-view path：`/dev/obmm_shmdev7`

执行：

- `bash ./benchmark/vemb_v16_expand_ub_memory_2node.sh`

结果：

- node1 本地 region102 attach 成功
- node0 对 owner1 的 region102 peer-view attach 成功
- topology 保持 `epoch=1 active={0,1}`
- post-write / post-read 输出都出现 `warm regions=3`

## 2026-07-09 双机验证记录

基于 `benchmark/vemb_v16_scaleout_real_2node.sh`，在以下双机环境完成了真实
扩容验证：

- `node0`: `192.168.90.111`
- `node1`: `192.168.90.112`

本次验证覆盖了当前推荐流程：

1. 启动阶段使用 `manifest`
2. 扩容阶段由旧节点使用 `--set-with-peer-view-map`
3. node0 在 runtime attach owner1 的：
   - warm region
   - remote meta owner view
   - UB RPC peer
4. 进入 auto scaleout、local done、full active publish
5. 校验 cutover 后写入与读取

同时验证了 `风险 2` 的修正方式：

- `storage_owner_resolver()` 改为读取原子切换的 snapshot
- 扩容场景下，新 owner resolver snapshot 不在 `topology_set()` 立即发布
- 而是在 full-active/cutover 时点再切换，避免 `VSIM key2` 在迁移窗口过早按
  post-expand owner 视图解析

实测结果：

- node0 `--set-with-peer-view-map` 返回成功：
  - `status=0`
  - `peer_view_map_status=0`
  - `topology_status=0`
- node0 runtime attach 三类资源全部成功
- source 自动迁移统计：
  `marked=627 skipped=397`
- coordinator 成功发布 full active topology：
  `scaleout_full_active_published=2 errors=0 targets=2`
- 最终 topology：
  `epoch=24 active={0,1} standby={0,1}`
- 迁移旧数据校验通过：
  `verified=8/627 migrated keys on owner=1`
- cutover 后写验证通过：
  `4000/4000 ok`
- cutover 后读验证通过：
  `4000/4000 ok`

本轮双机验证未观察到新增回归：

- 未出现 peer-view attach 失败
- 未出现 full-active publish 失败
- 未出现迁移后旧数据缺失
- 未出现 cutover 后读写失败

同日也完成了 UB RPC 动态追加 peer 的本地回归验证：

- 重新编译：
  - `make -C src vemb_v16_server`
  - `make -C benchmark vemb_v16_manifest_ut`
- 回归测试：
  - `./benchmark/vemb_v16_manifest_ut`
- 结论：
  - 运行时追加 `ub_rpc_peers` 后，TLC lookup rpc 回调保持有效
  - 不会再出现“新 peer attach 成功，但旧实例析构把新回调清空”的问题

## 风险 2 / 风险 3 收敛方案

后续实现建议把 `风险 2` 与 `风险 3` 统一收敛到“不可变 snapshot +
原子发布 + 延迟回收”的模型，而不是分别在读路径上零散补锁。

### 总体原则

1. 普通读路径只读取稳定 snapshot，不读取正在被原地改写的共享状态。
2. 控制面/attach 路径先构造完整新视图，再一次性原子发布。
3. 旧对象在新视图发布后不能立即释放，必须延迟回收。

### 分层模型

建议拆成两层运行时视图：

1. `routing snapshot`
2. `access snapshot`

`routing snapshot` 负责解决 `风险 2`，只承载 owner 路由判定所需状态：

- `owner_ring`
- `owner_hash_node_count`
- `owner_hash_nodes[]`

`access snapshot` 负责解决 `风险 3`，承载远端访问所需状态：

- `remote_meta_view_count`
- `remote_meta_views[]`
- `ub_rpc_runtime`

这样 `VSIM key2` 的运行时依赖会被明确拆开：

1. 先由 `routing snapshot` 判定 key 应归属哪个 owner
2. 再由 `access snapshot` 决定如何访问该 owner 的 remote-meta / UB RPC

### 时序要求

扩容场景下，推荐按以下时序切换：

1. 先 attach 新 owner 的 warm region / remote meta / UB RPC 资源
2. 先发布新的 `access snapshot`
3. 再在 full-active / cutover 时点发布新的 `routing snapshot`
4. 最后延迟回收旧 `ub_rpc_runtime`

这样可以避免：

- `routing snapshot` 已切到新 owner
- 但 remote-meta / UB RPC 访问能力尚未 ready
- 导致 `VSIM key2` 提前按 post-expand owner 视图访问失败

### 第一阶段实现

第一阶段优先保证正确性，性能保持在可接受范围：

1. `routing snapshot`
   - 双缓冲
   - 原子切换
   - 扩容场景延后到 full-active / cutover 发布

2. `access snapshot`
   - 把 `remote_meta_views[] + remote_meta_view_count + ub_rpc_runtime`
     放入同一份稳定视图
   - runtime attach 时先构造完整新 snapshot，再一次性发布
   - 写侧仅在发布时串行化；读侧通过原子指针 + double-check
     获取当前 snapshot / runtime，不在普通 lookup 路径加 mutex

3. `lookup_rpc`
   - 固定函数入口
   - 不再依赖可热替换函数指针语义作为并发边界

4. `ub_rpc` 生命周期
   - 新 runtime 先完整 ready
   - 发布到 `access snapshot`
   - 旧 runtime 使用 `refcount + retire` 延迟回收

第一阶段目标是先消除：

- owner routing 半更新可见
- remote-meta 视图半更新可见
- UB RPC 旧实例过早释放导致的 UAF/空回调窗口

### 对非扩容正常路径的影响

本次方案对非扩容 steady-state 的功能语义基本不做改变：

- `routing snapshot` 在非扩容场景仍然立即发布，不会延后切换
- `vadd / vemb` 的本地 warm-region 主路径不引入额外锁
- cutover 后的稳定态 owner 判定、local put/get 语义保持不变

主要变化集中在 `VSIM key2` 相关路径：

- 读取 `remote_meta_views[] / remote_meta_view_count / lookup_rpc /
  ub_rpc_runtime` 时，不再分别直接读取共享字段
- 改为先获取一份稳定的 `access snapshot`
- 整次 lookup 在同一份 snapshot 上完成，避免看到“路由已切换，
  但访问能力还是旧半套状态”的中间态

`ub_rpc fallback` 路径的变化是：

- 固定 `lookup_rpc` 入口
- 通过原子指针 + double-check 获取当前 runtime
- 对旧 runtime 使用 `refcount + retire` 延迟回收

因此，正常情况下的影响可以概括为：

- `vadd / vemb` 本地主路径：基本无感知
- `VSIM key2` remote-meta / UB RPC 路径：增加少量原子读写与
  refcount 开销
- 控制面 attach / topology refresh 路径：写侧需要先构造新 snapshot，
  再一次性原子发布

### 性能预期

当前尚未做严格的改动前后 A/B 基准，因此这里给出的是基于实现路径的
预期，而不是最终测量值。

- `vadd / vemb` 本地 steady-state：
  预期可近似视为无明显退化
- `VSIM key2` 走 remote-meta 路径：
  预期仅有轻微退化，主要来自一次 `access snapshot`
  acquire/release 的原子与 refcount 成本
- `VSIM key2` 走 UB RPC fallback 路径：
  预期仅有轻微退化，新增成本主要是 runtime 的原子获取与 refcount；
  相比 ring publish / wait response / remote handler 的总成本，占比应较小

总体判断是：

- 本次改动更偏正确性加固，而不是给主数据面热路径加锁
- 不预期出现对整体非扩容 steady-state 吞吐的明显退化
- 更可能看到的只是 `VSIM key2` 边缘路径上的小幅成本增加

### 第二阶段优化

在第一阶段稳定后，再按压测结果决定是否继续优化：

1. 把 `ub_rpc` 回收从 `refcount` 升级为 `epoch/RCU`
2. 进一步减少 `VSIM key2` fallback 路径上的原子开销
3. 视需要压缩 `access snapshot` 布局，减少 cache miss

第二阶段只在性能数据表明确有必要时推进，不作为第一阶段 blocker。
