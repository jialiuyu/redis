# VEMB Job Pool + Slot-ID Queue Design

日期：2026-07-06

本文整理 `proxy -> supernode` 下一阶段的 job queue 方案：在已经验证过的“小 descriptor + job_ptr”基础上，进一步去掉每请求 `zmalloc/zfree`，改为 **pool + slot_id + return ring**。

## 1. 背景

当前代码已经完成第一阶段：

1. `job_shard_queue` 不再传整块大 job struct
2. ring slot 里只传小 `job_desc`
3. 真实 job 由 proxy 分配
4. supernode 消费后释放

这一版已经证明：

1. “小 descriptor 替代大 slot memcpy” 是有效的
2. 所有权边界可以收敛清楚
3. `TCP mixed-80r20w` 的顺序语义可以保持不变

但新的瓶颈也比较明确：

1. job queue 的大 copy 基本已经不再是主矛盾
2. 每请求 `zmalloc/zfree` 变成了新的固定成本
3. 真实 payload 的 slice/load 开始比 queue copy 更显眼

因此第二阶段的目标不是再回退到“大结构体值传递”，而是：

**保留小 queue payload，同时去掉 heap alloc/free。**

## 2. 为什么不是直接传 `req_t *`

一个直觉上的想法是：ring slot 里直接放 `req_t *`，slot 大小也很小，看起来就能绕过 `zmalloc`。

但这个方案并不能直接成立，原因是：

1. `req_t` 往往来自 proxy 的临时接收缓冲或局部对象，生命周期不足以被 supernode 异步持有
2. 如果要让 `req_t *` 安全，就仍然需要把它搬到稳定存储里，本质上还是需要 heap 或 pool
3. `req_t` 是协议 ingress layout，不一定是最适合 `proxy -> supernode` 的内部执行 layout
4. 后续如果要迁移到 SHM/UB 或者拆进程，裸指针复用性很差

所以真正值得做的，不是“传 `req_t *`”，而是：

1. ring 里继续只传小引用
2. 真实 job 存在稳定的预分配 pool 里
3. 引用使用 `slot_id` / `pool_type` / `generation`

## 3. 目标

本设计的目标是：

1. 保持当前 `proxy worker -> supernode worker` shard routing 不变
2. 保持同一 channel 的 FIFO 与 `mixed-80r20w` 顺序语义不变
3. 保持 `job_shard_queue` 为固定小 slot
4. 去掉每请求 `zmalloc/zfree`
5. 为后续扩展到 SHM/UB backend 留接口

第一阶段只处理 `proxy -> supernode` job queue 及其回收，不改 `completion_ring`。

## 4. 总体思路

建议把当前“pointer descriptor”进一步演进成下面的模型：

```text
proxy worker
  -> from local pool alloc slot_id
  -> fill real job into pool slot
  -> publish small job_ref to shard queue

supernode worker
  -> dequeue job_ref
  -> locate pool slot by (proxy_worker_id, pool_type, slot_id)
  -> execute job
  -> publish completion as today
  -> publish slot_id to return ring

proxy worker
  -> drain return ring
  -> recycle slot back to its local free list
```

关键点：

1. `job_shard_queue` 继续只搬小对象
2. 真实 job 不在 heap，而在预分配 pool slot
3. supernode 不直接改 owner 的 free-list
4. slot 回收由 owner proxy worker 完成

## 5. 核心结构

### 5.1 Job Queue Payload

建议把 `job_shard_queue` payload 定成：

```c
typedef struct vemb_v16_job_ref {
    uint16_t proxy_worker_id;
    uint16_t pool_type;
    uint32_t slot_id;
    uint32_t generation;
    uint32_t req_id;
    uint8_t op;
    uint16_t reserved0;
} vemb_v16_job_ref_t;
```

字段说明：

1. `proxy_worker_id`
   标识 owner pool 属于哪个 proxy worker
2. `pool_type`
   标识具体哪一种 job pool
3. `slot_id`
   pool 内槽编号
4. `generation`
   防止 slot 快速复用导致 stale ref
5. `req_id/op`
   主要用于诊断、统计和防御式校验

### 5.2 Return Queue Payload

supernode 回收时不直接触碰 owner free-list，而是发一个 return record：

```c
typedef struct vemb_v16_job_return {
    uint16_t pool_type;
    uint16_t reserved0;
    uint32_t slot_id;
    uint32_t generation;
} vemb_v16_job_return_t;
```

第一版保持最小即可，不必再带完整调试字段。

### 5.3 Pool Slot Header

```c
typedef enum vemb_v16_job_slot_state {
    VEMB_V16_JOB_SLOT_FREE = 0,
    VEMB_V16_JOB_SLOT_RESERVED = 1,
    VEMB_V16_JOB_SLOT_PUBLISHED = 2,
    VEMB_V16_JOB_SLOT_RUNNING = 3,
} vemb_v16_job_slot_state_t;

typedef struct vemb_v16_job_slot_hdr {
    uint8_t op;
    uint8_t reserved0;
    atomic_uint state;
    uint32_t generation;
} vemb_v16_job_slot_hdr_t;
```

### 5.4 Pool Slot

建议每个 slot 固定承载一种 union：

```c
typedef struct vemb_v16_job_slot {
    vemb_v16_job_slot_hdr_t hdr;
    union {
        vemb_v16_vemb_job_t read_job;
        vemb_v16_vsim_key_key_job_t vsim_job;
        vemb_v16_vadd_job_t inline_job;
    } u;
} vemb_v16_job_slot_t;
```

这样好处是：

1. supernode 拿到 slot 后不需要二次解析变长 buffer
2. 和当前第一阶段代码结构兼容度高
3. 之后如果要继续收紧 layout，还可以在 union 内按需调整

## 6. Pool 拓扑

建议采用：

1. 每个 `proxy_io_worker` 拥有自己的本地 pool 组
2. pool 再按 job kind 拆分
3. supernode 只读 slot，不分配，也不直接归还 free-list

建议至少拆三类：

1. `READ_POOL`
   用于 `PING`、`VEMB_HANDLE`、`VEMB_INLINE`、`VREM`
2. `VSIM_KEY_KEY_POOL`
   用于 `VSIM_KEY_KEY`
3. `INLINE_VECTOR_POOL`
   用于 `VADD`、`VSIM_INLINE`

理由：

1. 读类请求占比高，不应该被 inline vector 大 slot 放大 footprint
2. `VSIM_KEY_KEY` 比普通读多一个 key2，单独拆开更清楚
3. `VADD/VSIM_INLINE` 仍然携带 full vector，应该和小读路径隔离

## 7. 地址定位

通过 `slot_id` 找回对象的方式保持简单：

```text
slot_addr = pool->slots + slot_id * pool->slot_stride
slot      = (vemb_v16_job_slot_t *)slot_addr
job       = &slot->u
```

所以 `slot_id` 的本质就是 “该 pool 中第几个槽位”。

## 8. 分配与回收

### 8.1 分配路径

proxy worker 只从自己的本地 free stack 分配：

```text
local free stack pop
  -> mark slot RESERVED
  -> fill payload
  -> mark slot PUBLISHED
  -> enqueue job_ref
```

如果 enqueue 失败：

1. slot 状态回滚到 `FREE`
2. `generation` 不变
3. slot 放回本地 free stack

### 8.2 为什么不让 supernode 直接 free

不建议让 supernode 直接把 slot push 回 proxy worker 的 free-list，原因是：

1. 会引入跨线程 free-list 同步
2. 很容易把第一版实现拖进 ABA 和 memory order 细节
3. 调试成本高

因此建议用 **return ring** 做“两段式回收”：

1. supernode 发布 `job_return`
2. owner proxy worker 轮询并真正回收 slot

这让分配端仍然保持：

1. 本地 free stack
2. 单线程 owner 管理
3. 无需全局锁

## 9. 所有权规则

建议把所有权定义为：

1. `FREE` / `RESERVED`
   归 owner proxy worker
2. `PUBLISHED` / `RUNNING`
   逻辑上属于 supernode 消费中
3. `RUNNING` 完成后
   supernode 只负责发 `job_return`
4. slot 真正回到 `FREE`
   必须由 owner proxy worker 执行

这样可以避免“谁有权修改 free-list”变得含糊。

## 10. 状态机

建议第一版采用足够简单的状态机：

```text
FREE
  -> RESERVED
  -> PUBLISHED
  -> RUNNING
  -> FREE
```

状态含义：

1. `FREE`
   可分配
2. `RESERVED`
   owner 正在填充，尚未对外可见
3. `PUBLISHED`
   已入 `job_shard_queue`
4. `RUNNING`
   supernode 已接手执行

第一版不建议引入过多中间状态，否则实现复杂度会大于收益。

## 11. Generation / ABA 防护

`slot_id` 单独使用有明显 ABA 风险：

1. 请求 A 使用 `slot 42`
2. A 完成后 `slot 42` 回收
3. 请求 B 又复用 `slot 42`
4. 慢路径仍持有旧 ref

因此建议：

1. 每次 slot 从 `FREE -> RESERVED` 时保留当前 `generation`
2. 真正回收到 `FREE` 后，`generation++`
3. supernode 消费前必须校验：
   - `slot->hdr.generation == job_ref.generation`
   - `slot->hdr.state == PUBLISHED`
4. proxy drain `job_return` 时也要校验 generation

## 12. 内存序

第一版使用保守的规则即可：

1. producer 先填 payload，再发布 `state=PUBLISHED`
2. enqueue `job_ref` 前对 slot 内容做 release
3. consumer 出队 `job_ref` 后，对 slot header 做 acquire
4. consumer 将状态改为 `RUNNING` 后再读取 payload
5. return ring 出队后，owner 再将 slot 改回 `FREE`

不需要第一版就追求极限 lock-free 技巧，先把语义闭环做对。

## 13. 顺序语义约束

这一点必须保持不变：

1. 同一 channel 的请求仍走当前 shard routing
2. 不按 op 拆独立 job queue
3. `mixed-80r20w` 中同连接读写顺序语义不能回退

换句话说：

**第二阶段只改 queue 里传的内容和对象存储位置，不改排序模型。**

## 14. 失败路径

### 14.1 Pool Exhausted

如果 owner pool 没有空 slot：

1. 计数 `pool_empty`
2. 当前请求返回 busy/err
3. 不阻塞整个 worker 长时间等待

第一版不建议做“pool 空时回退 heap alloc”，否则会让语义和性能分析变复杂。

### 14.2 Job Queue Publish Failure

如果 `job_ref` 入队失败：

1. slot 状态回退为 `FREE`
2. slot 放回 owner free stack
3. 请求走现有错误路径

### 14.3 Supernode Late Consume

如果 supernode 拿到 `job_ref` 后发现：

1. `generation` 不匹配
2. `state` 不是 `PUBLISHED`
3. `kind/op` 和 ref 不一致
4. channel 已失效

则：

1. 丢弃该 job
2. 记录诊断计数
3. 不继续访问 payload
4. 仍可选择 best-effort 发布 return，避免 slot 泄漏

### 14.4 Channel Close

channel close 不应该直接销毁 pool slot；slot 生命周期应独立于 channel 生命周期。

处理原则：

1. 已发布但未消费的 slot，仍然允许 supernode 拿到后按 channel state 丢弃
2. 已消费完成的 slot，仍由 return ring 回 owner 回收
3. proxy 不需要在 close 路径里扫描并暴力回收所有 in-flight slot

这能显著降低 close 路径复杂度。

## 15. 容量建议

建议每个 `proxy_worker x pool_kind` 单独定容量。

经验公式：

```text
pool_slots >= channels_per_proxy_worker * pipeline * safety_factor
```

其中：

1. `READ_POOL` 可以更大
2. `INLINE_VECTOR_POOL` 需要结合写比例单独核算
3. `safety_factor` 第一版可先取 `2`

## 16. 与当前 pointer-descriptor 方案的关系

当前第一阶段是：

```text
job_desc { job_ptr }
```

下一阶段会变成：

```text
job_ref { proxy_worker_id, pool_type, slot_id, generation }
```

二者的上层流程几乎一样：

1. proxy 先得到稳定 job 存储
2. ring 只发小引用
3. supernode 解引用执行
4. 执行后触发回收

因此第二阶段并不是推翻第一阶段，而是把“稳定对象存储”从 heap 替换成 pool slot。

## 17. 推荐落地顺序

### 17.1 Step 1

先定义：

1. `vemb_v16_job_ref_t`
2. `vemb_v16_job_return_t`
3. `vemb_v16_job_pool_t`
4. `vemb_v16_job_slot_hdr_t`

### 17.2 Step 2

给每个 `proxy_io_worker` 挂：

1. `READ_POOL`
2. `VSIM_KEY_KEY_POOL`
3. `INLINE_VECTOR_POOL`
4. 对应的 `return_ring`

### 17.3 Step 3

改 `publish_request_job()`：

1. 选 pool
2. alloc slot
3. fill slot payload
4. publish `job_ref`

### 17.4 Step 4

改 `drain_shard_queues()`：

1. poll `job_ref`
2. 定位 slot
3. 校验 `generation/state/kind/op`
4. 执行
5. publish `job_return`

### 17.5 Step 5

在 owner proxy worker 主循环里增加：

1. drain `return_ring`
2. 校验 generation
3. slot 回到 `FREE`
4. push 回本地 free stack

### 17.6 Step 6

先只覆盖：

1. `VEMB_INLINE`
2. `VADD`
3. `VREM`
4. `PING`
5. `VSIM_KEY_KEY`
6. `VSIM_INLINE`

### 17.7 Step 7

只跑：

1. 本地 build + UT
2. `TCP mixed-80r20w`
3. 对比 pointer-descriptor 版本 QPS 和采样 timing

## 18. 第一版范围控制

第一版不建议同时做下面这些事情：

1. `completion_ring` pool 化
2. SHM/UB pool backend
3. 跨进程共享 pool
4. 复杂 lock-free 全局 free-list
5. pool 空时 fallback 到 heap

先把本地 DRAM pool 版本做出来，确认收益和复杂度都可控。

## 19. 后续扩展

这套设计后续可以平滑扩展到：

1. **SHM/UB pool**
   `slot_id` 不依赖虚拟地址，天然适合共享后端
2. **completion pool**
   completion 也可变成 `completion_ref + slot`
3. **跨进程 proxy/supernode**
   只要共享 pool backend，协议层不必依赖裸指针
4. **更细粒度 layout**
   如果后续还要继续压 footprint，可把 union job 进一步按 op 收紧

## 20. Bottom Line

第二阶段的推荐方向可以概括为：

**保留小 queue payload，不回退到大 struct 值传递；用 per-proxy-worker 本地 job pool 承载真实 job，用 `slot_id + generation` 做稳定引用，并通过 return ring 让 owner 线程完成回收。**

这样可以同时拿到：

1. 小 `job_shard_queue` slot
2. 无每请求 `zmalloc/zfree`
3. 清晰的所有权边界
4. 与当前 mixed 顺序语义兼容
5. 面向 SHM/UB 的后续演进空间
