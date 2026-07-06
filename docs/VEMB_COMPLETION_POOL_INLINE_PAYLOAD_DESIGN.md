# VEMB Completion Pool + Inline Payload Design

日期：2026-07-06

本文整理 `supernode -> proxy` 这半条返回链路的下一阶段优化：在 `job pool + slot_id` 已经落地后，把 `completion_ring` 和 `inline payload` 一起改成 **小 ref + 预分配 pool + return ring**，避免继续在热路径里做“大 completion 值传递 + 每请求 zmalloc/zfree payload snapshot”。

## 1. 背景

当前 `proxy -> supernode` 的 job 路径已经完成两轮收敛：

1. `job_shard_queue` 不再传整块大 job struct
2. ring slot 已经缩成小 `job_ref`
3. 真实 job 改为 pool slot 承载
4. 回收通过 return ring 回到 owner proxy worker

但 `supernode -> proxy` 这半边仍然保留旧模型：

1. `completion_ring` 按 `sizeof(vemb_v16_completion_t)` 做 fixed-slot copy
2. `VEMB_INLINE` 读在 supernode 侧调用 `snapshot_vemb_payload()`
3. 每个 inline completion 仍然做一次 `zmalloc(vector_bytes)`
4. proxy 侧在 `completion_release_payload()` 里释放 payload

这意味着 job 侧已经把“大消息 memcpy”收掉了，但返回链路里仍然有两类固定成本：

1. completion 元数据按大 struct 在线程间复制
2. inline payload 按请求分配/释放堆内存

## 2. 当前瓶颈判断

结合现有代码与前面压测结论，可以把现状概括成：

1. `completion_publish_avg_ns` 已经不高，通常只有几十纳秒量级
2. `payload_local_slice` 比 completion publish 更容易变成显著热点
3. TCP vector-inline 路径还要承担 `writev`、backlog copy 和 1200B payload 发送

所以结论不是“completion 不值得优化”，而是：

**completion slimming 必须和 inline payload 生命周期一起做，收益才会成段出现。**

单做 completion metadata 瘦身，通常只能拿到 `1% - 3%`。  
completion 改成 ref/pool 后，叠加去掉每请求 payload `zmalloc/zfree`，整体更现实的目标是 `4% - 8%`。

## 3. 设计目标

这版设计目标是：

1. 保持每个 channel 独立 completion ring 的顺序边界
2. 保持慢客户端隔离和 backlog 语义不变
3. 把 `completion_ring` slot 从“大 completion struct”改成小 `completion_ref`
4. 去掉 `VEMB_INLINE` 每请求 `zmalloc/zfree`
5. 明确 inline payload 在 `write success / partial write / channel close` 下的释放责任
6. 不破坏当前 `VEMB_V16_OP_VEMB_INLINE` 即“response 携带 payload”的语义

第一阶段不改变 wire protocol，也不改变 channel backlog 的外部行为。

## 4. 当前问题拆解

### 4.1 Completion struct 过大

当前 `vemb_v16_completion_t` 同时承载：

1. 通用 header
2. handle 信息
3. redirect 信息
4. VSIM `score`
5. `inline_vector` 指针
6. `inline_vector_bytes`

结果是：

1. `VEMB_HANDLE` 也要为 `inline_vector`、`score`、`redirect_owner` 付固定布局成本
2. `completion_ring` 不论 op 都按这份最大结构 copy
3. proxy poll 后本地 batch buffer 也继续背这份 footprint

### 4.2 Inline payload 生命周期割裂

当前 inline payload 的 owner 流程是：

```text
supernode
  -> zmalloc payload snapshot
  -> publish completion by value
proxy
  -> encode/write response
  -> completion_release_payload()
```

这个模型的问题是：

1. payload 和 completion metadata 没有统一 slot 生命周期
2. per-request alloc/free 容易成为固定成本
3. 后续若想把 completion 也改成 ref，需要重新补一套 return/recycle 规则

### 4.3 Backlog / close 语义要求生命周期非常清楚

当前 TCP transport 允许：

1. `writev` 一次写完
2. 只写出一部分，然后把剩余字节复制进 channel backlog
3. channel 在 backlog 未清空前关闭

因此优化时不能假设“proxy dequeue 完 completion 就可以立刻把外部 payload 内存丢掉”。  
必须明确：何时 payload 已经被 socket 消费，何时只是被 backlog 接管，何时必须在 close 时统一清理。

## 5. 总体思路

建议把返回路径收敛成下面的模型：

```text
supernode worker
  -> alloc completion meta slot
  -> if inline read: alloc payload slot
  -> fill slots
  -> publish small completion_ref to per-channel completion ring

proxy worker
  -> dequeue completion_ref
  -> locate completion slot
  -> encode/write response directly from slot view
  -> if partial write: copy unsent tail into channel backlog
  -> publish completion_return

supernode worker
  -> drain completion_return ring
  -> recycle meta slot / payload slot
```

关键点：

1. ring 里继续只搬小对象
2. completion metadata 和 inline payload 都有稳定 pool slot
3. 真正 owner 仍是生产者 supernode worker
4. 回收通过 return ring 回到 owner，而不是 proxy 直接碰 supernode free-list

## 6. 为什么 completion 和 payload 要一起做

如果只把 `completion_ring` 改成传 `vemb_v16_completion_t *`，收益有限：

1. ring slot 的确会变小
2. 但 inline payload 仍然是每请求 `zmalloc/zfree`
3. payload 生命周期仍然独立，proxy 仍要手工 free
4. channel close/backlog 清理仍然维持“completion struct + 外挂堆指针”的旧规则

这只能吃到一部分 copy 收益，吃不到 allocator 和生命周期统一的收益。

更合理的做法是：

1. `completion_ring` 改成小 `completion_ref`
2. `completion metadata` 放进固定 pool
3. `inline payload` 放进独立 payload pool
4. proxy 只消费 ref，完成发送后通过 return ring 归还

## 7. 核心结构

### 7.1 Completion Ring Payload

建议 `completion_ring` payload 改成：

```c
typedef struct vemb_v16_completion_ref {
    uint16_t supernode_worker_id;
    uint16_t pool_id;
    uint32_t slot_id;
    uint32_t generation;
    uint32_t req_id;
    uint8_t op;
    uint8_t kind;
    uint16_t reserved0;
} vemb_v16_completion_ref_t;
```

字段含义：

1. `supernode_worker_id`
   标识 owner pool 属于哪个 supernode worker
2. `pool_id`
   标识 completion pool 类型
3. `slot_id`
   元数据 slot 编号
4. `generation`
   防止 slot 快速复用导致 stale ref
5. `req_id/op/kind`
   用于诊断、统计和防御式校验

### 7.2 Completion Return Payload

proxy 在“响应已交给 socket 或 backlog”之后，发布：

```c
typedef struct vemb_v16_completion_return {
    uint16_t pool_id;
    uint16_t payload_pool_id;
    uint32_t slot_id;
    uint32_t generation;
    uint32_t payload_slot_id;
    uint32_t payload_generation;
} vemb_v16_completion_return_t;
```

如果该 completion 没有 inline payload：

1. `payload_pool_id = 0`
2. `payload_slot_id = UINT32_MAX`
3. `payload_generation = 0`

### 7.3 Completion Meta Slot

建议把当前“大而全 completion struct”拆成：

```c
typedef struct vemb_v16_completion_meta {
    uint64_t vector_offset;
    uint64_t owner_generation;
    float score;
    uint32_t vector_bytes;
    uint32_t region_id;
    uint32_t local_slot;
    uint32_t redirect_owner;
    uint32_t payload_slot_id;
    uint32_t payload_generation;
    uint8_t status;
    uint8_t flags;
    uint16_t reserved0;
} vemb_v16_completion_meta_t;
```

按常规 8B 对齐，这版是 **48B**；上一版大约 **88B**。

这里保留了所有第一阶段响应编码需要的字段，但去掉了：

1. `inline_vector` 裸指针
2. 只有生命周期语义、没有协议语义的堆分配痕迹
3. `op` / `req_id`
4. `channel_index` / `channel_id`
5. `key_hash`
6. `payload_kind` / `payload_bytes`

原因分别是：

1. `op`、`req_id` 已经在 `completion_ref` 里，proxy 组包时直接用 ref 即可
2. completion ring 本身是 per-channel 的，所以 metadata 不必再重复携带 `channel_index/channel_id`
3. `key_hash` 只服务于诊断日志，不是 response encode 的必需字段；如果后面确实想保留，可只在 debug/diag 构建打开
4. 第一阶段 payload pool 只承载 inline vector snapshot，所以 `payload_bytes == vector_bytes`，`payload_kind` 也恒等于 “inline vector”

也就是说，这个 meta slot 更接近：

1. `vemb_v16_resp_t` 里真正会被编码的最小字段集
2. 再加两项 payload 回收定位信息：`payload_slot_id` / `payload_generation`

### 7.4 Inline Payload Slot

建议 payload 单独建 pool：

```c
typedef struct vemb_v16_inline_payload_slot {
    uint32_t generation;
    uint32_t payload_bytes;
    uint8_t data[];
} vemb_v16_inline_payload_slot_t;
```

默认按 `VEMB_V16_MAX_VECTOR_BYTES` 固定 stride 预分配。

这样做的好处是：

1. `VEMB_HANDLE` / `VSIM` completion 不会被 1200B payload footprint 放大
2. payload 生命周期和 metadata 生命周期可以解耦
3. 未来如果要引入多种 payload class，也只需要换 pool 拓扑

## 8. Pool 拓扑建议

建议 mirror job path，采用：

1. 每个 `supernode_worker` 拥有自己的 completion meta pool
2. 每个 `supernode_worker` 也拥有自己的 inline payload pool
3. 每个 `proxy_worker x supernode_worker` 维持一条 completion return ring

### 8.1 为什么 owner 选 supernode worker

因为：

1. completion 是 supernode 生产的
2. inline payload snapshot 也是 supernode 从 TLC load 出来的
3. supernode 最容易在本地 owner free-list 上分配/回收
4. proxy 只负责消费和归还，不承担跨线程释放

### 8.2 为什么不用 per-channel completion pool

不建议按 channel 分配 completion pool，原因是：

1. channel 数量多，内存碎片和空槽浪费更明显
2. active/idle channel 差异大，容量不好预估
3. close 时还要把 pool 生命周期绑定到 channel，更复杂

per-channel ring 保持顺序边界即可；真实对象更适合挂在 supernode worker 本地 pool。

## 9. 生产与消费流程

### 9.1 SuperNode 生产侧

对于普通 `VEMB_HANDLE` / `VREM` / `VSIM`：

```text
alloc completion meta slot
  -> fill response metadata
  -> publish completion_ref
```

对于 `VEMB_INLINE`：

```text
alloc completion meta slot
  -> alloc payload slot
  -> tlc_load_vector() directly into payload slot
  -> fill meta.payload_{slot_id,generation,bytes}
  -> publish completion_ref
```

这里关键变化是：

1. 不再 `zmalloc(vector_bytes)`
2. `snapshot_vemb_payload()` 改成“load into payload slot”
3. metadata 和 payload 都进入统一 owner/recycle 协议

### 9.2 Proxy 消费侧

proxy worker dequeue `completion_ref` 后：

1. 先通过 `(supernode_worker_id, pool_id, slot_id)` 定位 metadata slot
2. 如果 `payload_kind == INLINE_VECTOR`，再定位 payload slot
3. 直接用 slot view 生成 `resp`
4. 走现有 TCP/Aeron publish 逻辑

proxy 不再对 completion 做深拷贝，也不再自己 free payload。

## 10. TCP backlog 生命周期

这是整套设计里最容易出错的地方，建议规则写死。

### 10.1 一次写完

```text
proxy dequeue ref
  -> write/writev success
  -> publish completion_return
```

此时可以立即归还：

1. completion meta slot
2. payload slot

### 10.2 Partial write 后进入 backlog

当前 backlog 语义是“把未发送字节复制进 channel backlog buffer”。  
只要这个语义不变，流程可以定义为：

```text
proxy dequeue ref
  -> writev partial
  -> copy unsent bytes into backlog buffer
  -> publish completion_return
```

也就是说：

1. payload slot 不需要陪 backlog 活到 socket drain 完成
2. 它只需要活到“剩余字节已经拷进 backlog”为止
3. 这样可以避免在 backlog 里长期持有外部 pool ref

### 10.3 Channel close

如果 channel 在本批 completions 处理过程中关闭，规则是：

1. 尚未发布到 backlog 的 completion，proxy 直接发布 `completion_return`
2. 已经复制进 backlog 的字节由 backlog buffer 自己清理
3. close 路径不再需要手工 `zfree(inline_vector)`

这会让 close cleanup 比当前“struct 值 + 堆指针”模型更简单。

## 11. 为什么不把 payload 直接内嵌进 completion slot

另一个直觉方案是：

1. 一个 completion slot 里直接内嵌 metadata + 1200B payload buffer
2. `completion_ref` 只指向这一个大 slot

这个方案实现更直接，但不建议作为默认方向，原因是：

1. 绝大多数 completion 并不携带 inline payload
2. 所有 completion slot 都会被最大 payload stride 放大
3. meta-only completion 的 cache footprint 会明显变差
4. payload pool 独立后更容易按 workload 单独扩容

所以推荐：

1. `completion_meta_pool`
2. `inline_payload_pool`
3. metadata 用 ref 指向 payload

只有在后续 benchmark 证明“二次定位 payload slot”开销大于 cache 节省时，才考虑把二者重新合并。

## 12. 为什么不直接传 `vemb_v16_completion_t *`

如果 ring 里只传指针，确实可以先吃到 slot 变小的收益，但它仍然保留几个问题：

1. completion 对象本身仍然是“大而全布局”
2. `inline_vector` 仍然是裸堆指针
3. proxy 仍然负责 `zfree`
4. 没有 generation / slot state，stale pointer 防御弱
5. 后续再切 pool/slot_id 时还要再做一次语义迁移

所以更值得一次做对的是：

**直接上 `completion_ref + meta pool + payload pool + return ring`。**

## 13. 预期收益

按当前瓶颈分布，这套方案的收益可以拆成三层：

### 13.1 Completion metadata slimming

收益来源：

1. `completion_ring` slot 缩小
2. proxy batch poll 的本地 footprint 缩小
3. cache line 命中更紧凑

预估收益：`1% - 3%`

### 13.2 去掉每请求 payload alloc/free

收益来源：

1. 去掉 `zmalloc/zfree`
2. 去掉 allocator 跨线程抖动
3. payload 生命周期改成 owner-local recycle

预估收益：`2% - 4%`

### 13.3 合并后的总收益

因为二者会叠加影响 cache 和生命周期，所以整体通常不是简单相加。  
更现实的区间是：`4% - 8%`。

如果当前 `mixed-80r20w` TCP vector-inline 基线在 `2.84M QPS` 左右，则粗略对应：

1. `+4%` 约等于 `2.95M QPS`
2. `+6%` 约等于 `3.01M QPS`
3. `+8%` 约等于 `3.07M QPS`

这里的前提是：

1. backlog copy 没有放大
2. payload pool 容量不成为新 backpressure 点
3. `payload_local_slice` 仍是主热点时，收益上限会受 TLC load 成本限制

## 14. 实施顺序

建议按三步走，而不是一口气改完所有 transport 细节。

### Phase 1: Completion Ref

1. `completion_ring` 先改成传 `completion_ref`
2. 真实 completion metadata 放到 supernode 本地 pool
3. 暂时仍允许 payload 用旧 `zmalloc`

目的：

1. 先验证 completion ref 生命周期
2. 先把 ring copy 拿掉
3. 缩小改动面，先把 return ring 跑通

### Phase 2: Inline Payload Pool

1. `snapshot_vemb_payload()` 改成直接写 payload slot
2. proxy 不再 `zfree(inline_vector)`
3. close/backlog 路径统一走 `completion_return`

目的：

1. 去掉每请求 alloc/free
2. 把 payload 生命周期并入统一 owner/recycle

### Phase 3: Transport View Simplification

1. TCP transport 改成直接消费 slot view，而不是临时大 completion 数组
2. Aeron/SHM response 也收敛到同一套 completion view helper

目的：

1. 再减少一次本地 snapshot/copy
2. 让 completion 路径和 job path 的 ref 模型完全对齐

## 15. 风险与防御

### 15.1 Slot 泄漏

风险：

1. return ring 漏发
2. close 路径漏归还
3. ring full 时异常路径没清理

防御：

1. 每个 pool 维护 `free/reserved/published` 计数
2. 定期导出 `inflight completion slots`
3. 在 channel close 和 worker stop 时做 leak audit

### 15.2 Stale Ref

风险：

1. slot 很快复用
2. proxy 读到旧 ref

防御：

1. `generation` 必须进 ref
2. slot header 校验 `(slot_id, generation, state)`
3. mismatch 直接记 fatal counter 并丢弃

### 15.3 Payload Pool 容量不足

风险：

1. `VEMB_INLINE` 突发多时 payload slot 不够

防御：

1. payload pool 独立统计 `alloc_fail`
2. 先把返回降级成 `ERR`，不要 silent fallback 到 heap
3. benchmark 时重点关注高 pipeline、高连接数下的 payload slot 峰值

## 16. 落地后的新瓶颈判断

完成这轮后，最可能留下来的主瓶颈会是：

1. `payload_local_slice`
2. TCP `writev` + backlog copy
3. 1200B payload 本身的内存带宽与 socket 发送成本

也就是说，这轮优化能明显改善“返回路径的管理开销”，但它不会把真实 payload 复制和网络发送成本变没。  
如果后续还要继续抬 `mixed-80r20w` 的 TCP vector-inline 上限，更可能需要继续看：

1. payload load 是否还能减少一次 copy
2. backlog 是否能减少中间 buffer copy
3. `VEMB_INLINE` payload 是否能引入更强的零拷贝或页内引用模型

## 17. 结论

在 `job pool + slot_id` 已经落地之后，返回路径最值得做的不是“只把 completion 指针化”，而是一次把下面四件事串成完整模型：

1. `completion_ring` 传 `completion_ref`
2. `completion metadata` 进入 supernode 本地 pool
3. `inline payload` 进入独立 payload pool
4. proxy 通过 `completion_return` 把对象归还给 owner

这样做的好处是：

1. slot copy 继续缩小
2. 去掉每请求 payload `zmalloc/zfree`
3. backlog / close 生命周期更统一
4. 为下一步 transport view 和 payload copy 继续优化打下更干净的边界

这比“只改 completion 指针传递”更完整，也更值得作为下一阶段正式方案。
