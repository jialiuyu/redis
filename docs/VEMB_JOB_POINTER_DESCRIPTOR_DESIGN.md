# VEMB job 指针描述符瘦身设计

日期：2026-07-06

本文给出一版具体草图：将 `proxy -> SuperNode` 之间的 job 传递，从“整 slot 复制大结构体”改成“ring 传小 descriptor，真实 job 由 `proxy` 分配、`SuperNode` 消费后回收”。

## 1. 目标

当前 `job_shard_queue` 的 `slot_size` 统一按 `sizeof(vemb_v16_vadd_job_t)` 分配。即使只是 `vemb-handle`、`vemb-inline` 或 `vsim-key-key` 这类不携带 inline vector 的请求，也要按大 slot 做整块 `memcpy`。

目标是把线程间 ring 传输收敛成：

```text
proxy
  -> allocate job object
  -> fill job payload
  -> publish small descriptor to ring
supernode
  -> poll descriptor
  -> use job payload
  -> free / recycle job object
```

也就是说，ring 里传的是“小描述符”，而不是完整 job 结构体。

## 2. 总体思路

设计分成两层：

1. **真实 job 对象**
   由 `proxy` 按 op 分配，内容大小和布局按语义裁剪。
2. **ring descriptor**
   固定小结构，只负责在线程间转交 job 指针和少量元信息。

这样做后：

1. ring `slot_size` 可以从当前 `16712B` 级别降到十几或几十字节。
2. `vemb-handle` / `vemb-inline` / `vsim-key-key` 不再为 `vector[VEMB_V16_MAX_DIM]` 付复制成本。
3. 后续再引入 pool/slab 或 `slot-id`，语义边界也更清晰。

## 3. 结构草图

### 3.1 统一的小 descriptor

```c
typedef enum vemb_v16_job_kind {
    VEMB_V16_JOB_KIND_READ = 1,
    VEMB_V16_JOB_KIND_VSIM_KEY_KEY = 2,
    VEMB_V16_JOB_KIND_INLINE_VECTOR = 3,
} vemb_v16_job_kind_t;

typedef struct vemb_v16_job_desc {
    uint8_t kind;
    uint8_t op;
    uint16_t reserved0;
    uint32_t req_id;
    void *job_ptr;
} vemb_v16_job_desc_t;
```

说明：

- `kind` 用来决定 `job_ptr` 指向哪一种真实 job。
- `op` 保留在 descriptor 中，便于诊断日志和防御式校验。
- `req_id` 也保留在 descriptor 中，便于 ring 层面统计、日志和异常清理。
- `job_ptr` 是真正跨线程转移所有权的对象地址。

第一版也可以更简化，只保留：

```c
typedef struct vemb_v16_job_desc {
    void *job_ptr;
} vemb_v16_job_desc_t;
```

但从可观测性和 debug 成本看，带上 `kind/op/req_id` 会更稳。

### 3.2 真实 job 公共头

```c
typedef struct vemb_v16_job_hdr {
    uint8_t kind;
    uint8_t op;
    uint16_t flags;
    uint32_t req_id;
    uint32_t channel_index;
    uint64_t channel_id;
    uint64_t key_hash;
    uint64_t topology_epoch;
} vemb_v16_job_hdr_t;
```

这个 header 只保留所有 job 都需要的字段。

### 3.3 按语义拆分的真实 job

#### read job

```c
typedef struct vemb_v16_read_job {
    vemb_v16_job_hdr_t hdr;
    uint32_t key_len;
    uint32_t dim;
    uint32_t vector_bytes;
    char key[VEMB_V16_MAX_KEY_LEN];
} vemb_v16_read_job_t;
```

用于：

- `VEMB_V16_OP_VEMB_HANDLE`
- `VEMB_V16_OP_VEMB_INLINE`
- `VEMB_V16_OP_VREM`

#### key-key vsim job

```c
typedef struct vemb_v16_vsim_key_key_job {
    vemb_v16_job_hdr_t hdr;
    uint32_t key_len;
    uint32_t key2_len;
    uint32_t dim;
    uint32_t vector_bytes;
    uint64_t key2_hash;
    char key[VEMB_V16_MAX_KEY_LEN];
    char key2[VEMB_V16_MAX_KEY_LEN];
} vemb_v16_vsim_key_key_job_t;
```

用于：

- `VEMB_V16_OP_VSIM_KEY_KEY`

#### inline vector job

```c
typedef struct vemb_v16_inline_vector_job {
    vemb_v16_job_hdr_t hdr;
    uint32_t key_len;
    uint32_t dim;
    uint32_t vector_bytes;
    char key[VEMB_V16_MAX_KEY_LEN];
    float vector[VEMB_V16_MAX_DIM];
} vemb_v16_inline_vector_job_t;
```

用于：

- `VEMB_V16_OP_VADD`
- `VEMB_V16_OP_VSIM_INLINE`

## 4. 所有权规则

这条路径最重要的是对象所有权要非常明确。

### 4.1 成功发布前

真实 job 由 `proxy` 独占。

```text
proxy alloc -> fill -> try publish
```

如果 publish 失败：

- job 仍归 `proxy`
- `proxy` 负责释放或归还 pool

### 4.2 publish 成功后

所有权转交给 `SuperNode`。

```text
publish success
  -> ownership transferred
```

这之后：

- `proxy` 不得再访问该 job
- `SuperNode` 成为唯一 owner

### 4.3 消费完成后

`SuperNode` 负责回收：

- 第一版：直接 `zfree`
- 第二版：归还对应 job pool

## 5. 发布与消费伪代码

### 5.1 proxy 发布侧

```c
job = alloc_job_for_req(req);
if (!job)
    return error;

fill_job_from_req(job, req, ch);

vemb_v16_job_desc_t desc = {
    .kind = job->hdr.kind,
    .op = job->hdr.op,
    .req_id = job->hdr.req_id,
    .job_ptr = job,
};

if (vemb_v16_aeron_publish(&queue->ring, &desc) != 0) {
    free_job(job);
    return ring_full;
}

job = NULL; /* ownership transferred */
```

### 5.2 supernode 消费侧

```c
vemb_v16_job_desc_t desc;
if (vemb_v16_aeron_poll(&queue->ring, &desc) == 1) {
    vemb_v16_job_hdr_t *job = desc.job_ptr;
    handle_job_by_kind(ctx, job, scratch, ch);
    free_job(job);
}
```

## 6. free 如何做

### 6.1 第一阶段：先用 `zmalloc/zfree`

第一阶段的目标是先验证语义闭环，而不是一步做到 allocator 最优。

优点：

1. 改动集中。
2. 易于排查所有权错误。
3. 便于先验证 ring descriptor 方案本身是否稳定。

风险：

1. 高频路径上会有分配/释放成本。
2. 可能出现跨线程 free 带来的 allocator 抖动。

### 6.2 第二阶段：改成按类型 pool/slab

更稳的长期方向是：

- `read_job_pool`
- `vsim_key_key_job_pool`
- `inline_vector_job_pool`

接口上建议保持统一：

```c
void *vemb_v16_job_alloc(vemb_v16_job_kind_t kind);
void vemb_v16_job_free(vemb_v16_job_hdr_t *job);
```

这样第一阶段用 `zmalloc/zfree`，第二阶段切到 pool 时，上层流程可以不变。

## 7. shutdown 与异常回收

指针模型一定要补齐停机和异常路径。

### 7.1 publish 失败

- `proxy` 立即回收 job

### 7.2 channel 失活但 job 已入 ring

- `SuperNode` poll 到 job 后，先校验 `channel_index` 和 `channel_id`
- 若 channel 已失效，不做业务执行，直接回收 job

### 7.3 worker 停止

停机顺序建议是：

1. 停止新 publish
2. worker 持续 drain ring
3. 回收 ring 中残留 job
4. 最后销毁 pool / queue

否则很容易泄漏“已发布但未消费”的 job。

## 8. 对当前代码的最小改造路径

建议按下面顺序推进，而不是一次性重做全部路径。

### 阶段 A：只替换 ring payload

1. 保留现有 `apply_unified_shard_job()` 调度框架。
2. 新增 `vemb_v16_job_desc_t`。
3. `job_shard_queue` 的 `slot_size` 改成 `sizeof(vemb_v16_job_desc_t)`。
4. `publish_request_job()` 改成“先分配真实 job，再发布 descriptor”。
5. `drain_shard_queues()` 改成先 poll descriptor，再解引用 `job_ptr`。

这是收益最大、风险也最可控的一步。

### 阶段 B：按 job kind 拆 handler

1. `handle_read_job()`
2. `handle_vsim_key_key_job()`
3. `handle_inline_vector_job()`

这样可以逐步摆脱现在基于统一大结构体的调度习惯。

### 阶段 C：引入 pool

当 pointer handoff 语义稳定后，再把 `zmalloc/zfree` 换成按类型 pool/slab。

## 9. 与 slot-id 方案的关系

这套 pointer descriptor 方案和 `slot-id` 方案并不冲突。

可以把它理解成两阶段：

1. **先从“大结构体 copy”变成“指针 descriptor”**
2. **再从“堆对象指针”演进到“pool slot-id”**

第一阶段先解决：

- ring `slot_size` 过大
- 读请求为 `vector[]` 付无谓 copy 成本

第二阶段再解决：

- allocator 抖动
- 跨线程 free 成本
- 生命周期可控性

## 10. 结论

如果采用“`proxy` 创建 job，`SuperNode` 使用完后 free”的路线，最重要的不是语法，而是三件事：

1. ring 里传小 descriptor，而不再传整 job。
2. publish 成功前后要有严格的所有权切换规则。
3. 第一阶段先用统一 `alloc/free` 跑通，第二阶段再切到 pool/slab。

这条路径的直接收益是：把当前 `job_shard_queue` 上按 `16712B` 整 slot 搬运的模式，收敛成按十几或几十字节 descriptor 传递，再把真实 payload 的成本留在真正需要的 op 上承担。
