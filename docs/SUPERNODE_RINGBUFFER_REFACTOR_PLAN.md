# SuperNode Ring Buffer 重构方案

## 背景

当前 SuperNode 的 ring buffer 模型是：

1. 每个 supernode 只有一个 `input_rb`
2. 多个 worker 共享消费同一个 ring buffer
3. 写入侧使用 `write_mutex`
4. packet 会经历额外的序列化和拷贝

这套设计的问题不只是性能，还包括并发模型不够清晰：当前 `ring_buffer_pop()` 不是标准多消费者安全实现，但实际上又被多个 worker 同时消费。

## 重构目标

1. 把共享消费队列改成 `每个 worker 一个专属 queue`
2. 把当前带 `write_mutex` 的实现改成真正适合单生产者单消费者的 `SPSC ring buffer`
3. 降低 head/tail 的 cache line 竞争
4. 为后续零拷贝和 packet 瘦身打基础

## 目标架构

新的数据流建议改成：

`Proxy -> supernode_id -> worker_id -> worker 专属 ring buffer -> worker 处理`

具体规则：

1. 先通过一致性哈希确定 `supernode_id`
2. 再根据 `key_hash % num_workers` 计算 `worker_id`
3. 请求按 `(supernode_id, worker_id)` 进入对应 batch bucket
4. flush 后写入对应 worker 的专属 ring buffer
5. 每个 worker 只消费自己的 queue

这样每个 queue 都是单生产者单消费者模型，便于做真正无锁化。

## 需要调整的核心结构

### `proxy_aggregator_t`

当前是：

- 每个 supernode 一个 bucket
- 每个 supernode 一个 ring buffer

建议改成：

- 每个 `(supernode, worker)` 一个 bucket
- 每个 `(supernode, worker)` 一个 ring buffer

也就是逻辑上从一维变成二维：

- `supernode_id`
- `worker_id`

### `batch_bucket_t`

建议新增字段：

- `target_worker_id`

这样 flush 时能直接定位到目标 queue。

### `batch_packet_t`

建议新增字段：

- `worker_id`

这样 worker 端和调试日志里都能明确知道数据包目标归属。

### `supernode_t`

当前只有：

- 一个 `input_rb`

建议改成：

- `input_rbs[num_workers]`

每个 worker 在初始化时绑定自己的 queue。

## Ring Buffer 设计建议

### 第一阶段：保留变长消息协议

先不要一次性改太大。保留当前：

- `len + payload`
- `batch_packet_t` 变长消息

只把并发模型改成 SPSC。

### 第二阶段：SPSC ring buffer

SPSC 的基本规则：

1. producer 只写 `tail`
2. consumer 只写 `head`
3. 双方只读取对方指针
4. 去掉 `write_mutex`

推荐的结构布局：

```c
typedef struct ring_buffer {
    _Alignas(64) atomic_uint_fast64_t head;
    char pad1[64 - sizeof(atomic_uint_fast64_t)];
    _Alignas(64) atomic_uint_fast64_t tail;
    char pad2[64 - sizeof(atomic_uint_fast64_t)];
    uint8_t *buffer;
    size_t size;
    int fd;
} ring_buffer_t;
```

这样可以避免 `head` 和 `tail` 落在同一个 cache line 上。

## 具体改动点

### `src/proxy_aggregator.h`

需要改：

1. `batch_packet_t` 增加 `worker_id`
2. `batch_bucket_t` 增加 `target_worker_id`
3. `proxy_aggregator_t` 的 bucket/ring buffer 组织方式改成 `(supernode, worker)` 维度
4. `ring_buffer_t` 去掉 `write_mutex`，并做 cache line 对齐

### `src/proxy_aggregator.c`

需要改：

1. `proxy_aggregator_init()`
   创建每个 worker 专属 ring buffer
2. `proxy_enqueue_request()`
   在 `supernode_id` 之外计算 `worker_id`
3. `serialize_batch_for_sve()`
   写入 `worker_id`
4. `flush_thread_func()`
   按 `(supernode, worker)` 刷 bucket
5. `ring_buffer_push()/pop()`
   改成 SPSC 实现

### `src/supernode_worker.c`

需要改：

1. `supernode_t` 从单个 `input_rb` 改成 `input_rbs[]`
2. `supernode_init()`
   给每个 worker 创建并绑定自己的 ring buffer
3. `sve_worker_thread()`
   只消费自己的 queue

## 分阶段落地建议

### 阶段 1：修正队列拓扑

目标：

- 每个 worker 一个 queue
- 每个 worker 只读自己的 queue

先不改 bitmap，不改 packet 结构大小，不改零拷贝。

这是收益最大、风险最低的一步。

### 阶段 2：改成真正 SPSC

目标：

- 去掉 `write_mutex`
- 清理读写路径中的共享竞争

完成后 ring buffer 才真正具备高性能优化基础。

### 阶段 3：减少拷贝

目标：

- producer 直接在 ring buffer 中构造 packet
- consumer 直接读取 ring buffer 中的数据

这一步再逐步减少 `zmalloc + memcpy + memcpy` 这种双重拷贝路径。

### 阶段 4：瘦身 packet

如果 worker 侧只需要：

- `request_id`
- `key_hash`

那就删掉或弱化 `key_data[256]`，减少 ring buffer 带宽占用。

### 阶段 5：通知机制优化

当前是：

- flush 线程 `usleep(50)`
- worker 空队列 `usleep(10)`

后续可改成：

- 轻量自旋 + 退避
- `eventfd`
- `futex`
- 条件变量

## 预期收益

1. 消除多个 worker 竞争同一个 queue 的不清晰并发模型
2. 去掉写路径上的 `write_mutex`
3. 减少 head/tail cache line 抖动
4. 降低额外 memcpy 和 malloc/free 成本
5. 为后续 worker 局部化、shard 路由、甚至 per-worker bitmap 方案打基础

## 最终建议

最值得优先做的不是微调当前 ring buffer 代码，而是先把架构改成：

1. `每个 worker 一个 ring buffer`
2. `每个 worker 固定消费自己的 queue`
3. `ring buffer 改成 SPSC`

在这三步完成之前，继续在当前“共享 queue + 多 worker 消费 + write_mutex”模型上做细节优化，收益会比较有限。 

## 第一阶段具体改动清单

第一阶段只做一件事：把队列拓扑改对。

当前状态：

- 已开始落代码
- `proxy` 侧已按 `(supernode, worker)` 维度组织 bucket 和 ring buffer
- `supernode` 侧已切到 `per-worker input_rbs[]`
- 为了适配当前一体化进程模型，额外加入了进程内 ring buffer registry / refcount 复用逻辑

这一阶段的原则是：

1. 先实现 `per-worker queue`
2. 先保证每个 worker 只消费自己的 queue
3. 暂时不动 bitmap 逻辑
4. 暂时不做零拷贝
5. 暂时不重写 `ring_buffer_push/pop` 内部协议

### 第一阶段目标

改造完成后，系统应该从：

`1 supernode -> 1 input_rb -> N workers 共享消费`

变成：

`1 supernode -> N input_rb -> N workers 一一对应消费`

### 第一阶段涉及文件

1. `src/proxy_aggregator.h`
2. `src/proxy_aggregator.c`
3. `src/supernode_worker.h`
4. `src/supernode_worker.c`

### `src/proxy_aggregator.h` 具体改动

建议新增或调整以下字段。

#### `batch_bucket_t`

新增：

```c
int target_worker_id;
```

用途：

- flush 时直接定位目标 worker queue

#### `batch_packet_t`

新增：

```c
uint32_t worker_id;
```

建议放在 `supernode_id` 附近，保持路由信息集中。

#### `proxy_aggregator_t`

新增：

```c
size_t workers_per_supernode;
size_t total_worker_queues;
```

当前：

```c
ring_buffer_t **ring_buffers;
size_t num_ring_buffers;
```

建议继续保留，但语义改为：

- `ring_buffers` 不再是 “每个 supernode 一个”
- 改为 “每个 (supernode, worker) 一个”

也就是：

```c
index = supernode_id * workers_per_supernode + worker_id
```

建议再补一个 helper 宏或内联函数：

```c
static inline size_t worker_queue_index(size_t workers_per_supernode,
                                        int supernode_id,
                                        int worker_id);
```

这样后续代码不会到处重复手写 flatten 逻辑。

### `src/proxy_aggregator.c` 具体改动

#### 1. `proxy_aggregator_init()`

当前逻辑：

- `num_buckets = num_supernodes`
- `num_ring_buffers = num_supernodes`

第一阶段改成：

- `workers_per_supernode = server.supernode_workers`
- `num_buckets = num_supernodes * workers_per_supernode`
- `num_ring_buffers = num_supernodes * workers_per_supernode`

创建 bucket 时要同时设置：

```c
bucket->target_supernode_id = supernode_id;
bucket->target_worker_id = worker_id;
```

创建 ring buffer 时，名字也要变成 per-worker，例如：

```c
supernode_%d_worker_%d
```

#### 2. `proxy_enqueue_request()`

当前只算：

```c
supernode_id = consistent_hash_get_node(...)
```

第一阶段新增：

```c
uint32_t key_hash = murmur3_hash(key, strlen(key));
worker_id = key_hash % workers_per_supernode;
bucket_index = supernode_id * workers_per_supernode + worker_id;
```

注意：

- 这里最好只算一次 `key_hash`
- 后面 `serialize_batch_for_sve()` 直接复用，避免重复 hash

如果要进一步收敛，可以把 `key_hash` 存进 `proxy_request_t`。

#### 3. `proxy_request_t`

建议新增：

```c
uint32_t key_hash;
int target_worker_id;
int target_supernode_id;
```

这样 flush 和序列化阶段就不需要重新计算。

#### 4. `serialize_batch_for_sve()`

当前会写：

- `supernode_id`
- `request_id`
- `key_hash`

第一阶段还应补上：

- `worker_id`

并优先直接使用 `req->key_hash`，不要重复调用 `murmur3_hash()`。

#### 5. `flush_thread_func()`

当前遍历逻辑是按 supernode bucket 遍历。

第一阶段改完后，遍历粒度变成 `(supernode, worker)` bucket。

关键变化：

```c
ring_buffer_t *rb = agg->ring_buffers[bucket_index];
```

而不是：

```c
agg->ring_buffers[bucket->target_supernode_id]
```

### `src/supernode_worker.h` 具体改动

这一阶段可以只保留现有接口，不一定要改头文件 API。

如果想让意图更清晰，可以在注释里明确：

- `input_rb` 是 worker 私有 queue

不一定需要新增字段。

### `src/supernode_worker.c` 具体改动

#### 1. `supernode_t`

当前：

```c
ring_buffer_t *input_rb;
```

改成：

```c
ring_buffer_t **input_rbs;
```

语义：

- `input_rbs[i]` 对应 worker `i`

#### 2. `supernode_init()`

初始化时：

1. 分配 `input_rbs[num_workers]`
2. 为每个 worker 创建自己的 ring buffer
3. 命名保持与 proxy 端一致，例如：

```c
supernode_%d_worker_%d
```

#### 3. worker 绑定 queue

当前：

```c
ctx->input_rb = global_supernode->input_rb;
```

第一阶段改成：

```c
ctx->input_rb = global_supernode->input_rbs[i];
```

这样每个 worker 天然只消费自己的 queue。

### 第一阶段不改的内容

明确列出来，避免范围膨胀：

1. 不改 `bitmap` 结构
2. 不改 `sve_serial_contiguous_read()` 的 bitmap 获取逻辑
3. 不改 packet 内部请求格式
4. 不做 in-place 序列化
5. 不做 `eventfd/futex`
6. 不去掉 `write_mutex`

这些都放到后续阶段。

## 第一阶段伪代码

### proxy 侧

```c
supernode_id = consistent_hash_get_node(...);
key_hash = murmur3_hash(key, strlen(key));
worker_id = key_hash % workers_per_supernode;
bucket_index = supernode_id * workers_per_supernode + worker_id;
bucket = &agg->buckets[bucket_index];
enqueue(bucket, req);
```

### supernode 侧

```c
for (i = 0; i < num_workers; i++) {
    input_rbs[i] = ring_buffer_create(... supernode_id, i ...);
    workers[i].input_rb = input_rbs[i];
}
```

## 第一阶段验收标准

完成后至少要满足下面几点：

1. 每个 worker 只从自己的 queue 读取
2. proxy 能稳定把同一 `(supernode, worker)` 路由到固定 queue
3. 不再存在多个 worker 同时调用同一个 `ring_buffer_pop()` 的路径
4. 现有 batch flush 和 worker 处理流程功能不回退
5. 现有 bitmap 行为保持不变

## 第一阶段完成后的直接收益

1. 修正当前共享消费队列的并发模型
2. 为下一步改成 SPSC ring buffer 清空障碍
3. 为后续 per-worker 局部化调度打基础
4. 能更真实地逼近 `Partitioned` 场景，而不是所有 worker 混抢一个输入队列

## 第二阶段 SPSC Ring Buffer 接口设计

第二阶段的目标是：在第一阶段 `per-worker queue` 已经稳定运行的前提下，把当前带 `write_mutex` 的 ring buffer 改成真正的 `SPSC` 版本。

当前状态：

- 已开始落代码
- `write_mutex` 已从 ring buffer 结构和 `push/pop` 路径中移除
- `head/tail` 已切成分离 cache line 的 SPSC 布局
- `push/pop` 仍保留现有 `len + payload` 协议
- 目前仍属于“代码已改、待完整工程编译验证”的阶段

这一阶段仍然遵守两个原则：

1. 不同时做零拷贝大改
2. 先保留“变长消息 + 长度头”的协议

也就是说，第二阶段主要解决的是：

- 正确的单生产者单消费者语义
- 去掉 `write_mutex`
- 减少 head/tail 的共享竞争

### 第二阶段目标

第二阶段完成后，ring buffer 应满足：

1. 一个 queue 只有一个 producer
2. 一个 queue 只有一个 consumer
3. producer 只写 `tail`
4. consumer 只写 `head`
5. 双方都只读对方指针
6. 不再使用 `pthread_mutex_t write_mutex`

## 建议的数据结构

### 新版 `ring_buffer_t`

建议改成下面这种布局：

```c
typedef struct ring_buffer {
    _Alignas(64) atomic_uint_fast64_t head;
    char pad1[64 - sizeof(atomic_uint_fast64_t)];

    _Alignas(64) atomic_uint_fast64_t tail;
    char pad2[64 - sizeof(atomic_uint_fast64_t)];

    uint8_t *buffer;
    size_t size;
    int fd;
} ring_buffer_t;
```

设计意图：

1. `head` 和 `tail` 分开到不同 cache line
2. consumer 高频更新 `head`，producer 高频更新 `tail`
3. 减少 false sharing

### 语义约束

严格约定：

- `head` 只能由 consumer 更新
- `tail` 只能由 producer 更新
- producer 绝不写 `head`
- consumer 绝不写 `tail`

这是 SPSC 简化内存序和去锁的前提。

## 协议设计

### 保留变长消息协议

继续使用：

```text
[u32 len][payload bytes]
```

其中：

- `len` 是消息体长度
- `payload` 是 `batch_packet_t`

这样做的原因是：

1. 与第一阶段兼容
2. 不需要立刻重写 `batch_packet_t`
3. 不需要立刻做 slot 固定化

### 预留空间规则

producer 写入前：

1. 先读取 `head`
2. 计算剩余可写空间
3. 若不足，返回 `C_ERR`
4. 若足够，按当前 `tail` 位置写入长度头和 payload
5. 全部写完后，再一次性发布新的 `tail`

这里最关键的是：

- **尾指针只能在消息完全写完后更新**

这样 consumer 不会读到半包。

## 接口建议

### 基础接口

建议保留现有 API 名称，内部改成 SPSC 语义：

```c
ring_buffer_t *ring_buffer_create(size_t size, const char *name);
void ring_buffer_destroy(ring_buffer_t *rb);

int ring_buffer_push(ring_buffer_t *rb, const void *data, size_t len);
int ring_buffer_pop(ring_buffer_t *rb, void *data, size_t max_len, size_t *actual_len);

size_t ring_buffer_available_space(ring_buffer_t *rb);
size_t ring_buffer_available_data(ring_buffer_t *rb);
```

这样可以减少调用层改动。

### 可选扩展接口

为后续零拷贝做准备，可以先设计但不一定立刻实现：

```c
int ring_buffer_reserve(ring_buffer_t *rb, size_t len, void **ptr, size_t *contig_len);
int ring_buffer_commit_write(ring_buffer_t *rb, size_t len);

int ring_buffer_peek(ring_buffer_t *rb, void **ptr, size_t *len);
int ring_buffer_commit_read(ring_buffer_t *rb, size_t len);
```

这套接口可以支持第三阶段的“原地构造 packet / 原地消费 packet”。

## 内存序建议

### producer 侧

建议：

1. 读取 `head` 时使用 `memory_order_acquire`
2. 更新 `tail` 时使用 `memory_order_release`

伪代码：

```c
head = atomic_load_explicit(&rb->head, memory_order_acquire);
tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);

// 写入 payload
memcpy(...)

atomic_store_explicit(&rb->tail, new_tail, memory_order_release);
```

### consumer 侧

建议：

1. 读取 `tail` 时使用 `memory_order_acquire`
2. 更新 `head` 时使用 `memory_order_release`

伪代码：

```c
tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
head = atomic_load_explicit(&rb->head, memory_order_relaxed);

// 读取 payload
memcpy(...)

atomic_store_explicit(&rb->head, new_head, memory_order_release);
```

这样可以保证：

- producer 发布 `tail` 之前写入的数据对 consumer 可见
- consumer 推进 `head` 之前完成当前包读取

## wrap-around 处理建议

当前实现是直接允许“长度头”和“payload”跨边界拆写，这个思路可以继续保留。

第二阶段不必为了 SPSC 去强制引入“padding record”。

建议仍然支持：

1. 长度头跨边界写入
2. payload 跨边界写入

原因：

- 对当前代码侵入最小
- 逻辑已经存在，只需要把锁移除并改成单 producer / 单 consumer 前提

后续如果要进一步简化实现，再考虑用：

- padding marker
- 固定 slot
- page-aligned segment

## 第二阶段需要修改的函数

### `ring_buffer_create()`

要改：

1. `head/tail` 初始化改用 `atomic_init`
2. 去掉 `write_mutex` 初始化

### `ring_buffer_destroy()`

要改：

1. 去掉 `pthread_mutex_destroy(&rb->write_mutex)`

### `ring_buffer_available_space()`

要改：

1. 明确这是 producer 视角的函数
2. 使用 `head acquire + tail relaxed`

### `ring_buffer_available_data()`

要改：

1. 明确这是 consumer 视角的函数
2. 使用 `tail acquire + head relaxed`

### `ring_buffer_push()`

要改：

1. 删除 `pthread_mutex_lock/unlock`
2. 保持“先写数据，再发布 tail”
3. 只依赖单 producer 前提，不再做多 writer 保护

### `ring_buffer_pop()`

要改：

1. 保持“先读 tail，再读 payload，再推进 head”
2. 只依赖单 consumer 前提

## 第二阶段伪代码

### producer push

```c
int ring_buffer_push(rb, data, len) {
    head = load_acquire(rb->head);
    tail = load_relaxed(rb->tail);

    if (space(head, tail) < len + 4) return C_ERR;

    write_u32_len(rb, tail, len);
    write_payload(rb, tail + 4, data, len);

    store_release(rb->tail, tail + 4 + len);
    return C_OK;
}
```

### consumer pop

```c
int ring_buffer_pop(rb, out, max_len, actual_len) {
    tail = load_acquire(rb->tail);
    head = load_relaxed(rb->head);

    if (data_size(head, tail) < 4) return C_ERR;

    len = read_u32_len(rb, head);
    if (len > max_len) return C_ERR;
    if (data_size(head, tail) < 4 + len) return C_ERR;

    read_payload(rb, head + 4, out, len);
    store_release(rb->head, head + 4 + len);
    *actual_len = len;
    return C_OK;
}
```

## 第二阶段验收标准

1. `write_mutex` 完全移除
2. 每个 queue 的 producer/consumer 关系清晰且固定
3. 压测下不再出现多 worker 消费同一 queue 的路径
4. 与第一阶段相比功能不回退
5. push/pop 在 wrap-around 场景下仍正确
6. packet 不会出现半包、脏读或覆盖未消费数据

## 第二阶段完成后的收益

1. 写路径去锁
2. 减少 `head/tail` 的 cache line 争用
3. 使 ring buffer 真正符合单 producer / 单 consumer 的高性能模型
4. 为第三阶段零拷贝接口铺路

## 第三阶段零拷贝接口设计

第三阶段的目标不是再改变队列拓扑，而是减少当前消息路径里的额外内存分配和双重拷贝。

当前状态：

- 已开始落代码
- producer 侧已经拆出 `wire size + fill packet`
- worker 侧已经切到 `peek + commit_read`
- 写侧已经开始走“原地填充 packet”路径
- 已补 `padding record`
- 当前版本支持“连续 payload 优先 + 尾部 padding 回绕”
- 仍未支持“单条 payload 跨边界拆分”的零拷贝消息

当前路径大致是：

1. `serialize_batch_for_sve()` 先 `zmalloc` 一个 `batch_packet_t`
2. `ring_buffer_push()` 再把这个 packet `memcpy` 到 ring buffer
3. worker 端 `ring_buffer_pop()` 再把 packet `memcpy` 到本地栈 buffer
4. worker 再从本地 buffer 解析 packet

第三阶段要解决的就是这条链路里的：

- `malloc/free`
- producer 侧额外 copy
- consumer 侧额外 copy

## 第三阶段目标

完成后，目标路径应变成：

1. producer 直接在 ring buffer 中预留写入空间
2. producer 在 ring buffer 内原地构造 `batch_packet_t`
3. consumer 直接获得 ring buffer 内 payload 指针
4. consumer 处理完成后再提交读取进度

也就是：

`serialize to heap + copy + copy`

变成：

`reserve in ring -> in-place fill -> peek -> consume -> commit`

## 设计原则

1. 第三阶段依赖第一阶段和第二阶段已经完成
2. 只有在 `per-worker queue + SPSC ring buffer` 成立后，零拷贝接口才清晰
3. 不要求一步到位支持所有复杂情况，先支持单消息原地写入和原地消费
4. 先让 producer 不再 `zmalloc packet`
5. 再让 consumer 不再拷贝到本地 `buffer`

## 建议接口

### producer 侧

建议增加：

```c
int ring_buffer_reserve(ring_buffer_t *rb, size_t len, void **ptr, size_t *contig_len);
int ring_buffer_commit_write(ring_buffer_t *rb, size_t len);
void ring_buffer_cancel_write(ring_buffer_t *rb);
```

语义：

- `reserve`
  在 ring buffer 里预留一段即将写入的空间
- `commit_write`
  写入完成后推进 `tail`
- `cancel_write`
  如果中途失败，放弃这次预留

### consumer 侧

建议增加：

```c
int ring_buffer_peek(ring_buffer_t *rb, void **ptr, size_t *len);
int ring_buffer_commit_read(ring_buffer_t *rb, size_t len);
```

语义：

- `peek`
  不拷贝，只返回当前消息体指针和长度
- `commit_read`
  处理完成后推进 `head`

## 推荐的消息布局

第三阶段仍建议保留：

```text
[u32 len][payload]
```

但是 producer 不再先构造堆上的 packet，而是：

1. 计算 `payload_len`
2. 向 ring buffer 预留 `sizeof(u32) + payload_len`
3. 直接在返回区域里写 `len`
4. 直接在后续区域里构造 `batch_packet_t`
5. 写完后 `commit_write`

consumer 端：

1. `peek` 读取当前消息起始位置
2. 直接拿到 `payload` 指针
3. 把它视为 `batch_packet_t *`
4. 处理完成后 `commit_read`

## producer 侧设计

### reserve 语义

`ring_buffer_reserve()` 建议满足：

1. 如果当前剩余空间不足，返回 `C_ERR`
2. 如果可写区域是连续的，直接返回连续指针
3. 如果尾部剩余不够但头部空间够，允许通过 wrap-around 返回起始处指针
4. 在 `commit_write()` 前，不发布新 `tail`

### contig_len

`contig_len` 的作用是告诉调用方当前返回的是不是连续足够大的线性区域。

建议分两种策略：

#### 方案 A：简化版

要求一条消息必须能落在连续区域内。

优点：

- 接口简单
- 实现清晰
- 更容易原地构造 packet

缺点：

- 尾部剩余碎片可能暂时浪费

#### 方案 B：支持跨边界

允许一条消息拆成尾部和头部两段。

优点：

- 空间利用更高

缺点：

- producer 原地构造变复杂
- consumer `peek` 也会复杂

**建议第三阶段优先采用方案 A。**

也就是：

- 如果当前尾部连续空间不足以容纳整条消息
- 就写一个 padding record，回绕到 buffer 起始位置

这样能大幅降低接口复杂度。

## padding record 设计

如果采用“消息必须连续”的策略，建议引入 padding record。

格式建议：

```text
[u32 len = 0]
```

语义：

- 表示本轮写入结束，consumer 读到后直接把 `head` 回绕到 0

好处：

1. 原地构造 packet 很简单
2. consumer `peek` 总能拿到连续 payload
3. 为后续固定 slot 或更高级协议保留空间

## consumer 侧设计

### peek 语义

`ring_buffer_peek()` 建议满足：

1. 如果没有完整消息，返回 `C_ERR`
2. 如果读到 `padding record`，自动回绕再继续看下一条
3. 返回的 `ptr` 指向 payload 起始位置，不包含长度头
4. `len` 返回 payload 长度

### commit_read 语义

consumer 完成处理后：

```c
ring_buffer_commit_read(rb, sizeof(u32) + payload_len);
```

或者更进一步：

```c
ring_buffer_commit_read(rb, payload_len);
```

由内部统一加上长度头。

为了接口更直观，建议第二种：

- API 参数只传 payload 长度
- 内部自动处理 header 长度

## 和现有 `batch_packet_t` 的衔接

### 当前路径

当前：

```c
packet = serialize_batch_for_sve(bucket);
ring_buffer_push(rb, packet, packet->packet_size);
```

### 第三阶段建议路径

建议把 `serialize_batch_for_sve()` 拆成两层：

```c
size_t batch_packet_wire_size(batch_bucket_t *bucket);
int fill_batch_packet(batch_bucket_t *bucket, batch_packet_t *packet);
```

然后 flush 路径改成：

```c
payload_len = batch_packet_wire_size(bucket);
reserve(rb, sizeof(u32) + payload_len, &ptr, &contig_len);
write_u32_len(ptr, payload_len);
packet = (batch_packet_t *)((uint8_t *)ptr + sizeof(u32));
fill_batch_packet(bucket, packet);
commit_write(rb, sizeof(u32) + payload_len);
```

这样可以完全取消中间堆对象 `packet`。

## worker 侧改法

当前 worker 线程里有：

```c
uint8_t buffer[RING_BUFFER_BATCH_SIZE];
ring_buffer_pop(..., buffer, ..., &actual_len);
sve_worker_process_batch(ctx, (batch_packet_t *)buffer);
```

第三阶段建议改成：

```c
void *payload = NULL;
size_t payload_len = 0;

if (ring_buffer_peek(ctx->input_rb, &payload, &payload_len) == C_OK) {
    sve_worker_process_batch(ctx, (batch_packet_t *)payload);
    ring_buffer_commit_read(ctx->input_rb, payload_len);
}
```

这样：

- 不再把 packet 拷贝到本地栈 buffer
- worker 直接消费 ring buffer 内数据

## API 边界条件

第三阶段实现时，接口必须明确下面这些边界行为：

1. `reserve` 后如果 fill 失败，是否允许 cancel
2. `peek` 后如果处理失败，是否允许不提交读取
3. worker 处理期间，payload 指针必须保持有效
4. producer 在 consumer 提交前不能覆盖该消息区域
5. padding record 是否对外可见，还是内部自动吞掉

建议：

1. 支持 `cancel_write`
2. `peek` 后如果处理失败，不推进 `head`
3. padding record 对调用方透明

## 第三阶段验收标准

1. producer 路径不再为每个 batch `zmalloc packet`
2. `ring_buffer_push()` 不再承担消息体 memcpy 职责
3. worker 路径不再把 packet 复制到本地大 buffer
4. `sve_worker_process_batch()` 能直接处理 ring buffer 内 payload
5. wrap-around + padding record 场景下仍能正确读写
6. 功能正确，且与第二阶段相比无行为回退

## 第三阶段预期收益

1. 减少 producer 侧一次堆分配和一次 memcpy
2. 减少 consumer 侧一次 memcpy
3. 降低 batch packet 的端到端搬运成本
4. 让 ring buffer 更接近真正的高吞吐消息通道

## 第三阶段后的下一步

第三阶段完成后，再考虑第四阶段会更自然：

1. 瘦身 `batch_packet_t`
2. 删除无用字段或大块 `key_data`
3. 减少 payload 体积
4. 进一步提升 queue 有效吞吐

## 第四阶段 Packet 瘦身

当前状态：

- 已开始落代码
- `batch_packet_t` 中未被 worker 使用的 `key_len` 和 `key_data[256]` 已移除
- `proxy_request_t` 中排队阶段不再保存原始 key 字符串
- 当前 worker 路径只保留 `request_id + key_hash` 作为必要传输字段

### 当前收益

1. 显著缩小每条 request 在 batch packet 中的体积
2. 减少 queue 带宽占用
3. 减少 producer 侧无意义的数据搬运
4. 使零拷贝路径的收益更明显

### 当前边界

1. 这一步默认 worker 侧不再需要原始 key 文本
2. 如果后续业务逻辑需要在 worker 侧恢复原始 key，则必须引入额外 side channel 或单独 payload 扩展

## 定向验证

当前状态：

- 已补一个 standalone 的 ring buffer 定向 UT：
  [ring_buffer_ut.c](/Users/szza/codespace/work/hpc-redis/benchmark/ring_buffer_ut.c)
- 已接入 benchmark 构建：
  `make -C benchmark ring_buffer_ut`
- 已完成实际运行验证

当前覆盖的验证点：

1. 基本 `push/pop`
2. `peek + commit_read` 零拷贝读路径
3. `padding record` 回绕路径

当前结果：

- `ring_buffer_ut: all tests passed`

## 集成压测

当前状态：

- 已补一个更接近真实路径的 standalone batch/worker 集成压测：
  [ring_buffer_batch_bench.c](/Users/szza/codespace/work/hpc-redis/benchmark/ring_buffer_batch_bench.c)
- 已接入 benchmark 构建：
  `make -C benchmark ring_buffer_batch_bench`
- 已完成实际运行验证

当前覆盖的验证点：

1. `per-worker queue` 路由
2. batch packet 原地写入
3. worker 侧 `peek + commit_read`
4. padding 回绕在批量消息下的稳定性

当前结果：

- workers: `4`
- batches: `50000`
- requests per batch: `32`
- consumed batches: `50000`
- consumed requests: `1600000`
- total time: `8.820 ms`
- batch throughput: `5.67 M batch/s`
- request throughput: `181.41 M req/s`
