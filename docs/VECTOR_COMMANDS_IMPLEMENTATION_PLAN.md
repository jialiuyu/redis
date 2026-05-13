# VEMB / VSIM / VADD 实现方案

本文档给出基于当前仓库现状实现 `VEMB`、`VSIM`、`VADD` 的落地方案。

目标不是重新设计 Redis 的 RESP 解析，而是在现有模块命令入口之上，把
UB 引擎、Proxy 聚合层、SuperNode 批处理层真正接通，并明确哪些语义需要
先收敛、哪些能力可以后置。

## 1. 先说结论

如果要让 `VEMB`、`VSIM`、`VADD` 可用，正确的入口层级是：

```text
RESP
  -> Redis Module Command
  -> vset.c 中的 VEMB_RedisCommand / VSIM_RedisCommand / VADD_RedisCommand
  -> UB engine 或 Proxy 聚合路径
  -> SuperNode Worker
  -> 结果回到模块命令层
  -> RedisModule_Reply*
```

不应该从 Redis 核心的 `networking.c` 或 RESP parser 改起。原因是：

- 三个命令已经在模块层注册完成。
- 命令参数解析已经在 `modules/vector-sets/vset.c` 中实现。
- 当前缺失的是 UB 语义、异步批量执行、回包链路，而不是 RESP 入口。

## 2. 当前代码现状

### 2.1 命令入口已存在

以下命令入口已经实现：

- `VADD_RedisCommand()`
  文件: `modules/vector-sets/vset.c`
- `VSIM_RedisCommand()`
  文件: `modules/vector-sets/vset.c`
- `VEMB_RedisCommand()`
  文件: `modules/vector-sets/vset.c`

并且这三个命令都已经有 UB dispatch：

- `VADD` 会调用 `ve->vadd`
- `VSIM` 会调用 `ve->vsim`
- `VEMB` 会调用 `ve->vemb`

### 2.2 UB engine 已有雏形

`src/vector_engine_ub_impl.c` 已实现：

- `ub_engine_vadd()`
- `ub_engine_vsim()`
- `ub_engine_vemb()`
- `ub_engine_vrem()`
- `ub_engine_vcard()`
- `ub_engine_vdim()`

但它目前只是一个“裸 UB 向量表访问层”，还不是完整的 vector-set 实现。

### 2.3 Proxy 聚合层和 SuperNode 层只有骨架

当前已有这些组件：

- `src/proxy_aggregator.c`
- `src/proxy_batch_bucket.c`
- `src/proxy_flush_executor.c`
- `src/supernode_protocol.h`
- `src/supernode_worker.c`

但它们目前只支持非常弱的请求模型：

- `proxy_enqueue_request()` 参数过少
- `batch_packet_t` 只携带 `request_id + key_hash`
- worker 只消费批次并执行读路径，没有响应回传
- 没有和 Redis blocked client 机制打通

### 2.4 目前的 UB 语义与 Redis 原生语义不一致

`src/vector_engine_ub_impl.c` 当前明确存在这些约束：

- `key` 基本未参与真实寻址
- `element` 被解析成全局 row index
- `VSIM` 是全表 brute-force 扫描
- `WITHATTRIBS` 没有真实属性来源
- `FILTER` 不支持
- `VADD` 的 `SETATTR / REDUCE / CAS / BIN / Q8 / M / EF` 语义未完整覆盖

这意味着：即使命令“能执行”，其行为也还不等价于 Redis 原生 vector-set。

## 3. 实现原则

### 3.1 不改 RESP 主链路

不需要修改：

- `src/networking.c`
- `src/resp_parser.c`
- Redis 命令分发主流程

要修改的是模块命令层以及 UB/Proxy/SuperNode 子系统。

### 3.2 先把语义做对，再做批量化

优先级应该是：

1. 先让 `VADD / VEMB / VSIM` 在 UB 模式下语义正确
2. 再给读路径接入 Proxy 微批处理
3. 最后再决定写路径是否进入批量化

### 3.3 先支持最小功能子集

第一阶段建议只稳定支持：

- `VADD key VALUES|FP32 vector element`
- `VEMB key element [RAW]`
- `VSIM key VALUES|FP32|ELE ... [COUNT] [WITHSCORES]`

第一阶段明确不支持或暂不承诺：

- `WITHATTRIBS`
- `FILTER`
- `FILTER-EF`
- `CAS`
- `REDUCE`
- `BIN / Q8 / NOQUANT`
- `M / EF` 的 HNSW 相关语义

对这些未实现能力，应该返回显式错误，而不是静默忽略。

## 4. 推荐实现路径

## 4.1 第一阶段：做出“同步直连 UB”的最小闭环

这一阶段不使用 Proxy 聚合，直接完善 `vector_engine_ub_impl.c`。

### 目标

- `VADD` 可写
- `VEMB` 可读
- `VSIM` 可查
- 行为至少在 key 作用域内正确

### 核心问题

当前 UB 实现最大的问题不是性能，而是寻址模型错误：

- Redis 原生模型是 `(key, element) -> vector`
- 当前 UB 模型近似是 `element(row_id) -> vector`

这会导致多个 key 之间没有隔离。

### 必须补的元数据层

至少需要引入一层 UB metadata，用于维护：

- `key -> vector_set_meta`
- `vector_set_meta.dim`
- `vector_set_meta.cardinality`
- `vector_set_meta.element_to_row`
- `vector_set_meta.row_to_element`
- 行状态信息：空闲、已分配、已删除

实现上有两个可选方向：

#### 方案 A：元数据存在 Redis 本地内存

优点：

- 改动最小
- 复用现有 Redis 对象生命周期
- 更容易快速验证 `VADD/VEMB/VSIM`

缺点：

- UB 只负责向量本体，元数据不共享

#### 方案 B：元数据也放共享内存

优点：

- 更接近“全共享架构”

缺点：

- 复杂度明显更高
- 需要先解决分配器、一致性、恢复问题

建议先做方案 A。

### 第一阶段各命令该怎么落

#### VADD

命令入口仍然是 `VADD_RedisCommand()`。

UB 路径需要：

1. 校验当前 key 的 vector dimension 是否已建立
2. 如果 key 首次出现，创建该 key 的 metadata
3. 根据 element 查找是否已有 row
4. 若不存在则分配新 row
5. 把向量写入 UB
6. 更新 metadata
7. 返回 `1` 或更新语义对应值

这一阶段建议：

- 先不支持 `SETATTR`
- 先不支持 `CAS`
- 先不支持 `REDUCE`
- 先只支持 `FP32/VALUES`

#### VEMB

UB 路径需要：

1. 根据 key 找到对应 metadata
2. 根据 element 找到 row_id
3. 从 UB 读取该 row
4. 按 `RAW` 或数组格式回包

#### VSIM

UB 路径需要：

1. 根据 key 找到该 key 的有效 row 集合
2. 解析 query vector
3. 只在该 key 对应的 rows 上做扫描
4. 对每一行直接计算相似度分数
5. 返回“element + score”的完整结果集

当前 `ub_engine_vsim()` 的主要问题是：

- 扫的是全表，不是当前 key
- 过去把它当成 top-k 搜索处理，职责过重

当前建议改成：

- 线性扫描
- 逐行计算相似度
- 不在 `ub_engine_vsim()` 内做 top-k 选择

也就是说，当前 UB `VSIM` 更像：

- score-only similarity kernel

而不是完整的最终搜索语义。筛选、截断或排序如果需要，放到更外层完成。

## 4.2 第二阶段：只把读路径接入 Proxy 聚合

这一阶段建议只处理：

- `VEMB`
- `VSIM`

先不做 `VADD` 批量写。

### 原因

- 读请求天然适合微批处理
- 写请求涉及顺序、互斥、失败回滚和元数据一致性
- 先打通读路径可以最快验证 `Proxy -> SuperNode` 架构价值

### 模块命令层要怎么改

在 UB 模式下：

- `VEMB_RedisCommand()` 不再直接同步调用 `ve->vemb`
- `VSIM_RedisCommand()` 不再直接同步调用 `ve->vsim`

而是改成：

1. 解析命令参数
2. 构造 request context
3. `RedisModule_BlockClient()`
4. 将请求提交给 proxy aggregator
5. 等 worker 完成
6. 在 reply callback 中格式化 RESP 回复
7. `RedisModule_UnblockClient()`

这意味着：真正的异步边界应该在模块命令层，而不是硬塞进当前同步
`vector_engine_t` 接口。

### 如果 `VEMB / VSIM` 真正走 `proxy -> supernode`，需要哪些改造

这一节只讨论读路径，也就是：

- `VEMB`
- `VSIM`

不包括 `VADD`。

完整目标链路如下：

```text
VEMB / VSIM
  -> 模块命令入口
  -> blocked client
  -> proxy enqueue
  -> batch packet
  -> supernode worker
  -> completion/result routing
  -> reply callback
  -> client
```

当前代码离这条链路还差五块。

#### 1. 命令层：从同步直调改成异步提交

当前 UB 路径仍是同步调用：

- `VEMB_RedisCommand()` 直接调用 `ve->vemb`
- `VSIM_RedisCommand()` 直接调用 `ve->vsim`

如果要走 `proxy -> supernode`，这里必须改成：

1. 解析命令参数
2. 构造请求上下文
3. `RedisModule_BlockClient()`
4. 提交请求到 proxy
5. 等待 completion
6. 在 reply callback 中回包

需要保存的上下文至少包括：

`VEMB`

- `key`
- `element`
- `raw_output`
- `RedisModuleBlockedClient *bc`

`VSIM`

- `key`
- query vector
- `count`（保留字段，但可以由外层解释）
- `withscores`
- `withattribs`
- RESP2/RESP3 回复模式
- `RedisModuleBlockedClient *bc`

建议新增独立的请求上下文模块，而不是继续复用 `void *client_ctx`。

建议新增：

- `src/vector_proxy_request.h`
- `src/vector_proxy_request.c`

#### 2. Proxy 请求模型：从“单结果向量”扩展成通用向量请求

当前 `proxy_enqueue_request()` 的模型过于简化：

```c
int proxy_enqueue_request(const char *key, void *client_ctx,
                         float *result_buffer, size_t vector_dim);
```

它只适合“按 key 读一个向量到一个结果 buffer”这种极简场景，不足以表达
`VSIM`。

这里必须改成通用请求模型，至少能表达：

- 请求类型：`VEMB` / `VSIM`
- 路由 key
- `VEMB` 的 row_id 或 element
- `VSIM` 的 query vector
- `VSIM` 的请求配置
- 回复标志：`raw` / `withscores` / `withattribs`
- blocked client 或 completion handle

建议统一改成：

```c
int proxy_enqueue_vector_request(proxy_vector_request_t *req);
```

而不是继续用分散参数。

`src/proxy_batch_bucket.h` 中的 `proxy_request_t` 也需要同步扩展，至少要新增：

- `op_type`
- 输入 payload
- 输出 payload
- flags
- completion 信息

#### 3. 协议层：batch packet 需要支持 opcode 和 payload

当前 `src/supernode_protocol.h` 中的 `batch_packet_t` 只有：

- `request_id`
- `key_hash`

这不足以执行真实的 `VEMB` 和 `VSIM`。

需要把协议升级成“带 opcode 的可变长请求包”，至少支持：

- `VEMB_BATCH`
- `VSIM_BATCH`

建议把 packet 拆成：

`header`

- `magic`
- `packet_size`
- `num_requests`
- `op_type`
- `supernode_id`
- `worker_id`
- `batch_id`

`requests[]`

`VEMB`

- `request_id`
- `row_id`

`VSIM`

- `request_id`
- `query_offset`
- `query_dim`
- `count`
- `flags`

另外需要一个 payload 区，用来存放 `VSIM` 的 query vector 数据。

如果想降低第一版复杂度，建议：

- 第一版先只让 `VEMB` 走 proxy/supernode
- `VSIM` 放在下一步

因为 `VEMB` 的请求体和结果体都是固定形态，而 `VSIM` 同时带有变长输入和
变长输出。

#### 4. SuperNode 执行层：从 hash 占位实现变成按 opcode 执行真实读命令

当前 `supernode_worker.c` 里的逻辑仍然是占位实现：

- 从 packet 里取 `key_hash`
- 用 `% table_row_capacity` 推导 emb_id
- 调批量读取函数

这不能直接承接真实 `VEMB/VSIM`。

需要改成按 `op_type` 分发：

`VEMB`

1. 从请求里提取真实 `row_id`
2. 收集本批所有 `row_id[]`
3. 批量读取 embedding
4. 生成 `request_id -> vector` 结果

`VSIM`

1. 解析 query vector
2. 获取候选 row 集合
3. 扫描并计算相似度
4. 生成 `request_id -> [(row_id, score)...]` 完整分数结果

对于 `VEMB`，batch gather 的收益最直接；对于 `VSIM`，则需要在 worker 内部
执行批量相似度计算逻辑。

建议先打通 `VEMB`，再扩展 `VSIM`。

#### 5. 结果回传：当前最缺的一块

当前代码基本没有完整的“worker 完成后如何把结果送回 blocked client”的链路。

这块必须新增 completion 机制。

推荐两种实现方式：

方案 A：共享 completion 表

- proxy 侧为每个 `request_id` 注册 completion slot
- worker 把结果写入 slot
- completion 线程或主线程轮询完成状态
- 完成后 `RedisModule_UnblockClient()`

优点：

- 结构直观
- 改造面较小

缺点：

- 轮询成本较高

方案 B：结果 ring buffer

- 请求走 `proxy -> supernode` ring buffer
- 结果走 `supernode -> proxy` 结果 ring buffer
- proxy 结果线程收到后回调 blocked client

优点：

- 结构更完整
- 更接近真正双向异步协议

缺点：

- 改造面明显更大

建议第一版先实现方案 A。

#### 6. 回复格式：留在模块层，不放到 supernode

supernode 不应该直接生成 RESP。

建议职责划分如下：

- supernode 只返回结构化结果
- 模块层负责把结果格式化成 RESP2/RESP3

也就是：

`VEMB`

- supernode 返回 `float[dim]`
- 模块层决定返回 RAW 还是数组

`VSIM`

- supernode 返回 `[(row_id, score), ...]`
- 模块层决定是否输出 `WITHSCORES / WITHATTRIBS`
- 模块层负责 RESP2 和 RESP3 差异

这样可以避免 supernode 和 Redis 协议细节耦合。

#### 7. 推荐最小落地顺序

不建议同时改 `VEMB` 和 `VSIM`。

推荐顺序：

1. 新增 `proxy_vector_request_t`
2. 给 proxy 和 packet 增加 `op_type`
3. 打通 `VEMB -> blocked client -> proxy -> supernode -> completion -> reply`
4. 再扩展 `VSIM` 的请求结构
5. 再打通 `VSIM` 的回包和结果格式化

原因：

- `VEMB` 是“一请求一向量”
- 请求体小
- 返回体固定
- 更适合作为第一条闭环验证链路

而 `VSIM` 同时有：

- query vector 作为输入
- 完整分数结果列表作为输出
- 结果格式更复杂

#### 8. 第二阶段建议新增的文件

为避免把 completion 和 request 生命周期逻辑塞进现有模块，建议新增：

- `src/vector_proxy_request.h`
- `src/vector_proxy_request.c`
- `src/vector_proxy_completion.h`
- `src/vector_proxy_completion.c`

分别负责：

- 通用向量请求定义
- 请求对象生命周期
- request_id 到结果/blocked client 的映射
- 完成状态管理和唤醒

## 4.3 第三阶段：实现 `VADD` 的一致性写路径

在读路径稳定后，再决定 `VADD` 是否需要进入 Proxy 批处理。

建议先做同步或小批量写，原因如下：

- `VADD` 必须更新 metadata
- `VADD` 需要互斥控制
- `VADD` 的成功不能只表示“写入 ring buffer 成功”
- 一旦异步化，就要定义写完成确认点

### 写路径必须保证的语义

至少要保证：

1. row 分配和向量写入一致
2. metadata 与 UB 中的数据一致
3. 并发 `VEMB/VSIM` 不会读到半写状态
4. 失败时不会泄露未完成 row

### 推荐互斥模型

第一版建议用简单模型：

- metadata 层用 key 级别 mutex 或 rwlock
- UB row 写入用 row bitmap / row lock
- 元数据更新在写入成功后提交

如果后续要走完全无锁方案，再单独演进。

## 5. 需要修改的关键文件

## 5.1 第一阶段必须改

### `src/vector_engine_ub_impl.c`

需要增加：

- key 级 metadata 查找
- element 到 row 的映射
- row 分配逻辑
- 只针对 key 内部元素扫描的 `VSIM`
- 逐行相似度计算，不在 UB 内核中做 top-k
- 对不支持选项返回显式错误

### `src/vector_engine.h`

如果继续保持同步模型，这一阶段未必需要大改。

如果后面要走异步 proxy 路线，建议新增更清晰的分层：

- 同步直连接口
- 异步批处理接口

避免让一个接口同时承担两套模型。

### `modules/vector-sets/vset.c`

需要修改 UB dispatch 行为：

- 对不支持选项提前拒绝
- 把 UB 回复格式保持与原命令一致
- 后续为 blocked client 模式预留 request context

## 5.2 第二阶段必须改

### `src/proxy_aggregator.h`

当前接口过弱：

```c
int proxy_enqueue_request(const char *key, void *client_ctx,
                         float *result_buffer, size_t vector_dim);
```

必须扩展为能表达通用向量请求，例如：

- op type
- key
- element / row_id
- query vector
- count
- reply flags
- blocked client handle
- result carrier

### `src/proxy_batch_bucket.h`

`proxy_request_t` 目前只适合单一读向量请求，需要补：

- `op_type`
- 输入 payload
- 输出 payload
- 命令选项 flags
- blocked client 或回复上下文

### `src/supernode_protocol.h`

当前 `batch_packet_t` 只适合“按 hash 读行”的极简模型。

必须扩展为：

- `opcode`
- request payload
- response correlation 信息

至少需要支持三类 packet：

- `VEMB_BATCH`
- `VSIM_BATCH`
- `VADD_BATCH`

### `src/proxy_batch_bucket.c`

需要同步更新：

- packet size 计算
- packet 序列化逻辑
- bucket reset 生命周期

### `src/supernode_worker.c`

需要从“只消费请求”变成“消费请求并写回结果”。

要补的核心能力：

- 按 opcode 分发处理
- VEMB 批量读取
- VSIM 批量相似度计算
- 完成后回传结果

### 响应回传机制

这一块当前代码里基本不存在，需要单独设计。

推荐两种方案二选一：

#### 方案 A：共享完成表 + 主线程轮询

优点：

- 实现直观

缺点：

- 轮询成本高

#### 方案 B：结果 ring buffer + 事件唤醒

优点：

- 更像完整异步系统

缺点：

- 改动更大

建议优先做 A，验证成功后再升级到 B。

## 5.3 第三阶段可能要改

### `src/supernode_worker.c`

要增加写路径：

- row lock / bitmap acquire
- UB store
- metadata 提交确认

### metadata 模块

当前仓库里还没有一个明确的 UB metadata 独立模块，建议新增：

- `src/ub_metadata.h`
- `src/ub_metadata.c`

负责：

- key 元信息
- element-row 映射
- row allocator
- cardinality
- 删除状态

## 6. 各命令的推荐能力边界

## 6.1 VEMB

第一阶段可完整支持：

- `VEMB key element`
- `VEMB key element RAW`

这是最适合作为第一个打通命令的。

## 6.2 VSIM

第一阶段建议只支持：

- `VSIM key VALUES ...`
- `VSIM key FP32 ...`
- `VSIM key ELE ...`
- `WITHSCORES`

不建议第一阶段支持：

- `WITHATTRIBS`
- `FILTER`
- `FILTER-EF`
- `TRUTH`
- `NOTHREAD` 的 Redis 原语义复刻

说明：

- UB 本身没有 HNSW 图，也没有 attributes
- `TRUTH` 对 UB 无意义，因为它本来就是线性扫描

## 6.3 VADD

第一阶段建议只支持：

- `VADD key VALUES dim ... element`
- `VADD key FP32 blob element`

不建议第一阶段支持：

- `SETATTR`
- `CAS`
- `REDUCE`
- `NOQUANT`
- `BIN`
- `Q8`
- `M`
- `EF`

## 7. 推荐里程碑

### 里程碑 1：语义闭环

目标：

- 单 key 下 `VADD/VEMB/VSIM` 可用
- key 级隔离正确
- metadata 存在 Redis 本地内存
- VSIM 只做 key-scoped 相似度计算

验收标准：

- `VADD -> VEMB` 能读回
- `VADD` 多个 key 不串数据
- `VSIM` 返回当前 key 下完整分数结果

### 里程碑 2：读路径异步化

目标：

- `VEMB/VSIM` 经由 blocked client + proxy + supernode 完成
- worker 能回传结果

验收标准：

- Redis 主线程不被长查询阻塞
- 多个并发 `VEMB/VSIM` 能进入批次
- 回包格式和同步模式一致

### 里程碑 3：写路径一致性

目标：

- `VADD` 支持并发写
- 元数据和 UB 写入一致
- 与 `VEMB/VSIM` 并发不破坏可见性

验收标准：

- 并发写后数据不丢失
- 无明显半写、脏读

## 8. 最务实的实现建议

如果目标是尽快做出一个“能用”的版本，建议按下面的范围收敛：

### 第一批必须做

- metadata 层
- `VADD`
- `VEMB`
- `VSIM`
- 只支持 `FP32/VALUES`
- `VSIM` 只支持基础分数返回

### 第二批再做

- Proxy 微批处理
- blocked client 回包
- SuperNode 结果回传

### 第三批最后做

- `SETATTR`
- `FILTER`
- `WITHATTRIBS`
- `CAS`
- `REDUCE`
- `BIN/Q8`

## 9. 不建议的做法

以下做法不建议采用：

- 从 Redis 核心 RESP 解析器改起
- 在当前同步 `vector_engine_t` 接口里强行塞完整异步批量语义
- 在没有 metadata 的情况下继续用“element 即 row index”模型扩展功能
- 一开始就追求 `VADD/VSIM/VEMB` 全功能对齐 Redis 原生 vector-set

## 10. 最终建议

推荐采用下面这条主路线：

1. 先补 UB metadata，修正 key/element 语义
2. 让 `VADD/VEMB/VSIM` 在同步 UB 模式下工作正确
3. 把 `VSIM` 的实现收敛成逐行相似度计算内核
4. 只把 `VEMB/VSIM` 接入 Proxy 聚合与 SuperNode
5. 最后处理 `VADD` 的一致性写路径

这是当前代码基础上最稳妥、最容易逐步验证的实现方式。

## 11. 执行 TODO

本节把前文的阶段路线收敛为可执行的工程 TODO。后续如果继续细化某个阶段，
统一追加到本节。

## 11.1 Phase 1：同步 UB 语义闭环

目标：

- `VADD` 在 UB 模式下可写
- `VEMB` 在 UB 模式下可读
- `VSIM` 在 UB 模式下可查
- key 级隔离语义正确

### 文件级 TODO

#### `src/ub_metadata.h`

新增文件，定义 metadata 的核心结构和 API。

建议至少定义：

- `ub_vector_set_meta`
- `ub_row_entry`
- `ub_metadata_registry`

建议至少声明：

- `int ub_metadata_init(void);`
- `void ub_metadata_cleanup(void);`
- `ub_vector_set_meta *ub_metadata_get_or_create_set(const char *key, size_t dim);`
- `ub_vector_set_meta *ub_metadata_get_set(const char *key);`
- `int ub_metadata_lookup_row(ub_vector_set_meta *set, const char *element, uint64_t *row_id);`
- `int ub_metadata_alloc_row(ub_vector_set_meta *set, const char *element, uint64_t *row_id);`
- `int ub_metadata_set_row(ub_vector_set_meta *set, const char *element, uint64_t row_id);`
- `size_t ub_metadata_cardinality(const ub_vector_set_meta *set);`

#### `src/ub_metadata.c`

新增文件，实现：

- `key -> vector set metadata`
- `element -> row_id`
- `row_id -> element`
- `dim`
- `cardinality`
- row allocator
- 已删除/空闲状态

第一版建议：

- 元数据只存在 Redis 进程内存
- 使用简单锁实现
- 不做共享内存化

#### `src/Makefile`

把以下对象加入编译：

- `ub_metadata.o`

#### `src/vector_engine_ub_impl.c`

这是 Phase 1 的主改造点。

`ub_engine_vadd()`

- 不再忽略 `key`
- 根据 key 获取或创建 metadata
- 根据 element 查找是否已有 row
- 若不存在则分配新 row
- 写 UB
- 更新 metadata

`ub_engine_vemb()`

- 通过 `key + element` 查 row
- 再读取该 row
- 不再默认 `element` 是全局 row index

`ub_engine_vsim()`

- 不再扫全表
- 只扫描当前 key 对应的有效 row 集合
- 逐行计算相似度分数并返回完整结果集

同时需要：

- 对暂不支持的功能保持清晰行为
- 不依赖“单表全局 element -> row”假设

#### `modules/vector-sets/vset.c`

`VADD_RedisCommand()`

- UB 路径下显式拒绝：
  - `SETATTR`
  - `CAS`
  - `REDUCE`
  - `BIN`
  - `Q8`
  - `NOQUANT`

`VSIM_RedisCommand()`

- UB 路径下显式拒绝：
  - `FILTER`
  - `FILTER-EF`
  - `WITHATTRIBS`
  - `TRUTH`
- 如果 `EF` 暂无实际语义，要么报错，要么明确忽略并记录

`VEMB_RedisCommand()`

- 保持 RAW 与普通数组回复格式一致

### 函数级 TODO

#### `src/vector_engine_ub_impl.c`

建议新增或重构辅助函数：

- `static int ub_engine_lookup_set_meta(void *key, ub_vector_set_meta **meta);`
- `static int ub_engine_get_or_create_set_meta(void *key, size_t dim, ub_vector_set_meta **meta);`
- `static int ub_engine_lookup_row_for_element(void *key, void *element, uint64_t *row_id, sds *element_tmp);`
- `static int ub_engine_alloc_row_for_element(void *key, void *element, uint64_t *row_id, sds *element_tmp);`

建议进一步拆出：

- `ub_engine_vadd_resolve_target_row()`
- `ub_engine_vsim_collect_rows_for_key()`

#### `src/ub_metadata.c`

建议优先实现：

- registry 初始化/销毁
- key 查找
- element 查找
- row 分配
- cardinality 维护

### Phase 1 验收

- `VADD -> VEMB` 能正确读回
- 多 key 数据不串
- `VSIM` 只在当前 key 的元素范围内计算分数
- `VSIM` 返回稳定的完整分数结果

### Phase 1 当前进展

已完成：

- 新增 `ub_metadata.h/c`
- `ub_engine_vadd()` 按 `key` 建立 metadata 并分配 row
- `ub_engine_vemb()` 改为按 `key + element` 查找 row
- `ub_engine_vsim()` 改为只扫描当前 key 的有效 row 集合
- `ub_engine_vsim()` 改为 score-only similarity kernel
- `ub_engine_vcard()` 改为返回当前 key 的 cardinality
- `ub_engine_vdim()` 优先返回当前 key 的维度
- UB 路径下 `VADD/VSIM` 的未支持选项已显式拒绝
- 新增最小 key-scope 测试：
  - `modules/vector-sets/tests/ub_phase1_scope.py`
  - `modules/vector-sets/tests/ub_phase1_vrem.py`

待完成：

- 跑一次真实模块级测试，而不只是定向编译和 Python 语法检查

当前已知环境阻塞：

- 仓库完整二进制构建当前会被已有工程问题阻塞，不是 Phase 1 改动引起：
  - `proxy_aggregator.o` 依赖缺失的 `bucket_store.h`
  - `make clean` / 依赖构建流程中存在 `tests` 路径相关问题
- 因此当前 Phase 1 的验证方式仍以：
  - 定向编译
  - Python 测试脚本语法检查
  为主，待仓库构建问题消除后再跑真实模块测试

### Phase 1 验证流程

以下流程用于验证：

- `VADD`
- `VEMB`
- `VSIM`
- `VREM`
- `VCARD`
- `VDIM`

说明：

- 最终运行平台按 Linux 处理
- macOS 上的系统头或工具链兼容问题不纳入功能验证范围

#### 1. 静态验证

先做最小定向编译，确认 Phase 1 代码本身可编译：

```bash
make -C src ub_metadata.o vector_engine_ub_impl.o
```

然后检查新增测试脚本语法：

```bash
PYTHONDONTWRITEBYTECODE=1 python3 - <<'PY'
import ast, pathlib
for p in [
    pathlib.Path('modules/vector-sets/tests/ub_phase1_scope.py'),
    pathlib.Path('modules/vector-sets/tests/ub_phase1_vrem.py'),
]:
    ast.parse(p.read_text(), filename=str(p))
print('ok')
PY
```

#### 2. Linux 上的最小手工验证

在 Linux 环境中启动一个可用的 Redis 测试实例后，按顺序执行以下命令。

##### 2.1 验证 `VADD + VEMB`

```bash
VADD test:a VALUES 4 1 0 0 0 test:a:item:1
VADD test:a VALUES 4 0.9 0.1 0 0 test:a:item:2
VEMB test:a test:a:item:1
VEMB test:a test:a:item:2
```

预期：

- `VADD` 返回成功
- `VEMB` 返回对应向量

##### 2.2 验证 key-scope

```bash
VADD test:b VALUES 4 0 1 0 0 test:b:item:1
VEMB test:b test:a:item:1
```

预期：

- `VEMB test:b test:a:item:1` 返回 `nil`
- 证明 `key + element` 已经真正隔离

##### 2.3 验证 `VCARD / VDIM`

```bash
VCARD test:a
VCARD test:b
VDIM test:a
VDIM test:b
```

预期：

- `test:a` 的 `VCARD` 为 2
- `test:b` 的 `VCARD` 为 1
- 两个 key 的 `VDIM` 都为 4

##### 2.4 验证 `VSIM`

注意：

- 当前 UB `VSIM` 是 score-only similarity kernel
- 它返回当前 key 下完整分数结果
- 不再按 top-k 搜索解释

```bash
VSIM test:a VALUES 4 1 0 0 0 WITHSCORES
VSIM test:b VALUES 4 1 0 0 0 WITHSCORES
```

预期：

- `test:a` 的结果中只出现 `test:a:*`
- `test:b` 的结果中只出现 `test:b:*`
- 不发生 cross-key 混入

##### 2.5 验证 `VREM`

```bash
VREM test:a test:a:item:1
VEMB test:a test:a:item:1
VCARD test:a
VSIM test:a VALUES 4 1 0 0 0 WITHSCORES
```

预期：

- `VREM` 返回成功
- `VEMB` 返回 `nil`
- `VCARD` 减 1
- `VSIM` 不再返回被删除元素

##### 2.6 验证未支持选项

```bash
VADD test:x VALUES 4 1 0 0 0 x1 SETATTR '{"a":1}'
VADD test:x VALUES 4 1 0 0 0 x1 CAS
VSIM test:a VALUES 4 1 0 0 0 WITHATTRIBS
VSIM test:a VALUES 4 1 0 0 0 FILTER '.a > 1'
```

预期：

- 都返回明确错误
- 不会静默成功

#### 3. Python 最小测试入口

当前 vector-sets Python 测试入口是：

- `modules/vector-sets/test.py`

Phase 1 当前新增的最小测试文件是：

- `modules/vector-sets/tests/ub_phase1_scope.py`
- `modules/vector-sets/tests/ub_phase1_vrem.py`

在 Linux 测试实例准备完成后，优先验证这两个测试。

#### 4. Phase 1 验收口径

Phase 1 的验收重点是功能语义，不是性能：

- `VADD` 能写
- `VEMB` 能按 `key + element` 正确读取
- `VSIM` 只在当前 key 范围内计算分数
- `VCARD / VDIM` 按 key 独立
- `VREM` 删除后语义正确
- 不支持选项报错清晰

### Phase 1 最小测试入口

当前 vector-sets Python 测试入口是：

- `modules/vector-sets/test.py`

建议优先跑的最小用例如下：

- `modules/vector-sets/tests/ub_phase1_scope.py`
- `modules/vector-sets/tests/ub_phase1_vrem.py`

在有本地 Redis 测试实例的前提下，可通过该入口选择性加载并执行上述测试。

## 11.2 Phase 2A：先打通 `VEMB -> proxy -> supernode`

目标：

- `VEMB` 不再同步直调 UB
- `VEMB` 经由 `proxy -> supernode -> completion -> reply` 返回

### 文件级 TODO

#### `src/vector_proxy_request.h`

新增文件，定义上层业务请求对象 `proxy_vector_request_t`。

第一版只覆盖 `VEMB`，建议字段包括：

- `op_type`
- `request_id`
- `sds key`
- `sds element`
- `uint64_t row_id`
- `int raw_output`
- `RedisModuleBlockedClient *bc`
- `float *result_vector`
- `size_t result_dim`
- `int error_code`
- completion 状态

#### `src/vector_proxy_request.c`

新增文件，实现：

- 请求对象创建
- 请求对象销毁
- `key/element` 生命周期管理
- `result_vector` 生命周期管理

#### `src/vector_proxy_completion.h`

新增文件，定义 completion 机制。

第一版建议实现共享 completion 表，至少提供：

- `int vector_proxy_completion_init(void);`
- `void vector_proxy_completion_cleanup(void);`
- `int vector_proxy_completion_register(proxy_vector_request_t *req);`
- `proxy_vector_request_t *vector_proxy_completion_lookup(uint64_t request_id);`
- `int vector_proxy_completion_complete_vemb(uint64_t request_id, const float *vector, size_t dim, int error_code);`
- `int vector_proxy_completion_take(uint64_t request_id, proxy_vector_request_t **req);`

#### `src/vector_proxy_completion.c`

新增文件，实现：

- `request_id -> request` 映射
- 完成状态记录
- 结果拷贝
- 完成后的取回与移除

#### `src/Makefile`

加入以下对象：

- `vector_proxy_request.o`
- `vector_proxy_completion.o`

#### `src/proxy_batch_bucket.h`

重构 `proxy_request_t`，改成“轻包装层”，建议至少包含：

- `request_id`
- `key_hash`
- `target_supernode_id`
- `target_worker_id`
- `submit_time_us`
- `proxy_vector_request_t *owner`

避免把完整业务字段继续平铺在 `proxy_request_t` 中。

#### `src/proxy_batch_bucket.c`

更新：

- `proxy_request_create()`
- `proxy_request_destroy()`
- packet size 计算
- packet 填充逻辑

第一版 packet 只需要支持 `VEMB`。

#### `src/proxy_aggregator.h`

新增统一入口：

- `int proxy_enqueue_vector_request(proxy_vector_request_t *req);`

旧接口可以临时保留，但后续应收敛。

#### `src/proxy_aggregator.c`

改造 enqueue 流程：

- 从 `proxy_vector_request_t` 计算路由
- 创建 `proxy_request_t`
- 放入对应 bucket
- flush 时不再假定只有 `result_buffer/vector_dim`

#### `src/supernode_protocol.h`

第一版先支持：

- `VEMB_BATCH`

需要增加：

- `op_type`
- `row_id`

第一版不要同时引入 `VSIM` 的可变长 payload。

#### `src/supernode_worker.c`

第一版只做 `VEMB_BATCH`：

1. 从 packet 中解析 `row_id[]`
2. 批量读取这些 row
3. 将结果写入 completion 表

#### `modules/vector-sets/vset.c`

`VEMB_RedisCommand()`

UB 路径改成：

1. 解析 `key/element/raw_output`
2. 创建 `proxy_vector_request_t`
3. `RedisModule_BlockClient()`
4. 注册 completion
5. enqueue 到 proxy
6. 在 reply callback 中返回 RAW 或数组

### 函数级 TODO

#### `src/vector_proxy_request.c`

建议优先实现：

- `proxy_vector_request_t *proxy_vector_request_create_vemb(...);`
- `void proxy_vector_request_free(proxy_vector_request_t *req);`

#### `src/proxy_aggregator.c`

建议新增：

- `static int proxy_enqueue_vemb_request(proxy_vector_request_t *req);`

或统一：

- `int proxy_enqueue_vector_request(proxy_vector_request_t *req);`

#### `src/supernode_worker.c`

建议新增：

- `static int supernode_process_vemb_batch(...);`

### Phase 2A 验收

- 单个 `VEMB` 能经由 `proxy -> supernode` 返回正确结果
- 多个并发 `VEMB` 能形成批次
- 主线程不再同步等待 `ve->vemb()`

## 11.3 Phase 2B：再接入 `VSIM -> proxy -> supernode`

目标：

- `VSIM` 经由 `proxy -> supernode -> completion -> reply` 返回

### 文件级 TODO

#### `src/vector_proxy_request.h`

扩展 `proxy_vector_request_t`，增加 `VSIM` 所需字段：

- query vector
- query dim
- `withscores`
- `withattribs`
- 结果列表

#### `src/vector_proxy_request.c`

新增：

- `proxy_vector_request_create_vsim(...)`
- `VSIM` 查询向量生命周期管理
- `VSIM` 结果列表生命周期管理

#### `src/vector_proxy_completion.h`

增加：

- `vector_proxy_completion_complete_vsim(...)`

#### `src/vector_proxy_completion.c`

实现：

- `VSIM` 结果列表写回
- `request_id -> [(row_id, score)...]` 回收

#### `src/supernode_protocol.h`

增加：

- `VSIM_BATCH`
- query vector payload 区
- `count`
- flags

#### `src/proxy_batch_bucket.c`

支持 `VSIM` packet 序列化：

- query vector payload 布局
- 多请求时的 offset 管理

#### `src/supernode_worker.c`

增加 `VSIM` 执行分支：

1. 解析 query vector
2. 获取候选 row 集合
3. 扫描并计算相似度
4. 写回完整分数结果

#### `modules/vector-sets/vset.c`

`VSIM_RedisCommand()`

UB 路径改成：

1. 解析 query
2. 创建 `proxy_vector_request_t`
3. `RedisModule_BlockClient()`
4. 注册 completion
5. enqueue
6. 在 reply callback 中格式化 RESP2/RESP3

### 函数级 TODO

#### `src/vector_proxy_request.c`

建议新增：

- `proxy_vector_request_t *proxy_vector_request_create_vsim(...);`

#### `src/supernode_worker.c`

建议新增：

- `static int supernode_process_vsim_batch(...);`
- `static int supernode_vsim_topk(...);`

### Phase 2B 验收

- `VSIM` 经由 `proxy -> supernode` 返回正确结果
- `WITHSCORES` 语义与同步版本一致
- 多个并发 `VSIM` 能进入批次

## 11.4 Phase 3：`VADD` 写路径一致性

目标：

- `VADD` 写入 UB 与 metadata 一致
- 与 `VEMB/VSIM` 并发不破坏可见性

### 文件级 TODO

#### `src/ub_metadata.c`

增强：

- row 分配锁
- element 覆写与更新语义
- 删除后重用策略

#### `src/vector_engine_ub_impl.c`

`ub_engine_vadd()`

- 保证 metadata 与 UB 写入一致
- 失败时有明确回滚语义

#### `src/supernode_worker.c`

如果将来 `VADD` 也走 `proxy -> supernode`，需要增加：

- `VADD_BATCH`
- 写路径执行函数
- 写成功确认逻辑

### 函数级 TODO

建议新增或完善：

- `ub_engine_vadd_commit()`
- `ub_engine_vadd_rollback()`

### Phase 3 验收

- 并发写后无数据丢失
- 无明显半写、脏读
- metadata 与 UB 数据一致

## 11.5 当前推荐执行顺序

建议严格按下面顺序推进：

1. `Phase 1`
2. `Phase 2A`
3. `Phase 2B`
4. `Phase 3`

进一步展开：

1. 新增 `ub_metadata.h/c`
2. 修改 `vector_engine_ub_impl.c`
3. 修改 `vset.c` 的 UB 路径和选项拒绝逻辑
4. 新增 `vector_proxy_request.h/c`
5. 新增 `vector_proxy_completion.h/c`
6. 改 `proxy_aggregator.*`
7. 改 `proxy_batch_bucket.*`
8. 改 `supernode_protocol.h`
9. 改 `supernode_worker.c`
10. 先打通 `VEMB`
11. 再打通 `VSIM`
12. 最后处理 `VADD`
