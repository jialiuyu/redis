## 12. `new_load_vector_batch` 预取设计

### 12.1 设计目标

`new_load_vector_batch` 面向同一 worker 一次读取多个 key/vector 的场景，例如一次读取 100 个 key 的向量。当前逐条调用 `vemb_v16_tlc_load_vector()` 时，每条 vector 都独立触发地址解析、slot 校验和 payload copy，底层 UB/SHM payload 的 cacheline 访问缺少批量规划。

新的 batch load 目标是：

- 先把一批 vector 的 UB payload 地址解析出来。
- 计算这些 payload 覆盖的 cacheline 基地址。
- 对 cacheline 基地址去重、排序或合并连续区间。
- 在真实 copy 前提前发出 L2/L3 prefetch hint。
- 之后按原 batch 顺序执行 vector load/copy，尽量让 payload 从本地 L2/L3 命中，而不是在 copy 时才从 UB 链路取数。

需要注意：用户态代码不能真正把 cacheline 固定在 L2/L3。ARM `PRFM`、`__builtin_prefetch()` 或基于 SVE 的批量地址生成只能向硬件表达预取和保留倾向，例如 L2 keep 或 L3 keep。cacheline 是否保留、保留多久，仍由 CPU cache 策略决定。因此本文中的“固定到 L2/L3”在实现上应理解为“提前拉入并尽量保持热度”。

### 12.2 API 边界

建议新增 batch API，而不是直接替换单条 `vemb_v16_tlc_load_vector()`：

```text
vemb_v16_tlc_new_load_vector_batch(tlc, handles/items, count, output buffers, opts, result)
```

输入可以是已经解析好的 vector handle，也可以是 key/hash 列表。第一版建议优先支持 handle 列表，因为它离当前 `vemb_v16_tlc_load_vector()` 更近，能复用现有 slot validate/copy 语义；key 列表版本可以在上层先批量 `get_handle`，再进入 batch load。

配置项建议包含：

| 配置 | 含义 | 默认建议 |
| --- | --- | --- |
| `cacheline_size` | cacheline 对齐粒度 | 64B |
| `prefetch_level` | L2、L3 或 auto | auto |
| `prefetch_distance_lines` | copy 前提前多少 line 发起预取 | 8 到 32 |
| `max_prefetch_lines` | 单个 batch 最多预取多少 line | 按 cache budget 配置 |
| `window_vectors` | 每个窗口处理多少 vector | 8 到 32 |
| `dedup_enabled` | 是否去重 cacheline | 默认开启 |
| `sort_lines` | 是否排序并合并连续 line | 默认开启 |
| `fallback_scalar` | 小 batch 或非 ARM/SVE 时回退普通路径 | 默认开启 |
| `verify_generation` | load 前后是否保持原有 generation 校验 | 默认开启 |

结果统计建议包含：

| 字段 | 含义 |
| --- | --- |
| `requested_vectors` | 请求读取的 vector 数 |
| `resolved_vectors` | 成功解析到有效地址的 vector 数 |
| `unique_cachelines` | 去重后的 cacheline 数 |
| `prefetch_lines` | 实际发起预取的 cacheline 数 |
| `copy_bytes` | 实际复制的 payload 字节数 |
| `fallback_count` | 回退到单条 load 的 vector 数 |
| `error_count` | stale、越界、迁移 fence 或 tombstone 导致失败的数量 |

### 12.3 四阶段流水线

推荐实现拆成四个阶段。

第一阶段是 resolve。对每个 item 解析出稳定的 payload 位置：

```text
index
src_addr
dst_addr
bytes
region_id
region_index
local_slot
offset
owner_generation
```

resolve 必须完成所有当前读路径需要的合法性检查：region 存在、mapped address 非空、offset+bytes 不越界、bytes 等于当前 vector size、handle generation 可用于后续校验。对于 key 输入版本，还要保持 source fence、cutover、source_gc、tombstone 的语义。

第二阶段是 cacheline plan。对每个有效 item 计算其 payload 覆盖的 cacheline：

```text
line_begin = align_down(src_addr, cacheline_size)
line_end = align_up(src_addr + bytes, cacheline_size)
```

随后把所有 line 地址放入临时 plan，做去重和排序。对于 100 个 1200B vector，理论上每个 vector 覆盖约 19 条 64B cacheline，总量约 1900 条 line；如果存在连续分配或多个 vector 共用相邻区域，排序合并后可以减少重复预取。

第三阶段是 prefetch。对 plan 中的 line 发出预取 hint：

- 小窗口、马上 copy 的数据优先使用 L2 keep。
- 较大窗口或距离 copy 较远的数据可以使用 L3 keep。
- batch 很大时不要一次性预取全量 line，应按窗口滚动预取。
- 对连续 cacheline run 可以按 stride 顺序预取。
- 对离散 line 可以基于地址数组批量发起预取，SVE 主要用于批量地址生成和循环展开，不应依赖它提供 cache pin 语义。

第四阶段是 load/copy。按原始请求顺序把每个 vector copy 到对应输出 buffer，保持调用方可见顺序不变。copy 阶段仍必须沿用当前 payload copy 的 seqlock/generation 校验逻辑：copy 前确认 slot READY、`write_seq` 为偶数；copy 后再次确认 `write_seq` 未变化且仍为偶数。

### 12.4 窗口化策略

不要一次性把整个 batch 的所有 cacheline 都预取到 L2。推荐窗口化：

```text
build global or per-window cacheline plan
prefetch window 0
copy vectors 0..N-1
prefetch window 1
copy vectors N..2N-1
...
```

更激进的实现可以形成双窗口 overlap：

```text
prefetch window i + 1
copy window i
```

窗口大小应同时受 vector 数和 cacheline 数限制。例如：

- `window_vectors` 控制一次 copy 的 vector 个数。
- `window_prefetch_lines` 控制一次预取的最大 cacheline 数。
- `max_prefetch_lines` 控制整个 batch 的预取上限。

如果 `unique_cachelines` 远超预算，batch API 应降级为有限预取，而不是强行预取全量。

### 12.5 去重与合并策略

按 batch 大小选择不同策略：

| batch 规模 | 建议策略 |
| --- | --- |
| `count <= 8` | 不建复杂结构，线性去重或直接回退单条 load |
| `count <= 256` | cacheline 地址数组排序后去重 |
| 更大 batch | 开放寻址 hash set 去重，再按地址排序或按 region 分桶 |

排序后的 line 可以进一步合并成连续 run：

```text
run_base
run_line_count
```

连续 run 用普通 stride prefetch 即可，离散 run 保留为地址数组。这样可以减少循环分支，也方便后续针对 SVE 做批量地址处理。

### 12.6 Budgeted TopK Prefetch 设计

当前 standalone benchmark 中的 `build_cacheline_plan()` 先为每个 vector 生成 cacheline run，再按地址 `qsort`，合并连续 run，随后 `prefetch_runs()` 从排序后的 run 中按 cacheline 展开预取。这个设计可以验证基础行为，但它的预取选择本质上仍然是“地址排序后的前 N 条 cacheline”，并不判断这些 cacheline 是否最值得预取。

对于固定 1200B vector，单个 vector 约覆盖 19 条 64B cacheline。默认 batch=64 时，全量 payload 约 1216 条 cacheline。即使 `window_lines=512`，一次也可能预取约 32KB 数据；如果未来失去有效 prefetch 上限或 batch 继续增大，就可能把 L1/L2/L3 的有效工作集挤出，导致 `PLDKeep` 或 `__builtin_prefetch()` 变成 cache 污染源。因此 batch prefetch 应先预支 cache budget，再只选择 TopK 的高价值 cacheline span。

推荐第一版不要做逐 cacheline TopK，而是以 vector payload span 为选择单位：

```text
span_begin = align_down(src_addr, cacheline_size)
span_end = align_up(src_addr + bytes, cacheline_size)
span_lines = (span_end - span_begin) / cacheline_size
```

每个 span 对应一个 vector payload 覆盖的连续 cacheline 范围。TopK 选择决定“哪些 vector/span 值得预取”，选中后再按地址排序合并成 run，并由 prefetch 阶段逐 cacheline 发 hint。

#### Cache budget

预取预算应来自 cache size 的保守比例，而不是来自 batch size：

```text
prefetch_budget_lines = prefetch_budget_bytes / cacheline_size
```

默认建议：

| 层级 | 预算建议 | 说明 |
| --- | --- | --- |
| L1 keep | L1D 的 25% 到 50% | 只用于马上要 copy 的小窗口 |
| L2 keep | L2 的 10% 到 25% | 用于较大窗口或 hot bucket |
| L3 keep | LLC 的较小比例 | 只用于 copy 距离更远的数据 |

standalone benchmark 中可以先使用编译期常量，例如：

```c
#ifndef UB_BATCH_PREFETCH_BUDGET_BYTES
#define UB_BATCH_PREFETCH_BUDGET_BYTES (16u * 1024u)
#endif

#ifndef UB_BATCH_PREFETCH_BUCKET_SHIFT
#define UB_BATCH_PREFETCH_BUCKET_SHIFT 16u
#endif

#ifndef UB_BATCH_PREFETCH_WINDOW_VECTORS
#define UB_BATCH_PREFETCH_WINDOW_VECTORS 16u
#endif
```

其中 16KB budget 对 64B cacheline 是 256 lines，默认 1200B vector 约等于 13 个完整 vector 的 payload。这样可以避免一次预取整个 batch。

#### 地址前缀 bucket

为了识别 hot 或局部随机访问，先按地址前缀聚合 span：

```text
bucket_key = (region_index, span_begin >> bucket_shift)
```

推荐默认 `bucket_shift=16`，即 64KB bucket。这个粒度足够捕获连续和热点局部性，又不会像 2MB bucket 那样过粗导致污染。

每个 bucket 记录：

```text
region_index
prefix
span_count
line_estimate
min_begin
max_end
first_request_index
```

`span_count` 表示该地址区域被多少 vector 命中，`line_estimate` 表示预取这些 span 的成本，`first_request_index` 用来判断数据距离 copy 的远近。

#### TopK score

span 或 bucket 的评分应同时考虑局部性、紧迫性和成本：

```text
score = locality_score + urgency_score - cost_score
```

第一版可以使用简单整数评分：

```text
locality_score = bucket.span_count * 8
urgency_score = max(0, window_vectors - request_index)
cost_score = span_lines
score = locality_score + urgency_score - cost_score
```

如果要更偏向高密度 bucket，可以加入 density：

```text
bucket_span_lines = (bucket.max_end - bucket.min_begin) / cacheline_size
density_scaled = bucket.span_count * 1024 / max(bucket_span_lines, 1)
score = bucket.span_count * 8 + density_scaled - first_request_index / 16
```

选择时按 score 从高到低取 span，直到：

```text
selected_lines + span_lines > prefetch_budget_lines
```

超过预算的 span 可以跳过，继续尝试下一个更小或更近的 span。最终只对选中的 span 做排序合并。

#### 请求顺序窗口化

`PLDKeep` 最适合马上会读的数据，不适合提前很远预取一整批。因此 TopK 应与 request-order window 结合：

```text
for each request-order window:
    resolve valid spans
    aggregate buckets
    select TopK spans under cache budget
    sort and merge selected spans into runs
    prefetch selected runs
    copy this window in original request order
```

第一版可以先在整个 batch 上做 TopK；如果 benchmark 显示预取失效或污染，再切换为 `window_vectors=8` 或 `16` 的滚动窗口。

#### 策略分流

不同访问模式应使用不同预取强度：

| 条件 | 策略 |
| --- | --- |
| `runs == 1` 或地址近似连续 | 降低手动 prefetch 预算，甚至禁用；硬件预取通常已经足够 |
| `runs` 接近 batch size | random 离散访问，只预取 request-order 近端少量 span，或直接 no-prefetch |
| 多个 span 落在少数 bucket | hot/locality 场景，启用 bucket TopK |
| remote NC benchmark 无收益 | 默认降低 budget 或禁用 prefetch |

这里的 `runs` 是排序合并后的连续 cacheline 区间数量。`runs` 越小表示局部性越好，`runs` 越接近 batch size 表示访问越离散。

#### 输出统计

为了验证 TopK 是否按预期工作，结果统计需要增加：

| 字段 | 含义 |
| --- | --- |
| `candidate_spans` | 输入 span 数 |
| `candidate_lines` | 候选 span 覆盖的 cacheline 总数 |
| `selected_spans` | TopK 选中的 span 数 |
| `selected_lines` | 实际进入 prefetch 的 cacheline 数 |
| `selected_buckets` | 被选中的 bucket 数 |
| `prefetch_budget_lines` | 本次预算上限 |
| `plan_policy` | `sort_merge`、`bucket_topk`、`no_prefetch` 等 |

只有当 `selected_lines` 明显小于 `candidate_lines`，并且 p95/p99 或平均 copy ns 有改善时，TopK prefetch 才应进入默认路径。

### 12.7 L2/L3 选择

`prefetch_level=auto` 建议按预算选择：

| 条件 | 策略 |
| --- | --- |
| 当前窗口 cacheline 数较小，马上 copy | L2 keep |
| 当前窗口较大，或 copy 距离较远 | L3 keep |
| batch 很大，超过 L2 预算 | 分窗口，当前窗口 L2，后续窗口 L3 或不预取 |
| 随机离散访问比例高 | 降低 prefetch line 上限，避免污染 cache |

需要通过 benchmark 调整阈值。固定 1200B vector 的场景下，单 vector 约 19 条 64B line，100 个 vector 的全量 line 可能超过 L2 可承受范围，因此窗口化比全量预取更稳。

### 12.8 正确性约束

`new_load_vector_batch` 只能优化数据搬运时机，不能改变 TLC 一致性语义：

- source fence active 后，不能从旧 source 位置返回数据。
- tombstone key 不能从旧 warm/cache location 读出。
- `region_id` 和 `region_index` 仍是不同概念，不能假设二者相等。
- prefetch 必须只针对已经确认合法的 mapped region 地址。
- prefetch 不能越过 region 边界，不能对无效 handle 地址发 hint。
- remote meta 命中仍不是最终可信结果，必须通过本地 validate。
- copy 前后必须保持 `write_seq`、slot state、key/hash、`owner_generation` 校验。
- 出现 stale、writer race、migration cutover 时，按现有单条 load 语义重试或返回错误。

预取可以发生在 slot 初步校验之后，但不能替代最终 copy 前后的 seqlock 校验。即使预取把旧 cacheline 拉进本地 cache，只要 copy 后校验发现 `write_seq` 变化，也必须丢弃本次结果并重试或失败。

### 12.9 回退条件

以下情况建议直接回退到普通逐条 load 或 batch no-prefetch：

- `count` 小于最小阈值，例如小于 8。
- 非 ARM64/SVE 构建，或当前平台没有合适的 prefetch 指令。
- batch 中有效 handle 很少，plan 构建成本高于收益。
- `bytes` 不是固定 vector size，导致 plan 复杂度变高。
- 去重后的 cacheline 数超过预算且窗口化仍无法限制 cache 污染。
- 当前线程处在高写冲突或迁移活跃区间，stale/retry 比例较高。

第一版即使没有 SVE 专用路径，也可以通过 `__builtin_prefetch()` 或 ARM `PRFM` 做 portable prefetch，验证收益后再追加 SVE 地址批处理。

### 12.10 验证指标

需要新增 benchmark 对比以下路径：

| 路径 | 目的 |
| --- | --- |
| 原始 100 次 `vemb_v16_tlc_load_vector()` | baseline |
| batch load，不做 prefetch | 衡量批量 resolve/plan 的固定成本 |
| batch load + L2 prefetch | 衡量小窗口热数据收益 |
| batch load + L3 prefetch | 衡量大窗口和跨 vector 预热收益 |
| batch load + windowed prefetch | 衡量 cache 污染控制效果 |

建议采集：

- batch latency p50/p95/p99。
- 单 vector 平均 copy ns。
- `unique_cachelines / total_cachelines` 去重率。
- `prefetch_lines` 与 `copy_bytes` 的比例。
- stale/retry/error 计数。
- 硬件计数器中的 L1/L2/L3 refill、LLC miss、memory bandwidth。

只有当 batch prefetch 在目标 batch size 和真实 key 分布下稳定降低 p95/p99，才应把它接入默认读路径；否则保留为可配置优化路径。
