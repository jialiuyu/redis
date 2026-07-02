# Budgeted TopK Prefetch 设计与测试结果

## 摘要

Budgeted TopK Prefetch 是 `benchmark/vemb_v16_ub_batch_load_perf_ut.c` 中用于 UB batch vector load 的预取选择策略。它不改变 vector copy 的正确性路径，只改变 copy 前发出的 prefetch hint 地址。

当前实现的核心结论是：

- `seq/random/hot` 等明显不适合手动预取的场景，会通过 sample fast skip 快速返回 no-prefetch policy。
- `cluster` 这类中等局部性场景会进入 `bucket_topk`，从完整 batch 的候选 span 中只选少量高价值 span 预取。
- 最新远端 UB/CC 数据显示，`bucket_topk` 能稳定减少 `sort_merge` 的过量预取，相比 `sort_merge` 有小幅收益；但在 `batch=64` 下仍未稳定赢过直接 baseline load。
- 扩大 batch 后 TopK 有机会赢过 baseline；当前一次扫描中 `batch=256` 获得约 3.8% 提升，但 `batch=512` 又退化，说明收益不是单调随 batch 增长。

## 背景

旧的 `sort_merge` 预取路径会为 batch 中每个 vector 生成 cacheline run，再按地址排序合并，并从合并后的 run 中按 cacheline 发 prefetch hint。这个路径能验证基本预取行为，但它本质上倾向于预取地址排序后的候选区间，并不判断哪些 cacheline 最值得进入 cache。

固定 300 维 float vector 的 payload 为 1200B，约覆盖 19 条 64B cacheline。`batch=64` 时，全量 payload 约 1248 条 cacheline。即使实际 prefetch 被 `window_lines` 或 budget 截断，盲目扩大预取范围仍可能挤占 L1/L2/L3 的有效工作集，使 `PLDKeep` 或 `__builtin_prefetch()` 变成 cache pollution 来源。

因此 batch prefetch 需要先设定 cache budget，再只选择 TopK 的高价值地址区间。

## 设计目标

- 预取规模由 cache budget 控制，而不是由 batch size 直接决定。
- 优先预取马上会 copy、局部性更强、cacheline 成本更低的数据。
- 对顺序、极热、极散、non-cacheable 场景快速跳过手动预取。
- 保留原始 request order 的 copy 语义，不改变读路径正确性。
- 为 standalone benchmark 和后续 TLC batch load 提供同一类 plan 思路。

## 核心概念

### Span

`prefetch_span_t` 表示一个请求对应的 vector payload 在内存中覆盖的 cacheline 地址区间：

```text
begin = align_down(vector_addr, cacheline_size)
end = align_up(vector_addr + vector_bytes, cacheline_size)
lines = (end - begin) / cacheline_size
```

一个 span 是 TopK planner 的基本输入单元。TopK 最终选择的是若干 bucket，再把这些 bucket 中可放入预算的 span 输出成 prefetch run。

### Bucket

bucket 用来聚合同一粗粒度地址区域内的 span：

```text
bucket_key = (region_index, begin >> UB_BATCH_PREFETCH_BUCKET_SHIFT)
```

当前默认 `UB_BATCH_PREFETCH_BUCKET_SHIFT=16`，即 64KB bucket。64KB bucket 在 64B cacheline 下最多覆盖 1024 条 cacheline。不同 `region_index` 的相同 prefix 不会合并。

每个 bucket 维护：

| 字段 | 含义 |
| --- | --- |
| `span_count` | 该 bucket 命中的 vector span 数 |
| `line_estimate` | 预取这些 span 的 cacheline 成本估计 |
| `min_begin/max_end` | bucket 内 span 覆盖范围 |
| `first_request_index` | 最早在 batch 中被访问的位置 |

### Budget

全局预取预算来自编译期配置：

```c
#define UB_BATCH_PREFETCH_BUDGET_BYTES (16u * 1024u)
```

在 64B cacheline 下对应 256 lines。进入 `bucket_topk` 后，当前实现进一步收紧为：

```c
#define UB_BATCH_PREFETCH_TOPK_BUDGET_LINES 128u
```

这个 cap 用于中等局部性场景，避免 TopK 选出过多 span。远端 cluster 数据显示，约 101 lines 的实际 selected lines 比全量 1248 lines 更稳。

## 策略分流

当前 planner 分为 sample fast skip、full-batch skip 和 bucket TopK 三层。

### Sample Fast Skip

sample 阶段只查看前 `UB_BATCH_PREFETCH_SAMPLE_VECTORS=16` 个 request。它的作用是快速识别明显不值得手动预取的 batch，并把已解析的 span 留给后续 full planner 复用。

| 条件 | policy | 含义 |
| --- | --- | --- |
| no budget | `no_prefetch` | 没有可用 prefetch budget |
| `--cacheable no` | `no_prefetch_nc` | non-cacheable 映射不做 cache prefetch |
| sample 是 forward-contiguous stream | `no_prefetch_contig_sample` | 顺序流通常由硬件预取处理 |
| sample bucket span 很小 | `no_prefetch_hot_sample` | hot/cache-resident 场景不值得手动预取 |
| sample bucket span 很大 | `no_prefetch_sparse_sample` | sparse/random 场景容易污染 cache |

### Full-Batch Skip

如果 sample 不能证明应该跳过，planner 会补齐剩余 span，再基于完整 batch 做第二轮判断：

| 条件 | policy | 含义 |
| --- | --- | --- |
| 完整 batch 连续 | `no_prefetch_contig` | 仍交给硬件预取 |
| 单 region 且 bucket span 很小 | `no_prefetch_hot_fast` | 完整 batch 很集中 |
| 单 region 且 bucket span 大于等于 batch | `no_prefetch_sparse_fast` | 完整 batch 很离散 |
| 实际 bucket 数少于等于 8 | `no_prefetch_hot` | bucket 聚合后仍过热 |
| 实际 bucket 数接近 batch size | `no_prefetch_sparse` | bucket 聚合后仍过散 |

只有既不连续、不极热、也不极散的中等局部性 batch 才进入 `bucket_topk`。

### Bucket TopK

TopK 对每个 bucket 计算一个启发式分数：

```text
locality_score = bucket.span_count * 8
urgency_score = max(0, UB_BATCH_PREFETCH_WINDOW_VECTORS - bucket.first_request_index)
cost_score = bucket.line_estimate
score = locality_score + urgency_score - cost_score
```

公式含义是：奖励 bucket 复用和早期访问，惩罚 cacheline 成本。

bucket 选择使用贪心策略：每轮从尚未选择且能放入剩余预算的 bucket 中选出最优 bucket。比较规则为：

1. `score` 更高优先。
2. `first_request_index` 更小优先。
3. `line_estimate` 更小优先。
4. `prefix` 更小作为稳定 tie-break。

选中 bucket 后，planner 再按 request order 扫描 span，只把属于已选 bucket 且仍能放入 budget 的 span 转成 final prefetch run。最后用 `merge_runs_small()` 合并相邻或重叠 run，减少最终 prefetch run 数。

## 当前实现

当前落地路径位于 `benchmark/vemb_v16_ub_batch_load_perf_ut.c`。benchmark 输出三类对照：

- `baseline`：不 plan、不 prefetch，按 request order 调用 `sve_streaming_load_f32()` 逐 vector load。
- `batch-plan`：旧 `sort_merge` 对照路径，统计全量候选 run。
- `batch-prefetch`：sample fast skip；不能跳过时进入 bucket TopK。

当前支持的访问模式：

| pattern | 含义 | 典型 policy |
| --- | --- | --- |
| `seq` | 顺序访问 | `no_prefetch_contig_sample` |
| `random` | 全 rows 范围随机访问 | `no_prefetch_sparse_sample` |
| `hot` | 小 hot range 内随机访问 | `no_prefetch_hot_sample` |
| `cluster` | 多个相邻 bucket 内随机访问 | `bucket_topk` |

`cluster` 用于模拟中等局部性：batch 内请求分布在约 16 个相邻 64KB bucket，每个 bucket 有多个 vector。它是当前最容易触发 TopK 的模式。

当前 `bucket_topk` 流程如下：

```text
sample first 16 spans
if sample can skip:
    return no-prefetch policy

collect remaining spans
aggregate 64KB buckets
apply full-batch skip guards
cap TopK budget to 128 lines
score buckets
select buckets under budget
expand selected buckets back to request-order spans
merge adjacent/overlapping runs
prefetch selected runs cacheline by cacheline
copy batch in original request order
```

注意：曾尝试“选中 bucket 后直接预取 bucket 的 `min_begin..max_end`”，但 cluster 中 bucket 内空洞较多，实际 hint lines 从约 102 增到约 476，overfetch 明显，因此当前实现回退为 span 级 run 输出。

## 测试环境

测试机器和路径来自 `benchmark/test_host.md`：

```text
ssh -p 22 root@192.168.90.111
cd /root/szz/codespace/hpc-redis
CC path: /dev/obmm_shmdev1
NC path: /dev/obmm_shmdev5
```

主要测试参数：

```text
--ub-path /dev/obmm_shmdev1
--cacheable yes
--rows 8192
--dim 300
--pattern cluster
USE_SVE=yes
checksum disabled
```

## 测试结果

### Batch 64 复测

2026-07-02 同步本地当前代码到远端后，`batch=64`、`iters=1000` 的 cluster 复测结果如下：

| run | baseline | sort_merge | bucket_topk | bucket_topk plan | prefetch | policy |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 78.85 | 89.25 | 87.54 | 1901.98 | 215.20 | `bucket_topk` |
| 2 | 71.85 | 88.32 | 87.38 | 1892.35 | 215.96 | `bucket_topk` |
| 3 | 75.65 | 89.67 | 87.27 | 1886.02 | 216.50 | `bucket_topk` |
| 4 | 68.08 | 89.11 | 86.73 | 1858.10 | 215.30 | `bucket_topk` |
| 5 | 68.91 | 89.01 | 86.95 | 1863.21 | 215.56 | `bucket_topk` |
| avg | 72.67 | 89.07 | 87.17 | 1880.33 | 215.70 | `bucket_topk` |

这组数据中：

- `bucket_topk` 从约 1248 candidate lines 中选择约 101 lines。
- 最终 prefetch runs 约 5 个，选中 bucket 约 2.4 个。
- `bucket_topk` 相比 `sort_merge` 平均提升约 2.1%。
- `bucket_topk` 相比当前 baseline 平均慢约 20.0%。
- 主要成本仍是 TopK planning，约 1.86-1.90 us/batch。

### Batch 扩大扫描

为观察 batch size 对 TopK 的影响，保持总 vector 数大致接近，执行了一组 cluster 扫描：

| batch | iters | baseline | sort_merge | bucket_topk | speedup vs baseline |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 1000 | 81.50 | 88.55 | 87.04 | 0.936x |
| 128 | 500 | 72.56 | 99.07 | 81.04 | 0.895x |
| 256 | 250 | 80.74 | 109.67 | 77.79 | 1.038x |
| 512 | 125 | 65.18 | 121.26 | 74.48 | 0.875x |

这组数据说明：

- batch 变大后，`sort_merge` 的全量候选成本增长明显，TopK 更容易体现“少预取”的价值。
- `batch=256` 下 `bucket_topk` 比 baseline 快约 3.8%，是当前扫描中最好的点。
- `batch=512` 未继续提升，说明 TopK 收益不是随 batch 单调增长；planning 成本、budget 固定 cap、cache 状态都会影响结果。

### NC Path

remote NC path 测试显示 prefetch 基本无收益，因此 `--cacheable no` 默认返回 `no_prefetch_nc`。当前策略不会在 NC path 上发 cache prefetch hint。

## 正确性约束

Budgeted TopK Prefetch 只能改变 prefetch 地址选择，不能改变读路径正确性：

- prefetch 只能对已经解析合法的 mapped region 地址发 hint。
- prefetch 不能越过 region mapping 边界。
- prefetch 不要求覆盖每个后续 load；`prefetch_runs` 是 `load_vector` 读地址集合的子集。
- copy 顺序仍按原始 request order。
- copy 前后仍要保持 `write_seq`、slot state、key/hash、`owner_generation` 校验。
- `region_id` 和 `region_index` 仍是不同概念，不能假设二者相等。
- source fence、cutover、source_gc、tombstone 语义不能被 prefetch 绕过。

即使 prefetch 把旧 cacheline 拉入本地 cache，只要 copy 后校验发现 `write_seq` 变化，也必须丢弃本次结果并重试或失败。

## 结论

当前 Budgeted TopK Prefetch 的设计目标已经在 standalone benchmark 中落地：它能识别不适合手动预取的访问模式，并在中等局部性场景下将候选 prefetch lines 从约 1248 lines 降到约 101 lines。

最新数据表明：

- `bucket_topk` 的选择行为稳定。
- `bucket_topk` 能减少 `sort_merge` 的过量预取，并在 `batch=64` 下稳定略快于 `sort_merge`。
- 当前 `batch=64` 下整体仍未稳定赢过 baseline，主要原因是 TopK planning 成本较高。
- 扩大 batch 可以放大 TopK 的价值，`batch=256` 曾出现约 3.8% 的 baseline 提升，但收益仍受 cache 状态和 planning 成本影响。

因此，TopK 目前适合作为实验路径和 `sort_merge` 的改进方向；是否作为默认路径，还需要继续降低 planning 成本，并在更稳定的 benchmark 条件下确认 median/p95 收益。

## 后续工作

- 对 `batch=192/256/320` 做多轮 median 测试，确认 batch size 的收益窗口。
- 降低 `bucket_topk` 的 `plan ns/batch`，特别是 bucket 聚合和 bucket selection 成本。
- 评估固定 K bucket 选择，减少贪心选择循环成本。
- 根据 batch size、bucket 命中率、NC/CC path 自动调整 TopK budget。
- 评估 request-order rolling window，避免过早预取远端 request。
- 接入 TLC batch load 时，保持 resolve、prefetch、copy、校验阶段分离。
