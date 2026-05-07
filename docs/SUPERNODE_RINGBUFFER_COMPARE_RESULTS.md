# SuperNode Ring Buffer 设计对照结果

## 对比对象

本次对照 benchmark：

[ring_buffer_compare_bench.c](/Users/szza/codespace/work/hpc-redis/benchmark/ring_buffer_compare_bench.c)

对比两种设计：

1. `legacy_shared_queue`
   近似原始方案：共享 queue、多 worker 竞争消费、复制路径、肥 packet
2. `refactored_per_worker_queue`
   重构后方案：per-worker queue、SPSC、zero-copy 读写、slim packet

## 测试参数

| 参数 | 值 |
|------|----|
| workers | 4 |
| batches | 50000 |
| requests per batch | 32 |

## 结果表

| 方案 | packet bytes | total time (ms) | batch throughput (M batch/s) | request throughput (M req/s) | consumed batches | consumed requests |
|------|--------------|-----------------|-------------------------------|------------------------------|------------------|-------------------|
| legacy_shared_queue | 8992 | 68.738 | 0.73 | 23.28 | 50000 | 1600000 |
| refactored_per_worker_queue | 548 | 6.183 | 8.09 | 258.77 | 50000 | 1600000 |

## 量化对比

| 指标 | 重构后相对原始方案 |
|------|--------------------|
| packet 大小 | 降低 93.91%（8992 -> 548） |
| 总耗时 | 降低 91.01%（68.738 ms -> 6.183 ms） |
| batch 吞吐 | 提升 10.99x（0.73 -> 8.09 M batch/s） |
| request 吞吐 | 提升 11.12x（23.28 -> 258.77 M req/s） |

## 结果解读

1. 两种方案的 `consumed_batches` 和 `consumed_requests` 都与理论值完全一致，说明结果不是靠丢包换来的。
2. 重构后方案最直接的收益来自两点：
   - queue 从共享竞争改成 `per-worker queue + SPSC`
   - packet 从 `8992 bytes` 缩到 `548 bytes`
3. 在这组 standalone 对照测试下，重构后方案的 request 吞吐达到原始方案的约 `11.12x`。
4. 总耗时从 `68.738 ms` 降到 `6.183 ms`，说明队列竞争和消息搬运成本都被明显压缩。

## 结论

1. 从这组对照数据看，ring buffer 重构不是小优化，而是数量级改善。
2. 即使不考虑更高层的 UB/SVE 计算收益，仅队列路径本身，重构方案已经表现出显著优势。
3. 当前结果支持继续沿着：
   `per-worker queue`、`SPSC`、`zero-copy`、`slim packet`
   这条方向推进。 
