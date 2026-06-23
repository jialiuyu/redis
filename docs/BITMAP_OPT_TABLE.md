# Bitmap 测试结论整理

## 测试配置

| 项目 | 值 |
|------|----|
| 原子类型 | `atomic_uint_fast64_t` |
| 内存序 | `acquire/release/relaxed` |
| 对比实现 | `CAS optimized` / `CAS bounded retries` / `fetch_or` |
| Bounded CAS 最大重试次数 | 4 |
| 热点竞争窗口 | 64 bits shared by all threads |
| 每线程操作数 | 100000 |

## 关键结果

| 场景 | 实现 | 吞吐（M ops/sec） | 平均延迟（ns/op） | 成功率 | 总重试次数 |
|------|------|-------------------|-------------------|--------|------------|
| Low Concurrency / 2 线程 / Partitioned | CAS optimized | 13.00 | 77 | 100.00% | 0 |
| Low Concurrency / 2 线程 / Partitioned | CAS bounded retries | 21.16 | 47 | 100.00% | 0 |
| Low Concurrency / 2 线程 / Partitioned | fetch_or | 10.73 | 93 | 100.00% | 0 |
| Medium Concurrency / 4 线程 / Partitioned | CAS optimized | 12.41 | 81 | 100.00% | 0 |
| Medium Concurrency / 4 线程 / Partitioned | CAS bounded retries | 16.87 | 59 | 100.00% | 0 |
| Medium Concurrency / 4 线程 / Partitioned | fetch_or | 16.70 | 60 | 100.00% | 0 |
| High Concurrency / 8 线程 / Partitioned | CAS optimized | 7.93 | 126 | 100.00% | 0 |
| High Concurrency / 8 线程 / Partitioned | CAS bounded retries | 8.09 | 124 | 100.00% | 0 |
| High Concurrency / 8 线程 / Partitioned | fetch_or | 8.32 | 120 | 100.00% | 0 |
| Very High Concurrency / 16 线程 / Partitioned | CAS optimized | 7.58 | 132 | 100.00% | 0 |
| Very High Concurrency / 16 线程 / Partitioned | CAS bounded retries | 7.60 | 132 | 100.00% | 0 |
| Very High Concurrency / 16 线程 / Partitioned | fetch_or | 7.69 | 130 | 100.00% | 0 |
| Hotspot Concurrency / 8 线程 / High contention | CAS optimized | 3.04 | 329 | 54.37% | 2176040 |
| Hotspot Concurrency / 8 线程 / High contention | CAS bounded retries | 3.15 | 318 | 50.87% | 1787815 |
| Hotspot Concurrency / 8 线程 / High contention | fetch_or | 5.85 | 171 | 87.43% | 0 |
| Hotspot Very High Concurrency / 16 线程 / High contention | CAS optimized | 2.72 | 367 | 52.58% | 5337696 |
| Hotspot Very High Concurrency / 16 线程 / High contention | CAS bounded retries | 2.95 | 339 | 46.11% | 3535494 |
| Hotspot Very High Concurrency / 16 线程 / High contention | fetch_or | 5.61 | 178 | 82.61% | 0 |

## 简化结论

1. 在低冲突的 `Partitioned` 场景下，三种实现差距不大，`CAS bounded retries` 在本次测试里表现最好或接近最好。
2. 在热点竞争场景下，`CAS bounded retries` 比无限重试的 `CAS optimized` 更稳，重试次数和延迟都更低。
3. 但在热点竞争下，`fetch_or` 仍然明显最好，吞吐、延迟、成功率都领先两种 CAS 方案。
4. 这说明“有界重试 CAS”是比“无限重试 CAS”更适合 fail-fast 场景的折中方案，但它还不足以替代当前 `fetch_or`。

## 非 Partitioned 场景下 `fetch_or` 的量化优势

| 场景 | `fetch_or` vs `CAS optimized` | `fetch_or` vs `CAS bounded retries` |
|------|-------------------------------|--------------------------------------|
| Hotspot Concurrency / 8 线程 | 吞吐高 92.4%（5.85 vs 3.04），延迟低 48.0%（171 vs 329），成功率高 33.06 个百分点（87.43% vs 54.37%） | 吞吐高 85.7%（5.85 vs 3.15），延迟低 46.2%（171 vs 318），成功率高 36.56 个百分点（87.43% vs 50.87%） |
| Hotspot Very High Concurrency / 16 线程 | 吞吐高 106.3%（5.61 vs 2.72），延迟低 51.5%（178 vs 367），成功率高 30.03 个百分点（82.61% vs 52.58%） | 吞吐高 90.2%（5.61 vs 2.95），延迟低 47.5%（178 vs 339），成功率高 36.50 个百分点（82.61% vs 46.11%） |

结论可以直接概括为：

1. 在非 `Partitioned` 的热点竞争场景里，`fetch_or` 的吞吐通常比两种 CAS 高约 `1.86x` 到 `2.06x`。
2. 同时，`fetch_or` 的平均延迟约降低 `46%` 到 `52%`。
3. 成功率方面，`fetch_or` 相比两种 CAS 提高了约 `30` 到 `37` 个百分点。
4. 因此，如果实际负载不是稳定分区访问，而是存在明显热点重叠，那么 `fetch_or` 在当前测试中表现出更强、更稳定的量化优势。

## 结合当前 SuperNode 的结论

1. 当前 SuperNode 是共享队列加全局 bitmap，不是纯分区访问模型。
2. 所以 `Partitioned` 结果只能看作低冲突理想上界，不能直接代表真实线上收益。
3. 对当前架构来说，更有参考价值的是热点竞争结果。
4. 从这次结果看，如果真实流量热点明显，`fetch_or` 仍然是更合适的默认实现。
5. 如果后续把路由和队列模型改成更稳定的分区访问，再重新评估 `CAS bounded retries` 是否值得替换。
