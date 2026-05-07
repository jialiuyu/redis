# SVE Row Copy Compare 结果整理

## 测试说明

该测试对比两种 SVE row copy 实现：

- `current`：当前 `sve_serial_contiguous_read()` 使用的“满向量循环 + 尾部谓词”实现
- `legacy`：旧的“始终使用 full predicate”实现

日志来源：[sve_row_copy_compare_log](/Users/szza/codespace/work/hpc-redis/benchmark/sve_row_copy_compare_log)

## 结果表

| dim | vl | current ns/iter | legacy ns/iter | current bytes/ns | legacy bytes/ns | speedup | current result | current tail | legacy result | legacy tail |
|-----|----|-----------------|----------------|------------------|-----------------|---------|----------------|--------------|---------------|-------------|
| 256 | 8 | 16.98 | 63.04 | 60.29 | 16.24 | 3.71x | ok | clean | ok | clean |
| 300 | 8 | 20.62 | 73.86 | 58.19 | 16.25 | 3.58x | ok | clean | ok | dirty |
| 512 | 8 | 29.69 | 119.87 | 68.98 | 17.08 | 4.04x | ok | clean | ok | clean |
| 1024 | 8 | 77.09 | 232.17 | 53.13 | 17.64 | 3.01x | ok | clean | ok | clean |

## 量化对比

| dim | current 相比 legacy 的延迟改善 | current 相比 legacy 的带宽提升 |
|-----|-------------------------------|-------------------------------|
| 256 | 271.26% | 271.24% |
| 300 | 258.20% | 258.09% |
| 512 | 303.74% | 303.86% |
| 1024 | 201.17% | 201.19% |

## 简要结论

1. `current` 在 4 个测试维度上全部快于 `legacy`，速度提升范围为 `3.01x` 到 `4.04x`。
2. 最快提升出现在 `dim=512`，`current` 比 `legacy` 快 `4.04x`。
3. `dim=300` 时，`legacy` 的 `tail=dirty`，说明旧实现会污染尾部填充值；`current` 为 `tail=clean`，说明当前实现修复了尾部越界写问题。
4. 因此，从性能和正确性两方面看，当前 row copy 实现都优于旧实现，没有回退理由。

## 面向代码结论

1. 当前 `copy_current` 采用“整向量批量拷贝 + 尾部单独谓词处理”的写法是正确方向。
2. 旧的 `copy_legacy` 在非向量长度整数倍的维度上存在尾部脏写风险，不适合继续保留为默认路径。
3. 对于类似 `300` 维 embedding 这种实际常见维度，当前实现既更快，也更安全。 
