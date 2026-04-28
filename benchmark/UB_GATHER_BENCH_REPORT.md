# UB gather_load 跨节点性能测试报告

## 测试环境

- 节点: 鲲鹏 920 (ARMv8.2-a + SVE 256-bit)
- UB 链路: UB 2.0 跨节点
- 写端 memid=5, 读端 memid=1
- shm_size=8G, rows=1
- 编译: gcc -O3 -march=native (SVE) / scalar

## dim=64, rows=1, data=256 bytes

| 场景 | 方法 | 耗时 (ns) |
|---|---|---|
| UB.MEM | SVE gather-load | 710 |
| local mock | SVE gather-load | 90 |
| UB.MEM | scalar memcpy | 5660 |

## dim=4096, rows=1, data=16384 bytes

| 场景 | 方法 | 耗时 (ns) |
|---|---|---|
| UB.MEM | SVE gather-load | 13130 |
| local mock | SVE gather-load | 440 |
| UB.MEM | scalar memcpy | 265621 |

## 跨 dim 对比 (SVE gather-load, UB.MEM)

| dim | data (bytes) | 耗时 (ns) | SVE vs memcpy 加速比 |
|---|---|---|---|
| 64 | 256 | 710 | 7.97x |
| 4096 | 16384 | 13130 | 20.23x |

## 分析

- UB.MEM 单次访问延迟约 710 ns (dim=64)，符合 UB 2.0 跨节点延迟预期 (500ns~2μs)
- local mock 基线 90 ns (dim=64)，数据在 L2/L3 cache，UB 链路引入约 620 ns 额外延迟
- dim 增大 64x (64→4096)，UB.MEM 耗时仅增大 18.5x，大 dim 有效摊薄固定延迟
- SVE vs scalar memcpy 加速比随 dim 增大而增大 (8x→20x)，大数据量下 SVE 优势显著
- scalar memcpy 在 UB.MEM 上 dim=4096 时耗时 265μs，说明 memcpy 对远端内存极不友好
