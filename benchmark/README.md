# Redis UB+SVE 端到端性能对比测试

## 概述

这是一个完整的 C/C++ 基准测试套件，用于对比传统 Redis 集群和 SuperNode + UB.mem + SVE2 架构的性能。

**✅ 最新更新**：所有 SVE2 模拟逻辑已替换为真实的 ARM SVE2 指令实现。

## 测试场景

### 场景 1：传统 Redis 集群
- **规模**：30-40 万台 Redis 服务器（默认 35 万台）
- **用户 Embedding**：100 亿 UID（10 billion）
- **物品 Embedding**：1000 亿 Item（100 billion）
- **Embedding 维度**：300
- **单个 Embedding 大小**：300 × 4 bytes = 1.2 KB
- **单服务器 QPS**：10,000
- **总 QPS**：35 亿（3.5 billion）

### 场景 2：SuperNode + UB.mem + SVE2
- **规模**：150 个超节点
- **用户 Embedding**：100 亿 UID
- **物品 Embedding**：1000 亿 Item
- **Embedding 维度**：300
- **单超节点容量**：4TB
- **每节点 Worker 数**：16
- **批量大小**：3000 请求/批

## 性能目标

| 指标 | 传统 Redis | SuperNode 目标 | 收益比 |
|------|-----------|---------------|--------|
| 总吞吐量 | 35 亿 QPS | 240 亿 IOPS | 6.86x |
| 单请求延迟 | ~500 μs | < 100 μs | 5x |
| 硬件成本 | 35 万台 × $5k | 150 台 × $50k | 233x |
| 保守收益比 | 1x | 2.96x | 2.96x |
| 目标收益比 | 1x | 30-100x | 30-100x |

## 文件结构

```
benchmark/
├── README.md                          # 本文档
├── Makefile                           # 编译脚本
├── benchmark_common.h                 # 公共定义和工具函数
├── redis_traditional_benchmark.c      # 传统 Redis 基准测试
├── supernode_benchmark.c              # SuperNode 基准测试（真实 SVE2）
├── supernode_server.c                 # SuperNode 服务器实现
├── supernode_real_benchmark.c         # SuperNode 客户端（真实网络）
├── sve_compute_standalone.h           # SVE2 API 定义
├── sve_compute_standalone.c           # SVE2 真实实现
├── compare_results.c                  # 结果对比分析工具
├── run_full_benchmark.sh              # 完整测试脚本
├── ../docs/benchmark/COMPLETE_BENCHMARK_GUIDE.md        # 完整使用指南
├── ../docs/benchmark/SVE2_REAL_IMPLEMENTATION_COMPLETE.md # SVE2 实现文档
└── results/                           # 测试结果目录（自动创建）
    └── run_YYYYMMDD_HHMMSS/          # 每次运行的结果
        ├── redis_results.txt          # Redis 测试结果
        ├── supernode_results.txt      # SuperNode 测试结果
        ├── comparison_report.txt      # 对比报告
        └── report.html                # HTML 报告
```

## 快速开始

### 1. 编译基准测试

```bash
cd redis/benchmark
make
```

这将编译三个程序：
- `redis_traditional_benchmark` - 传统 Redis 模拟器
- `supernode_benchmark` - SuperNode 模拟器
- `compare_results` - 结果对比工具

### 2. 运行快速测试（推荐）

```bash
# 快速测试：1M 查询，8 线程，约 1-2 分钟
make test
```

### 3. 运行完整测试

```bash
# 完整测试：10M 查询，16 线程，约 10-15 分钟
make benchmark
```

### 4. 运行压力测试

```bash
# 压力测试：100M 查询，32 线程，约 1-2 小时
make stress
```

### 5. 使用自动化脚本

```bash
# 运行默认配置（10M 查询）
./run_full_benchmark.sh

# 快速测试
./run_full_benchmark.sh --quick

# 压力测试
./run_full_benchmark.sh --stress

# 自定义配置
./run_full_benchmark.sh --queries 50000000 --threads 32
```

## 详细使用说明

### 单独运行基准测试

#### 传统 Redis 测试

```bash
# 默认配置
./redis_traditional_benchmark

# 自定义配置
./redis_traditional_benchmark \
    --queries 10000000 \
    --threads 16 \
    --servers 350000

# 查看帮助
./redis_traditional_benchmark --help
```

#### SuperNode 测试

```bash
# 默认配置
./supernode_benchmark

# 自定义配置
./supernode_benchmark \
    --queries 10000000 \
    --threads 16 \
    --supernodes 150

# 查看帮助
./supernode_benchmark --help
```

#### 对比结果

```bash
# 对比两个测试结果
./compare_results redis_results.txt supernode_results.txt

# 保存对比报告
./compare_results redis_results.txt supernode_results.txt > comparison.txt
```

### 参数说明

| 参数 | 说明 | 默认值 | 推荐范围 |
|------|------|--------|----------|
| `--queries` | 测试查询数量 | 10,000,000 | 1M - 100M |
| `--threads` | 并发线程数 | 16 | 8 - 32 |
| `--servers` | Redis 服务器数量 | 350,000 | 300k - 400k |
| `--supernodes` | 超节点数量 | 150 | 100 - 200 |

### 测试配置建议

#### 快速验证（1-2 分钟）
```bash
./run_full_benchmark.sh --queries 1000000 --threads 8
```

#### 标准测试（10-15 分钟）
```bash
./run_full_benchmark.sh --queries 10000000 --threads 16
```

#### 压力测试（1-2 小时）
```bash
./run_full_benchmark.sh --queries 100000000 --threads 32
```

## 测试原理

### 传统 Redis 模拟

模拟以下延迟组件：

1. **网络延迟**：300 μs ± 50 μs（数据中心内网）
2. **磁盘 I/O**：150 μs ± 30 μs（20% 概率，SSD 随机读）
3. **CPU 处理**：40 μs ± 10 μs（查找 + 反序列化）

**总延迟**：约 400-500 μs（平均）

### SuperNode 模拟

模拟以下优化技术（使用真实 ARM SVE2 指令）：

1. **批量聚合**：200 μs（3000 请求/批）
2. **Ring Buffer 通信**：2 μs（零拷贝）
3. **Bitmap CAS**：30 ns（无锁并发控制）
4. **SVE2 Gather Load**：真实 ARM SVE2 指令（8-16x 并行）

**总延迟**：约 70-90 μs（平均，标量回退模式）
**预期延迟**：约 5-10 μs（ARM SVE2 硬件加速）

### 关键技术优势

| 技术 | 传统方案 | SuperNode 方案 | 提升 |
|------|----------|---------------|------|
| 批量处理 | 单请求 | 3000 请求/批 | 3000x |
| 并发控制 | 互斥锁 (~200 ns) | Bitmap CAS (~30 ns) | 6.7x |
| 内存访问 | 单次读取 | SVE2 并行 (8-16x) | 8-16x |
| 通信方式 | 网络 (~300 μs) | 共享内存 (~2 μs) | 150x |

## 输出示例

### 对比报告示例

```
╔════════════════════════════════════════════════════════════════════════════╗
║                    Redis UB+SVE 性能对比报告                              ║
╚════════════════════════════════════════════════════════════════════════════╝

┌─────────────────────────────────────────────────────────────────────────┐
│ 性能指标对比                                                            │
├──────────────────────────┬──────────────┬──────────────┬───────────────┤
│ 指标                     │ 传统 Redis   │ SuperNode    │ 收益比        │
├──────────────────────────┼──────────────┼──────────────┼───────────────┤
│ 吞吐量 (QPS)             │   35000000.00│  240000000.00│        6.86x  │
│ 平均延迟 (μs)            │       490.00 │        85.00 │        5.76x  │
└──────────────────────────┴──────────────┴──────────────┴───────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│ 综合评估                                                                │
├─────────────────────────────────────────────────────────────────────────┤
│ ✅ 吞吐量提升: 6.86x (达到保守目标 2.96x)                              │
│ ✅ 平均延迟: 85.00 μs (达到目标 < 100 μs)                              │
│ ✅ 硬件成本节省: 233.33x                                               │
├─────────────────────────────────────────────────────────────────────────┤
│ ✅ 总体评价: 良好 - 达到保守性能目标                                   │
└─────────────────────────────────────────────────────────────────────────┘
```

## 性能调优建议

### 1. 线程数配置

- **CPU 密集型**：线程数 = CPU 核心数
- **I/O 密集型**：线程数 = CPU 核心数 × 2
- **推荐**：从 16 开始，根据 CPU 核心数调整

### 2. 查询数量

- **快速验证**：1M 查询（1-2 分钟）
- **标准测试**：10M 查询（10-15 分钟）
- **压力测试**：100M 查询（1-2 小时）

### 3. 批量大小

- 当前固定为 3000（在 `benchmark_common.h` 中定义）
- 可以修改 `BATCH_SIZE` 测试不同批量大小的影响

## 故障排查

### 编译错误

```bash
# 检查 gcc 版本（需要支持 C11）
gcc --version

# 检查 pthread 库
gcc -pthread -xc - -o /dev/null <<< 'int main(){}'

# 清理并重新编译
make clean
make
```

### 运行时错误

```bash
# 检查可执行文件权限
chmod +x redis_traditional_benchmark
chmod +x supernode_benchmark
chmod +x run_full_benchmark.sh

# 检查内存（需要足够内存存储查询）
free -h
```

### 性能异常

- 确保系统负载较低
- 关闭不必要的后台进程
- 使用 `--threads` 参数调整并发度
- 减少 `--queries` 数量进行快速测试

## 扩展和定制

### 修改 Embedding 维度

编辑 `benchmark_common.h`：

```c
#define EMBEDDING_DIM 300  // 修改为其他维度
```

### 修改批量大小

编辑 `benchmark_common.h`：

```c
#define BATCH_SIZE 3000  // 修改为其他批量大小
```

### 修改延迟模型

编辑 `redis_traditional_benchmark.c` 或 `supernode_benchmark.c` 中的延迟模拟函数。

## 参考文档

- [完整基准测试指南](../docs/benchmark/COMPLETE_BENCHMARK_GUIDE.md) - 详细使用说明
- [SVE2 真实实现文档](../docs/benchmark/SVE2_REAL_IMPLEMENTATION_COMPLETE.md) - SVE2 实现细节
- [Redis 基线结果](REDIS_BASELINE_RESULTS.md) - Redis 性能分析
- [SuperNode 真实结果](SUPERNODE_REAL_RESULTS.md) - SuperNode 性能分析
- [对比报告](COMPARISON_REPORT.md) - 详细性能对比
- [最终总结](../docs/benchmark/FINAL_SUMMARY.md) - 执行摘要
- [Redis Quick Start](../docs/benchmark/QUICKSTART_REDIS_BASELINE.md)
- [SuperNode Quick Start](../docs/benchmark/QUICKSTART_SUPERNODE.md)
- [UB+SVE 集成文档](../docs/UB_SVE_INTEGRATION_README.md)
- [Bitmap CAS 优化文档](../docs/BITMAP_CAS_OPTIMIZATION.md)
- [快速开始指南](../docs/QUICKSTART_UB_SVE.md)
- [实现总结](../IMPLEMENTATION_SUMMARY.md)

## 许可证

与 Redis 主项目保持一致。
