# Redis UB+SVE2 快速开始指南 (v10)

## 环境要求

- Kunpeng 920/930 (aarch64, SVE2 256-bit)
- 可选: Ascend 910C NPU (用于 CPU/NPU 对比)

## 1. 启动服务

```bash
cd /sharedata/qiuwu/redis
mkdir -p /tmp/redis-test-baseline /tmp/redis-test-ub

# 清理端口 (如被占用)
for p in 6379 6380; do
  pid=$(lsof -ti :$p 2>/dev/null)
  [ -n "$pid" ] && echo "Port $p: killing PID $pid" && kill -9 $pid && sleep 0.5
done

# Baseline Redis (端口 6379)
./src/redis-server ./redis-baseline.conf

# Optimized + TLC Module (端口 6380)
./src/redis-server ./redis-ub-sve.conf

# 验证
./src/redis-cli -p 6379 ping   # → PONG
./src/redis-cli -p 6380 ping   # → PONG
./src/redis-cli -p 6380 MODULE LIST  # → tlc module
```

## 2. 填充真实数据 (1M+)

```bash
# 通过网络接口填充 1.1M 条 1200B 数据到 UB 内存三层缓存
./src/redis-cli -p 6380 TLC.FILL 1100000
# → "Filled 1100000 entries in 1.720 s (0.64 M ops/s)"

# 查看统计
./src/redis-cli -p 6380 TLC.STATS
```

## 3. 运行 Benchmark

```bash
# hiredis 多线程网络 benchmark (推荐)
./benchmark/tlc_client_bench --ops 500000 --threads 8 --pipeline 16

# 或用 redis-benchmark 标准工具
./src/redis-benchmark -p 6379 -t set,get -q -n 500000 -c 50 --threads 8 -d 1200
./src/redis-benchmark -p 6380 -t set,get -q -n 500000 -c 50 --threads 8 -d 1200
```

## 4. TLC 命令

| 命令 | 说明 |
|------|------|
| `TLC.PUT <id> <value>` | 写入 (1200B, UB 内存) |
| `TLC.GET <id>` | 读取 (HOT→WARM→COLD) |
| `TLC.MPUT <id1> <v1> ...` | 批量写入 |
| `TLC.MGET <id1> ...` | 批量读取 |
| `TLC.FILL <count>` | 预填充随机数据 |
| `TLC.STATS` | 缓存统计 |

## 5. 性能结果

| 场景 | Baseline | TLC Module | 提升 |
|------|----------|-----------|------|
| 80R/20W 无 Pipeline | 123K QPS | 213K QPS | **1.73x** |
| 80R/20W P=16 | 747K QPS | 828K QPS | **1.11x** |
| 100% GET P=16 | 786K QPS | 889K QPS | **1.13x** |

## 6. 关闭

```bash
./src/redis-cli -p 6379 shutdown nosave
./src/redis-cli -p 6380 shutdown nosave
```

详细报告见 `V10_BENCHMARK_REPORT.md`。
