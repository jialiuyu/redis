# Redis Baseline Benchmark 快速开始指南

## 概述

这个 benchmark 工具用于测试真实 Redis 服务器的性能，为 SuperNode 方案提供对比基准。

## 前置条件

1. **Redis 服务器**: 需要一个运行中的 Redis 服务器
2. **编译工具**: gcc, make
3. **依赖库**: hiredis (已包含在 redis/deps/hiredis)

## 快速开始

### 1. 编译 Redis 和 Benchmark

```bash
# 进入 Redis 目录
cd /sharedata/qiuwu/moreai/redis

# 编译 Redis
make -j$(nproc)

# 编译 Benchmark
cd benchmark
make redis_traditional_benchmark
```

### 2. 启动 Redis 服务器

如果还没有运行 Redis 服务器:

```bash
# 在 redis 目录下
./src/redis-server --port 6381 --save "" --appendonly no
```

或者检查是否已有 Redis 运行:

```bash
./src/redis-cli -p 6381 ping
# 应该返回: PONG
```

### 3. 运行 Benchmark

#### 快速测试 (10K 查询)
```bash
cd benchmark
./redis_traditional_benchmark --queries 10000 --threads 4
```

#### 标准测试 (1M 查询)
```bash
./redis_traditional_benchmark --queries 1000000 --threads 16
```

#### 完整测试 (10M 查询)
```bash
./redis_traditional_benchmark --queries 10000000 --threads 16 --servers 350000
```

## 命令行参数

```
--queries N     查询数量 (默认: 10000000)
--threads N     线程数量 (默认: 16)
--servers N     模拟服务器数量 (默认: 350000)
--help          显示帮助信息
```

## 输出说明

### 实时输出
```
Thread 0: 100000 / 625000 queries (avg latency: 126.31 μs)
```
- 显示每个线程的进度和平均延迟

### 最终结果
```
========================================
Redis Baseline Results
========================================
Total time: 161.06 seconds
Total requests: 10000000
Throughput: 62089.01 QPS (0.06 M QPS)

Latency:
  Average: 127.98 μs
  Min: 18.20 μs
  Max: 194879.26 μs
========================================
```

### 全规模估算
```
========================================
Full Scale Estimation
========================================
Total embeddings: 110 billion
Measured single-server QPS: 62089.01
Average latency: 127.98 μs
Estimated total QPS: 21731154148 (21.73 billion)
Time to process all: 5.06 seconds (0.08 minutes)

Hardware Cost:
  Servers: 350000
  Estimated cost: $1750 million (@ $5k per server)
========================================
```

## 自动化测试

运行完整的测试套件:

```bash
./test_redis_baseline.sh
```

这将运行多个规模的测试并生成汇总报告。

## 结果文件

测试结果保存在 `results/` 目录:

```
results/
├── redis_baseline_10M.txt          # 10M 查询测试结果
├── baseline_YYYYMMDD_HHMMSS/       # 自动化测试结果
│   ├── test_10k.txt
│   ├── test_100k.txt
│   ├── test_1m.txt
│   ├── test_10m.txt
│   └── SUMMARY.md
```

## 性能调优建议

### 1. 增加并发连接
```bash
./redis_traditional_benchmark --queries 10000000 --threads 32
```

### 2. 调整 Redis 配置
```bash
# 增加最大客户端连接数
redis-cli CONFIG SET maxclients 10000

# 禁用持久化以提高性能
redis-cli CONFIG SET save ""
redis-cli CONFIG SET appendonly no
```

### 3. 系统优化
```bash
# 增加文件描述符限制
ulimit -n 65535

# 禁用 THP (Transparent Huge Pages)
echo never > /sys/kernel/mm/transparent_hugepage/enabled
```

## 常见问题

### Q: 连接 Redis 失败
```
❌ Cannot connect to Redis at 127.0.0.1:6381
```

**解决方法**:
1. 检查 Redis 是否运行: `ps aux | grep redis-server`
2. 检查端口是否正确: `netstat -tlnp | grep 6381`
3. 启动 Redis: `./src/redis-server --port 6381`

### Q: 编译失败
```
fatal error: hiredis.h: No such file or directory
```

**解决方法**:
1. 确保在 redis 目录下先编译: `make -j$(nproc)`
2. 检查 hiredis 库: `ls deps/hiredis/libhiredis.a`

### Q: 性能不稳定
```
Max: 194879.26 μs (很高的最大延迟)
```

**可能原因**:
1. Redis 内存不足，触发 swap
2. 系统负载过高
3. 网络抖动

**解决方法**:
1. 增加 Redis 内存: `redis-cli CONFIG SET maxmemory 8gb`
2. 减少并发线程数
3. 清理 Redis 数据: `redis-cli FLUSHALL`

## 技术细节

### 数据格式
- **Key**: `emb:{id}` (例如: `emb:12345`)
- **Value**: 二进制 float 数组 (300 维 * 4 字节 = 1200 字节)

### 测试流程
1. 建立 N 个 Redis 连接（N = 线程数）
2. 生成随机查询（80% UID, 20% Item）
3. 多线程并发执行查询
4. 如果 key 不存在，生成并存储随机 Embedding
5. 统计延迟和吞吐量

### 性能指标
- **QPS**: 每秒查询数
- **延迟**: 从发送请求到收到响应的时间
- **吞吐量**: 单位时间内处理的请求数

## 相关文档

- [REDIS_BASELINE_RESULTS.md](REDIS_BASELINE_RESULTS.md) - 详细测试结果
- [REDIS_BASELINE_IMPLEMENTATION.md](REDIS_BASELINE_IMPLEMENTATION.md) - 实现细节
- [README.md](README.md) - Benchmark 套件总览

## 联系方式

如有问题，请查看相关文档或联系开发团队。

---

**最后更新**: 2026-02-10
**版本**: 1.0
