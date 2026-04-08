# SuperNode Benchmark 快速开始指南

## 概述

本指南帮助你快速运行 SuperNode vs Redis 性能对比测试。

## 前置条件

- Linux 系统 (推荐 Ubuntu 20.04+)
- GCC 编译器
- 至少 128 GB RAM (用于 SuperNode 服务器)
- Redis 服务器 (用于 baseline 测试)

## 快速开始 (5 分钟)

### 1. 编译所有程序

```bash
cd /sharedata/qiuwu/moreai/redis/benchmark
make clean
make all
```

预期输出:
```
✅ Built: redis_traditional_benchmark
✅ Built: supernode_benchmark
✅ Built: compare_results
✅ Built: supernode_real_benchmark
✅ Built: supernode_server
```

### 2. 启动 SuperNode 服务器

```bash
# 在一个终端窗口中启动
./supernode_server --port 6388
```

预期输出:
```
Allocating embedding store: 111.76 GB...
Initializing embeddings...
  Initialized 10000000 / 100000000 embeddings
  ...
✅ Embedding store initialized

========================================
SuperNode Server Started
========================================
Listening on port: 6388
Max embeddings: 100000000
Embedding dimension: 300
========================================
```

**注意**: 初始化需要 2-3 分钟，请耐心等待。

### 3. 运行快速测试 (1K 查询)

在另一个终端窗口中:

```bash
# 测试 SuperNode
./supernode_real_benchmark --queries 1000 --threads 2 --supernodes 150
```

预期输出:
```
========================================
SuperNode Real Benchmark
========================================
...
Throughput: 659174.92 QPS (0.66 M QPS)
Average latency: 2.49 μs
========================================
```

### 4. 运行完整测试 (10M 查询)

```bash
# 测试 SuperNode (约 2 秒)
./supernode_real_benchmark --queries 10000000 --threads 16 --supernodes 150 \
    | tee results/supernode_real_10M.txt

# 测试 Redis (约 160 秒，需要 Redis 服务器在 6381 端口)
./redis_traditional_benchmark --queries 10000000 --threads 16 --servers 350000 \
    | tee results/redis_baseline_10M.txt
```

### 5. 查看结果

```bash
# SuperNode 结果
cat results/supernode_real_10M.txt

# Redis 结果
cat results/redis_baseline_10M.txt

# 对比报告
cat COMPARISON_REPORT.md
```

## 详细步骤

### 步骤 1: 准备 Redis 服务器 (用于 baseline)

如果还没有 Redis 服务器:

```bash
# 编译 Redis
cd /sharedata/qiuwu/moreai/redis
make -j$(nproc)

# 启动 Redis
./src/redis-server --port 6381 --save "" --appendonly no
```

验证 Redis 运行:
```bash
./src/redis-cli -p 6381 ping
# 应该返回: PONG
```

### 步骤 2: 编译 Benchmark 程序

```bash
cd /sharedata/qiuwu/moreai/redis/benchmark

# 清理旧文件
make clean

# 编译所有程序
make all
```

编译成功后，你应该看到以下可执行文件:
- `redis_traditional_benchmark` - Redis baseline 测试
- `supernode_benchmark` - SuperNode 模拟测试
- `supernode_real_benchmark` - SuperNode 真实测试
- `supernode_server` - SuperNode 服务器
- `compare_results` - 结果对比工具

### 步骤 3: 启动 SuperNode 服务器

```bash
# 启动服务器 (需要 128 GB+ RAM)
./supernode_server --port 6388
```

**初始化过程**:
1. 分配 111.76 GB 内存
2. 初始化 1 亿个 embeddings
3. 每个 embedding 300 维
4. 总共需要 2-3 分钟

**内存不足?**
如果内存不足，可以修改 `supernode_server.c` 中的 `MAX_EMBEDDINGS`:
```c
#define MAX_EMBEDDINGS (10 * 1000 * 1000)  /* 改为 1000 万 */
```
然后重新编译。

### 步骤 4: 运行测试

#### 测试 1: 小规模测试 (1K 查询)

```bash
# SuperNode
./supernode_real_benchmark --queries 1000 --threads 2

# Redis
./redis_traditional_benchmark --queries 1000 --threads 2
```

#### 测试 2: 中等规模测试 (100K 查询)

```bash
# SuperNode (约 0.1 秒)
./supernode_real_benchmark --queries 100000 --threads 8

# Redis (约 2 秒)
./redis_traditional_benchmark --queries 100000 --threads 8
```

#### 测试 3: 大规模测试 (1M 查询)

```bash
# SuperNode (约 0.2 秒)
./supernode_real_benchmark --queries 1000000 --threads 16

# Redis (约 16 秒)
./redis_traditional_benchmark --queries 1000000 --threads 16
```

#### 测试 4: 完整测试 (10M 查询)

```bash
# SuperNode (约 1.7 秒)
./supernode_real_benchmark --queries 10000000 --threads 16 --supernodes 150 \
    | tee results/supernode_real_10M.txt

# Redis (约 161 秒)
./redis_traditional_benchmark --queries 10000000 --threads 16 --servers 350000 \
    | tee results/redis_baseline_10M.txt
```

### 步骤 5: 分析结果

#### 查看原始结果

```bash
# SuperNode 结果
cat results/supernode_real_10M.txt

# Redis 结果
cat results/redis_baseline_10M.txt
```

#### 查看对比报告

```bash
# 详细对比报告
cat COMPARISON_REPORT.md

# SuperNode 详细结果
cat SUPERNODE_REAL_RESULTS.md

# Redis 详细结果
cat REDIS_BASELINE_RESULTS.md

# 最终总结
cat FINAL_SUMMARY.md
```

## 命令行参数

### supernode_real_benchmark

```bash
./supernode_real_benchmark [options]

Options:
  --queries N      查询数量 (默认: 10000000)
  --threads N      线程数量 (默认: 16)
  --supernodes N   SuperNode 数量 (默认: 150)
  --help           显示帮助
```

### redis_traditional_benchmark

```bash
./redis_traditional_benchmark [options]

Options:
  --queries N    查询数量 (默认: 10000000)
  --threads N    线程数量 (默认: 16)
  --servers N    Redis 服务器数量 (默认: 350000)
  --help         显示帮助
```

### supernode_server

```bash
./supernode_server [options]

Options:
  --port N    服务器端口 (默认: 6388)
  --help      显示帮助
```

## 常见问题

### Q1: SuperNode 服务器启动失败

**错误**: `Failed to allocate embedding store`

**原因**: 内存不足

**解决方法**:
1. 检查可用内存: `free -h`
2. 减少 embedding 数量 (修改 `MAX_EMBEDDINGS`)
3. 使用更大内存的机器

### Q2: 连接 SuperNode 失败

**错误**: `Failed to connect to 127.0.0.1:6388`

**原因**: SuperNode 服务器未启动或端口被占用

**解决方法**:
1. 检查服务器是否运行: `ps aux | grep supernode_server`
2. 检查端口是否监听: `netstat -tlnp | grep 6388`
3. 重启服务器

### Q3: Redis 连接失败

**错误**: `Cannot connect to Redis at 127.0.0.1:6381`

**解决方法**:
1. 启动 Redis: `./src/redis-server --port 6381`
2. 检查 Redis 状态: `./src/redis-cli -p 6381 ping`

### Q4: 性能结果异常

**问题**: QPS 太低或延迟太高

**可能原因**:
1. 系统负载过高
2. 内存不足，触发 swap
3. 网络问题

**解决方法**:
1. 检查系统负载: `top`
2. 检查内存: `free -h`
3. 检查 swap: `swapon -s`
4. 关闭其他程序
5. 重启测试

### Q5: 编译错误

**错误**: `hiredis.h: No such file or directory`

**解决方法**:
```bash
# 先编译 Redis (包含 hiredis)
cd /sharedata/qiuwu/moreai/redis
make -j$(nproc)

# 再编译 benchmark
cd benchmark
make clean
make all
```

## 性能调优

### 增加并发

```bash
# 增加线程数
./supernode_real_benchmark --queries 10000000 --threads 32
```

### 增加批量大小

修改 `supernode_real_benchmark.c`:
```c
#define SUPERNODE_BATCH_SIZE 6000  /* 从 3000 改为 6000 */
```

重新编译:
```bash
make supernode_real_benchmark
```

### 使用 NUMA 优化

```bash
# 绑定到特定 NUMA 节点
numactl --cpunodebind=0 --membind=0 ./supernode_server --port 6388
```

### 禁用 THP (Transparent Huge Pages)

```bash
echo never > /sys/kernel/mm/transparent_hugepage/enabled
```

## 监控和调试

### 监控 SuperNode 服务器

```bash
# 查看服务器日志
tail -f /path/to/supernode_server.log

# 查看连接数
netstat -an | grep 6388 | wc -l

# 查看内存使用
ps aux | grep supernode_server
```

### 监控系统资源

```bash
# CPU 使用率
top -p $(pgrep supernode_server)

# 内存使用
free -h

# 网络流量
iftop -i eth0
```

### 调试模式

添加调试输出:
```bash
# 修改代码，增加 printf 调试信息
# 重新编译
make supernode_real_benchmark
```

## 下一步

1. 阅读详细文档:
   - [COMPARISON_REPORT.md](COMPARISON_REPORT.md) - 完整对比报告
   - [SUPERNODE_REAL_RESULTS.md](SUPERNODE_REAL_RESULTS.md) - SuperNode 详细结果
   - [FINAL_SUMMARY.md](FINAL_SUMMARY.md) - 最终总结

2. 尝试不同配置:
   - 调整批量大小
   - 调整线程数
   - 调整 SuperNode 数量

3. 优化性能:
   - NUMA 优化
   - 网络优化
   - 内存优化

4. 生产部署:
   - 高可用配置
   - 监控告警
   - 备份恢复

## 联系支持

如有问题，请查看:
- [README.md](README.md) - 项目概述
- [BENCHMARK_COMPLETION_SUMMARY.md](BENCHMARK_COMPLETION_SUMMARY.md) - 完成总结

---

**最后更新**: 2026-02-10
**版本**: 1.0
