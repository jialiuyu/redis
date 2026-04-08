# Redis UB+SVE 快速开始指南

## 5 分钟快速部署

### 前置条件

```bash
# 检查架构
uname -m  # 应该输出 aarch64 或 arm64

# 检查 SVE 支持
grep sve /proc/cpuinfo

# 检查大页支持
cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
```

### 步骤 1: 编译 Redis

```bash
cd /sharedata/qiuwu/moreai/redis

# 应用 Makefile 补丁
cd src
patch < ../Makefile.ub-sve.patch

# 编译
cd ..
make ARCH=aarch64 USE_UB_SVE=yes -j$(nproc)
```

### 步骤 2: 配置

```bash
# 复制配置文件
cp redis-ub-sve.conf /etc/redis/redis-ub-sve.conf

# 编辑配置（根据实际情况调整）
vim /etc/redis/redis-ub-sve.conf

# 关键配置项：
# - supernode-id: 当前超节点 ID (0-149)
# - supernode-num-workers: Worker 线程数（建议 = CPU 核心数）
# - proxy-num-supernodes: 总超节点数（150）
```

### 步骤 3: 启动 Redis

#### 方式 A: Proxy 模式（接入层）

```bash
./src/redis-server /etc/redis/redis-ub-sve.conf \
    --vector-engine ub \
    --proxy-aggregator-enabled yes \
    --proxy-num-supernodes 150
```

#### 方式 B: SuperNode 模式（计算层）

```bash
./src/redis-server /etc/redis/redis-ub-sve.conf \
    --supernode-id 0 \
    --supernode-num-workers 16
```

#### 方式 C: 一体化模式（同机部署）

```bash
./src/redis-server /etc/redis/redis-ub-sve.conf \
    --vector-engine ub \
    --proxy-aggregator-enabled yes \
    --supernode-id 0 \
    --supernode-num-workers 16
```

### 步骤 4: 验证

```bash
# 连接 Redis
./src/redis-cli

# 检查向量引擎
127.0.0.1:6379> VENGINE GET
"ub"

# 添加测试向量
127.0.0.1:6379> VADD myvectors VALUES 300 <300个浮点数> user:1

# 查询向量
127.0.0.1:6379> VEMB myvectors user:1

# 查看统计
127.0.0.1:6379> PROXY STATS
127.0.0.1:6379> SUPERNODE STATS
```

### 步骤 5: 性能测试

```bash
# 运行集成测试
./test_ub_sve_integration.sh

# 运行性能测试
./batch_embedding_test

# 预期结果：
# ✅ Latency: < 100 μs
# ✅ Throughput: > 50000 QPS
```

## 常见问题

### Q1: 编译失败 - SVE 指令不支持

**解决方案**:
```bash
# 使用标量回退模式编译
make CFLAGS="-march=armv8-a -O2"
```

### Q2: Ring Buffer 创建失败

**解决方案**:
```bash
# 增加共享内存限制
sudo sysctl -w kernel.shmmax=17179869184  # 16GB
sudo sysctl -w kernel.shmall=4194304

# 或者在 /etc/sysctl.conf 中添加：
# kernel.shmmax = 17179869184
# kernel.shmall = 4194304
```

### Q3: UB.mem 映射失败

**解决方案**:
```bash
# 配置大页
sudo sysctl -w vm.nr_hugepages=2048  # 4GB (2048 * 2MB)

# 永久配置：
echo "vm.nr_hugepages = 2048" | sudo tee -a /etc/sysctl.conf
```

### Q4: 性能未达到预期

**检查清单**:
1. CPU 频率是否锁定在最高？
   ```bash
   sudo cpupower frequency-set -g performance
   ```

2. NUMA 绑定是否正确？
   ```bash
   numactl --cpunodebind=0 --membind=0 ./src/redis-server ...
   ```

3. 批量大小是否合适？
   ```bash
   # 在 redis.conf 中调整
   proxy-batch-limit 3000  # 尝试 2000-6000
   ```

4. Worker 线程数是否合适？
   ```bash
   # 建议 = CPU 核心数
   supernode-num-workers 16
   ```

## 监控和调优

### 实时监控

```bash
# 监控 Proxy 统计
watch -n 1 'redis-cli PROXY STATS'

# 监控 SuperNode 统计
watch -n 1 'redis-cli SUPERNODE STATS'

# 监控 Worker 统计
redis-cli WORKER STATS 0
```

### 关键指标

| 指标 | 目标值 | 说明 |
|------|--------|------|
| Average batch size | 2500-3500 | 批量大小 |
| Average batch latency | < 300 μs | 批量延迟 |
| Locked skips | < 1% | 锁冲突率 |
| SVE operations | > 95% | SVE 利用率 |
| Throughput | > 50000 QPS | 吞吐量 |

### 性能调优

#### 1. 批量大小优化

```bash
# 测试不同批量大小
for size in 1000 2000 3000 4000 5000 6000; do
    redis-cli CONFIG SET proxy-batch-limit $size
    ./batch_embedding_test
done
```

#### 2. 超时时间优化

```bash
# 测试不同超时时间
for timeout in 100 200 300 400 500; do
    redis-cli CONFIG SET proxy-timeout-us $timeout
    ./batch_embedding_test
done
```

#### 3. Worker 数量优化

```bash
# 测试不同 Worker 数量
for workers in 8 12 16 20 24; do
    # 重启 Redis 并测试
    ./src/redis-server --supernode-num-workers $workers &
    sleep 5
    ./batch_embedding_test
    killall redis-server
done
```

## 生产部署建议

### 1. 硬件配置

- **CPU**: 鲲鹏 920/930，64 核以上
- **内存**: 8GB 系统内存 + 4TB UB.mem
- **网络**: UB-Mesh 互联，0.2 TB/s 带宽
- **存储**: NVMe SSD（用于日志和配置）

### 2. 系统配置

```bash
# /etc/sysctl.conf
vm.nr_hugepages = 2048
kernel.shmmax = 17179869184
kernel.shmall = 4194304
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535

# 应用配置
sudo sysctl -p
```

### 3. 服务配置

```bash
# /etc/systemd/system/redis-ub-sve.service
[Unit]
Description=Redis UB+SVE Server
After=network.target

[Service]
Type=forking
ExecStart=/usr/local/bin/redis-server /etc/redis/redis-ub-sve.conf
ExecStop=/usr/local/bin/redis-cli shutdown
Restart=always
User=redis
Group=redis

# 性能优化
LimitNOFILE=65535
LimitNPROC=65535

[Install]
WantedBy=multi-user.target
```

### 4. 监控告警

```bash
# Prometheus 监控指标
redis_ub_sve_batch_size
redis_ub_sve_batch_latency_us
redis_ub_sve_locked_skips
redis_ub_sve_throughput_qps
redis_ub_sve_worker_utilization
```

## 下一步

1. 阅读完整文档: [UB_SVE_INTEGRATION_README.md](UB_SVE_INTEGRATION_README.md)
2. 查看设计文档: [design.md](design.md)
3. 运行完整测试: `./test_ub_sve_integration.sh`
4. 性能调优: 根据实际负载调整参数
5. 生产部署: 参考生产部署建议

## 获取帮助

- 技术问题: 查看 [UB_SVE_INTEGRATION_README.md](UB_SVE_INTEGRATION_README.md)
- 性能问题: 查看监控和调优章节
- Bug 报告: 联系开发团队

---

**版本**: v1.0  
**更新日期**: 2026-02-03
