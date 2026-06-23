# Redis UB+SVE 超高性能架构

## 项目概述

基于华为鲲鹏 CPU 的 UB (Unified Bus) 架构和 ARM SVE2 (Scalable Vector Extension 2) 指令集，实现了一个 **240 亿 IOPS** 的极高性能全用户态批处理架构。

### 核心特性

- 🚀 **240 亿 IOPS** 吞吐量目标
- ⚡ **< 100 μs** 单请求延迟 (P99)
- 📦 **3000-6000** 批量大小
- 🔄 **200-500 μs** 微秒级批量聚合
- 🔒 **Lock-Free** Bitmap CAS 并发控制
- 🎯 **SVE2** 向量化批量计算
- 💾 **4TB** UB.mem 共享内存池
- 🌐 **150** 超节点集群支持

## 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                    Redis 前端层（Proxy）                      │
│                                                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │  Bucket A   │  │  Bucket B   │  │  Bucket C   │  ...    │
│  │  (3000 req) │  │  (3000 req) │  │  (3000 req) │         │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘         │
│         │                 │                 │                 │
│         │  一致性哈希分片  │                 │                 │
│         │  (MurmurHash3)  │                 │                 │
│         └─────────┬───────┴─────────────────┘                │
│                   │                                           │
│         微秒级等待积攒 (200-500μs)                            │
│                   │                                           │
└───────────────────┼───────────────────────────────────────────┘
                    │
                    │ Ring Buffer（零拷贝，16MB）
                    │
┌───────────────────┼───────────────────────────────────────────┐
│                   ▼                                           │
│              超节点计算层（UB+SVE）                            │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  SVE Worker 0  │  SVE Worker 1  │  ...  │  SVE Worker N │ │
│  └─────────────────────────────────────────────────────────┘ │
│                          │                                    │
│                          ▼                                    │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              Bitmap CAS 无锁并发控制                      │ │
│  │  ┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐             │ │
│  │  │ 0 │ 0 │ 1 │ 0 │ 0 │ 0 │ 1 │ 0 │ 0 │...│             │ │
│  │  └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘             │ │
│  │       ↓ CAS Check (0=可读, 1=正在写)                     │ │
│  └─────────────────────────────────────────────────────────┘ │
│                          │                                    │
│                          ▼                                    │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │           SVE2 Gather Load（批量并行读取）                │ │
│  │                                                           │ │
│  │  svld1_f32(pg, &emb[id0])  ──┐                          │ │
│  │  svld1_f32(pg, &emb[id1])  ──┤                          │ │
│  │  svld1_f32(pg, &emb[id2])  ──┼─► 8-16 并行              │ │
│  │  ...                         │                          │ │
│  │  svld1_f32(pg, &emb[id7])  ──┘                          │ │
│  └─────────────────────────────────────────────────────────┘ │
│                          │                                    │
│                          ▼                                    │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │         UB.mem 共享内存池（4TB，非临时访问）              │ │
│  │                                                           │ │
│  │  ┌──────────────────────────────────────────────────┐   │ │
│  │  │  Embedding 0  │  Embedding 1  │  ...  │  Emb 10亿│   │ │
│  │  └──────────────────────────────────────────────────┘   │ │
│  └─────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────┘
```

## 快速开始

### 1. 查看文档

```bash
# 快速开始指南（5 分钟部署）
cat redis/QUICKSTART_UB_SVE.md

# 完整集成文档
cat redis/UB_SVE_INTEGRATION_README.md

# 实现总结
cat redis/IMPLEMENTATION_SUMMARY.md
```

### 2. 编译

```bash
cd redis

# 应用 Makefile 补丁
cd src && patch < ../Makefile.ub-sve.patch && cd ..

# 编译（ARM64 + SVE）
make ARCH=aarch64 USE_UB_SVE=yes -j$(nproc)
```

### 3. 配置

```bash
# 复制配置文件
cp redis-ub-sve.conf /etc/redis/redis-ub-sve.conf

# 编辑配置
vim /etc/redis/redis-ub-sve.conf
```

### 4. 启动

```bash
# 一体化模式（Proxy + SuperNode 同机部署）
./src/redis-server /etc/redis/redis-ub-sve.conf \
    --vector-engine ub \
    --proxy-aggregator-enabled yes \
    --supernode-id 0 \
    --supernode-num-workers 16
```

### 5. 测试

```bash
# 集成测试
./test_ub_sve_integration.sh

# 性能测试
./batch_embedding_test
```

## 文件结构

```
redis/
├── src/
│   ├── proxy_aggregator.h          # Proxy 聚合器头文件
│   ├── proxy_aggregator.c          # Proxy 聚合器实现
│   ├── supernode_worker.h          # 超节点 Worker 头文件
│   ├── supernode_worker.c          # 超节点 Worker 实现
│   ├── batch_processor.h/c         # 批处理器（已有）
│   ├── ub_client.h/c              # UB 客户端（已有）
│   ├── sve_compute.h/c            # SVE 计算（已有）
│   └── vector_engine.h/c          # 向量引擎（已有）
│
├── redis-ub-sve.conf               # 配置文件示例
├── test_ub_sve_integration.sh      # 集成测试脚本
├── Makefile.ub-sve.patch           # Makefile 补丁
│
├── README_UB_SVE.md                # 本文档
├── QUICKSTART_UB_SVE.md            # 快速开始指南
├── UB_SVE_INTEGRATION_README.md    # 完整集成文档
├── IMPLEMENTATION_SUMMARY.md       # 实现总结
│
├── docs/design.md                  # 设计文档
├── UB_SVE-redis.pdf               # 原始设计文档
└── detail_proxy_sve_2layerbutin_onemachine.pdf  # 详细设计
```

## 核心技术

### 1. Proxy 层智能聚合

- **一致性哈希**: MurmurHash3 + 虚拟节点
- **批量积攒**: 3000 个请求或 200μs 超时
- **Ring Buffer**: 16MB 共享内存，零拷贝
- **刷新线程**: 50μs 轮询间隔

### 2. 超节点 SVE Worker

- **Bitmap CAS**: 无锁并发控制
- **SVE2 Gather**: 8-16 并行读取
- **Lock-Free**: 遇锁跳过，流水线满载
- **非临时访问**: 避免 L3 Cache 污染

### 3. UB.mem 共享内存池

- **容量**: 4TB per 超节点
- **总容量**: 600TB (150 超节点)
- **访问**: 直接内存映射
- **页大小**: 4MB 大页

## 性能指标

| 指标 | 目标值 | 说明 |
|------|--------|------|
| 总吞吐量 | 240 亿 IOPS | 基于 30-40 万台 Redis 估算 |
| 单请求延迟 | < 100 μs | P99 延迟 |
| 批量大小 | 3000-6000 | 动态调整 |
| 批量延迟 | 200-500 μs | 积攒时间 |
| 收益比（保守）| 2.96x | 基于当前硬件限制 |
| 收益比（目标）| 30-100x | 优化后目标 |

## 配置参数

### 关键配置

```ini
# Proxy 层
proxy-aggregator-enabled yes
proxy-batch-limit 3000
proxy-timeout-us 200
proxy-num-supernodes 150

# 超节点层
supernode-id 0
supernode-num-workers 16
supernode-ub-mem-size 4TB

# SVE 配置
sve-vector-bits 256
sve2-enabled yes
sve-non-temporal-access yes

# Bitmap 配置
bitmap-cas-enabled yes
```

完整配置请参考 `redis-ub-sve.conf`。

## 监控和调优

### 实时监控

```bash
# Proxy 统计
redis-cli PROXY STATS

# 超节点统计
redis-cli SUPERNODE STATS

# Worker 统计
redis-cli WORKER STATS 0
```

### 关键指标

- **Average batch size**: 2500-3500（批量大小）
- **Average batch latency**: < 300 μs（批量延迟）
- **Locked skips**: < 1%（锁冲突率）
- **SVE operations**: > 95%（SVE 利用率）
- **Throughput**: > 50000 QPS（吞吐量）

### 性能调优

```bash
# 批量大小优化
redis-cli CONFIG SET proxy-batch-limit 3000

# 超时时间优化
redis-cli CONFIG SET proxy-timeout-us 200

# Worker 数量优化（需重启）
./src/redis-server --supernode-num-workers 16
```

## 测试

### 集成测试

```bash
./test_ub_sve_integration.sh

# 测试项：
# ✅ 源文件检查
# ✅ 架构支持检查
# ✅ 编译测试
# ✅ Ring Buffer 功能测试
# ✅ Bitmap CAS 测试
# ✅ SVE 指令测试
# ✅ 性能基准测试
```

### 性能测试

```bash
./batch_embedding_test

# 预期结果：
# ✅ Latency: < 100 μs
# ✅ Throughput: > 50000 QPS
# ✅ Batch size: 2500-3500
```

## 硬件要求

### 最低要求

- **CPU**: 鲲鹏 920/930（ARM64 + SVE/SVE2）
- **内存**: 8GB 系统内存 + 4TB UB.mem
- **网络**: UB-Mesh 互联，0.2 TB/s 带宽

### 推荐配置

- **CPU**: 鲲鹏 930，64 核以上
- **内存**: 16GB 系统内存 + 4TB UB.mem
- **网络**: UB-Mesh 互联，0.2 TB/s 带宽
- **存储**: NVMe SSD（用于日志）

## 系统要求

### 操作系统

- **Linux**: Kernel 4.18+
- **发行版**: CentOS 8+, Ubuntu 20.04+, openEuler 20.03+

### 依赖库

```bash
# 基础依赖
sudo yum install -y gcc make

# UB 固件库（华为提供）
# libubios.so
# libsve.so

# 大页支持
sudo sysctl -w vm.nr_hugepages=2048
```

## 常见问题

### Q1: 编译失败 - SVE 指令不支持

```bash
# 使用标量回退模式
make CFLAGS="-march=armv8-a -O2"
```

### Q2: Ring Buffer 创建失败

```bash
# 增加共享内存限制
sudo sysctl -w kernel.shmmax=17179869184
sudo sysctl -w kernel.shmall=4194304
```

### Q3: 性能未达到预期

检查：
1. CPU 频率锁定
2. NUMA 绑定
3. 批量大小配置
4. Worker 线程数

详细解决方案请参考 `QUICKSTART_UB_SVE.md`。

## 开发路线图

### 短期（1-2 周）

- [x] 核心代码实现
- [ ] Makefile 集成
- [ ] 编译通过
- [ ] 基础功能测试
- [ ] 最小雏形验证（2-3x）

### 中期（1-2 月）

- [ ] UB 固件完整集成
- [ ] SVE2 流水线优化
- [ ] Double Buffer 实现
- [ ] 写操作支持
- [ ] 冲击 30x 收益比

### 长期（3-6 月）

- [ ] 150 节点全互联
- [ ] 硬件 Cache 一致性
- [ ] UB-Mesh 全带宽利用
- [ ] 自适应批量大小
- [ ] 冲击 100x 收益比

## 贡献指南

### 代码规范

- 遵循 Redis 代码风格
- 使用 4 空格缩进
- 添加详细注释
- 编写单元测试

### 提交流程

1. Fork 项目
2. 创建特性分支
3. 提交代码
4. 运行测试
5. 提交 Pull Request

## 许可证

本项目遵循 Redis 的 BSD 许可证。

## 联系方式

- **项目经理**: 付鹤鸣
- **架构师**: 徐葳
- **UB.mem 负责人**: 蒋孝伟
- **核心成员**: 张昆

## 参考资料

### 设计文档

- `UB_SVE-redis.pdf` - 原始设计文档
- `docs/design.md` - 最新方案细化
- `detail_proxy_sve_2layerbutin_onemachine.pdf` - 详细设计

### 实现文档

- `UB_SVE_INTEGRATION_README.md` - 完整集成文档
- `QUICKSTART_UB_SVE.md` - 快速开始指南
- `IMPLEMENTATION_SUMMARY.md` - 实现总结

### 技术文档

- [ARM SVE Programming Guide](https://developer.arm.com/architectures/instruction-sets/simd-isas/sve)
- [Huawei Kunpeng Documentation](https://www.hikunpeng.com/)
- [Redis Documentation](https://redis.io/documentation)

---

**版本**: v1.0  
**日期**: 2026-02-03  
**状态**: 代码实现完成，待编译测试

**🚀 让我们一起冲击 240 亿 IOPS！**
