# Redis UB+SVE 超高性能架构实现

## 概述

本实现基于设计文档 `UB_SVE-redis.pdf` 和 `docs/design.md`，实现了一个 240 亿 IOPS 的极高性能全用户态批处理架构。

### 核心设计

#### 1. 两层架构（同机部署）

```
┌─────────────────────────────────────────────────────────────┐
│                    Redis 前端层（Proxy）                      │
│  - 接收海量并发请求                                           │
│  - 微秒级等待积攒（200-500μs）                                │
│  - 批量大小：3000-6000 个请求                                 │
│  - 一致性哈希分片                                             │
└──────────────────────┬──────────────────────────────────────┘
                       │ Ring Buffer（零拷贝）
┌──────────────────────┴──────────────────────────────────────┐
│                  超节点计算层（UB+SVE）                       │
│  - 纯用户态 SVE2 向量计算                                     │
│  - Bitmap CAS 无锁并发控制                                    │
│  - UB.mem 共享内存池（4TB）                                   │
│  - 非临时内存访问（避免 L3 Cache 污染）                       │
└─────────────────────────────────────────────────────────────┘
```

#### 2. 关键技术点

##### 2.1 Proxy 层智能聚合（蓄水池策略）

**文件**: `proxy_aggregator.h/c`

- **批量积攒**: 等待 200-500μs 或积攒到 3000 个请求
- **一致性哈希**: MurmurHash3 + 虚拟节点，均衡分布到 150 个超节点
- **Ring Buffer**: 16MB 共享内存，零拷贝通信
- **触发条件**:
  - 数量达标: `count >= 3000`
  - 时间达标: `age >= 200μs`

```c
// 核心流程
1. 请求到达 -> Hash(key) -> 确定超节点
2. 放入对应 Bucket
3. 检查触发条件
4. 序列化为 SVE 友好格式
5. 写入 Ring Buffer（零拷贝）
```

##### 2.2 超节点 SVE Worker（计算引擎）

**文件**: `supernode_worker.h/c`

- **Bitmap CAS 锁**: 全局状态位图，原子操作
- **SVE2 Gather Load**: 批量并行读取 Embedding
- **Lock-Free 策略**: 遇锁不等待，跳过或返回默认值
- **非临时访问**: 冷数据穿透 L3 Cache

```c
// 核心处理流程
1. 轮询 Ring Buffer 获取 Batch
2. SVE Gather Load 锁状态（预取）
3. 检查 Bitmap（CAS）
   - 状态 = 0（无锁）-> 继续
   - 状态 = 1（正在写）-> 跳过，避免流水线停顿
4. SVE2 Gather Load 数据（批量并行）
5. 相似度计算（可选）
6. 结果回填
```

##### 2.3 Bitmap + CAS 无锁并发控制

**必要性分析**:
- 240 亿 IOPS 下，任何锁竞争都是致命的
- 读写比例: 1000:1（读远多于写）
- 优化策略: 乐观并发控制（Optimistic Concurrency Control）

**实现**:
```c
// 写侧
bitmap_set_bit_cas(bitmap, emb_id);  // CAS 0->1
update_embedding_data(emb_id, data);
bitmap_clear_bit_cas(bitmap, emb_id); // CAS 1->0

// 读侧
if (bitmap_test_bit(bitmap, emb_id) == 1) {
    // 正在写，跳过或返回旧值
    skip_or_return_default();
} else {
    // SVE2 Gather Load
    sve2_gather_load(emb_id, result);
}
```

## 文件结构

```
redis/src/
├── proxy_aggregator.h/c      # Proxy 层聚合器
├── supernode_worker.h/c      # 超节点 SVE Worker
├── batch_processor.h/c       # 批处理器（已有）
├── ub_client.h/c            # UB 客户端（已有）
├── sve_compute.h/c          # SVE 计算（已有）
└── vector_engine.h/c        # 向量引擎抽象层（已有）

redis/
├── UB_SVE_INTEGRATION_README.md  # 本文档
├── test_ub_sve_integration.sh    # 集成测试脚本
└── docs/design.md                # 设计文档
```

## 编译和构建

### 前置条件

- ARM64 架构（鲲鹏 CPU）
- SVE/SVE2 支持
- UB 固件库（`libubios.so`）
- GCC 9.0+ 或 Clang 10.0+

### 编译选项

```bash
cd redis
make CFLAGS="-march=armv8.2-a+sve -O3 -DUSE_UB_SVE"
```

### 配置

在 `redis.conf` 中添加:

```ini
# 启用 UB+SVE 引擎
vector-engine ub

# Proxy 聚合器配置
proxy-aggregator-enabled yes
proxy-batch-limit 3000
proxy-timeout-us 200
proxy-num-supernodes 150

# 超节点配置
supernode-id 0
supernode-num-workers 16
supernode-ub-mem-size 4TB
```

## 性能目标

### 设计指标

| 指标 | 目标值 | 说明 |
|------|--------|------|
| 总吞吐量 | 240 亿 IOPS | 基于 30-40 万台 Redis 估算 |
| 单请求延迟 | < 100 μs | P99 延迟 |
| 批量大小 | 3000-6000 | 动态调整 |
| 批量延迟 | 200-500 μs | 积攒时间 |
| 收益比（保守）| 2.96x | 基于当前硬件限制 |
| 收益比（目标）| 30-100x | 优化后目标 |

### 性能分析

#### 当前限制因素

1. **UB 带宽**: 0.2 TB/s（相比昇腾 0.9 TB/s 降级 4.5 倍）
2. **单超节点内存**: 4TB 物理寻址限制
3. **超节点数量**: 需要 150 个（600TB ÷ 4TB）

#### 优化路径

```
当前 2.96x -> 中期 30x -> 终极 100x
```

**关键优化点**:
1. 接入层 WAIT 及批量转发性能
2. CAS Lock-Free 锁性能
3. SVE2 流水线利用率
4. UB-Mesh 带宽利用率

## 使用示例

### 启动 Redis（Proxy 模式）

```bash
redis-server --vector-engine ub \
             --proxy-aggregator-enabled yes \
             --proxy-num-supernodes 150
```

### 启动超节点

```bash
redis-server --supernode-id 0 \
             --supernode-num-workers 16 \
             --ub-mem-base 0x100000000 \
             --ub-mem-size 4TB
```

### 客户端请求

```bash
# 添加向量
redis-cli VADD myvectors VALUES 300 <300 floats> user:12345

# 批量查询（自动聚合）
for i in {1..10000}; do
    redis-cli VEMB myvectors user:$i &
done
wait
```

### 监控统计

```bash
# Proxy 统计
redis-cli PROXY STATS

# 超节点统计
redis-cli SUPERNODE STATS

# Worker 统计
redis-cli WORKER STATS 0
```

## 测试

### 单元测试

```bash
cd redis
make test
```

### 集成测试

```bash
./test_ub_sve_integration.sh
```

### 性能测试

```bash
# 批量 Embedding 测试
./batch_embedding_test

# 预期输出:
# ✅ Latency target met: 85.3 μs ≤ 100 μs
# ✅ Throughput target met: 52000 QPS ≥ 50000 QPS
```

## 架构优势

### 1. 零拷贝通信

- Ring Buffer 共享内存
- 避免内核态切换
- 减少 CPU 开销

### 2. 批量化处理

- 3000 个请求 -> 1 次 Ring Buffer 交互
- 减少交互次数 3000 倍
- 提升吞吐量

### 3. SVE2 向量化

- 8-16 个 Key 并行检查锁状态
- 批量 Gather Load
- 提升计算密度

### 4. Lock-Free 并发

- Bitmap CAS 原子操作
- 遇锁不等待
- 流水线永远满载

### 5. Cache 友好

- 非临时内存访问
- 冷数据穿透 L3 Cache
- 保护热数据（索引）

## 待确认事项

### 华为方

1. ✅ SVE LD1D 指令是否支持 UB.mem 地址空间？
2. ⏳ VA/PA 映射：150 个超节点组网时的地址管理？
3. ⏳ 物理寻址扩展：是否支持 > 4TB？

### AIGCode 方

1. ⏳ SVE 远端指令延迟：具体增加多少？
2. ✅ Ring Buffer 实现：共享内存 + 原子指针
3. ✅ 内存映射策略：启动时预映射 UB.mem
4. ⏳ 数据包分布：确认 80-90% 为 4KB

## 下一步工作

### 短期（1-2 周）

- [ ] 完成最小雏形验证
- [ ] 实测 2-3x 性能提升
- [ ] 压测 Ring Buffer 性能
- [ ] 验证 CAS 锁性能

### 中期（1-2 月）

- [ ] 优化 SVE2 流水线
- [ ] 实现 Double Buffer
- [ ] 支持写操作（Point-to-Point）
- [ ] 冲击 30x 收益比

### 长期（3-6 月）

- [ ] 150 节点全互联
- [ ] 硬件 Cache 一致性
- [ ] UB-Mesh 全带宽利用
- [ ] 冲击 100x 收益比

## 参考文档

- `UB_SVE-redis.pdf` - 原始设计文档
- `docs/design.md` - 最新方案细化
- `detail_proxy_sve_2layerbutin_onemachine.pdf` - 详细设计

## 联系方式

- 项目经理：付鹤鸣
- 架构师：徐葳
- UB.mem 负责人：蒋孝伟
- 核心成员：张昆

---

**版本**: v1.0  
**日期**: 2026-02-03  
**状态**: 开发中
