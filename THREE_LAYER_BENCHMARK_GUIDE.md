# three_layer_benchmark 测试用例说明

## 测试概述

`three_layer_benchmark` 是一个专门测试**三层缓存架构**性能和可靠性的基准测试程序。

## 架构说明

### 三层缓存结构

```
┌─────────────────────────────────────────────────────────────┐
│                    三层缓存架构                              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │   HOT层     │  │   WARM层     │  │   COLD层     │  │
│  │  (L3缓存)   │  │  (内存)      │  │  (仅追加)    │  │
│  │  128K条目    │  │  1M条目      │  │  64个段      │  │
│  │  16字节/条目  │  │  1200字节/条目 │  │  持久化存储   │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │
│         │                   │                   │             │
│         └───────────────────┴───────────────────┘             │
│                         索引加速                            │
└─────────────────────────────────────────────────────────────┘
```

### 层级特性

| 层级 | 容量 | 条目大小 | 存储内容 | 特性 |
|------|--------|----------|----------|------|
| HOT | 128K | 16字节 | key + warm_idx | 索引加速，4条目/缓存行 |
| WARM | 1M | 1200字节 | key + value + state | 完整值存储，支持LRU |
| COLD | 64段 | 1200字节 | key + value + offset | 仅追加，持久化存储 |

### 核心技术

1. **Bitmap CAS并发控制**
   - 零互斥锁设计
   - 基于CAS的乐观并发
   - 无系统调用，纯用户态

2. **Ring Buffer通知机制**
   - 4096个事件槽位
   - 异步事件传播
   - 支持写、提升、驱逐、刷新等事件

3. **Paxos一致性协议**
   - 3个分片，2个副本
   - Quorum = 2
   - 自动冲突解决

4. **2x3高可用架构**
   - 2个IDC（数据中心）
   - 每个IDC 3个分片
   - 总共6个分区
   - 支持故障转移和恢复

## 测试场景

### 1. Read-Heavy测试（读密集型）

**参数配置**:
- 操作数量: 1,000,000
- 线程数: 8
- 读/写操作比: 80%读 / 20%写
- 键分布: Zipfian (s=1.2，热键偏斜)

**测试目的**:
- 验证热数据在HOT层的命中率
- 测试LRU提升机制
- 评估读密集型工作负载的性能

**预期结果**:
```
Read-Heavy (80R/20W)
  Ops: 1000000, Threads: 8, Write%: 20

Results:
  Total time:    X.XXX seconds
  Throughput:    XX.XX QPS (X.XX M QPS)
  Avg latency:   X.XXX μs
  Read hits:     XXXXXX
  Read misses:   XXXXX
  Writes:        XXXXX
  Hit rate:      XX.X%
```

### 2. Write-Heavy测试（写密集型）

**参数配置**:
- 操作数量: 500,000 (读密集型的一半)
- 线程数: 8
- 读/写操作比: 20%读 / 80%写
- 键分布: Zipfian (s=1.2)

**测试目的**:
- 验证写入性能
- 测试WARM层的写入吞吐量
- 评估写密集型工作负载的性能

**预期结果**:
```
Write-Heavy (20R/80W)
  Ops: 500000, Threads: 8, Write%: 80

Results:
  Total time:    X.XXX seconds
  Throughput:    XX.XX QPS (X.XX M QPS)
  Avg latency:   X.XXX μs
  Read hits:     XXXXX
  Read misses:   XXXXX
  Writes:        XXXXX
  Hit rate:      XX.X%
```

### 3. 高可用故障转移测试

**测试流程**:

#### 阶段1: 正常运行
```
After IDC-1 Failover
  Ops: 250000, Threads: 8, Write%: 20
```

#### 阶段2: 模拟IDC-1故障
```c
printf("\n--- Simulating IDC 1 failure ---\n");
tlc_ha_failover(&cache, 1);
```

#### 阶段3: 故障后运行
```
After IDC-1 Failover
  Ops: 250000, Threads: 8, Write%: 20
```

#### 阶段4: 恢复IDC-1
```c
printf("\n--- Recovering IDC 1 ---\n");
tlc_ha_recover(&cache, 1);
```

#### 阶段5: 恢复后运行
```
After IDC-1 Recovery
  Ops: 250000, Threads: 8, Write%: 20
```

**测试目的**:
- 验证故障转移机制
- 测试Paxos协议的容错能力
- 评估故障期间的性能影响
- 验证恢复后的数据一致性

## 工作负载特性

### Zipfian分布

```c
static uint64_t zipfian_key(unsigned int *seed, uint64_t max_key) {
    double u = (double)rand_r(seed) / RAND_MAX;
    /* Zipf with s=1.2: heavily skewed toward small keys */
    double z = pow(u, 1.0 / 1.2);
    return (uint64_t)(z * (double)max_key) % max_key;
}
```

**特性**:
- 参数 s=1.2，表示较强的偏斜分布
- 小键的访问频率远高于大键
- 模拟真实世界中的热/冷数据访问模式
- 测试缓存层级间的数据流动

### 预填充机制

```c
#define DEFAULT_WARM_FILL 100000  /* pre-fill warm layer */
```

**填充策略**:
1. 预填充WARM层: 100,000个条目
2. 预填充COLD层: 10,000个条目
3. 重置统计计数器
4. 开始基准测试

## 性能指标

### 吞吐量指标

- **QPS**: 每秒查询数
- **M QPS**: 百万级QPS
- **计算公式**: `QPS = 总操作数 / 执行时间(秒)`

### 延迟指标

- **平均延迟**: 微秒级 (μs)
- **计算公式**: `平均延迟 = 最大执行时间 / 总操作数 / 1000`

### 命中率指标

- **命中率**: HIT / (HIT + MISS)
- **分层命中**: 分别统计HOT、WARM、COLD层命中
- **穿透率**: 所有层都未命中的比例

## 并发控制机制

### Bitmap CAS锁

```c
static inline void bmp_lock_acquire(bitmap_lock_t *bl, uint32_t bucket) {
    size_t wi = bucket / BMP_BITS_PER_WORD;
    uint64_t bit = 1ULL << (bucket % BMP_BITS_PER_WORD);
    for (;;) {
        uint64_t old = atomic_load_explicit(&bl->words[wi], memory_order_relaxed);
        if (!(old & bit)) {
            if (atomic_compare_exchange_weak_explicit(&bl->words[wi], &old, old | bit,
                    memory_order_acquire, memory_order_relaxed))
                return;
        }
        /* spin yield */
        #if defined(__aarch64__)
        __asm__ volatile("yield" ::: "memory");
        #elif defined(__x86_64__)
        __asm__ volatile("pause" ::: "memory");
        #endif
    }
}
```

**优势**:
- 无互斥锁开销
- 纯用户态操作
- ARM64友好的yield指令
- 支持高并发访问

## 使用方法

### 编译

```bash
cd /home/xuwei/code/hpc-redis/benchmark
gcc -o three_layer_benchmark three_layer_benchmark.c \
    ../src/three_layer_cache.c -I../src -lpthread
```

### 运行默认测试

```bash
./three_layer_benchmark
```

**默认参数**:
- 查询数: 1,000,000
- 线程数: 8
- 预填充: 100,000

### 自定义参数运行

```bash
# 自定义查询数和线程数
./three_layer_benchmark --queries 2000000 --threads 16

# 高并发测试
./three_layer_benchmark --queries 5000000 --threads 32
```

## 预期性能目标

### 在华为鲲鹏930处理器上

| 测试场景 | 目标QPS | 目标延迟 | 目标命中率 |
|---------|----------|----------|-----------|
| Read-Heavy | >10M QPS | <10μs | >90% |
| Write-Heavy | >5M QPS | <20μs | >80% |
| HA故障转移 | >8M QPS | <15μs | >85% |

### 系统资源要求

- **CPU**: 鲲鹏930，64+核心
- **内存**: 16GB系统内存
- **存储**: NVMe SSD用于COLD层持久化
- **网络**: 10Gbps+用于IDC间通信

## 测试输出示例

```
Three-Layer Cache Benchmark
HOT(L3 128K) → WARM(mem 1024K) → COLD(append-only)
HA: 2x3 WeChat-style, Paxos conflict resolution

Pre-filling WARM layer with 100000 entries...
Pre-fill done.

========================================
  Read-Heavy (80R/20W)
  Ops: 1000000, Threads: 8, Write%: 20
========================================

Results:
  Total time:    0.123 seconds
  Throughput:    8.13M QPS
  Avg latency:   8.1 μs
  Read hits:     800000
  Read misses:   20000
  Writes:        200000
  Hit rate:      97.5%

========================================
  Write-Heavy (20R/80W)
  Ops: 500000, Threads: 8, Write%: 80
========================================

Results:
  Total time:    0.089 seconds
  Throughput:    5.62M QPS
  Avg latency:   14.2 μs
  Read hits:     90000
  Read misses:   10000
  Writes:        400000
  Hit rate:      90.0%

--- Simulating IDC 1 failure ---

========================================
  After IDC-1 Failover
  Ops: 250000, Threads: 8, Write%: 20
========================================

Results:
  Total time:    0.045 seconds
  Throughput:    5.56M QPS
  Avg latency:   14.4 μs
  Read hits:     200000
  Read misses:   5000
  Writes:        50000
  Hit rate:      97.5%

--- Recovering IDC 1 ---

========================================
  After IDC-1 Recovery
  Ops: 250000, Threads: 8, Write%: 20
========================================

Results:
  Total time:    0.043 seconds
  Throughput:    5.81M QPS
  Avg latency:   13.9 μs
  Read hits:     200000
  Read misses:   5000
  Writes:        50000
  Hit rate:      97.5%

✅ Benchmark complete.
```

## 总结

`three_layer_benchmark` 是一个综合性的性能和可靠性测试工具，主要验证：

1. **三层缓存架构的有效性**
   - HOT层的索引加速
   - WARM层的LRU管理
   - COLD层的持久化能力

2. **高并发性能**
   - Bitmap CAS无锁并发
   - 多线程扩展性
   - ARM64优化

3. **高可用性**
   - Paxos一致性协议
   - 故障转移机制
   - 自动恢复能力

4. **真实工作负载**
   - Zipfian键分布
   - 读/写混合模式
   - 热/冷数据访问

这个测试用例是HPC-Redis项目中验证核心缓存架构性能的关键工具。