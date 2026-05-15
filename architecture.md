# HPC-Redis 架构详解

---

## 一、项目概述

HPC-Redis 是基于 Redis 7.2 的高性能向量计算引擎（High-Performance Computing Redis），在标准 Redis 之上构建了超大规模向量检索与计算系统。项目主要面向华为鲲鹏 930 ARM64 平台（openEuler 22.03 LTS-SP3），利用硬件特性实现极致性能。

### 核心创新点

| 特性 | 说明 | 性能目标 |
|------|------|----------|
| **UB 高速总线** | 用户态零拷贝通信，4TB 共享内存池 | 延迟 < 1μs |
| **ARM SVE2 加速** | 可伸缩向量扩展（256/512位），批量余弦/GEMM | 4-way FMA 流水线隐藏延迟 |
| **超节点架构** | Bitmap CAS 无锁并发 + SVE2 Gather Load | 16 线程并行 |
| **三层缓存** | HOT(16B) → WARM(1200B) → COLD(追加写) | L1 缓存行对齐 |
| **智能聚合器** | 微秒级批量聚合，一致性哈希分发 | 3000-6000 请求/批次 |

### 当前状态

- 分支 `hpc-redis`（16 次提交）
- **已链接入 redis-server 的组件**：vector_engine、ub_client、sve_compute、batch_processor
- **独立运行组件**：proxy_aggregator、supernode_worker、three_layer_cache 系列
- **部分桩实现**：UB 后端的 vadd/vsim 等函数返回模拟数据；SVE2 数学库（`sve2_gemm.h`、`sve2_adam.h`）为生产级代码

---

## 二、构建与测试

### 构建命令

```bash
make                            # 标准构建
make BUILD_WITH_MODULES=yes     # 含模块（RedisBloom/RediSearch/RedisJSON/RedisTimeSeries/vector-sets）
make SANITIZER=address          # AddressSanitizer 构建
make noopt                      # 无优化调试 (-O0)
make valgrind                   # Valgrind 构建
make clean                      # 清理（保留 deps/）
make distclean                  # 完全清理（含 deps/）
```

**构建前提**：首次克隆需先构建 `deps/` 目录。xxhash 需手动构建（见 `BUILD_GUIDE.md`）。

**构建产物**（在 `src/` 下）：
- `redis-server`（含 HPC 组件）、`redis-cli`、`redis-benchmark`
- `redis-check-rdb`、`redis-check-aof`
- `redis-sentinel`（符号链接到 redis-server）
- `tlc-*-server`（独立 TLC 缓存服务器，不链接入 redis-server）

### 测试命令

```bash
make test                       # 完整 Tcl 测试套件
./runtest --single unit/type/string   # 单个测试文件
./runtest --host <h> --port <p>       # 对外部服务器测试
./runtest-moduleapi                    # 模块 API 测试
./runtest-sentinel                     # Sentinel 测试
./runtest-cluster                      # 集群测试
```

测试目录：`tests/unit/`、`tests/unit/type/`、`tests/integration/`、`tests/cluster/`、`tests/sentinel/`、`tests/vectorset/`

---

## 三、系统架构

### 3.1 Redis 核心事件循环（未修改）

Redis 核心代码保持不变，仍然是单线程命令执行模型：

```
main() [server.c:7566]
  → initServer()                    # 初始化服务器结构体
  → aeCreateEventLoop()             # 创建事件循环 (epoll/kqueue/evport/select)
  → aeMain()                        # 进入主循环
       ↓
  事件循环 [ae.c] — 单线程处理所有事件
       ↓
  ┌────────────────┬────────────────┬────────────────┐
  ↓                ↓                ↓                ↓
 连接接受        请求读取         定时事件        I/O 线程
 [networking.c]  [networking.c]   [server.c cron]  [iothread.c]
  ↓                ↓
 createClient   readQueryFromClient()
                   ↓
             processInputBuffer()
                   ↓
             processCommand() → 命令查找表
                   ↓
             addReply*() → 输出缓冲区 → I/O 线程写回
```

**关键特性**：
- 单线程命令执行（避免数据竞争）
- I/O 线程（`iothread.c`）并行处理 socket 读写
- 全局状态：`struct redisServer server`（`server.h`，约 4400 行的巨型结构体）

### 3.2 HPC 向量数据流

当收到向量相关命令时，请求沿以下路径处理：

```
客户端 RESP 请求 (VADD/VREM/VSIM/VEMB)
  │
  ↓ ① 标准协议解析
 networking.c — RESP 协议解析（完全未修改）
  │
  ↓ ② 向量引擎路由
 vector_engine.c — 抽象层：选择 Redis HNSW 或 UB 后端
  │               type=0 → redis_engine_* (标准 Redis 路径)
  │               type=1 → ub_engine_*   (UB 高速路径)
  │
  ↓ ③ 批量收集
 batch_processor.c — 50μs 超时批次，最大 1024 请求
  │                  后台 pthread 线程处理
  │
  ↓ ④ 智能聚合分发（独立组件）
 proxy_aggregator.c — 一致性哈希 (MurmurHash3)
  │                    3000-6000 请求/批，200μs 时间窗口
  │                    SPSC Ring Buffer 零拷贝传输
  │
  ↓ ⑤ 超节点计算（独立组件）
 supernode_worker.c — 最多 16 个 SVE2 工作线程
  │                    Bitmap CAS 无锁并发控制
  │                    4TB UB.mem 共享内存池直接访问
  │
  ↓ ⑥ SVE2 向量加速
 sve_compute.c — ARM SVE2 gather+余弦相似度
  │              1M embedding 缓存 (LRU)
  │              sve2_gemm.h — 矩阵乘法（4-累加器展开）
  │              sve2_adam.h — Adam 优化器
  │
  ↓ ⑦ UB 总线通信
 ub_client.c — 用户态零拷贝
                dlopen("libubios.so") 运行时加载
                dlopen("libsve.so") SVE 库
  │
  ↓ ⑧ 三层缓存（独立组件）
 three_layer_cache*.c
    HOT (128K × 16B) → WARM (1M × 1200B) → COLD (64 追加段)
```

### 3.3 Redis 核心修改量（极小）

仅修改 3 个文件，约 20 行代码：

| 文件 | 位置 | 修改内容 |
|------|------|----------|
| `src/server.h` | 第 49-52 行 | 定义 `VECTOR_ENGINE_REDIS=0`、`VECTOR_ENGINE_UB=1`、`vector_engine_type_t` 类型 |
| `src/server.h` | 第 1921-1923 行 | 在 `redisServer` 结构体中增加 `vector_engine_type` + `vector_engine_enabled` 字段 |
| `src/config.c` | 第 111-115 行 | `vector_engine_enum[]` 枚举映射 |
| `src/config.c` | 第 2623-2632 行 | `updateVectorEngine()` 配置变更回调函数 |
| `src/config.c` | 第 3198 行 | `createEnumConfig("vector-engine", ...)` 注册为可运行时修改的配置项 |
| `src/Makefile` | 第 385 行 | 在 `REDIS_SERVER_OBJ` 末尾追加 HPC 目标文件 |

---

## 四、组件详解

### 4.1 组件分类总览

**已链接入 redis-server 的组件**（在标准 `redis-server` 二进制中可用）：

| 组件 | 文件 | 功能 | 行数 |
|------|------|------|------|
| 向量引擎 | `vector_engine.c/h` | 抽象层，统一 Redis HNSW 与 UB 后端 | 339 |
| 批量处理器 | `batch_processor.c/h` | 50μs 批次收集，后台 pthread | ~200 |
| UB 客户端 | `ub_client.c/h` | UB 总线客户端，dlopen 外部库 | ~300 |
| SVE 计算 | `sve_compute.c/h` | SVE2 向量运算 + 1M 缓存 | ~400 |
| SVE2 GEMM | `sve2_gemm.h` | 纯头文件矩阵乘法 | 398 |
| SVE2 Adam | `sve2_adam.h` | 纯头文件优化器 | 118 |
| Aeron IPC | `aeron_ipc.h` | 纯头文件 SPSC 环形缓冲区 | 217 |

**独立组件**（不在 redis-server 中，用于独立 TLC 服务器和基准测试）：

| 组件 | 文件 | 功能 |
|------|------|------|
| 代理聚合器 | `proxy_aggregator.c/h` | 智能批量聚合，一致性哈希，环形缓冲区 |
| 超节点工作器 | `supernode_worker.c/h` | Bitmap CAS 工作线程，SVE2 Gather Load |
| 三层缓存 | `three_layer_cache.c/h` | HOT→WARM→COLD 基础实现 |
| UB 三层缓存 | `three_layer_cache_ub.c/h` | UB 内存后端，SVE2 融合计算 |
| TLC 服务器 | `tlc_*_server.c` (8个) | 独立缓存服务器（支持多种传输协议） |
| KCP | `ikcp.c/h` | 可靠 UDP 协议 |
| URMA | `urma.c/h` | 用户态 RDMA |

### 4.2 向量引擎 (Vector Engine)

`vector_engine.c/h` — 提供统一的后端抽象：

```c
// 虚表结构（函数指针表）
typedef struct vector_engine {
    vector_engine_type_t type;
    int  (*init)(void);
    void (*cleanup)(void);
    int  (*vadd)(void *ctx, void *key, vector_data_t *vec, void *elem, void *attr);
    int  (*vrem)(void *ctx, void *key, void *elem);
    int  (*vsim)(void *ctx, void *key, vector_data_t *query, size_t count,
                  vector_query_result_t **results, size_t *num_results);
    int  (*vemb)(void *ctx, void *key, void *elem, vector_data_t *result);
    int  (*vcard)(void *ctx, void *key);
    int  (*vdim)(void *ctx, void *key);
    int  (*set_config)(const char *key, const char *value);
    sds  (*get_config)(const char *key);
    sds  (*get_stats)(void);
} vector_engine_t;
```

**运行时切换**：通过 `CONFIG SET vector-engine ub` 在运行时切换后端，无需重启。`config.c` 中的 `updateVectorEngine()` 回调调用 `vector_engine_switch()` 实现切换。

### 4.3 批量处理器 (Batch Processor)

`batch_processor.c/h` — 高吞吐量请求聚合：

- **最大批次**：1024 个请求
- **超时**：50 微秒（`BATCH_TIMEOUT_US=50000`）
- **并发批次**：8 个（`MAX_CONCURRENT_BATCHES=8`）
- **后台线程**：`batch_processor_thread()` 在独立 pthread 中运行
- **提交接口**：`batch_submit_vemb_request(key, element, result, timeout_us)`

### 4.4 UB 客户端 (UB Client)

`ub_client.c/h` — 与 UB 高速总线通信：

**实体类型**：
- `COMPUTE_NODE` — 计算节点
- `MEMORY_TILE` — 内存瓦片
- `FABRIC_MANAGER` — 集线管理器
- `STORAGE_NODE` — 存储节点

**消息类型**：`MEMORY_LOAD`、`MEMORY_QUERY`、`MEMORY_INFO`、`VECTOR_GATHER`、`VECTOR_SCATTER`

**关键 API**：
- `ub_client_init()` / `ub_client_cleanup()`
- `ub_client_connect_fabric_manager()` — 连接集线管理器
- `ub_client_load_embedding_table(name, &addr_space)` — 加载嵌入表到地址空间
- `ub_client_perform_gather_load(addr_space, indices, n, results, dim)` — SVE Gather Load
- `ub_mmap_remote_memory(ubas_addr, size, token_id, &local)` — 零拷贝内存映射

**外部依赖**：运行时通过 `dlopen` 加载 `libubios.so`（UB 总线固件库）和 `libsve.so`（SVE 运算库），这两个库不在仓库中。

### 4.5 SVE 计算引擎 (SVE Compute)

`sve_compute.c/h` — ARM SVE2 加速的向量运算：

**核心能力**：
- **批量 Gather/Scatter**：从非连续内存地址批量加载/存储向量
- **相似度计算**：余弦相似度（SVE2 FMA + 水平求和）
- **1M Embedding 缓存**：LRU 缓存减少内存访问
- **量化/反量化**：FP32 ↔ INT8 量化（减少内存带宽占用）
- **流式加载/存储**：非临时访问（bypass 缓存，适合大批量数据）

**特性检测**：`sve_detect_capabilities(ctx)` 自动检测 SVE/SVE2/BF16 支持情况。

### 4.6 SVE2 头文件数学库

**`sve2_gemm.h`** (398 行) — 纯头文件 SVE2 矩阵运算：
- `sve2_gemv_f32` — GEMV 微内核（4 累加器展开，2 行预取）
- `sve2_gemm_f32` — 完整 GEMM（L3 分块，tile_K=256，4 行寄存器分块）
- `sve2_cosine_similarity` — 余弦相似度（dot + norm + div）
- `sve2_fused_gather_similarity` — 融合 Gather+余弦（单次遍历）
- `sve2_fused_gather_gemm` — 融合 Gather+GEMM（单次遍历）

**`sve2_adam.h`** (118 行) — 纯头文件 Adam 优化器：
- `sve2_adam_update` — 向量化 Adam/AdamW 参数更新
- 一次处理 8 个 float（SVE 256 位宽度）

**跨平台兼容**：所有 SVE 代码使用 `#ifdef __ARM_FEATURE_SVE` 保护，在 x86 上回退到标量循环。

### 4.7 代理聚合器 (Proxy Aggregator) — 独立组件

`proxy_aggregator.c/h` — 智能批量聚合与分发：

**一致性哈希** (MurmurHash3)：
- 每个物理节点 10 个虚拟节点
- 二分查找定位节点
- 读写锁保护环结构
- 支持动态添加/删除节点

**环形缓冲区** (Ring Buffer)：
- `shm_open + mmap` 共享内存实现
- 16MB 缓冲区
- 原子 head/tail 指针
- 零拷贝数据传输
- 支持跨进程通信

**批量触发条件**：
- 数量达标：`count >= 3000`
- 时间达标：`age >= 200μs`
- 双重条件取先到者

### 4.8 超节点工作器 (SuperNode Worker) — 独立组件

`supernode_worker.c/h` — UB.mem 计算引擎：

**无锁并发** (Bitmap CAS)：
- 64 字节对齐的原子字（`aligned_atomic_word_t`）避免 false sharing
- `bitmap_try_acquire` — CAS 自旋锁：0→1（获取）
- `bitmap_release` — atomic fetch_and：1→0（释放）
- `__ATOMIC_ACQ_REL` 内存序

**UB 内存空间** (`ub_memory_space_t`)：
- 物理基地址 + 映射大小 + NUMA 节点
- 4TB 共享内存池
- 4MB 页面大小

**工作线程**：最多 16 个 SVE2 工作线程（`sve_worker_thread`），每个线程独立处理环形缓冲区中的批量请求。

---

## 五、三层缓存系统

### 5.1 层次结构

三层缓存（Three-Layer Cache, TLC）采用层次化设计，利用缓存行对齐和不同策略优化各层访问：

| 层 | 容量 | 条目大小 | 机制 | 并发控制 |
|----|------|----------|------|----------|
| **HOT** | 128K (2^17) | 16 字节 | 开放寻址哈希，4 条目/缓存行 | ARM LDP/STP 原子，无锁 |
| **WARM** | 1M (2^20) | 1200 字节 | 哈希表 + Bitmap 锁 | Bitmap CAS，锁外 memcpy |
| **COLD** | 64 段 | 1200 字节 | 追加写段，偏移索引 | Bitmap CAS，顺序写 |

### 5.2 HOT 层结构

```c
// 精确 16 字节（一个缓存行放 4 个条目）
typedef struct {
    uint64_t key;         // 8 字节键
    int32_t  warm_idx;    // WARM 层索引（直接指针跳转）
    uint32_t _pad;        // 对齐填充
} hot_index_t;

typedef struct {
    hot_index_t *table;          // 开放寻址哈希表
    size_t       capacity;       // 128K
    uint32_t     mask;           // capacity - 1（快速取模）
    bitmap_lock_t bmp;           // 位图锁
    atomic_uint_fast64_t hits, misses;  // 统计
} hot_layer_t;
```

**设计亮点**：16 字节正好是 ARM LDP/STP 指令的原子操作粒度，实现真正的无锁读取。

### 5.3 WARM 层结构

```c
typedef struct {
    uint64_t key;
    uint8_t  value[1200];        // 值数据
    entry_state_t state;         // 条目状态
    uint64_t write_ts_ns;        // 写入时间戳
    uint64_t ttl_ns;             // TTL
    uint32_t access_count;       // 访问计数
} warm_entry_t;
```

**锁策略**：先在 Bitmap 中 CAS 获取对应位的锁，然后在条目上执行 memcpy（锁外），最后释放锁。这样 memcpy 的时间不阻塞其他不冲突的访问。

### 5.4 COLD 层结构

追加写（append-only）设计：
- 最多 64 个段（`TLC_MAX_COLD_SEGMENTS`），每段 1M
- 新条目追加到当前活跃段
- 段满时创建新段
- 偏移索引支持随机读取

### 5.5 UB 变体 (three_layer_cache_ub)

UB 变体将所有层次的后端存储替换为 UB 共享内存：

- **4 个 UB 节点**：一致性哈希环分布在 4 个物理 UB 节点上
- **SVE2 融合计算**：Gather + 点积在单次遍历中完成
- **额外 API**：`tlc_sve2_similarity`、`tlc_sve2_gemm`、`tlc_sve2_gather`

### 5.6 TLC 独立服务器

多种传输后端的独立缓存服务器（`src/tlc_*_server.c`）：

| 服务器 | 传输协议 | 特点 |
|--------|----------|------|
| `tlc_server.c` | 基础 | 本地内存缓存 |
| `tlc_unified_server.c` | 统一 | 多协议统一抽象 |
| `tlc_aeron_server.c` | Aeron | 共享内存 SPSC，目标 < 0.1μs |
| `tlc_dpdk_server.c` | DPDK | 内核旁路网络 |
| `tlc_fc_server.c` | FC | Fibre Channel 存储网络 |
| `tlc_urma_server.c` | URMA | 用户态 RDMA |
| `tlc_v10_server.c` | V10 | Aeron IPC 迭代版本 |
| `tlc_v13_server.c` | V13 | 优化迭代 |
| `tlc_v14_server.c` | V14 | 最新迭代 |

---

## 六、无锁与 SIMD 实现细节

### 6.1 Bitmap CAS 无锁机制

项目中广泛使用 Bitmap CAS（Compare-And-Swap）实现无锁并发：

```c
// 获取锁（自旋等待）
bmp_lock_acquire(bl, bucket) {
    // 1. 以 relaxed 序加载当前 Bitmap 字
    // 2. 测试目标位是否为 0
    // 3. CAS：0→1，memory_order_acquire
    // 4. 失败则自旋重试
}

// 释放锁
bmp_lock_release(bl, bucket) {
    // atomic fetch_and 清除目标位
    // memory_order_release
}
```

**防 false sharing**：原子字 64 字节对齐（`aligned_atomic_word_t`），确保不同位不会共享缓存行。

**平台优化**：ARM64 使用 `yield` 指令，x86 使用 `_mm_pause`。

### 6.2 SPSC 环形缓冲区

`aeron_ipc.h` 实现的零锁零系统调用消息传递：

- **生产者**：只写 `tail`（`__atomic_store_n` + release）
- **消费者**：只写 `head`（`__atomic_store_n` + release）
- **容量**：4096 槽 × ~24KB/槽 ≈ 96MB
- **自适应自旋**：纯自旋 → yield → nanosleep 三阶段

### 6.3 SVE2 SIMD 加速

ARM SVE2 的关键优势：
- **可变向量长度**：同一段代码适配 128-2048 位
- **Predicate 寄存器**：自动处理尾部元素（无需循环展开特殊处理）
- **Gather/Scatter**：非连续内存的高效批量加载/存储
- **FMA 流水线**：4 周期延迟，4-way 累加器展开完全隐藏

**GEMV 微内核优化**（`sve2_gemm.h`）：
```
4 个累加器 (acc0-acc3) 交错计算
  ↓ 隐藏 4 周期 FMA 延迟
2 行预取 (prefetch 2-4 条 cache line 提前)
  ↓ 数据预取与计算重叠
tile_K=256 分块
  ↓ L3 缓存友好
```

---

## 七、配置参数

### 已注册的 Redis 配置（可运行时修改）

```conf
vector-engine redis|ub           # 向量后端选择（redis=标准HNSW, ub=UB高速总线）
```

### HPC 专用配置（在 `redis-ub-sve.conf` 中定义，由组件自行解析）

```conf
# ── 代理聚合器 ──
proxy-aggregator-enabled yes     # 是否启用代理聚合器
proxy-batch-limit 3000           # 批量触发数量 (推荐 3000-6000)
proxy-timeout-us 200             # 批量超时微秒 (200-500μs)
proxy-num-supernodes 150         # 超节点数量 (600TB / 4TB)
proxy-ring-buffer-size 16MB      # 环形缓冲区大小

# ── 超节点 ──
supernode-id 0                   # 本节点 ID (0-149)
supernode-num-workers 16         # SVE2 工作线程数
supernode-ub-mem-base 0x100000000  # UB 内存物理基地址
supernode-ub-mem-size 4TB        # UB 内存大小

# ── SVE 向量 ──
sve-vector-bits 256              # 向量位宽 (256/512)
sve2-enabled yes                 # 是否启用 SVE2
sve-non-temporal-access yes      # 非临时访问 (bypass 缓存)
sve-batch-size 1024              # SVE 批量大小

# ── Bitmap CAS ──
bitmap-cas-enabled yes           # 是否启用 Bitmap CAS
bitmap-size 16777216             # Bitmap 大小 (10^9 / 64)
bitmap-cas-retry 3               # CAS 重试次数

# ── UB 固件 ──
ub-firmware-lib /usr/lib/libubios.so  # UB 固件库路径
ub-fabric-manager-eid 0x0001     # 集线管理器实体 ID
ub-connect-timeout 5000          # 连接超时 ms
ub-request-timeout 1000          # 请求超时 ms

# ── I/O 线程 ──
io-threads 4                     # I/O 线程数 (1-128)
io-threads-do-reads yes          # I/O 线程也做请求读取

# ── 内存 ──
maxmemory 8GB                    # Redis 最大内存 (不含 UB.mem)
maxmemory-policy allkeys-lru     # 淘汰策略
```

---

## 八、基准测试

```bash
cd benchmark && make             # 构建所有基准测试二进制
make test                        # 快速 1M 查询
make benchmark                   # 完整 10M 查询
make stress                      # 压力 100M 查询
```

**关键基准测试**：
- `three_layer_benchmark_v4` — 本地三层缓存
- `three_layer_benchmark_ub` — UB 后端三层缓存
- `tlc_*_bench` — 各传输协议单独测试
- `tlc_client_bench` — 完整客户端测试
- `supernode_benchmark` — 超节点性能

**结果解析**：`benchmark/parse_tlc_bench.py`

---

## 九、代码约定

| 约定 | 说明 |
|------|------|
| 语言标准 | C99/C11（`_Atomic` 需要 C11 编译器） |
| 内存管理 | `zmalloc`/`zfree`（包装器，跟踪总内存） |
| 日志 | `serverLog(LL_NOTICE, ...)`（Redis 标准） |
| 全局状态 | `struct redisServer server`（extern） |
| HPC 全局变量 | `global_ub_client`、`global_sve_context` 等（`global_` 前缀） |
| SVE 代码保护 | `#ifdef __ARM_FEATURE_SVE`（非 ARM 回退到标量 memcpy） |
| 非临时访问 | `#pragma omp simd` 或 `svstnt1_*` 流式存储 |
| Bitmap CAS | `__atomic_compare_exchange_n` + `__ATOMIC_ACQ_REL` |
| 许可证 | RSALv2/SSPLv1/AGPLv3 三重许可 |

---

## 十、全局变量一览

| 全局变量 | 类型 | 定义文件 |
|----------|------|----------|
| `server` | `struct redisServer` | `server.c` (Redis 标准) |
| `current_vector_engine` | `vector_engine_t *` | `vector_engine.c/h` |
| `global_ub_client` | `ub_client_t *` | `ub_client.c/h` |
| `global_sve_context` | `sve_context_t *` | `sve_compute.c/h` |
| `global_batch_processor` | `batch_processor_t *` | `batch_processor.c/h` |
| `global_proxy_aggregator` | `proxy_aggregator_t *` | `proxy_aggregator.c/h` |
| `global_supernode` | `supernode_t *` | `supernode_worker.c/h` |

---

## 十一、注意事项与陷阱

1. **无 .gitignore** — `src/*.o`、`src/*.d`、`src/redis-*`、`src/tlc-*-server` 二进制文件已被提交到仓库。执行 `make clean` 后 `git status` 会显示大量删除。
2. **UB 库不在仓库中** — `ub_client.c` 调用 `dlopen("libubios.so")` 和 `dlopen("libsve.so")`，这两个是华为专有库，需单独安装。
3. **预编译文件** — 初始提交包含为 ARM 预编译的 `.o/.d` 文件，在 x86 上不可用，需重新编译。
4. **Ascend 工具链** — 部分基准测试链接 `/usr/local/Ascend/ascend-toolkit/`（NPU GEMM 卸载）。
5. **向量命令** — VADD/VREM/VSIM/VEMB 的 JSON 定义不在 `src/commands/` 中，来自外部 `modules/vector-sets/` 模块（HNSW 实现）。内部 `vector_engine.c` 是后端抽象层。
6. **Paxos/HA** — `paxos_acceptor_t`、`ha_manager_t` 等结构已定义但未完全实现（远景架构）。
7. **部分桩实现** — UB 后端部分函数（`ub_engine_vadd`、`ub_engine_vsim` 等）返回错误或模拟数据。SVE2 数学头文件为生产级代码。
8. **日期差异** — `WORK_COMPLETED.md` 标注 2026-02-03，但 git 历史从 2026-04-08 开始（代码从外部源导入）。

---

## 十二、Agent 工具

- `tools/proposal_agent.py` — 交互式方案文档生成智能体，接收用户输入并输出结构化 Markdown 方案文档
- `tools/PROPOSAL_AGENT_README.md` — 使用说明与示例

---

## 十三、文件地图

### Redis 核心（未修改）
`ae.c`(事件循环) · `networking.c`(RESP协议) · `db.c`(键空间) · `object.c`(robj对象) · `rdb.c`(RDB快照) · `aof.c`(AOF持久化) · `replication.c`(主从复制) · `cluster.c`(集群) · `sentinel.c`(哨兵) · `config.c`(配置, 已修改) · `server.h`(服务器, 已修改)

### 数据结构
`t_string.c` · `t_list.c` · `t_set.c` · `t_zset.c` · `t_hash.c` · `t_stream.c` · `sds.c`(动态字符串) · `dict.c`(哈希表) · `kvstore.c`(Slot哈希) · `quicklist.c`(紧凑链表) · `listpack.c`(紧凑包) · `rax.c`(基数树) · `ebuckets.c`(分层过期) · `fwtree.c`(Fenwick树)

### HPC 新增代码
`vector_engine.c/h` · `batch_processor.c/h` · `ub_client.c/h` · `sve_compute.c/h` · `sve2_gemm.h` · `sve2_adam.h` · `aeron_ipc.h` · `proxy_aggregator.c/h` · `supernode_worker.c/h` · `three_layer_cache.c/h` · `three_layer_cache_ub.c/h` · `tlc_*_server.c` · `ikcp.c/h` · `urma.c/h`

### 文档
`CLAUDE.md` · `architecture.md`(本文件) · `BUILD_GUIDE.md` · `WORK_COMPLETED.md` · `THREE_LAYER_BENCHMARK_GUIDE.md` · `V10_BENCHMARK_REPORT.md` · `README_UB_SVE.md` · `QUICKSTART_UB_SVE.md`