# SuperNode vs UB Client — UB.MEM 读写路径对比

本文档梳理两套 UB.MEM 访问实现的读写路径。

## 1. 架构概览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Redis 主进程                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────┐              ┌─────────────────────┐              │
│  │   Vector Engine     │              │    SuperNode        │              │
│  │   (vset.c 调用)     │              │    (独立子系统)      │              │
│  └──────────┬──────────┘              └──────────┬──────────┘              │
│             │                                    │                          │
│             ▼                                    ▼                          │
│  ┌─────────────────────┐              ┌─────────────────────┐              │
│  │   ub_client.c       │              │ supernode_worker.c  │              │
│  │   (数据面客户端)     │              │   (SVE Worker)      │              │
│  └──────────┬──────────┘              └──────────┬──────────┘              │
│             │                                    │                          │
└─────────────┼────────────────────────────────────┼──────────────────────────┘
              │                                    │
              ▼                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           UB.MEM 共享内存                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ Row 0: [float × dim] [padding]                                      │    │
│  │ Row 1: [float × dim] [padding]                                      │    │
│  │ ...                                                                 │    │
│  │ Row N: [float × dim] [padding]                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 2. UB Client 路径（当前 Vector Engine 使用）

### 2.1 初始化流程

```
ub_client_init(cfg)
    │
    ├── 复制配置到 global_ub_client->config
    │
    └── 延迟加载：首次访问时才 mmap
```

### 2.2 读路径（Gather Load）

```
VSIM / VEMB 命令
    │
    ▼
vector_engine_ub_impl.c
    │
    ├── ub_engine_vsim()
    │       │
    │       ▼
    │   ub_client_perform_contiguous_load()  ← 全表扫描
    │       │
    │       ├── 计算 base + start_index * stride
    │       │
    │       ├── [无 padding] memcpy 整块
    │       │
    │       └── [有 padding] 逐行 SVE LD1W + ST1W
    │
    └── ub_engine_vemb()
            │
            ▼
        ub_client_load_single()  ← 单行读取
            │
            └── memcpy(dst, base + index * stride, dim * sizeof(float))
```

**关键特点：**
- 无并发控制（无 bitmap/锁）
- 无 ownership 管理（可选配置）
- 直接 mmap 访问，依赖 OS 页表

### 2.3 写路径（Scatter Store）

```
VADD / VREM 命令
    │
    ▼
vector_engine_ub_impl.c
    │
    ├── ub_engine_vadd()
    │       │
    │       ▼
    │   ub_client_store_single()
    │       │
    │       ├── maybe_set_write_ownership()  ← 可选 OBMM ownership
    │       │
    │       ├── memcpy(base + index * stride, src, dim * sizeof(float))
    │       │
    │       └── maybe_release_ownership()
    │
    └── ub_engine_vrem()
            │
            └── ub_client_store_single(zero_buf)  ← 软删除
```

### 2.4 数据结构

```c
/* 全局客户端 */
ub_client_t {
    ub_mem_config_t config;        // 配置
    ub_address_space_t *global_ubas; // 地址空间
    uint64_t cache_hits/misses;
    uint64_t total_requests;
}

/* 地址空间 */
ub_address_space_t {
    void *mapped_addr;             // mmap 后的虚拟地址
    size_t size;                   // 表大小
    size_t vector_stride_bytes;    // 行步长
    int shm_fd;                    // 设备文件描述符
    int cacheable;                 // 是否可缓存
    int use_ownership;             // 是否使用 OBMM ownership
}
```

## 3. SuperNode 路径（独立子系统）

### 3.1 初始化流程

```
supernode_init(node_id, num_workers)
    │
    ├── ub_mem_init()
    │       │
    │       └── mmap(MAP_HUGETLB)  ← 大页映射
    │
    ├── bitmap_create()  ← 并发控制 bitmap
    │
    ├── ring_buffer_create()  ← 输入队列
    │
    └── 启动 N 个 sve_worker_thread()
```

### 3.2 读路径（Gather Load with Bitmap）

```
Proxy Aggregator
    │
    ▼
Ring Buffer (batch_packet_t)
    │
    ▼
sve_worker_thread()
    │
    ▼
sve_worker_process_batch()
    │
    ├── 提取 emb_ids[]
    │
    └── sve2_batch_gather_load()
            │
            ▼
        sve_gather_read()  (真正的 SVE gather load)
            │
            ├── for each block of 8 embeddings:
            │       │
            │       ├── bitmap_try_acquire(emb_id)  ← CAS 获取锁
            │       │       │
            │       │       ├── [失败] valid_mask[i] = 0, 跳过
            │       │       │
            │       │       └── [成功] 继续
            │       │
            │       ├── ub_mem_get_embedding_addr(emb_id)
            │       │
            │       ├── SVE LD1W 加载向量
            │       │
            │       └── bitmap_release(emb_id)  ← 释放锁
            │
            └── 返回 results[] + valid_mask[]
```

**关键特点：**
- Bitmap CAS 无锁并发控制
- Lock-Free 策略：遇锁不等待，跳过
- SVE2 向量指令加速
- 大页内存（4MB HugePage）

### 3.3 数据结构

```c
/* 超节点主结构 */
supernode_t {
    int node_id;
    int num_workers;
    sve_worker_context_t *workers;
    ub_memory_space_t *ub_mem;
    state_bitmap_t *global_bitmap;
    ring_buffer_t *input_rb;
}

/* UB 内存空间 */
ub_memory_space_t {
    void *base_addr;               // mmap 后的虚拟地址
    uint64_t physical_base;        // 物理基地址
    size_t size;
    uint32_t token_id;             // 访问令牌
    int numa_node;
}

/* 状态 Bitmap */
state_bitmap_t {
    aligned_atomic_word_t *bits;   // 64 字节对齐的原子字
    size_t num_words;
}

/* Embedding 条目 */
embedding_entry_t {
    uint64_t id;
    float data[SUPERNODE_EMBEDDING_DIM];
} __attribute__((aligned(64)));
```

## 4. 关键差异对比

| 维度 | UB Client | SuperNode |
|------|-----------|-----------|
| **定位** | Vector Engine 数据面 | 独立批处理子系统 |
| **并发控制** | 无（或 OBMM ownership） | Bitmap CAS 无锁 |
| **内存映射** | 普通 mmap | MAP_HUGETLB 大页 |
| **访问模式** | 单次请求同步 | 批量异步（Ring Buffer） |
| **线程模型** | Redis 主线程 | 独立 Worker 线程池 |
| **SVE 使用** | 可选（编译时） | 核心路径 |
| **锁策略** | 无 | Lock-Free（遇锁跳过） |
| **数据布局** | 可配置 stride | 固定 64B 对齐 |

## 5. 读写路径图示

### 5.1 UB Client 读路径

```
┌──────────────┐
│ VSIM/VEMB    │
│ 命令         │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ ub_engine_   │
│ vsim/vemb    │
└──────┬───────┘
       │
       ▼
┌──────────────┐     ┌─────────────────────────────────────┐
│ ub_client_   │     │           UB.MEM                    │
│ perform_     │────▶│  ┌─────┬─────┬─────┬─────┬─────┐   │
│ contiguous/  │     │  │ R0  │ R1  │ R2  │ ... │ Rn  │   │
│ gather_load  │◀────│  └─────┴─────┴─────┴─────┴─────┘   │
└──────────────┘     └─────────────────────────────────────┘
       │
       │ memcpy / SVE LD1W
       ▼
┌──────────────┐
│ 本地 buffer  │
│ (results[])  │
└──────────────┘
```

### 5.2 SuperNode 读路径

```
┌──────────────┐
│ Proxy        │
│ Aggregator   │
└──────┬───────┘
       │ batch_packet_t
       ▼
┌──────────────┐
│ Ring Buffer  │
└──────┬───────┘
       │
       ▼
┌──────────────┐     ┌─────────────────────────────────────┐
│ SVE Worker   │     │         Bitmap (CAS)                │
│ Thread       │────▶│  ┌─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┐   │
└──────┬───────┘     │  │0│1│0│0│1│0│0│0│1│0│0│0│0│0│0│   │
       │             │  └─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┘   │
       │ try_acquire └─────────────────────────────────────┘
       │
       ▼
┌──────────────┐     ┌─────────────────────────────────────┐
│ SVE2 Gather  │     │           UB.MEM (HugePage)         │
│ Load         │────▶│  ┌─────┬─────┬─────┬─────┬─────┐   │
│              │     │  │ E0  │ E1  │ E2  │ ... │ En  │   │
│              │◀────│  └─────┴─────┴─────┴─────┴─────┘   │
└──────┬───────┘     └─────────────────────────────────────┘
       │
       │ SVE LD1W + ST1W
       ▼
┌──────────────┐
│ results[] +  │
│ valid_mask[] │
└──────────────┘
       │
       │ release
       ▼
┌──────────────┐
│ Bitmap       │
│ (释放锁)     │
└──────────────┘
```

## 6. 统一建议

当前存在两套独立的 UB.MEM 访问实现，建议：

1. **短期**：保持两套并存
   - UB Client 用于 Vector Engine（简单同步访问）
   - SuperNode 用于高吞吐批处理场景

2. **中期**：统一底层访问层
   - 抽取公共的 `ub_mem_access.c`
   - 提供 `ub_mem_read_single()` / `ub_mem_read_batch()` / `ub_mem_write_single()`
   - 上层按需选择是否使用 bitmap 并发控制

3. **长期**：考虑是否需要 SuperNode
   - 如果 Vector Engine 路线 A 改造完成（HNSW + UB 存储）
   - SuperNode 的批处理能力可能被 HNSW 图遍历 + scatter_load 替代
