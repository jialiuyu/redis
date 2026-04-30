/*
 * SuperNode Worker - UB.mem + SVE2 计算层
 * 实现超高性能的纯用户态批量向量计算
 * 
 * 核心特性：
 * - Bitmap CAS 无锁并发控制
 * - SVE2 Gather Load 批量读取
 * - 非临时内存访问（避免 L3 Cache 污染）
 * - UB.mem 共享内存池直接访问
 */

#ifndef __SUPERNODE_WORKER_H
#define __SUPERNODE_WORKER_H

#include "server.h"
#include "proxy_aggregator.h"
#include "sve_config.h"
#include <stdint.h>
#include <pthread.h>
#include <stdatomic.h>

/* 超节点配置 */
#define SUPERNODE_MAX_WORKERS 16        /* 最大 Worker 线程数 */
#define SUPERNODE_EMBEDDING_DIM 300     /* Embedding 维度 */
#define SUPERNODE_MAX_EMBEDDINGS (1024 * 1024 * 1024)  /* 10亿 embeddings */

/* UB.mem 配置 */
#define UB_MEM_BASE_ADDR 0x100000000ULL /* UB.mem 基地址 */
#define UB_MEM_SIZE (4ULL * 1024 * 1024 * 1024 * 1024)  /* 4TB */
#define UB_MEM_PAGE_SIZE (4 * 1024 * 1024)  /* 4MB 大页 */

/* Bitmap 配置 */
#define BITMAP_BITS_PER_WORD 64
#define BITMAP_SIZE (SUPERNODE_MAX_EMBEDDINGS / BITMAP_BITS_PER_WORD)

/* SVE2 配置 */
#define SVE_VECTOR_BITS 256             /* SVE 向量位宽 */
#define SVE_ELEMENTS_PER_VECTOR (SVE_VECTOR_BITS / 32)  /* 每向量元素数（float32）*/

/* 对齐的原子字 - 防止伪共享（False Sharing）
 * 在多核系统中，如果两个原子量位于同一个 CPU 缓存行（通常 64 字节），
 * 会导致严重的性能下降。通过 64 字节对齐，每个原子量独占一个缓存行。
 */
typedef struct aligned_atomic_word {
    atomic_uint_fast64_t word;          /* 原子 64 位字 */
} __attribute__((aligned(64))) aligned_atomic_word_t;

/* 状态 Bitmap - 用于并发控制
 * 使用 C11 原子操作和精确的内存序，实现高性能无锁并发控制
 */
typedef struct state_bitmap {
    aligned_atomic_word_t *bits;        /* 对齐的原子 Bitmap 数据 */
    size_t num_words;                   /* 字数 */
    void *shm_addr;                     /* 共享内存地址 */
    size_t shm_size;                    /* 共享内存大小 */
} state_bitmap_t;

/* UB.mem 地址空间 */
typedef struct ub_memory_space {
    void *base_addr;                    /* 基地址（映射后的虚拟地址）*/
    uint64_t physical_base;             /* 物理基地址 */
    size_t size;                        /* 大小 */
    uint32_t token_id;                  /* 访问令牌 */
    int numa_node;                      /* NUMA 节点 */
} ub_memory_space_t;

/* Embedding 数据结构 */
typedef struct embedding_entry {
    float data[SUPERNODE_EMBEDDING_DIM]; /* 向量数据 */
} __attribute__((aligned(64))) embedding_entry_t;

/* SVE Worker 上下文 */
typedef struct sve_worker_context {
    int worker_id;                      /* Worker ID */
    pthread_t thread;                   /* 线程句柄 */
    int running;                        /* 运行标志 */
    
    /* Ring Buffer（从 Proxy 接收）*/
    ring_buffer_t *input_rb;
    
    /* UB.mem 访问 */
    ub_memory_space_t *ub_mem;
    
    /* 状态 Bitmap */
    state_bitmap_t *bitmap;
    
    /* SVE 上下文 */
    size_t sve_vl;                      /* SVE 向量长度（字节）*/
    
    /* 统计信息 */
    atomic_uint_fast64_t total_batches;
    atomic_uint_fast64_t total_requests;
    atomic_uint_fast64_t locked_skips;  /* 因锁跳过的请求数 */
    atomic_uint_fast64_t sve_operations;
    atomic_uint_fast64_t total_latency_us;
    
} sve_worker_context_t;

/* SuperNode 主结构 */
typedef struct supernode {
    int node_id;                        /* 节点 ID */
    int num_workers;                    /* Worker 数量 */
    sve_worker_context_t *workers;      /* Worker 数组 */
    
    /* UB.mem 共享内存池 */
    ub_memory_space_t *ub_mem;
    
    /* 全局状态 Bitmap */
    state_bitmap_t *global_bitmap;
    
    /* Ring Buffer */
    ring_buffer_t *input_rb;
    
    int running;
    
} supernode_t;

/* 全局超节点实例 */
extern supernode_t *global_supernode;

/* ========== API 函数 ========== */

/* 初始化和清理 */
int supernode_init(int node_id, int num_workers);
void supernode_shutdown(void);

/* Bitmap 操作 - 高性能无锁实现 */
state_bitmap_t *bitmap_create(size_t num_bits);
void bitmap_destroy(state_bitmap_t *bitmap);

/* 测试位状态（非原子快照，仅用于调试）*/
int bitmap_test_bit(state_bitmap_t *bitmap, uint64_t bit_index);

/* 尝试获取（占用）资源位：0 -> 1
 * 使用 CAS 循环，保证原子性
 * @return C_OK 成功获取；C_ERR 已被占用
 */
int bitmap_try_acquire(state_bitmap_t *bitmap, uint64_t bit_index);

/* 释放资源位：1 -> 0
 * 使用原子 fetch_and 操作，比 CAS 循环更高效
 */
void bitmap_release(state_bitmap_t *bitmap, uint64_t bit_index);

/* 兼容旧接口 */
#define bitmap_set_bit_cas bitmap_try_acquire
#define bitmap_clear_bit_cas(bitmap, index) (bitmap_release(bitmap, index), C_OK)

/* UB.mem 操作 */
ub_memory_space_t *ub_mem_init(uint64_t physical_base, size_t size);
void ub_mem_cleanup(ub_memory_space_t *ub_mem);
void *ub_mem_get_embedding_addr(ub_memory_space_t *ub_mem, uint64_t emb_id);

/* SVE Worker */
void *sve_worker_thread(void *arg);
int sve_worker_process_batch(sve_worker_context_t *ctx, batch_packet_t *packet);

/* SVE2 批量操作 */
int sve2_batch_gather_load(sve_worker_context_t *ctx,
                           uint64_t *emb_ids,
                           size_t num_ids,
                           float *results);

int sve2_gather_with_bitmap_check(sve_worker_context_t *ctx,
                                  uint64_t *emb_ids,
                                  size_t num_ids,
                                  float *results,
                                  uint8_t *valid_mask);

/* 非临时内存访问 */
void sve_streaming_load(const void *src, void *dst, size_t size);
void sve_streaming_store(const void *src, void *dst, size_t size);

/* 统计信息 */
sds supernode_get_stats(void);
sds sve_worker_get_stats(sve_worker_context_t *ctx);

#endif /* __SUPERNODE_WORKER_H */
