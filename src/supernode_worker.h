/*
 * SuperNode Worker - UB.mem + SVE2 计算层
 * 实现超高性能的纯用户态批量向量计算
 *
 * 核心特性：
 * - Bitmap CAS 无锁并发控制（由 sve_operation 模块提供）
 * - SVE2 Gather Load 批量读取（由 sve_operation 模块提供）
 * - UB.mem 共享内存池直接访问
 *
 * SVE 计算和 bitmap 操作全部委托给 sve_operation.h/c，
 * 本模块只负责 SuperNode 的生命周期管理和 Worker 线程调度。
 */

#ifndef __SUPERNODE_WORKER_H
#define __SUPERNODE_WORKER_H

#include "server.h"
#include "proxy_aggregator.h"
#include "sve_operation.h"
#include <stdint.h>
#include <pthread.h>
#include <stdatomic.h>

/* UB.mem 配置 */
#define UB_MEM_BASE_ADDR 0x100000000ULL
#define UB_MEM_SIZE (4ULL * 1024 * 1024 * 1024 * 1024)
#define UB_MEM_PAGE_SIZE (4 * 1024 * 1024)

/* SVE Worker 上下文 */
typedef struct sve_worker_context {
    int worker_id;
    pthread_t thread;
    int running;

    /* Ring Buffer（从 Proxy 接收）*/
    ring_buffer_t *input_rb;

    /* 读取路径上下文 */
    sve_gather_ctx_t gather_ctx;

    /* SVE 上下文 */
    size_t sve_vl;

    /* 统计信息 */
    atomic_uint_fast64_t total_batches;
    atomic_uint_fast64_t total_requests;
    atomic_uint_fast64_t sve_operations;
    atomic_uint_fast64_t total_latency_us;
    sve_operation_stats_t op_stats;
} sve_worker_context_t;

/* ========== API ========== */
int  supernode_init(int node_id, int num_workers);
void supernode_shutdown(void);

/* UB.mem 管理（mmap 分配/释放，SuperNode 特有）*/
sve_ub_mem_t *ub_mem_init(uint64_t physical_base, size_t size);
void ub_mem_cleanup(sve_ub_mem_t *ub_mem);

/* Worker 线程 */
void *sve_worker_thread(void *arg);
int   sve_worker_process_batch(sve_worker_context_t *ctx, batch_packet_t *packet);

/* 统计 */
sds supernode_get_stats(void);
sds sve_worker_get_stats(sve_worker_context_t *ctx);

#endif /* __SUPERNODE_WORKER_H */
