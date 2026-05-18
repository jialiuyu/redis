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

#include "sve_operation.h"
#include "ring_buffer.h"
#include "monotonic.h"
#include <stddef.h>
#include <stdint.h>
#include <pthread.h>
#include <stdatomic.h>

/* SVE Worker 上下文 */
typedef struct sve_worker_context {
    int worker_id;
    pthread_t thread;
    int running;

    /* Request/response rings */
    ring_buffer_t *input_rb;
    ring_buffer_t *output_rb;

    /* 读取路径上下文 */
    sve_gather_ctx_t gather_ctx;

    /* VEMB hot-path scratch buffers, owned by this worker thread. */
    uint64_t *vemb_row_scratch;
    float *vemb_vector_scratch;
    size_t vemb_scratch_capacity;

    /* 统计信息 */
    atomic_uint_fast64_t total_batches;
    atomic_uint_fast64_t total_requests;
    atomic_uint_fast64_t sve_operations;
    atomic_uint_fast64_t total_latency_us;
    atomic_uint_fast64_t total_queue_latency_us;
    atomic_uint_fast64_t max_queue_latency_us;
    atomic_uint_fast64_t total_gather_latency_us;
    atomic_uint_fast64_t max_gather_latency_us;
    atomic_uint_fast64_t total_compute_latency_us;
    atomic_uint_fast64_t max_compute_latency_us;
    atomic_uint_fast64_t total_response_latency_us;
    atomic_uint_fast64_t max_response_latency_us;
    atomic_uint_fast64_t vemb_scratch_grows;
    atomic_uint_fast64_t vemb_scratch_rows;
    sve_operation_stats_t op_stats;
} sve_worker_context_t;

/* ========== API ========== */
int  supernode_init(int node_id, int num_workers);
void supernode_shutdown(void);

/* Worker 线程 */
void *sve_worker_thread(void *arg);

/* 统计 */
sds supernode_get_stats(void);
sds sve_worker_get_stats(sve_worker_context_t *ctx);

#endif /* __SUPERNODE_WORKER_H */
