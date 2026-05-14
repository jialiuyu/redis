/*
 * SuperNode Worker Implementation
 *
 * 只负责：真实 UB 地址空间接入、Worker 线程调度、SuperNode 生命周期。
 * SVE 计算和 bitmap 操作全部直接调用 sve_operation 模块的 sve_* 函数。
 */

#define REDISMODULE_CORE_MODULE

#include "supernode_worker.h"
#include "vector_proxy_completion.h"
#include "macro.h"
#include "ring_buffer_mgr.h"
#include "supernode_protocol.h"
#include "server.h"
#include "ub_client.h"

#include <stddef.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#ifdef __linux__
#include <sched.h>
#endif

/* SuperNode 主结构 */
typedef struct supernode {
    int node_id;
    int num_workers;
    sve_worker_context_t *workers;

    ub_address_space_t *ubas;
    state_bitmap_t *global_bitmap;
    int ub_client_owned;

    int running;
} supernode_t;

static supernode_t *global_supernode = NULL;

/* ========== Worker 线程 ========== */

#define SVE_WORKER_SPIN_PHASE1 64U
#define SVE_WORKER_SPIN_PHASE2 256U

static inline void sve_worker_idle_wait(unsigned int *idle_iters) {
    if (*idle_iters < SVE_WORKER_SPIN_PHASE1) {
        /* Pure spin first to catch the next batch without a syscall. */
        __asm__ volatile("" ::: "memory");
    } else if (*idle_iters < SVE_WORKER_SPIN_PHASE2) {
#if defined(__aarch64__)
        __asm__ volatile("yield" ::: "memory");
#elif defined(__x86_64__)
        __asm__ volatile("pause" ::: "memory");
#else
        __asm__ volatile("" ::: "memory");
#endif
    } else {
        struct timespec ts = {0, 1000}; /* 1 microsecond */
        nanosleep(&ts, NULL);
    }

    (*idle_iters)++;
}

static int sve_worker_process_batch(sve_worker_context_t *ctx, batch_packet_t *packet) {
    RETURN_IF(!ctx || !packet, C_ERR);

    uint64_t start_time = ustime();

    if (packet->magic != BATCH_PACKET_MAGIC) {
        serverLog(LL_WARNING, "Invalid batch packet magic: 0x%x", packet->magic);
        return C_ERR;
    }

    serverLog(LL_DEBUG, "Worker %d processing batch: %u requests, batch_id=%llu",
              ctx->worker_id, packet->num_requests,
              (unsigned long long)packet->batch_id);

    if (packet->op_type != BATCH_PACKET_OP_VEMB) {
        serverLog(LL_WARNING, "Unsupported batch packet op_type: %u", packet->op_type);
        return C_ERR;
    }

    uint64_t *emb_ids = zmalloc(packet->num_requests * sizeof(uint64_t));
    RETURN_IF(!emb_ids, C_ERR);

    for (uint32_t i = 0; i < packet->num_requests; i++)
        emb_ids[i] = packet->requests[i].row_id;

    float *results = zmalloc(packet->num_requests * ctx->gather_ctx.vector_dim * sizeof(float));
    if (!results) { zfree(emb_ids); return C_ERR; }

    /*
     * Use contiguous load: each embedding is a contiguous row in memory,
     * so per-embedding sequential load is optimal. SVE gather load would
     * only help if we needed partial dimensions or column-oriented access.
     */
    int ret = sve_serial_contiguous_read(&ctx->gather_ctx, emb_ids,
                                         packet->num_requests, results);

    if (ret == C_OK) {
        for (uint32_t i = 0; i < packet->num_requests; i++) {
            if (vector_proxy_completion_complete_vemb(packet->requests[i].request_id,
                                                      results + ((size_t)i * ctx->gather_ctx.vector_dim),
                                                      ctx->gather_ctx.vector_dim,
                                                      C_OK) == C_OK) {
                proxy_vector_request_t *req = vector_proxy_completion_lookup(packet->requests[i].request_id);
                if (req && req->bc) {
                    RedisModule_UnblockClient(req->bc, req);
                }
            }
        }
    }

    zfree(results);
    zfree(emb_ids);

    uint64_t latency = ustime() - start_time;
    atomic_fetch_add_explicit(&ctx->total_batches, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&ctx->total_requests, packet->num_requests, memory_order_relaxed);
    atomic_fetch_add_explicit(&ctx->total_latency_us, latency, memory_order_relaxed);

    serverLog(LL_DEBUG, "Worker %d completed batch in %llu μs",
              ctx->worker_id, (unsigned long long)latency);
    return ret;
}

void *sve_worker_thread(void *arg) {
    sve_worker_context_t *ctx = (sve_worker_context_t *)arg;
    unsigned int idle_iters = 0;

    serverLog(LL_NOTICE, "SVE Worker %d started", ctx->worker_id);

#ifdef __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(ctx->worker_id, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
#endif

    while (ctx->running) {
        void *payload = NULL;
        size_t payload_len = 0;

        int ret = ring_buffer_peek(ctx->input_rb, &payload, &payload_len);
        if (ret == C_OK && payload_len > 0) {
            idle_iters = 0;
            if (sve_worker_process_batch(ctx, (batch_packet_t *)payload) == C_OK) {
                ring_buffer_commit_read(ctx->input_rb, payload_len);
            }
        } else {
            sve_worker_idle_wait(&idle_iters);
        }
    }

    serverLog(LL_NOTICE, "SVE Worker %d stopped", ctx->worker_id);
    return NULL;
}

/* ========== SuperNode 生命周期 ========== */

int supernode_init(int node_id, int num_workers) {
    RETURN_IF(global_supernode, C_OK);
    num_workers = server.supernode_workers;
    if (num_workers <= 0) {
        int detected_cpus = sysconf(_SC_NPROCESSORS_ONLN);
        num_workers = max(detected_cpus, 1);
    }

    global_supernode = zcalloc(sizeof(supernode_t));
    if (!global_supernode) goto failed;

    global_supernode->node_id = node_id;
    global_supernode->num_workers = num_workers;
    global_supernode->ub_client_owned = 0;

    if (server.ub.vector_dimension <= 0) {
        serverLog(LL_WARNING, "Invalid UB vector dimension: %d", server.ub.vector_dimension);
        goto failed;
    }

    if (ub_client_init(&server.ub) != C_OK) {
        serverLog(LL_WARNING, "Failed to initialize UB client for SuperNode");
        goto failed;
    }

    if (ub_client_load_embedding_table(server.ub.table_name, &global_supernode->ubas) != C_OK ||
        !global_supernode->ubas) {
        serverLog(LL_WARNING, "Failed to attach UB table for SuperNode");
        goto failed;
    }

    size_t vector_dim = (size_t)server.ub.vector_dimension;
    size_t vector_stride_bytes = global_supernode->ubas->vector_stride_bytes;
    if (vector_stride_bytes < vector_dim * sizeof(float)) {
        serverLog(LL_WARNING,
                  "Invalid UB vector stride: %zu for vector dimension %zu",
                  vector_stride_bytes, vector_dim);
        goto failed;
    }

    size_t table_row_capacity = vector_stride_bytes == 0 ? 0 :
        (uint64_t)(global_supernode->ubas->size / vector_stride_bytes);
    if (table_row_capacity == 0) {
        serverLog(LL_WARNING, "UB table capacity is zero");
        goto failed;
    }

    /* Bitmap — 直接调用 bitmap_init */
    global_supernode->global_bitmap = zmalloc(sizeof(state_bitmap_t));
    if (!global_supernode->global_bitmap ||
        bitmap_init(global_supernode->global_bitmap, table_row_capacity) != 0) {
        goto failed;
    }

    /* Workers */
    global_supernode->workers = zcalloc(sizeof(sve_worker_context_t) * num_workers);
    if (!global_supernode->workers) goto failed;

    if (ring_buffer_mgr_init((size_t)num_workers, RING_BUFFER_SIZE) != C_OK) {
        goto failed;
    }
    if (ring_buffer_mgr_ensure_supernodes((size_t)node_id + 1) != C_OK) {
        goto failed;
    }

    for (int i = 0; i < num_workers; i++) {
        ring_buffer_t *rb = ring_buffer_mgr_get(node_id, i);
        if (!rb) goto failed;
        
        sve_worker_context_t *ctx = &global_supernode->workers[i];
        ctx->worker_id = i;
        ctx->running = 1;
        ctx->input_rb = rb;

        atomic_init(&ctx->total_batches, 0);
        atomic_init(&ctx->total_requests, 0);
        atomic_init(&ctx->sve_operations, 0);
        atomic_init(&ctx->total_latency_us, 0);
        atomic_init(&ctx->op_stats.lock_success, 0);
        atomic_init(&ctx->op_stats.lock_failure, 0);
        sve_gather_ctx_init(&ctx->gather_ctx,
                            global_supernode->ubas,
                            global_supernode->global_bitmap,
                            vector_dim,
                            vector_stride_bytes,
                            table_row_capacity,
                            &ctx->op_stats);

        if (pthread_create(&ctx->thread, NULL, sve_worker_thread, ctx) != 0) {
            serverLog(LL_WARNING, "Failed to create SVE worker %d", i);
            goto failed;
        }
    }

    global_supernode->running = 1;
    serverLog(LL_NOTICE, "SuperNode %d initialized with %d workers", node_id, num_workers);
    return C_OK;

failed:
    supernode_shutdown();
    return C_ERR;
}

void supernode_shutdown(void) {
    RETURN_IF(!global_supernode);

    global_supernode->running = 0;

    if (global_supernode->workers) {
        for (int i = 0; i < global_supernode->num_workers; i++) {
            sve_worker_context_t *ctx = &global_supernode->workers[i];
            if (ctx->running) {
                ctx->running = 0;
                pthread_join(ctx->thread, NULL);
            }
        }
        zfree(global_supernode->workers);
    }

    ring_buffer_mgr_shutdown();

    if (global_supernode->global_bitmap) {
        bitmap_destroy(global_supernode->global_bitmap);
        zfree(global_supernode->global_bitmap);
    }

    if (global_supernode->ub_client_owned)
        ub_client_cleanup();

    zfree(global_supernode);
    global_supernode = NULL;
    serverLog(LL_NOTICE, "SuperNode shutdown");
}

/* ========== 统计 ========== */

sds supernode_get_stats(void) {
    sds stats = sdsempty();

    if (!global_supernode) {
        stats = sdscat(stats, "SuperNode: Not initialized\n");
        return stats;
    }

    stats = sdscatprintf(stats, "SuperNode Stats (Node %d):\n", global_supernode->node_id);
    stats = sdscatprintf(stats, "  Workers: %d\n", global_supernode->num_workers);
    stats = sdscatprintf(stats, "  UB table size: %zu GB\n",
                         global_supernode->ubas->size / (1024 * 1024 * 1024));

    uint64_t tb = 0, tr = 0, ts = 0, tt = 0;
    uint64_t bls = 0, blf = 0;
    uint64_t go = 0, so = 0, ge = 0, se = 0;

    for (int i = 0; i < global_supernode->num_workers; i++) {
        sve_worker_context_t *c = &global_supernode->workers[i];
        tb += atomic_load_explicit(&c->total_batches, memory_order_relaxed);
        tr += atomic_load_explicit(&c->total_requests, memory_order_relaxed);
        ts += atomic_load_explicit(&c->sve_operations, memory_order_relaxed);
        tt += atomic_load_explicit(&c->total_latency_us, memory_order_relaxed);
        bls += atomic_load_explicit(&c->op_stats.lock_success, memory_order_relaxed);
        blf += atomic_load_explicit(&c->op_stats.lock_failure, memory_order_relaxed);
    }

    stats = sdscatprintf(stats, "  Total batches: %llu\n", (unsigned long long)tb);
    stats = sdscatprintf(stats, "  Total requests: %llu\n", (unsigned long long)tr);
    stats = sdscatprintf(stats, "  Bitmap lock success: %llu\n", (unsigned long long)bls);
    stats = sdscatprintf(stats, "  Bitmap lock failure: %llu\n", (unsigned long long)blf);
    stats = sdscatprintf(stats, "  SVE operations: %llu\n", (unsigned long long)ts);

    if (tb > 0) {
        stats = sdscatprintf(stats, "  Avg batch latency: %.1f μs\n", (double)tt / tb);
        stats = sdscatprintf(stats, "  Avg batch size: %.1f\n", (double)tr / tb);
    }

    stats = sdscat(stats, "  Scatter/Gather:\n");
    stats = sdscatprintf(stats, "    Gather ops: %llu  elements: %llu\n",
                         (unsigned long long)go, (unsigned long long)ge);
    stats = sdscatprintf(stats, "    Scatter ops: %llu  elements: %llu\n",
                         (unsigned long long)so, (unsigned long long)se);
    if (go > 0)
        stats = sdscatprintf(stats, "    Gather lane util: %.1f%%\n",
                             100.0 * (double)ge / ((double)go * SVE_OP_VL));
    if (so > 0)
        stats = sdscatprintf(stats, "    Scatter lane util: %.1f%%\n",
                             100.0 * (double)se / ((double)so * SVE_OP_VL));

    return stats;
}

sds sve_worker_get_stats(sve_worker_context_t *ctx) {
    sds stats = sdsempty();
    if (!ctx) { stats = sdscat(stats, "Worker: Invalid\n"); return stats; }

    stats = sdscatprintf(stats, "Worker %d:\n", ctx->worker_id);
    stats = sdscatprintf(stats, "  Batches: %llu  Requests: %llu\n",
                         (unsigned long long)atomic_load_explicit(&ctx->total_batches, memory_order_relaxed),
                         (unsigned long long)atomic_load_explicit(&ctx->total_requests, memory_order_relaxed));
    stats = sdscatprintf(stats, "  Bitmap lock success: %llu\n",
                         (unsigned long long)atomic_load_explicit(&ctx->op_stats.lock_success, memory_order_relaxed));
    stats = sdscatprintf(stats, "  Bitmap lock failure: %llu\n",
                         (unsigned long long)atomic_load_explicit(&ctx->op_stats.lock_failure, memory_order_relaxed));
    return stats;
}
