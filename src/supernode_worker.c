/*
 * SuperNode Worker Implementation
 *
 * 只负责：真实 UB 地址空间接入、Worker 线程调度、SuperNode 生命周期。
 * SVE 计算和 bitmap 操作全部直接调用 sve_operation 模块的 sve_* 函数。
 */

#define _GNU_SOURCE

#include "supernode_worker.h"
#include "macro.h"
#include "ring_buffer_mgr.h"
#include "sve_compute.h"
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

static int supernode_write_vemb_response(sve_worker_context_t *ctx,
                                         uint64_t request_id,
                                         const float *vector,
                                         size_t dim,
                                         int status) {
    size_t payload_len = sizeof(batch_result_packet_t) + sizeof(float) * dim;
    batch_result_packet_t *resp = NULL;

    RETURN_IF(!ctx || !ctx->output_rb || !vector || dim == 0, C_ERR);
    RETURN_IF(ring_buffer_reserve(ctx->output_rb, payload_len, (void **)&resp) != C_OK, C_ERR);

    resp->magic = BATCH_PACKET_MAGIC;
    resp->packet_size = (uint32_t)payload_len;
    resp->op_type = BATCH_PACKET_OP_VEMB;
    resp->status = (uint32_t)status;
    resp->request_id = request_id;
    resp->dim = (uint32_t)dim;
    resp->reserved = 0;
    memcpy(resp->data, vector, sizeof(float) * dim);

    if (ring_buffer_commit_write(ctx->output_rb, payload_len) != C_OK) {
        ring_buffer_cancel_write(ctx->output_rb);
        return C_ERR;
    }
    return C_OK;
}

static int supernode_write_vsim_response(sve_worker_context_t *ctx,
                                         uint64_t request_id,
                                         const uint64_t *rows,
                                         const float *scores,
                                         size_t result_count,
                                         int status) {
    size_t payload_len = sizeof(batch_vsim_result_packet_t) +
                         sizeof(batch_vsim_result_entry_t) * result_count;
    batch_vsim_result_packet_t *resp = NULL;

    RETURN_IF(!ctx || !ctx->output_rb, C_ERR);
    RETURN_IF(result_count > 0 && (!rows || !scores), C_ERR);
    RETURN_IF(ring_buffer_reserve(ctx->output_rb, payload_len, (void **)&resp) != C_OK, C_ERR);

    resp->magic = BATCH_PACKET_MAGIC;
    resp->packet_size = (uint32_t)payload_len;
    resp->op_type = BATCH_PACKET_OP_VSIM;
    resp->status = (uint32_t)status;
    resp->request_id = request_id;
    resp->num_results = (uint32_t)result_count;
    resp->reserved = 0;

    for (size_t i = 0; i < result_count; i++) {
        resp->results[i].row_id = rows[i];
        resp->results[i].score = scores[i];
    }

    if (ring_buffer_commit_write(ctx->output_rb, payload_len) != C_OK) {
        ring_buffer_cancel_write(ctx->output_rb);
        return C_ERR;
    }
    return C_OK;
}

static float *supernode_alloc_candidate_vectors(size_t row_count, size_t dim) {
    RETURN_IF(row_count == 0 || dim == 0, NULL);
    return zmalloc(sizeof(float) * row_count * dim);
}

static int supernode_load_candidate_vectors(sve_worker_context_t *ctx,
                                            uint64_t *rows,
                                            size_t row_count,
                                            size_t dim,
                                            float **vectors_out) {
    RETURN_IF(!ctx || !rows || row_count == 0 || dim == 0 || !vectors_out, C_ERR);
    *vectors_out = NULL;

    float *vectors = supernode_alloc_candidate_vectors(row_count, dim);
    RETURN_IF(!vectors, C_ERR);

    int ret = sve_serial_contiguous_read(&ctx->gather_ctx, rows, row_count, vectors);
    if (ret != C_OK) {
        zfree(vectors);
        return C_ERR;
    }

    *vectors_out = vectors;
    return C_OK;
}

static int supernode_process_vsim_batch(sve_worker_context_t *ctx,
                                        batch_vsim_packet_t *vsim) {
    RETURN_IF(!ctx || !vsim, C_ERR);
    RETURN_IF(vsim->query_dim == 0 || vsim->candidate_count == 0, C_ERR);

    const float *query = vsim->payload;
    const uint64_t *rows = (const uint64_t *)((const uint8_t *)(vsim->payload + vsim->query_dim));
    float *candidates = NULL;
    float *scores = NULL;
    int ret = C_ERR;

    ret = supernode_load_candidate_vectors(ctx,
                                           (uint64_t *)rows,
                                           vsim->candidate_count,
                                           vsim->query_dim,
                                           &candidates);
    if (ret != C_OK) return C_ERR;

    scores = zmalloc(sizeof(float) * vsim->candidate_count);
    if (!scores) goto cleanup;

    for (size_t i = 0; i < vsim->candidate_count; i++) {
        const float *cand = candidates + i * vsim->query_dim;
        scores[i] = sve_cosine_similarity_f32(query, cand, vsim->query_dim);
    }

    ret = supernode_write_vsim_response(ctx,
                                        vsim->request_id,
                                        rows,
                                        scores,
                                        vsim->candidate_count,
                                        C_OK);

cleanup:
    zfree(candidates);
    zfree(scores);
    return ret;
}

static int supernode_process_vemb_batch(sve_worker_context_t *ctx,
                                        batch_packet_t *packet) {
    RETURN_IF(!ctx || !packet, C_ERR);

    uint64_t *emb_ids = zmalloc(packet->num_requests * sizeof(uint64_t));
    float *results = NULL;
    int ret = C_ERR;

    if (!emb_ids) return C_ERR;
    for (uint32_t i = 0; i < packet->num_requests; i++)
        emb_ids[i] = packet->requests[i].row_id;

    ret = supernode_load_candidate_vectors(ctx,
                                           emb_ids,
                                           packet->num_requests,
                                           ctx->gather_ctx.vector_dim,
                                           &results);
    if (ret != C_OK) goto cleanup;

    for (uint32_t i = 0; i < packet->num_requests; i++) {
        ret = supernode_write_vemb_response(ctx,
                                            packet->requests[i].request_id,
                                            results + ((size_t)i * ctx->gather_ctx.vector_dim),
                                            ctx->gather_ctx.vector_dim,
                                            C_OK);
        if (ret != C_OK) break;
    }

cleanup:
    zfree(results);
    zfree(emb_ids);
    return ret;
}

static int sve_worker_process_batch(sve_worker_context_t *ctx, batch_packet_t *packet) {
    RETURN_IF(!ctx || !packet, C_ERR);

    uint64_t start_time = ustime();
    int ret = C_ERR;

    if (packet->magic != BATCH_PACKET_MAGIC) {
        serverLog(LL_WARNING, "Invalid batch packet magic: 0x%x", packet->magic);
        return C_ERR;
    }

    serverLog(LL_DEBUG, "Worker %d processing batch: %u requests, batch_id=%llu",
              ctx->worker_id, packet->num_requests,
              (unsigned long long)packet->batch_id);

    if (packet->op_type == BATCH_PACKET_OP_VSIM) {
        ret = supernode_process_vsim_batch(ctx, (batch_vsim_packet_t *)packet);
    } else if (packet->op_type == BATCH_PACKET_OP_VEMB) {
        ret = supernode_process_vemb_batch(ctx, packet);
    } else {
        serverLog(LL_WARNING, "Unsupported batch packet op_type: %u", packet->op_type);
        return C_ERR;
    }

    uint64_t latency = ustime() - start_time;
    atomic_fetch_add_explicit(&ctx->total_batches, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&ctx->total_requests, packet->num_requests, memory_order_relaxed);
    atomic_fetch_add_explicit(&ctx->total_latency_us, latency, memory_order_relaxed);

    serverLog(LL_DEBUG, "Worker %d completed batch in %llu μs",
              ctx->worker_id, (unsigned long long)latency);
    return ret;
}

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
        ring_buffer_t *req_rb = ring_buffer_mgr_get_request(node_id, i);
        ring_buffer_t *resp_rb = ring_buffer_mgr_get_response(node_id, i);
        if (!req_rb || !resp_rb) goto failed;
        
        sve_worker_context_t *ctx = &global_supernode->workers[i];
        ctx->worker_id = i;
        ctx->running = 1;
        ctx->input_rb = req_rb;
        ctx->output_rb = resp_rb;

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
