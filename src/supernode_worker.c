/*
 * SuperNode Worker Implementation
 *
 * 只负责：UB.mem mmap 管理、Worker 线程调度、SuperNode 生命周期。
 * SVE 计算和 bitmap 操作全部直接调用 sve_operation 模块的 sve_* 函数。
 */

#include "supernode_worker.h"
#include "server.h"
#include <sys/mman.h>
#include <sys/time.h>
#include <fcntl.h>
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

    sve_ub_mem_t *ub_mem;
    state_bitmap_t *global_bitmap;
    ring_buffer_t *input_rb;

    int running;
} supernode_t;

supernode_t *global_supernode = NULL;

/* ========== UB.mem 管理 ========== */

sve_ub_mem_t *ub_mem_init(uint64_t physical_base, size_t size) {
    sve_ub_mem_t *ub = zmalloc(sizeof(sve_ub_mem_t));
    if (!ub) return NULL;

    ub->physical_base = physical_base;
    ub->size = size;
    ub->token_id = 0x12345678;
    ub->numa_node = 0;

    ub->base_addr = mmap(NULL, size, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (ub->base_addr == MAP_FAILED) {
        serverLog(LL_WARNING, "Failed to mmap UB.mem (hugetlb): %s", strerror(errno));
        ub->base_addr = mmap(NULL, size, PROT_READ | PROT_WRITE,
                             MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (ub->base_addr == MAP_FAILED) {
            zfree(ub);
            return NULL;
        }
    }

    serverLog(LL_NOTICE, "UB.mem initialized: base=0x%llx, size=%zu GB",
              (unsigned long long)physical_base, size / (1024 * 1024 * 1024));
    return ub;
}

void ub_mem_cleanup(sve_ub_mem_t *ub) {
    if (!ub) return;
    if (ub->base_addr && ub->base_addr != MAP_FAILED)
        munmap(ub->base_addr, ub->size);
    zfree(ub);
}

/* ========== Worker 线程 ========== */

int sve_worker_process_batch(sve_worker_context_t *ctx, batch_packet_t *packet) {
    if (!ctx || !packet) return C_ERR;

    uint64_t start_time = get_time_us();

    if (packet->magic != 0xCAC0BEEF) {
        serverLog(LL_WARNING, "Invalid batch packet magic: 0x%x", packet->magic);
        return C_ERR;
    }

    serverLog(LL_DEBUG, "Worker %d processing batch: %u requests, batch_id=%llu",
              ctx->worker_id, packet->num_requests,
              (unsigned long long)packet->batch_id);

    uint64_t *emb_ids = zmalloc(packet->num_requests * sizeof(uint64_t));
    if (!emb_ids) return C_ERR;

    for (uint32_t i = 0; i < packet->num_requests; i++)
        emb_ids[i] = packet->requests[i].key_hash % SUPERNODE_MAX_EMBEDDINGS;

    float *results = zmalloc(packet->num_requests * SUPERNODE_EMBEDDING_DIM * sizeof(float));
    if (!results) { zfree(emb_ids); return C_ERR; }

    /*
     * Use contiguous load: each embedding is a contiguous row in memory,
     * so per-embedding sequential load is optimal. SVE gather load would
     * only help if we needed partial dimensions or column-oriented access.
     */
    int ret = sve_serial_contiguous_read(ctx->ub_mem, ctx->bitmap, emb_ids,
                                         packet->num_requests, results,
                                         &ctx->op_stats);

    zfree(results);
    zfree(emb_ids);

    uint64_t latency = get_time_us() - start_time;
    atomic_fetch_add(&ctx->total_batches, 1);
    atomic_fetch_add(&ctx->total_requests, packet->num_requests);
    atomic_fetch_add(&ctx->total_latency_us, latency);

    serverLog(LL_DEBUG, "Worker %d completed batch in %llu μs",
              ctx->worker_id, (unsigned long long)latency);
    return ret;
}

void *sve_worker_thread(void *arg) {
    sve_worker_context_t *ctx = (sve_worker_context_t *)arg;

    serverLog(LL_NOTICE, "SVE Worker %d started", ctx->worker_id);

#ifdef __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(ctx->worker_id, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
#endif

    while (ctx->running) {
        uint8_t buffer[RING_BUFFER_BATCH_SIZE];
        size_t actual_len = 0;

        int ret = ring_buffer_pop(ctx->input_rb, buffer, sizeof(buffer), &actual_len);
        if (ret == C_OK && actual_len > 0) {
            sve_worker_process_batch(ctx, (batch_packet_t *)buffer);
        } else {
            usleep(10);
        }
    }

    serverLog(LL_NOTICE, "SVE Worker %d stopped", ctx->worker_id);
    return NULL;
}

/* ========== SuperNode 生命周期 ========== */

int supernode_init(int node_id, int num_workers) {
    if (global_supernode) return C_OK;

    if (num_workers <= 0 || num_workers > SUPERNODE_MAX_WORKERS) {
        serverLog(LL_WARNING, "Invalid number of workers: %d", num_workers);
        return C_ERR;
    }

    global_supernode = zcalloc(sizeof(supernode_t));
    if (!global_supernode) return C_ERR;

    global_supernode->node_id = node_id;
    global_supernode->num_workers = num_workers;

    /* UB.mem */
    global_supernode->ub_mem = ub_mem_init(UB_MEM_BASE_ADDR, UB_MEM_SIZE);
    if (!global_supernode->ub_mem) { supernode_shutdown(); return C_ERR; }

    /* Bitmap — 直接调用 bitmap_init */
    global_supernode->global_bitmap = zmalloc(sizeof(state_bitmap_t));
    if (!global_supernode->global_bitmap ||
        bitmap_init(global_supernode->global_bitmap, SUPERNODE_MAX_EMBEDDINGS) != 0) {
        supernode_shutdown();
        return C_ERR;
    }

    /* Ring Buffer */
    char rb_name[64];
    snprintf(rb_name, sizeof(rb_name), "supernode_%d_input", node_id);
    global_supernode->input_rb = ring_buffer_create(RING_BUFFER_SIZE, rb_name);
    if (!global_supernode->input_rb) { supernode_shutdown(); return C_ERR; }

    /* Workers */
    global_supernode->workers = zcalloc(sizeof(sve_worker_context_t) * num_workers);

    for (int i = 0; i < num_workers; i++) {
        sve_worker_context_t *ctx = &global_supernode->workers[i];
        ctx->worker_id = i;
        ctx->running = 1;
        ctx->input_rb = global_supernode->input_rb;
        ctx->ub_mem = global_supernode->ub_mem;
        ctx->bitmap = global_supernode->global_bitmap;
        ctx->sve_vl = SVE_OP_VECTOR_BITS / 8;

        atomic_init(&ctx->total_batches, 0);
        atomic_init(&ctx->total_requests, 0);
        atomic_init(&ctx->sve_operations, 0);
        atomic_init(&ctx->total_latency_us, 0);
        atomic_init(&ctx->op_stats.lock_success, 0);
        atomic_init(&ctx->op_stats.lock_failure, 0);

        if (pthread_create(&ctx->thread, NULL, sve_worker_thread, ctx) != 0) {
            serverLog(LL_WARNING, "Failed to create SVE worker %d", i);
            supernode_shutdown();
            return C_ERR;
        }
    }

    global_supernode->running = 1;
    serverLog(LL_NOTICE, "SuperNode %d initialized with %d workers", node_id, num_workers);
    return C_OK;
}

void supernode_shutdown(void) {
    if (!global_supernode) return;

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

    if (global_supernode->input_rb)
        ring_buffer_destroy(global_supernode->input_rb);

    if (global_supernode->global_bitmap) {
        bitmap_destroy(global_supernode->global_bitmap);
        zfree(global_supernode->global_bitmap);
    }

    if (global_supernode->ub_mem)
        ub_mem_cleanup(global_supernode->ub_mem);

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
    stats = sdscatprintf(stats, "  UB.mem size: %zu GB\n",
                         global_supernode->ub_mem->size / (1024 * 1024 * 1024));

    uint64_t tb = 0, tr = 0, ts = 0, tt = 0;
    uint64_t bls = 0, blf = 0;
    uint64_t go = 0, so = 0, ge = 0, se = 0;

    for (int i = 0; i < global_supernode->num_workers; i++) {
        sve_worker_context_t *c = &global_supernode->workers[i];
        tb += atomic_load(&c->total_batches);
        tr += atomic_load(&c->total_requests);
        ts += atomic_load(&c->sve_operations);
        tt += atomic_load(&c->total_latency_us);
        bls += atomic_load(&c->op_stats.lock_success);
        blf += atomic_load(&c->op_stats.lock_failure);
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
                         (unsigned long long)atomic_load(&ctx->total_batches),
                         (unsigned long long)atomic_load(&ctx->total_requests));
    stats = sdscatprintf(stats, "  Bitmap lock success: %llu\n",
                         (unsigned long long)atomic_load(&ctx->op_stats.lock_success));
    stats = sdscatprintf(stats, "  Bitmap lock failure: %llu\n",
                         (unsigned long long)atomic_load(&ctx->op_stats.lock_failure));
    return stats;
}
