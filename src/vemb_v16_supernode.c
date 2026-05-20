#define _GNU_SOURCE

#include "vemb_v16_supernode.h"
#include "vemb_v16_dataplane.h"
#include "vemb_v16_log.h"
#include "vemb_v16_protocol.h"

#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <time.h>

#define VEMB_V16_SAMPLE_MASK 1023u

static uint64_t monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static inline void vemb_v16_supernode_relax(void) {
#if defined(__x86_64__)
    __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__)
    __asm__ volatile("yield" ::: "memory");
#else
    __asm__ volatile("" ::: "memory");
#endif
}

void *vemb_v16_supernode_thread_main(void *arg) {
    vemb_v16_supernode_ctx_t *ctx = arg;
    vemb_v16_vemb_job_t vemb_job;
    vemb_v16_vadd_job_t vadd_job;

#ifdef __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET((int)((ctx->worker_id * 2 + 2) % 64), &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
#endif

    serverLog(LL_VERBOSE, "vemb_v16 supernode worker started: worker_id=%u",
              ctx->worker_id);
    while (atomic_load_explicit(ctx->running, memory_order_relaxed) &&
           atomic_load_explicit(ctx->channel_active, memory_order_acquire)) {
        if (vemb_v16_aeron_poll(ctx->vemb_job_ring, &vemb_job)) {
            vemb_v16_job_base_t *job = &vemb_job.base;
            atomic_fetch_add_explicit(ctx->supernode_vemb_poll, 1,
                                      memory_order_relaxed);
            vemb_v16_completion_t completion = {
                .status = VEMB_V16_STATUS_OK,
                .op = job->op,
                .flags = job->flags,
                .req_id = job->req_id,
                .channel_index = job->channel_index,
                .channel_id = job->channel_id,
                .key_hash = job->key_hash,
                .dim = job->dim,
                .vector_bytes = job->vector_bytes,
            };
            uint32_t row_id = 0;
            int sample = ((job->req_id & VEMB_V16_SAMPLE_MASK) == 0);
            uint64_t lookup_start = sample ? monotonic_ns() : 0;
            if (vemb_v16_table_lookup(ctx->table, job->key, job->key_len,
                                      job->key_hash, &row_id) != 0) {
                completion.status = VEMB_V16_STATUS_NOT_FOUND;
                atomic_fetch_add_explicit(ctx->not_found, 1, memory_order_relaxed);
            } else {
                if (job->dim != vemb_v16_table_dim(ctx->table) ||
                    job->vector_bytes != vemb_v16_table_stride(ctx->table)) {
                    completion.status = VEMB_V16_STATUS_ERR;
                } else {
                    completion.vector_offset =
                        (uint64_t)row_id * vemb_v16_table_stride(ctx->table);
                    completion.vector_bytes = vemb_v16_table_stride(ctx->table);
                }
            }
            if (sample) {
                atomic_fetch_add_explicit(ctx->sample_count, 1,
                                          memory_order_relaxed);
                atomic_fetch_add_explicit(ctx->sample_table_lookup_ns,
                                          monotonic_ns() - lookup_start,
                                          memory_order_relaxed);
            }
            atomic_fetch_add_explicit(ctx->vemb_requests, 1, memory_order_relaxed);
            uint64_t completion_start = sample ? monotonic_ns() : 0;
            while (vemb_v16_aeron_publish(ctx->completion_ring, &completion) != 0 &&
                   atomic_load_explicit(ctx->running, memory_order_relaxed) &&
                   atomic_load_explicit(ctx->channel_active, memory_order_acquire)) {
                atomic_fetch_add_explicit(ctx->supernode_completion_ring_full, 1,
                                          memory_order_relaxed);
                vemb_v16_supernode_relax();
            }
            if (sample) {
                atomic_fetch_add_explicit(ctx->sample_completion_publish_ns,
                                          monotonic_ns() - completion_start,
                                          memory_order_relaxed);
            }
            atomic_fetch_add_explicit(ctx->supernode_completion_publish, 1,
                                      memory_order_relaxed);
            atomic_fetch_add_explicit(ctx->completed_jobs, 1, memory_order_relaxed);
            continue;
        }

        if (!vemb_v16_aeron_poll(ctx->vadd_job_ring, &vadd_job)) {
            vemb_v16_supernode_relax();
            continue;
        }
        atomic_fetch_add_explicit(ctx->supernode_vadd_poll, 1,
                                  memory_order_relaxed);
        vemb_v16_job_base_t *job = &vadd_job.base;

        vemb_v16_completion_t completion = {
            .status = VEMB_V16_STATUS_OK,
            .op = job->op,
            .flags = job->flags,
            .req_id = job->req_id,
            .channel_index = job->channel_index,
            .channel_id = job->channel_id,
            .key_hash = job->key_hash,
            .dim = job->dim,
            .vector_bytes = job->vector_bytes,
        };

        if (job->op == VEMB_V16_OP_VADD_INLINE) {
            uint32_t row_id = 0;
            if (job->dim != vemb_v16_table_dim(ctx->table) ||
                job->vector_bytes != vemb_v16_table_stride(ctx->table) ||
                vemb_v16_table_upsert(ctx->table, job->key, job->key_len,
                                      job->key_hash, vadd_job.vector,
                                      job->vector_bytes, &row_id) != 0) {
                completion.status = VEMB_V16_STATUS_ERR;
            } else {
                completion.vector_offset =
                    (uint64_t)row_id * vemb_v16_table_stride(ctx->table);
            }
            atomic_fetch_add_explicit(ctx->vadd_requests, 1, memory_order_relaxed);
        } else {
            completion.status = VEMB_V16_STATUS_ERR;
        }

        while (vemb_v16_aeron_publish(ctx->completion_ring, &completion) != 0 &&
               atomic_load_explicit(ctx->running, memory_order_relaxed) &&
               atomic_load_explicit(ctx->channel_active, memory_order_acquire)) {
            atomic_fetch_add_explicit(ctx->supernode_completion_ring_full, 1,
                                      memory_order_relaxed);
            vemb_v16_supernode_relax();
        }
        atomic_fetch_add_explicit(ctx->supernode_completion_publish, 1,
                                  memory_order_relaxed);
        atomic_fetch_add_explicit(ctx->completed_jobs, 1, memory_order_relaxed);
    }
    serverLog(LL_VERBOSE, "vemb_v16 supernode worker stopped: worker_id=%u",
              ctx->worker_id);
    return NULL;
}
