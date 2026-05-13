/*
 * Batch/worker integration benchmark for the SuperNode queue path.
 *
 * Uses the real:
 * - ring_buffer implementation
 * - proxy_batch_bucket packet builder
 * - proxy_flush_executor publish path
 */

#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "test_runtime_shim.h"
#include "../src/proxy_batch_bucket.h"
#include "../src/proxy_flush_executor.h"
#include "../src/ring_buffer.h"

#define NUM_WORKERS 4
#define NUM_BATCHES 50000
#define REQUESTS_PER_BATCH 32
#define TEST_RING_BUFFER_SIZE 4096

typedef struct worker_ctx {
    int worker_id;
    ring_buffer_t *rb;
    atomic_uint_fast64_t consumed_batches;
    atomic_uint_fast64_t consumed_requests;
    int running;
    pthread_t thread;
} worker_ctx_t;

static uint64_t now_ns(void) {
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static ring_buffer_t *test_rb_create(size_t size) {
    ring_buffer_t *rb = calloc(1, sizeof(*rb));

    if (!rb) return NULL;
    rb->buffer = calloc(1, size);
    if (!rb->buffer) {
        free(rb);
        return NULL;
    }
    rb->size = size;
    rb->fd = -1;
    atomic_init(&rb->head, 0);
    atomic_init(&rb->tail, 0);
    atomic_init(&rb->refcount, 1);
    return rb;
}

static void test_rb_destroy(ring_buffer_t *rb) {
    if (!rb) return;
    free(rb->buffer);
    free(rb);
}

static int prepare_batch(proxy_batch_bucket_t *bucket, int worker_id,
                         uint64_t batch_id, size_t nreq) {
    if (!bucket || bucket->count != 0) return C_ERR;

    for (size_t i = 0; i < nreq; i++) {
        proxy_request_t *req = proxy_request_create(
            batch_id * 1000 + i,
            ((uint64_t)worker_id << 32) | (uint64_t)i,
            0,
            worker_id,
            batch_id,
            NULL,
            NULL,
            0);
        if (!req) {
            proxy_batch_bucket_reset(bucket, batch_id);
            return C_ERR;
        }
        if (proxy_batch_bucket_append(bucket, req) != C_OK) {
            proxy_request_destroy(req);
            proxy_batch_bucket_reset(bucket, batch_id);
            return C_ERR;
        }
    }

    return C_OK;
}

static int flush_batch(proxy_flush_executor_t *executor,
                       proxy_batch_bucket_t *bucket,
                       ring_buffer_t *rb,
                       uint64_t flush_time_us) {
    int ret;

    pthread_mutex_lock(&bucket->mutex);
    ret = proxy_executor_flush_bucket_locked(
        executor, bucket, rb, 1, flush_time_us, PROXY_FLUSH_TRIGGER_BACKGROUND);
    pthread_mutex_unlock(&bucket->mutex);
    return ret;
}

static void *worker_main(void *arg) {
    worker_ctx_t *ctx = arg;

    while (ctx->running ||
           atomic_load_explicit(&ctx->rb->head, memory_order_relaxed) !=
               atomic_load_explicit(&ctx->rb->tail, memory_order_acquire)) {
        void *payload = NULL;
        size_t payload_len = 0;

        if (ring_buffer_peek(ctx->rb, &payload, &payload_len) == C_OK) {
            batch_packet_t *pkt = payload;

            if (pkt->magic != BATCH_PACKET_MAGIC) {
                fprintf(stderr, "bad magic on worker %d\n", ctx->worker_id);
                exit(2);
            }
            if ((int)pkt->worker_id != ctx->worker_id) {
                fprintf(stderr, "wrong worker route: got %u expected %d\n",
                        pkt->worker_id, ctx->worker_id);
                exit(3);
            }
            atomic_fetch_add(&ctx->consumed_batches, 1);
            atomic_fetch_add(&ctx->consumed_requests, pkt->num_requests);
            ring_buffer_commit_read(ctx->rb, payload_len);
        }
    }

    return NULL;
}

int main(void) {
    ring_buffer_t *rbs[NUM_WORKERS] = {0};
    proxy_batch_bucket_t buckets[NUM_WORKERS] = {0};
    worker_ctx_t workers[NUM_WORKERS] = {0};
    proxy_flush_executor_t executor;
    uint64_t start, elapsed;
    uint64_t total_batches = 0, total_requests = 0;

    proxy_flush_executor_init(&executor);

    for (int i = 0; i < NUM_WORKERS; i++) {
        rbs[i] = test_rb_create(TEST_RING_BUFFER_SIZE);
        if (!rbs[i]) {
            fprintf(stderr, "rb_create failed\n");
            return 1;
        }
        if (proxy_batch_bucket_init(&buckets[i], REQUESTS_PER_BATCH, 0, i, 0) != C_OK) {
            fprintf(stderr, "bucket init failed\n");
            return 1;
        }
        workers[i].worker_id = i;
        workers[i].rb = rbs[i];
        workers[i].running = 1;
        atomic_init(&workers[i].consumed_batches, 0);
        atomic_init(&workers[i].consumed_requests, 0);
        pthread_create(&workers[i].thread, NULL, worker_main, &workers[i]);
    }

    start = now_ns();
    for (uint64_t batch_id = 0; batch_id < NUM_BATCHES; batch_id++) {
        int worker_id = (int)(batch_id % NUM_WORKERS);

        if (prepare_batch(&buckets[worker_id], worker_id, batch_id, REQUESTS_PER_BATCH) != C_OK) {
            fprintf(stderr, "prepare_batch failed\n");
            return 1;
        }
        while (flush_batch(&executor, &buckets[worker_id], rbs[worker_id], batch_id) != C_OK) {
        }
    }

    for (int i = 0; i < NUM_WORKERS; i++) {
        workers[i].running = 0;
        pthread_join(workers[i].thread, NULL);
        total_batches += atomic_load(&workers[i].consumed_batches);
        total_requests += atomic_load(&workers[i].consumed_requests);
    }
    elapsed = now_ns() - start;

    printf("ring_buffer_batch_bench\n");
    printf("  workers: %d\n", NUM_WORKERS);
    printf("  batches: %" PRIu64 "\n", (uint64_t)NUM_BATCHES);
    printf("  requests per batch: %d\n", REQUESTS_PER_BATCH);
    printf("  packet bytes: %zu\n",
           sizeof(batch_packet_t) +
               REQUESTS_PER_BATCH * sizeof(((batch_packet_t *)0)->requests[0]));
    printf("  consumed batches: %" PRIu64 "\n", total_batches);
    printf("  consumed requests: %" PRIu64 "\n", total_requests);
    printf("  total time: %.3f ms\n", (double)elapsed / 1e6);
    printf("  batch throughput: %.2f M batch/s\n",
           (double)total_batches / ((double)elapsed / 1e9) / 1e6);
    printf("  request throughput: %.2f M req/s\n",
           (double)total_requests / ((double)elapsed / 1e9) / 1e6);

    if (total_batches != NUM_BATCHES ||
        total_requests != (uint64_t)NUM_BATCHES * REQUESTS_PER_BATCH) {
        fprintf(stderr, "count mismatch\n");
        return 1;
    }

    for (int i = 0; i < NUM_WORKERS; i++) {
        proxy_batch_bucket_cleanup(&buckets[i]);
        test_rb_destroy(rbs[i]);
    }
    return 0;
}
