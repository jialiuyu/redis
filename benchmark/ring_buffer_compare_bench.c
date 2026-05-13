/*
 * Compare the legacy shared-queue design against the refactored
 * per-worker queue + zero-copy design.
 *
 * The refactored path uses the real:
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
#define SHARED_RING_BUFFER_SIZE (1 << 20)
#define WORKER_RING_BUFFER_SIZE 4096

typedef struct {
    uint32_t magic;
    uint32_t packet_size;
    uint32_t num_requests;
    uint32_t supernode_id;
    uint64_t timestamp_us;
    uint64_t batch_id;
    struct {
        uint64_t request_id;
        uint64_t key_hash;
        uint32_t key_len;
        char key_data[256];
    } requests[];
} __attribute__((packed)) legacy_batch_packet_t;

typedef struct {
    atomic_uint_fast64_t head;
    atomic_uint_fast64_t tail;
    uint8_t *buffer;
    size_t size;
    pthread_mutex_t write_mutex;
    pthread_mutex_t read_mutex;
} legacy_ring_buffer_t;

typedef struct {
    int worker_id;
    legacy_ring_buffer_t *legacy_rb;
    ring_buffer_t *refactored_rb;
    atomic_uint_fast64_t consumed_batches;
    atomic_uint_fast64_t consumed_requests;
    int running;
    pthread_t thread;
    int mode; /* 0=legacy, 1=refactored */
} worker_ctx_t;

static uint64_t now_ns(void) {
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static size_t legacy_packet_size_for(size_t nreq) {
    return sizeof(legacy_batch_packet_t) +
           nreq * sizeof(((legacy_batch_packet_t *)0)->requests[0]);
}

static legacy_ring_buffer_t *legacy_rb_create(size_t size) {
    legacy_ring_buffer_t *rb = calloc(1, sizeof(*rb));

    if (!rb) return NULL;
    rb->buffer = calloc(1, size);
    if (!rb->buffer) {
        free(rb);
        return NULL;
    }
    rb->size = size;
    atomic_init(&rb->head, 0);
    atomic_init(&rb->tail, 0);
    pthread_mutex_init(&rb->write_mutex, NULL);
    pthread_mutex_init(&rb->read_mutex, NULL);
    return rb;
}

static void legacy_rb_destroy(legacy_ring_buffer_t *rb) {
    if (!rb) return;
    pthread_mutex_destroy(&rb->write_mutex);
    pthread_mutex_destroy(&rb->read_mutex);
    free(rb->buffer);
    free(rb);
}

static size_t legacy_available_space(legacy_ring_buffer_t *rb) {
    uint64_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    uint64_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);

    if (tail >= head) return rb->size - (tail - head) - 1;
    return head - tail - 1;
}

static size_t legacy_available_data(legacy_ring_buffer_t *rb) {
    uint64_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    uint64_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);

    if (tail >= head) return tail - head;
    return rb->size - (head - tail);
}

static int legacy_rb_push(legacy_ring_buffer_t *rb, const void *data, size_t len) {
    uint64_t tail;
    size_t pos, remaining;
    uint32_t packet_len;

    if (!rb || !data || len == 0) return C_ERR;
    pthread_mutex_lock(&rb->write_mutex);
    if (legacy_available_space(rb) < len + sizeof(uint32_t)) {
        pthread_mutex_unlock(&rb->write_mutex);
        return C_ERR;
    }

    tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
    pos = tail % rb->size;
    packet_len = (uint32_t)len;
    remaining = rb->size - pos;
    if (remaining >= sizeof(uint32_t)) {
        memcpy(rb->buffer + pos, &packet_len, sizeof(uint32_t));
        pos = (pos + sizeof(uint32_t)) % rb->size;
    } else {
        memcpy(rb->buffer + pos, &packet_len, remaining);
        memcpy(rb->buffer,
               ((const uint8_t *)&packet_len) + remaining,
               sizeof(uint32_t) - remaining);
        pos = sizeof(uint32_t) - remaining;
    }

    remaining = rb->size - pos;
    if (remaining >= len) {
        memcpy(rb->buffer + pos, data, len);
    } else {
        memcpy(rb->buffer + pos, data, remaining);
        memcpy(rb->buffer, ((const uint8_t *)data) + remaining, len - remaining);
    }

    atomic_store_explicit(&rb->tail, tail + sizeof(uint32_t) + len, memory_order_release);
    pthread_mutex_unlock(&rb->write_mutex);
    return C_OK;
}

static int legacy_rb_pop(legacy_ring_buffer_t *rb, void *data, size_t max_len, size_t *actual_len) {
    uint64_t head;
    size_t pos, remaining;
    uint32_t packet_len;

    if (!rb || !data) return C_ERR;
    pthread_mutex_lock(&rb->read_mutex);
    if (legacy_available_data(rb) < sizeof(uint32_t)) {
        pthread_mutex_unlock(&rb->read_mutex);
        return C_ERR;
    }

    head = atomic_load_explicit(&rb->head, memory_order_acquire);
    pos = head % rb->size;
    remaining = rb->size - pos;
    if (remaining >= sizeof(uint32_t)) {
        memcpy(&packet_len, rb->buffer + pos, sizeof(uint32_t));
        pos = (pos + sizeof(uint32_t)) % rb->size;
    } else {
        memcpy(&packet_len, rb->buffer + pos, remaining);
        memcpy(((uint8_t *)&packet_len) + remaining, rb->buffer, sizeof(uint32_t) - remaining);
        pos = sizeof(uint32_t) - remaining;
    }

    if (packet_len > max_len ||
        legacy_available_data(rb) < sizeof(uint32_t) + packet_len) {
        pthread_mutex_unlock(&rb->read_mutex);
        return C_ERR;
    }

    remaining = rb->size - pos;
    if (remaining >= packet_len) {
        memcpy(data, rb->buffer + pos, packet_len);
    } else {
        memcpy(data, rb->buffer + pos, remaining);
        memcpy(((uint8_t *)data) + remaining, rb->buffer, packet_len - remaining);
    }

    atomic_store_explicit(&rb->head, head + sizeof(uint32_t) + packet_len, memory_order_release);
    pthread_mutex_unlock(&rb->read_mutex);
    if (actual_len) *actual_len = packet_len;
    return C_OK;
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

static int prepare_refactored_batch(proxy_batch_bucket_t *bucket, int worker_id,
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

static int flush_refactored_batch(proxy_flush_executor_t *executor,
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

static void fill_legacy_packet(legacy_batch_packet_t *pkt, uint64_t batch_id, size_t nreq) {
    static const char *key = "user:123456789";

    pkt->magic = BATCH_PACKET_MAGIC;
    pkt->packet_size = (uint32_t)legacy_packet_size_for(nreq);
    pkt->num_requests = (uint32_t)nreq;
    pkt->supernode_id = 0;
    pkt->timestamp_us = batch_id;
    pkt->batch_id = batch_id;
    for (size_t i = 0; i < nreq; i++) {
        pkt->requests[i].request_id = batch_id * 1000 + i;
        pkt->requests[i].key_hash = batch_id + i;
        pkt->requests[i].key_len = (uint32_t)strlen(key);
        strncpy(pkt->requests[i].key_data, key, sizeof(pkt->requests[i].key_data) - 1);
        pkt->requests[i].key_data[sizeof(pkt->requests[i].key_data) - 1] = '\0';
    }
}

static void *worker_main(void *arg) {
    worker_ctx_t *ctx = arg;

    while (ctx->running ||
           (ctx->mode == 0
                ? atomic_load_explicit(&ctx->legacy_rb->head, memory_order_relaxed) !=
                      atomic_load_explicit(&ctx->legacy_rb->tail, memory_order_acquire)
                : atomic_load_explicit(&ctx->refactored_rb->head, memory_order_relaxed) !=
                      atomic_load_explicit(&ctx->refactored_rb->tail, memory_order_acquire))) {
        if (ctx->mode == 0) {
            uint8_t buffer[16384];
            size_t actual_len = 0;

            if (legacy_rb_pop(ctx->legacy_rb, buffer, sizeof(buffer), &actual_len) == C_OK) {
                legacy_batch_packet_t *pkt = (legacy_batch_packet_t *)buffer;

                if (pkt->magic != BATCH_PACKET_MAGIC) {
                    fprintf(stderr, "legacy bad magic\n");
                    exit(2);
                }
                atomic_fetch_add(&ctx->consumed_batches, 1);
                atomic_fetch_add(&ctx->consumed_requests, pkt->num_requests);
            }
        } else {
            void *payload = NULL;
            size_t payload_len = 0;

            if (ring_buffer_peek(ctx->refactored_rb, &payload, &payload_len) == C_OK) {
                batch_packet_t *pkt = payload;

                if (pkt->magic != BATCH_PACKET_MAGIC || (int)pkt->worker_id != ctx->worker_id) {
                    fprintf(stderr, "refactored route/magic failure\n");
                    exit(3);
                }
                atomic_fetch_add(&ctx->consumed_batches, 1);
                atomic_fetch_add(&ctx->consumed_requests, pkt->num_requests);
                ring_buffer_commit_read(ctx->refactored_rb, payload_len);
            }
        }
    }

    return NULL;
}

static void run_legacy(void) {
    legacy_ring_buffer_t *rb = legacy_rb_create(SHARED_RING_BUFFER_SIZE);
    worker_ctx_t workers[NUM_WORKERS] = {0};
    uint64_t total_batches = 0, total_requests = 0, start, elapsed;

    if (!rb) {
        fprintf(stderr, "legacy rb_create failed\n");
        exit(1);
    }
    for (int i = 0; i < NUM_WORKERS; i++) {
        workers[i].worker_id = i;
        workers[i].legacy_rb = rb;
        workers[i].mode = 0;
        workers[i].running = 1;
        atomic_init(&workers[i].consumed_batches, 0);
        atomic_init(&workers[i].consumed_requests, 0);
        pthread_create(&workers[i].thread, NULL, worker_main, &workers[i]);
    }

    start = now_ns();
    for (uint64_t batch_id = 0; batch_id < NUM_BATCHES; batch_id++) {
        size_t pkt_size = legacy_packet_size_for(REQUESTS_PER_BATCH);
        legacy_batch_packet_t *pkt = malloc(pkt_size);

        if (!pkt) exit(1);
        fill_legacy_packet(pkt, batch_id, REQUESTS_PER_BATCH);
        while (legacy_rb_push(rb, pkt, pkt_size) != C_OK) {
        }
        free(pkt);
    }

    for (int i = 0; i < NUM_WORKERS; i++) workers[i].running = 0;
    for (int i = 0; i < NUM_WORKERS; i++) {
        pthread_join(workers[i].thread, NULL);
        total_batches += atomic_load(&workers[i].consumed_batches);
        total_requests += atomic_load(&workers[i].consumed_requests);
    }
    elapsed = now_ns() - start;

    printf("legacy_shared_queue\n");
    printf("  workers: %d\n", NUM_WORKERS);
    printf("  packet bytes: %zu\n", legacy_packet_size_for(REQUESTS_PER_BATCH));
    printf("  batches: %" PRIu64 "\n", (uint64_t)NUM_BATCHES);
    printf("  requests per batch: %d\n", REQUESTS_PER_BATCH);
    printf("  consumed batches: %" PRIu64 "\n", total_batches);
    printf("  consumed requests: %" PRIu64 "\n", total_requests);
    printf("  total time: %.3f ms\n", (double)elapsed / 1e6);
    printf("  batch throughput: %.2f M batch/s\n",
           (double)total_batches / ((double)elapsed / 1e9) / 1e6);
    printf("  request throughput: %.2f M req/s\n\n",
           (double)total_requests / ((double)elapsed / 1e9) / 1e6);

    if (total_batches != NUM_BATCHES ||
        total_requests != (uint64_t)NUM_BATCHES * REQUESTS_PER_BATCH) {
        fprintf(stderr, "legacy count mismatch\n");
        exit(4);
    }
    legacy_rb_destroy(rb);
}

static void run_refactored(void) {
    ring_buffer_t *rbs[NUM_WORKERS] = {0};
    proxy_batch_bucket_t buckets[NUM_WORKERS] = {0};
    worker_ctx_t workers[NUM_WORKERS] = {0};
    proxy_flush_executor_t executor;
    uint64_t total_batches = 0, total_requests = 0, start, elapsed;

    proxy_flush_executor_init(&executor);

    for (int i = 0; i < NUM_WORKERS; i++) {
        rbs[i] = test_rb_create(WORKER_RING_BUFFER_SIZE);
        if (!rbs[i]) {
            fprintf(stderr, "refactored rb_create failed\n");
            exit(1);
        }
        if (proxy_batch_bucket_init(&buckets[i], REQUESTS_PER_BATCH, 0, i, 0) != C_OK) {
            fprintf(stderr, "refactored bucket init failed\n");
            exit(1);
        }
        workers[i].worker_id = i;
        workers[i].refactored_rb = rbs[i];
        workers[i].mode = 1;
        workers[i].running = 1;
        atomic_init(&workers[i].consumed_batches, 0);
        atomic_init(&workers[i].consumed_requests, 0);
        pthread_create(&workers[i].thread, NULL, worker_main, &workers[i]);
    }

    start = now_ns();
    for (uint64_t batch_id = 0; batch_id < NUM_BATCHES; batch_id++) {
        int worker_id = (int)(batch_id % NUM_WORKERS);

        if (prepare_refactored_batch(&buckets[worker_id], worker_id, batch_id,
                                     REQUESTS_PER_BATCH) != C_OK) {
            fprintf(stderr, "refactored prepare failed\n");
            exit(1);
        }
        while (flush_refactored_batch(&executor, &buckets[worker_id], rbs[worker_id], batch_id) !=
               C_OK) {
        }
    }

    for (int i = 0; i < NUM_WORKERS; i++) workers[i].running = 0;
    for (int i = 0; i < NUM_WORKERS; i++) {
        pthread_join(workers[i].thread, NULL);
        total_batches += atomic_load(&workers[i].consumed_batches);
        total_requests += atomic_load(&workers[i].consumed_requests);
    }
    elapsed = now_ns() - start;

    printf("refactored_per_worker_queue\n");
    printf("  workers: %d\n", NUM_WORKERS);
    printf("  packet bytes: %zu\n",
           sizeof(batch_packet_t) +
               REQUESTS_PER_BATCH * sizeof(((batch_packet_t *)0)->requests[0]));
    printf("  batches: %" PRIu64 "\n", (uint64_t)NUM_BATCHES);
    printf("  requests per batch: %d\n", REQUESTS_PER_BATCH);
    printf("  consumed batches: %" PRIu64 "\n", total_batches);
    printf("  consumed requests: %" PRIu64 "\n", total_requests);
    printf("  total time: %.3f ms\n", (double)elapsed / 1e6);
    printf("  batch throughput: %.2f M batch/s\n",
           (double)total_batches / ((double)elapsed / 1e9) / 1e6);
    printf("  request throughput: %.2f M req/s\n\n",
           (double)total_requests / ((double)elapsed / 1e9) / 1e6);

    if (total_batches != NUM_BATCHES ||
        total_requests != (uint64_t)NUM_BATCHES * REQUESTS_PER_BATCH) {
        fprintf(stderr, "refactored count mismatch\n");
        exit(5);
    }
    for (int i = 0; i < NUM_WORKERS; i++) {
        proxy_batch_bucket_cleanup(&buckets[i]);
        test_rb_destroy(rbs[i]);
    }
}

int main(void) {
    run_legacy();
    run_refactored();
    return 0;
}
