/*
 * Standalone batch/worker integration benchmark for the SuperNode queue path.
 *
 * Simulates:
 * - per-worker queue routing
 * - in-place batch packet write
 * - zero-copy worker consume
 * - padding-record wraparound under repeated batch traffic
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdatomic.h>
#include <string.h>
#include <pthread.h>
#include <time.h>

#define C_OK 0
#define C_ERR -1

#define NUM_WORKERS 4
#define NUM_BATCHES 50000
#define REQUESTS_PER_BATCH 32
#define RING_BUFFER_SIZE 4096

typedef struct ring_buffer {
    _Alignas(64) atomic_uint_fast64_t head;
    char head_pad[64 - sizeof(atomic_uint_fast64_t)];
    _Alignas(64) atomic_uint_fast64_t tail;
    char tail_pad[64 - sizeof(atomic_uint_fast64_t)];
    uint8_t *buffer;
    size_t size;
    uint64_t reserved_commit_tail;
    size_t reserved_payload_len;
    int reservation_active;
} ring_buffer_t;

typedef struct batch_packet {
    uint32_t magic;
    uint32_t packet_size;
    uint32_t num_requests;
    uint32_t supernode_id;
    uint32_t worker_id;
    uint64_t timestamp_us;
    uint64_t batch_id;
    struct {
        uint64_t request_id;
        uint64_t key_hash;
    } requests[];
} __attribute__((packed)) batch_packet_t;

typedef struct worker_ctx {
    int worker_id;
    ring_buffer_t *rb;
    atomic_uint_fast64_t consumed_batches;
    atomic_uint_fast64_t consumed_requests;
    int running;
    pthread_t thread;
} worker_ctx_t;

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static ring_buffer_t *rb_create(size_t size) {
    ring_buffer_t *rb = calloc(1, sizeof(*rb));
    if (!rb) return NULL;
    rb->buffer = calloc(1, size);
    if (!rb->buffer) {
        free(rb);
        return NULL;
    }
    rb->size = size;
    atomic_init(&rb->head, 0);
    atomic_init(&rb->tail, 0);
    return rb;
}

static void rb_destroy(ring_buffer_t *rb) {
    if (!rb) return;
    free(rb->buffer);
    free(rb);
}

static int rb_reserve(ring_buffer_t *rb, size_t payload_len, void **payload) {
    if (!rb || !payload || payload_len == 0) return C_ERR;
    if (rb->reservation_active) return C_ERR;

    uint64_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    uint64_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    size_t used = (tail >= head) ? (size_t)(tail - head) : (rb->size - (size_t)(head - tail));
    size_t total_len = sizeof(uint32_t) + payload_len;
    if (rb->size - used - 1 < total_len) return C_ERR;

    size_t pos = tail % rb->size;
    size_t contiguous = rb->size - pos;
    if (contiguous < total_len) {
        if (contiguous < sizeof(uint32_t)) return C_ERR;
        uint32_t padding_len = 0;
        memcpy(rb->buffer + pos, &padding_len, sizeof(uint32_t));
        tail += contiguous;
        pos = 0;
        contiguous = rb->size;

        head = atomic_load_explicit(&rb->head, memory_order_acquire);
        used = (tail >= head) ? (size_t)(tail - head) : (rb->size - (size_t)(head - tail));
        if (rb->size - used - 1 < total_len || contiguous < total_len) return C_ERR;
    }

    uint32_t packet_len = (uint32_t)payload_len;
    memcpy(rb->buffer + pos, &packet_len, sizeof(uint32_t));
    *payload = rb->buffer + pos + sizeof(uint32_t);
    rb->reserved_commit_tail = tail + total_len;
    rb->reserved_payload_len = payload_len;
    rb->reservation_active = 1;
    return C_OK;
}

static int rb_commit_write(ring_buffer_t *rb, size_t payload_len) {
    if (!rb || payload_len == 0) return C_ERR;
    if (!rb->reservation_active || rb->reserved_payload_len != payload_len) return C_ERR;
    atomic_store_explicit(&rb->tail, rb->reserved_commit_tail, memory_order_release);
    rb->reservation_active = 0;
    rb->reserved_payload_len = 0;
    rb->reserved_commit_tail = 0;
    return C_OK;
}

static int rb_peek(ring_buffer_t *rb, void **payload, size_t *payload_len) {
    if (!rb || !payload || !payload_len) return C_ERR;

    while (1) {
        uint64_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
        uint64_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
        size_t available = (tail >= head) ? (size_t)(tail - head) : (rb->size - (size_t)(head - tail));
        if (available < sizeof(uint32_t)) return C_ERR;

        size_t pos = head % rb->size;
        uint32_t packet_len;
        size_t remaining = rb->size - pos;
        if (remaining >= sizeof(uint32_t)) {
            memcpy(&packet_len, rb->buffer + pos, sizeof(uint32_t));
            pos += sizeof(uint32_t);
        } else {
            memcpy(&packet_len, rb->buffer + pos, remaining);
            memcpy(((uint8_t *)&packet_len) + remaining, rb->buffer, sizeof(uint32_t) - remaining);
            pos = sizeof(uint32_t) - remaining;
        }

        if (packet_len == 0) {
            atomic_store_explicit(&rb->head, head + remaining, memory_order_release);
            continue;
        }
        if (available < sizeof(uint32_t) + packet_len) return C_ERR;
        if (rb->size - pos < packet_len) return C_ERR;

        *payload = rb->buffer + pos;
        *payload_len = packet_len;
        return C_OK;
    }
}

static int rb_commit_read(ring_buffer_t *rb, size_t payload_len) {
    if (!rb || payload_len == 0) return C_ERR;
    uint64_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    atomic_store_explicit(&rb->head, head + sizeof(uint32_t) + payload_len, memory_order_release);
    return C_OK;
}

static size_t packet_size_for(size_t nreq) {
    return sizeof(batch_packet_t) + nreq * sizeof(((batch_packet_t *)0)->requests[0]);
}

static void fill_packet(batch_packet_t *pkt, int worker_id, uint64_t batch_id, size_t nreq) {
    pkt->magic = 0xCAC0BEEF;
    pkt->packet_size = (uint32_t)packet_size_for(nreq);
    pkt->num_requests = (uint32_t)nreq;
    pkt->supernode_id = 0;
    pkt->worker_id = (uint32_t)worker_id;
    pkt->timestamp_us = batch_id;
    pkt->batch_id = batch_id;
    for (size_t i = 0; i < nreq; i++) {
        pkt->requests[i].request_id = batch_id * 1000 + i;
        pkt->requests[i].key_hash = (uint64_t)worker_id << 32 | i;
    }
}

static void *worker_main(void *arg) {
    worker_ctx_t *ctx = arg;
    while (ctx->running || atomic_load_explicit(&ctx->rb->head, memory_order_relaxed) !=
                            atomic_load_explicit(&ctx->rb->tail, memory_order_acquire)) {
        void *payload = NULL;
        size_t payload_len = 0;
        if (rb_peek(ctx->rb, &payload, &payload_len) == C_OK) {
            batch_packet_t *pkt = payload;
            if (pkt->magic != 0xCAC0BEEF) {
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
            rb_commit_read(ctx->rb, payload_len);
        }
    }
    return NULL;
}

int main(void) {
    ring_buffer_t *rbs[NUM_WORKERS] = {0};
    worker_ctx_t workers[NUM_WORKERS] = {0};
    uint64_t start, elapsed;
    uint64_t total_batches = 0, total_requests = 0;

    for (int i = 0; i < NUM_WORKERS; i++) {
        rbs[i] = rb_create(RING_BUFFER_SIZE);
        if (!rbs[i]) {
            fprintf(stderr, "rb_create failed\n");
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
        size_t pkt_size = packet_size_for(REQUESTS_PER_BATCH);
        batch_packet_t *pkt = NULL;

        while (rb_reserve(rbs[worker_id], pkt_size, (void **)&pkt) != C_OK) {
        }
        fill_packet(pkt, worker_id, batch_id, REQUESTS_PER_BATCH);
        if (rb_commit_write(rbs[worker_id], pkt_size) != C_OK) {
            fprintf(stderr, "commit_write failed\n");
            return 1;
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
    printf("  consumed batches: %" PRIu64 "\n", total_batches);
    printf("  consumed requests: %" PRIu64 "\n", total_requests);
    printf("  total time: %.3f ms\n", (double)elapsed / 1e6);
    printf("  batch throughput: %.2f M batch/s\n", (double)total_batches / ((double)elapsed / 1e9) / 1e6);
    printf("  request throughput: %.2f M req/s\n", (double)total_requests / ((double)elapsed / 1e9) / 1e6);

    if (total_batches != NUM_BATCHES || total_requests != (uint64_t)NUM_BATCHES * REQUESTS_PER_BATCH) {
        fprintf(stderr, "count mismatch\n");
        return 1;
    }

    for (int i = 0; i < NUM_WORKERS; i++) {
        rb_destroy(rbs[i]);
    }
    return 0;
}
