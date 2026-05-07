/*
 * Compare the legacy shared-queue design against the refactored
 * per-worker queue + SPSC + zero-copy design.
 *
 * Notes:
 * - The legacy path uses a read mutex on pop to provide a correctness-preserving
 *   approximation of the original shared multi-consumer queue.
 * - The refactored path matches the new design: per-worker queue, reserve/commit,
 *   peek/commit_read, slim packet.
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
} __attribute__((packed)) new_batch_packet_t;

typedef struct {
    atomic_uint_fast64_t head;
    atomic_uint_fast64_t tail;
    uint8_t *buffer;
    size_t size;
    pthread_mutex_t write_mutex;
    pthread_mutex_t read_mutex;
} legacy_ring_buffer_t;

typedef struct {
    _Alignas(64) atomic_uint_fast64_t head;
    char head_pad[64 - sizeof(atomic_uint_fast64_t)];
    _Alignas(64) atomic_uint_fast64_t tail;
    char tail_pad[64 - sizeof(atomic_uint_fast64_t)];
    uint8_t *buffer;
    size_t size;
    uint64_t reserved_commit_tail;
    size_t reserved_payload_len;
    int reservation_active;
} new_ring_buffer_t;

typedef struct {
    int worker_id;
    legacy_ring_buffer_t *legacy_rb;
    new_ring_buffer_t *new_rb;
    atomic_uint_fast64_t consumed_batches;
    atomic_uint_fast64_t consumed_requests;
    int running;
    pthread_t thread;
    int mode; /* 0=legacy, 1=new */
} worker_ctx_t;

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static size_t legacy_packet_size_for(size_t nreq) {
    return sizeof(legacy_batch_packet_t) + nreq * sizeof(((legacy_batch_packet_t *)0)->requests[0]);
}

static size_t new_packet_size_for(size_t nreq) {
    return sizeof(new_batch_packet_t) + nreq * sizeof(((new_batch_packet_t *)0)->requests[0]);
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
    if (!rb || !data || len == 0) return C_ERR;
    pthread_mutex_lock(&rb->write_mutex);
    if (legacy_available_space(rb) < len + sizeof(uint32_t)) {
        pthread_mutex_unlock(&rb->write_mutex);
        return C_ERR;
    }
    uint64_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
    size_t pos = tail % rb->size;
    uint32_t packet_len = (uint32_t)len;
    size_t remaining = rb->size - pos;
    if (remaining >= sizeof(uint32_t)) {
        memcpy(rb->buffer + pos, &packet_len, sizeof(uint32_t));
        pos = (pos + sizeof(uint32_t)) % rb->size;
    } else {
        memcpy(rb->buffer + pos, &packet_len, remaining);
        memcpy(rb->buffer, ((uint8_t *)&packet_len) + remaining, sizeof(uint32_t) - remaining);
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
    if (!rb || !data) return C_ERR;
    pthread_mutex_lock(&rb->read_mutex);
    if (legacy_available_data(rb) < sizeof(uint32_t)) {
        pthread_mutex_unlock(&rb->read_mutex);
        return C_ERR;
    }
    uint64_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    size_t pos = head % rb->size;
    uint32_t packet_len;
    size_t remaining = rb->size - pos;
    if (remaining >= sizeof(uint32_t)) {
        memcpy(&packet_len, rb->buffer + pos, sizeof(uint32_t));
        pos = (pos + sizeof(uint32_t)) % rb->size;
    } else {
        memcpy(&packet_len, rb->buffer + pos, remaining);
        memcpy(((uint8_t *)&packet_len) + remaining, rb->buffer, sizeof(uint32_t) - remaining);
        pos = sizeof(uint32_t) - remaining;
    }
    if (packet_len > max_len) {
        pthread_mutex_unlock(&rb->read_mutex);
        return C_ERR;
    }
    if (legacy_available_data(rb) < sizeof(uint32_t) + packet_len) {
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

static new_ring_buffer_t *new_rb_create(size_t size) {
    new_ring_buffer_t *rb = calloc(1, sizeof(*rb));
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

static void new_rb_destroy(new_ring_buffer_t *rb) {
    if (!rb) return;
    free(rb->buffer);
    free(rb);
}

static int new_rb_reserve(new_ring_buffer_t *rb, size_t payload_len, void **payload) {
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

static int new_rb_commit_write(new_ring_buffer_t *rb, size_t payload_len) {
    if (!rb || payload_len == 0) return C_ERR;
    if (!rb->reservation_active || rb->reserved_payload_len != payload_len) return C_ERR;
    atomic_store_explicit(&rb->tail, rb->reserved_commit_tail, memory_order_release);
    rb->reservation_active = 0;
    rb->reserved_payload_len = 0;
    rb->reserved_commit_tail = 0;
    return C_OK;
}

static int new_rb_peek(new_ring_buffer_t *rb, void **payload, size_t *payload_len) {
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

static int new_rb_commit_read(new_ring_buffer_t *rb, size_t payload_len) {
    if (!rb || payload_len == 0) return C_ERR;
    uint64_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    atomic_store_explicit(&rb->head, head + sizeof(uint32_t) + payload_len, memory_order_release);
    return C_OK;
}

static void fill_legacy_packet(legacy_batch_packet_t *pkt, uint64_t batch_id, size_t nreq) {
    static const char *key = "user:123456789";
    pkt->magic = 0xCAC0BEEF;
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

static void fill_new_packet(new_batch_packet_t *pkt, int worker_id, uint64_t batch_id, size_t nreq) {
    pkt->magic = 0xCAC0BEEF;
    pkt->packet_size = (uint32_t)new_packet_size_for(nreq);
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
    while (ctx->running ||
           (ctx->mode == 0
                ? atomic_load_explicit(&ctx->legacy_rb->head, memory_order_relaxed) !=
                      atomic_load_explicit(&ctx->legacy_rb->tail, memory_order_acquire)
                : atomic_load_explicit(&ctx->new_rb->head, memory_order_relaxed) !=
                      atomic_load_explicit(&ctx->new_rb->tail, memory_order_acquire))) {
        if (ctx->mode == 0) {
            uint8_t buffer[16384];
            size_t actual_len = 0;
            if (legacy_rb_pop(ctx->legacy_rb, buffer, sizeof(buffer), &actual_len) == C_OK) {
                legacy_batch_packet_t *pkt = (legacy_batch_packet_t *)buffer;
                if (pkt->magic != 0xCAC0BEEF) {
                    fprintf(stderr, "legacy bad magic\n");
                    exit(2);
                }
                atomic_fetch_add(&ctx->consumed_batches, 1);
                atomic_fetch_add(&ctx->consumed_requests, pkt->num_requests);
            }
        } else {
            void *payload = NULL;
            size_t payload_len = 0;
            if (new_rb_peek(ctx->new_rb, &payload, &payload_len) == C_OK) {
                new_batch_packet_t *pkt = payload;
                if (pkt->magic != 0xCAC0BEEF || (int)pkt->worker_id != ctx->worker_id) {
                    fprintf(stderr, "new route/magic failure\n");
                    exit(3);
                }
                atomic_fetch_add(&ctx->consumed_batches, 1);
                atomic_fetch_add(&ctx->consumed_requests, pkt->num_requests);
                new_rb_commit_read(ctx->new_rb, payload_len);
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
    printf("  batch throughput: %.2f M batch/s\n", (double)total_batches / ((double)elapsed / 1e9) / 1e6);
    printf("  request throughput: %.2f M req/s\n\n", (double)total_requests / ((double)elapsed / 1e9) / 1e6);

    if (total_batches != NUM_BATCHES || total_requests != (uint64_t)NUM_BATCHES * REQUESTS_PER_BATCH) {
        fprintf(stderr, "legacy count mismatch\n");
        exit(4);
    }
    legacy_rb_destroy(rb);
}

static void run_refactored(void) {
    new_ring_buffer_t *rbs[NUM_WORKERS] = {0};
    worker_ctx_t workers[NUM_WORKERS] = {0};
    uint64_t total_batches = 0, total_requests = 0, start, elapsed;

    for (int i = 0; i < NUM_WORKERS; i++) {
        rbs[i] = new_rb_create(WORKER_RING_BUFFER_SIZE);
        if (!rbs[i]) {
            fprintf(stderr, "new rb_create failed\n");
            exit(1);
        }
        workers[i].worker_id = i;
        workers[i].new_rb = rbs[i];
        workers[i].mode = 1;
        workers[i].running = 1;
        atomic_init(&workers[i].consumed_batches, 0);
        atomic_init(&workers[i].consumed_requests, 0);
        pthread_create(&workers[i].thread, NULL, worker_main, &workers[i]);
    }

    start = now_ns();
    for (uint64_t batch_id = 0; batch_id < NUM_BATCHES; batch_id++) {
        int worker_id = (int)(batch_id % NUM_WORKERS);
        size_t pkt_size = new_packet_size_for(REQUESTS_PER_BATCH);
        new_batch_packet_t *pkt = NULL;
        while (new_rb_reserve(rbs[worker_id], pkt_size, (void **)&pkt) != C_OK) {
        }
        fill_new_packet(pkt, worker_id, batch_id, REQUESTS_PER_BATCH);
        if (new_rb_commit_write(rbs[worker_id], pkt_size) != C_OK) {
            fprintf(stderr, "new commit_write failed\n");
            exit(1);
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
    printf("  packet bytes: %zu\n", new_packet_size_for(REQUESTS_PER_BATCH));
    printf("  batches: %" PRIu64 "\n", (uint64_t)NUM_BATCHES);
    printf("  requests per batch: %d\n", REQUESTS_PER_BATCH);
    printf("  consumed batches: %" PRIu64 "\n", total_batches);
    printf("  consumed requests: %" PRIu64 "\n", total_requests);
    printf("  total time: %.3f ms\n", (double)elapsed / 1e6);
    printf("  batch throughput: %.2f M batch/s\n", (double)total_batches / ((double)elapsed / 1e9) / 1e6);
    printf("  request throughput: %.2f M req/s\n\n", (double)total_requests / ((double)elapsed / 1e9) / 1e6);

    if (total_batches != NUM_BATCHES || total_requests != (uint64_t)NUM_BATCHES * REQUESTS_PER_BATCH) {
        fprintf(stderr, "refactored count mismatch\n");
        exit(5);
    }
    for (int i = 0; i < NUM_WORKERS; i++) new_rb_destroy(rbs[i]);
}

int main(void) {
    run_legacy();
    run_refactored();
    return 0;
}
