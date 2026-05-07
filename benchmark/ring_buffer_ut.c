/*
 * Standalone ring buffer self-test for the SuperNode queue path.
 *
 * Covers:
 * - basic push/pop
 * - reserve/commit_write + peek/commit_read
 * - padding-record wraparound
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdatomic.h>
#include <string.h>

#define C_OK 0
#define C_ERR -1

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

static int rb_push(ring_buffer_t *rb, const void *data, size_t len) {
    void *payload = NULL;
    if (rb_reserve(rb, len, &payload) != C_OK) return C_ERR;
    memcpy(payload, data, len);
    return rb_commit_write(rb, len);
}

static int rb_pop(ring_buffer_t *rb, void *data, size_t max_len, size_t *actual_len) {
    void *payload = NULL;
    size_t payload_len = 0;
    if (rb_peek(rb, &payload, &payload_len) != C_OK) return C_ERR;
    if (payload_len > max_len) return C_ERR;
    memcpy(data, payload, payload_len);
    if (rb_commit_read(rb, payload_len) != C_OK) return C_ERR;
    if (actual_len) *actual_len = payload_len;
    return C_OK;
}

static int test_basic_push_pop(void) {
    ring_buffer_t *rb = rb_create(128);
    char out[32];
    size_t actual = 0;
    const char *msg = "hello";

    if (!rb) return C_ERR;
    if (rb_push(rb, msg, strlen(msg) + 1) != C_OK) return C_ERR;
    if (rb_pop(rb, out, sizeof(out), &actual) != C_OK) return C_ERR;
    if (actual != strlen(msg) + 1) return C_ERR;
    if (strcmp(out, msg) != 0) return C_ERR;
    rb_destroy(rb);
    return C_OK;
}

static int test_zero_copy_peek(void) {
    ring_buffer_t *rb = rb_create(128);
    const char *msg = "peek-msg";
    void *payload = NULL;
    size_t payload_len = 0;

    if (!rb) return C_ERR;
    if (rb_push(rb, msg, strlen(msg) + 1) != C_OK) return C_ERR;
    if (rb_peek(rb, &payload, &payload_len) != C_OK) return C_ERR;
    if (payload_len != strlen(msg) + 1) return C_ERR;
    if (strcmp((const char *)payload, msg) != 0) return C_ERR;
    if (rb_commit_read(rb, payload_len) != C_OK) return C_ERR;
    rb_destroy(rb);
    return C_OK;
}

static int test_padding_wraparound(void) {
    ring_buffer_t *rb = rb_create(64);
    char out[32];
    size_t actual = 0;
    const char *msg1 = "12345678901234567890";
    const char *msg2 = "tail-wrap";

    if (!rb) return C_ERR;
    if (rb_push(rb, msg1, strlen(msg1) + 1) != C_OK) return C_ERR;
    if (rb_pop(rb, out, sizeof(out), &actual) != C_OK) return C_ERR;
    if (strcmp(out, msg1) != 0) return C_ERR;

    if (rb_push(rb, msg2, strlen(msg2) + 1) != C_OK) return C_ERR;
    if (rb_pop(rb, out, sizeof(out), &actual) != C_OK) return C_ERR;
    if (strcmp(out, msg2) != 0) return C_ERR;

    rb_destroy(rb);
    return C_OK;
}

int main(void) {
    if (test_basic_push_pop() != C_OK) {
        fprintf(stderr, "test_basic_push_pop failed\n");
        return 1;
    }
    if (test_zero_copy_peek() != C_OK) {
        fprintf(stderr, "test_zero_copy_peek failed\n");
        return 1;
    }
    if (test_padding_wraparound() != C_OK) {
        fprintf(stderr, "test_padding_wraparound failed\n");
        return 1;
    }

    printf("ring_buffer_ut: all tests passed\n");
    return 0;
}
