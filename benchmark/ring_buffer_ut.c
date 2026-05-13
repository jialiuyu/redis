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

#include "test_runtime_shim.h"
#include "../src/ring_buffer.h"

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
    atomic_init(&rb->refcount, 1);
    return rb;
}

static void rb_destroy(ring_buffer_t *rb) {
    if (!rb) return;
    free(rb->buffer);
    free(rb);
}

static int test_basic_push_pop(void) {
    ring_buffer_t *rb = rb_create(128);
    char out[32];
    size_t actual = 0;
    const char *msg = "hello";

    if (!rb) return C_ERR;
    if (ring_buffer_push(rb, msg, strlen(msg) + 1) != C_OK) return C_ERR;
    if (ring_buffer_pop(rb, out, sizeof(out), &actual) != C_OK) return C_ERR;
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
    if (ring_buffer_push(rb, msg, strlen(msg) + 1) != C_OK) return C_ERR;
    if (ring_buffer_peek(rb, &payload, &payload_len) != C_OK) return C_ERR;
    if (payload_len != strlen(msg) + 1) return C_ERR;
    if (strcmp((const char *)payload, msg) != 0) return C_ERR;
    if (ring_buffer_commit_read(rb, payload_len) != C_OK) return C_ERR;
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
    if (ring_buffer_push(rb, msg1, strlen(msg1) + 1) != C_OK) return C_ERR;
    if (ring_buffer_pop(rb, out, sizeof(out), &actual) != C_OK) return C_ERR;
    if (strcmp(out, msg1) != 0) return C_ERR;

    if (ring_buffer_push(rb, msg2, strlen(msg2) + 1) != C_OK) return C_ERR;
    if (ring_buffer_pop(rb, out, sizeof(out), &actual) != C_OK) return C_ERR;
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
