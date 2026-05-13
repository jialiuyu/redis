#include <assert.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "test_runtime_shim.h"
#include "../src/consistent_hash.h"
#include "../src/proxy_router.h"
#include "../src/proxy_flush_scheduler.h"
#include "../src/proxy_batch_bucket.h"
#include "../src/proxy_flush_executor.h"
#include "../src/ring_buffer.h"

static ring_buffer_t *test_rb_create(size_t size) {
    ring_buffer_t *rb = calloc(1, sizeof(*rb));
    assert(rb != NULL);
    rb->buffer = calloc(1, size);
    assert(rb->buffer != NULL);
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

static void test_flush_scheduler(void) {
    flush_scheduler_t scheduler;
    flush_scheduler_init(&scheduler, 4, 200);

    flush_decision_t d1 = flush_scheduler_on_append(&scheduler, 3);
    assert(d1.should_flush == 0);

    flush_decision_t d2 = flush_scheduler_on_append(&scheduler, 4);
    assert(d2.should_flush == 1);
    assert(d2.flush_reason_full == 1);

    flush_decision_t d3 = flush_scheduler_on_poll(&scheduler, 2, 100);
    assert(d3.should_flush == 0);

    flush_decision_t d4 = flush_scheduler_on_poll(&scheduler, 2, 250);
    assert(d4.should_flush == 1);
    assert(d4.flush_reason_full == 0);
}

static void test_proxy_batch_bucket_packet(void) {
    proxy_batch_bucket_t bucket = {0};
    assert(proxy_batch_bucket_init(&bucket, 4, 7, 3, 100) == C_OK);

    proxy_request_t *r1 = proxy_request_create(11, 101, 7, 3, 100, NULL, NULL, 0);
    proxy_request_t *r2 = proxy_request_create(12, 102, 7, 3, 101, NULL, NULL, 0);
    assert(r1 && r2);

    assert(proxy_batch_bucket_append(&bucket, r1) == C_OK);
    assert(proxy_batch_bucket_append(&bucket, r2) == C_OK);
    assert(bucket.count == 2);

    size_t packet_size = proxy_batch_bucket_packet_size(&bucket);
    assert(packet_size == sizeof(batch_packet_t) + 2 * sizeof(((batch_packet_t *)0)->requests[0]));

    batch_packet_t *packet = malloc(packet_size);
    assert(packet);
    assert(proxy_batch_bucket_fill_packet(&bucket, packet, packet_size, 999, 77) == C_OK);
    assert(packet->magic == BATCH_PACKET_MAGIC);
    assert(packet->num_requests == 2);
    assert(packet->supernode_id == 7);
    assert(packet->worker_id == 3);
    assert(packet->timestamp_us == 999);
    assert(packet->batch_id == 77);
    assert(packet->requests[0].request_id == 11);
    assert(packet->requests[1].key_hash == 102);

    free(packet);
    proxy_batch_bucket_reset(&bucket, 200);
    assert(bucket.count == 0);
    proxy_batch_bucket_cleanup(&bucket);
}

static void test_ring_buffer_cancel_write(void) {
    ring_buffer_t *rb = test_rb_create(128);
    void *payload = NULL;
    size_t out_len = 0;
    char out[16];
    const char *msg = "ok";

    assert(ring_buffer_reserve(rb, 8, &payload) == C_OK);
    assert(ring_buffer_cancel_write(rb) == C_OK);
    assert(ring_buffer_reserve(rb, strlen(msg) + 1, &payload) == C_OK);
    memcpy(payload, msg, strlen(msg) + 1);
    assert(ring_buffer_commit_write(rb, strlen(msg) + 1) == C_OK);
    assert(ring_buffer_pop(rb, out, sizeof(out), &out_len) == C_OK);
    assert(strcmp(out, msg) == 0);
    test_rb_destroy(rb);
}

static void test_proxy_flush_executor_success(void) {
    proxy_flush_executor_t executor;
    proxy_batch_bucket_t bucket = {0};
    ring_buffer_t *rb = test_rb_create(256);
    void *payload = NULL;
    size_t payload_len = 0;

    proxy_flush_executor_init(&executor);
    assert(proxy_batch_bucket_init(&bucket, 4, 1, 2, 100) == C_OK);
    assert(proxy_batch_bucket_append(&bucket,
           proxy_request_create(1, 123, 1, 2, 100, NULL, NULL, 0)) == C_OK);

    assert(proxy_executor_flush_bucket_locked(&executor, &bucket, rb, 1, 200,
           PROXY_FLUSH_TRIGGER_IMMEDIATE_APPEND) == C_OK);
    assert(bucket.count == 0);
    assert(atomic_load_explicit(&executor.stats.total_flushes, memory_order_relaxed) == 1);
    assert(atomic_load_explicit(&executor.stats.immediate_flush_attempts, memory_order_relaxed) == 1);
    assert(atomic_load_explicit(&executor.stats.immediate_flush_successes, memory_order_relaxed) == 1);

    assert(ring_buffer_peek(rb, &payload, &payload_len) == C_OK);
    assert(((batch_packet_t *)payload)->magic == BATCH_PACKET_MAGIC);

    proxy_batch_bucket_cleanup(&bucket);
    test_rb_destroy(rb);
}

static void test_proxy_flush_executor_failure_metrics(void) {
    proxy_flush_executor_t executor;
    proxy_batch_bucket_t bucket = {0};
    ring_buffer_t *rb = test_rb_create(16);

    proxy_flush_executor_init(&executor);
    assert(proxy_batch_bucket_init(&bucket, 4, 1, 2, 100) == C_OK);
    assert(proxy_batch_bucket_append(&bucket,
           proxy_request_create(1, 123, 1, 2, 100, NULL, NULL, 0)) == C_OK);

    assert(proxy_executor_flush_bucket_locked(&executor, &bucket, rb, 1, 200,
           PROXY_FLUSH_TRIGGER_IMMEDIATE_CAPACITY) == C_ERR);
    assert(atomic_load_explicit(&executor.stats.immediate_flush_attempts, memory_order_relaxed) == 1);
    assert(atomic_load_explicit(&executor.stats.enqueue_rejections_full, memory_order_relaxed) == 1);

    assert(proxy_executor_flush_bucket_locked(&executor, &bucket, rb, 1, 200,
           PROXY_FLUSH_TRIGGER_IMMEDIATE_APPEND) == C_ERR);
    assert(atomic_load_explicit(&executor.stats.immediate_flush_attempts, memory_order_relaxed) == 2);
    assert(atomic_load_explicit(&executor.stats.immediate_flush_deferred, memory_order_relaxed) == 1);

    proxy_batch_bucket_cleanup(&bucket);
    test_rb_destroy(rb);
}

static void test_proxy_router(void) {
    proxy_router_t router;
    proxy_route_t r1, r2;

    assert(proxy_router_init(&router, 4, 8) == C_OK);
    assert(proxy_router_route(&router, "alpha", &r1) == C_OK);
    assert(proxy_router_route(&router, "alpha", &r2) == C_OK);
    assert(r1.supernode_id == r2.supernode_id);
    assert(r1.worker_id == r2.worker_id);
    assert(r1.key_hash == r2.key_hash);
    assert(r1.supernode_id >= 0 && r1.supernode_id < 4);
    assert(r1.worker_id >= 0 && r1.worker_id < 8);
    proxy_router_cleanup(&router);
}

int main(void) {
    test_flush_scheduler();
    test_proxy_batch_bucket_packet();
    test_ring_buffer_cancel_write();
    test_proxy_flush_executor_success();
    test_proxy_flush_executor_failure_metrics();
    test_proxy_router();
    printf("proxy_components_ut: all tests passed\n");
    return 0;
}
