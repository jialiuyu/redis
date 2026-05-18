#include <assert.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

    proxy_vector_request_t o1 = {.op_type = PROXY_VECTOR_OP_VEMB, .row_id = 1011};
    proxy_vector_request_t o2 = {.op_type = PROXY_VECTOR_OP_VEMB, .row_id = 1022};
    proxy_request_t *r1 = proxy_request_create(11, 101, 7, 3, 100, &o1);
    proxy_request_t *r2 = proxy_request_create(12, 102, 7, 3, 101, &o2);
    assert(r1 && r2);

    assert(proxy_batch_bucket_append(&bucket, r1) == C_OK);
    assert(proxy_batch_bucket_append(&bucket, r2) == C_OK);
    assert(bucket.count == 2);

    size_t packet_size = proxy_batch_bucket_packet_size(&bucket);
    assert(packet_size == sizeof(batch_packet_t) + 2 * sizeof(((batch_packet_t *)0)->requests[0]));

    batch_packet_t *packet = malloc(packet_size);
    assert(packet);
    assert(proxy_batch_bucket_fill_packet(&bucket, packet, packet_size, 999, 77) == C_OK);
    assert(packet->hdr.magic == BATCH_PACKET_MAGIC);
    assert(packet->hdr.num_requests == 2);
    assert(packet->hdr.op_type == BATCH_PACKET_OP_VEMB);
    assert(packet->hdr.supernode_id == 7);
    assert(packet->hdr.worker_id == 3);
    assert(packet->hdr.timestamp_us == 999);
    assert(packet->hdr.batch_id == 77);
    assert(packet->requests[0].request_id == 11);
    assert(packet->requests[0].row_id == 1011);
    assert(packet->requests[1].request_id == 12);
    assert(packet->requests[1].row_id == 1022);

    free(packet);
    proxy_batch_bucket_reset(&bucket, 200);
    assert(bucket.count == 0);
    proxy_batch_bucket_cleanup(&bucket);
}

static void test_proxy_batch_bucket_vsim_packet(void) {
    proxy_batch_bucket_t bucket = {0};
    assert(proxy_batch_bucket_init(&bucket, 2, 2, 5, 100) == C_OK);

    float query[] = {1.0f, 2.0f, 3.0f};
    uint64_t rows[] = {10, 20, 30, 40};
    proxy_vector_request_t owner = {
        .op_type = PROXY_VECTOR_OP_VSIM,
        .request_id = 99,
        .query_vector = query,
        .query_dim = 3,
        .candidate_rows = rows,
        .candidate_count = 4,
        .requested_count = 2,
        .withscores = 1,
    };
    proxy_request_t *req = proxy_request_create(99, 123, 2, 5, 100, &owner);
    assert(req);
    assert(proxy_batch_bucket_append(&bucket, req) == C_OK);

    size_t packet_size = proxy_batch_bucket_packet_size(&bucket);
    assert(packet_size == sizeof(batch_vsim_packet_t) +
                          sizeof(float) * 3 +
                          sizeof(uint64_t) * 4);

    batch_vsim_packet_t *packet = malloc(packet_size);
    assert(packet);
    assert(proxy_batch_bucket_fill_packet(&bucket, (batch_packet_t *)packet,
                                          packet_size, 777, 88) == C_OK);
    assert(packet->hdr.magic == BATCH_PACKET_MAGIC);
    assert(packet->hdr.op_type == BATCH_PACKET_OP_VSIM);
    assert(packet->hdr.supernode_id == 2);
    assert(packet->hdr.worker_id == 5);
    assert(packet->hdr.batch_id == 88);
    assert(packet->flags == 1);
    assert(packet->request_id == 99);
    assert(packet->query_dim == 3);
    assert(packet->requested_count == 2);
    assert(packet->candidate_count == 4);
    assert(packet->payload[0] == 1.0f);
    assert(packet->payload[2] == 3.0f);
    uint64_t *packet_rows = (uint64_t *)(packet->payload + packet->query_dim);
    assert(packet_rows[0] == 10);
    assert(packet_rows[3] == 40);

    free(packet);
    proxy_batch_bucket_cleanup(&bucket);
}

static void test_proxy_batch_bucket_arrival_ewma(void) {
    proxy_batch_bucket_t bucket = {0};
    assert(proxy_batch_bucket_init(&bucket, 2, 1, 1, 100) == C_OK);

    proxy_batch_bucket_note_arrival(&bucket, 1000);
    assert(bucket.last_append_time_us == 1000);
    assert(bucket.recent_gap_ewma_us == 0);

    proxy_batch_bucket_note_arrival(&bucket, 1040);
    assert(bucket.last_append_time_us == 1040);
    assert(bucket.recent_gap_ewma_us == 40);

    proxy_batch_bucket_note_arrival(&bucket, 1080);
    assert(bucket.last_append_time_us == 1080);
    assert(bucket.recent_gap_ewma_us == 40);

    proxy_batch_bucket_note_arrival(&bucket, 1096);
    assert(bucket.last_append_time_us == 1096);
    assert(bucket.recent_gap_ewma_us == 37);

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
    proxy_vector_request_t owner = {.op_type = PROXY_VECTOR_OP_VEMB, .row_id = 456};
    assert(proxy_batch_bucket_append(&bucket,
           proxy_request_create(1, 123, 1, 2, 100, &owner)) == C_OK);

    assert(proxy_executor_flush_bucket_locked(&executor, &bucket, rb, 1, 200,
           PROXY_FLUSH_TRIGGER_IMMEDIATE_APPEND) == C_OK);
    assert(bucket.count == 0);
    assert(atomic_load_explicit(&executor.stats.total_flushes, memory_order_relaxed) == 1);
    assert(atomic_load_explicit(&executor.stats.immediate_flush_attempts, memory_order_relaxed) == 1);
    assert(atomic_load_explicit(&executor.stats.immediate_flush_successes, memory_order_relaxed) == 1);

    assert(ring_buffer_peek(rb, &payload, &payload_len) == C_OK);
    assert(((batch_packet_t *)payload)->hdr.magic == BATCH_PACKET_MAGIC);
    assert(((batch_packet_t *)payload)->requests[0].row_id == 456);

    proxy_batch_bucket_cleanup(&bucket);
    test_rb_destroy(rb);
}

static void test_proxy_flush_executor_failure_metrics(void) {
    proxy_flush_executor_t executor;
    proxy_batch_bucket_t bucket = {0};
    ring_buffer_t *rb = test_rb_create(16);

    proxy_flush_executor_init(&executor);
    assert(proxy_batch_bucket_init(&bucket, 4, 1, 2, 100) == C_OK);
    proxy_vector_request_t owner = {.op_type = PROXY_VECTOR_OP_VEMB, .row_id = 456};
    assert(proxy_batch_bucket_append(&bucket,
           proxy_request_create(1, 123, 1, 2, 100, &owner)) == C_OK);

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

static void test_proxy_router_hot_key_distribution(void) {
    proxy_router_t router;
    proxy_route_t base, route;
    int seen[8] = {0};
    int distinct = 0;

    assert(proxy_router_init(&router, 2, 8) == C_OK);
    assert(proxy_router_route(&router, "hotkey", &base) == C_OK);
    for (uint64_t row = 0; row < 32; row++) {
        assert(proxy_router_route_by_row(&router, "hotkey", row, &route) == C_OK);
        assert(route.supernode_id == base.supernode_id);
        assert(route.key_hash == base.key_hash);
        assert(route.worker_id == (int)(row % 8));
        if (!seen[route.worker_id]) {
            seen[route.worker_id] = 1;
            distinct++;
        }
    }
    assert(distinct == 8);

    assert(proxy_router_route_by_request(&router, "hotkey", 17, &route) == C_OK);
    assert(route.supernode_id == base.supernode_id);
    assert(route.worker_id == 1);
    proxy_router_cleanup(&router);
}

int main(void) {
    test_flush_scheduler();
    test_proxy_batch_bucket_packet();
    test_proxy_batch_bucket_vsim_packet();
    test_proxy_batch_bucket_arrival_ewma();
    test_ring_buffer_cancel_write();
    test_proxy_flush_executor_success();
    test_proxy_flush_executor_failure_metrics();
    test_proxy_router();
    test_proxy_router_hot_key_distribution();
    printf("proxy_components_ut: all tests passed\n");
    return 0;
}
