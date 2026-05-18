#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "test_runtime_shim.h"

#include "../src/batch_latency_trace.h"
#include "../src/ring_buffer.c"

typedef struct RedisModuleCtx RedisModuleCtx;
typedef struct RedisModuleBlockedClient RedisModuleBlockedClient;
typedef struct ub_vector_set_meta ub_vector_set_meta_t;
typedef int (*RedisModuleCmdFunc)(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

static int g_unblock_count = 0;
static void *g_last_unblock_privdata = NULL;

static int test_RedisModule_UnblockClient(RedisModuleBlockedClient *bc, void *privdata) {
    (void)bc;
    g_unblock_count++;
    g_last_unblock_privdata = privdata;
    return REDISMODULE_OK;
}

static void *test_RedisModule_GetBlockedClientPrivateData(RedisModuleCtx *ctx) {
    return ctx;
}

static int test_RedisModule_ReplyWithError(RedisModuleCtx *ctx, const char *err) {
    (void)ctx;
    (void)err;
    return REDISMODULE_OK;
}

static int test_RedisModule_ReplyWithNull(RedisModuleCtx *ctx) {
    (void)ctx;
    return REDISMODULE_OK;
}

static int test_RedisModule_ReplyWithArray(RedisModuleCtx *ctx, long len) {
    (void)ctx;
    (void)len;
    return REDISMODULE_OK;
}

static int test_RedisModule_ReplyWithSimpleString(RedisModuleCtx *ctx, const char *msg) {
    (void)ctx;
    (void)msg;
    return REDISMODULE_OK;
}

static int test_RedisModule_ReplyWithStringBuffer(RedisModuleCtx *ctx,
                                                  const char *buf,
                                                  size_t len) {
    (void)ctx;
    (void)buf;
    (void)len;
    return REDISMODULE_OK;
}

static int test_RedisModule_ReplyWithDouble(RedisModuleCtx *ctx, double d) {
    (void)ctx;
    (void)d;
    return REDISMODULE_OK;
}

static RedisModuleBlockedClient *test_RedisModule_GetBlockedClientHandle(RedisModuleCtx *ctx) {
    (void)ctx;
    return NULL;
}

static int test_RedisModule_BlockedClientMeasureTimeEnd(RedisModuleBlockedClient *bc) {
    (void)bc;
    return REDISMODULE_OK;
}

static const char *test_RedisModule_StringPtrLen(const RedisModuleString *str,
                                                 size_t *len) {
    const char *s = (const char *)str;
    if (len) *len = s ? strlen(s) : 0;
    return s;
}

static RedisModuleBlockedClient *test_RedisModule_BlockClient(RedisModuleCtx *ctx,
                                                              RedisModuleCmdFunc reply_callback,
                                                              RedisModuleCmdFunc timeout_callback,
                                                              void (*free_privdata)(RedisModuleCtx *, void *),
                                                              long long timeout_ms) {
    (void)ctx;
    (void)reply_callback;
    (void)timeout_callback;
    (void)free_privdata;
    (void)timeout_ms;
    return (RedisModuleBlockedClient *)1;
}

static int test_RedisModule_AbortBlock(RedisModuleBlockedClient *bc) {
    (void)bc;
    return REDISMODULE_OK;
}

static void test_RedisModule_BlockClientSetPrivateData(RedisModuleBlockedClient *bc,
                                                       void *private_data) {
    (void)bc;
    (void)private_data;
}

static int test_RedisModule_BlockedClientMeasureTimeStart(RedisModuleBlockedClient *bc) {
    (void)bc;
    return REDISMODULE_OK;
}

#define RedisModule_UnblockClient test_RedisModule_UnblockClient
#define RedisModule_GetBlockedClientPrivateData test_RedisModule_GetBlockedClientPrivateData
#define RedisModule_ReplyWithError test_RedisModule_ReplyWithError
#define RedisModule_ReplyWithNull test_RedisModule_ReplyWithNull
#define RedisModule_ReplyWithArray test_RedisModule_ReplyWithArray
#define RedisModule_ReplyWithSimpleString test_RedisModule_ReplyWithSimpleString
#define RedisModule_ReplyWithStringBuffer test_RedisModule_ReplyWithStringBuffer
#define RedisModule_ReplyWithDouble test_RedisModule_ReplyWithDouble
#define RedisModule_GetBlockedClientHandle test_RedisModule_GetBlockedClientHandle
#define RedisModule_BlockedClientMeasureTimeEnd test_RedisModule_BlockedClientMeasureTimeEnd
#define RedisModule_StringPtrLen test_RedisModule_StringPtrLen
#define RedisModule_BlockClient test_RedisModule_BlockClient
#define RedisModule_AbortBlock test_RedisModule_AbortBlock
#define RedisModule_BlockClientSetPrivateData test_RedisModule_BlockClientSetPrivateData
#define RedisModule_BlockedClientMeasureTimeStart test_RedisModule_BlockedClientMeasureTimeStart

#include "../src/vector_proxy_request.c"

static ring_buffer_t g_request_rings[128];
static uint8_t g_ring_buffers[128][1 << 16];
static size_t g_ring_count = 0;

static void reset_test_rings(size_t count) {
    assert(count <= 128);
    g_ring_count = count;
    for (size_t i = 0; i < count; i++) {
        memset(&g_request_rings[i], 0, sizeof(g_request_rings[i]));
        memset(g_ring_buffers[i], 0, sizeof(g_ring_buffers[i]));
        g_request_rings[i].buffer = g_ring_buffers[i];
        g_request_rings[i].size = sizeof(g_ring_buffers[i]);
        g_request_rings[i].fd = -1;
        atomic_init(&g_request_rings[i].head, 0);
        atomic_init(&g_request_rings[i].tail, 0);
        atomic_init(&g_request_rings[i].refcount, 1);
    }
}

int ring_buffer_mgr_init(size_t workers_per_node, size_t ring_buffer_size) {
    (void)ring_buffer_size;
    reset_test_rings(workers_per_node);
    return C_OK;
}

void ring_buffer_mgr_shutdown(void) {
    g_ring_count = 0;
}

int ring_buffer_mgr_ensure_supernodes(size_t num_supernodes) {
    (void)num_supernodes;
    return C_OK;
}

ring_buffer_t *ring_buffer_mgr_get_request(int supernode_id, int worker_id) {
    (void)supernode_id;
    if (worker_id < 0 || (size_t)worker_id >= g_ring_count) return NULL;
    return &g_request_rings[worker_id];
}

ring_buffer_t *ring_buffer_mgr_get_response(int supernode_id, int worker_id) {
    (void)supernode_id;
    (void)worker_id;
    return NULL;
}

int batch_latency_trace_init(void) {
    return C_OK;
}

void batch_latency_trace_cleanup(void) {
}

int batch_latency_trace_begin(uint64_t batch_id,
                              uint32_t op_type,
                              uint32_t num_requests,
                              uint64_t proxy_batch_wait_total_us,
                              uint64_t proxy_batch_wait_max_us,
                              uint64_t proxy_flush_us,
                              const batch_latency_trace_proxy_meta_t *proxy_meta) {
    (void)batch_id;
    (void)op_type;
    (void)num_requests;
    (void)proxy_batch_wait_total_us;
    (void)proxy_batch_wait_max_us;
    (void)proxy_flush_us;
    (void)proxy_meta;
    return C_OK;
}

int batch_latency_trace_record_request_completion(uint64_t batch_id,
                                                  uint64_t result_queue_us,
                                                  uint64_t request_e2e_us) {
    (void)batch_id;
    (void)result_queue_us;
    (void)request_e2e_us;
    return C_OK;
}

sds batch_latency_trace_dump_recent(const char *title, size_t limit) {
    (void)title;
    (void)limit;
    return sdsempty();
}

sds sdsnewlen(const void *init, size_t initlen) {
    char *out = zmalloc(initlen + 1);
    if (!out) return NULL;
    if (init && initlen > 0) memcpy(out, init, initlen);
    out[initlen] = '\0';
    return out;
}

sds sdscatsds(sds s, const sds t) {
    return sdscat(s, t);
}

ub_vector_set_meta_t *ub_metadata_get_set(const char *key) {
    (void)key;
    return NULL;
}

int ub_metadata_lookup_row(ub_vector_set_meta_t *set, const char *element, uint64_t *row_id) {
    (void)set;
    (void)element;
    if (row_id) *row_id = 0;
    return C_ERR;
}

#include "../src/proxy_aggregator.c"

static proxy_vector_request_t make_vemb(uint64_t request_id, uint64_t row_id) {
    proxy_vector_request_t req = {0};
    req.op_type = PROXY_VECTOR_OP_VEMB;
    req.request_id = request_id;
    req.row_id = row_id;
    req.submit_time_us = getMonotonicUs();
    return req;
}

static fc_vemb_packet_t *read_fc_packet(size_t worker, size_t *payload_len) {
    void *payload = NULL;
    assert(worker < g_ring_count);
    assert(ring_buffer_peek(&g_request_rings[worker], &payload, payload_len) == C_OK);
    return payload;
}

static void setup_proxy(size_t workers, size_t batch_limit) {
    test_runtime_reset_server();
    server.supernode_workers = (int)workers;
    server.proxy.batch_limit = batch_limit;
    server.proxy.vemb_fc_slots = 0;
    server.proxy.vemb_fc_max_scan = 0;
    assert(proxy_aggregator_init(1) == C_OK);
}

static void test_fc_single_submit(void) {
    setup_proxy(1, 32);

    proxy_vector_request_t req = make_vemb(1, 0);
    assert(proxy_enqueue_vector_request("key", &req) == C_OK);
    assert(req.batch_id != 0);

    size_t payload_len = 0;
    fc_vemb_packet_t *packet = read_fc_packet(0, &payload_len);
    assert(payload_len == sizeof(fc_vemb_packet_t) + sizeof(packet->requests[0]));
    assert(packet->hdr.magic == BATCH_PACKET_MAGIC);
    assert(packet->hdr.num_requests == 1);
    assert(packet->requests[0].row_id == 0);
    assert(packet->requests[0].owner == &req);
    assert(ring_buffer_commit_read(&g_request_rings[0], payload_len) == C_OK);

    sds stats = proxy_aggregator_get_stats();
    assert(strstr(stats, "Combine rounds: 1") != NULL);
    assert(strstr(stats, "Average FC batch size: 1.00") != NULL);
    sdsfree(stats);
    proxy_aggregator_shutdown();
}

static void test_fc_drain_combines_two(void) {
    setup_proxy(1, 32);
    proxy_fc_board_t *board = &proxy->boards[0];
    atomic_store_explicit(&board->combiner_lock, 1, memory_order_release);

    proxy_vector_request_t req1 = make_vemb(1, 0);
    proxy_vector_request_t req2 = make_vemb(2, 0);
    assert(proxy_enqueue_vector_request("key", &req1) == C_OK);
    assert(proxy_enqueue_vector_request("key", &req2) == C_OK);
    assert(req1.batch_id == 0);
    assert(req2.batch_id == 0);
    assert(atomic_load_explicit(&board->pending_count, memory_order_acquire) == 2);

    atomic_store_explicit(&board->combiner_lock, 0, memory_order_release);
    assert(proxy_fc_try_combine(board, board->slot_count) == C_OK);
    assert(req1.batch_id != 0);
    assert(req2.batch_id == req1.batch_id);

    size_t payload_len = 0;
    fc_vemb_packet_t *packet = read_fc_packet(0, &payload_len);
    assert(payload_len == sizeof(fc_vemb_packet_t) +
                          sizeof(packet->requests[0]) * 2);
    assert(packet->hdr.num_requests == 2);
    assert(ring_buffer_commit_read(&g_request_rings[0], payload_len) == C_OK);

    sds stats = proxy_aggregator_get_stats();
    assert(strstr(stats, "Combine rounds: 1") != NULL);
    assert(strstr(stats, "Combined requests: 2") != NULL);
    assert(strstr(stats, "Average FC batch size: 2.00") != NULL);
    sdsfree(stats);
    proxy_aggregator_shutdown();
}

static void test_vsim_removed(void) {
    setup_proxy(1, 32);
    float *query = zmalloc(sizeof(float) * 4);
    assert(proxy_submit_vsim(NULL, NULL, query, 4, 10, 0) == REDISMODULE_OK);
    proxy_aggregator_shutdown();
}

int main(void) {
    test_fc_single_submit();
    test_fc_drain_combines_two();
    test_vsim_removed();
    printf("proxy_aggregator_ut: all tests passed\n");
    return 0;
}
