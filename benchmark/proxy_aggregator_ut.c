#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "test_runtime_shim.h"

#include "../src/consistent_hash.c"
#include "../src/proxy_router.c"
#include "../src/proxy_flush_scheduler.c"
#include "../src/proxy_batch_bucket.c"
#include "../src/proxy_active_bucket_heap.c"
#include "../src/ring_buffer.c"
#include "../src/proxy_flush_executor.c"

typedef struct RedisModuleCtx RedisModuleCtx;
typedef struct RedisModuleBlockedClient RedisModuleBlockedClient;
typedef struct ub_vector_set_meta ub_vector_set_meta_t;
typedef int (*RedisModuleCmdFunc)(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

static int test_RedisModule_UnblockClient(RedisModuleBlockedClient *bc, void *privdata) {
    (void)bc;
    (void)privdata;
    return REDISMODULE_OK;
}

static void *test_RedisModule_GetBlockedClientPrivateData(RedisModuleCtx *ctx) {
    (void)ctx;
    return NULL;
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

static int test_RedisModule_ReplyWithEmptyArray(RedisModuleCtx *ctx) {
    (void)ctx;
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
#define RedisModule_ReplyWithEmptyArray test_RedisModule_ReplyWithEmptyArray
#define RedisModule_StringPtrLen test_RedisModule_StringPtrLen
#define RedisModule_BlockClient test_RedisModule_BlockClient
#define RedisModule_AbortBlock test_RedisModule_AbortBlock
#define RedisModule_BlockClientSetPrivateData test_RedisModule_BlockClientSetPrivateData
#define RedisModule_BlockedClientMeasureTimeStart test_RedisModule_BlockedClientMeasureTimeStart

static ring_buffer_t *test_rb_create(size_t size) {
    ring_buffer_t *rb = zcalloc(sizeof(*rb));
    assert(rb != NULL);
    rb->buffer = zcalloc(size);
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
    zfree(rb->buffer);
    zfree(rb);
}

int ring_buffer_mgr_init(size_t workers_per_node, size_t ring_buffer_size) {
    (void)workers_per_node;
    (void)ring_buffer_size;
    return C_OK;
}

void ring_buffer_mgr_shutdown(void) {
}

int ring_buffer_mgr_ensure_supernodes(size_t num_supernodes) {
    (void)num_supernodes;
    return C_OK;
}

ring_buffer_t *ring_buffer_mgr_get_request(int supernode_id, int worker_id) {
    (void)supernode_id;
    (void)worker_id;
    return NULL;
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

size_t ub_metadata_collect_rows(ub_vector_set_meta_t *set,
                                uint64_t **rows_out,
                                sds **elements_out) {
    (void)set;
    if (rows_out) *rows_out = NULL;
    if (elements_out) *elements_out = NULL;
    return 0;
}

int vector_proxy_completion_register(proxy_vector_request_t *req) {
    (void)req;
    return C_OK;
}

proxy_vector_request_t *vector_proxy_completion_lookup(uint64_t request_id) {
    (void)request_id;
    return NULL;
}

int vector_proxy_completion_complete_vemb(uint64_t request_id,
                                          const float *vector,
                                          size_t dim,
                                          int error_code) {
    (void)request_id;
    (void)vector;
    (void)dim;
    (void)error_code;
    return C_ERR;
}

int vector_proxy_completion_complete_vsim(uint64_t request_id,
                                          const uint64_t *row_ids,
                                          const float *scores,
                                          size_t num_results,
                                          int error_code) {
    (void)request_id;
    (void)row_ids;
    (void)scores;
    (void)num_results;
    (void)error_code;
    return C_ERR;
}

int vector_proxy_completion_take(uint64_t request_id, proxy_vector_request_t **req) {
    (void)request_id;
    if (req) *req = NULL;
    return C_ERR;
}

proxy_vector_request_t *proxy_vector_request_create_vemb(uint64_t request_id,
                                                         uint64_t row_id,
                                                         int raw_output,
                                                         RedisModuleBlockedClient *bc) {
    proxy_vector_request_t *req = zcalloc(sizeof(*req));
    if (!req) return NULL;
    req->op_type = PROXY_VECTOR_OP_VEMB;
    req->request_id = request_id;
    req->row_id = row_id;
    req->raw_output = raw_output;
    req->bc = bc;
    req->submit_time_us = getMonotonicUs();
    return req;
}

proxy_vector_request_t *proxy_vector_request_create_vsim(uint64_t request_id,
                                                         float *query_vector,
                                                         size_t query_dim,
                                                         uint64_t *candidate_rows,
                                                         sds *candidate_elements,
                                                         size_t candidate_count,
                                                         size_t requested_count,
                                                         int withscores,
                                                         RedisModuleBlockedClient *bc) {
    proxy_vector_request_t *req = zcalloc(sizeof(*req));
    if (!req) return NULL;
    req->op_type = PROXY_VECTOR_OP_VSIM;
    req->request_id = request_id;
    req->query_vector = query_vector;
    req->query_dim = query_dim;
    req->candidate_rows = candidate_rows;
    req->candidate_elements = candidate_elements;
    req->candidate_count = candidate_count;
    req->requested_count = requested_count;
    req->withscores = withscores;
    req->bc = bc;
    req->submit_time_us = getMonotonicUs();
    return req;
}

void proxy_vector_request_free(proxy_vector_request_t *req) {
    zfree(req);
}

#include "../src/proxy_aggregator.c"

static void reset_server_proxy(int workers, int batch_limit, int time_limit_us, int max_supernodes) {
    test_runtime_reset_server();
    server.supernode_workers = workers;
    server.proxy.batch_limit = batch_limit;
    server.proxy.time_limit_us = time_limit_us;
    server.proxy.max_supernodes = max_supernodes;
    server.proxy.vemb_submit_mode = PROXY_VEMB_SUBMIT_MODE_ADAPTIVE;
    server.proxy.vemb_adaptive = 1;
    server.proxy.vemb_direct_gap_us = 20;
    server.proxy.vemb_fc_slots = 0;
    server.proxy.vemb_fc_max_scan = 0;
}

static void setup_test_proxy(int workers, int batch_limit, int time_limit_us, int max_supernodes) {
    reset_server_proxy(workers, batch_limit, time_limit_us, max_supernodes);
    proxy = zcalloc(sizeof(*proxy));
    assert(proxy != NULL);

    proxy_aggregator_config_init(&proxy->config);
    proxy->num_supernodes = (size_t)max_supernodes;
    proxy->num_buckets = proxy->num_supernodes * proxy->config.workers_per_node;
    proxy->buckets = proxy_batch_bucket_create_array(proxy->num_buckets);
    assert(proxy->buckets != NULL);
    proxy->vemb_fc_boards = zcalloc(sizeof(proxy_vemb_fc_board_t) * proxy->num_buckets);
    assert(proxy->vemb_fc_boards != NULL);

    assert(pthread_mutex_init(&proxy->active_buckets_lock, NULL) == 0);
    assert(pthread_cond_init(&proxy->active_buckets_cond, NULL) == 0);
    proxy->active_buckets_lock_initialized = 1;
    proxy->active_buckets_cond_initialized = 1;

    proxy_flush_executor_init(&proxy->executor);
    flush_scheduler_init(&proxy->scheduler, proxy->config.batch_limit, proxy->config.time_limit_us);
    assert(proxy_router_init(&proxy->router, (int)proxy->num_supernodes, proxy->config.workers_per_node) == C_OK);
    atomic_init(&proxy->total_requests, 0);

    for (size_t sn = 0; sn < proxy->num_supernodes; sn++) {
        for (size_t worker = 0; worker < proxy->config.workers_per_node; worker++) {
            size_t idx = worker_queue_index(proxy->config.workers_per_node, (int)sn, (int)worker);
            assert(proxy_batch_bucket_init(&proxy->buckets[idx], proxy->config.batch_limit,
                                           (int)sn, (int)worker, ustime()) == C_OK);
            proxy->buckets[idx].rb = test_rb_create(1 << 20);
            assert(proxy->buckets[idx].rb != NULL);
            assert(proxy_vemb_fc_board_init(&proxy->vemb_fc_boards[idx],
                                            proxy_vemb_fc_default_slot_count(proxy),
                                            idx) == C_OK);
        }
    }
    assert(proxy_active_bucket_heap_init(&proxy->active_bucket_heap,
                                         proxy->buckets,
                                         proxy->num_buckets,
                                         proxy->config.time_limit_us) == C_OK);
}

static void teardown_test_proxy(void) {
    if (!proxy) return;
    if (proxy->running) {
        pthread_mutex_lock(&proxy->active_buckets_lock);
        proxy->running = 0;
        if (proxy->active_buckets_cond_initialized) {
            pthread_cond_signal(&proxy->active_buckets_cond);
        }
        pthread_mutex_unlock(&proxy->active_buckets_lock);
        pthread_join(proxy->flush_thread, NULL);
    }
    if (proxy->buckets) {
        for (size_t i = 0; i < proxy->num_buckets; i++) {
            test_rb_destroy(proxy->buckets[i].rb);
            proxy->buckets[i].rb = NULL;
        }
        proxy_batch_bucket_destroy_array(proxy->buckets, proxy->num_buckets);
    }
    if (proxy->vemb_fc_boards) {
        for (size_t i = 0; i < proxy->num_buckets; i++) {
            proxy_vemb_fc_board_cleanup(&proxy->vemb_fc_boards[i]);
        }
        zfree(proxy->vemb_fc_boards);
    }
    proxy_router_cleanup(&proxy->router);
    if (proxy->active_buckets_lock_initialized) pthread_mutex_destroy(&proxy->active_buckets_lock);
    if (proxy->active_buckets_cond_initialized) pthread_cond_destroy(&proxy->active_buckets_cond);
    proxy_active_bucket_heap_cleanup(&proxy->active_bucket_heap);
    zfree(proxy);
    proxy = NULL;
}

static void start_test_flush_thread(void) {
    assert(proxy != NULL);
    proxy->running = 1;
    assert(pthread_create(&proxy->flush_thread, NULL, flush_thread_func, proxy) == 0);
}

static ring_buffer_t *test_bucket_rb(size_t supernode_id, size_t worker_id) {
    size_t idx = worker_queue_index(proxy->config.workers_per_node,
                                    (int)supernode_id,
                                    (int)worker_id);
    assert(idx < proxy->num_buckets);
    return proxy->buckets[idx].rb;
}

static proxy_vector_request_t test_vemb_owner(uint64_t request_id,
                                              uint64_t row_id) {
    proxy_vector_request_t owner = {0};
    owner.op_type = PROXY_VECTOR_OP_VEMB;
    owner.request_id = request_id;
    owner.row_id = row_id;
    return owner;
}

static void test_direct_vemb_flush_success(void) {
    setup_test_proxy(1, 1, 1000000, 1);

    ring_buffer_t *rb = test_bucket_rb(0, 0);
    assert(rb != NULL);
    proxy_vector_request_t owner = test_vemb_owner(1, 0);
    assert(proxy_enqueue_vector_request("alpha", &owner) == C_OK);

    void *payload = NULL;
    size_t payload_len = 0;
    assert(ring_buffer_peek(rb, &payload, &payload_len) == C_OK);
    batch_packet_t *packet = payload;
    assert(packet->hdr.magic == BATCH_PACKET_MAGIC);
    assert(packet->hdr.num_requests == 1);
    assert(packet->hdr.supernode_id == 0);
    assert(packet->hdr.worker_id == 0);
    assert(packet->requests[0].request_id == 1);
    assert(packet->requests[0].row_id == 0);
    assert(ring_buffer_commit_read(rb, payload_len) == C_OK);

    sds stats = proxy_aggregator_get_stats();
    assert(strstr(stats, "Total requests: 1") != NULL);
    assert(strstr(stats, "Total flushes: 1") != NULL);
    assert(strstr(stats, "Immediate flush successes: 1") != NULL);
    sdsfree(stats);

    teardown_test_proxy();
}

static void test_direct_vemb_ring_full_rejected(void) {
    setup_test_proxy(1, 1, 1000000, 1);
    proxy->config.vemb_submit_mode = PROXY_VEMB_SUBMIT_MODE_DIRECT;

    ring_buffer_t *rb = test_bucket_rb(0, 0);
    assert(rb != NULL);

    void *payload = NULL;
    assert(ring_buffer_reserve(rb, rb->size - 16, &payload) == C_OK);

    proxy_vector_request_t owner = test_vemb_owner(1, 0);
    assert(proxy_enqueue_vector_request("alpha", &owner) == C_ERR);

    sds stats = proxy_aggregator_get_stats();
    assert(strstr(stats, "Total requests: 0") != NULL);
    assert(strstr(stats, "Immediate flush attempts: 1") != NULL);
    assert(strstr(stats, "Enqueue rejections (full): 1") != NULL);
    sdsfree(stats);

    assert(ring_buffer_cancel_write(rb) == C_OK);
    teardown_test_proxy();
}

typedef struct enqueue_thread_ctx {
    int iters;
    const char *key;
    atomic_uint_fast64_t next_id;
} enqueue_thread_ctx_t;

static void *enqueue_thread_main(void *arg) {
    enqueue_thread_ctx_t *ctx = arg;
    for (int i = 0; i < ctx->iters; i++) {
        uint64_t request_id =
            atomic_fetch_add_explicit(&ctx->next_id, 1, memory_order_relaxed);
        proxy_vector_request_t *owner = zcalloc(sizeof(*owner));
        assert(owner != NULL);
        *owner = test_vemb_owner(request_id, request_id);
        int ret = proxy_enqueue_vector_request(ctx->key, owner);
        assert(ret == C_OK);
    }
    return NULL;
}

static void test_concurrent_enqueue_with_background_flush(void) {
    const int num_threads = 4;
    const int iters_per_thread = 32;
    pthread_t tids[num_threads];
    enqueue_thread_ctx_t ctx = {.iters = iters_per_thread, .key = "shared-key"};
    atomic_init(&ctx.next_id, 1);

    setup_test_proxy(1, 64, 200, 1);
    start_test_flush_thread();

    for (int i = 0; i < num_threads; i++) {
        assert(pthread_create(&tids[i], NULL, enqueue_thread_main, &ctx) == 0);
    }
    for (int i = 0; i < num_threads; i++) {
        pthread_join(tids[i], NULL);
    }

    /* Give the flush thread time to drain remaining active buckets. */
    struct timespec ts = {0, 20 * 1000 * 1000};
    nanosleep(&ts, NULL);

    sds stats = proxy_aggregator_get_stats();
    char needle[64];
    snprintf(needle, sizeof(needle), "Total requests: %d", num_threads * iters_per_thread);
    assert(strstr(stats, needle) != NULL);
    assert(strstr(stats, "Total flushes:") != NULL);
    assert(strstr(stats, "Active bucket peak:") != NULL);
    sdsfree(stats);

    teardown_test_proxy();
}

static void test_vemb_fc_drain_pending(void) {
    setup_test_proxy(1, 64, 200, 1);
    proxy->config.vemb_submit_mode = PROXY_VEMB_SUBMIT_MODE_FC;

    proxy_vemb_fc_board_t *board = &proxy->vemb_fc_boards[0];
    atomic_store_explicit(&board->combiner_lock, 1, memory_order_release);
    proxy_vector_request_t owner = test_vemb_owner(1, 0);
    assert(proxy_enqueue_vector_request("alpha", &owner) == C_OK);
    assert(atomic_load_explicit(&board->pending_count, memory_order_acquire) == 1);
    assert(owner.batch_id == 0);

    atomic_store_explicit(&board->combiner_lock, 0, memory_order_release);
    assert(proxy_vemb_fc_drain_pending(proxy, NULL) == 1);
    assert(atomic_load_explicit(&board->pending_count, memory_order_acquire) == 0);
    assert(owner.batch_id != 0);

    ring_buffer_t *rb = test_bucket_rb(0, 0);
    void *payload = NULL;
    size_t payload_len = 0;
    assert(ring_buffer_peek(rb, &payload, &payload_len) == C_OK);
    batch_packet_t *packet = payload;
    assert(packet->hdr.magic == BATCH_PACKET_MAGIC);
    assert(packet->hdr.num_requests == 1);
    assert(packet->requests[0].request_id == 1);
    assert(packet->requests[0].row_id == 0);
    assert(ring_buffer_commit_read(rb, payload_len) == C_OK);

    sds stats = proxy_aggregator_get_stats();
    assert(strstr(stats, "VEMB FC published: 1") != NULL);
    assert(strstr(stats, "VEMB FC combines: 1") != NULL);
    assert(strstr(stats, "VEMB FC combined requests: 1") != NULL);
    sdsfree(stats);

    teardown_test_proxy();
}

int main(void) {
    test_direct_vemb_flush_success();
    test_direct_vemb_ring_full_rejected();
    test_concurrent_enqueue_with_background_flush();
    test_vemb_fc_drain_pending();
    printf("proxy_aggregator_ut: all tests passed\n");
    return 0;
}
