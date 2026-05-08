#include <assert.h>
#include <stdarg.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "../src/server.h"

struct redisServer server = {0};

void _serverLog(int level, const char *fmt, ...) {
    va_list ap;
    (void)level;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    fputc('\n', stderr);
    va_end(ap);
}

void serverLogFromHandler(int level, const char *fmt, ...) {
    (void)level;
    (void)fmt;
}

long long ustime(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long)tv.tv_sec * 1000000LL + tv.tv_usec;
}

mstime_t mstime(void) {
    return ustime() / 1000;
}

void *zmalloc(size_t size) {
    return malloc(size ? size : 1);
}

void *zcalloc(size_t size) {
    return calloc(1, size ? size : 1);
}

void zfree(void *ptr) {
    free(ptr);
}

char *zstrdup(const char *s) {
    if (!s) return NULL;
    size_t len = strlen(s) + 1;
    char *out = malloc(len);
    if (!out) return NULL;
    memcpy(out, s, len);
    return out;
}

sds sdsempty(void) {
    return zstrdup("");
}

sds sdscat(sds s, const char *t) {
    size_t slen = strlen(s);
    size_t tlen = strlen(t);
    s = realloc(s, slen + tlen + 1);
    assert(s != NULL);
    memcpy(s + slen, t, tlen + 1);
    return s;
}

sds sdscatprintf(sds s, const char *fmt, ...) {
    va_list ap;
    char buf[2048];
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    return sdscat(s, buf);
}

void sdsfree(sds s) {
    free(s);
}

#include "../src/consistent_hash.c"
#include "../src/proxy_router.c"
#include "../src/proxy_flush_scheduler.c"
#include "../src/proxy_batch_bucket.c"
#include "../src/ring_buffer.c"
#include "../src/proxy_flush_executor.c"
#include "../src/proxy_aggregator.c"

static void reset_server_proxy(int workers, int batch_limit, int time_limit_us, int max_supernodes) {
    memset(&server, 0, sizeof(server));
    server.supernode_workers = workers;
    server.proxy.batch_limit = batch_limit;
    server.proxy.time_limit_us = time_limit_us;
    server.proxy.max_supernodes = max_supernodes;
}

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

static void setup_test_proxy(int workers, int batch_limit, int time_limit_us, int max_supernodes) {
    reset_server_proxy(workers, batch_limit, time_limit_us, max_supernodes);
    proxy = zcalloc(sizeof(*proxy));
    assert(proxy != NULL);

    proxy_aggregator_config_init(&proxy->config);
    proxy->num_supernodes = (size_t)max_supernodes;
    proxy->num_buckets = proxy->num_supernodes * proxy->config.workers_per_node;
    proxy->buckets = proxy_batch_bucket_create_array(proxy->num_buckets);
    assert(proxy->buckets != NULL);
    proxy->ring_buffers = zcalloc(sizeof(ring_buffer_t *) * proxy->num_buckets);
    assert(proxy->ring_buffers != NULL);

    assert(pthread_mutex_init(&proxy->active_buckets_lock, NULL) == 0);
    assert(pthread_cond_init(&proxy->active_buckets_cond, NULL) == 0);
    proxy->active_buckets_lock_initialized = 1;
    proxy->active_buckets_cond_initialized = 1;
    proxy->active_bucket_indices = zcalloc(sizeof(size_t) * proxy->num_buckets);
    proxy->active_bucket_slots = zcalloc(sizeof(size_t) * proxy->num_buckets);
    proxy->active_bucket_registered = zcalloc(sizeof(uint8_t) * proxy->num_buckets);
    assert(proxy->active_bucket_indices && proxy->active_bucket_slots && proxy->active_bucket_registered);

    proxy_flush_executor_init(&proxy->executor);
    flush_scheduler_init(&proxy->scheduler, proxy->config.batch_limit, proxy->config.time_limit_us);
    assert(proxy_router_init(&proxy->router, (int)proxy->num_supernodes, proxy->config.workers_per_node) == C_OK);
    atomic_init(&proxy->total_requests, 0);

    for (size_t sn = 0; sn < proxy->num_supernodes; sn++) {
        for (size_t worker = 0; worker < proxy->config.workers_per_node; worker++) {
            size_t idx = worker_queue_index(proxy->config.workers_per_node, (int)sn, (int)worker);
            assert(proxy_batch_bucket_init(&proxy->buckets[idx], proxy->config.batch_limit,
                                           (int)sn, (int)worker, ustime()) == C_OK);
            proxy->ring_buffers[idx] = test_rb_create(1 << 20);
        }
    }
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
        proxy_batch_bucket_destroy_array(proxy->buckets, proxy->num_buckets);
    }
    if (proxy->ring_buffers) {
        for (size_t i = 0; i < proxy->num_buckets; i++) {
            test_rb_destroy(proxy->ring_buffers[i]);
        }
        zfree(proxy->ring_buffers);
    }
    proxy_router_cleanup(&proxy->router);
    if (proxy->active_buckets_lock_initialized) pthread_mutex_destroy(&proxy->active_buckets_lock);
    if (proxy->active_buckets_cond_initialized) pthread_cond_destroy(&proxy->active_buckets_cond);
    zfree(proxy->active_bucket_indices);
    zfree(proxy->active_bucket_slots);
    zfree(proxy->active_bucket_registered);
    zfree(proxy);
    proxy = NULL;
}

static void start_test_flush_thread(void) {
    assert(proxy != NULL);
    proxy->running = 1;
    assert(pthread_create(&proxy->flush_thread, NULL, flush_thread_func, proxy) == 0);
}

static void test_immediate_flush_success(void) {
    setup_test_proxy(1, 1, 1000000, 1);

    ring_buffer_t *rb = proxy_aggregator_get_worker_rb(0, 0);
    assert(rb != NULL);
    assert(proxy_enqueue_request("alpha", NULL, NULL, 0) == C_OK);

    void *payload = NULL;
    size_t payload_len = 0;
    assert(ring_buffer_peek(rb, &payload, &payload_len) == C_OK);
    batch_packet_t *packet = payload;
    assert(packet->magic == BATCH_PACKET_MAGIC);
    assert(packet->num_requests == 1);
    assert(packet->supernode_id == 0);
    assert(packet->worker_id == 0);
    assert(ring_buffer_commit_read(rb, payload_len) == C_OK);

    sds stats = proxy_aggregator_get_stats();
    assert(strstr(stats, "Total requests: 1") != NULL);
    assert(strstr(stats, "Total flushes: 1") != NULL);
    assert(strstr(stats, "Immediate flush successes: 1") != NULL);
    sdsfree(stats);

    teardown_test_proxy();
}

static void test_immediate_deferred_and_rejected(void) {
    setup_test_proxy(1, 1, 1000000, 1);

    ring_buffer_t *rb = proxy_aggregator_get_worker_rb(0, 0);
    assert(rb != NULL);

    void *payload = NULL;
    assert(ring_buffer_reserve(rb, 8, &payload) == C_OK);

    assert(proxy_enqueue_request("alpha", NULL, NULL, 0) == C_OK);
    assert(proxy_enqueue_request("beta", NULL, NULL, 0) == C_ERR);

    sds stats = proxy_aggregator_get_stats();
    assert(strstr(stats, "Total requests: 1") != NULL);
    assert(strstr(stats, "Immediate flush attempts: 2") != NULL);
    assert(strstr(stats, "Immediate flush deferred: 1") != NULL);
    assert(strstr(stats, "Enqueue rejections (full): 1") != NULL);
    sdsfree(stats);

    assert(ring_buffer_cancel_write(rb) == C_OK);
    teardown_test_proxy();
}

typedef struct enqueue_thread_ctx {
    int iters;
    const char *key;
} enqueue_thread_ctx_t;

static void *enqueue_thread_main(void *arg) {
    enqueue_thread_ctx_t *ctx = arg;
    for (int i = 0; i < ctx->iters; i++) {
        int ret = proxy_enqueue_request(ctx->key, NULL, NULL, 0);
        assert(ret == C_OK);
    }
    return NULL;
}

static void test_concurrent_enqueue_with_background_flush(void) {
    const int num_threads = 4;
    const int iters_per_thread = 32;
    pthread_t tids[num_threads];
    enqueue_thread_ctx_t ctx = {.iters = iters_per_thread, .key = "shared-key"};

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

int main(void) {
    test_immediate_flush_success();
    test_immediate_deferred_and_rejected();
    test_concurrent_enqueue_with_background_flush();
    printf("proxy_aggregator_ut: all tests passed\n");
    return 0;
}
