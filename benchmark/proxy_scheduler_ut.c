#include <assert.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

#include "../src/server.h"

struct redisServer server = {0};

void _serverLog(int level, const char *fmt, ...) {
    (void)level;
    (void)fmt;
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
    va_start(ap, fmt);
    char buf[1024];
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    return sdscat(s, buf);
}

#include "../src/consistent_hash.c"
#include "../src/proxy_router.c"
#include "../src/proxy_flush_scheduler.c"
#include "../src/proxy_batch_bucket.c"
#include "../src/proxy_active_bucket_heap.c"
#include "../src/ring_buffer.c"
#include "../src/proxy_flush_executor.c"
#include "../src/proxy_aggregator.c"

static proxy_aggregator_t *create_test_aggregator(size_t num_buckets, uint64_t time_limit_us) {
    proxy_aggregator_t *agg = zcalloc(sizeof(*agg));
    assert(agg != NULL);

    agg->num_buckets = num_buckets;
    agg->config.time_limit_us = time_limit_us;
    assert(pthread_mutex_init(&agg->active_buckets_lock, NULL) == 0);
    assert(pthread_cond_init(&agg->active_buckets_cond, NULL) == 0);
    agg->active_buckets_lock_initialized = 1;
    agg->active_buckets_cond_initialized = 1;
    agg->buckets = proxy_batch_bucket_create_array(num_buckets);
    assert(agg->buckets != NULL);
    assert(proxy_active_bucket_heap_init(&agg->active_bucket_heap, agg->buckets,
                                         num_buckets, time_limit_us) == C_OK);

    for (size_t i = 0; i < num_buckets; i++) {
        assert(proxy_batch_bucket_init(&agg->buckets[i], 4, (int)i, 0, 1000 + i) == C_OK);
    }

    return agg;
}

static void destroy_test_aggregator(proxy_aggregator_t *agg) {
    if (!agg) return;
    if (agg->buckets) {
        proxy_batch_bucket_destroy_array(agg->buckets, agg->num_buckets);
    }
    if (agg->active_buckets_lock_initialized) {
        pthread_mutex_destroy(&agg->active_buckets_lock);
    }
    if (agg->active_buckets_cond_initialized) {
        pthread_cond_destroy(&agg->active_buckets_cond);
    }
    proxy_active_bucket_heap_cleanup(&agg->active_bucket_heap);
    zfree(agg);
}

static void test_active_bucket_heap_order(void) {
    proxy_aggregator_t *agg = create_test_aggregator(3, 100);

    agg->buckets[0].last_flush_time_us = 1000;
    agg->buckets[1].last_flush_time_us = 500;
    agg->buckets[2].last_flush_time_us = 800;

    proxy_active_bucket_add(agg, 0);
    proxy_active_bucket_add(agg, 1);
    proxy_active_bucket_add(agg, 2);

    assert(agg->active_bucket_heap.count == 3);
    assert(agg->active_bucket_heap.indices[0] == 1);

    proxy_active_bucket_remove(agg, 1);
    assert(agg->active_bucket_heap.count == 2);
    assert(agg->active_bucket_heap.indices[0] == 2);

    proxy_active_bucket_add(agg, 2);
    assert(agg->active_bucket_heap.count == 2);

    destroy_test_aggregator(agg);
}

typedef struct wait_test_ctx {
    proxy_aggregator_t *agg;
    atomic_int woke;
} wait_test_ctx_t;

static void *wait_thread_main(void *arg) {
    wait_test_ctx_t *ctx = arg;
    proxy_flush_thread_wait(ctx->agg, 0, 0, UINT64_MAX);
    atomic_store(&ctx->woke, 1);
    return NULL;
}

static void test_wait_wakeup_on_new_bucket(void) {
    proxy_aggregator_t *agg = create_test_aggregator(1, 100);
    wait_test_ctx_t ctx = {.agg = agg};
    pthread_t tid;

    atomic_init(&ctx.woke, 0);
    agg->running = 1;
    assert(pthread_create(&tid, NULL, wait_thread_main, &ctx) == 0);

    struct timespec ts = {0, 5 * 1000 * 1000};
    nanosleep(&ts, NULL);

    proxy_active_bucket_add(agg, 0);
    pthread_join(tid, NULL);
    assert(atomic_load(&ctx.woke) == 1);

    destroy_test_aggregator(agg);
}

int main(void) {
    test_active_bucket_heap_order();
    test_wait_wakeup_on_new_bucket();
    printf("proxy_scheduler_ut: all tests passed\n");
    return 0;
}
