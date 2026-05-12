/*
 * Proxy Aggregator Implementation
 * 智能批量聚合器 - 蓄水池策略
 */

#include "proxy_aggregator.h"
#include "macro.h"
#include "server.h"
#include "proxy_batch_bucket.h"
#include "proxy_active_bucket_heap.h"
#include "proxy_flush_scheduler.h"
#include "proxy_flush_executor.h"
#include "proxy_router.h"
#include "ring_buffer_mgr.h"

#include <stddef.h>
#include <string.h>
#include <time.h>
#include <stdatomic.h>
#include <stdlib.h>

typedef struct proxy_aggregator_config {
    size_t batch_limit;
    uint64_t time_limit_us;
    size_t max_supernodes;
    size_t workers_per_node;
} proxy_aggregator_config_t;

typedef struct proxy_aggregator {
    proxy_batch_bucket_t *buckets; // 批量桶 - 每个 (supernode, worker) 一个 
    size_t num_buckets;
    size_t num_supernodes;

    /* 运行时配置 */
    proxy_aggregator_config_t config;

    /* 一致性哈希环 */
    proxy_router_t router;

    /* Flush 调度 */
    flush_scheduler_t scheduler;

    /* Flush 执行 */
    proxy_flush_executor_t executor;

    /* 活跃 bucket 的最小 deadline heap */
    proxy_active_bucket_heap_t active_bucket_heap;
    pthread_mutex_t active_buckets_lock;
    pthread_cond_t active_buckets_cond;
    int active_buckets_lock_initialized;
    int active_buckets_cond_initialized;

    /* 工作线程 */
    pthread_t flush_thread;
    int running;

    /* 统计信息 */
    atomic_uint_fast64_t total_requests;
    atomic_uint_fast64_t active_bucket_peak;
    atomic_uint_fast64_t active_wait_wakeups;
    atomic_uint_fast64_t timed_wait_wakeups;
    atomic_uint_fast64_t flush_retry_count;
} proxy_aggregator_t;

static proxy_aggregator_t *proxy = NULL;

/* 原子请求 ID 生成器 */
static atomic_uint_fast64_t next_request_id = 1;

static inline void proxy_aggregator_config_init(proxy_aggregator_config_t *cfg) {
    RETURN_IF(!cfg);
    cfg->batch_limit = server.proxy.batch_limit > 0 ? server.proxy.batch_limit : (size_t)PROXY_BATCH_LIMIT;
    cfg->time_limit_us = server.proxy.time_limit_us > 0 ? server.proxy.time_limit_us : (uint64_t)PROXY_TIME_LIMIT_US;
    cfg->max_supernodes = server.proxy.max_supernodes > 0 ? server.proxy.max_supernodes : (size_t)PROXY_MAX_SUPERNODES;
    cfg->workers_per_node = server.supernode_workers > 0 ?  server.supernode_workers : (size_t)max((int)sysconf(_SC_NPROCESSORS_ONLN), 1);
}

static inline size_t worker_queue_index(size_t workers_per_node,
                                        int supernode_id,
                                        int worker_id) {
    return (size_t)supernode_id * workers_per_node + (size_t)worker_id;
}

static void proxy_active_bucket_add(proxy_aggregator_t *agg, size_t bucket_index) {
    RETURN_IF(!agg || !agg->active_buckets_lock_initialized);

    pthread_mutex_lock(&agg->active_buckets_lock);
    size_t old_count = agg->active_bucket_heap.count;
    proxy_active_bucket_heap_add(&agg->active_bucket_heap, bucket_index);
    if (agg->active_bucket_heap.count > old_count &&
        agg->active_bucket_heap.count >
            atomic_load_explicit(&agg->active_bucket_peak, memory_order_relaxed)) {
        atomic_store_explicit(&agg->active_bucket_peak, agg->active_bucket_heap.count,
                              memory_order_relaxed);
    }
    pthread_cond_signal(&agg->active_buckets_cond);
    pthread_mutex_unlock(&agg->active_buckets_lock);
}

static void proxy_active_bucket_remove(proxy_aggregator_t *agg, size_t bucket_index) {
    RETURN_IF(!agg || !agg->active_buckets_lock_initialized);

    pthread_mutex_lock(&agg->active_buckets_lock);
    proxy_active_bucket_heap_remove(&agg->active_bucket_heap, bucket_index);
    pthread_mutex_unlock(&agg->active_buckets_lock);
}

static inline void proxy_flush_thread_idle_wait(int processed_any) {
    struct timespec ts = {0, processed_any ? 1000 : 10000};

    nanosleep(&ts, NULL);
}

static inline struct timespec proxy_timespec_from_abs_us(uint64_t abs_us) {
    struct timespec ts;
    ts.tv_sec = (time_t)(abs_us / 1000000);
    ts.tv_nsec = (long)((abs_us % 1000000) * 1000);
    return ts;
}

static void proxy_flush_thread_wait(proxy_aggregator_t *agg, int processed_any,
                                    int has_ready_bucket, uint64_t earliest_deadline_us) {
    RETURN_IF(!agg || !agg->active_buckets_lock_initialized);

    if (processed_any || has_ready_bucket) {
        proxy_flush_thread_idle_wait(1);
        return;
    }

    pthread_mutex_lock(&agg->active_buckets_lock);
    if (agg->running) {
        if (agg->active_bucket_heap.count == 0) {
            pthread_cond_wait(&agg->active_buckets_cond, &agg->active_buckets_lock);
            atomic_fetch_add_explicit(&agg->active_wait_wakeups, 1, memory_order_relaxed);
        } else if (earliest_deadline_us != UINT64_MAX) {
            struct timespec deadline_ts = proxy_timespec_from_abs_us(earliest_deadline_us);
            pthread_cond_timedwait(&agg->active_buckets_cond, &agg->active_buckets_lock, &deadline_ts);
            atomic_fetch_add_explicit(&agg->timed_wait_wakeups, 1, memory_order_relaxed);
        }
    }
    pthread_mutex_unlock(&agg->active_buckets_lock);
}

static void *flush_thread_func(void *arg) {
    proxy_aggregator_t *agg = (proxy_aggregator_t *)arg;
    
    serverLog(LL_NOTICE, "Proxy aggregator flush thread started");
    
    while (agg->running) {
        uint64_t current_time = ustime();
        int processed_any = 0;
        int has_ready_bucket = 0;
        uint64_t earliest_deadline_us = UINT64_MAX;

        pthread_mutex_lock(&agg->active_buckets_lock);
        size_t bucket_index = 0;
        (void)proxy_active_bucket_heap_peek(&agg->active_bucket_heap, &bucket_index, &earliest_deadline_us);
        pthread_mutex_unlock(&agg->active_buckets_lock);

        while (agg->running) {
            pthread_mutex_lock(&agg->active_buckets_lock);
            if (agg->active_bucket_heap.count == 0) {
                pthread_mutex_unlock(&agg->active_buckets_lock);
                break;
            }
            bucket_index = agg->active_bucket_heap.indices[0];
            pthread_mutex_unlock(&agg->active_buckets_lock);

            proxy_batch_bucket_t *bucket = &agg->buckets[bucket_index];
            
            pthread_mutex_lock(&bucket->mutex);
            size_t bucket_count = bucket->count;
            if (bucket_count > 0) {
                uint64_t diff = current_time - bucket->last_flush_time_us;
                flush_decision_t decision = flush_scheduler_on_poll(&agg->scheduler, bucket_count, diff);
                if (decision.should_flush) {
                    has_ready_bucket = 1;
                    if (proxy_executor_flush_bucket_locked(&agg->executor, bucket, bucket->rb,
                                                  decision.flush_reason_full,
                                                  current_time,
                                                  PROXY_FLUSH_TRIGGER_BACKGROUND) == C_OK) {
                        proxy_active_bucket_remove(agg, bucket_index);
                        processed_any = 1;
                        pthread_mutex_unlock(&bucket->mutex);
                        continue;
                    } else {
                        atomic_fetch_add_explicit(&agg->flush_retry_count, 1, memory_order_relaxed);
                    }
                } else if (agg->scheduler.time_limit_us > 0) {
                    // 最早应该醒来的绝对时间
                    uint64_t deadline_us = current_time + (agg->scheduler.time_limit_us - diff);
                    if (deadline_us < earliest_deadline_us) {
                        earliest_deadline_us = deadline_us;
                    }
                }
            } else {
                proxy_active_bucket_remove(agg, bucket_index);
            }
            
            pthread_mutex_unlock(&bucket->mutex);
            break;
        }
        
        proxy_flush_thread_wait(agg, processed_any, has_ready_bucket, earliest_deadline_us);
    }
    
    serverLog(LL_NOTICE, "Proxy aggregator flush thread stopped");
    return NULL;
}

/* ========== Proxy Aggregator API ========== */

/* 初始化 Proxy 聚合器 */
int proxy_aggregator_init(int num_supernodes) {
    RETURN_IF(proxy, C_OK);

    proxy = zcalloc(sizeof(proxy_aggregator_t));
    if (!proxy) return C_ERR;

    proxy_aggregator_config_init(&proxy->config);

    if (num_supernodes <= 0 || (size_t)num_supernodes > proxy->config.max_supernodes) {
        serverLog(LL_WARNING, "Invalid number of supernodes: %d", num_supernodes);
        goto failed;
    }

    proxy->num_supernodes = (size_t)num_supernodes;
    flush_scheduler_init(&proxy->scheduler,
                         proxy->config.batch_limit, proxy->config.time_limit_us);
    proxy_flush_executor_init(&proxy->executor);
    
    proxy->num_buckets =
        (size_t)num_supernodes * proxy->config.workers_per_node;
    if (pthread_mutex_init(&proxy->active_buckets_lock, NULL) != 0) {
        goto failed;
    }
    proxy->active_buckets_lock_initialized = 1;
    if (pthread_cond_init(&proxy->active_buckets_cond, NULL) != 0) {
        goto failed;
    }
    proxy->active_buckets_cond_initialized = 1;
    proxy->buckets =
        proxy_batch_bucket_create_array(proxy->num_buckets);
    if (!proxy->buckets) goto failed;
    if (proxy_active_bucket_heap_init(&proxy->active_bucket_heap,
                                      proxy->buckets,
                                      proxy->num_buckets,
                                      proxy->config.time_limit_us) != C_OK) {
        goto failed;
    }

    if (ring_buffer_mgr_init(proxy->config.workers_per_node,
                             RING_BUFFER_SIZE) != C_OK) {
        goto failed;
    }
    if (ring_buffer_mgr_ensure_supernodes((size_t)num_supernodes) != C_OK) {
        goto failed;
    }
    
    for (int sn = 0; sn < num_supernodes; sn++) {
        for (size_t worker = 0; worker < proxy->config.workers_per_node; worker++) {
            size_t idx = worker_queue_index(proxy->config.workers_per_node, sn, (int)worker);
            proxy_batch_bucket_t *bucket = &proxy->buckets[idx];
            int ret = proxy_batch_bucket_init(bucket, proxy->config.batch_limit, sn, (int)worker, ustime());
            if (ret != C_OK) {
                goto failed;
            }
            bucket->rb = ring_buffer_mgr_get(sn, (int)worker);
            if (!bucket->rb) {
                goto failed;
            }
        }
    }
    
    /* 初始化一致性哈希环 */
    if (proxy_router_init(&proxy->router, num_supernodes,
                          proxy->config.workers_per_node) != C_OK) {
        goto failed;
    }
    
    /* 初始化统计 */
    atomic_init(&proxy->total_requests, 0);
    atomic_init(&proxy->active_bucket_peak, 0);
    atomic_init(&proxy->active_wait_wakeups, 0);
    atomic_init(&proxy->timed_wait_wakeups, 0);
    atomic_init(&proxy->flush_retry_count, 0);
    
    /* 启动刷新线程 */
    proxy->running = 1;
    if (pthread_create(&proxy->flush_thread, NULL, flush_thread_func, proxy) != 0) {
        serverLog(LL_WARNING, "Failed to create flush thread");
        proxy->running = 0;
        goto failed;
    }
    
    serverLog(LL_NOTICE, "Proxy aggregator initialized with %d supernodes x %zu workers",
              num_supernodes, proxy->config.workers_per_node);
    return C_OK;

failed:
    proxy_aggregator_shutdown();
    return C_ERR;
}

/* 关闭 Proxy 聚合器 */
void proxy_aggregator_shutdown(void) {
    RETURN_IF(!proxy);
    
    /* 停止刷新线程 */
    if (proxy->running) {
        pthread_mutex_lock(&proxy->active_buckets_lock);
        proxy->running = 0;
        if (proxy->active_buckets_cond_initialized) {
            pthread_cond_signal(&proxy->active_buckets_cond);
        }
        pthread_mutex_unlock(&proxy->active_buckets_lock);
        pthread_join(proxy->flush_thread, NULL);
    }
    
    /* 清理批量桶 */
    if (proxy->buckets) {
        proxy_batch_bucket_destroy_array(proxy->buckets,
                                         proxy->num_buckets);
    }

    if (proxy->active_buckets_lock_initialized) {
        pthread_mutex_destroy(&proxy->active_buckets_lock);
        proxy->active_buckets_lock_initialized = 0;
    }
    if (proxy->active_buckets_cond_initialized) {
        pthread_cond_destroy(&proxy->active_buckets_cond);
        proxy->active_buckets_cond_initialized = 0;
    }
    proxy_active_bucket_heap_cleanup(&proxy->active_bucket_heap);
    
    /* 清理一致性哈希环 */
    proxy_router_cleanup(&proxy->router);
    
    ring_buffer_mgr_shutdown();
    
    zfree(proxy);
    proxy = NULL;
    
    serverLog(LL_NOTICE, "Proxy aggregator shutdown");
}

/* 提交请求到聚合器 */
int proxy_enqueue_request(const char *key, void *client_ctx, 
                         float *result_buffer, size_t vector_dim) {
    RETURN_IF(!proxy || !key, C_ERR);
    
    proxy_route_t route;
    RETURN_IF(proxy_router_route(&proxy->router, key, &route) != C_OK, C_ERR);
    size_t bucket_index = worker_queue_index(proxy->config.workers_per_node,
                                             route.supernode_id, route.worker_id);
    proxy_batch_bucket_t *bucket = &proxy->buckets[bucket_index];
    RETURN_IF(!bucket->rb, C_ERR);
    
    proxy_request_t *req = proxy_request_create(
        atomic_fetch_add_explicit(&next_request_id, 1, memory_order_relaxed),
        route.key_hash,
        route.supernode_id,
        route.worker_id,
        ustime(),
        client_ctx,
        result_buffer,
        vector_dim);
    RETURN_IF(!req, C_ERR);
    
    pthread_mutex_lock(&bucket->mutex);
    if (bucket->count >= bucket->capacity) {
        if (proxy_executor_flush_bucket_locked(&proxy->executor, bucket, bucket->rb, 1,
                                      req->submit_time_us,
                                      PROXY_FLUSH_TRIGGER_IMMEDIATE_CAPACITY) != C_OK) {
            pthread_mutex_unlock(&bucket->mutex);
            proxy_request_destroy(req);
            return C_ERR; /* 桶满且同步 flush 失败 */
        }
    }

    if (proxy_batch_bucket_append(bucket, req) != C_OK) {
        pthread_mutex_unlock(&bucket->mutex);
        proxy_request_destroy(req);
        return C_ERR;
    }
    proxy_active_bucket_add(proxy, bucket_index);

    flush_decision_t decision =
        flush_scheduler_on_append(&proxy->scheduler, bucket->count);
    if (decision.should_flush) {
        if (proxy_executor_flush_bucket_locked(&proxy->executor, bucket, bucket->rb,
                                      decision.flush_reason_full,
                                      req->submit_time_us,
                                      PROXY_FLUSH_TRIGGER_IMMEDIATE_APPEND) != C_OK) {
            serverLog(LL_DEBUG,
                      "Immediate flush deferred for supernode %d worker %d: ring buffer busy",
                      route.supernode_id, route.worker_id);
        } else {
            proxy_active_bucket_remove(proxy, bucket_index);
        }
    }

    pthread_mutex_unlock(&bucket->mutex);
    atomic_fetch_add_explicit(&proxy->total_requests, 1, memory_order_relaxed);
    return C_OK;
}

sds proxy_aggregator_get_stats(void) {
    sds stats = sdsempty();
    
    if (!proxy) {
        stats = sdscat(stats, "Proxy Aggregator: Not initialized\n");
        return stats;
    }
    
    uint64_t total_req =
        atomic_load_explicit(&proxy->total_requests, memory_order_relaxed);
    uint64_t active_bucket_peak =
        atomic_load_explicit(&proxy->active_bucket_peak, memory_order_relaxed);
    uint64_t active_wait_wakeups =
        atomic_load_explicit(&proxy->active_wait_wakeups, memory_order_relaxed);
    uint64_t timed_wait_wakeups =
        atomic_load_explicit(&proxy->timed_wait_wakeups, memory_order_relaxed);
    uint64_t flush_retry_count =
        atomic_load_explicit(&proxy->flush_retry_count, memory_order_relaxed);
    uint64_t total_flush =
        atomic_load_explicit(&proxy->executor.stats.total_flushes,
                             memory_order_relaxed);
    uint64_t batch_full =
        atomic_load_explicit(&proxy->executor.stats.batch_full_flushes,
                             memory_order_relaxed);
    uint64_t timeout =
        atomic_load_explicit(&proxy->executor.stats.timeout_flushes,
                             memory_order_relaxed);
    uint64_t immediate_attempts =
        atomic_load_explicit(&proxy->executor.stats.immediate_flush_attempts,
                             memory_order_relaxed);
    uint64_t immediate_successes =
        atomic_load_explicit(&proxy->executor.stats.immediate_flush_successes,
                             memory_order_relaxed);
    uint64_t immediate_deferred =
        atomic_load_explicit(&proxy->executor.stats.immediate_flush_deferred,
                             memory_order_relaxed);
    uint64_t enqueue_rejections =
        atomic_load_explicit(&proxy->executor.stats.enqueue_rejections_full,
                             memory_order_relaxed);
    
    pthread_mutex_lock(&proxy->active_buckets_lock);
    size_t active_bucket_count = proxy->active_bucket_heap.count;
    pthread_mutex_unlock(&proxy->active_buckets_lock);

    stats = sdscatprintf(stats, "Proxy Aggregator Stats:\n");
    stats = sdscatprintf(stats, "  Supernodes: %zu\n", proxy->num_supernodes);
    stats = sdscatprintf(stats, "  Workers per supernode: %zu\n",
                         proxy->config.workers_per_node);
    stats = sdscatprintf(stats, "  Total requests: %llu\n", (unsigned long long)total_req);
    stats = sdscatprintf(stats, "  Active buckets: %zu\n", active_bucket_count);
    stats = sdscatprintf(stats, "  Active bucket peak: %llu\n",
                         (unsigned long long)active_bucket_peak);
    stats = sdscatprintf(stats, "  Active wait wakeups: %llu\n",
                         (unsigned long long)active_wait_wakeups);
    stats = sdscatprintf(stats, "  Timed wait wakeups: %llu\n",
                         (unsigned long long)timed_wait_wakeups);
    stats = sdscatprintf(stats, "  Flush retry count: %llu\n",
                         (unsigned long long)flush_retry_count);
    stats = sdscatprintf(stats, "  Total flushes: %llu\n", (unsigned long long)total_flush);
    stats = sdscatprintf(stats, "  Batch full flushes: %llu\n", (unsigned long long)batch_full);
    stats = sdscatprintf(stats, "  Timeout flushes: %llu\n", (unsigned long long)timeout);
    stats = sdscatprintf(stats, "  Immediate flush attempts: %llu\n",
                         (unsigned long long)immediate_attempts);
    stats = sdscatprintf(stats, "  Immediate flush successes: %llu\n",
                         (unsigned long long)immediate_successes);
    stats = sdscatprintf(stats, "  Immediate flush deferred: %llu\n",
                         (unsigned long long)immediate_deferred);
    stats = sdscatprintf(stats, "  Enqueue rejections (full): %llu\n",
                         (unsigned long long)enqueue_rejections);
    
    if (total_flush > 0) {
        double avg_batch_size = (double)total_req / total_flush;
        stats = sdscatprintf(stats, "  Average batch size: %.1f\n", avg_batch_size);
    }

    if (immediate_attempts > 0) {
        double immediate_success_rate =
            (double)immediate_successes * 100.0 / (double)immediate_attempts;
        stats = sdscatprintf(stats, "  Immediate flush success rate: %.1f%%\n",
                             immediate_success_rate);
    }
    
    return stats;
}
