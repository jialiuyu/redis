/*
 * Proxy Aggregator Implementation
 * 智能批量聚合器 - 蓄水池策略
 */

#define REDISMODULE_CORE_MODULE

#include "proxy_aggregator.h"
#include "batch_latency_trace.h"
#include "macro.h"
#include "monotonic.h"
#include "server.h"
#include "proxy_batch_bucket.h"
#include "proxy_active_bucket_heap.h"
#include "proxy_flush_scheduler.h"
#include "proxy_flush_executor.h"
#include "proxy_router.h"
#include "ring_buffer_mgr.h"
#include "vector_proxy_completion.h"
#include "ub_metadata.h"

#include <stddef.h>
#include <string.h>
#include <time.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <limits.h>

#define PROXY_VEMB_DIRECT_GAP_US 20
#define PROXY_VEMB_FC_MIN_SLOTS 64
#define PROXY_VEMB_FC_SLOT_MULTIPLIER 4

typedef enum proxyVembFcSlotState {
    PROXY_VEMB_FC_SLOT_EMPTY = 0,
    PROXY_VEMB_FC_SLOT_WRITING = 1,
    PROXY_VEMB_FC_SLOT_PENDING = 2,
    PROXY_VEMB_FC_SLOT_CLAIMED = 3,
} proxyVembFcSlotState;

typedef struct proxy_vemb_fc_slot {
    _Alignas(64) atomic_int state;
    uint64_t request_id;
    uint64_t row_id;
    uint64_t submit_time_us;
    proxy_vector_request_t *owner;
} proxy_vemb_fc_slot_t;

typedef struct proxy_vemb_fc_board {
    proxy_vemb_fc_slot_t *slots;
    size_t slot_count;
    size_t bucket_index;
    atomic_int combiner_lock;
    atomic_size_t pending_count;
} proxy_vemb_fc_board_t;

typedef struct proxy_aggregator_config {
    size_t batch_limit;
    uint64_t time_limit_us;
    size_t max_supernodes;
    size_t workers_per_node;
    int vemb_submit_mode;
    int vemb_adaptive;
    uint64_t vemb_direct_gap_us;
    size_t vemb_fc_slots;
    size_t vemb_fc_max_scan;
} proxy_aggregator_config_t;

typedef struct proxy_aggregator {
    proxy_batch_bucket_t *buckets; // 批量桶 - 每个 (supernode, worker) 一个 
    ring_buffer_t **response_rings;
    proxy_vemb_fc_board_t *vemb_fc_boards;
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
    pthread_t result_thread;
    int running;

    /* 统计信息 */
    atomic_uint_fast64_t total_requests;
    atomic_uint_fast64_t active_bucket_peak;
    atomic_uint_fast64_t active_wait_wakeups;
    atomic_uint_fast64_t timed_wait_wakeups;
    atomic_uint_fast64_t flush_retry_count;
    atomic_uint_fast64_t vemb_direct_submits;
    atomic_uint_fast64_t vsim_direct_submits;
    atomic_uint_fast64_t vemb_batch_enqueues;
    atomic_uint_fast64_t vemb_adaptive_checks;
    atomic_uint_fast64_t vemb_adaptive_selected;
    atomic_uint_fast64_t vemb_adaptive_bucket_nonempty;
    atomic_uint_fast64_t vemb_adaptive_gap_selected;
    atomic_uint_fast64_t vemb_adaptive_ewma_selected;
    atomic_uint_fast64_t vemb_direct_fallbacks;
    atomic_uint_fast64_t direct_ring_full;
    atomic_uint_fast64_t vemb_fc_published;
    atomic_uint_fast64_t vemb_fc_combines;
    atomic_uint_fast64_t vemb_fc_combined_requests;
    atomic_uint_fast64_t vemb_fc_slot_busy;
    atomic_uint_fast64_t vemb_fc_ring_busy;
    atomic_uint_fast64_t vemb_fc_fallbacks;
} proxy_aggregator_t;

static proxy_aggregator_t *proxy = NULL;
static atomic_uint_fast64_t next_request_id = 1;

static void proxy_active_bucket_add(proxy_aggregator_t *agg, size_t bucket_index);
static void proxy_active_bucket_remove(proxy_aggregator_t *agg, size_t bucket_index);
static void proxy_flush_thread_signal(proxy_aggregator_t *agg);

static inline void proxy_aggregator_config_init(proxy_aggregator_config_t *cfg) {
    RETURN_IF(!cfg);
    cfg->batch_limit = server.proxy.batch_limit > 0 ? server.proxy.batch_limit : (size_t)PROXY_BATCH_LIMIT;
    cfg->time_limit_us = server.proxy.time_limit_us > 0 ? server.proxy.time_limit_us : (uint64_t)PROXY_TIME_LIMIT_US;
    cfg->max_supernodes = server.proxy.max_supernodes > 0 ? server.proxy.max_supernodes : (size_t)PROXY_MAX_SUPERNODES;
    cfg->workers_per_node = server.supernode_workers > 0 ?  server.supernode_workers : (size_t)max((int)sysconf(_SC_NPROCESSORS_ONLN), 1);
    cfg->vemb_submit_mode = server.proxy.vemb_submit_mode;
    cfg->vemb_adaptive = server.proxy.vemb_adaptive;
    cfg->vemb_direct_gap_us = server.proxy.vemb_direct_gap_us > 0 ?
        server.proxy.vemb_direct_gap_us : (uint64_t)PROXY_VEMB_DIRECT_GAP_US;
    cfg->vemb_fc_slots = server.proxy.vemb_fc_slots;
    cfg->vemb_fc_max_scan = server.proxy.vemb_fc_max_scan;
}

static inline size_t worker_queue_index(size_t workers_per_node,
                                        int supernode_id,
                                        int worker_id) {
    return (size_t)supernode_id * workers_per_node + (size_t)worker_id;
}

static inline size_t proxy_vemb_fc_slot_index(uint64_t request_id,
                                              size_t slot_count) {
    RETURN_IF(slot_count == 0, 0);

    uint64_t x = request_id;
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    return (size_t)(x % slot_count);
}

static size_t proxy_vemb_fc_default_slot_count(proxy_aggregator_t *agg) {
    RETURN_IF(!agg, PROXY_VEMB_FC_MIN_SLOTS);

    size_t slots = agg->config.vemb_fc_slots > 0 ?
        agg->config.vemb_fc_slots :
        agg->config.workers_per_node * PROXY_VEMB_FC_SLOT_MULTIPLIER;
    if (slots < PROXY_VEMB_FC_MIN_SLOTS) slots = PROXY_VEMB_FC_MIN_SLOTS;
    return slots;
}

static int proxy_route_vector_request(proxy_aggregator_t *agg,
                                      const char *key,
                                      proxy_vector_request_t *owner,
                                      proxy_route_t *route) {
    RETURN_IF(!agg || !key || !owner || !route, C_ERR);

    if (owner->op_type == PROXY_VECTOR_OP_VEMB) {
        return proxy_router_route_by_row(&agg->router, key, owner->row_id, route);
    } else if (owner->op_type == PROXY_VECTOR_OP_VSIM) {
        return proxy_router_route_by_request(&agg->router, key, owner->request_id, route);
    }

    return proxy_router_route(&agg->router, key, route);
}

static inline int proxy_vemb_direct_path_enabled(proxy_aggregator_t *agg) {
    RETURN_IF(!agg, 0);

    return agg->config.batch_limit <= 1 || agg->config.time_limit_us == 0;
}

static inline uint64_t proxy_bucket_gap_us(const proxy_batch_bucket_t *bucket,
                                           uint64_t now_us) {
    RETURN_IF(!bucket || bucket->last_append_time_us == 0 ||
              now_us < bucket->last_append_time_us, 0);

    return now_us - bucket->last_append_time_us;
}

static inline int proxy_vemb_adaptive_direct_selected(proxy_aggregator_t *agg,
                                                      proxy_batch_bucket_t *bucket,
                                                      uint64_t now_us) {
    RETURN_IF(!agg || !bucket, 0);
    RETURN_IF(!agg->config.vemb_adaptive, 0);

    atomic_fetch_add_explicit(&agg->vemb_adaptive_checks, 1, memory_order_relaxed);
    if (bucket->count != 0) {
        atomic_fetch_add_explicit(&agg->vemb_adaptive_bucket_nonempty,
                                  1, memory_order_relaxed);
        return 0;
    }

    uint64_t gap_us = proxy_bucket_gap_us(bucket, now_us);
    if (bucket->last_append_time_us == 0 || gap_us >= agg->config.vemb_direct_gap_us) {
        atomic_fetch_add_explicit(&agg->vemb_adaptive_selected, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&agg->vemb_adaptive_gap_selected, 1, memory_order_relaxed);
        return 1;
    }

    if (bucket->recent_gap_ewma_us >= agg->config.vemb_direct_gap_us) {
        atomic_fetch_add_explicit(&agg->vemb_adaptive_selected, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&agg->vemb_adaptive_ewma_selected, 1, memory_order_relaxed);
        return 1;
    }

    return 0;
}

static void proxy_record_direct_attempt(proxy_aggregator_t *agg,
                                        uint32_t op_type,
                                        int success) {
    RETURN_IF(!agg);

    atomic_fetch_add_explicit(&agg->executor.stats.immediate_flush_attempts,
                              1, memory_order_relaxed);
    if (success) {
        atomic_fetch_add_explicit(&agg->executor.stats.immediate_flush_successes, 1, memory_order_relaxed);
        if (op_type == BATCH_PACKET_OP_VEMB) {
            atomic_fetch_add_explicit(&agg->vemb_direct_submits, 1, memory_order_relaxed);
        } else if (op_type == BATCH_PACKET_OP_VSIM) {
            atomic_fetch_add_explicit(&agg->vsim_direct_submits, 1, memory_order_relaxed);
        }
    } else {
        atomic_fetch_add_explicit(&agg->executor.stats.enqueue_rejections_full, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&agg->direct_ring_full, 1, memory_order_relaxed);
    }
}

static int proxy_submit_vemb_batch_locked(proxy_aggregator_t *agg,
                                          proxy_batch_bucket_t *bucket,
                                          const proxy_route_t *route,
                                          proxy_vector_request_t **owners,
                                          size_t count,
                                          monotime flush_start,
                                          uint32_t proxy_path,
                                          uint32_t flush_trigger) {
    RETURN_IF(!agg || !bucket || !route || !owners || count == 0, C_ERR);
    RETURN_IF(!bucket->rb || count > UINT32_MAX, C_ERR);
    RETURN_IF(count > (SIZE_MAX - sizeof(batch_packet_t)) /
              sizeof(((batch_packet_t *)0)->requests[0]), C_ERR);

    size_t packet_size = sizeof(batch_packet_t) +
                         count * sizeof(((batch_packet_t *)0)->requests[0]);
    RETURN_IF(packet_size > UINT32_MAX, C_ERR);

    uint64_t wait_sum_us = 0;
    uint64_t wait_max_us = 0;
    for (size_t i = 0; i < count; i++) {
        proxy_vector_request_t *owner = owners[i];
        RETURN_IF(!owner || owner->op_type != PROXY_VECTOR_OP_VEMB, C_ERR);
        uint64_t wait_us = owner->submit_time_us > 0 ?
            (uint64_t)(flush_start - owner->submit_time_us) : 0;
        wait_sum_us += wait_us;
        if (wait_us > wait_max_us) wait_max_us = wait_us;
    }

    uint64_t bucket_gap_us = proxy_bucket_gap_us(bucket, flush_start);
    uint64_t bucket_ewma_gap_us = bucket->recent_gap_ewma_us;
    uint64_t batch_id =
        atomic_fetch_add_explicit(&agg->executor.next_batch_id, 1, memory_order_relaxed);

    batch_packet_t *packet = NULL;
    if (ring_buffer_reserve(bucket->rb, packet_size, (void **)&packet) != C_OK) {
        return C_ERR;
    }

    packet->hdr.magic = BATCH_PACKET_MAGIC;
    packet->hdr.packet_size = (uint32_t)packet_size;
    packet->hdr.num_requests = (uint32_t)count;
    packet->hdr.op_type = BATCH_PACKET_OP_VEMB;
    packet->hdr.supernode_id = (uint32_t)route->supernode_id;
    packet->hdr.worker_id = (uint32_t)route->worker_id;
    packet->hdr.timestamp_us = flush_start;
    packet->hdr.batch_id = batch_id;
    for (size_t i = 0; i < count; i++) {
        packet->requests[i].request_id = owners[i]->request_id;
        packet->requests[i].row_id = owners[i]->row_id;
        owners[i]->batch_id = batch_id;
    }

    int ret = ring_buffer_commit_write(bucket->rb, packet_size);
    if (ret != C_OK) {
        for (size_t i = 0; i < count; i++) owners[i]->batch_id = 0;
        ring_buffer_cancel_write(bucket->rb);
        return ret;
    }

    uint64_t flush_latency_us = elapsedUs(flush_start);
    proxy_batch_bucket_note_arrival(bucket, flush_start);
    atomic_fetch_add_explicit(&agg->executor.stats.total_flushes, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&agg->executor.stats.total_batches, 1, memory_order_relaxed);
    if (proxy_path == BATCH_TRACE_PROXY_PATH_BATCH) {
        proxy_flush_executor_record_batch_size(&agg->executor, count);
    }
    batch_latency_trace_proxy_meta_t proxy_meta = {
        .proxy_path = proxy_path,
        .flush_reason = count == 1 ? BATCH_TRACE_FLUSH_REASON_DIRECT :
                                     BATCH_TRACE_FLUSH_REASON_FULL,
        .flush_trigger = flush_trigger,
        .bucket_depth = (uint32_t)count,
        .bucket_gap_us = bucket_gap_us,
        .bucket_ewma_gap_us = bucket_ewma_gap_us,
    };
    (void)batch_latency_trace_begin(batch_id,
                                    BATCH_PACKET_OP_VEMB,
                                    (uint32_t)count,
                                    wait_sum_us,
                                    wait_max_us,
                                    flush_latency_us,
                                    &proxy_meta);
    atomic_fetch_add_explicit(&agg->total_requests, count, memory_order_relaxed);
    return C_OK;
}

static int proxy_direct_submit_vemb_locked(proxy_aggregator_t *agg,
                                           proxy_batch_bucket_t *bucket,
                                           const proxy_route_t *route,
                                           proxy_vector_request_t *owner,
                                           monotime flush_start) {
    RETURN_IF(!agg || !bucket || !route || !owner ||
              owner->op_type != PROXY_VECTOR_OP_VEMB, C_ERR);
    proxy_vector_request_t *owners[1] = {owner};
    int ret = proxy_submit_vemb_batch_locked(agg, bucket, route, owners, 1,
                                             flush_start,
                                             BATCH_TRACE_PROXY_PATH_DIRECT,
                                             BATCH_TRACE_FLUSH_TRIGGER_DIRECT);
    if (ret != C_OK) {
        proxy_record_direct_attempt(agg, BATCH_PACKET_OP_VEMB, 0);
        return ret;
    }

    proxy_record_direct_attempt(agg, BATCH_PACKET_OP_VEMB, 1);
    return C_OK;
}

static int proxy_direct_submit_vemb(const char *key, proxy_vector_request_t *owner) {
    RETURN_IF(!proxy || !key || !owner || owner->op_type != PROXY_VECTOR_OP_VEMB, C_ERR);

    proxy_route_t route;
    RETURN_IF(proxy_router_route_by_row(&proxy->router, key, owner->row_id, &route) != C_OK,
              C_ERR);

    size_t bucket_index = worker_queue_index(proxy->config.workers_per_node,
                                             route.supernode_id, route.worker_id);
    RETURN_IF(bucket_index >= proxy->num_buckets, C_ERR);
    proxy_batch_bucket_t *bucket = &proxy->buckets[bucket_index];
    RETURN_IF(!bucket->rb, C_ERR);

    monotime flush_start = getMonotonicUs();
    pthread_mutex_lock(&bucket->mutex);
    int ret = proxy_direct_submit_vemb_locked(proxy, bucket, &route, owner, flush_start);
    pthread_mutex_unlock(&bucket->mutex);
    return ret;
}

static int proxy_enqueue_vector_request_bucket(const char *key, proxy_vector_request_t *owner) {
    RETURN_IF(!proxy || !key || !owner, C_ERR);

    proxy_route_t route;
    RETURN_IF(proxy_route_vector_request(proxy, key, owner, &route) != C_OK, C_ERR);
    size_t bucket_index = worker_queue_index(proxy->config.workers_per_node,
                                             route.supernode_id, route.worker_id);
    RETURN_IF(bucket_index >= proxy->num_buckets, C_ERR);
    proxy_batch_bucket_t *bucket = &proxy->buckets[bucket_index];
    RETURN_IF(!bucket->rb, C_ERR);

    proxy_request_t *req = proxy_request_create(
        owner->request_id,
        route.key_hash,
        route.supernode_id,
        route.worker_id,
        getMonotonicUs(),
        owner);
    RETURN_IF(!req, C_ERR);

    pthread_mutex_lock(&bucket->mutex);
    if (owner->op_type == PROXY_VECTOR_OP_VEMB &&
        proxy->config.vemb_submit_mode == PROXY_VEMB_SUBMIT_MODE_ADAPTIVE &&
        proxy_vemb_adaptive_direct_selected(proxy, bucket, req->submit_time_us)) {
        monotime submit_time_us = req->submit_time_us;
        int ret = proxy_direct_submit_vemb_locked(proxy, bucket, &route, owner, submit_time_us);
        if (ret == C_OK) {
            pthread_mutex_unlock(&bucket->mutex);
            proxy_request_destroy(req);
            return C_OK;
        }
        atomic_fetch_add_explicit(&proxy->vemb_direct_fallbacks, 1, memory_order_relaxed);
    }

    if (bucket->count >= bucket->capacity) {
        if (proxy_executor_flush_bucket_locked(&proxy->executor, bucket, bucket->rb, 1,
                                      req->submit_time_us,
                                      PROXY_FLUSH_TRIGGER_IMMEDIATE_CAPACITY) != C_OK) {
            pthread_mutex_unlock(&bucket->mutex);
            proxy_request_destroy(req);
            return C_ERR;
        }
    }

    if (proxy_batch_bucket_append(bucket, req) != C_OK) {
        pthread_mutex_unlock(&bucket->mutex);
        proxy_request_destroy(req);
        return C_ERR;
    }
    if (owner->op_type == PROXY_VECTOR_OP_VEMB) {
        atomic_fetch_add_explicit(&proxy->vemb_batch_enqueues, 1, memory_order_relaxed);
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

static int proxy_vemb_fc_board_init(proxy_vemb_fc_board_t *board,
                                    size_t slot_count,
                                    size_t bucket_index) {
    RETURN_IF(!board || slot_count == 0, C_ERR);

    board->slots = zcalloc(sizeof(proxy_vemb_fc_slot_t) * slot_count);
    RETURN_IF(!board->slots, C_ERR);
    board->slot_count = slot_count;
    board->bucket_index = bucket_index;
    atomic_init(&board->combiner_lock, 0);
    atomic_init(&board->pending_count, 0);
    for (size_t i = 0; i < slot_count; i++) {
        atomic_init(&board->slots[i].state, PROXY_VEMB_FC_SLOT_EMPTY);
    }
    return C_OK;
}

static void proxy_vemb_fc_board_cleanup(proxy_vemb_fc_board_t *board) {
    RETURN_IF(!board);

    zfree(board->slots);
    board->slots = NULL;
    board->slot_count = 0;
    board->bucket_index = 0;
    atomic_store_explicit(&board->pending_count, 0, memory_order_relaxed);
}

static int proxy_vemb_fc_try_combine(proxy_aggregator_t *agg,
                                     proxy_vemb_fc_board_t *board,
                                     const proxy_route_t *route) {
    RETURN_IF(!agg || !board || !route || !board->slots, C_ERR);

    if (atomic_load_explicit(&board->pending_count, memory_order_acquire) == 0) {
        return C_OK;
    }

    if (atomic_load_explicit(&board->combiner_lock, memory_order_relaxed) != 0) {
        return C_OK;
    }
    int expected = 0;
    if (!atomic_compare_exchange_strong_explicit(&board->combiner_lock, &expected, 1,
                                                 memory_order_acq_rel,
                                                 memory_order_relaxed)) {
        return C_OK;
    }

    proxy_vector_request_t **owners = NULL;
    int ret = C_OK;
    int batch_submit_failed = 0;
    size_t count = 0;
    size_t max_count = agg->config.batch_limit > 0 ? agg->config.batch_limit : board->slot_count;
    if (max_count > board->slot_count) max_count = board->slot_count;
    size_t scan_limit = board->slot_count;
    if (agg->config.vemb_fc_max_scan > 0 && scan_limit > agg->config.vemb_fc_max_scan) {
        scan_limit = agg->config.vemb_fc_max_scan;
    }
    if (scan_limit == 0) scan_limit = 1;
    if (max_count > scan_limit) max_count = scan_limit;
    owners = zmalloc(sizeof(*owners) * max_count);
    if (!owners) {
        atomic_store_explicit(&board->combiner_lock, 0, memory_order_release);
        return C_ERR;
    }

    uint64_t route_seed = ((uint64_t)(uint32_t)route->supernode_id << 32) |
                          (uint32_t)route->worker_id;
    size_t start = proxy_vemb_fc_slot_index(route_seed + getMonotonicUs(),
                                            board->slot_count);
    for (size_t scanned = 0; scanned < scan_limit && count < max_count; scanned++) {
        proxy_vemb_fc_slot_t *slot = &board->slots[(start + scanned) % board->slot_count];
        int state = PROXY_VEMB_FC_SLOT_PENDING;
        if (atomic_compare_exchange_strong_explicit(&slot->state, &state,
                                                    PROXY_VEMB_FC_SLOT_CLAIMED,
                                                    memory_order_acq_rel,
                                                    memory_order_relaxed)) {
            owners[count++] = slot->owner;
        }
    }

    if (count > 0) {
        monotime flush_start = getMonotonicUs();
        proxy_batch_bucket_t *bucket = &agg->buckets[board->bucket_index];
        pthread_mutex_lock(&bucket->mutex);
        ret = proxy_submit_vemb_batch_locked(agg, bucket, route, owners, count,
                                             flush_start,
                                             count == 1 ? BATCH_TRACE_PROXY_PATH_DIRECT :
                                                          BATCH_TRACE_PROXY_PATH_BATCH,
                                             BATCH_TRACE_FLUSH_TRIGGER_DIRECT);
        pthread_mutex_unlock(&bucket->mutex);

        if (ret == C_OK) {
            atomic_fetch_add_explicit(&agg->vemb_fc_combines, 1, memory_order_relaxed);
            atomic_fetch_add_explicit(&agg->vemb_fc_combined_requests, count,
                                      memory_order_relaxed);
        } else {
            batch_submit_failed = 1;
            atomic_fetch_add_explicit(&agg->vemb_fc_ring_busy, 1, memory_order_relaxed);
        }
    }

    int fallback_ret = C_OK;
    for (size_t i = 0; i < board->slot_count; i++) {
        proxy_vemb_fc_slot_t *slot = &board->slots[i];
        if (atomic_load_explicit(&slot->state, memory_order_acquire) ==
            PROXY_VEMB_FC_SLOT_CLAIMED) {
            int submitted = !batch_submit_failed;
            if (batch_submit_failed && slot->owner) {
                owners[0] = slot->owner;
                proxy_batch_bucket_t *bucket = &agg->buckets[board->bucket_index];
                monotime flush_start = slot->submit_time_us > 0 ?
                    slot->submit_time_us : getMonotonicUs();
                pthread_mutex_lock(&bucket->mutex);
                int single_ret = proxy_submit_vemb_batch_locked(agg, bucket, route, owners, 1,
                                                                flush_start,
                                                                BATCH_TRACE_PROXY_PATH_DIRECT,
                                                                BATCH_TRACE_FLUSH_TRIGGER_DIRECT);
                pthread_mutex_unlock(&bucket->mutex);
                if (single_ret == C_OK) {
                    submitted = 1;
                    atomic_fetch_add_explicit(&agg->vemb_fc_combines, 1,
                                              memory_order_relaxed);
                    atomic_fetch_add_explicit(&agg->vemb_fc_combined_requests, 1,
                                              memory_order_relaxed);
                } else {
                    fallback_ret = C_ERR;
                    atomic_fetch_add_explicit(&agg->vemb_fc_fallbacks, 1,
                                              memory_order_relaxed);
                }
            }
            if (!slot->owner) submitted = 1;
            if (submitted) {
                slot->owner = NULL;
                slot->request_id = 0;
                slot->row_id = 0;
                slot->submit_time_us = 0;
                atomic_fetch_sub_explicit(&board->pending_count, 1,
                                          memory_order_acq_rel);
                atomic_store_explicit(&slot->state, PROXY_VEMB_FC_SLOT_EMPTY,
                                      memory_order_release);
            } else {
                atomic_store_explicit(&slot->state, PROXY_VEMB_FC_SLOT_PENDING,
                                      memory_order_release);
            }
        }
    }

    zfree(owners);
    atomic_store_explicit(&board->combiner_lock, 0, memory_order_release);
    return batch_submit_failed ? fallback_ret : ret;
}

static int proxy_submit_vemb_fc(const char *key, proxy_vector_request_t *owner) {
    RETURN_IF(!proxy || !key || !owner || owner->op_type != PROXY_VECTOR_OP_VEMB, C_ERR);
    RETURN_IF(!proxy->vemb_fc_boards, C_ERR);

    proxy_route_t route;
    RETURN_IF(proxy_router_route_by_row(&proxy->router, key, owner->row_id, &route) != C_OK,
              C_ERR);
    size_t bucket_index = worker_queue_index(proxy->config.workers_per_node,
                                             route.supernode_id, route.worker_id);
    RETURN_IF(bucket_index >= proxy->num_buckets, C_ERR);
    proxy_vemb_fc_board_t *board = &proxy->vemb_fc_boards[bucket_index];
    RETURN_IF(!board->slots || board->slot_count == 0, C_ERR);

    size_t start = proxy_vemb_fc_slot_index(owner->request_id, board->slot_count);
    proxy_vemb_fc_slot_t *slot = NULL;
    for (size_t i = 0; i < board->slot_count; i++) {
        proxy_vemb_fc_slot_t *candidate = &board->slots[(start + i) % board->slot_count];
        int expected = PROXY_VEMB_FC_SLOT_EMPTY;
        if (atomic_compare_exchange_strong_explicit(&candidate->state, &expected,
                                                    PROXY_VEMB_FC_SLOT_WRITING,
                                                    memory_order_acq_rel,
                                                    memory_order_relaxed)) {
            slot = candidate;
            break;
        }
    }
    if (!slot) {
        atomic_fetch_add_explicit(&proxy->vemb_fc_slot_busy, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&proxy->vemb_fc_fallbacks, 1, memory_order_relaxed);
        return proxy_enqueue_vector_request_bucket(key, owner);
    }

    owner->submit_time_us = getMonotonicUs();
    slot->request_id = owner->request_id;
    slot->row_id = owner->row_id;
    slot->submit_time_us = owner->submit_time_us;
    slot->owner = owner;
    atomic_store_explicit(&slot->state, PROXY_VEMB_FC_SLOT_PENDING,
                          memory_order_release);
    atomic_fetch_add_explicit(&board->pending_count, 1, memory_order_acq_rel);
    atomic_fetch_add_explicit(&proxy->vemb_fc_published, 1, memory_order_relaxed);
    proxy_flush_thread_signal(proxy);

    int ret = proxy_vemb_fc_try_combine(proxy, board, &route);
    if (ret != C_OK) {
        int state = PROXY_VEMB_FC_SLOT_PENDING;
        if (atomic_compare_exchange_strong_explicit(&slot->state, &state,
                                                    PROXY_VEMB_FC_SLOT_EMPTY,
                                                    memory_order_acq_rel,
                                                    memory_order_relaxed)) {
            atomic_fetch_sub_explicit(&board->pending_count, 1,
                                      memory_order_acq_rel);
            slot->owner = NULL;
            slot->request_id = 0;
            slot->row_id = 0;
            slot->submit_time_us = 0;
            return proxy_enqueue_vector_request_bucket(key, owner);
        }
        atomic_fetch_add_explicit(&proxy->vemb_fc_fallbacks, 1, memory_order_relaxed);
        return C_OK;
    }
    return C_OK;
}

static int proxy_vemb_fc_drain_board(proxy_aggregator_t *agg, size_t bucket_index) {
    RETURN_IF(!agg || !agg->vemb_fc_boards || bucket_index >= agg->num_buckets, 0);

    proxy_vemb_fc_board_t *board = &agg->vemb_fc_boards[bucket_index];
    size_t pending_before =
        atomic_load_explicit(&board->pending_count, memory_order_acquire);
    RETURN_IF(pending_before == 0, 0);

    proxy_batch_bucket_t *bucket = &agg->buckets[bucket_index];
    proxy_route_t route = {
        .supernode_id = bucket->target_supernode_id,
        .worker_id = bucket->target_worker_id,
        .key_hash = 0,
    };
    (void)proxy_vemb_fc_try_combine(agg, board, &route);

    size_t pending_after =
        atomic_load_explicit(&board->pending_count, memory_order_acquire);
    return pending_after < pending_before;
}

static int proxy_vemb_fc_drain_pending(proxy_aggregator_t *agg,
                                       int *has_pending) {
    RETURN_IF(!agg || !agg->vemb_fc_boards, 0);

    int drained_any = 0;
    int pending = 0;
    for (size_t i = 0; i < agg->num_buckets; i++) {
        if (atomic_load_explicit(&agg->vemb_fc_boards[i].pending_count,
                                 memory_order_acquire) == 0) {
            continue;
        }
        pending = 1;
        if (proxy_vemb_fc_drain_board(agg, i)) {
            drained_any = 1;
        }
    }
    if (has_pending) *has_pending = pending;
    return drained_any;
}

static int proxy_direct_submit_vsim(const char *key, proxy_vector_request_t *owner) {
    RETURN_IF(!proxy || !key || !owner || owner->op_type != PROXY_VECTOR_OP_VSIM, C_ERR);
    RETURN_IF(!owner->query_vector || owner->query_dim == 0 ||
              !owner->candidate_rows || owner->candidate_count == 0, C_ERR);
    RETURN_IF(owner->query_dim > UINT32_MAX || owner->requested_count > UINT32_MAX ||
              owner->candidate_count > UINT32_MAX, C_ERR);

    proxy_route_t route;
    RETURN_IF(proxy_router_route_by_request(&proxy->router, key, owner->request_id, &route) != C_OK,
              C_ERR);

    size_t bucket_index = worker_queue_index(proxy->config.workers_per_node,
                                             route.supernode_id, route.worker_id);
    RETURN_IF(bucket_index >= proxy->num_buckets, C_ERR);
    proxy_batch_bucket_t *bucket = &proxy->buckets[bucket_index];
    RETURN_IF(!bucket->rb, C_ERR);

    size_t packet_size = sizeof(batch_vsim_packet_t) +
                         sizeof(float) * owner->query_dim +
                         sizeof(uint64_t) * owner->candidate_count;
    RETURN_IF(packet_size > UINT32_MAX, C_ERR);

    monotime flush_start = getMonotonicUs();
    uint64_t wait_us = owner->submit_time_us > 0 ?
        (uint64_t)(flush_start - owner->submit_time_us) : 0;
    uint64_t batch_id =
        atomic_fetch_add_explicit(&proxy->executor.next_batch_id, 1, memory_order_relaxed);

    batch_vsim_packet_t *packet = NULL;
    pthread_mutex_lock(&bucket->mutex);
    uint64_t bucket_gap_us = proxy_bucket_gap_us(bucket, flush_start);
    uint64_t bucket_ewma_gap_us = bucket->recent_gap_ewma_us;
    if (ring_buffer_reserve(bucket->rb, packet_size, (void **)&packet) != C_OK) {
        pthread_mutex_unlock(&bucket->mutex);
        proxy_record_direct_attempt(proxy, BATCH_PACKET_OP_VSIM, 0);
        return C_ERR;
    }

    packet->hdr.magic = BATCH_PACKET_MAGIC;
    packet->hdr.packet_size = (uint32_t)packet_size;
    packet->hdr.num_requests = 1;
    packet->hdr.op_type = BATCH_PACKET_OP_VSIM;
    packet->hdr.supernode_id = (uint32_t)route.supernode_id;
    packet->hdr.worker_id = (uint32_t)route.worker_id;
    packet->hdr.timestamp_us = flush_start;
    packet->hdr.batch_id = batch_id;
    packet->flags = owner->withscores ? 1u : 0u;
    packet->request_id = owner->request_id;
    packet->query_dim = (uint32_t)owner->query_dim;
    packet->requested_count = (uint32_t)owner->requested_count;
    packet->candidate_count = (uint32_t)owner->candidate_count;
    packet->reserved = 0;
    memcpy(packet->payload, owner->query_vector, sizeof(float) * owner->query_dim);
    memcpy((uint8_t *)(packet->payload + owner->query_dim),
           owner->candidate_rows,
           sizeof(uint64_t) * owner->candidate_count);
    owner->batch_id = batch_id;

    int ret = ring_buffer_commit_write(bucket->rb, packet_size);
    if (ret != C_OK) {
        owner->batch_id = 0;
        ring_buffer_cancel_write(bucket->rb);
        pthread_mutex_unlock(&bucket->mutex);
        proxy_record_direct_attempt(proxy, BATCH_PACKET_OP_VSIM, 0);
        return ret;
    }
    proxy_batch_bucket_note_arrival(bucket, flush_start);
    pthread_mutex_unlock(&bucket->mutex);

    uint64_t flush_latency_us = elapsedUs(flush_start);
    proxy_record_direct_attempt(proxy, BATCH_PACKET_OP_VSIM, 1);
    atomic_fetch_add_explicit(&proxy->executor.stats.total_flushes, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&proxy->executor.stats.total_batches, 1, memory_order_relaxed);
    batch_latency_trace_proxy_meta_t proxy_meta = {
        .proxy_path = BATCH_TRACE_PROXY_PATH_DIRECT,
        .flush_reason = BATCH_TRACE_FLUSH_REASON_DIRECT,
        .flush_trigger = BATCH_TRACE_FLUSH_TRIGGER_DIRECT,
        .bucket_depth = 1,
        .bucket_gap_us = bucket_gap_us,
        .bucket_ewma_gap_us = bucket_ewma_gap_us,
    };
    (void)batch_latency_trace_begin(batch_id,
                                    BATCH_PACKET_OP_VSIM,
                                    1,
                                    wait_us,
                                    wait_us,
                                    flush_latency_us,
                                    &proxy_meta);
    atomic_fetch_add_explicit(&proxy->total_requests, 1, memory_order_relaxed);
    return C_OK;
}

static void proxy_flush_thread_signal(proxy_aggregator_t *agg) {
    RETURN_IF(!agg || !agg->active_buckets_lock_initialized ||
              !agg->active_buckets_cond_initialized);

    pthread_mutex_lock(&agg->active_buckets_lock);
    pthread_cond_signal(&agg->active_buckets_cond);
    pthread_mutex_unlock(&agg->active_buckets_lock);
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

static inline struct timespec proxy_timespec_from_rel_us(uint64_t rel_us) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += (time_t)(rel_us / 1000000);
    ts.tv_nsec += (long)((rel_us % 1000000) * 1000);
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec += ts.tv_nsec / 1000000000L;
        ts.tv_nsec %= 1000000000L;
    }
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
            struct timespec deadline_ts = proxy_timespec_from_rel_us(earliest_deadline_us);
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
        uint64_t current_time = getMonotonicUs();
        int processed_any = 0;
        int has_ready_bucket = 0;
        uint64_t earliest_wait_us = UINT64_MAX;

        int has_pending_fc = 0;
        if (proxy_vemb_fc_drain_pending(agg, &has_pending_fc)) {
            processed_any = 1;
        }

        pthread_mutex_lock(&agg->active_buckets_lock);
        size_t bucket_index = 0;
        (void)proxy_active_bucket_heap_peek(&agg->active_bucket_heap, &bucket_index, &earliest_wait_us);
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
                    uint64_t wait_us = agg->scheduler.time_limit_us - diff;
                    if (wait_us < earliest_wait_us) {
                        earliest_wait_us = wait_us;
                    }
                }
            } else {
                proxy_active_bucket_remove(agg, bucket_index);
            }
            
            pthread_mutex_unlock(&bucket->mutex);
            break;
        }
        
        proxy_flush_thread_wait(agg, processed_any, has_ready_bucket || has_pending_fc,
                                earliest_wait_us);
    }
    
    serverLog(LL_NOTICE, "Proxy aggregator flush thread stopped");
    return NULL;
}

static void *result_thread_func(void *arg) {
    proxy_aggregator_t *agg = (proxy_aggregator_t *)arg;

    serverLog(LL_NOTICE, "Proxy aggregator result thread started");

    while (agg->running) {
        int processed_any = 0;

        for (size_t i = 0; i < agg->num_buckets; i++) {
            ring_buffer_t *rb = agg->response_rings ? agg->response_rings[i] : NULL;
            if (!rb) continue;

            while (agg->running) {
                void *payload = NULL;
                size_t payload_len = 0;
                if (ring_buffer_peek(rb, &payload, &payload_len) != C_OK || payload_len == 0) {
                    break;
                }

                batch_result_packet_t *resp = (batch_result_packet_t *)payload;
                if (resp->magic != BATCH_PACKET_MAGIC) {
                    break;
                }

                if (resp->op_type == BATCH_PACKET_OP_VEMB) {
                    if (payload_len < sizeof(batch_result_packet_t)) {
                        break;
                    }
                    size_t expected_len = sizeof(batch_result_packet_t) + sizeof(float) * resp->dim;
                    if (payload_len < expected_len) {
                        break;
                    }

                    if (vector_proxy_completion_complete_vemb(resp->request_id,
                                                              resp->status == C_OK ? resp->data : NULL,
                                                              resp->dim,
                                                              (int)resp->status) == C_OK) {
                        proxy_vector_request_t *req = vector_proxy_completion_lookup(resp->request_id);
                        if (req && req->bc) {
                            uint64_t result_queue_us =
                                req->completion_time_us > 0 ? elapsedUs(req->completion_time_us) : 0;
                            uint64_t request_e2e_us =
                                req->submit_time_us > 0 ? elapsedUs(req->submit_time_us) : 0;
                            if (req->batch_id > 0) {
                                (void)batch_latency_trace_record_request_completion(req->batch_id,
                                                                                    result_queue_us,
                                                                                    request_e2e_us);
                            }
                            RedisModule_UnblockClient(req->bc, req);
                        }
                    }
                } else if (resp->op_type == BATCH_PACKET_OP_VSIM) {
                    batch_vsim_result_packet_t *vsim = (batch_vsim_result_packet_t *)payload;
                    size_t expected_len = sizeof(batch_vsim_result_packet_t) +
                                          sizeof(batch_vsim_result_entry_t) * vsim->num_results;
                    if (payload_len < expected_len) {
                        break;
                    }

                    uint64_t *row_ids = NULL;
                    float *scores = NULL;
                    if (vsim->num_results > 0) {
                        row_ids = zmalloc(sizeof(uint64_t) * vsim->num_results);
                        scores = zmalloc(sizeof(float) * vsim->num_results);
                        if (!row_ids || !scores) {
                            zfree(row_ids);
                            zfree(scores);
                            break;
                        }
                        for (uint32_t j = 0; j < vsim->num_results; j++) {
                            row_ids[j] = vsim->results[j].row_id;
                            scores[j] = vsim->results[j].score;
                        }
                    }

                    if (vector_proxy_completion_complete_vsim(vsim->request_id,
                                                              row_ids,
                                                              scores,
                                                              vsim->num_results,
                                                              (int)vsim->status) == C_OK) {
                        proxy_vector_request_t *req = vector_proxy_completion_lookup(vsim->request_id);
                        if (req && req->bc) {
                            uint64_t result_queue_us =
                                req->completion_time_us > 0 ? elapsedUs(req->completion_time_us) : 0;
                            uint64_t request_e2e_us =
                                req->submit_time_us > 0 ? elapsedUs(req->submit_time_us) : 0;
                            if (req->batch_id > 0) {
                                (void)batch_latency_trace_record_request_completion(req->batch_id,
                                                                                    result_queue_us,
                                                                                    request_e2e_us);
                            }
                            RedisModule_UnblockClient(req->bc, req);
                        }
                    }
                    zfree(row_ids);
                    zfree(scores);
                } else {
                    break;
                }

                ring_buffer_commit_read(rb, payload_len);
                processed_any = 1;
            }
        }

        if (!processed_any) {
            struct timespec ts = {0, 10000};
            nanosleep(&ts, NULL);
        }
    }

    serverLog(LL_NOTICE, "Proxy aggregator result thread stopped");
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
    batch_latency_trace_init();
    
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
    proxy->response_rings = zcalloc(sizeof(ring_buffer_t *) * proxy->num_buckets);
    if (!proxy->response_rings) goto failed;
    proxy->vemb_fc_boards = zcalloc(sizeof(proxy_vemb_fc_board_t) * proxy->num_buckets);
    if (!proxy->vemb_fc_boards) goto failed;
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
            int ret = proxy_batch_bucket_init(bucket, proxy->config.batch_limit, sn, (int)worker, getMonotonicUs());
            if (ret != C_OK) {
                goto failed;
            }
            bucket->rb = ring_buffer_mgr_get_request(sn, (int)worker);
            if (!bucket->rb) {
                goto failed;
            }
            proxy->response_rings[idx] = ring_buffer_mgr_get_response(sn, (int)worker);
            if (!proxy->response_rings[idx]) {
                goto failed;
            }
            if (proxy_vemb_fc_board_init(&proxy->vemb_fc_boards[idx],
                                         proxy_vemb_fc_default_slot_count(proxy),
                                         idx) != C_OK) {
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
    atomic_init(&proxy->vemb_direct_submits, 0);
    atomic_init(&proxy->vsim_direct_submits, 0);
    atomic_init(&proxy->vemb_batch_enqueues, 0);
    atomic_init(&proxy->vemb_adaptive_checks, 0);
    atomic_init(&proxy->vemb_adaptive_selected, 0);
    atomic_init(&proxy->vemb_adaptive_bucket_nonempty, 0);
    atomic_init(&proxy->vemb_adaptive_gap_selected, 0);
    atomic_init(&proxy->vemb_adaptive_ewma_selected, 0);
    atomic_init(&proxy->vemb_direct_fallbacks, 0);
    atomic_init(&proxy->direct_ring_full, 0);
    atomic_init(&proxy->vemb_fc_published, 0);
    atomic_init(&proxy->vemb_fc_combines, 0);
    atomic_init(&proxy->vemb_fc_combined_requests, 0);
    atomic_init(&proxy->vemb_fc_slot_busy, 0);
    atomic_init(&proxy->vemb_fc_ring_busy, 0);
    atomic_init(&proxy->vemb_fc_fallbacks, 0);
    
    /* 启动刷新线程 */
    proxy->running = 1;
    if (pthread_create(&proxy->flush_thread, NULL, flush_thread_func, proxy) != 0) {
        serverLog(LL_WARNING, "Failed to create flush thread");
        proxy->running = 0;
        goto failed;
    }
    if (pthread_create(&proxy->result_thread, NULL, result_thread_func, proxy) != 0) {
        serverLog(LL_WARNING, "Failed to create result thread");
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
        pthread_join(proxy->result_thread, NULL);
    }
    
    /* 清理批量桶 */
    if (proxy->buckets) {
        proxy_batch_bucket_destroy_array(proxy->buckets,
                                         proxy->num_buckets);
    }
    zfree(proxy->response_rings);
    proxy->response_rings = NULL;
    if (proxy->vemb_fc_boards) {
        for (size_t i = 0; i < proxy->num_buckets; i++) {
            proxy_vemb_fc_board_cleanup(&proxy->vemb_fc_boards[i]);
        }
        zfree(proxy->vemb_fc_boards);
        proxy->vemb_fc_boards = NULL;
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
    batch_latency_trace_cleanup();
    
    zfree(proxy);
    proxy = NULL;
    
    serverLog(LL_NOTICE, "Proxy aggregator shutdown");
}

/* 提交请求到聚合器 */
int proxy_enqueue_request(const char *key, void *client_ctx, 
                         float *result_buffer, size_t vector_dim) {
    UNUSED(key);
    UNUSED(client_ctx);
    UNUSED(result_buffer);
    UNUSED(vector_dim);
    serverLog(LL_WARNING, "proxy_enqueue_request() is deprecated for UB row-based packets");
    return C_ERR;
}

int proxy_enqueue_vector_request(const char *key, proxy_vector_request_t *owner) {
    RETURN_IF(!proxy || !key || !owner, C_ERR);

    if (owner->op_type == PROXY_VECTOR_OP_VSIM) {
        return proxy_direct_submit_vsim(key, owner);
    }

    if (owner->op_type == PROXY_VECTOR_OP_VEMB) {
        if (proxy->config.vemb_submit_mode == PROXY_VEMB_SUBMIT_MODE_FC) {
            return proxy_submit_vemb_fc(key, owner);
        }
        if (proxy->config.vemb_submit_mode == PROXY_VEMB_SUBMIT_MODE_DIRECT) {
            return proxy_direct_submit_vemb(key, owner);
        }
    }

    return proxy_enqueue_vector_request_bucket(key, owner);
}

static int proxy_vemb_reply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    proxy_vector_request_t *req =
        (proxy_vector_request_t *)RedisModule_GetBlockedClientPrivateData(ctx);
    if (!req) {
        return RedisModule_ReplyWithError(ctx, "ERR missing VEMB proxy request");
    }

    proxy_vector_request_t *owned_req = NULL;
    if (vector_proxy_completion_take(req->request_id, &owned_req) != C_OK || !owned_req) {
        return RedisModule_ReplyWithError(ctx, "ERR missing VEMB completion");
    }

    if (owned_req->error_code != C_OK) {
        proxy_vector_request_free(owned_req);
        return RedisModule_ReplyWithError(ctx, "ERR UB engine vemb failed");
    }

    if (!owned_req->result_vector) {
        proxy_vector_request_free(owned_req);
        return RedisModule_ReplyWithNull(ctx);
    }

    if (owned_req->raw_output) {
        RedisModule_ReplyWithArray(ctx, 3);
        RedisModule_ReplyWithSimpleString(ctx, "fp32");
        RedisModule_ReplyWithStringBuffer(ctx,
            (const char*)owned_req->result_vector,
            owned_req->result_dim * sizeof(float));
        RedisModule_ReplyWithDouble(ctx, 1.0);
    } else {
        RedisModule_ReplyWithArray(ctx, owned_req->result_dim);
        for (size_t i = 0; i < owned_req->result_dim; i++) {
            RedisModule_ReplyWithDouble(ctx, owned_req->result_vector[i]);
        }
    }

    RedisModule_BlockedClientMeasureTimeEnd(RedisModule_GetBlockedClientHandle(ctx));
    proxy_vector_request_free(owned_req);
    return REDISMODULE_OK;
}

static int proxy_vsim_reply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    proxy_vector_request_t *req =
        (proxy_vector_request_t *)RedisModule_GetBlockedClientPrivateData(ctx);
    if (!req) {
        return RedisModule_ReplyWithError(ctx, "ERR missing VSIM proxy request");
    }

    proxy_vector_request_t *owned_req = NULL;
    if (vector_proxy_completion_take(req->request_id, &owned_req) != C_OK || !owned_req) {
        return RedisModule_ReplyWithError(ctx, "ERR missing VSIM completion");
    }

    if (owned_req->error_code != C_OK) {
        proxy_vector_request_free(owned_req);
        return RedisModule_ReplyWithError(ctx, "ERR UB engine vsim failed");
    }

    RedisModule_ReplyWithArray(ctx, owned_req->withscores ? (long long)(owned_req->result_count * 2) :
                                                     (long long)owned_req->result_count);
    for (size_t i = 0; i < owned_req->result_count; i++) {
        const char *element = "";
        if (owned_req->candidate_elements &&
            i < owned_req->candidate_count &&
            owned_req->candidate_elements[i]) {
            element = owned_req->candidate_elements[i];
        } else if (owned_req->result_rows && owned_req->candidate_rows &&
                   owned_req->candidate_elements) {
            for (size_t j = 0; j < owned_req->candidate_count; j++) {
                if (owned_req->candidate_rows[j] == owned_req->result_rows[i] &&
                    owned_req->candidate_elements[j]) {
                    element = owned_req->candidate_elements[j];
                    break;
                }
            }
        }
        RedisModule_ReplyWithStringBuffer(ctx, element, strlen(element));
        if (owned_req->withscores) {
            RedisModule_ReplyWithDouble(ctx, owned_req->result_scores ? owned_req->result_scores[i] : 0.0);
        }
    }

    RedisModule_BlockedClientMeasureTimeEnd(RedisModule_GetBlockedClientHandle(ctx));
    proxy_vector_request_free(owned_req);
    return REDISMODULE_OK;
}

int proxy_submit_vemb(RedisModuleCtx *ctx,
                      void *key,
                      void *element,
                      int raw_output) {
    RETURN_IF(!ctx || !key || !element, REDISMODULE_ERR);
    RETURN_IF(!proxy, RedisModule_ReplyWithError(ctx, "ERR proxy aggregator not initialized"));

    RedisModuleString *key_obj = key;
    RedisModuleString *element_obj = element;

    size_t key_len, element_len;
    const char *key_cstr = RedisModule_StringPtrLen(key_obj, &key_len);
    const char *element_cstr = RedisModule_StringPtrLen(element_obj, &element_len);
    if (!key_cstr || !element_cstr) {
        return RedisModule_ReplyWithError(ctx, "ERR invalid key or element");
    }

    sds key_tmp = sdsnewlen(key_cstr, key_len);
    sds element_tmp = sdsnewlen(element_cstr, element_len);
    if (!key_tmp || !element_tmp) {
        sdsfree(key_tmp);
        sdsfree(element_tmp);
        return RedisModule_ReplyWithError(ctx, "ERR oom");
    }

    ub_vector_set_meta_t *set = ub_metadata_get_set(key_tmp);
    uint64_t row_id = 0;
    if (!set || ub_metadata_lookup_row(set, element_tmp, &row_id) != C_OK) {
        sdsfree(key_tmp);
        sdsfree(element_tmp);
        return RedisModule_ReplyWithNull(ctx);
    }

    uint64_t request_id =
        atomic_fetch_add_explicit(&next_request_id, 1, memory_order_relaxed);
    RedisModuleBlockedClient *bc =
        RedisModule_BlockClient(ctx, proxy_vemb_reply, NULL, NULL, 0);
    if (!bc) {
        sdsfree(key_tmp);
        sdsfree(element_tmp);
        return RedisModule_ReplyWithError(ctx, "ERR failed to block client");
    }

    proxy_vector_request_t *req =
        proxy_vector_request_create_vemb(request_id, row_id, raw_output, bc);
    if (!req) {
        RedisModule_AbortBlock(bc);
        sdsfree(key_tmp);
        sdsfree(element_tmp);
        return RedisModule_ReplyWithError(ctx, "ERR oom");
    }

    if (vector_proxy_completion_register(req) != C_OK) {
        proxy_vector_request_free(req);
        RedisModule_AbortBlock(bc);
        sdsfree(key_tmp);
        sdsfree(element_tmp);
        return RedisModule_ReplyWithError(ctx, "ERR failed to register VEMB completion");
    }

    RedisModule_BlockClientSetPrivateData(bc, req);
    RedisModule_BlockedClientMeasureTimeStart(bc);
    int submit_ret = proxy_vemb_direct_path_enabled(proxy) ?
                     proxy_direct_submit_vemb(key_tmp, req) :
                     proxy_enqueue_vector_request(key_tmp, req);
    if (submit_ret != C_OK) {
        proxy_vector_request_t *taken = NULL;
        vector_proxy_completion_take(req->request_id, &taken);
        if (taken) proxy_vector_request_free(taken);
        RedisModule_AbortBlock(bc);
        sdsfree(key_tmp);
        sdsfree(element_tmp);
        return RedisModule_ReplyWithError(ctx, "ERR failed to enqueue VEMB request");
    }

    sdsfree(key_tmp);
    sdsfree(element_tmp);
    return REDISMODULE_OK;
}

int proxy_submit_vsim(RedisModuleCtx *ctx,
                      void *key,
                      float *query_vector,
                      size_t query_dim,
                      size_t requested_count,
                      int withscores) {
    RETURN_IF(!ctx || !key || !query_vector || query_dim == 0, REDISMODULE_ERR);
    RETURN_IF(!proxy, RedisModule_ReplyWithError(ctx, "ERR proxy aggregator not initialized"));

    RedisModuleString *key_obj = key;
    size_t key_len;
    const char *key_cstr = RedisModule_StringPtrLen(key_obj, &key_len);
    if (!key_cstr) {
        zfree(query_vector);
        return RedisModule_ReplyWithError(ctx, "ERR invalid key");
    }

    sds key_tmp = sdsnewlen(key_cstr, key_len);
    if (!key_tmp) {
        zfree(query_vector);
        return RedisModule_ReplyWithError(ctx, "ERR oom");
    }

    ub_vector_set_meta_t *set = ub_metadata_get_set(key_tmp);
    uint64_t *rows = NULL;
    sds *elements = NULL;
    size_t candidate_count = set ? ub_metadata_collect_rows(set, &rows, &elements) : 0;
    if (candidate_count == 0) {
        sdsfree(key_tmp);
        zfree(query_vector);
        RedisModule_ReplyWithEmptyArray(ctx);
        return REDISMODULE_OK;
    }

    uint64_t request_id =
        atomic_fetch_add_explicit(&next_request_id, 1, memory_order_relaxed);
    RedisModuleBlockedClient *bc =
        RedisModule_BlockClient(ctx, proxy_vsim_reply, NULL, NULL, 0);
    if (!bc) {
        sdsfree(key_tmp);
        zfree(query_vector);
        zfree(rows);
        if (elements) {
            for (size_t i = 0; i < candidate_count; i++) sdsfree(elements[i]);
            zfree(elements);
        }
        return RedisModule_ReplyWithError(ctx, "ERR failed to block client");
    }

    proxy_vector_request_t *req =
        proxy_vector_request_create_vsim(request_id, query_vector, query_dim,
                                         rows, elements, candidate_count,
                                         requested_count, withscores, bc);
    if (!req) {
        RedisModule_AbortBlock(bc);
        sdsfree(key_tmp);
        zfree(query_vector);
        zfree(rows);
        if (elements) {
            for (size_t i = 0; i < candidate_count; i++) sdsfree(elements[i]);
            zfree(elements);
        }
        return RedisModule_ReplyWithError(ctx, "ERR oom");
    }

    if (vector_proxy_completion_register(req) != C_OK) {
        proxy_vector_request_free(req);
        RedisModule_AbortBlock(bc);
        sdsfree(key_tmp);
        return RedisModule_ReplyWithError(ctx, "ERR failed to register VSIM completion");
    }

    RedisModule_BlockClientSetPrivateData(bc, req);
    RedisModule_BlockedClientMeasureTimeStart(bc);
    if (proxy_direct_submit_vsim(key_tmp, req) != C_OK) {
        proxy_vector_request_t *taken = NULL;
        vector_proxy_completion_take(req->request_id, &taken);
        if (taken) proxy_vector_request_free(taken);
        RedisModule_AbortBlock(bc);
        sdsfree(key_tmp);
        return RedisModule_ReplyWithError(ctx, "ERR failed to enqueue VSIM request");
    }

    sdsfree(key_tmp);
    return REDISMODULE_OK;
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
    uint64_t batch_flush_requests =
        atomic_load_explicit(&proxy->executor.stats.batch_flush_requests,
                             memory_order_relaxed);
    uint64_t batch_size_1 =
        atomic_load_explicit(&proxy->executor.stats.batch_size_hist_1,
                             memory_order_relaxed);
    uint64_t batch_size_2_4 =
        atomic_load_explicit(&proxy->executor.stats.batch_size_hist_2_4,
                             memory_order_relaxed);
    uint64_t batch_size_5_16 =
        atomic_load_explicit(&proxy->executor.stats.batch_size_hist_5_16,
                             memory_order_relaxed);
    uint64_t batch_size_17_plus =
        atomic_load_explicit(&proxy->executor.stats.batch_size_hist_17_plus,
                             memory_order_relaxed);
    uint64_t vemb_direct_submits =
        atomic_load_explicit(&proxy->vemb_direct_submits, memory_order_relaxed);
    uint64_t vsim_direct_submits =
        atomic_load_explicit(&proxy->vsim_direct_submits, memory_order_relaxed);
    uint64_t vemb_batch_enqueues =
        atomic_load_explicit(&proxy->vemb_batch_enqueues, memory_order_relaxed);
    uint64_t vemb_adaptive_checks =
        atomic_load_explicit(&proxy->vemb_adaptive_checks, memory_order_relaxed);
    uint64_t vemb_adaptive_selected =
        atomic_load_explicit(&proxy->vemb_adaptive_selected, memory_order_relaxed);
    uint64_t vemb_adaptive_bucket_nonempty =
        atomic_load_explicit(&proxy->vemb_adaptive_bucket_nonempty, memory_order_relaxed);
    uint64_t vemb_adaptive_gap_selected =
        atomic_load_explicit(&proxy->vemb_adaptive_gap_selected, memory_order_relaxed);
    uint64_t vemb_adaptive_ewma_selected =
        atomic_load_explicit(&proxy->vemb_adaptive_ewma_selected, memory_order_relaxed);
    uint64_t vemb_direct_fallbacks =
        atomic_load_explicit(&proxy->vemb_direct_fallbacks, memory_order_relaxed);
    uint64_t direct_ring_full =
        atomic_load_explicit(&proxy->direct_ring_full, memory_order_relaxed);
    uint64_t vemb_fc_published =
        atomic_load_explicit(&proxy->vemb_fc_published, memory_order_relaxed);
    uint64_t vemb_fc_combines =
        atomic_load_explicit(&proxy->vemb_fc_combines, memory_order_relaxed);
    uint64_t vemb_fc_combined_requests =
        atomic_load_explicit(&proxy->vemb_fc_combined_requests, memory_order_relaxed);
    uint64_t vemb_fc_slot_busy =
        atomic_load_explicit(&proxy->vemb_fc_slot_busy, memory_order_relaxed);
    uint64_t vemb_fc_ring_busy =
        atomic_load_explicit(&proxy->vemb_fc_ring_busy, memory_order_relaxed);
    uint64_t vemb_fc_fallbacks =
        atomic_load_explicit(&proxy->vemb_fc_fallbacks, memory_order_relaxed);
    
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
    stats = sdscatprintf(stats, "  Batch flush requests: %llu\n",
                         (unsigned long long)batch_flush_requests);
    stats = sdscatprintf(stats, "  Batch size histogram: 1=%llu 2-4=%llu 5-16=%llu 17+=%llu\n",
                         (unsigned long long)batch_size_1,
                         (unsigned long long)batch_size_2_4,
                         (unsigned long long)batch_size_5_16,
                         (unsigned long long)batch_size_17_plus);
    stats = sdscatprintf(stats, "  Immediate flush attempts: %llu\n",
                         (unsigned long long)immediate_attempts);
    stats = sdscatprintf(stats, "  Immediate flush successes: %llu\n",
                         (unsigned long long)immediate_successes);
    stats = sdscatprintf(stats, "  Immediate flush deferred: %llu\n",
                         (unsigned long long)immediate_deferred);
    stats = sdscatprintf(stats, "  Enqueue rejections (full): %llu\n",
                         (unsigned long long)enqueue_rejections);
    stats = sdscatprintf(stats, "  VEMB direct submits: %llu\n",
                         (unsigned long long)vemb_direct_submits);
    stats = sdscatprintf(stats, "  VSIM direct submits: %llu\n",
                         (unsigned long long)vsim_direct_submits);
    stats = sdscatprintf(stats, "  VEMB batch enqueues: %llu\n",
                         (unsigned long long)vemb_batch_enqueues);
    stats = sdscatprintf(stats, "  VEMB adaptive checks: %llu\n",
                         (unsigned long long)vemb_adaptive_checks);
    stats = sdscatprintf(stats, "  VEMB adaptive selected: %llu\n",
                         (unsigned long long)vemb_adaptive_selected);
    stats = sdscatprintf(stats, "  VEMB adaptive bucket nonempty: %llu\n",
                         (unsigned long long)vemb_adaptive_bucket_nonempty);
    stats = sdscatprintf(stats, "  VEMB adaptive gap selected: %llu\n",
                         (unsigned long long)vemb_adaptive_gap_selected);
    stats = sdscatprintf(stats, "  VEMB adaptive EWMA selected: %llu\n",
                         (unsigned long long)vemb_adaptive_ewma_selected);
    stats = sdscatprintf(stats, "  VEMB direct fallbacks: %llu\n",
                         (unsigned long long)vemb_direct_fallbacks);
    stats = sdscatprintf(stats, "  Direct ring full: %llu\n",
                         (unsigned long long)direct_ring_full);
    stats = sdscatprintf(stats, "  VEMB FC published: %llu\n",
                         (unsigned long long)vemb_fc_published);
    stats = sdscatprintf(stats, "  VEMB FC combines: %llu\n",
                         (unsigned long long)vemb_fc_combines);
    stats = sdscatprintf(stats, "  VEMB FC combined requests: %llu\n",
                         (unsigned long long)vemb_fc_combined_requests);
    stats = sdscatprintf(stats, "  VEMB FC slot busy: %llu\n",
                         (unsigned long long)vemb_fc_slot_busy);
    stats = sdscatprintf(stats, "  VEMB FC ring busy: %llu\n",
                         (unsigned long long)vemb_fc_ring_busy);
    stats = sdscatprintf(stats, "  VEMB FC fallbacks: %llu\n",
                         (unsigned long long)vemb_fc_fallbacks);
    
    if (total_flush > 0) {
        double avg_submit_size = (double)total_req / total_flush;
        stats = sdscatprintf(stats, "  Average submit size: %.1f\n", avg_submit_size);
    }

    if (batch_full + timeout > 0) {
        double avg_batch_size =
            (double)batch_flush_requests / (double)(batch_full + timeout);
        stats = sdscatprintf(stats, "  Average batch flush size: %.1f\n", avg_batch_size);
    }

    if (vemb_fc_combines > 0) {
        double avg_fc_batch =
            (double)vemb_fc_combined_requests / (double)vemb_fc_combines;
        stats = sdscatprintf(stats, "  Average VEMB FC batch size: %.1f\n",
                             avg_fc_batch);
    }

    if (immediate_attempts > 0) {
        double immediate_success_rate =
            (double)immediate_successes * 100.0 / (double)immediate_attempts;
        stats = sdscatprintf(stats, "  Immediate flush success rate: %.1f%%\n",
                             immediate_success_rate);
    }

    sds traces = batch_latency_trace_dump_recent("  Recent batch traces", 16);
    stats = sdscatsds(stats, traces);
    sdsfree(traces);
    
    return stats;
}
