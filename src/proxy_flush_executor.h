#ifndef __PROXY_FLUSH_EXECUTOR_H
#define __PROXY_FLUSH_EXECUTOR_H

#include "proxy_batch_bucket.h"
#include "ring_buffer.h"

#include <stddef.h>
#include <stdatomic.h>
#include <stdint.h>

typedef struct proxy_flush_stats {
    atomic_uint_fast64_t total_batches;
    atomic_uint_fast64_t total_flushes;
    atomic_uint_fast64_t batch_full_flushes;
    atomic_uint_fast64_t timeout_flushes;
    atomic_uint_fast64_t batch_flush_requests;
    atomic_uint_fast64_t batch_size_hist_1;
    atomic_uint_fast64_t batch_size_hist_2_4;
    atomic_uint_fast64_t batch_size_hist_5_16;
    atomic_uint_fast64_t batch_size_hist_17_plus;
    atomic_uint_fast64_t immediate_flush_attempts;
    atomic_uint_fast64_t immediate_flush_successes;
    atomic_uint_fast64_t immediate_flush_deferred;
    atomic_uint_fast64_t enqueue_rejections_full;
} proxy_flush_stats_t;

typedef struct proxy_flush_executor {
    proxy_flush_stats_t stats;
    atomic_uint_fast64_t next_batch_id;
} proxy_flush_executor_t;

typedef enum proxyFlushTrigger {
    PROXY_FLUSH_TRIGGER_BACKGROUND,
    PROXY_FLUSH_TRIGGER_IMMEDIATE_CAPACITY,
    PROXY_FLUSH_TRIGGER_IMMEDIATE_APPEND,
} proxyFlushTrigger;

uint32_t proxy_flush_trigger_trace_value(proxyFlushTrigger trigger);
void proxy_flush_executor_record_batch_size(proxy_flush_executor_t *executor,
                                            size_t batch_size);
void proxy_flush_executor_init(proxy_flush_executor_t *executor);

// proxy_executor_flush_bucket_locked
int proxy_executor_flush_bucket_locked(proxy_flush_executor_t *executor,
                                       proxy_batch_bucket_t *bucket,
                                       ring_buffer_t *rb,
                                       int flush_reason_full,
                                       uint64_t flush_time_us,
                                       proxyFlushTrigger trigger);

#endif /* __PROXY_FLUSH_EXECUTOR_H */
