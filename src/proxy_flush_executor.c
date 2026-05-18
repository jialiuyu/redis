#include "proxy_flush_executor.h"

#include "batch_latency_trace.h"
#include "macro.h"
#include "monotonic.h"
#include "server.h"

void proxy_flush_executor_init(proxy_flush_executor_t *executor) {
    RETURN_IF(!executor);

    atomic_init(&executor->stats.total_batches, 0);
    atomic_init(&executor->stats.total_flushes, 0);
    atomic_init(&executor->stats.batch_full_flushes, 0);
    atomic_init(&executor->stats.timeout_flushes, 0);
    atomic_init(&executor->stats.batch_flush_requests, 0);
    atomic_init(&executor->stats.batch_size_hist_1, 0);
    atomic_init(&executor->stats.batch_size_hist_2_4, 0);
    atomic_init(&executor->stats.batch_size_hist_5_16, 0);
    atomic_init(&executor->stats.batch_size_hist_17_plus, 0);
    atomic_init(&executor->stats.immediate_flush_attempts, 0);
    atomic_init(&executor->stats.immediate_flush_successes, 0);
    atomic_init(&executor->stats.immediate_flush_deferred, 0);
    atomic_init(&executor->stats.enqueue_rejections_full, 0);
    atomic_init(&executor->next_batch_id, 1);
}

uint32_t proxy_flush_trigger_trace_value(proxyFlushTrigger trigger) {
    switch (trigger) {
    case PROXY_FLUSH_TRIGGER_BACKGROUND:
        return BATCH_TRACE_FLUSH_TRIGGER_BACKGROUND;
    case PROXY_FLUSH_TRIGGER_IMMEDIATE_CAPACITY:
        return BATCH_TRACE_FLUSH_TRIGGER_IMMEDIATE_CAPACITY;
    case PROXY_FLUSH_TRIGGER_IMMEDIATE_APPEND:
        return BATCH_TRACE_FLUSH_TRIGGER_IMMEDIATE_APPEND;
    default:
        return BATCH_TRACE_FLUSH_TRIGGER_UNKNOWN;
    }
}

void proxy_flush_executor_record_batch_size(proxy_flush_executor_t *executor,
                                            size_t batch_size) {
    RETURN_IF(!executor || batch_size == 0);

    atomic_fetch_add_explicit(&executor->stats.batch_flush_requests,
                              batch_size, memory_order_relaxed);
    if (batch_size == 1) {
        atomic_fetch_add_explicit(&executor->stats.batch_size_hist_1,
                                  1, memory_order_relaxed);
    } else if (batch_size <= 4) {
        atomic_fetch_add_explicit(&executor->stats.batch_size_hist_2_4,
                                  1, memory_order_relaxed);
    } else if (batch_size <= 16) {
        atomic_fetch_add_explicit(&executor->stats.batch_size_hist_5_16,
                                  1, memory_order_relaxed);
    } else {
        atomic_fetch_add_explicit(&executor->stats.batch_size_hist_17_plus,
                                  1, memory_order_relaxed);
    }
}

static void proxy_flush_executor_record_immediate_metrics(proxy_flush_executor_t *executor,
                                                          proxyFlushTrigger trigger,
                                                          int success) {
    RETURN_IF(!executor || trigger == PROXY_FLUSH_TRIGGER_BACKGROUND);

    atomic_fetch_add_explicit(&executor->stats.immediate_flush_attempts, 1, memory_order_relaxed);
    if (success) {
        atomic_fetch_add_explicit(&executor->stats.immediate_flush_successes, 1, memory_order_relaxed);
    } else if (trigger == PROXY_FLUSH_TRIGGER_IMMEDIATE_APPEND) {
        atomic_fetch_add_explicit(&executor->stats.immediate_flush_deferred, 1, memory_order_relaxed);
    } else if (trigger == PROXY_FLUSH_TRIGGER_IMMEDIATE_CAPACITY) {
        atomic_fetch_add_explicit(&executor->stats.enqueue_rejections_full, 1, memory_order_relaxed);
    }
}

/*
 * Flush one bucket into its bound ring buffer.
 *
 * Usage:
 * - Called by either the request thread (immediate flush path) or the
 *   background flush thread.
 * - Serializes the current bucket content into one batch packet and publishes
 *   it to the target ring buffer.
 *
 * Important:
 * - The caller must already hold bucket->mutex.
 * - This function assumes the bucket cannot be appended to concurrently while
 *   it is building and publishing the batch packet.
 * - On success, the bucket is reset in place; on failure, the bucket content
 *   is left intact so the caller can retry later.
 */
int proxy_executor_flush_bucket_locked(proxy_flush_executor_t *executor,
                                            proxy_batch_bucket_t *bucket,
                                            ring_buffer_t *rb,
                                            int flush_reason_full,
                                            uint64_t flush_time_us,
                                            proxyFlushTrigger trigger) {
    RETURN_IF(!executor || !bucket || !rb || bucket->count == 0, C_ERR);

    monotime flush_start = getMonotonicUs();
    uint64_t batching_delay_sum = 0;
    uint64_t batching_delay_max = 0;
    for (size_t i = 0; i < bucket->count; i++) {
        proxy_request_t *req = bucket->requests[i];
        proxy_vector_request_t *owner = req ? req->owner : NULL;
        if (!owner || owner->submit_time_us == 0) continue;
        uint64_t delay = flush_start - owner->submit_time_us;
        batching_delay_sum += delay;
        if (delay > batching_delay_max) batching_delay_max = delay;
    }

    size_t packet_size = proxy_batch_bucket_packet_size(bucket);
    RETURN_IF(packet_size == 0, C_ERR);
    uint64_t batch_id =
        atomic_fetch_add_explicit(&executor->next_batch_id, 1, memory_order_relaxed);

    batch_packet_t *packet = NULL;
    if (ring_buffer_reserve(rb, packet_size, (void **)&packet) != C_OK) {
        proxy_flush_executor_record_immediate_metrics(executor, trigger, 0);
        return C_ERR;
    }
    if (proxy_batch_bucket_fill_packet(
            bucket, packet, packet_size, flush_start,
            batch_id) != C_OK) {
        ring_buffer_cancel_write(rb);
        proxy_flush_executor_record_immediate_metrics(executor, trigger, 0);
        return C_ERR;
    }

    int ret = ring_buffer_commit_write(rb, packet_size);
    if (ret != C_OK) {
        ring_buffer_cancel_write(rb);
        proxy_flush_executor_record_immediate_metrics(executor, trigger, 0);
        return ret;
    }

    if (ret == C_OK) {
        uint64_t flush_latency_us = elapsedUs(flush_start);
        atomic_fetch_add_explicit(&executor->stats.total_flushes, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&executor->stats.total_batches, 1, memory_order_relaxed);
        proxy_flush_executor_record_batch_size(executor, bucket->count);
        for (size_t i = 0; i < bucket->count; i++) {
            proxy_request_t *req = bucket->requests[i];
            if (req && req->owner) req->owner->batch_id = batch_id;
        }
        uint64_t bucket_gap_us = 0;
        if (bucket->last_append_time_us > 0 && flush_start >= bucket->last_append_time_us) {
            bucket_gap_us = flush_start - bucket->last_append_time_us;
        }
        batch_latency_trace_proxy_meta_t proxy_meta = {
            .proxy_path = BATCH_TRACE_PROXY_PATH_BATCH,
            .flush_reason = flush_reason_full ? BATCH_TRACE_FLUSH_REASON_FULL :
                                                 BATCH_TRACE_FLUSH_REASON_TIMEOUT,
            .flush_trigger = proxy_flush_trigger_trace_value(trigger),
            .bucket_depth = (uint32_t)bucket->count,
            .bucket_gap_us = bucket_gap_us,
            .bucket_ewma_gap_us = bucket->recent_gap_ewma_us,
        };
        (void)batch_latency_trace_begin(batch_id,
                                        packet->hdr.op_type,
                                        packet->hdr.num_requests,
                                        batching_delay_sum,
                                        batching_delay_max,
                                        flush_latency_us,
                                        &proxy_meta);
        if (flush_reason_full) {
            atomic_fetch_add_explicit(&executor->stats.batch_full_flushes, 1, memory_order_relaxed);
        } else {
            atomic_fetch_add_explicit(&executor->stats.timeout_flushes, 1, memory_order_relaxed);
        }

        serverLog(LL_DEBUG, "Flushed batch: %zu requests to supernode %d",
                  bucket->count, bucket->target_supernode_id);
        proxy_batch_bucket_reset(bucket, flush_time_us);
        proxy_flush_executor_record_immediate_metrics(executor, trigger, 1);
    }

    return ret;
}
