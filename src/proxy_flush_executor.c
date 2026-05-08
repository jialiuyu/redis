#include "proxy_flush_executor.h"

#include "macro.h"
#include "server.h"

void proxy_flush_executor_init(proxy_flush_executor_t *executor) {
    RETURN_IF(!executor);

    atomic_init(&executor->stats.total_batches, 0);
    atomic_init(&executor->stats.total_flushes, 0);
    atomic_init(&executor->stats.batch_full_flushes, 0);
    atomic_init(&executor->stats.timeout_flushes, 0);
    atomic_init(&executor->stats.immediate_flush_attempts, 0);
    atomic_init(&executor->stats.immediate_flush_successes, 0);
    atomic_init(&executor->stats.immediate_flush_deferred, 0);
    atomic_init(&executor->stats.enqueue_rejections_full, 0);
    atomic_init(&executor->next_batch_id, 1);
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

int proxy_executor_flush_bucket_locked(proxy_flush_executor_t *executor,
                                            proxy_batch_bucket_t *bucket,
                                            ring_buffer_t *rb,
                                            int flush_reason_full,
                                            uint64_t flush_time_us,
                                            proxyFlushTrigger trigger) {
    RETURN_IF(!executor || !bucket || !rb || proxy_batch_bucket_count(bucket) == 0, C_ERR);

    size_t packet_size = proxy_batch_bucket_packet_size(bucket);
    RETURN_IF(packet_size == 0, C_ERR);

    batch_packet_t *packet = NULL;
    if (ring_buffer_reserve(rb, packet_size, (void **)&packet) != C_OK) {
        proxy_flush_executor_record_immediate_metrics(executor, trigger, 0);
        return C_ERR;
    }
    if (proxy_batch_bucket_fill_packet(
            bucket, packet, packet_size, ustime(),
            atomic_fetch_add_explicit(&executor->next_batch_id, 1, memory_order_relaxed)) != C_OK) {
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
        atomic_fetch_add_explicit(&executor->stats.total_flushes, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&executor->stats.total_batches, 1, memory_order_relaxed);
        if (flush_reason_full) {
            atomic_fetch_add_explicit(&executor->stats.batch_full_flushes, 1, memory_order_relaxed);
        } else {
            atomic_fetch_add_explicit(&executor->stats.timeout_flushes, 1, memory_order_relaxed);
        }

        serverLog(LL_DEBUG, "Flushed batch: %zu requests to supernode %d",
                  proxy_batch_bucket_count(bucket), proxy_batch_bucket_target_supernode_id(bucket));
        proxy_batch_bucket_reset(bucket, flush_time_us);
        proxy_flush_executor_record_immediate_metrics(executor, trigger, 1);
    }

    return ret;
}
