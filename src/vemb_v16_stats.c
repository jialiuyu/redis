#include "vemb_v16_stats.h"

#include <stddef.h>

static uint64_t counter_load(atomic_uint_fast64_t *counter) {
    return atomic_load_explicit(counter, memory_order_relaxed);
}

static void stats_set_max(uint64_t *dst, uint64_t value) {
    if (*dst < value)
        *dst = value;
}

void vemb_v16_stats_add_channel_counters(vemb_v16_stats_t *dst,
                                         vemb_v16_channel_counters_t *src) {
    dst->total_requests += counter_load(&src->total_requests);
    dst->vadd_requests += counter_load(&src->vadd_requests);
    dst->vemb_requests += counter_load(&src->vemb_requests);
    dst->vsim_requests += counter_load(&src->vsim_requests);
    dst->not_found += counter_load(&src->not_found);
    dst->published_jobs += counter_load(&src->published_jobs);
    dst->completed_jobs += counter_load(&src->completed_jobs);
    dst->proxy_vemb_ring_full += counter_load(&src->proxy_vemb_ring_full);
    dst->proxy_vadd_ring_full += counter_load(&src->proxy_vadd_ring_full);
    dst->proxy_response_ring_full += counter_load(&src->proxy_response_ring_full);
    dst->supernode_completion_publish += counter_load(&src->supernode_completion_publish);
    dst->supernode_completion_ring_full += counter_load(&src->supernode_completion_ring_full);
    dst->moved_count += counter_load(&src->moved_count);
    dst->stale_count += counter_load(&src->stale_count);
    dst->ask_count += counter_load(&src->ask_count);
    dst->forward_count += counter_load(&src->forward_count);
    dst->duplicate_request_count += counter_load(&src->duplicate_request_count);
}

void vemb_v16_stats_add(vemb_v16_stats_t *dst, const vemb_v16_stats_t *src) {
    dst->total_requests += src->total_requests;
    dst->vadd_requests += src->vadd_requests;
    dst->vemb_requests += src->vemb_requests;
    dst->vsim_requests += src->vsim_requests;
    dst->not_found += src->not_found;
    dst->published_jobs += src->published_jobs;
    dst->completed_jobs += src->completed_jobs;
    dst->proxy_vemb_ring_full += src->proxy_vemb_ring_full;
    dst->proxy_vadd_ring_full += src->proxy_vadd_ring_full;
    dst->read_pool_alloc_ok += src->read_pool_alloc_ok;
    dst->read_pool_alloc_fail += src->read_pool_alloc_fail;
    stats_set_max(&dst->read_pool_inuse_peak, src->read_pool_inuse_peak);
    if (src->read_pool_free_min != 0 &&
        (dst->read_pool_free_min == 0 ||
         dst->read_pool_free_min > src->read_pool_free_min)) {
        dst->read_pool_free_min = src->read_pool_free_min;
    }
    dst->proxy_response_ring_full += src->proxy_response_ring_full;
    dst->supernode_completion_publish += src->supernode_completion_publish;
    dst->supernode_completion_ring_full += src->supernode_completion_ring_full;
    dst->moved_count += src->moved_count;
    dst->stale_count += src->stale_count;
    dst->ask_count += src->ask_count;
    dst->forward_count += src->forward_count;
    dst->duplicate_request_count += src->duplicate_request_count;
    dst->source_gc_count += src->source_gc_count;
    if (dst->gc_safe_watermark < src->gc_safe_watermark)
        dst->gc_safe_watermark = src->gc_safe_watermark;
    dst->migration_baseline_sent += src->migration_baseline_sent;
    dst->migration_baseline_skipped += src->migration_baseline_skipped;
    dst->migration_baseline_error += src->migration_baseline_error;
    dst->migration_baseline_retry_queued +=
        src->migration_baseline_retry_queued;
    dst->migration_baseline_retry_sent += src->migration_baseline_retry_sent;
    dst->migration_baseline_retry_pending +=
        src->migration_baseline_retry_pending;
    dst->warm_eviction_success += src->warm_eviction_success;
    dst->warm_eviction_fail += src->warm_eviction_fail;
    dst->warm_same_key_overwrite += src->warm_same_key_overwrite;
    dst->warm_stale_handle_reject += src->warm_stale_handle_reject;
    dst->remote_meta_stale += src->remote_meta_stale;
    dst->remote_meta_lookup_hit += src->remote_meta_lookup_hit;
    dst->remote_meta_lookup_miss += src->remote_meta_lookup_miss;
    dst->remote_meta_lookup_busy += src->remote_meta_lookup_busy;
    dst->remote_meta_lookup_way_probe += src->remote_meta_lookup_way_probe;
    dst->remote_meta_lookup_set_conflict += src->remote_meta_lookup_set_conflict;
    dst->remote_meta_publish_async_enqueue += src->remote_meta_publish_async_enqueue;
    dst->remote_meta_publish_async_drop += src->remote_meta_publish_async_drop;
    dst->remote_meta_publish_async_coalesce += src->remote_meta_publish_async_coalesce;
    dst->remote_meta_publish_ok += src->remote_meta_publish_ok;
    dst->remote_meta_publish_busy += src->remote_meta_publish_busy;
    dst->remote_meta_publish_insert += src->remote_meta_publish_insert;
    dst->remote_meta_publish_update += src->remote_meta_publish_update;
    dst->remote_meta_publish_evict += src->remote_meta_publish_evict;
    dst->remote_meta_publish_ns += src->remote_meta_publish_ns;
    dst->ub_lookup_rpc_count += src->ub_lookup_rpc_count;
    dst->ub_lookup_rpc_ok += src->ub_lookup_rpc_ok;
    dst->ub_lookup_rpc_not_found += src->ub_lookup_rpc_not_found;
    dst->ub_lookup_rpc_busy += src->ub_lookup_rpc_busy;
    dst->ub_lookup_rpc_timeout += src->ub_lookup_rpc_timeout;
    dst->ub_lookup_rpc_error += src->ub_lookup_rpc_error;
    dst->ub_lookup_rpc_handle += src->ub_lookup_rpc_handle;
    dst->ub_lookup_rpc_snapshot += src->ub_lookup_rpc_snapshot;
    dst->ub_lookup_rpc_ns += src->ub_lookup_rpc_ns;
    dst->remote_meta_repair_enqueue += src->remote_meta_repair_enqueue;
    dst->remote_meta_repair_ok += src->remote_meta_repair_ok;
    dst->remote_meta_repair_drop += src->remote_meta_repair_drop;
}
