#include "vemb_v16_stats.h"

#include <stddef.h>

static uint64_t counter_load(atomic_uint_fast64_t *counter) {
    return atomic_load_explicit(counter, memory_order_relaxed);
}

static void stats_set_max(uint64_t *dst, uint64_t value) {
    if (*dst < value)
        *dst = value;
}

static void atomic_update_max_u64(atomic_uint_fast64_t *counter,
                                  uint64_t value) {
    uint_fast64_t prev = atomic_load_explicit(counter, memory_order_relaxed);
    while (prev < value &&
           !atomic_compare_exchange_weak_explicit(counter,
                                                  &prev,
                                                  value,
                                                  memory_order_relaxed,
                                                  memory_order_relaxed)) {
    }
}

void vemb_v16_timing_acc_add(vemb_v16_timing_acc_t *acc, uint64_t ns) {
    acc->count++;
    acc->ns += ns;
    if (acc->max_ns < ns)
        acc->max_ns = ns;
}

void vemb_v16_channel_counters_add_timing(vemb_v16_channel_counters_t *stats,
                                          vemb_v16_timing_stage_t stage,
                                          const vemb_v16_timing_acc_t *acc) {
    if (!acc || acc->count == 0)
        return;

    atomic_uint_fast64_t *count = NULL;
    atomic_uint_fast64_t *sum = NULL;
    atomic_uint_fast64_t *max = NULL;
    switch (stage) {
    case VEMB_V16_TIMING_JOB_TOTAL:
        count = &stats->timing_job_count;
        sum = &stats->timing_job_total_ns;
        max = &stats->timing_job_total_max_ns;
        break;
    case VEMB_V16_TIMING_PRIMARY_LOOKUP:
        count = &stats->timing_primary_lookup_count;
        sum = &stats->timing_primary_lookup_ns;
        max = &stats->timing_primary_lookup_max_ns;
        break;
    case VEMB_V16_TIMING_SECONDARY_LOOKUP:
        count = &stats->timing_secondary_lookup_count;
        sum = &stats->timing_secondary_lookup_ns;
        max = &stats->timing_secondary_lookup_max_ns;
        break;
    case VEMB_V16_TIMING_REMOTE_META_LOOKUP:
        count = &stats->timing_remote_meta_lookup_count;
        sum = &stats->timing_remote_meta_lookup_ns;
        max = &stats->timing_remote_meta_lookup_max_ns;
        break;
    case VEMB_V16_TIMING_PAYLOAD_LOCAL_SLICE:
        count = &stats->timing_payload_local_slice_count;
        sum = &stats->timing_payload_local_slice_ns;
        max = &stats->timing_payload_local_slice_max_ns;
        break;
    case VEMB_V16_TIMING_PAYLOAD_REMOTE_SLICE:
        count = &stats->timing_payload_remote_slice_count;
        sum = &stats->timing_payload_remote_slice_ns;
        max = &stats->timing_payload_remote_slice_max_ns;
        break;
    case VEMB_V16_TIMING_COMPUTE:
        count = &stats->timing_compute_count;
        sum = &stats->timing_compute_ns;
        max = &stats->timing_compute_max_ns;
        break;
    default:
        return;
    }
    atomic_fetch_add_explicit(count, acc->count, memory_order_relaxed);
    atomic_fetch_add_explicit(sum, acc->ns, memory_order_relaxed);
    atomic_update_max_u64(max, acc->max_ns);
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
    dst->proxy_request_poll += counter_load(&src->proxy_request_poll);
    dst->proxy_completion_poll += counter_load(&src->proxy_completion_poll);
    dst->proxy_vemb_publish += counter_load(&src->proxy_vemb_publish);
    dst->proxy_vadd_publish += counter_load(&src->proxy_vadd_publish);
    dst->proxy_vemb_ring_full += counter_load(&src->proxy_vemb_ring_full);
    dst->proxy_vadd_ring_full += counter_load(&src->proxy_vadd_ring_full);
    dst->proxy_response_publish += counter_load(&src->proxy_response_publish);
    dst->proxy_response_ring_full += counter_load(&src->proxy_response_ring_full);
    dst->supernode_vemb_poll += counter_load(&src->supernode_vemb_poll);
    dst->supernode_vadd_poll += counter_load(&src->supernode_vadd_poll);
    dst->supernode_completion_publish += counter_load(&src->supernode_completion_publish);
    dst->supernode_completion_ring_full += counter_load(&src->supernode_completion_ring_full);
    dst->sample_count += counter_load(&src->sample_count);
    dst->sample_table_lookup_ns += counter_load(&src->sample_table_lookup_ns);
    dst->sample_bitmap_lock_ns += counter_load(&src->sample_bitmap_lock_ns);
    dst->sample_bitmap_unlock_ns += counter_load(&src->sample_bitmap_unlock_ns);
    dst->sample_vector_load_ns += counter_load(&src->sample_vector_load_ns);
    dst->sample_completion_publish_ns += counter_load(&src->sample_completion_publish_ns);
    dst->channel_ops += counter_load(&src->channel_ops);
    dst->timing_job_count += counter_load(&src->timing_job_count);
    dst->timing_job_total_ns += counter_load(&src->timing_job_total_ns);
    stats_set_max(&dst->timing_job_total_max_ns, counter_load(&src->timing_job_total_max_ns));
    dst->timing_primary_lookup_count += counter_load(&src->timing_primary_lookup_count);
    dst->timing_primary_lookup_ns += counter_load(&src->timing_primary_lookup_ns);
    stats_set_max(&dst->timing_primary_lookup_max_ns, counter_load(&src->timing_primary_lookup_max_ns));
    dst->timing_secondary_lookup_count += counter_load(&src->timing_secondary_lookup_count);
    dst->timing_secondary_lookup_ns += counter_load(&src->timing_secondary_lookup_ns);
    stats_set_max(&dst->timing_secondary_lookup_max_ns, counter_load(&src->timing_secondary_lookup_max_ns));
    dst->timing_remote_meta_lookup_count += counter_load(&src->timing_remote_meta_lookup_count);
    dst->timing_remote_meta_lookup_ns += counter_load(&src->timing_remote_meta_lookup_ns);
    stats_set_max(&dst->timing_remote_meta_lookup_max_ns, counter_load(&src->timing_remote_meta_lookup_max_ns));
    dst->timing_payload_local_slice_count += counter_load(&src->timing_payload_local_slice_count);
    dst->timing_payload_local_slice_ns += counter_load(&src->timing_payload_local_slice_ns);
    stats_set_max(&dst->timing_payload_local_slice_max_ns, counter_load(&src->timing_payload_local_slice_max_ns));
    dst->timing_payload_remote_slice_count += counter_load(&src->timing_payload_remote_slice_count);
    dst->timing_payload_remote_slice_ns += counter_load(&src->timing_payload_remote_slice_ns);
    stats_set_max(&dst->timing_payload_remote_slice_max_ns, counter_load(&src->timing_payload_remote_slice_max_ns));
    dst->timing_compute_count += counter_load(&src->timing_compute_count);
    dst->timing_compute_ns += counter_load(&src->timing_compute_ns);
    stats_set_max(&dst->timing_compute_max_ns, counter_load(&src->timing_compute_max_ns));
}

void vemb_v16_stats_add(vemb_v16_stats_t *dst, const vemb_v16_stats_t *src) {
    dst->total_requests += src->total_requests;
    dst->vadd_requests += src->vadd_requests;
    dst->vemb_requests += src->vemb_requests;
    dst->vsim_requests += src->vsim_requests;
    dst->not_found += src->not_found;
    dst->published_jobs += src->published_jobs;
    dst->completed_jobs += src->completed_jobs;
    dst->proxy_request_poll += src->proxy_request_poll;
    dst->proxy_completion_poll += src->proxy_completion_poll;
    dst->proxy_vemb_publish += src->proxy_vemb_publish;
    dst->proxy_vadd_publish += src->proxy_vadd_publish;
    dst->proxy_vemb_ring_full += src->proxy_vemb_ring_full;
    dst->proxy_vadd_ring_full += src->proxy_vadd_ring_full;
    dst->proxy_response_publish += src->proxy_response_publish;
    dst->proxy_response_ring_full += src->proxy_response_ring_full;
    dst->supernode_vemb_poll += src->supernode_vemb_poll;
    dst->supernode_vadd_poll += src->supernode_vadd_poll;
    dst->supernode_completion_publish += src->supernode_completion_publish;
    dst->supernode_completion_ring_full += src->supernode_completion_ring_full;
    dst->sample_count += src->sample_count;
    dst->sample_table_lookup_ns += src->sample_table_lookup_ns;
    dst->sample_bitmap_lock_ns += src->sample_bitmap_lock_ns;
    dst->sample_bitmap_unlock_ns += src->sample_bitmap_unlock_ns;
    dst->sample_vector_load_ns += src->sample_vector_load_ns;
    dst->sample_completion_publish_ns += src->sample_completion_publish_ns;
    dst->channel_ops += src->channel_ops;
    dst->timing_job_count += src->timing_job_count;
    dst->timing_job_total_ns += src->timing_job_total_ns;
    stats_set_max(&dst->timing_job_total_max_ns, src->timing_job_total_max_ns);
    dst->timing_primary_lookup_count += src->timing_primary_lookup_count;
    dst->timing_primary_lookup_ns += src->timing_primary_lookup_ns;
    stats_set_max(&dst->timing_primary_lookup_max_ns, src->timing_primary_lookup_max_ns);
    dst->timing_secondary_lookup_count += src->timing_secondary_lookup_count;
    dst->timing_secondary_lookup_ns += src->timing_secondary_lookup_ns;
    stats_set_max(&dst->timing_secondary_lookup_max_ns, src->timing_secondary_lookup_max_ns);
    dst->timing_remote_meta_lookup_count += src->timing_remote_meta_lookup_count;
    dst->timing_remote_meta_lookup_ns += src->timing_remote_meta_lookup_ns;
    stats_set_max(&dst->timing_remote_meta_lookup_max_ns, src->timing_remote_meta_lookup_max_ns);
    dst->timing_payload_local_slice_count += src->timing_payload_local_slice_count;
    dst->timing_payload_local_slice_ns += src->timing_payload_local_slice_ns;
    stats_set_max(&dst->timing_payload_local_slice_max_ns, src->timing_payload_local_slice_max_ns);
    dst->timing_payload_remote_slice_count += src->timing_payload_remote_slice_count;
    dst->timing_payload_remote_slice_ns += src->timing_payload_remote_slice_ns;
    stats_set_max(&dst->timing_payload_remote_slice_max_ns, src->timing_payload_remote_slice_max_ns);
    dst->timing_compute_count += src->timing_compute_count;
    dst->timing_compute_ns += src->timing_compute_ns;
    stats_set_max(&dst->timing_compute_max_ns, src->timing_compute_max_ns);
}
