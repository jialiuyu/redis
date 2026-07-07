#ifndef __VEMB_V16_STATS_H
#define __VEMB_V16_STATS_H

#include "vemb_v16_protocol.h"

#include <stdatomic.h>
#include <stdint.h>

typedef enum vemb_v16_timing_stage {
    VEMB_V16_TIMING_JOB_TOTAL = 0,
    VEMB_V16_TIMING_PRIMARY_LOOKUP,
    VEMB_V16_TIMING_SECONDARY_LOOKUP,
    VEMB_V16_TIMING_REMOTE_META_LOOKUP,
    VEMB_V16_TIMING_PAYLOAD_LOCAL_SLICE,
    VEMB_V16_TIMING_PAYLOAD_REMOTE_SLICE,
    VEMB_V16_TIMING_COMPUTE,
} vemb_v16_timing_stage_t;

typedef struct vemb_v16_timing_acc {
    uint64_t count;
    uint64_t ns;
    uint64_t max_ns;
} vemb_v16_timing_acc_t;

typedef struct vemb_v16_channel_counters {
    atomic_uint_fast64_t total_requests;
    atomic_uint_fast64_t vadd_requests;
    atomic_uint_fast64_t vemb_requests;
    atomic_uint_fast64_t vsim_requests;
    atomic_uint_fast64_t not_found;
    atomic_uint_fast64_t published_jobs;
    atomic_uint_fast64_t completed_jobs;
    atomic_uint_fast64_t proxy_vemb_ring_full;
    atomic_uint_fast64_t proxy_vadd_ring_full;
    atomic_uint_fast64_t proxy_response_ring_full;
    atomic_uint_fast64_t supernode_completion_publish;
    atomic_uint_fast64_t supernode_completion_ring_full;
    atomic_uint_fast64_t sample_count;
    atomic_uint_fast64_t sample_table_lookup_ns;
    atomic_uint_fast64_t sample_bitmap_lock_ns;
    atomic_uint_fast64_t sample_bitmap_unlock_ns;
    atomic_uint_fast64_t sample_vector_load_ns;
    atomic_uint_fast64_t sample_completion_publish_ns;
    atomic_uint_fast64_t moved_count;
    atomic_uint_fast64_t stale_count;
    atomic_uint_fast64_t ask_count;
    atomic_uint_fast64_t forward_count;
    atomic_uint_fast64_t duplicate_request_count;
    atomic_uint_fast64_t timing_job_count;
    atomic_uint_fast64_t timing_job_total_ns;
    atomic_uint_fast64_t timing_job_total_max_ns;
    atomic_uint_fast64_t timing_primary_lookup_count;
    atomic_uint_fast64_t timing_primary_lookup_ns;
    atomic_uint_fast64_t timing_primary_lookup_max_ns;
    atomic_uint_fast64_t timing_secondary_lookup_count;
    atomic_uint_fast64_t timing_secondary_lookup_ns;
    atomic_uint_fast64_t timing_secondary_lookup_max_ns;
    atomic_uint_fast64_t timing_remote_meta_lookup_count;
    atomic_uint_fast64_t timing_remote_meta_lookup_ns;
    atomic_uint_fast64_t timing_remote_meta_lookup_max_ns;
    atomic_uint_fast64_t timing_payload_local_slice_count;
    atomic_uint_fast64_t timing_payload_local_slice_ns;
    atomic_uint_fast64_t timing_payload_local_slice_max_ns;
    atomic_uint_fast64_t timing_payload_remote_slice_count;
    atomic_uint_fast64_t timing_payload_remote_slice_ns;
    atomic_uint_fast64_t timing_payload_remote_slice_max_ns;
    atomic_uint_fast64_t timing_compute_count;
    atomic_uint_fast64_t timing_compute_ns;
    atomic_uint_fast64_t timing_compute_max_ns;
} vemb_v16_channel_counters_t;

void vemb_v16_timing_acc_add(vemb_v16_timing_acc_t *acc, uint64_t ns);
void vemb_v16_channel_counters_add_timing(vemb_v16_channel_counters_t *stats,
                                          vemb_v16_timing_stage_t stage,
                                          const vemb_v16_timing_acc_t *acc);
void vemb_v16_stats_add_channel_counters(vemb_v16_stats_t *dst,
                                         vemb_v16_channel_counters_t *src);
void vemb_v16_stats_add(vemb_v16_stats_t *dst, const vemb_v16_stats_t *src);

#endif
