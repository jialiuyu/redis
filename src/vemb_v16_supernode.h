#ifndef __VEMB_V16_SUPERNODE_H
#define __VEMB_V16_SUPERNODE_H

#include "vemb_v16_aeron_ring.h"
#include "vemb_v16_tlc.h"

#include <stdatomic.h>
#include <stdint.h>

typedef struct vemb_v16_channel_counters {
    atomic_uint_fast64_t total_requests;
    atomic_uint_fast64_t vadd_requests;
    atomic_uint_fast64_t vemb_requests;
    atomic_uint_fast64_t not_found;
    atomic_uint_fast64_t published_jobs;
    atomic_uint_fast64_t completed_jobs;
    atomic_uint_fast64_t proxy_request_poll;
    atomic_uint_fast64_t proxy_completion_poll;
    atomic_uint_fast64_t proxy_vemb_publish;
    atomic_uint_fast64_t proxy_vadd_publish;
    atomic_uint_fast64_t proxy_vemb_ring_full;
    atomic_uint_fast64_t proxy_vadd_ring_full;
    atomic_uint_fast64_t proxy_response_publish;
    atomic_uint_fast64_t proxy_response_ring_full;
    atomic_uint_fast64_t supernode_vemb_poll;
    atomic_uint_fast64_t supernode_vadd_poll;
    atomic_uint_fast64_t supernode_completion_publish;
    atomic_uint_fast64_t supernode_completion_ring_full;
    atomic_uint_fast64_t sample_count;
    atomic_uint_fast64_t sample_table_lookup_ns;
    atomic_uint_fast64_t sample_bitmap_lock_ns;
    atomic_uint_fast64_t sample_bitmap_unlock_ns;
    atomic_uint_fast64_t sample_vector_load_ns;
    atomic_uint_fast64_t sample_completion_publish_ns;
    atomic_uint_fast64_t channel_ops;
} vemb_v16_channel_counters_t;

typedef struct vemb_v16_supernode_ctx {
    uint32_t worker_id;
    atomic_int *channel_active;
    atomic_int *running;
    vemb_v16_aeron_ring_t *vemb_job_ring;
    vemb_v16_aeron_ring_t *vadd_job_ring;
    vemb_v16_aeron_ring_t *completion_ring;
    vemb_v16_tlc_t *tlc;
    vemb_v16_channel_counters_t *stats;
    sve_operation_stats_t *sve_stats;
} vemb_v16_supernode_ctx_t;

void *vemb_v16_supernode_thread_main(void *arg);

#endif
