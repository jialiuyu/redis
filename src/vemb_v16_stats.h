#ifndef __VEMB_V16_STATS_H
#define __VEMB_V16_STATS_H

#include "vemb_v16_protocol.h"

#include <stdatomic.h>
#include <stdint.h>

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
    atomic_uint_fast64_t moved_count;
    atomic_uint_fast64_t stale_count;
    atomic_uint_fast64_t ask_count;
    atomic_uint_fast64_t forward_count;
    atomic_uint_fast64_t duplicate_request_count;
} vemb_v16_channel_counters_t;

void vemb_v16_stats_add_channel_counters(vemb_v16_stats_t *dst,
                                         vemb_v16_channel_counters_t *src);
void vemb_v16_stats_add(vemb_v16_stats_t *dst, const vemb_v16_stats_t *src);

#endif
