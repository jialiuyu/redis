#ifndef __VEMB_V16_SUPERNODE_H
#define __VEMB_V16_SUPERNODE_H

#include "vemb_v16_aeron_ring.h"
#include "vemb_v16_dataplane.h"
#include "vemb_v16_tlc.h"

#include <stddef.h>
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
    atomic_int *completion_notify_armed;
    int *completion_notify_fd;
    vemb_v16_aeron_ring_t *vemb_job_ring;
    vemb_v16_aeron_ring_t *vadd_job_ring;
    vemb_v16_aeron_ring_t *completion_ring;
    vemb_v16_tlc_t *tlc;
    vemb_v16_channel_counters_t *stats;
    sve_operation_stats_t *sve_stats;
} vemb_v16_supernode_ctx_t;

typedef struct vemb_v16_supernode_scratch {
    vemb_v16_vemb_job_t *vemb_jobs;
    vemb_v16_vadd_job_t *vadd_jobs;
    float *read_result;
    size_t read_result_bytes;
} vemb_v16_supernode_scratch_t;

int vemb_v16_supernode_scratch_init(vemb_v16_supernode_scratch_t *scratch);
void vemb_v16_supernode_scratch_cleanup(vemb_v16_supernode_scratch_t *scratch);
void vemb_v16_supernode_handle_vemb_job(vemb_v16_supernode_ctx_t *ctx,
                                        vemb_v16_vemb_job_t *vemb_job,
                                        float **read_result,
                                        size_t *read_result_bytes);
void vemb_v16_supernode_handle_vadd_job(vemb_v16_supernode_ctx_t *ctx,
                                        vemb_v16_vadd_job_t *vadd_job);
int vemb_v16_supernode_drain(vemb_v16_supernode_ctx_t *ctx,
                             vemb_v16_supernode_scratch_t *scratch);
void *vemb_v16_supernode_thread_main(void *arg);

#endif
