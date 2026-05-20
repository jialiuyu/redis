#ifndef __VEMB_V16_SUPERNODE_H
#define __VEMB_V16_SUPERNODE_H

#include "vemb_v16_aeron_ring.h"
#include "vemb_v16_table.h"

#include <stdatomic.h>
#include <stdint.h>

typedef struct vemb_v16_supernode_ctx {
    uint32_t worker_id;
    volatile int *channel_active;
    atomic_int *running;
    vemb_v16_aeron_ring_t *vemb_job_ring;
    vemb_v16_aeron_ring_t *vadd_job_ring;
    vemb_v16_aeron_ring_t *completion_ring;
    vemb_v16_table_t *table;
    atomic_uint_fast64_t *vadd_requests;
    atomic_uint_fast64_t *vemb_requests;
    atomic_uint_fast64_t *not_found;
    atomic_uint_fast64_t *completed_jobs;
    atomic_uint_fast64_t *supernode_vemb_poll;
    atomic_uint_fast64_t *supernode_vadd_poll;
    atomic_uint_fast64_t *supernode_completion_publish;
    atomic_uint_fast64_t *supernode_completion_ring_full;
    atomic_uint_fast64_t *sample_count;
    atomic_uint_fast64_t *sample_table_lookup_ns;
    atomic_uint_fast64_t *sample_bitmap_lock_ns;
    atomic_uint_fast64_t *sample_bitmap_unlock_ns;
    atomic_uint_fast64_t *sample_vector_load_ns;
    atomic_uint_fast64_t *sample_completion_publish_ns;
} vemb_v16_supernode_ctx_t;

void *vemb_v16_supernode_thread_main(void *arg);

#endif
