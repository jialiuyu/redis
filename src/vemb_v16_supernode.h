#ifndef __VEMB_V16_SUPERNODE_H
#define __VEMB_V16_SUPERNODE_H

#include "vemb_v16_aeron_ring.h"
#include "vemb_v16_dataplane.h"
#include "vemb_v16_stats.h"
#include "vemb_v16_storage.h"

#include <stddef.h>
#include <stdatomic.h>
#include <stdint.h>

typedef struct vemb_v16_supernode_ctx {
    uint32_t worker_id;
    atomic_int *channel_active;
    atomic_int *running;
    atomic_int *completion_notify_armed;
    int *completion_notify_fd;
    vemb_v16_aeron_ring_t *completion_ring;
    vemb_v16_storage_ctx_t *storage;
    vemb_v16_channel_counters_t *stats;
    sve_operation_stats_t *sve_stats;
} vemb_v16_supernode_ctx_t;

typedef struct vemb_v16_supernode_scratch {
    vemb_v16_job_ref_t *job_refs;
} vemb_v16_supernode_scratch_t;

int vemb_v16_supernode_scratch_init(vemb_v16_supernode_scratch_t *scratch);
void vemb_v16_supernode_scratch_cleanup(vemb_v16_supernode_scratch_t *scratch);
void vemb_v16_supernode_handle_base_job(vemb_v16_supernode_ctx_t *ctx,
                                        const vemb_v16_job_base_t *job);
void vemb_v16_supernode_handle_vemb_job(vemb_v16_supernode_ctx_t *ctx,
                                        const vemb_v16_vemb_job_t *vemb_job);
void vemb_v16_supernode_handle_vsim_key_key_job(
    vemb_v16_supernode_ctx_t *ctx,
    const vemb_v16_vsim_key_key_job_t *vsim_job);
void vemb_v16_supernode_handle_vrem_job(vemb_v16_supernode_ctx_t *ctx,
                                        const vemb_v16_vemb_job_t *vrem_job);
void vemb_v16_supernode_handle_vadd_job(vemb_v16_supernode_ctx_t *ctx,
                                        const vemb_v16_vadd_job_t *vadd_job);

#endif
