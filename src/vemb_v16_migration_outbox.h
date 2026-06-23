#ifndef __VEMB_V16_MIGRATION_OUTBOX_H
#define __VEMB_V16_MIGRATION_OUTBOX_H

#include "vemb_v16_protocol.h"

#include <stdint.h>

typedef enum vemb_v16_migration_outbox_rc {
    VEMB_V16_MIGRATION_OUTBOX_OK = 0,
    VEMB_V16_MIGRATION_OUTBOX_INVALID = -1,
    VEMB_V16_MIGRATION_OUTBOX_FULL = -2,
    VEMB_V16_MIGRATION_OUTBOX_EMPTY = -3,
    VEMB_V16_MIGRATION_OUTBOX_BARRIER = -4,
    VEMB_V16_MIGRATION_OUTBOX_NOMEM = -5,
} vemb_v16_migration_outbox_rc_t;

typedef enum vemb_v16_migration_outbox_state {
    VEMB_V16_MIGRATION_OUTBOX_OPEN = 0,
    VEMB_V16_MIGRATION_OUTBOX_BARRIERED = 1,
    VEMB_V16_MIGRATION_OUTBOX_FENCING = 1,
    VEMB_V16_MIGRATION_OUTBOX_CUTOVER_READY = 2,
} vemb_v16_migration_outbox_state_t;

typedef struct vemb_v16_migration_outbox_config {
    uint32_t source_owner;
    uint32_t target_owner;
    uint32_t shard_id;
    uint32_t capacity;
    uint64_t topology_epoch;
} vemb_v16_migration_outbox_config_t;

typedef struct vemb_v16_migration_outbox_stats {
    uint32_t state;
    uint32_t capacity;
    uint32_t pending_count;
    uint32_t shard_id;
    uint32_t source_owner;
    uint32_t target_owner;
    uint64_t topology_epoch;
    uint64_t next_delta_seq;
    uint64_t acked_seq;
    uint64_t barrier_seq;
} vemb_v16_migration_outbox_stats_t;

typedef struct vemb_v16_migration_outbox vemb_v16_migration_outbox_t;

int vemb_v16_migration_outbox_create(
    vemb_v16_migration_outbox_t **out,
    const vemb_v16_migration_outbox_config_t *config);
void vemb_v16_migration_outbox_destroy(
    vemb_v16_migration_outbox_t *outbox);
int vemb_v16_migration_outbox_append(
    vemb_v16_migration_outbox_t *outbox,
    const vemb_v16_ub_migration_delta_desc_t *delta,
    vemb_v16_ub_migration_delta_desc_t *assigned_delta);
int vemb_v16_migration_outbox_peek(
    const vemb_v16_migration_outbox_t *outbox,
    vemb_v16_ub_migration_delta_desc_t *delta);
int vemb_v16_migration_outbox_ack(
    vemb_v16_migration_outbox_t *outbox,
    uint64_t applied_seq);
int vemb_v16_migration_outbox_barrier(
    vemb_v16_migration_outbox_t *outbox,
    uint64_t *barrier_seq);
int vemb_v16_migration_outbox_checkpoint(
    vemb_v16_migration_outbox_t *outbox,
    uint64_t *checkpoint_seq);
int vemb_v16_migration_outbox_begin_final_fence(
    vemb_v16_migration_outbox_t *outbox,
    uint64_t *final_barrier_seq);
int vemb_v16_migration_outbox_abort_final_fence(
    vemb_v16_migration_outbox_t *outbox);
int vemb_v16_migration_outbox_cutover_ready(
    const vemb_v16_migration_outbox_t *outbox);
void vemb_v16_migration_outbox_get_stats(
    const vemb_v16_migration_outbox_t *outbox,
    vemb_v16_migration_outbox_stats_t *stats);

#endif
