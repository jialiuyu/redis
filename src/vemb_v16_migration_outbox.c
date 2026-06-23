#include "vemb_v16_migration_outbox.h"

#include <stdlib.h>
#include <string.h>

struct vemb_v16_migration_outbox {
    uint32_t source_owner;
    uint32_t target_owner;
    uint32_t shard_id;
    uint32_t capacity;
    uint32_t head;
    uint32_t count;
    uint32_t state;
    uint64_t topology_epoch;
    uint64_t next_delta_seq;
    uint64_t acked_seq;
    uint64_t barrier_seq;
    vemb_v16_ub_migration_delta_desc_t *entries;
};

static int valid_config(
    const vemb_v16_migration_outbox_config_t *config) {
    return config &&
           config->capacity > 0 &&
           config->source_owner != UINT32_MAX &&
           config->target_owner != UINT32_MAX &&
           config->source_owner != config->target_owner;
}

static int valid_delta(
    const vemb_v16_ub_migration_delta_desc_t *delta) {
    if (!delta ||
        delta->key_len == 0 ||
        delta->key_len > VEMB_V16_MAX_KEY_LEN) {
        return 0;
    }
    return delta->op == VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT ||
           delta->op == VEMB_V16_UB_MIGRATION_RPC_DELTA_DELETE;
}

static uint32_t tail_index(const vemb_v16_migration_outbox_t *outbox) {
    return (outbox->head + outbox->count) % outbox->capacity;
}

static uint64_t last_assigned_seq(
    const vemb_v16_migration_outbox_t *outbox) {
    return outbox->next_delta_seq == 0 ? 0 : outbox->next_delta_seq - 1;
}

static void maybe_mark_cutover_ready(
    vemb_v16_migration_outbox_t *outbox) {
    if (outbox->state == VEMB_V16_MIGRATION_OUTBOX_BARRIERED &&
        outbox->acked_seq >= outbox->barrier_seq) {
        outbox->state = VEMB_V16_MIGRATION_OUTBOX_CUTOVER_READY;
    }
}

int vemb_v16_migration_outbox_create(
    vemb_v16_migration_outbox_t **out,
    const vemb_v16_migration_outbox_config_t *config) {
    if (!out || !valid_config(config))
        return VEMB_V16_MIGRATION_OUTBOX_INVALID;

    vemb_v16_migration_outbox_t *outbox = calloc(1, sizeof(*outbox));
    if (!outbox)
        return VEMB_V16_MIGRATION_OUTBOX_NOMEM;
    outbox->entries = calloc(config->capacity, sizeof(outbox->entries[0]));
    if (!outbox->entries) {
        free(outbox);
        return VEMB_V16_MIGRATION_OUTBOX_NOMEM;
    }

    outbox->source_owner = config->source_owner;
    outbox->target_owner = config->target_owner;
    outbox->shard_id = config->shard_id;
    outbox->capacity = config->capacity;
    outbox->topology_epoch = config->topology_epoch;
    outbox->next_delta_seq = 1;
    outbox->state = VEMB_V16_MIGRATION_OUTBOX_OPEN;
    *out = outbox;
    return VEMB_V16_MIGRATION_OUTBOX_OK;
}

void vemb_v16_migration_outbox_destroy(
    vemb_v16_migration_outbox_t *outbox) {
    if (!outbox)
        return;
    free(outbox->entries);
    free(outbox);
}

int vemb_v16_migration_outbox_append(
    vemb_v16_migration_outbox_t *outbox,
    const vemb_v16_ub_migration_delta_desc_t *delta,
    vemb_v16_ub_migration_delta_desc_t *assigned_delta) {
    if (!outbox || !valid_delta(delta))
        return VEMB_V16_MIGRATION_OUTBOX_INVALID;
    if (outbox->state != VEMB_V16_MIGRATION_OUTBOX_OPEN)
        return VEMB_V16_MIGRATION_OUTBOX_BARRIER;
    if (outbox->count == outbox->capacity)
        return VEMB_V16_MIGRATION_OUTBOX_FULL;

    vemb_v16_ub_migration_delta_desc_t assigned;
    memset(&assigned, 0, sizeof(assigned));
    assigned = *delta;
    assigned.delta_seq = outbox->next_delta_seq++;
    assigned.topology_epoch = outbox->topology_epoch;
    assigned.source_owner = outbox->source_owner;
    assigned.target_owner = outbox->target_owner;
    assigned.shard_id = outbox->shard_id;
    if (assigned.op == VEMB_V16_UB_MIGRATION_RPC_DELTA_DELETE) {
        assigned.tombstone = 1;
        assigned.value_size = 0;
        assigned.bytes = 0;
        assigned.region_id = UINT32_MAX;
        assigned.local_slot = UINT32_MAX;
        assigned.offset = 0;
        assigned.owner_generation = 0;
    }

    outbox->entries[tail_index(outbox)] = assigned;
    outbox->count++;
    if (assigned_delta)
        *assigned_delta = assigned;
    return VEMB_V16_MIGRATION_OUTBOX_OK;
}

int vemb_v16_migration_outbox_peek(
    const vemb_v16_migration_outbox_t *outbox,
    vemb_v16_ub_migration_delta_desc_t *delta) {
    if (!outbox || !delta)
        return VEMB_V16_MIGRATION_OUTBOX_INVALID;
    if (outbox->count == 0)
        return VEMB_V16_MIGRATION_OUTBOX_EMPTY;
    *delta = outbox->entries[outbox->head];
    return VEMB_V16_MIGRATION_OUTBOX_OK;
}

int vemb_v16_migration_outbox_ack(
    vemb_v16_migration_outbox_t *outbox,
    uint64_t applied_seq) {
    if (!outbox)
        return VEMB_V16_MIGRATION_OUTBOX_INVALID;

    uint64_t capped_seq = applied_seq;
    uint64_t last_seq = last_assigned_seq(outbox);
    if (capped_seq > last_seq)
        capped_seq = last_seq;
    if (capped_seq > outbox->acked_seq)
        outbox->acked_seq = capped_seq;

    while (outbox->count > 0 &&
           outbox->entries[outbox->head].delta_seq <= outbox->acked_seq) {
        memset(&outbox->entries[outbox->head],
               0,
               sizeof(outbox->entries[outbox->head]));
        outbox->head = (outbox->head + 1) % outbox->capacity;
        outbox->count--;
    }
    maybe_mark_cutover_ready(outbox);
    return VEMB_V16_MIGRATION_OUTBOX_OK;
}

int vemb_v16_migration_outbox_barrier(
    vemb_v16_migration_outbox_t *outbox,
    uint64_t *barrier_seq) {
    return vemb_v16_migration_outbox_checkpoint(outbox, barrier_seq);
}

int vemb_v16_migration_outbox_checkpoint(
    vemb_v16_migration_outbox_t *outbox,
    uint64_t *checkpoint_seq) {
    if (!outbox)
        return VEMB_V16_MIGRATION_OUTBOX_INVALID;
    if (outbox->state == VEMB_V16_MIGRATION_OUTBOX_OPEN) {
        outbox->barrier_seq = last_assigned_seq(outbox);
    }
    if (checkpoint_seq)
        *checkpoint_seq = outbox->barrier_seq;
    return VEMB_V16_MIGRATION_OUTBOX_OK;
}

int vemb_v16_migration_outbox_begin_final_fence(
    vemb_v16_migration_outbox_t *outbox,
    uint64_t *final_barrier_seq) {
    if (!outbox)
        return VEMB_V16_MIGRATION_OUTBOX_INVALID;
    if (outbox->state == VEMB_V16_MIGRATION_OUTBOX_OPEN) {
        outbox->barrier_seq = last_assigned_seq(outbox);
        outbox->state = VEMB_V16_MIGRATION_OUTBOX_FENCING;
        maybe_mark_cutover_ready(outbox);
    }
    if (final_barrier_seq)
        *final_barrier_seq = outbox->barrier_seq;
    return VEMB_V16_MIGRATION_OUTBOX_OK;
}

int vemb_v16_migration_outbox_abort_final_fence(
    vemb_v16_migration_outbox_t *outbox) {
    if (!outbox)
        return VEMB_V16_MIGRATION_OUTBOX_INVALID;
    if (outbox->state == VEMB_V16_MIGRATION_OUTBOX_FENCING ||
        outbox->state == VEMB_V16_MIGRATION_OUTBOX_CUTOVER_READY) {
        outbox->state = VEMB_V16_MIGRATION_OUTBOX_OPEN;
    }
    return VEMB_V16_MIGRATION_OUTBOX_OK;
}

int vemb_v16_migration_outbox_cutover_ready(
    const vemb_v16_migration_outbox_t *outbox) {
    return outbox &&
           outbox->state == VEMB_V16_MIGRATION_OUTBOX_CUTOVER_READY;
}

void vemb_v16_migration_outbox_get_stats(
    const vemb_v16_migration_outbox_t *outbox,
    vemb_v16_migration_outbox_stats_t *stats) {
    if (!stats)
        return;
    memset(stats, 0, sizeof(*stats));
    if (!outbox)
        return;
    stats->state = outbox->state;
    stats->capacity = outbox->capacity;
    stats->pending_count = outbox->count;
    stats->shard_id = outbox->shard_id;
    stats->source_owner = outbox->source_owner;
    stats->target_owner = outbox->target_owner;
    stats->topology_epoch = outbox->topology_epoch;
    stats->next_delta_seq = outbox->next_delta_seq;
    stats->acked_seq = outbox->acked_seq;
    stats->barrier_seq = outbox->barrier_seq;
}
