#include "../src/vemb_v16_migration_outbox.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

static vemb_v16_migration_outbox_t *new_outbox(uint32_t capacity) {
    vemb_v16_migration_outbox_t *outbox = NULL;
    vemb_v16_migration_outbox_config_t cfg = {
        .source_owner = 1,
        .target_owner = 3,
        .shard_id = 42,
        .capacity = capacity,
        .topology_epoch = 7,
    };
    assert(vemb_v16_migration_outbox_create(&outbox, &cfg) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(outbox);
    return outbox;
}

static vemb_v16_ub_migration_delta_desc_t make_delta(
        uint32_t op,
        const char *key,
        uint64_t key_hash,
        uint64_t key_version) {
    vemb_v16_ub_migration_delta_desc_t delta;
    memset(&delta, 0, sizeof(delta));
    delta.op = op;
    delta.key_hash = key_hash;
    delta.key_version = key_version;
    delta.key_len = (uint32_t)strlen(key);
    memcpy(delta.key, key, delta.key_len);
    if (op == VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT) {
        delta.value_size = 1200;
        delta.region_id = 11;
        delta.local_slot = 9;
        delta.bytes = 1200;
        delta.offset = 4096;
        delta.owner_generation = 77;
    } else {
        delta.tombstone = 1;
    }
    return delta;
}

static void test_append_ack_and_peek(void) {
    vemb_v16_migration_outbox_t *outbox = new_outbox(3);
    vemb_v16_ub_migration_delta_desc_t in;
    vemb_v16_ub_migration_delta_desc_t assigned;
    vemb_v16_ub_migration_delta_desc_t peeked;
    vemb_v16_migration_outbox_stats_t stats;

    in = make_delta(VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT,
                    "key:1",
                    101,
                    10);
    assert(vemb_v16_migration_outbox_append(outbox, &in, &assigned) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(assigned.delta_seq == 1);
    assert(assigned.topology_epoch == 7);
    assert(assigned.source_owner == 1);
    assert(assigned.target_owner == 3);
    assert(assigned.shard_id == 42);
    assert(assigned.bytes == 1200);

    in = make_delta(VEMB_V16_UB_MIGRATION_RPC_DELTA_DELETE,
                    "key:2",
                    102,
                    11);
    in.bytes = 999;
    in.region_id = 99;
    assert(vemb_v16_migration_outbox_append(outbox, &in, &assigned) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(assigned.delta_seq == 2);
    assert(assigned.tombstone == 1);
    assert(assigned.bytes == 0);
    assert(assigned.region_id == UINT32_MAX);
    assert(assigned.local_slot == UINT32_MAX);

    vemb_v16_migration_outbox_get_stats(outbox, &stats);
    assert(stats.pending_count == 2);
    assert(stats.next_delta_seq == 3);
    assert(stats.acked_seq == 0);
    assert(stats.state == VEMB_V16_MIGRATION_OUTBOX_OPEN);

    assert(vemb_v16_migration_outbox_peek(outbox, &peeked) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(peeked.delta_seq == 1);
    assert(peeked.key_hash == 101);

    assert(vemb_v16_migration_outbox_ack(outbox, 1) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    vemb_v16_migration_outbox_get_stats(outbox, &stats);
    assert(stats.pending_count == 1);
    assert(stats.acked_seq == 1);
    assert(vemb_v16_migration_outbox_peek(outbox, &peeked) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(peeked.delta_seq == 2);

    assert(vemb_v16_migration_outbox_ack(outbox, 100) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    vemb_v16_migration_outbox_get_stats(outbox, &stats);
    assert(stats.pending_count == 0);
    assert(stats.acked_seq == 2);
    assert(vemb_v16_migration_outbox_peek(outbox, &peeked) ==
           VEMB_V16_MIGRATION_OUTBOX_EMPTY);

    in = make_delta(VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT,
                    "key:3",
                    103,
                    12);
    assert(vemb_v16_migration_outbox_append(outbox, &in, &assigned) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(assigned.delta_seq == 3);

    vemb_v16_migration_outbox_destroy(outbox);
}

static void test_full_outbox(void) {
    vemb_v16_migration_outbox_t *outbox = new_outbox(2);
    vemb_v16_ub_migration_delta_desc_t in =
        make_delta(VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT,
                   "full:1",
                   201,
                   1);
    assert(vemb_v16_migration_outbox_append(outbox, &in, NULL) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    in = make_delta(VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT,
                    "full:2",
                    202,
                    2);
    assert(vemb_v16_migration_outbox_append(outbox, &in, NULL) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    in = make_delta(VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT,
                    "full:3",
                    203,
                    3);
    assert(vemb_v16_migration_outbox_append(outbox, &in, NULL) ==
           VEMB_V16_MIGRATION_OUTBOX_FULL);
    vemb_v16_migration_outbox_destroy(outbox);
}

static void test_checkpoint_allows_append_and_final_fence_blocks(void) {
    vemb_v16_migration_outbox_t *outbox = new_outbox(4);
    vemb_v16_ub_migration_delta_desc_t in =
        make_delta(VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT,
                   "barrier:1",
                   301,
                   1);
    uint64_t barrier_seq = 0;
    vemb_v16_ub_migration_delta_desc_t assigned;
    vemb_v16_migration_outbox_stats_t stats;

    assert(vemb_v16_migration_outbox_append(outbox, &in, NULL) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    in = make_delta(VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT,
                    "barrier:2",
                    302,
                    2);
    assert(vemb_v16_migration_outbox_append(outbox, &in, NULL) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(vemb_v16_migration_outbox_barrier(outbox, &barrier_seq) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(barrier_seq == 2);
    assert(!vemb_v16_migration_outbox_cutover_ready(outbox));
    assert(vemb_v16_migration_outbox_append(outbox, &in, &assigned) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(assigned.delta_seq == 3);

    assert(vemb_v16_migration_outbox_ack(outbox, 2) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(!vemb_v16_migration_outbox_cutover_ready(outbox));
    assert(vemb_v16_migration_outbox_begin_final_fence(outbox,
                                                       &barrier_seq) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(barrier_seq == 3);
    assert(vemb_v16_migration_outbox_append(outbox, &in, NULL) ==
           VEMB_V16_MIGRATION_OUTBOX_BARRIER);
    assert(vemb_v16_migration_outbox_abort_final_fence(outbox) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    vemb_v16_migration_outbox_get_stats(outbox, &stats);
    assert(stats.state == VEMB_V16_MIGRATION_OUTBOX_OPEN);
    assert(vemb_v16_migration_outbox_append(outbox, &in, &assigned) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(assigned.delta_seq == 4);
    assert(vemb_v16_migration_outbox_begin_final_fence(outbox,
                                                       &barrier_seq) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(barrier_seq == 4);
    assert(vemb_v16_migration_outbox_ack(outbox, 4) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(vemb_v16_migration_outbox_cutover_ready(outbox));
    vemb_v16_migration_outbox_get_stats(outbox, &stats);
    assert(stats.pending_count == 0);
    assert(stats.barrier_seq == 4);
    assert(stats.state == VEMB_V16_MIGRATION_OUTBOX_CUTOVER_READY);

    barrier_seq = 0;
    assert(vemb_v16_migration_outbox_barrier(outbox, &barrier_seq) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(barrier_seq == 4);
    vemb_v16_migration_outbox_destroy(outbox);
}

static void test_empty_final_fence_is_ready(void) {
    vemb_v16_migration_outbox_t *outbox = new_outbox(1);
    uint64_t barrier_seq = 99;
    assert(vemb_v16_migration_outbox_barrier(outbox, &barrier_seq) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(barrier_seq == 0);
    assert(!vemb_v16_migration_outbox_cutover_ready(outbox));
    assert(vemb_v16_migration_outbox_begin_final_fence(outbox,
                                                       &barrier_seq) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(barrier_seq == 0);
    assert(vemb_v16_migration_outbox_cutover_ready(outbox));
    vemb_v16_migration_outbox_destroy(outbox);
}

static void test_invalid_inputs(void) {
    vemb_v16_migration_outbox_t *outbox = NULL;
    vemb_v16_migration_outbox_config_t cfg = {
        .source_owner = 1,
        .target_owner = 1,
        .capacity = 1,
        .topology_epoch = 1,
    };
    vemb_v16_ub_migration_delta_desc_t bad =
        make_delta(VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT,
                   "bad",
                   401,
                   1);
    assert(vemb_v16_migration_outbox_create(&outbox, &cfg) ==
           VEMB_V16_MIGRATION_OUTBOX_INVALID);
    outbox = new_outbox(1);
    bad.key_len = 0;
    assert(vemb_v16_migration_outbox_append(outbox, &bad, NULL) ==
           VEMB_V16_MIGRATION_OUTBOX_INVALID);
    bad = make_delta(0xff, "bad", 402, 2);
    assert(vemb_v16_migration_outbox_append(outbox, &bad, NULL) ==
           VEMB_V16_MIGRATION_OUTBOX_INVALID);
    vemb_v16_migration_outbox_destroy(outbox);
}

int main(void) {
    test_append_ack_and_peek();
    test_full_outbox();
    test_checkpoint_allows_append_and_final_fence_blocks();
    test_empty_final_fence_is_ready();
    test_invalid_inputs();
    printf("vemb_v16_migration_outbox_ut: all tests passed\n");
    return 0;
}
