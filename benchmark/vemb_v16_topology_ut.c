#include "../src/vemb_v16_topology.h"
#include "../src/vemb_v16_hash.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

static uint64_t hash_key_u32(const char *prefix, uint32_t i) {
    char key[64];
    snprintf(key, sizeof(key), "%s:%u", prefix, i);
    return vemb_v16_xxh3_64_str(key, strlen(key));
}

static int ring_has_vnode(const vemb_v16_topology_ring_t *ring,
                          const vemb_v16_topology_ring_node_t *node) {
    for (uint32_t i = 0; i < ring->node_count; i++) {
        if (ring->nodes[i].owner_id == node->owner_id &&
            ring->nodes[i].vnode_id == node->vnode_id &&
            ring->nodes[i].hash_value == node->hash_value) {
            return 1;
        }
    }
    return 0;
}

static void test_old_vnodes_stay_stable_after_insert(void) {
    vemb_v16_topology_ring_t old_ring;
    vemb_v16_topology_ring_t new_ring;
    const uint32_t old_owners[] = {1, 2};
    const uint32_t new_owners[] = {1, 2, 3};

    assert(vemb_v16_topology_ring_build(&old_ring,
                                        1,
                                        old_owners,
                                        2,
                                        10) == VEMB_V16_TOPOLOGY_OK);
    assert(vemb_v16_topology_ring_build(&new_ring,
                                        2,
                                        new_owners,
                                        3,
                                        10) == VEMB_V16_TOPOLOGY_OK);
    for (uint32_t i = 0; i < old_ring.node_count; i++)
        assert(ring_has_vnode(&new_ring, &old_ring.nodes[i]));
}

static void test_expand_plan_only_moves_to_added_owner(void) {
    vemb_v16_topology_ring_t old_ring;
    vemb_v16_topology_ring_t new_ring;
    uint64_t keys[1024];
    vemb_v16_topology_migrate_entry_t entries[1024];
    uint32_t entry_count = 0;
    const uint32_t old_owners[] = {1, 2};
    const uint32_t new_owners[] = {1, 2, 3};

    assert(vemb_v16_topology_ring_build(&old_ring,
                                        7,
                                        old_owners,
                                        2,
                                        10) == VEMB_V16_TOPOLOGY_OK);
    assert(vemb_v16_topology_ring_build(&new_ring,
                                        8,
                                        new_owners,
                                        3,
                                        10) == VEMB_V16_TOPOLOGY_OK);
    for (uint32_t i = 0; i < 1024; i++)
        keys[i] = hash_key_u32("expand", i);

    assert(vemb_v16_topology_build_expand_plan(&old_ring,
                                               &new_ring,
                                               3,
                                               keys,
                                               1024,
                                               entries,
                                               1024,
                                               &entry_count) ==
           VEMB_V16_TOPOLOGY_OK);
    assert(entry_count > 0);
    for (uint32_t i = 0; i < entry_count; i++) {
        assert(entries[i].target_owner == 3);
        assert(entries[i].source_owner == 1 || entries[i].source_owner == 2);
        assert(entries[i].source_owner != entries[i].target_owner);
        assert(entries[i].topology_epoch == 8);
    }

    for (uint32_t i = 0; i < 1024; i++) {
        uint32_t old_owner =
            vemb_v16_topology_ring_owner(&old_ring, keys[i]);
        uint32_t new_owner =
            vemb_v16_topology_ring_owner(&new_ring, keys[i]);
        assert(old_owner == new_owner || new_owner == 3);
    }
}

static void test_expand_plan_rejects_old_node_rebalance(void) {
    vemb_v16_topology_ring_t old_ring;
    vemb_v16_topology_ring_t new_ring;
    const uint32_t old_owners[] = {1, 2};
    const uint32_t new_owners[] = {2, 3};
    uint64_t bad_key = 0;
    uint32_t found = 0;
    vemb_v16_topology_migrate_entry_t entry;
    uint32_t entry_count = 0;

    assert(vemb_v16_topology_ring_build(&old_ring,
                                        1,
                                        old_owners,
                                        2,
                                        10) == VEMB_V16_TOPOLOGY_OK);
    assert(vemb_v16_topology_ring_build(&new_ring,
                                        2,
                                        new_owners,
                                        2,
                                        10) == VEMB_V16_TOPOLOGY_OK);
    for (uint32_t i = 0; i < 100000; i++) {
        uint64_t key_hash = hash_key_u32("rebalance", i);
        uint32_t old_owner =
            vemb_v16_topology_ring_owner(&old_ring, key_hash);
        uint32_t new_owner =
            vemb_v16_topology_ring_owner(&new_ring, key_hash);
        if (old_owner != new_owner && new_owner != 3) {
            bad_key = key_hash;
            found = 1;
            break;
        }
    }
    assert(found);
    assert(vemb_v16_topology_build_expand_plan(&old_ring,
                                               &new_ring,
                                               3,
                                               &bad_key,
                                               1,
                                               &entry,
                                               1,
                                               &entry_count) ==
           VEMB_V16_TOPOLOGY_UNSUPPORTED_REBALANCE);
}

int main(void) {
    test_old_vnodes_stay_stable_after_insert();
    test_expand_plan_only_moves_to_added_owner();
    test_expand_plan_rejects_old_node_rebalance();
    printf("vemb_v16_topology_ut: all tests passed\n");
    return 0;
}
