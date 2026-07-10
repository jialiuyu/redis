#include "vemb_v16_topology.h"

#include "vemb_v16_hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int cmp_u32(const void *a, const void *b) {
    uint32_t va = *(const uint32_t *)a;
    uint32_t vb = *(const uint32_t *)b;
    if (va < vb) return -1;
    if (va > vb) return 1;
    return 0;
}

static int ring_node_cmp(const void *a, const void *b) {
    const vemb_v16_topology_ring_node_t *ra = a;
    const vemb_v16_topology_ring_node_t *rb = b;
    if (ra->hash_value < rb->hash_value) return -1;
    if (ra->hash_value > rb->hash_value) return 1;
    if (ra->owner_id < rb->owner_id) return -1;
    if (ra->owner_id > rb->owner_id) return 1;
    if (ra->vnode_id < rb->vnode_id) return -1;
    if (ra->vnode_id > rb->vnode_id) return 1;
    return 0;
}

int vemb_v16_topology_owner_exists(
    const vemb_v16_topology_ring_t *ring,
    uint32_t owner_id) {
    if (!ring)
        return 0;
    for (uint32_t i = 0; i < ring->owner_count; i++) {
        if (ring->owners[i] == owner_id)
            return 1;
    }
    return 0;
}

int vemb_v16_topology_ring_build(vemb_v16_topology_ring_t *ring,
                                 uint64_t topology_epoch,
                                 const uint32_t *owners,
                                 uint32_t owner_count,
                                 uint32_t vnode_count) {
    if (!ring || !owners || owner_count == 0 ||
        owner_count > VEMB_V16_TOPOLOGY_MAX_OWNERS) {
        return VEMB_V16_TOPOLOGY_INVALID;
    }
    if (vnode_count == 0)
        vnode_count = VEMB_V16_TOPOLOGY_DEFAULT_VNODES;
    if (vnode_count > VEMB_V16_TOPOLOGY_MAX_VNODES_PER_OWNER ||
        owner_count * vnode_count > VEMB_V16_TOPOLOGY_MAX_RING_NODES) {
        return VEMB_V16_TOPOLOGY_INVALID;
    }

    memset(ring, 0, sizeof(*ring));
    ring->topology_epoch = topology_epoch;
    ring->owner_count = owner_count;
    ring->vnode_count = vnode_count;
    memcpy(ring->owners, owners, sizeof(uint32_t) * owner_count);
    qsort(ring->owners, ring->owner_count, sizeof(ring->owners[0]), cmp_u32);
    for (uint32_t i = 1; i < ring->owner_count; i++) {
        if (ring->owners[i - 1] == ring->owners[i])
            return VEMB_V16_TOPOLOGY_INVALID;
    }

    for (uint32_t i = 0; i < ring->owner_count; i++) {
        uint32_t owner_id = ring->owners[i];
        for (uint32_t vnode = 0; vnode < vnode_count; vnode++) {
            char vnode_key[64];
            snprintf(vnode_key,
                     sizeof(vnode_key),
                    "supernode_%u_vnode_%u",
                     owner_id,
                     vnode);
            ring->nodes[ring->node_count++] =
                (vemb_v16_topology_ring_node_t){
                    .hash_value = vemb_v16_xxh3_64_str(vnode_key,
                                                       strlen(vnode_key)),
                    .owner_id = owner_id,
                    .vnode_id = vnode,
                };
        }
    }
    qsort(ring->nodes,
          ring->node_count,
          sizeof(ring->nodes[0]),
          ring_node_cmp);
    return VEMB_V16_TOPOLOGY_OK;
}

uint32_t vemb_v16_topology_ring_owner(
    const vemb_v16_topology_ring_t *ring,
    uint64_t key_hash) {
    if (!ring || ring->node_count == 0)
        return UINT32_MAX;

    uint64_t hash = key_hash;
    uint32_t left = 0;
    uint32_t right = ring->node_count;
    while (left < right) {
        uint32_t mid = left + (right - left) / 2;
        if (ring->nodes[mid].hash_value < hash)
            left = mid + 1;
        else
            right = mid;
    }
    if (left >= ring->node_count)
        left = 0;
    return ring->nodes[left].owner_id;
}

int vemb_v16_topology_build_expand_plan(
    const vemb_v16_topology_ring_t *old_ring,
    const vemb_v16_topology_ring_t *new_ring,
    uint32_t added_owner,
    const uint64_t *key_hashes,
    uint32_t key_count,
    vemb_v16_topology_migrate_entry_t *entries,
    uint32_t max_entries,
    uint32_t *entry_count) {
    if (!old_ring || !new_ring || !key_hashes || !entry_count ||
        old_ring->node_count == 0 || new_ring->node_count == 0 ||
        !vemb_v16_topology_owner_exists(new_ring, added_owner) ||
        vemb_v16_topology_owner_exists(old_ring, added_owner)) {
        return VEMB_V16_TOPOLOGY_INVALID;
    }

    uint32_t out = 0;
    for (uint32_t i = 0; i < key_count; i++) {
        uint64_t key_hash = key_hashes[i];
        uint32_t old_owner =
            vemb_v16_topology_ring_owner(old_ring, key_hash);
        uint32_t new_owner =
            vemb_v16_topology_ring_owner(new_ring, key_hash);
        if (old_owner == UINT32_MAX || new_owner == UINT32_MAX)
            return VEMB_V16_TOPOLOGY_INVALID;
        if (old_owner == new_owner)
            continue;
        if (new_owner != added_owner)
            return VEMB_V16_TOPOLOGY_UNSUPPORTED_REBALANCE;
        if (!entries || out >= max_entries)
            return VEMB_V16_TOPOLOGY_NO_SPACE;
        entries[out++] = (vemb_v16_topology_migrate_entry_t){
            .key_hash = key_hash,
            .source_owner = old_owner,
            .target_owner = new_owner,
            .topology_epoch = new_ring->topology_epoch,
        };
    }
    *entry_count = out;
    return VEMB_V16_TOPOLOGY_OK;
}
