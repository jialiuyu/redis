#ifndef __VEMB_V16_TOPOLOGY_H
#define __VEMB_V16_TOPOLOGY_H

#include <stddef.h>
#include <stdint.h>

#define VEMB_V16_TOPOLOGY_DEFAULT_VNODES 10u
#define VEMB_V16_TOPOLOGY_MAX_OWNERS 64u
#define VEMB_V16_TOPOLOGY_MAX_VNODES_PER_OWNER 128u
#define VEMB_V16_TOPOLOGY_MAX_RING_NODES \
    (VEMB_V16_TOPOLOGY_MAX_OWNERS * VEMB_V16_TOPOLOGY_MAX_VNODES_PER_OWNER)

enum vemb_v16_topology_rc {
    VEMB_V16_TOPOLOGY_OK = 0,
    VEMB_V16_TOPOLOGY_INVALID = -1,
    VEMB_V16_TOPOLOGY_NO_SPACE = -2,
    VEMB_V16_TOPOLOGY_UNSUPPORTED_REBALANCE = -3,
};

typedef struct vemb_v16_topology_ring_node {
    uint32_t hash_value;
    uint32_t owner_id;
    uint32_t vnode_id;
} vemb_v16_topology_ring_node_t;

typedef struct vemb_v16_topology_ring {
    uint64_t topology_epoch;
    uint32_t owner_count;
    uint32_t vnode_count;
    uint32_t node_count;
    uint32_t owners[VEMB_V16_TOPOLOGY_MAX_OWNERS];
    vemb_v16_topology_ring_node_t
        nodes[VEMB_V16_TOPOLOGY_MAX_RING_NODES];
} vemb_v16_topology_ring_t;

typedef struct vemb_v16_topology_migrate_entry {
    uint64_t key_hash;
    uint32_t source_owner;
    uint32_t target_owner;
    uint64_t topology_epoch;
} vemb_v16_topology_migrate_entry_t;

int vemb_v16_topology_ring_build(vemb_v16_topology_ring_t *ring,
                                 uint64_t topology_epoch,
                                 const uint32_t *owners,
                                 uint32_t owner_count,
                                 uint32_t vnode_count);
uint32_t vemb_v16_topology_ring_owner(
    const vemb_v16_topology_ring_t *ring,
    uint64_t key_hash);
int vemb_v16_topology_owner_exists(
    const vemb_v16_topology_ring_t *ring,
    uint32_t owner_id);
int vemb_v16_topology_build_expand_plan(
    const vemb_v16_topology_ring_t *old_ring,
    const vemb_v16_topology_ring_t *new_ring,
    uint32_t added_owner,
    const uint64_t *key_hashes,
    uint32_t key_count,
    vemb_v16_topology_migrate_entry_t *entries,
    uint32_t max_entries,
    uint32_t *entry_count);

#endif
