#include "consistent_hash.h"
#include "macro.h"

#include <string.h>

int consistent_hash_init(consistent_hash_ring_t **ring, int num_supernodes) {
    *ring = zmalloc(sizeof(consistent_hash_ring_t));
    RETURN_IF(!*ring, C_ERR);

    (*ring)->capacity = PROXY_HASH_RING_SIZE;
    (*ring)->num_nodes = 0;
    (*ring)->nodes = zmalloc(sizeof(hash_node_t) * (*ring)->capacity);

    if (!(*ring)->nodes) {
        zfree(*ring);
        return C_ERR;
    }

    pthread_rwlock_init(&(*ring)->lock, NULL);

    for (int i = 0; i < num_supernodes; i++) {
        for (int v = 0; v < 10; v++) {
            consistent_hash_add_node(*ring, i);
        }
    }

    serverLog(LL_NOTICE, "Consistent hash ring initialized with %d supernodes, %zu virtual nodes",
              num_supernodes, (*ring)->num_nodes);

    return C_OK;
}

void consistent_hash_destroy(consistent_hash_ring_t *ring) {
    RETURN_IF(!ring);

    pthread_rwlock_destroy(&ring->lock);
    zfree(ring->nodes);
    zfree(ring);
}

int consistent_hash_add_node(consistent_hash_ring_t *ring, int supernode_id) {
    RETURN_IF(!ring || ring->num_nodes >= ring->capacity, C_ERR);

    pthread_rwlock_wrlock(&ring->lock);

    char node_key[64];
    snprintf(node_key, sizeof(node_key), "supernode_%d_vnode_%zu",
             supernode_id, ring->num_nodes);

    uint32_t hash = murmur3_hash(node_key, strlen(node_key));

    size_t insert_pos = ring->num_nodes;
    for (size_t i = 0; i < ring->num_nodes; i++) {
        if (hash < ring->nodes[i].hash_value) {
            insert_pos = i;
            break;
        }
    }

    if (insert_pos < ring->num_nodes) {
        memmove(&ring->nodes[insert_pos + 1], &ring->nodes[insert_pos],
                (ring->num_nodes - insert_pos) * sizeof(hash_node_t));
    }

    ring->nodes[insert_pos].hash_value = hash;
    ring->nodes[insert_pos].supernode_id = supernode_id;
    ring->num_nodes++;

    pthread_rwlock_unlock(&ring->lock);
    return C_OK;
}

int consistent_hash_remove_node(consistent_hash_ring_t *ring, int supernode_id) {
    RETURN_IF(!ring, C_ERR);

    pthread_rwlock_wrlock(&ring->lock);

    size_t write_pos = 0;
    for (size_t i = 0; i < ring->num_nodes; i++) {
        if (ring->nodes[i].supernode_id != supernode_id) {
            if (write_pos != i) {
                ring->nodes[write_pos] = ring->nodes[i];
            }
            write_pos++;
        }
    }

    ring->num_nodes = write_pos;

    pthread_rwlock_unlock(&ring->lock);
    return C_OK;
}

int consistent_hash_get_node_by_hash(consistent_hash_ring_t *ring, uint32_t hash) {
    RETURN_IF(!ring || ring->num_nodes == 0, 0);

    pthread_rwlock_rdlock(&ring->lock);

    size_t left = 0, right = ring->num_nodes;
    while (left < right) {
        size_t mid = left + (right - left) / 2;
        if (ring->nodes[mid].hash_value < hash) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    if (left >= ring->num_nodes) left = 0;

    int supernode_id = ring->nodes[left].supernode_id;

    pthread_rwlock_unlock(&ring->lock);
    return supernode_id;
}

int consistent_hash_get_node(consistent_hash_ring_t *ring, const char *key) {
    RETURN_IF(!ring || ring->num_nodes == 0, 0);
    uint32_t hash = murmur3_hash(key, strlen(key));
    return consistent_hash_get_node_by_hash(ring, hash);
}
