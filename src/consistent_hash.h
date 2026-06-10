#ifndef __CONSISTENT_HASH_H
#define __CONSISTENT_HASH_H

#include "server.h"
#include "vemb_v16_hash.h"
#include <pthread.h>
#include <stdint.h>

#define PROXY_HASH_RING_SIZE 1024

typedef struct hash_node {
    uint32_t hash_value;                /* 哈希值 */
    int supernode_id;                   /* 超节点 ID */
} hash_node_t;

typedef struct consistent_hash_ring {
    hash_node_t *nodes;                 /* 哈希节点数组 */
    size_t num_nodes;                   /* 节点数量 */
    size_t capacity;                    /* 容量 */
    pthread_rwlock_t lock;              /* 读写锁 */
} consistent_hash_ring_t;

int consistent_hash_init(consistent_hash_ring_t **ring, int num_supernodes);
void consistent_hash_destroy(consistent_hash_ring_t *ring);
int consistent_hash_get_node_by_hash(consistent_hash_ring_t *ring, uint32_t hash);
int consistent_hash_get_node(consistent_hash_ring_t *ring, const char *key);
int consistent_hash_add_node(consistent_hash_ring_t *ring, int supernode_id);
int consistent_hash_remove_node(consistent_hash_ring_t *ring, int supernode_id);

#endif /* __CONSISTENT_HASH_H */
