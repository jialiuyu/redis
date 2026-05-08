#ifndef __PROXY_ROUTER_H
#define __PROXY_ROUTER_H

#include "consistent_hash.h"

#include <stddef.h>
#include <stdint.h>

typedef struct proxy_route {
    int supernode_id;
    int worker_id;
    uint32_t key_hash;
} proxy_route_t;

typedef struct proxy_router {
    consistent_hash_ring_t *hash_ring;
    size_t workers_per_node;
} proxy_router_t;

int proxy_router_init(proxy_router_t *router, int num_supernodes, size_t workers_per_node);
void proxy_router_cleanup(proxy_router_t *router);
int proxy_router_route(proxy_router_t *router, const char *key, proxy_route_t *route);

#endif /* __PROXY_ROUTER_H */
