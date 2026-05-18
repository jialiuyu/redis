#include "macro.h"
#include "proxy_router.h"

#include <string.h>

int proxy_router_init(proxy_router_t *router, int num_supernodes, size_t workers_per_node) {
    RETURN_IF(!router || num_supernodes <= 0 || workers_per_node == 0, C_ERR);

    router->hash_ring = NULL;
    router->workers_per_node = workers_per_node;
    RETURN_IF(consistent_hash_init(&router->hash_ring, num_supernodes) != C_OK, C_ERR);
    return C_OK;
}

void proxy_router_cleanup(proxy_router_t *router) {
    RETURN_IF(!router);

    if (router->hash_ring) {
        consistent_hash_destroy(router->hash_ring);
        router->hash_ring = NULL;
    }
    router->workers_per_node = 0;
}

int proxy_router_route(proxy_router_t *router, const char *key, proxy_route_t *route) {
    RETURN_IF(!router || !key || !route || !router->hash_ring || router->workers_per_node == 0,
              C_ERR);

    route->key_hash = murmur3_hash(key, strlen(key));
    route->supernode_id = consistent_hash_get_node_by_hash(router->hash_ring, route->key_hash);
    route->worker_id = (int)(route->key_hash % router->workers_per_node);
    return C_OK;
}

int proxy_router_route_by_row(proxy_router_t *router, const char *key,
                              uint64_t row_id, proxy_route_t *route) {
    RETURN_IF(proxy_router_route(router, key, route) != C_OK, C_ERR);

    route->worker_id = (int)(row_id % router->workers_per_node);
    return C_OK;
}

int proxy_router_route_by_request(proxy_router_t *router, const char *key,
                                  uint64_t request_id, proxy_route_t *route) {
    RETURN_IF(proxy_router_route(router, key, route) != C_OK, C_ERR);

    route->worker_id = (int)(request_id % router->workers_per_node);
    return C_OK;
}
