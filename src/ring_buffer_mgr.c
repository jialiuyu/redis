#include "ring_buffer_mgr.h"

#include "macro.h"
#include "server.h"

#include <pthread.h>
#include <stdio.h>

typedef struct ring_buffer_mgr {
    ring_buffer_t **request_rings;
    ring_buffer_t **response_rings;
    size_t num_supernodes;
    size_t workers_per_node;
    size_t ring_buffer_size;
    size_t refcount;
} ring_buffer_mgr_t;

static ring_buffer_mgr_t *mgr = NULL;
static pthread_mutex_t mgr_lock = PTHREAD_MUTEX_INITIALIZER;

static inline size_t ring_buffer_mgr_index(size_t workers_per_node,
                                           size_t supernode_id,
                                           size_t worker_id) {
    return supernode_id * workers_per_node + worker_id;
}

static void ring_buffer_mgr_release_locked(void) {
    if (!mgr) {
        return;
    }

    if (mgr->request_rings) {
        size_t total = mgr->num_supernodes * mgr->workers_per_node;
        for (size_t i = 0; i < total; i++) {
            if (mgr->request_rings[i]) {
                ring_buffer_destroy(mgr->request_rings[i]);
            }
            if (mgr->response_rings && mgr->response_rings[i]) {
                ring_buffer_destroy(mgr->response_rings[i]);
            }
        }
    }

    zfree(mgr->request_rings);
    zfree(mgr->response_rings);
    zfree(mgr);
    mgr = NULL;
}

int ring_buffer_mgr_init(size_t workers_per_node, size_t ring_buffer_size) {
    RETURN_IF(workers_per_node == 0 || ring_buffer_size == 0, C_ERR);

    pthread_mutex_lock(&mgr_lock);
    if (mgr) {
        if (mgr->workers_per_node != workers_per_node ||
            mgr->ring_buffer_size != ring_buffer_size) {
            pthread_mutex_unlock(&mgr_lock);
            serverLog(LL_WARNING,
                      "Ring buffer manager configuration mismatch: existing workers=%zu size=%zu requested workers=%zu size=%zu",
                      mgr->workers_per_node, mgr->ring_buffer_size,
                      workers_per_node, ring_buffer_size);
            return C_ERR;
        }
        mgr->refcount++;
        pthread_mutex_unlock(&mgr_lock);
        return C_OK;
    }

    mgr = zcalloc(sizeof(*mgr));
    if (!mgr) {
        pthread_mutex_unlock(&mgr_lock);
        return C_ERR;
    }

    mgr->workers_per_node = workers_per_node;
    mgr->ring_buffer_size = ring_buffer_size;
    mgr->refcount = 1;

    pthread_mutex_unlock(&mgr_lock);
    serverLog(LL_NOTICE, "Ring buffer manager initialized with %zu workers per supernode",
              workers_per_node);
    return C_OK;
}

int ring_buffer_mgr_ensure_supernodes(size_t num_supernodes) {
    RETURN_IF(num_supernodes == 0, C_ERR);

    pthread_mutex_lock(&mgr_lock);
    if (!mgr) {
        pthread_mutex_unlock(&mgr_lock);
        return C_ERR;
    }
    if (num_supernodes <= mgr->num_supernodes) {
        pthread_mutex_unlock(&mgr_lock);
        return C_OK;
    }

    size_t old_total = mgr->num_supernodes * mgr->workers_per_node;
    size_t new_total = num_supernodes * mgr->workers_per_node;
    ring_buffer_t **new_request_rings =
        zrealloc(mgr->request_rings, sizeof(*new_request_rings) * new_total);
    if (!new_request_rings) {
        pthread_mutex_unlock(&mgr_lock);
        return C_ERR;
    }
    ring_buffer_t **new_response_rings =
        zrealloc(mgr->response_rings, sizeof(*new_response_rings) * new_total);
    if (!new_response_rings) {
        pthread_mutex_unlock(&mgr_lock);
        return C_ERR;
    }

    mgr->request_rings = new_request_rings;
    mgr->response_rings = new_response_rings;
    for (size_t i = old_total; i < new_total; i++) {
        mgr->request_rings[i] = NULL;
        mgr->response_rings[i] = NULL;
    }

    for (size_t sn = mgr->num_supernodes; sn < num_supernodes; sn++) {
        for (size_t worker = 0; worker < mgr->workers_per_node; worker++) {
            size_t idx = ring_buffer_mgr_index(mgr->workers_per_node, sn, worker);
            char req_name[64];
            char resp_name[64];

            snprintf(req_name, sizeof(req_name), "supernode_%zu_worker_%zu_req", sn, worker);
            mgr->request_rings[idx] = ring_buffer_create(mgr->ring_buffer_size, req_name);
            if (!mgr->request_rings[idx]) {
                pthread_mutex_unlock(&mgr_lock);
                serverLog(LL_WARNING,
                          "Failed to create request ring for supernode %zu worker %zu",
                          sn, worker);
                return C_ERR;
            }

            snprintf(resp_name, sizeof(resp_name), "supernode_%zu_worker_%zu_resp", sn, worker);
            mgr->response_rings[idx] = ring_buffer_create(mgr->ring_buffer_size, resp_name);
            if (!mgr->response_rings[idx]) {
                pthread_mutex_unlock(&mgr_lock);
                serverLog(LL_WARNING,
                          "Failed to create response ring for supernode %zu worker %zu",
                          sn, worker);
                return C_ERR;
            }
        }
    }

    mgr->num_supernodes = num_supernodes;
    pthread_mutex_unlock(&mgr_lock);
    return C_OK;
}

void ring_buffer_mgr_shutdown(void) {
    pthread_mutex_lock(&mgr_lock);
    if (!mgr) {
        pthread_mutex_unlock(&mgr_lock);
        return;
    }

    if (mgr->refcount > 1) {
        mgr->refcount--;
        pthread_mutex_unlock(&mgr_lock);
        return;
    }

    ring_buffer_mgr_release_locked();
    pthread_mutex_unlock(&mgr_lock);
    serverLog(LL_NOTICE, "Ring buffer manager shutdown");
}

ring_buffer_t *ring_buffer_mgr_get_request(int supernode_id, int worker_id) {
    RETURN_IF(!mgr || supernode_id < 0 || worker_id < 0, NULL);
    RETURN_IF((size_t)supernode_id >= mgr->num_supernodes, NULL);
    RETURN_IF((size_t)worker_id >= mgr->workers_per_node, NULL);

    size_t idx = ring_buffer_mgr_index(mgr->workers_per_node,
                                       (size_t)supernode_id,
                                       (size_t)worker_id);
    return mgr->request_rings[idx];
}

ring_buffer_t *ring_buffer_mgr_get_response(int supernode_id, int worker_id) {
    RETURN_IF(!mgr || supernode_id < 0 || worker_id < 0, NULL);
    RETURN_IF((size_t)supernode_id >= mgr->num_supernodes, NULL);
    RETURN_IF((size_t)worker_id >= mgr->workers_per_node, NULL);

    size_t idx = ring_buffer_mgr_index(mgr->workers_per_node,
                                       (size_t)supernode_id,
                                       (size_t)worker_id);
    return mgr->response_rings[idx];
}
