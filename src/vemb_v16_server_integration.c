#define _GNU_SOURCE

#include "vemb_v16_server_integration.h"
#include "vemb_v16_proxy.h"
#include "vemb_v16_server_tcp_client.h"
#include "vemb_v16_storage.h"
#include "server.h"

/* vemb_v16_log compatibility: standalone server links vemb_v16_log.o,
 * but redis-server already has serverLog in server.c.  We only need
 * the global verbosity variable that vemb_v16_log.h's serverLog macro
 * references when compiling vemb_v16_proxy.o / vemb_v16_supernode.o. */
int vemb_v16_log_verbosity_value = LL_NOTICE;

#include <pthread.h>
#include <string.h>

static void *proxy_run_thread(void *arg) {
    vemb_v16_proxy_run((vemb_v16_proxy_t *)arg);
    return NULL;
}

static vemb_v16_storage_ctx_t *g_vemb_storage = NULL;

int vemb_v16_server_integration_init(void) {
    if (!server.vemb_v16_enabled) return 0;

    const char *uds_path = server.vemb_v16_uds_path
        ? server.vemb_v16_uds_path
        : VEMB_V16_UDS_PATH;
    const char *vector_region = server.vemb_v16_vector_region
        ? server.vemb_v16_vector_region
        : VEMB_V16_DEFAULT_VECTOR_REGION;
    uint32_t dim = server.vemb_v16_dim > 0
        ? (uint32_t)server.vemb_v16_dim
        : VEMB_V16_DEFAULT_DIM;
    uint32_t max_vectors = server.vemb_v16_max_vectors > 0
        ? (uint32_t)server.vemb_v16_max_vectors
        : VEMB_V16_DEFAULT_MAX_VECTORS;
    uint32_t warm_backend = VEMB_V16_REGION_LOCAL_SHM;
    if (server.vemb_v16_warm_backend &&
        !strcmp(server.vemb_v16_warm_backend, "ub")) {
        warm_backend = VEMB_V16_REGION_UB;
    }

    serverLog(LL_NOTICE,
              "VEMB V16 integration init: uds=%s dim=%u max_vectors=%u "
              "vector_region=%s warm_backend=%s",
              uds_path, dim, max_vectors, vector_region,
              warm_backend == VEMB_V16_REGION_UB ? "ub" : "shm");

    vemb_v16_storage_ctx_t *storage = NULL;
    if (vemb_v16_storage_ctx_create(&storage,
                                    dim,
                                    dim * sizeof(float),
                                    max_vectors,
                                    vector_region,
                                    0,
                                    warm_backend,
                                    server.vemb_v16_warm_mmap_offset) != 0) {
        serverLog(LL_WARNING, "vemb_v16_storage_ctx_create failed");
        return -1;
    }

    if (vemb_v16_proxy_create(&server.vemb_v16_proxy,
                              uds_path,
                              dim,
                              max_vectors,
                              storage) != 0) {
        serverLog(LL_WARNING, "vemb_v16_proxy_create failed");
        vemb_v16_storage_ctx_destroy(storage);
        return -1;
    }
    g_vemb_storage = storage;

    if (server.vemb_v16_supernode_workers > 0) {
        if (vemb_v16_proxy_set_supernode_workers(
                server.vemb_v16_proxy,
                (uint32_t)server.vemb_v16_supernode_workers) != 0) {
            serverLog(LL_WARNING,
                      "vemb_v16_proxy_set_supernode_workers failed");
            vemb_v16_proxy_destroy(server.vemb_v16_proxy);
            server.vemb_v16_proxy = NULL;
            return -1;
        }
    }

    if (server.vemb_v16_proxy_io_threads > 0) {
        if (vemb_v16_proxy_set_proxy_io_threads(
                server.vemb_v16_proxy,
                (uint32_t)server.vemb_v16_proxy_io_threads) != 0) {
            serverLog(LL_WARNING,
                      "vemb_v16_proxy_set_proxy_io_threads failed");
            vemb_v16_proxy_destroy(server.vemb_v16_proxy);
            server.vemb_v16_proxy = NULL;
            return -1;
        }
    }

    if (server.vemb_v16_tcp_port > 0) {
        const char *host = (server.vemb_v16_tcp_host && server.vemb_v16_tcp_host[0])
                           ? server.vemb_v16_tcp_host
                           : VEMB_V16_TCP_HOST;
        uint16_t port = (uint16_t)server.vemb_v16_tcp_port;
        if (vemb_v16_proxy_enable_tcp(server.vemb_v16_proxy,
                                      host,
                                      port) != 0) {
            serverLog(LL_WARNING, "vemb_v16_proxy_enable_tcp failed");
            vemb_v16_proxy_destroy(server.vemb_v16_proxy);
            server.vemb_v16_proxy = NULL;
            return -1;
        }
    }

    if (pthread_create(&server.vemb_v16_proxy_thread, NULL,
                       proxy_run_thread, server.vemb_v16_proxy) != 0) {
        serverLog(LL_WARNING, "pthread_create for proxy_run_thread failed");
        vemb_v16_proxy_destroy(server.vemb_v16_proxy);
        server.vemb_v16_proxy = NULL;
        return -1;
    }

    serverLog(LL_NOTICE, "VEMB V16 integration ready");
    return 0;
}

void vemb_v16_server_integration_shutdown(void) {
    serverLog(LL_NOTICE, "VEMB V16 integration shutdown...");

    /* Close the internal TCP client connection before stopping proxy */
    vemb_v16_stc_cleanup();

    if (server.vemb_v16_proxy) {
        vemb_v16_proxy_stop(server.vemb_v16_proxy);
        pthread_join(server.vemb_v16_proxy_thread, NULL);
        vemb_v16_proxy_destroy(server.vemb_v16_proxy);
        server.vemb_v16_proxy = NULL;
    }
    if (g_vemb_storage) {
        vemb_v16_storage_ctx_destroy(g_vemb_storage);
        g_vemb_storage = NULL;
    }

    serverLog(LL_NOTICE, "VEMB V16 integration shutdown complete");
}
