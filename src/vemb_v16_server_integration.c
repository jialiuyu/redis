#define _GNU_SOURCE

#include "vemb_v16_server_integration.h"
#include "vemb_v16_proxy.h"
#include "vemb_v16_server_tcp_client.h"
#include "vemb_v16_storage.h"
#include "vemb_v16_protocol.h"
#include "server.h"
#include "connection.h"

/* vemb_v16_log compatibility: standalone server links vemb_v16_log.o,
 * but redis-server already has serverLog in server.c.  We only need
 * the global verbosity variable that vemb_v16_log.h's serverLog macro
 * references when compiling vemb_v16_proxy.o / vemb_v16_supernode.o. */
int vemb_v16_log_verbosity_value = LL_NOTICE;

#include <pthread.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <poll.h>

static void *proxy_run_thread(void *arg) {
    vemb_v16_proxy_run((vemb_v16_proxy_t *)arg);
    return NULL;
}

static vemb_v16_storage_ctx_t *g_vemb_storage = NULL;

int vemb_v16_server_integration_init(void) {
    if (!server.vemb_v16_enabled) return 0;

    const char *vector_region = server.vemb_v16_vector_region
        ? server.vemb_v16_vector_region
        : VEMB_V16_DEFAULT_VECTOR_REGION;
    uint32_t dim = server.vemb_v16_dim > 0
        ? (uint32_t)server.vemb_v16_dim
        : VEMB_V16_DEFAULT_DIM;
    uint32_t max_vectors = server.vemb_v16_max_vectors > 0
        ? (uint32_t)server.vemb_v16_max_vectors
        : VEMB_V16_DEFAULT_MAX_VECTORS;
    uint32_t warm_backend = VEMB_V16_REGION_UB;

    serverLog(LL_NOTICE,
              "VEMB V16 integration init: dim=%u max_vectors=%u "
              "vector_region=%s warm_backend=ub",
              dim, max_vectors, vector_region);

    vemb_v16_storage_ctx_t *storage = NULL;
    if (server.vemb_v16_warm_regions_manifest &&
        server.vemb_v16_warm_regions_manifest[0]) {
        vemb_v16_warm_regions_manifest_t manifest;
        memset(&manifest, 0, sizeof(manifest));
        if (vemb_v16_parse_warm_regions_manifest(server.vemb_v16_warm_regions_manifest,
                                                  dim * sizeof(float),
                                                  &manifest) != 0) {
            serverLog(LL_WARNING, "vemb_v16_parse_warm_regions_manifest failed: %s",
                      server.vemb_v16_warm_regions_manifest);
            return -1;
        }
        if (server.vemb_v16_reset_warm_regions) {
            if (vemb_v16_storage_reset_manifest_regions(&manifest) != 0) {
                serverLog(LL_WARNING, "vemb_v16_storage_reset_manifest_regions failed");
                return -1;
            }
        }
        if (vemb_v16_storage_ctx_create_from_manifest(&storage,
                                                       dim,
                                                       dim * sizeof(float),
                                                       max_vectors,
                                                       &manifest) != 0) {
            serverLog(LL_WARNING, "vemb_v16_storage_ctx_create_from_manifest failed");
            return -1;
        }
    } else {
        vemb_v16_warm_regions_manifest_t manifest;
        memset(&manifest, 0, sizeof(manifest));
        manifest.local_ub_node_id = 0;
        manifest.has_local_ub_node_id = 1;
        manifest.local_region_weight = 4;
        manifest.region_count = 1;
        vemb_v16_manifest_region_t *region = &manifest.regions[0];
        region->region_id = 1;
        region->backend_type = warm_backend;
        region->home_ub_node_id = 0;
        region->is_local = 1;
        region->has_is_local = 1;
        region->weight = 1;
        region->value_size = dim * sizeof(float);
        region->mmap_offset = server.vemb_v16_warm_mmap_offset;
        region->region_bytes = max_vectors * region->value_size;
        strncpy(region->path, vector_region, sizeof(region->path) - 1);
        region->path[sizeof(region->path) - 1] = '\0';

        if (server.vemb_v16_reset_warm_regions) {
            if (vemb_v16_storage_reset_manifest_regions(&manifest) != 0) {
                serverLog(LL_WARNING, "vemb_v16_storage_reset_manifest_regions failed");
                return -1;
            }
        }
        if (vemb_v16_storage_ctx_create_from_manifest(&storage,
                                                       dim,
                                                       dim * sizeof(float),
                                                       max_vectors,
                                                       &manifest) != 0) {
            serverLog(LL_WARNING, "vemb_v16_storage_ctx_create_from_manifest failed");
            return -1;
        }
    }

    if (vemb_v16_proxy_create(&server.vemb_v16_proxy,
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

    /* Enable inject pipe so Redis accept path can hand off VEMB connections */
    if (server.vemb_v16_sniff_port > 0) {
        if (vemb_v16_proxy_enable_inject(server.vemb_v16_proxy) != 0) {
            serverLog(LL_WARNING, "vemb_v16_proxy_enable_inject failed");
            vemb_v16_proxy_destroy(server.vemb_v16_proxy);
            server.vemb_v16_proxy = NULL;
            return -1;
        }
        serverLog(LL_NOTICE, "VEMB V16 sniff enabled on port %d", server.vemb_v16_sniff_port);
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

/* -------------------------------------------------------------------
 * VEMB V16 protocol sniffing on Redis port 6379
 *
 * When a new TCP connection arrives, peek at the first 4 bytes.
 * If they match the VEMB binary protocol magic (0x56313645),
 * steal the fd and hand it to the VEMB proxy thread via an
 * inject pipe.  Otherwise return 0 and let normal RESP processing
 * continue.  The overhead for RESP connections is one MSG_PEEK
 * recv syscall per connection establishment.
 * ------------------------------------------------------------------- */
int vemb_v16_sniff_and_handoff(connection *conn) {
    /* Fast path: sniffing disabled or proxy not running */
    if (!server.vemb_v16_sniff_port || !server.vemb_v16_proxy)
        return 0;
    /* Cannot sniff through TLS */
    if (connIsTLS(conn))
        return 0;

    int fd = conn->fd;
    if (fd < 0) return 0;

    /* Wait briefly for the client to send its first bytes.
     * Non-blocking clients (e.g., libevent-based tools like memtier_benchmark)
     * may not have sent data yet when accept fires.  Use poll() with a short
     * timeout so we can peek at the magic once the HELLO frame arrives.
     * Cost: up to 5 ms added latency per new RESP connection — negligible
     * for long-lived connections. */
    struct pollfd pfd = { .fd = fd, .events = POLLIN };
    int ready = poll(&pfd, 1, 5);  /* 5 ms */
    if (ready <= 0) return 0;      /* No data → treat as RESP */

    /* Peek at first 4 bytes without consuming them */
    uint8_t buf[4];
    ssize_t n = recv(fd, buf, 4, MSG_PEEK);
    if (n < 4) return 0;   /* Not enough data yet, treat as RESP */

    /* Check against VEMB V16 magic number (native byte order) */
    uint32_t magic;
    memcpy(&magic, buf, sizeof(magic));
    if (magic != VEMB_V16_MAGIC) return 0;

    /* VEMB detected — steal the fd so connClose won't close it */
    conn->fd = -1;

    serverLog(LL_VERBOSE, "VEMB V16 protocol detected on fd %d, injecting to proxy", fd);

    if (vemb_v16_proxy_inject_fd(server.vemb_v16_proxy, fd) != 0) {
        /* Injection failed (pipe full?), close the fd */
        serverLog(LL_WARNING, "VEMB V16 inject_fd failed for fd %d, closing", fd);
        close(fd);
    }
    return 1;
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
