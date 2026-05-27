#define _GNU_SOURCE

#include "vemb_v16_proxy.h"
#include "vemb_v16_log.h"
#include "monotonic.h"

#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static vemb_v16_proxy_t *g_proxy;

static void on_signal(int sig) {
    (void)sig;
    vemb_v16_proxy_stop(g_proxy);
}

int main(int argc, char **argv) {
    const char *uds_path = VEMB_V16_UDS_PATH;
    const char *vector_region_name = VEMB_V16_DEFAULT_VECTOR_REGION;
    uint32_t dim = VEMB_V16_DEFAULT_DIM;
    uint32_t max_vectors = VEMB_V16_DEFAULT_MAX_VECTORS;
    uint32_t warm_region_id = 0;
    uint32_t warm_backend_type = VEMB_V16_REGION_LOCAL_SHM;
    uint64_t warm_mmap_offset = 0;
    int loglevel = LL_NOTICE;
    const char *transport = "shm";
    const char *tcp_host = VEMB_V16_TCP_HOST;
    uint16_t tcp_port = VEMB_V16_TCP_PORT;
    uint32_t proxy_io_threads = 0;
    uint32_t supernode_workers = 0;

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--socket") && i + 1 < argc) {
            uds_path = argv[++i];
        } else if (!strcmp(argv[i], "--transport") && i + 1 < argc) {
            transport = argv[++i];
            if (strcmp(transport, "shm") &&
                strcmp(transport, "tcp") &&
                strcmp(transport, "both")) {
                fprintf(stderr, "invalid transport\n");
                return 1;
            }
        } else if (!strcmp(argv[i], "--tcp-host") && i + 1 < argc) {
            tcp_host = argv[++i];
        } else if (!strcmp(argv[i], "--tcp-port") && i + 1 < argc) {
            tcp_port = (uint16_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--proxy-io-threads") && i + 1 < argc) {
            proxy_io_threads = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--supernode-workers") && i + 1 < argc) {
            supernode_workers = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--vector-region") && i + 1 < argc) {
            vector_region_name = argv[++i];
        } else if (!strcmp(argv[i], "--dim") && i + 1 < argc) {
            dim = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--max-vectors") && i + 1 < argc) {
            max_vectors = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--region-id") && i + 1 < argc) {
            warm_region_id = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--warm-backend") && i + 1 < argc) {
            const char *backend = argv[++i];
            if (!strcmp(backend, "shm")) {
                warm_backend_type = VEMB_V16_REGION_LOCAL_SHM;
            } else if (!strcmp(backend, "ub")) {
                warm_backend_type = VEMB_V16_REGION_UB;
            } else {
                fprintf(stderr, "invalid warm backend\n");
                return 1;
            }
        } else if (!strcmp(argv[i], "--warm-mmap-offset") && i + 1 < argc) {
            warm_mmap_offset = strtoull(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--loglevel") && i + 1 < argc) {
            if (vemb_v16_parse_log_level(argv[++i], &loglevel) != 0) {
                fprintf(stderr, "invalid loglevel\n");
                return 1;
            }
        } else if (!strcmp(argv[i], "--help")) {
            printf("usage: %s [--transport shm|tcp|both] [--socket PATH] [--tcp-host HOST] [--tcp-port PORT] [--proxy-io-threads N] [--supernode-workers N] [--vector-region SHM_NAME_OR_UB_PATH] [--region-id N] [--warm-backend shm|ub] [--warm-mmap-offset N] [--dim N] [--max-vectors N] [--loglevel debug|verbose|notice|warning|nothing]\n", argv[0]);
            return 0;
        }
    }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);
    monotonicInit();
    vemb_v16_log_init();
    vemb_v16_set_log_level(loglevel);
    serverLog(LL_NOTICE, "vemb_v16 server starting: transport=%s uds=%s tcp=%s:%u proxy_io_threads=%u supernode_workers=%u dim=%u max_vectors=%u vector_region=%s",
              transport, uds_path, tcp_host, tcp_port, proxy_io_threads,
              supernode_workers, dim, max_vectors, vector_region_name);

    if (vemb_v16_proxy_create(&g_proxy,
                              uds_path,
                              dim,
                              max_vectors,
                              vector_region_name,
                              warm_region_id,
                              warm_backend_type,
                              warm_mmap_offset) != 0) {
        serverLog(LL_WARNING, "failed to create vemb_v16 proxy");
        return 1;
    }
    if (supernode_workers &&
        vemb_v16_proxy_set_supernode_workers(g_proxy, supernode_workers) != 0) {
        serverLog(LL_WARNING, "failed to configure vemb_v16 supernode workers");
        vemb_v16_proxy_destroy(g_proxy);
        g_proxy = NULL;
        return 1;
    }
    if (proxy_io_threads &&
        vemb_v16_proxy_set_proxy_io_threads(g_proxy, proxy_io_threads) != 0) {
        serverLog(LL_WARNING, "failed to configure vemb_v16 proxy io threads");
        vemb_v16_proxy_destroy(g_proxy);
        g_proxy = NULL;
        return 1;
    }
    if (!strcmp(transport, "tcp") || !strcmp(transport, "both")) {
        if (vemb_v16_proxy_enable_tcp(g_proxy, tcp_host, tcp_port) != 0) {
            serverLog(LL_WARNING, "failed to enable vemb_v16 tcp transport");
            vemb_v16_proxy_destroy(g_proxy);
            g_proxy = NULL;
            return 1;
        }
    }

    int ret = vemb_v16_proxy_run(g_proxy);
    vemb_v16_stats_t stats;
    vemb_v16_proxy_get_stats(g_proxy, &stats);
    serverLog(LL_NOTICE, "vemb_v16 stats: total=%llu vadd=%llu vemb=%llu not_found=%llu published=%llu completed=%llu active_channels=%llu",
           (unsigned long long)stats.total_requests,
           (unsigned long long)stats.vadd_requests,
           (unsigned long long)stats.vemb_requests,
           (unsigned long long)stats.not_found,
           (unsigned long long)stats.published_jobs,
           (unsigned long long)stats.completed_jobs,
           (unsigned long long)stats.active_channels);
    serverLog(LL_NOTICE, "vemb_v16 stats: bitmap_lock_success=%llu bitmap_lock_failure=%llu sample_vector_load_ns=%llu",
           (unsigned long long)stats.bitmap_lock_success,
           (unsigned long long)stats.bitmap_lock_failure,
           (unsigned long long)stats.sample_vector_load_ns);
    serverLog(LL_NOTICE, "vemb_v16 stats: depth request=%llu response=%llu vemb_job=%llu vadd_job=%llu completion=%llu channel_ops=%llu",
           (unsigned long long)stats.request_ring_depth,
           (unsigned long long)stats.response_ring_depth,
           (unsigned long long)stats.vemb_job_ring_depth,
           (unsigned long long)stats.vadd_job_ring_depth,
           (unsigned long long)stats.completion_ring_depth,
           (unsigned long long)stats.channel_ops);

    vemb_v16_proxy_destroy(g_proxy);
    g_proxy = NULL;
    return ret == 0 ? 0 : 1;
}
