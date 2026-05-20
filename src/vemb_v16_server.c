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
    uint32_t dim = VEMB_V16_DEFAULT_DIM;
    uint32_t max_vectors = VEMB_V16_DEFAULT_MAX_VECTORS;
    int loglevel = LL_NOTICE;

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--socket") && i + 1 < argc) {
            uds_path = argv[++i];
        } else if (!strcmp(argv[i], "--dim") && i + 1 < argc) {
            dim = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--max-vectors") && i + 1 < argc) {
            max_vectors = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--loglevel") && i + 1 < argc) {
            if (vemb_v16_parse_log_level(argv[++i], &loglevel) != 0) {
                fprintf(stderr, "invalid loglevel\n");
                return 1;
            }
        } else if (!strcmp(argv[i], "--help")) {
            printf("usage: %s [--socket PATH] [--dim N] [--max-vectors N] [--loglevel debug|verbose|notice|warning|nothing]\n", argv[0]);
            return 0;
        }
    }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);
    monotonicInit();
    vemb_v16_log_init();
    vemb_v16_set_log_level(loglevel);
    serverLog(LL_NOTICE, "vemb_v16 server starting: uds=%s dim=%u max_vectors=%u",
              uds_path, dim, max_vectors);

    if (vemb_v16_proxy_create(&g_proxy, uds_path, dim, max_vectors) != 0) {
        serverLog(LL_WARNING, "failed to create vemb_v16 proxy");
        return 1;
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

    vemb_v16_proxy_destroy(g_proxy);
    g_proxy = NULL;
    return ret == 0 ? 0 : 1;
}
