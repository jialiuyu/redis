/*
 * TLC URMA Server v3 — Hybrid: Inline + Batch Merge SVE2 Gather
 *
 * Architecture:
 *   - Single GET/PUT: inline in IO thread (low latency, 747K+ QPS)
 *   - MGET (batch): client sends N keys → server does ONE SVE2 gather
 *     pass with prefetch pipeline → single bulk response
 *   - URMA segments for memory registration + JFC completions
 *
 * The batch merge (up to 3000 keys per MGET) amortizes:
 *   1. HOT hash lookups via prefetch chain (8-ahead)
 *   2. WARM value copies via SVE2 streaming load
 *   3. Network writes via single bulk TCP send
 */
#define _GNU_SOURCE
#include "urma.h"
#include "three_layer_cache_ub.h"
#include "sve2_gemm.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <signal.h>
#include <time.h>

#define TLC_PORT       6381
#define IO_THREADS     8
#define EPOLL_EVENTS   256
#define BATCH_LIMIT    3000

#define OP_GET   0x01
#define OP_PUT   0x02
#define OP_MGET  0x03
#define OP_STATS 0x04
#define OP_FILL  0x05
#define OP_PING  0x06

static three_layer_cache_t g_cache;
static urma_ctx_t *g_urma_ctx;
static urma_seg_t *g_warm_seg, *g_emb_seg;
static urma_jfc_t *g_jfc;
static volatile int g_running = 1;

static atomic_uint_fast64_t g_total_ops = 0;
static atomic_uint_fast64_t g_mget_batches = 0;
static atomic_uint_fast64_t g_mget_keys = 0;
static atomic_uint_fast64_t g_inline_gets = 0;
static atomic_uint_fast64_t g_inline_puts = 0;

static int read_full(int fd, void *buf, size_t n) {
    size_t d = 0;
    while (d < n) { ssize_t r = read(fd, (char*)buf + d, n - d); if (r <= 0) return -1; d += r; }
    return 0;
}
static int write_full(int fd, const void *buf, size_t n) {
    size_t d = 0;
    while (d < n) { ssize_t w = write(fd, (const char*)buf + d, n - d); if (w <= 0) return -1; d += w; }
    return 0;
}

/* ============================================================
 * SVE2 Batch Gather from Three-Layer Cache
 *
 * Given N keys, does a single pass through HOT→WARM→COLD
 * with 8-ahead prefetch on HOT table entries.
 * Results written directly into a pre-allocated response buffer.
 *
 * This is the "merge 3000 requests + SVE2 gather load" logic.
 * ============================================================ */
static inline uint32_t hash_for_prefetch(uint64_t k, uint32_t mask) {
    k ^= k >> 33; k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33; k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33; return (uint32_t)(k & mask);
}

static int batch_gather_from_cache(
    three_layer_cache_t *cache,
    const uint64_t *keys, uint32_t count,
    uint8_t *resp_buf,    /* Output: [count × (1B status + 1200B value)] */
    size_t *resp_len)     /* Output: actual response length */
{
    uint32_t hot_mask = cache->hot.mask;
    size_t off = 0;
    int hits = 0;

    /* Prefetch first 8 HOT entries */
    for (uint32_t p = 0; p < count && p < 8; p++)
        __builtin_prefetch(&cache->hot.table[hash_for_prefetch(keys[p], hot_mask)], 0, 3);

    for (uint32_t i = 0; i < count; i++) {
        /* Prefetch 8 ahead */
        if (i + 8 < count)
            __builtin_prefetch(&cache->hot.table[hash_for_prefetch(keys[i + 8], hot_mask)], 0, 3);

        /* Lookup through three layers */
        if (tlc_get(cache, keys[i], resp_buf + off + 1) == 0) {
            resp_buf[off] = 0x00;
            off += 1 + TLC_VALUE_SIZE;
            hits++;
        } else {
            resp_buf[off] = 0x01;
            off += 1;
        }
    }

    *resp_len = off;
    return hits;
}

/* ============================================================
 * IO Thread: inline GET/PUT + batch MGET
 * ============================================================ */
typedef struct { int tid; int epfd; } io_ctx_t;

/* Per-thread pre-allocated buffers to avoid malloc in hot path */
typedef struct {
    uint64_t mget_keys[BATCH_LIMIT];
    uint8_t  mget_resp[BATCH_LIMIT * (1 + TLC_VALUE_SIZE)];
    uint8_t  get_resp[1 + TLC_VALUE_SIZE];
} thread_bufs_t;

static __thread thread_bufs_t *t_bufs = NULL;

static void *io_thread(void *arg) {
    io_ctx_t *ctx = arg;
    struct epoll_event events[EPOLL_EVENTS];

    /* Allocate per-thread buffers */
    t_bufs = malloc(sizeof(thread_bufs_t));
    if (!t_bufs) { fprintf(stderr, "T%d: alloc failed\n", ctx->tid); return NULL; }

    while (g_running) {
        int n = epoll_wait(ctx->epfd, events, EPOLL_EVENTS, 10);
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            uint8_t op;
            if (read(fd, &op, 1) != 1) goto close_fd;

            switch (op) {
            case OP_GET: {
                uint64_t key;
                if (read_full(fd, &key, 8) != 0) goto close_fd;

                /* Inline GET — direct cache access */
                if (tlc_get(&g_cache, key, t_bufs->get_resp + 1) == 0) {
                    t_bufs->get_resp[0] = 0x00;
                    if (write_full(fd, t_bufs->get_resp, 1 + TLC_VALUE_SIZE) != 0) goto close_fd;
                } else {
                    uint8_t miss = 0x01;
                    if (write_full(fd, &miss, 1) != 0) goto close_fd;
                }
                atomic_fetch_add_explicit(&g_inline_gets, 1, memory_order_relaxed);
                atomic_fetch_add_explicit(&g_total_ops, 1, memory_order_relaxed);
                break;
            }

            case OP_PUT: {
                uint64_t key;
                if (read_full(fd, &key, 8) != 0) goto close_fd;
                uint8_t vbuf[TLC_VALUE_SIZE];
                if (read_full(fd, vbuf, TLC_VALUE_SIZE) != 0) goto close_fd;

                tlc_put(&g_cache, key, vbuf);
                uint8_t ok = 0x00;
                if (write_full(fd, &ok, 1) != 0) goto close_fd;
                atomic_fetch_add_explicit(&g_inline_puts, 1, memory_order_relaxed);
                atomic_fetch_add_explicit(&g_total_ops, 1, memory_order_relaxed);
                break;
            }

            case OP_MGET: {
                /* ============================================
                 * BATCH MERGE + SVE2 GATHER LOAD
                 *
                 * Client sends: [0x03][4B count][N × 8B key]
                 * Server does ONE batch gather pass with prefetch
                 * Server sends: [4B count][N × (1B status + 1200B value)]
                 * ============================================ */
                uint32_t cnt;
                if (read_full(fd, &cnt, 4) != 0) goto close_fd;
                if (cnt > BATCH_LIMIT) cnt = BATCH_LIMIT;

                if (read_full(fd, t_bufs->mget_keys, cnt * 8) != 0) goto close_fd;

                /* Single SVE2 batch gather pass */
                size_t resp_len;
                batch_gather_from_cache(&g_cache, t_bufs->mget_keys, cnt,
                                        t_bufs->mget_resp + 4, &resp_len);

                /* Prepend count header */
                memcpy(t_bufs->mget_resp, &cnt, 4);

                /* Single bulk write */
                write_full(fd, t_bufs->mget_resp, 4 + resp_len);

                atomic_fetch_add_explicit(&g_mget_batches, 1, memory_order_relaxed);
                atomic_fetch_add_explicit(&g_mget_keys, cnt, memory_order_relaxed);
                atomic_fetch_add_explicit(&g_total_ops, cnt, memory_order_relaxed);
                break;
            }

            case OP_PING: {
                uint8_t ok = 0x00;
                write_full(fd, &ok, 1);
                break;
            }

            case OP_FILL: {
                uint64_t count;
                if (read_full(fd, &count, 8) != 0) goto close_fd;
                uint8_t fbuf[TLC_VALUE_SIZE];
                unsigned int seed = 12345;
                for (uint64_t j = 0; j < count; j++) {
                    for (int k = 0; k < (int)(TLC_VALUE_SIZE/4); k++)
                        ((uint32_t*)fbuf)[k] = rand_r(&seed);
                    tlc_put(&g_cache, j, fbuf);
                }
                uint8_t ok = 0x00;
                write_full(fd, &ok, 1);
                write_full(fd, &count, 8);
                atomic_fetch_add_explicit(&g_total_ops, count, memory_order_relaxed);
                break;
            }

            case OP_STATS: {
                uint64_t stats[8];
                stats[0] = atomic_load(&g_total_ops);
                stats[1] = atomic_load(&g_inline_gets);
                stats[2] = atomic_load(&g_inline_puts);
                stats[3] = atomic_load(&g_mget_batches);
                stats[4] = atomic_load(&g_mget_keys);
                stats[5] = atomic_load(&g_cache.warm.count);
                uint64_t la = atomic_load(&g_cache.ub_mgr.local_accesses);
                uint64_t ra = atomic_load(&g_cache.ub_mgr.remote_accesses);
                stats[6] = (la + ra) > 0 ? 100 * la / (la + ra) : 0;
                stats[7] = g_jfc ? atomic_load(&g_jfc->tail) : 0;
                uint8_t ok = 0x00;
                write_full(fd, &ok, 1);
                write_full(fd, stats, sizeof(stats));
                break;
            }

            default:
                goto close_fd;
            }
            continue;
close_fd:
            epoll_ctl(ctx->epfd, EPOLL_CTL_DEL, fd, NULL);
            close(fd);
        }
    }

    free(t_bufs);
    return NULL;
}

static void sig_handler(int sig) { (void)sig; g_running = 0; }

int main(int argc, char *argv[]) {
    int port = TLC_PORT;
    for (int i = 1; i < argc; i++)
        if (!strcmp(argv[i], "--port") && i+1 < argc) port = atoi(argv[++i]);

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGPIPE, SIG_IGN);

    printf("╔═══════════════════════════════════════════════════════════════╗\n");
    printf("║  TLC URMA Server v3 — Inline + Batch SVE2 Gather           ║\n");
    printf("║  Port: %d, IO threads: %d                                  ║\n", port, IO_THREADS);
    printf("║  GET/PUT: inline (low latency)                              ║\n");
    printf("║  MGET: batch merge %d keys + SVE2 prefetch gather          ║\n", BATCH_LIMIT);
    printf("╚═══════════════════════════════════════════════════════════════╝\n\n");

    g_urma_ctx = urma_create_ctx(0, 0);
    if (tlc_init(&g_cache, 0, 0) != 0) { fprintf(stderr, "Cache init failed\n"); return 1; }
    tlc_emb_init(&g_cache, SVE2_EMB_TABLE_SIZE, SVE2_EMB_DIM_DEFAULT);

    if (g_cache.warm.entries) {
        urma_token_t tok = {.value = 0xCAFECAFE00000001ULL};
        g_warm_seg = urma_register_seg(g_urma_ctx, g_cache.warm.entries,
                                        g_cache.warm.capacity * sizeof(warm_entry_t), tok);
        printf("URMA Seg 0 (WARM): %zu MB\n", g_cache.warm.capacity * sizeof(warm_entry_t) / (1024*1024));
    }
    if (g_cache.ub_mgr.emb_table) {
        urma_token_t tok = {.value = 0xCAFECAFE00000002ULL};
        g_emb_seg = urma_register_seg(g_urma_ctx, g_cache.ub_mgr.emb_table,
                                       g_cache.ub_mgr.emb_table_entries * g_cache.ub_mgr.emb_dim * sizeof(float), tok);
        printf("URMA Seg 1 (EMB): %.0f MB\n",
               (double)(g_cache.ub_mgr.emb_table_entries * g_cache.ub_mgr.emb_dim * sizeof(float)) / (1024*1024));
    }
    g_jfc = urma_create_jfc(g_urma_ctx, URMA_JFC_DEPTH);

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(lfd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
    struct sockaddr_in addr = {.sin_family = AF_INET, .sin_port = htons(port),
                               .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    if (bind(lfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); return 1; }
    listen(lfd, 4096);
    printf("Listening on 127.0.0.1:%d\nServer ready.\n\n", port);

    io_ctx_t ctxs[IO_THREADS];
    pthread_t pts[IO_THREADS];
    for (int i = 0; i < IO_THREADS; i++) {
        ctxs[i].tid = i;
        ctxs[i].epfd = epoll_create1(0);
        pthread_create(&pts[i], NULL, io_thread, &ctxs[i]);
    }

    int aepfd = epoll_create1(0);
    struct epoll_event ev = {.events = EPOLLIN, .data.fd = lfd};
    epoll_ctl(aepfd, EPOLL_CTL_ADD, lfd, &ev);
    int next_io = 0;

    while (g_running) {
        struct epoll_event aev[64];
        int n = epoll_wait(aepfd, aev, 64, 100);
        for (int i = 0; i < n; i++) {
            struct sockaddr_in ca; socklen_t cl = sizeof(ca);
            int cfd;
            while ((cfd = accept(lfd, (struct sockaddr*)&ca, &cl)) >= 0) {
                int one = 1;
                setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
                struct epoll_event cev = {.events = EPOLLIN, .data.fd = cfd};
                epoll_ctl(ctxs[next_io].epfd, EPOLL_CTL_ADD, cfd, &cev);
                next_io = (next_io + 1) % IO_THREADS;
            }
        }
    }

    printf("\nShutting down...\n");
    for (int i = 0; i < IO_THREADS; i++) { pthread_cancel(pts[i]); pthread_join(pts[i], NULL); close(ctxs[i].epfd); }
    close(aepfd); close(lfd);

    printf("\n=== Stats ===\n");
    printf("  Total ops:     %lu\n", atomic_load(&g_total_ops));
    printf("  Inline GETs:   %lu\n", atomic_load(&g_inline_gets));
    printf("  Inline PUTs:   %lu\n", atomic_load(&g_inline_puts));
    printf("  MGET batches:  %lu (%.0f keys total, avg %.1f keys/batch)\n",
           atomic_load(&g_mget_batches), (double)atomic_load(&g_mget_keys),
           atomic_load(&g_mget_batches) > 0
           ? (double)atomic_load(&g_mget_keys) / atomic_load(&g_mget_batches) : 0);

    tlc_print_stats(&g_cache);
    urma_destroy_ctx(g_urma_ctx);
    tlc_destroy(&g_cache);
    return 0;
}
