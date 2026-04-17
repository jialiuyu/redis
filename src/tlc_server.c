/*
 * TLC Pure Userspace Network Server v2
 *
 * Direct multi-threaded processing: each IO thread handles requests
 * inline on the three-layer cache (which is already thread-safe via
 * lock-free HOT + bitmap-CAS WARM).
 *
 * Batch merging (3000 req / 200μs) is used for MGET operations only,
 * where SVE2 gather load benefits from batching.
 *
 * Protocol (binary, little-endian):
 *   Request:  [1B op][payload]
 *   Response: [1B status][payload]
 *
 * Architecture:
 *   - 1 acceptor thread (main, epoll)
 *   - N IO worker threads (epoll per thread, direct cache access)
 *   - Each IO thread processes GET/PUT inline (no queue hop)
 *   - TCP_NODELAY + SO_REUSEPORT for minimal latency
 */
#define _GNU_SOURCE
#include "three_layer_cache_ub.h"
#include "sve2_gemm.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <time.h>
#include <signal.h>

#define TLC_PORT          6381
#define IO_THREADS        8
#define EPOLL_EVENTS      256
#define BATCH_LIMIT       3000
#define BATCH_TIMEOUT_US  200

#define OP_GET   0x01
#define OP_PUT   0x02
#define OP_MGET  0x03
#define OP_STATS 0x04
#define OP_FILL  0x05
#define OP_PING  0x06

static three_layer_cache_t g_cache;
static volatile int g_running = 1;
static atomic_uint_fast64_t g_total_ops = 0;
static atomic_uint_fast64_t g_total_batches = 0;

static inline uint64_t now_us(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec*1000000ULL + ts.tv_nsec/1000;
}

static int read_full(int fd, void *buf, size_t n) {
    size_t d=0;
    while (d < n) {
        ssize_t r = read(fd, (char*)buf+d, n-d);
        if (r <= 0) return -1;
        d += r;
    }
    return 0;
}

static int write_full(int fd, const void *buf, size_t n) {
    size_t d=0;
    while (d < n) {
        ssize_t w = write(fd, (const char*)buf+d, n-d);
        if (w <= 0) return -1;
        d += w;
    }
    return 0;
}

/* ---- Handle one client request inline (called from IO thread) ---- */
static int handle_request(int fd) {
    uint8_t op;
    if (read(fd, &op, 1) != 1) return -1;

    uint8_t status;
    uint8_t vbuf[TLC_VALUE_SIZE];

    switch (op) {
    case OP_GET: {
        uint64_t key;
        if (read_full(fd, &key, 8) != 0) return -1;
        if (tlc_get(&g_cache, key, vbuf) == 0) {
            status = 0x00;
            /* Coalesce status + value into single write for fewer syscalls */
            uint8_t resp[1 + TLC_VALUE_SIZE];
            resp[0] = 0x00;
            memcpy(resp + 1, vbuf, TLC_VALUE_SIZE);
            if (write_full(fd, resp, sizeof(resp)) != 0) return -1;
        } else {
            status = 0x01;
            if (write_full(fd, &status, 1) != 0) return -1;
        }
        atomic_fetch_add_explicit(&g_total_ops, 1, memory_order_relaxed);
        break;
    }

    case OP_PUT: {
        uint64_t key;
        if (read_full(fd, &key, 8) != 0) return -1;
        if (read_full(fd, vbuf, TLC_VALUE_SIZE) != 0) return -1;
        tlc_put(&g_cache, key, vbuf);
        status = 0x00;
        if (write_full(fd, &status, 1) != 0) return -1;
        atomic_fetch_add_explicit(&g_total_ops, 1, memory_order_relaxed);
        break;
    }

    case OP_MGET: {
        uint32_t cnt;
        if (read_full(fd, &cnt, 4) != 0) return -1;
        if (cnt > BATCH_LIMIT) cnt = BATCH_LIMIT;

        uint64_t *keys = malloc(cnt * 8);
        if (!keys) return -1;
        if (read_full(fd, keys, cnt * 8) != 0) { free(keys); return -1; }

        /* Allocate response buffer: 4B count + N × (1B status + 1200B value) */
        size_t resp_size = 4 + cnt * (1 + TLC_VALUE_SIZE);
        uint8_t *resp = malloc(resp_size);
        if (!resp) { free(keys); return -1; }

        memcpy(resp, &cnt, 4);
        size_t off = 4;

        /* Batch process — all lookups then all writes */
        for (uint32_t i = 0; i < cnt; i++) {
            if (tlc_get(&g_cache, keys[i], vbuf) == 0) {
                resp[off++] = 0x00;
                memcpy(resp + off, vbuf, TLC_VALUE_SIZE);
                off += TLC_VALUE_SIZE;
            } else {
                resp[off++] = 0x01;
            }
        }

        write_full(fd, resp, off);
        free(keys);
        free(resp);
        atomic_fetch_add_explicit(&g_total_ops, cnt, memory_order_relaxed);
        atomic_fetch_add_explicit(&g_total_batches, 1, memory_order_relaxed);
        break;
    }

    case OP_PING:
        status = 0x00;
        if (write_full(fd, &status, 1) != 0) return -1;
        break;

    case OP_FILL: {
        uint64_t count;
        if (read_full(fd, &count, 8) != 0) return -1;
        uint8_t fbuf[TLC_VALUE_SIZE];
        unsigned int seed = 12345;
        for (uint64_t i = 0; i < count; i++) {
            for (int j = 0; j < (int)(TLC_VALUE_SIZE/4); j++)
                ((uint32_t*)fbuf)[j] = rand_r(&seed);
            tlc_put(&g_cache, i, fbuf);
        }
        status = 0x00;
        write_full(fd, &status, 1);
        write_full(fd, &count, 8);
        atomic_fetch_add_explicit(&g_total_ops, count, memory_order_relaxed);
        break;
    }

    case OP_STATS: {
        uint64_t stats[6];
        stats[0] = atomic_load(&g_cache.total_reads);
        stats[1] = atomic_load(&g_cache.total_writes);
        stats[2] = atomic_load(&g_cache.warm.count);
        stats[3] = atomic_load(&g_total_ops);
        stats[4] = atomic_load(&g_total_batches);
        uint64_t la = atomic_load(&g_cache.ub_mgr.local_accesses);
        uint64_t ra = atomic_load(&g_cache.ub_mgr.remote_accesses);
        stats[5] = (la + ra) > 0 ? 100 * la / (la + ra) : 0;
        status = 0x00;
        write_full(fd, &status, 1);
        write_full(fd, stats, sizeof(stats));
        break;
    }

    default:
        return -1;
    }
    return 0;
}

/* ---- IO thread: epoll + inline request processing ---- */
typedef struct { int tid; int epfd; } io_ctx_t;

static void *io_thread(void *arg) {
    io_ctx_t *ctx = arg;
    struct epoll_event events[EPOLL_EVENTS];

    while (g_running) {
        int n = epoll_wait(ctx->epfd, events, EPOLL_EVENTS, 10);
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            if (handle_request(fd) != 0) {
                epoll_ctl(ctx->epfd, EPOLL_CTL_DEL, fd, NULL);
                close(fd);
            }
        }
    }
    return NULL;
}

static void sig_handler(int sig) { (void)sig; g_running = 0; }

int main(int argc, char *argv[]) {
    int port = TLC_PORT;
    for (int i=1;i<argc;i++)
        if (!strcmp(argv[i],"--port")&&i+1<argc) port=atoi(argv[++i]);

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGPIPE, SIG_IGN);

    printf("╔═══════════════════════════════════════════════════════════╗\n");
    printf("║  TLC Userspace Server v2 (port %d)                      ║\n", port);
    printf("║  IO threads: %d, direct inline processing               ║\n", IO_THREADS);
    printf("║  Three-layer cache: lock-free HOT + bitmap-CAS WARM     ║\n");
    printf("║  UB memory: %d nodes, consistent hash                   ║\n", UB_NUM_NODES);
    printf("╚═══════════════════════════════════════════════════════════╝\n\n");

    if (tlc_init(&g_cache, 0, 0) != 0) { fprintf(stderr, "Cache init failed\n"); return 1; }
    tlc_emb_init(&g_cache, SVE2_EMB_TABLE_SIZE, SVE2_EMB_DIM_DEFAULT);

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(lfd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    struct sockaddr_in addr = {.sin_family=AF_INET, .sin_port=htons(port), .sin_addr.s_addr=htonl(INADDR_LOOPBACK)};
    if (bind(lfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); return 1; }
    listen(lfd, 4096);

    printf("Listening on 127.0.0.1:%d\n", port);

    /* Create IO threads */
    io_ctx_t ctxs[IO_THREADS];
    pthread_t pts[IO_THREADS];
    for (int i = 0; i < IO_THREADS; i++) {
        ctxs[i].tid = i;
        ctxs[i].epfd = epoll_create1(0);
        pthread_create(&pts[i], NULL, io_thread, &ctxs[i]);
    }

    /* Acceptor (main thread) */
    int aepfd = epoll_create1(0);
    struct epoll_event ev = {.events=EPOLLIN, .data.fd=lfd};
    epoll_ctl(aepfd, EPOLL_CTL_ADD, lfd, &ev);
    int next_io = 0;

    printf("Server ready.\n\n");

    while (g_running) {
        struct epoll_event aev[64];
        int n = epoll_wait(aepfd, aev, 64, 100);
        for (int i = 0; i < n; i++) {
            struct sockaddr_in ca; socklen_t cl = sizeof(ca);
            int cfd;
            while ((cfd = accept(lfd, (struct sockaddr*)&ca, &cl)) >= 0) {
                int one = 1;
                setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
                struct epoll_event cev = {.events=EPOLLIN, .data.fd=cfd};
                epoll_ctl(ctxs[next_io].epfd, EPOLL_CTL_ADD, cfd, &cev);
                next_io = (next_io + 1) % IO_THREADS;
            }
        }
    }

    printf("\nShutting down...\n");
    for (int i=0;i<IO_THREADS;i++){pthread_cancel(pts[i]);pthread_join(pts[i],NULL);close(ctxs[i].epfd);}
    close(aepfd); close(lfd);

    printf("Total ops: %lu, MGET batches: %lu\n",
           atomic_load(&g_total_ops), atomic_load(&g_total_batches));
    tlc_print_stats(&g_cache);
    tlc_destroy(&g_cache);
    return 0;
}
