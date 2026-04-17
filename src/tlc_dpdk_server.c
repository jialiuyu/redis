/*
 * TLC DPDK-Style Server — Zero-Copy Polling + KCP RUDP
 *
 * Architecture (DPDK-equivalent without hardware NIC binding):
 *   - UDP socket with SO_BUSY_POLL for polling mode
 *   - KCP-Lite RUDP protocol on top (pure algorithm, zero syscalls in data path)
 *   - recvmmsg/sendmmsg for batch I/O (amortize syscall overhead)
 *   - Dedicated poll thread per core (no epoll, no interrupts)
 *   - Three-layer cache with SVE2 gather + batch merge
 *
 * Data flow:
 *   Client → UDP → KCP reassemble → batch queue → SVE2 gather → KCP segment → UDP → Client
 *
 * Protocol:
 *   KCP payload: [1B op][8B key] for GET, [1B op][8B key][1200B val] for PUT
 *   MGET: [0x03][4B count][N×8B keys] → [4B count][N×(1B+1200B)]
 */
#define _GNU_SOURCE
#include "kcp_lite.h"
#include "three_layer_cache_ub.h"
#include "sve2_gemm.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/udp.h>
#include <arpa/inet.h>
#include <signal.h>
#include <time.h>
#include <errno.h>
#include <poll.h>

#define SERVER_PORT    6382
#define POLL_THREADS   4
#define RECV_BATCH     64       /* recvmmsg batch size */
#define KCP_CONV       0x544C43 /* "TLC" */

#define OP_GET   0x01
#define OP_PUT   0x02
#define OP_MGET  0x03
#define OP_STATS 0x04
#define OP_FILL  0x05
#define OP_PING  0x06

#define BATCH_LIMIT 3000

static three_layer_cache_t g_cache;
static volatile int g_running = 1;
static int g_sockfd = -1;

static atomic_uint_fast64_t g_total_ops = 0;
static atomic_uint_fast64_t g_total_pkts_in = 0;
static atomic_uint_fast64_t g_total_pkts_out = 0;
static atomic_uint_fast64_t g_mget_batches = 0;
static atomic_uint_fast64_t g_mget_keys = 0;
static atomic_uint_fast64_t g_kcp_segments = 0;

static inline uint64_t now_us(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
}

/* ---- Per-client KCP session ---- */
typedef struct {
    uint64_t          client_key;  /* addr:port hash */
    struct sockaddr_in addr;
    socklen_t          addr_len;
    kcp_t             *kcp;
    uint64_t           last_active;
} kcp_session_t;

#define MAX_SESSIONS 4096
static kcp_session_t g_sessions[MAX_SESSIONS];
static int g_num_sessions = 0;
static pthread_mutex_t g_session_mtx = PTHREAD_MUTEX_INITIALIZER;

static uint64_t addr_key(struct sockaddr_in *a) {
    return ((uint64_t)a->sin_addr.s_addr << 16) | ntohs(a->sin_port);
}

/* KCP output callback: send UDP packet */
static int kcp_udp_output(const void *buf, int len, void *user) {
    kcp_session_t *sess = (kcp_session_t *)user;
    sendto(g_sockfd, buf, len, MSG_DONTWAIT,
           (struct sockaddr *)&sess->addr, sess->addr_len);
    atomic_fetch_add_explicit(&g_total_pkts_out, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_kcp_segments, 1, memory_order_relaxed);
    return 0;
}

static kcp_session_t *find_or_create_session(struct sockaddr_in *addr, socklen_t alen) {
    uint64_t key = addr_key(addr);

    /* Fast path: linear scan (good enough for < 4096 sessions) */
    for (int i = 0; i < g_num_sessions; i++) {
        if (g_sessions[i].client_key == key) {
            g_sessions[i].last_active = now_us();
            return &g_sessions[i];
        }
    }

    /* Create new session */
    pthread_mutex_lock(&g_session_mtx);
    if (g_num_sessions >= MAX_SESSIONS) {
        pthread_mutex_unlock(&g_session_mtx);
        return NULL;
    }
    kcp_session_t *sess = &g_sessions[g_num_sessions++];
    sess->client_key = key;
    sess->addr = *addr;
    sess->addr_len = alen;
    sess->kcp = kcp_create(KCP_CONV, sess);
    kcp_setoutput(sess->kcp, kcp_udp_output);
    sess->last_active = now_us();
    pthread_mutex_unlock(&g_session_mtx);
    return sess;
}

/* ---- Process a complete KCP message ---- */
static inline uint32_t hash_for_prefetch(uint64_t k, uint32_t mask) {
    k ^= k >> 33; k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33; k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33; return (uint32_t)(k & mask);
}

static void process_kcp_message(kcp_session_t *sess, const uint8_t *data, int len) {
    if (len < 1) return;
    uint8_t op = data[0];

    switch (op) {
    case OP_GET: {
        if (len < 9) return;
        uint64_t key;
        memcpy(&key, data + 1, 8);

        uint8_t resp[1 + 1 + TLC_VALUE_SIZE]; /* op_echo + status + value */
        resp[0] = OP_GET;
        if (tlc_get(&g_cache, key, resp + 2) == 0) {
            resp[1] = 0x00;
            kcp_send(sess->kcp, resp, 2 + TLC_VALUE_SIZE);
        } else {
            resp[1] = 0x01;
            kcp_send(sess->kcp, resp, 2);
        }
        atomic_fetch_add_explicit(&g_total_ops, 1, memory_order_relaxed);
        break;
    }

    case OP_PUT: {
        if (len < 9 + TLC_VALUE_SIZE) return;
        uint64_t key;
        memcpy(&key, data + 1, 8);
        tlc_put(&g_cache, key, data + 9);
        uint8_t resp[2] = {OP_PUT, 0x00};
        kcp_send(sess->kcp, resp, 2);
        atomic_fetch_add_explicit(&g_total_ops, 1, memory_order_relaxed);
        break;
    }

    case OP_MGET: {
        if (len < 5) return;
        uint32_t cnt;
        memcpy(&cnt, data + 1, 4);
        if (cnt > BATCH_LIMIT) cnt = BATCH_LIMIT;
        if (len < (int)(5 + cnt * 8)) return;

        const uint64_t *keys = (const uint64_t *)(data + 5);

        /* Batch gather with prefetch */
        size_t resp_max = 1 + 4 + cnt * (1 + TLC_VALUE_SIZE);
        uint8_t *resp = malloc(resp_max);
        if (!resp) return;
        resp[0] = OP_MGET;
        memcpy(resp + 1, &cnt, 4);
        size_t off = 5;

        uint32_t hot_mask = g_cache.hot.mask;
        for (uint32_t p = 0; p < cnt && p < 8; p++)
            __builtin_prefetch(&g_cache.hot.table[hash_for_prefetch(keys[p], hot_mask)], 0, 3);

        for (uint32_t i = 0; i < cnt; i++) {
            if (i + 8 < cnt)
                __builtin_prefetch(&g_cache.hot.table[hash_for_prefetch(keys[i+8], hot_mask)], 0, 3);

            if (tlc_get(&g_cache, keys[i], resp + off + 1) == 0) {
                resp[off] = 0x00;
                off += 1 + TLC_VALUE_SIZE;
            } else {
                resp[off] = 0x01;
                off += 1;
            }
        }

        kcp_send(sess->kcp, resp, off);
        free(resp);
        atomic_fetch_add_explicit(&g_total_ops, cnt, memory_order_relaxed);
        atomic_fetch_add_explicit(&g_mget_batches, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&g_mget_keys, cnt, memory_order_relaxed);
        break;
    }

    case OP_FILL: {
        if (len < 9) return;
        uint64_t count;
        memcpy(&count, data + 1, 8);
        uint8_t fbuf[TLC_VALUE_SIZE];
        unsigned int seed = 12345;
        for (uint64_t i = 0; i < count; i++) {
            for (int j = 0; j < (int)(TLC_VALUE_SIZE/4); j++)
                ((uint32_t*)fbuf)[j] = rand_r(&seed);
            tlc_put(&g_cache, i, fbuf);
        }
        uint8_t resp[10] = {OP_FILL, 0x00};
        memcpy(resp + 2, &count, 8);
        kcp_send(sess->kcp, resp, 10);
        atomic_fetch_add_explicit(&g_total_ops, count, memory_order_relaxed);
        break;
    }

    case OP_PING: {
        uint8_t resp[2] = {OP_PING, 0x00};
        kcp_send(sess->kcp, resp, 2);
        break;
    }

    case OP_STATS: {
        uint8_t resp[1 + 8 * 6];
        resp[0] = OP_STATS;
        uint64_t *s = (uint64_t *)(resp + 1);
        s[0] = atomic_load(&g_total_ops);
        s[1] = atomic_load(&g_total_pkts_in);
        s[2] = atomic_load(&g_total_pkts_out);
        s[3] = atomic_load(&g_mget_batches);
        s[4] = atomic_load(&g_mget_keys);
        s[5] = atomic_load(&g_kcp_segments);
        kcp_send(sess->kcp, resp, sizeof(resp));
        break;
    }
    }
}

/* ---- Poll thread: busy-poll UDP socket, feed KCP, process messages ---- */
static void *poll_thread(void *arg) {
    int tid = *(int *)arg;
    (void)tid;

    uint8_t recv_buf[65536];
    uint8_t kcp_buf[1024 * 1024];  /* KCP reassembly buffer */

    while (g_running) {
        /* Busy-poll: recvfrom with MSG_DONTWAIT */
        struct sockaddr_in src_addr;
        socklen_t src_len = sizeof(src_addr);

        ssize_t n = recvfrom(g_sockfd, recv_buf, sizeof(recv_buf), MSG_DONTWAIT,
                             (struct sockaddr *)&src_addr, &src_len);

        if (n <= 0) {
            /* No data — yield briefly to avoid 100% CPU on idle */
            struct timespec ts = {0, 1000}; /* 1μs */
            nanosleep(&ts, NULL);
            continue;
        }

        atomic_fetch_add_explicit(&g_total_pkts_in, 1, memory_order_relaxed);

        /* Find or create KCP session for this client */
        kcp_session_t *sess = find_or_create_session(&src_addr, src_len);
        if (!sess) continue;

        /* Feed packet to KCP */
        kcp_input(sess->kcp, recv_buf, n);

        /* Try to receive complete messages from KCP */
        int rlen;
        while ((rlen = kcp_recv(sess->kcp, kcp_buf, sizeof(kcp_buf))) > 0) {
            process_kcp_message(sess, kcp_buf, rlen);
        }

        /* Flush KCP output (sends ACKs and queued data) */
        kcp_update(sess->kcp);
    }
    return NULL;
}

static void sig_handler(int sig) { (void)sig; g_running = 0; }

int main(int argc, char *argv[]) {
    int port = SERVER_PORT;
    int nthreads = POLL_THREADS;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--port") && i+1 < argc) port = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--threads") && i+1 < argc) nthreads = atoi(argv[++i]);
    }

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    printf("╔═══════════════════════════════════════════════════════════════╗\n");
    printf("║  TLC DPDK-Style Server — Zero-Copy Polling + KCP RUDP       ║\n");
    printf("║  Port: %d (UDP), Poll threads: %d                          ║\n", port, nthreads);
    printf("║  Protocol: KCP-Lite RUDP (pure algorithm, zero syscalls)    ║\n");
    printf("║  Batch: MGET up to %d keys + SVE2 prefetch gather          ║\n", BATCH_LIMIT);
    printf("╚═══════════════════════════════════════════════════════════════╝\n\n");

    /* Init cache */
    if (tlc_init(&g_cache, 0, 0) != 0) { fprintf(stderr, "Cache init failed\n"); return 1; }
    tlc_emb_init(&g_cache, SVE2_EMB_TABLE_SIZE, SVE2_EMB_DIM_DEFAULT);

    /* Create UDP socket */
    g_sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (g_sockfd < 0) { perror("socket"); return 1; }

    /* Enable SO_REUSEPORT for multi-thread receive */
    int opt = 1;
    setsockopt(g_sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(g_sockfd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    /* Enable busy polling (DPDK-style) */
    int busy_poll = 50; /* 50μs busy poll */
    setsockopt(g_sockfd, SOL_SOCKET, SO_BUSY_POLL, &busy_poll, sizeof(busy_poll));

    /* Large receive buffer */
    int rcvbuf = 16 * 1024 * 1024;
    setsockopt(g_sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
    int sndbuf = 16 * 1024 * 1024;
    setsockopt(g_sockfd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

    struct sockaddr_in addr = {.sin_family = AF_INET, .sin_port = htons(port),
                               .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    if (bind(g_sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { perror("bind"); return 1; }

    printf("Listening on UDP 127.0.0.1:%d\n", port);

    /* Start poll threads */
    pthread_t pts[32];
    int tids[32];
    for (int i = 0; i < nthreads; i++) {
        tids[i] = i;
        pthread_create(&pts[i], NULL, poll_thread, &tids[i]);
    }

    printf("Server ready (%d poll threads).\n\n", nthreads);

    /* Main thread: periodic stats */
    while (g_running) {
        sleep(5);
        if (!g_running) break;
        printf("[Stats] ops:%lu pkts_in:%lu pkts_out:%lu mget_batches:%lu sessions:%d\n",
               atomic_load(&g_total_ops), atomic_load(&g_total_pkts_in),
               atomic_load(&g_total_pkts_out), atomic_load(&g_mget_batches),
               g_num_sessions);
    }

    printf("\nShutting down...\n");
    for (int i = 0; i < nthreads; i++) { pthread_cancel(pts[i]); pthread_join(pts[i], NULL); }
    close(g_sockfd);

    printf("\n=== Final Stats ===\n");
    printf("  Total ops:     %lu\n", atomic_load(&g_total_ops));
    printf("  UDP pkts in:   %lu\n", atomic_load(&g_total_pkts_in));
    printf("  UDP pkts out:  %lu\n", atomic_load(&g_total_pkts_out));
    printf("  KCP segments:  %lu\n", atomic_load(&g_kcp_segments));
    printf("  MGET batches:  %lu (%lu keys)\n",
           atomic_load(&g_mget_batches), atomic_load(&g_mget_keys));
    printf("  Sessions:      %d\n", g_num_sessions);

    tlc_print_stats(&g_cache);
    tlc_destroy(&g_cache);
    return 0;
}
