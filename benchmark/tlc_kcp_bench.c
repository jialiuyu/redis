/*
 * TLC KCP Benchmark Client — UDP + KCP RUDP
 *
 * Connects to the DPDK-style TLC server via UDP/KCP.
 * Tests: single GET/PUT, MGET batch, FILL.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <math.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <errno.h>

#include "../src/kcp_lite.h"

#define VALUE_SIZE 1200
#define OP_GET  0x01
#define OP_PUT  0x02
#define OP_MGET 0x03
#define OP_FILL 0x05
#define OP_PING 0x06
#define KCP_CONV 0x544C43

static inline uint64_t now_ns(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec*1000000000ULL + ts.tv_nsec;
}
static uint64_t zipf(unsigned *s, uint64_t mx) {
    return (uint64_t)(pow((double)rand_r(s)/RAND_MAX,1.0/1.2)*(double)mx)%mx;
}

typedef struct {
    int sockfd;
    struct sockaddr_in server_addr;
} udp_ctx_t;

static int kcp_udp_output(const void *buf, int len, void *user) {
    udp_ctx_t *ctx = (udp_ctx_t *)user;
    sendto(ctx->sockfd, buf, len, MSG_DONTWAIT,
           (struct sockaddr *)&ctx->server_addr, sizeof(ctx->server_addr));
    return 0;
}

/* Send KCP message and wait for response */
static int kcp_request(kcp_t *kcp, udp_ctx_t *udp, const void *req, int req_len,
                       void *resp, int resp_max, int timeout_ms) {
    kcp_send(kcp, req, req_len);
    kcp_flush(kcp);

    uint8_t pkt[65536];
    uint64_t deadline = now_ns() + (uint64_t)timeout_ms * 1000000ULL;

    while (now_ns() < deadline) {
        struct sockaddr_in from;
        socklen_t flen = sizeof(from);
        ssize_t n = recvfrom(udp->sockfd, pkt, sizeof(pkt), MSG_DONTWAIT,
                             (struct sockaddr *)&from, &flen);
        if (n > 0) {
            kcp_input(kcp, pkt, n);
            int rlen = kcp_recv(kcp, resp, resp_max);
            if (rlen > 0) return rlen;
        }
        kcp_update(kcp);
        struct timespec ts = {0, 100000}; /* 100μs */
        nanosleep(&ts, NULL);
    }
    return -1; /* timeout */
}

typedef struct {
    int tid, port; size_t nops; int wpct; uint64_t max_key;
    uint64_t ns, put_ok, get_ok, get_miss;
} bench_t;

static void *worker(void *arg) {
    bench_t *t = arg;
    unsigned seed = t->tid + 42;

    /* Create UDP socket */
    udp_ctx_t udp;
    udp.sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp.sockfd < 0) { t->ns = 0; return NULL; }

    /* Bind to ephemeral port */
    struct sockaddr_in local = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    bind(udp.sockfd, (struct sockaddr *)&local, sizeof(local));

    udp.server_addr.sin_family = AF_INET;
    udp.server_addr.sin_port = htons(t->port);
    inet_pton(AF_INET, "127.0.0.1", &udp.server_addr.sin_addr);

    /* Create KCP session */
    kcp_t *kcp = kcp_create(KCP_CONV, &udp);
    kcp_setoutput(kcp, kcp_udp_output);

    char value[VALUE_SIZE];
    for (int i = 0; i < VALUE_SIZE; i++) value[i] = (char)(rand_r(&seed) & 0xFF);

    uint8_t req[16 + VALUE_SIZE];
    uint8_t resp[16 + VALUE_SIZE];

    uint64_t start = now_ns();
    for (size_t i = 0; i < t->nops; i++) {
        uint64_t key = zipf(&seed, t->max_key);
        int is_write = (rand_r(&seed) % 100) < t->wpct;

        if (is_write) {
            req[0] = OP_PUT;
            memcpy(req + 1, &key, 8);
            memcpy(req + 9, value, VALUE_SIZE);
            int rlen = kcp_request(kcp, &udp, req, 9 + VALUE_SIZE, resp, sizeof(resp), 1000);
            if (rlen >= 2 && resp[1] == 0x00) t->put_ok++;
        } else {
            req[0] = OP_GET;
            memcpy(req + 1, &key, 8);
            int rlen = kcp_request(kcp, &udp, req, 9, resp, sizeof(resp), 1000);
            if (rlen >= 2) {
                if (resp[1] == 0x00) t->get_ok++;
                else t->get_miss++;
            }
        }
    }
    t->ns = now_ns() - start;

    kcp_release(kcp);
    close(udp.sockfd);
    return NULL;
}

static void run(const char *label, int port, size_t ops, int nt, int wpct, uint64_t maxk) {
    printf("\n  %s (Ops:%zu Thr:%d W%%:%d)\n", label, ops, nt, wpct);
    bench_t *th = calloc(nt, sizeof(*th));
    pthread_t *pt = calloc(nt, sizeof(*pt));
    for (int i = 0; i < nt; i++) {
        th[i] = (bench_t){i, port, ops/nt, wpct, maxk, 0, 0, 0, 0};
        pthread_create(&pt[i], NULL, worker, &th[i]);
    }
    for (int i = 0; i < nt; i++) pthread_join(pt[i], NULL);
    uint64_t mx = 0, p = 0, g = 0, m = 0;
    for (int i = 0; i < nt; i++) {
        if (th[i].ns > mx) mx = th[i].ns;
        p += th[i].put_ok; g += th[i].get_ok; m += th[i].get_miss;
    }
    double s = (double)mx / 1e9;
    printf("    → %.0f QPS (%.2f M/s) Lat:%.1fμs PUT:%lu GET:%lu MISS:%lu\n",
           (double)ops/s, (double)ops/s/1e6, (double)mx/ops/1e3, p, g, m);
    free(th); free(pt);
}

/* MGET benchmark */
typedef struct {
    int tid, port; size_t n_batches; uint32_t batch_size; uint64_t max_key;
    uint64_t ns, total_keys, hits, misses;
} mget_bench_t;

static void *mget_worker(void *arg) {
    mget_bench_t *t = arg;
    unsigned seed = t->tid + 500;

    udp_ctx_t udp;
    udp.sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in local = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    bind(udp.sockfd, (struct sockaddr *)&local, sizeof(local));
    udp.server_addr.sin_family = AF_INET;
    udp.server_addr.sin_port = htons(t->port);
    inet_pton(AF_INET, "127.0.0.1", &udp.server_addr.sin_addr);

    kcp_t *kcp = kcp_create(KCP_CONV, &udp);
    kcp_setoutput(kcp, kcp_udp_output);

    size_t req_size = 1 + 4 + t->batch_size * 8;
    uint8_t *req = malloc(req_size);
    size_t resp_max = 1 + 4 + t->batch_size * (1 + VALUE_SIZE);
    uint8_t *resp = malloc(resp_max);

    uint64_t start = now_ns();
    for (size_t b = 0; b < t->n_batches; b++) {
        req[0] = OP_MGET;
        memcpy(req + 1, &t->batch_size, 4);
        uint64_t *keys = (uint64_t *)(req + 5);
        for (uint32_t i = 0; i < t->batch_size; i++)
            keys[i] = zipf(&seed, t->max_key);

        int rlen = kcp_request(kcp, &udp, req, req_size, resp, resp_max, 5000);
        if (rlen > 5) {
            uint32_t cnt;
            memcpy(&cnt, resp + 1, 4);
            int off = 5;
            for (uint32_t i = 0; i < cnt && off < rlen; i++) {
                if (resp[off] == 0x00) { t->hits++; off += 1 + VALUE_SIZE; }
                else { t->misses++; off += 1; }
            }
            t->total_keys += cnt;
        }
    }
    t->ns = now_ns() - start;

    free(req); free(resp);
    kcp_release(kcp);
    close(udp.sockfd);
    return NULL;
}

static void run_mget(const char *label, int port, size_t n_batches, int nt,
                     uint32_t batch_size, uint64_t maxk) {
    printf("\n  %s (Batches:%zu×%u keys, Thr:%d)\n", label, n_batches, batch_size, nt);
    mget_bench_t *th = calloc(nt, sizeof(*th));
    pthread_t *pt = calloc(nt, sizeof(*pt));
    for (int i = 0; i < nt; i++) {
        th[i] = (mget_bench_t){i, port, n_batches/nt, batch_size, maxk, 0, 0, 0, 0};
        pthread_create(&pt[i], NULL, mget_worker, &th[i]);
    }
    for (int i = 0; i < nt; i++) pthread_join(pt[i], NULL);
    uint64_t mx = 0, tk = 0, h = 0, m = 0;
    for (int i = 0; i < nt; i++) {
        if (th[i].ns > mx) mx = th[i].ns;
        tk += th[i].total_keys; h += th[i].hits; m += th[i].misses;
    }
    double s = (double)mx / 1e9;
    printf("    → %.0f keys/s (%.2f M/s) Lat:%.1fμs/batch Hits:%lu Miss:%lu\n",
           (double)tk/s, (double)tk/s/1e6, (double)mx/n_batches/1e3, h, m);
    free(th); free(pt);
}

int main(int argc, char *argv[]) {
    size_t ops = 100000;
    int threads = 4;
    int port = 6382;

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--ops") && i+1 < argc) ops = (size_t)atol(argv[++i]);
        else if (!strcmp(argv[i], "--threads") && i+1 < argc) threads = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--port") && i+1 < argc) port = atoi(argv[++i]);
    }

    uint64_t maxk = 1100000;

    printf("╔═══════════════════════════════════════════════════════════════════╗\n");
    printf("║  TLC KCP/UDP Benchmark                                           ║\n");
    printf("║  Ops: %zu  Threads: %d  Port: %d (UDP)                         ║\n",
           ops, threads, port);
    printf("╚═══════════════════════════════════════════════════════════════════╝\n");

    /* Fill via single connection */
    printf("\n  Filling server with 1.1M entries...\n");
    udp_ctx_t udp;
    udp.sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in local = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    bind(udp.sockfd, (struct sockaddr *)&local, sizeof(local));
    udp.server_addr.sin_family = AF_INET;
    udp.server_addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &udp.server_addr.sin_addr);

    kcp_t *kcp = kcp_create(KCP_CONV, &udp);
    kcp_setoutput(kcp, kcp_udp_output);

    uint8_t fill_req[9];
    fill_req[0] = OP_FILL;
    uint64_t fill_count = 1100000;
    memcpy(fill_req + 1, &fill_count, 8);
    uint8_t fill_resp[16];
    int rlen = kcp_request(kcp, &udp, fill_req, 9, fill_resp, sizeof(fill_resp), 30000);
    if (rlen > 0) printf("    Fill response received (%d bytes)\n", rlen);
    else printf("    Fill timeout (server may still be filling)\n");
    kcp_release(kcp);
    close(udp.sockfd);
    sleep(2); /* Wait for fill to complete */

    /* Single GET/PUT benchmarks */
    printf("\n╔══════════════════════════════════════╗\n");
    printf("║  KCP/UDP Single GET/PUT              ║\n");
    printf("╚══════════════════════════════════════╝\n");

    run("KCP 80R/20W", port, ops, threads, 20, maxk);
    run("KCP 100%% GET", port, ops, threads, 0, maxk);

    /* MGET batch benchmarks */
    printf("\n╔══════════════════════════════════════════════╗\n");
    printf("║  KCP/UDP MGET Batch + SVE2 Gather            ║\n");
    printf("╚══════════════════════════════════════════════╝\n");

    run_mget("MGET batch=100", port, 1000, threads, 100, maxk);
    run_mget("MGET batch=500", port, 200, threads, 500, maxk);

    printf("\n✅ KCP Benchmark complete.\n");
    return 0;
}
