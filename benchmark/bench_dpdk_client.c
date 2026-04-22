/*
 * DPDK Server Benchmark Client (Simplified)
 * UDP + KCP 协议测试，不依赖 SVE2 库
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <stdatomic.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>

#define SERVER_PORT    6382
#define VALUE_SIZE     1200
#define OP_GET         0x01
#define OP_PUT         0x02
#define OP_PING        0x06
#define KCP_CONV       0x544C43
#define KCP_MTU        1400
#define KCP_WND_SND    256
#define KCP_WND_RCV    256
#define KCP_OVERHEAD   24
#define KCP_MAX_SEG    (KCP_MTU - KCP_OVERHEAD)
#define KCP_CMD_PUSH   81
#define KCP_CMD_ACK    82

#define MAX_THREADS    16
#define MAX_KEYS       1100000

typedef struct {
    uint32_t conv;
    uint8_t  cmd;
    uint8_t  frg;
    uint16_t wnd;
    uint32_t ts;
    uint32_t sn;
    uint32_t una;
    uint32_t len;
} __attribute__((packed)) kcp_seg_hdr_t;

typedef struct {
    int tid;
    int sockfd;
    struct sockaddr_in server_addr;
    size_t nops;
    int wpct;
    int pipeline;
    uint64_t max_key;
    uint64_t ns_total;
    uint64_t ops_done;
    uint64_t latency_ns;
    uint32_t snd_nxt;
    uint32_t rcv_nxt;
} thread_ctx_t;

static atomic_uint_fast64_t g_total_ops = 0;
static atomic_uint_fast64_t g_total_ns = 0;

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static inline uint32_t now_ms(void) {
    return (uint32_t)(now_ns() / 1000000);
}

static int send_kcp(thread_ctx_t *ctx, const void *data, int len) {
    uint8_t pkt[KCP_MTU];
    kcp_seg_hdr_t hdr = {
        .conv = KCP_CONV,
        .cmd = KCP_CMD_PUSH,
        .frg = 0,
        .wnd = KCP_WND_RCV,
        .ts = now_ms(),
        .sn = ctx->snd_nxt++,
        .una = ctx->rcv_nxt,
        .len = (uint32_t)len
    };
    
    memcpy(pkt, &hdr, sizeof(hdr));
    memcpy(pkt + sizeof(hdr), data, len);
    
    return sendto(ctx->sockfd, pkt, sizeof(hdr) + len, MSG_DONTWAIT,
                  (struct sockaddr *)&ctx->server_addr, sizeof(ctx->server_addr));
}

static int recv_response(thread_ctx_t *ctx, uint8_t *buf, int max_len, int timeout_ms) {
    struct pollfd pfd = {.fd = ctx->sockfd, .events = POLLIN, .revents = 0};
    
    if (poll(&pfd, 1, timeout_ms) <= 0) return -1;
    
    struct sockaddr_in from;
    socklen_t fromlen = sizeof(from);
    uint8_t pkt[2048];
    
    int n = recvfrom(ctx->sockfd, pkt, sizeof(pkt), 0,
                     (struct sockaddr *)&from, &fromlen);
    if (n < (int)sizeof(kcp_seg_hdr_t)) return -1;
    
    kcp_seg_hdr_t hdr;
    memcpy(&hdr, pkt, sizeof(hdr));
    
    if (hdr.conv != KCP_CONV || hdr.cmd != KCP_CMD_PUSH) return -1;
    
    ctx->rcv_nxt = hdr.sn + 1;
    
    int data_len = hdr.len;
    if (data_len > max_len) data_len = max_len;
    memcpy(buf, pkt + sizeof(hdr), data_len);
    
    return data_len;
}

static void *worker(void *arg) {
    thread_ctx_t *ctx = (thread_ctx_t *)arg;
    unsigned seed = ctx->tid + 42;
    
    ctx->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (ctx->sockfd < 0) {
        fprintf(stderr, "Thread %d: socket failed\n", ctx->tid);
        return NULL;
    }
    
    fcntl(ctx->sockfd, F_SETFL, O_NONBLOCK);
    
    ctx->server_addr.sin_family = AF_INET;
    ctx->server_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, "127.0.0.1", &ctx->server_addr.sin_addr);
    
    uint8_t value[VALUE_SIZE];
    for (int i = 0; i < VALUE_SIZE; i++) value[i] = (char)(rand_r(&seed) & 0xFF);
    
    uint8_t req[9 + VALUE_SIZE];
    uint8_t resp[VALUE_SIZE + 10];
    
    uint64_t start = now_ns();
    
    for (size_t i = 0; i < ctx->nops; i++) {
        uint64_t key = (rand_r(&seed) % ctx->max_key);
        int is_write = (rand_r(&seed) % 100) < ctx->wpct;
        
        uint64_t req_start = now_ns();
        
        if (is_write) {
            req[0] = OP_PUT;
            memcpy(req + 1, &key, 8);
            memcpy(req + 9, value, VALUE_SIZE);
            send_kcp(ctx, req, 9 + VALUE_SIZE);
        } else {
            req[0] = OP_GET;
            memcpy(req + 1, &key, 8);
            send_kcp(ctx, req, 9);
        }
        
        int retries = 0;
        while (retries < 50) {
            int n = recv_response(ctx, resp, sizeof(resp), 10);
            if (n > 0) {
                ctx->latency_ns += (now_ns() - req_start);
                ctx->ops_done++;
                break;
            }
            retries++;
        }
    }
    
    uint64_t end = now_ns();
    ctx->ns_total = end - start;
    
    atomic_fetch_add(&g_total_ops, ctx->ops_done);
    atomic_fetch_add(&g_total_ns, ctx->ns_total);
    
    close(ctx->sockfd);
    return NULL;
}

static void *worker_pipeline(void *arg) {
    thread_ctx_t *ctx = (thread_ctx_t *)arg;
    unsigned seed = ctx->tid + 42;
    
    ctx->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (ctx->sockfd < 0) return NULL;
    
    fcntl(ctx->sockfd, F_SETFL, O_NONBLOCK);
    
    ctx->server_addr.sin_family = AF_INET;
    ctx->server_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, "127.0.0.1", &ctx->server_addr.sin_addr);
    
    uint8_t value[VALUE_SIZE];
    for (int i = 0; i < VALUE_SIZE; i++) value[i] = (char)(rand_r(&seed) & 0xFF);
    
    int P = ctx->pipeline;
    uint8_t *req_buf = malloc(P * (9 + VALUE_SIZE));
    int *is_write = malloc(P * sizeof(int));
    
    uint64_t start = now_ns();
    size_t sent = 0, recv_cnt = 0;
    
    while (recv_cnt < ctx->nops) {
        int batch = 0;
        for (; batch < P && sent < ctx->nops; batch++, sent++) {
            uint64_t key = (rand_r(&seed) % ctx->max_key);
            is_write[batch] = (rand_r(&seed) % 100) < ctx->wpct;
            
            uint8_t *req = req_buf + batch * (9 + VALUE_SIZE);
            if (is_write[batch]) {
                req[0] = OP_PUT;
                memcpy(req + 1, &key, 8);
                memcpy(req + 9, value, VALUE_SIZE);
                send_kcp(ctx, req, 9 + VALUE_SIZE);
            } else {
                req[0] = OP_GET;
                memcpy(req + 1, &key, 8);
                send_kcp(ctx, req, 9);
            }
        }
        
        for (int r = 0; r < 20 && recv_cnt < sent; r++) {
            uint8_t resp[VALUE_SIZE + 10];
            int n = recv_response(ctx, resp, sizeof(resp), 5);
            if (n > 0) {
                recv_cnt++;
                ctx->ops_done++;
            }
        }
    }
    
    uint64_t end = now_ns();
    ctx->ns_total = end - start;
    
    atomic_fetch_add(&g_total_ops, ctx->ops_done);
    atomic_fetch_add(&g_total_ns, ctx->ns_total);
    
    free(req_buf);
    free(is_write);
    close(ctx->sockfd);
    
    return NULL;
}

int main(int argc, char **argv) {
    int threads = 8;
    size_t nops = 500000;
    int wpct = 20;
    int pipeline = 16;
    uint64_t max_key = MAX_KEYS;
    
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--threads")) threads = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--ops")) nops = atol(argv[++i]);
        else if (!strcmp(argv[i], "--write")) wpct = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--pipeline")) pipeline = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--maxkey")) max_key = atol(argv[++i]);
    }
    
    printf("DPDK Server Benchmark (UDP+KCP, port %d)\n", SERVER_PORT);
    printf("Ops: %zu  Threads: %d  Pipeline: %d  W%%: %d  MaxKey: %lu\n\n",
           nops, threads, pipeline, wpct, max_key);
    
    thread_ctx_t *ctxs = calloc(threads, sizeof(thread_ctx_t));
    pthread_t *pts = calloc(threads, sizeof(pthread_t));
    
    for (int i = 0; i < threads; i++) {
        ctxs[i].tid = i;
        ctxs[i].nops = nops / threads;
        ctxs[i].wpct = wpct;
        ctxs[i].pipeline = pipeline;
        ctxs[i].max_key = max_key;
    }
    
    for (int i = 0; i < threads; i++) {
        if (pipeline > 1)
            pthread_create(&pts[i], NULL, worker_pipeline, &ctxs[i]);
        else
            pthread_create(&pts[i], NULL, worker, &ctxs[i]);
    }
    
    for (int i = 0; i < threads; i++) {
        pthread_join(pts[i], NULL);
    }
    
    uint64_t total_ops = atomic_load(&g_total_ops);
    uint64_t total_ns = 0;
    for (int i = 0; i < threads; i++) total_ns += ctxs[i].ns_total;
    
    double qps = total_ops > 0 ? (double)total_ops * threads * 1e9 / total_ns : 0;
    double avg_ns = total_ops > 0 ? (double)total_ns / total_ops : 0;
    
    printf("Results:\n");
    printf("  Total ops: %lu\n", total_ops);
    printf("  QPS:       %.0f\n", qps);
    printf("  Latency:   %.0f ns (%.2f us)\n", avg_ns, avg_ns / 1000.0);
    
    free(ctxs);
    free(pts);
    
    return 0;
}