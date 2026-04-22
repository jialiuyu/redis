/*
 * Unified Server Benchmark Client
 * TCP + UDS 多传输层测试
 *
 * Protocol (same for TCP and UDS):
 *   GET: [0x01][8B key] -> [1B status][1200B value]
 *   PUT: [0x02][8B key][1200B value] -> [1B status]
 *   MGET: [0x03][4B count][N*8B keys] -> [4B count][N*(1B+1200B)]
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
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>

#define TCP_PORT    6381
#define UDS_PATH    "/tmp/tlc.sock"
#define VALUE_SIZE  1200
#define OP_GET      0x01
#define OP_PUT      0x02
#define OP_MGET     0x03
#define OP_PING     0x06

#define MAX_THREADS 16
#define MAX_KEYS    1100000

typedef struct {
    int tid;
    int fd;
    int use_uds;
    size_t nops;
    int wpct;
    int pipeline;
    uint64_t max_key;
    uint64_t ns_total;
    uint64_t ops_done;
    uint64_t latency_ns;
} thread_ctx_t;

static atomic_uint_fast64_t g_total_ops = 0;
static atomic_uint_fast64_t g_total_ns = 0;

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static int read_full(int fd, void *buf, size_t n) {
    size_t d = 0;
    while (d < n) {
        ssize_t r = read(fd, (char *)buf + d, n - d);
        if (r <= 0) return -1;
        d += r;
    }
    return 0;
}

static int write_full(int fd, const void *buf, size_t n) {
    size_t d = 0;
    while (d < n) {
        ssize_t w = write(fd, (const char *)buf + d, n - d);
        if (w <= 0) return -1;
        d += w;
    }
    return 0;
}

static int connect_tcp(void) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(TCP_PORT)
    };
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int connect_uds(void) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    
    struct sockaddr_un addr = {
        .sun_family = AF_UNIX
    };
    strncpy(addr.sun_path, UDS_PATH, sizeof(addr.sun_path) - 1);
    
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static void *worker(void *arg) {
    thread_ctx_t *ctx = (thread_ctx_t *)arg;
    unsigned seed = ctx->tid + 42;
    
    ctx->fd = ctx->use_uds ? connect_uds() : connect_tcp();
    if (ctx->fd < 0) {
        fprintf(stderr, "Thread %d: connect failed (%s)\n", ctx->tid,
                ctx->use_uds ? "UDS" : "TCP");
        return NULL;
    }
    
    uint8_t value[VALUE_SIZE];
    for (int i = 0; i < VALUE_SIZE; i++) value[i] = (char)(rand_r(&seed) & 0xFF);
    
    uint8_t req[9 + VALUE_SIZE];
    uint8_t resp[1 + VALUE_SIZE];
    
    uint64_t start = now_ns();
    
    for (size_t i = 0; i < ctx->nops; i++) {
        uint64_t key = (rand_r(&seed) % ctx->max_key);
        int is_write = (rand_r(&seed) % 100) < ctx->wpct;
        
        uint64_t req_start = now_ns();
        
        if (is_write) {
            req[0] = OP_PUT;
            memcpy(req + 1, &key, 8);
            memcpy(req + 9, value, VALUE_SIZE);
            write_full(ctx->fd, req, 9 + VALUE_SIZE);
            read_full(ctx->fd, resp, 1);
        } else {
            req[0] = OP_GET;
            memcpy(req + 1, &key, 8);
            write_full(ctx->fd, req, 9);
            read_full(ctx->fd, resp, 1);
            if (resp[0] == 0x00) {
                read_full(ctx->fd, resp + 1, VALUE_SIZE);
            }
        }
        
        ctx->latency_ns += (now_ns() - req_start);
        ctx->ops_done++;
    }
    
    uint64_t end = now_ns();
    ctx->ns_total = end - start;
    
    atomic_fetch_add(&g_total_ops, ctx->ops_done);
    atomic_fetch_add(&g_total_ns, ctx->ns_total);
    
    close(ctx->fd);
    return NULL;
}

static void *worker_pipeline(void *arg) {
    thread_ctx_t *ctx = (thread_ctx_t *)arg;
    unsigned seed = ctx->tid + 42;
    
    ctx->fd = ctx->use_uds ? connect_uds() : connect_tcp();
    if (ctx->fd < 0) return NULL;
    
    uint8_t value[VALUE_SIZE];
    for (int i = 0; i < VALUE_SIZE; i++) value[i] = (char)(rand_r(&seed) & 0xFF);
    
    int P = ctx->pipeline;
    uint8_t *req_buf = malloc(P * (9 + VALUE_SIZE));
    int *is_write = malloc(P * sizeof(int));
    uint8_t *resp_buf = malloc(P * (1 + VALUE_SIZE));
    
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
                write_full(ctx->fd, req, 9 + VALUE_SIZE);
            } else {
                req[0] = OP_GET;
                memcpy(req + 1, &key, 8);
                write_full(ctx->fd, req, 9);
            }
        }
        
        for (int b = 0; b < batch; b++) {
            uint8_t st;
            if (read_full(ctx->fd, &st, 1) != 0) break;
            if (!is_write[b] && st == 0x00) {
                char v[VALUE_SIZE];
                read_full(ctx->fd, v, VALUE_SIZE);
            }
            recv_cnt++;
            ctx->ops_done++;
        }
    }
    
    uint64_t end = now_ns();
    ctx->ns_total = end - start;
    
    atomic_fetch_add(&g_total_ops, ctx->ops_done);
    atomic_fetch_add(&g_total_ns, ctx->ns_total);
    
    free(req_buf);
    free(is_write);
    free(resp_buf);
    close(ctx->fd);
    
    return NULL;
}

int main(int argc, char **argv) {
    int threads = 8;
    size_t nops = 500000;
    int wpct = 20;
    int pipeline = 16;
    uint64_t max_key = MAX_KEYS;
    int use_uds = 0;
    
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--threads")) threads = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--ops")) nops = atol(argv[++i]);
        else if (!strcmp(argv[i], "--write")) wpct = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--pipeline")) pipeline = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--maxkey")) max_key = atol(argv[++i]);
        else if (!strcmp(argv[i], "--uds")) use_uds = 1;
        else if (!strcmp(argv[i], "--tcp")) use_uds = 0;
    }
    
    printf("Unified Server Benchmark (%s, port %d)\n",
           use_uds ? "UDS" : "TCP", use_uds ? -1 : TCP_PORT);
    printf("Ops: %zu  Threads: %d  Pipeline: %d  W%%: %d  MaxKey: %lu\n\n",
           nops, threads, pipeline, wpct, max_key);
    
    thread_ctx_t *ctxs = calloc(threads, sizeof(thread_ctx_t));
    pthread_t *pts = calloc(threads, sizeof(pthread_t));
    
    for (int i = 0; i < threads; i++) {
        ctxs[i].tid = i;
        ctxs[i].use_uds = use_uds;
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