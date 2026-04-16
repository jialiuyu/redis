/*
 * TLC Raw Protocol Benchmark Client
 *
 * Connects to the pure userspace TLC server (port 6381)
 * using the binary protocol. Multi-threaded, pipelined.
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
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <stdint.h>

#define VALUE_SIZE 1200
#define OP_GET  0x01
#define OP_PUT  0x02
#define OP_PING 0x06
#define OP_FILL 0x05

static inline uint64_t now_ns(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec*1000000000ULL + ts.tv_nsec;
}
static uint64_t zipf(unsigned *s, uint64_t mx) {
    return (uint64_t)(pow((double)rand_r(s)/RAND_MAX,1.0/1.2)*(double)mx)%mx;
}

static int read_full(int fd, void *buf, size_t n) {
    size_t d=0; while(d<n){ssize_t r=read(fd,(char*)buf+d,n-d);if(r<=0)return -1;d+=r;} return 0;
}
static int write_full(int fd, const void *buf, size_t n) {
    size_t d=0; while(d<n){ssize_t w=write(fd,(const char*)buf+d,n-d);if(w<=0)return -1;d+=w;} return 0;
}

static int connect_server(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    struct sockaddr_in addr = {.sin_family=AF_INET, .sin_port=htons(port)};
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { close(fd); return -1; }
    return fd;
}

typedef struct {
    int tid, port; size_t nops; int wpct; uint64_t max_key;
    int pipeline;
    uint64_t ns, put_ok, get_ok, get_miss;
} bench_t;

static void *worker(void *arg) {
    bench_t *t = arg;
    unsigned seed = t->tid + 42;
    int fd = connect_server(t->port);
    if (fd < 0) { fprintf(stderr, "T%d: connect failed\n", t->tid); t->ns=0; return NULL; }

    char value[VALUE_SIZE];
    for (int i=0;i<VALUE_SIZE;i++) value[i]=(char)(rand_r(&seed)&0xFF);

    int P = t->pipeline > 0 ? t->pipeline : 1;
    int *ops = malloc(P * sizeof(int));  /* 0=GET, 1=PUT */

    uint64_t start = now_ns();
    size_t i = 0;
    while (i < t->nops) {
        /* Send up to P requests */
        int batch = 0;
        for (; batch < P && i + batch < t->nops; batch++) {
            uint64_t key = zipf(&seed, t->max_key);
            int is_write = (rand_r(&seed) % 100) < t->wpct;
            ops[batch] = is_write;

            if (is_write) {
                uint8_t op = OP_PUT;
                write_full(fd, &op, 1);
                write_full(fd, &key, 8);
                write_full(fd, value, VALUE_SIZE);
            } else {
                uint8_t op = OP_GET;
                write_full(fd, &op, 1);
                write_full(fd, &key, 8);
            }
        }

        /* Read all responses */
        for (int b = 0; b < batch; b++) {
            uint8_t status;
            if (read_full(fd, &status, 1) != 0) goto done;
            if (ops[b]) {
                /* PUT response: just status */
                if (status == 0) t->put_ok++;
            } else {
                /* GET response: status + optional value */
                if (status == 0) {
                    char rbuf[VALUE_SIZE];
                    if (read_full(fd, rbuf, VALUE_SIZE) != 0) goto done;
                    t->get_ok++;
                } else {
                    t->get_miss++;
                }
            }
        }
        i += batch;
    }
done:
    t->ns = now_ns() - start;
    free(ops);
    close(fd);
    return NULL;
}

static void run(const char *label, int port, size_t ops, int nt, int wpct, uint64_t maxk, int pipeline) {
    printf("\n  %s (Ops:%zu Thr:%d W%%:%d P:%d)\n", label, ops, nt, wpct, pipeline);
    bench_t *th = calloc(nt, sizeof(*th));
    pthread_t *pt = calloc(nt, sizeof(*pt));
    for (int i=0;i<nt;i++) {
        th[i]=(bench_t){i,port,ops/nt,wpct,maxk,pipeline,0,0,0,0};
        pthread_create(&pt[i],NULL,worker,&th[i]);
    }
    for (int i=0;i<nt;i++) pthread_join(pt[i],NULL);
    uint64_t mx=0,p=0,g=0,m=0;
    for (int i=0;i<nt;i++){if(th[i].ns>mx)mx=th[i].ns;p+=th[i].put_ok;g+=th[i].get_ok;m+=th[i].get_miss;}
    double s=(double)mx/1e9;
    printf("    → %.0f QPS (%.2f M/s) Lat:%.1fμs PUT:%lu GET:%lu MISS:%lu\n",
           (double)ops/s, (double)ops/s/1e6, (double)mx/ops/1e3, p,g,m);
    free(th); free(pt);
}

/* ============================================================
 * MGET Benchmark — batch merge + SVE2 gather load
 * ============================================================ */
#define OP_MGET 0x03

typedef struct {
    int tid, port; size_t n_batches; uint32_t batch_size; uint64_t max_key;
    uint64_t ns, total_keys, hits, misses;
} mget_bench_t;

static void *mget_worker(void *arg) {
    mget_bench_t *t = arg;
    unsigned seed = t->tid + 500;
    int fd = connect_server(t->port);
    if (fd < 0) { t->ns = 0; return NULL; }

    uint64_t *keys = malloc(t->batch_size * 8);
    /* Response buffer: 4B count + batch_size × (1B + 1200B) */
    size_t resp_max = 4 + t->batch_size * (1 + VALUE_SIZE);
    uint8_t *resp = malloc(resp_max);

    uint64_t start = now_ns();
    for (size_t b = 0; b < t->n_batches; b++) {
        /* Generate batch of keys */
        for (uint32_t i = 0; i < t->batch_size; i++)
            keys[i] = zipf(&seed, t->max_key);

        /* Send MGET: [0x03][4B count][N × 8B key] */
        uint8_t op = OP_MGET;
        write_full(fd, &op, 1);
        write_full(fd, &t->batch_size, 4);
        write_full(fd, keys, t->batch_size * 8);

        /* Read response: [4B count][N × (1B status + optional 1200B)] */
        uint32_t cnt;
        read_full(fd, &cnt, 4);
        for (uint32_t i = 0; i < cnt; i++) {
            uint8_t status;
            if (read_full(fd, &status, 1) != 0) goto done;
            if (status == 0x00) {
                if (read_full(fd, resp, VALUE_SIZE) != 0) goto done;
                t->hits++;
            } else {
                t->misses++;
            }
        }
        t->total_keys += cnt;
    }
done:
    t->ns = now_ns() - start;
    free(keys);
    free(resp);
    close(fd);
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
    printf("    → %.0f keys/s (%.2f M/s)  Lat: %.1f μs/batch  Hits:%lu Miss:%lu\n",
           (double)tk / s, (double)tk / s / 1e6, (double)mx / n_batches / 1e3, h, m);
    free(th); free(pt);
}

int main(int argc, char *argv[]) {
    size_t ops = 500000;
    int threads = 8;
    int port_tlc = 6381;
    int port_redis_base = 6379;
    int port_redis_opt = 6380;

    for (int i=1;i<argc;i++) {
        if (!strcmp(argv[i],"--ops")&&i+1<argc) ops=(size_t)atol(argv[++i]);
        else if (!strcmp(argv[i],"--threads")&&i+1<argc) threads=atoi(argv[++i]);
    }

    uint64_t maxk = 1100000;

    printf("╔═══════════════════════════════════════════════════════════════════╗\n");
    printf("║  TLC Raw Protocol Benchmark                                      ║\n");
    printf("║  Ops: %zu  Threads: %d  Value: %dB  Keys: %lu                  ║\n",
           ops, threads, VALUE_SIZE, maxk);
    printf("╚═══════════════════════════════════════════════════════════════════╝\n");

    /* First fill the TLC server */
    printf("\n  Filling TLC server with 1.1M entries...\n");
    int fd = connect_server(port_tlc);
    if (fd < 0) {
        fprintf(stderr, "Cannot connect to TLC server on port %d\n", port_tlc);
        printf("  (Start with: ./src/tlc-server --port %d)\n", port_tlc);
        return 1;
    }
    uint8_t op = OP_FILL;
    uint64_t fill_count = 1100000;
    write_full(fd, &op, 1);
    write_full(fd, &fill_count, 8);
    uint8_t status;
    read_full(fd, &status, 1);
    uint64_t filled;
    read_full(fd, &filled, 8);
    printf("    Filled %lu entries\n", filled);
    close(fd);

    /* TLC raw server benchmarks */
    printf("\n╔══════════════════════════════════════╗\n");
    printf("║  TLC Raw Server (port %d)            ║\n", port_tlc);
    printf("╚══════════════════════════════════════╝\n");

    run("TLC Raw 80R/20W (no pipeline)", port_tlc, ops, threads, 20, maxk, 1);
    run("TLC Raw 80R/20W (P=16)", port_tlc, ops, threads, 20, maxk, 16);
    run("TLC Raw 80R/20W (P=64)", port_tlc, ops, threads, 20, maxk, 64);
    run("TLC Raw 100%% GET (P=16)", port_tlc, ops, threads, 0, maxk, 16);
    run("TLC Raw 100%% GET (P=64)", port_tlc, ops, threads, 0, maxk, 64);
    run("TLC Raw 100%% PUT (P=16)", port_tlc, ops, threads, 100, maxk, 16);

    /* MGET batch merge benchmarks */
    printf("\n╔══════════════════════════════════════════════╗\n");
    printf("║  MGET Batch Merge + SVE2 Gather Load         ║\n");
    printf("╚══════════════════════════════════════════════╝\n");

    run_mget("MGET batch=100",  port_tlc, 10000, threads, 100, maxk);
    run_mget("MGET batch=500",  port_tlc, 2000,  threads, 500, maxk);
    run_mget("MGET batch=1000", port_tlc, 1000,  threads, 1000, maxk);
    run_mget("MGET batch=3000", port_tlc, 400,   threads, 3000, maxk);

    printf("\n✅ TLC Raw Benchmark complete.\n");
    return 0;
}
