/*
 * TLC Transport Benchmark — TCP vs UDS vs SHM
 *
 * Tests all three transports against the unified server.
 * Same workload, same data, different transport layers.
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
#include <sys/un.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdatomic.h>

#include "../src/tlc_transport.h"

#define VALUE_SIZE 1200
#define OP_GET  0x01
#define OP_PUT  0x02
#define OP_MGET 0x03
#define OP_FILL 0x05
#define OP_PING 0x06

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

/* ---- TCP connect ---- */
static int connect_tcp(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    struct sockaddr_in a = {.sin_family=AF_INET, .sin_port=htons(port)};
    inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
    if (connect(fd, (struct sockaddr*)&a, sizeof(a)) < 0) { close(fd); return -1; }
    return fd;
}

/* ---- UDS connect ---- */
static int connect_uds(const char *path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un a = {.sun_family = AF_UNIX};
    strncpy(a.sun_path, path, sizeof(a.sun_path) - 1);
    if (connect(fd, (struct sockaddr*)&a, sizeof(a)) < 0) { close(fd); return -1; }
    return fd;
}

/* ---- Socket-based benchmark (TCP or UDS) ---- */
typedef struct {
    int tid; size_t nops; int wpct; uint64_t max_key;
    int use_uds; int port;
    int pipeline;
    uint64_t ns, put_ok, get_ok, get_miss;
} sock_bench_t;

static void *sock_worker(void *arg) {
    sock_bench_t *t = arg;
    unsigned seed = t->tid + 42;
    int fd = t->use_uds ? connect_uds("/tmp/tlc.sock") : connect_tcp(t->port);
    if (fd < 0) { t->ns = 0; return NULL; }

    char value[VALUE_SIZE];
    for (int i = 0; i < VALUE_SIZE; i++) value[i] = (char)(rand_r(&seed) & 0xFF);

    int P = t->pipeline > 0 ? t->pipeline : 1;
    int *ops = malloc(P * sizeof(int));

    uint64_t start = now_ns();
    size_t i = 0;
    while (i < t->nops) {
        int batch = 0;
        for (; batch < P && i + batch < t->nops; batch++) {
            uint64_t key = zipf(&seed, t->max_key);
            int is_write = (rand_r(&seed) % 100) < t->wpct;
            ops[batch] = is_write;
            if (is_write) {
                uint8_t op = OP_PUT; write_full(fd, &op, 1);
                write_full(fd, &key, 8); write_full(fd, value, VALUE_SIZE);
            } else {
                uint8_t op = OP_GET; write_full(fd, &op, 1);
                write_full(fd, &key, 8);
            }
        }
        for (int b = 0; b < batch; b++) {
            uint8_t status;
            if (read_full(fd, &status, 1) != 0) goto done;
            if (ops[b]) { if (status == 0) t->put_ok++; }
            else {
                if (status == 0) { char rb[VALUE_SIZE]; read_full(fd, rb, VALUE_SIZE); t->get_ok++; }
                else t->get_miss++;
            }
        }
        i += batch;
    }
done:
    t->ns = now_ns() - start;
    free(ops); close(fd);
    return NULL;
}

static void run_sock(const char *label, int port, int use_uds, size_t ops, int nt,
                     int wpct, uint64_t maxk, int pipeline) {
    printf("\n  %s (Ops:%zu Thr:%d W%%:%d P:%d)\n", label, ops, nt, wpct, pipeline);
    sock_bench_t *th = calloc(nt, sizeof(*th));
    pthread_t *pt = calloc(nt, sizeof(*pt));
    for (int i = 0; i < nt; i++) {
        th[i] = (sock_bench_t){i, ops/nt, wpct, maxk, use_uds, port, pipeline, 0,0,0,0};
        pthread_create(&pt[i], NULL, sock_worker, &th[i]);
    }
    for (int i = 0; i < nt; i++) pthread_join(pt[i], NULL);
    uint64_t mx=0,p=0,g=0,m=0;
    for (int i=0;i<nt;i++){if(th[i].ns>mx)mx=th[i].ns;p+=th[i].put_ok;g+=th[i].get_ok;m+=th[i].get_miss;}
    double s=(double)mx/1e9;
    printf("    → %.0f QPS (%.2f M/s) Lat:%.2fμs PUT:%lu GET:%lu MISS:%lu\n",
           (double)ops/s, (double)ops/s/1e6, (double)mx/ops/1e3, p,g,m);
    free(th); free(pt);
}

/* ---- SHM benchmark (zero-syscall) ---- */
typedef struct {
    int tid; size_t nops; int wpct; uint64_t max_key;
    shm_ring_t *shm;
    uint64_t ns, put_ok, get_ok, get_miss;
} shm_bench_t;

static void *shm_worker(void *arg) {
    shm_bench_t *t = arg;
    unsigned seed = t->tid + 42;
    shm_ring_t *shm = t->shm;

    char value[VALUE_SIZE];
    for (int i = 0; i < VALUE_SIZE; i++) value[i] = (char)(rand_r(&seed) & 0xFF);

    uint64_t start = now_ns();
    for (size_t i = 0; i < t->nops; i++) {
        uint64_t key = zipf(&seed, t->max_key);
        int is_write = (rand_r(&seed) % 100) < t->wpct;

        /* Acquire a slot */
        uint64_t tail = atomic_fetch_add_explicit(&shm->req_tail, 1, memory_order_relaxed);
        shm_slot_t *slot = &shm->slots[tail & SHM_RING_MASK];

        /* Wait for slot to be empty (spin) */
        while (__atomic_load_n(&slot->state, __ATOMIC_ACQUIRE) != SLOT_EMPTY) {
#if defined(__aarch64__)
            __asm__ volatile("yield" ::: "memory");
#endif
        }

        /* Write request — zero syscall, pure memory write */
        if (is_write) {
            slot->data[0] = OP_PUT;
            memcpy(slot->data + 1, &key, 8);
            memcpy(slot->data + 9, value, VALUE_SIZE);
            slot->len = 9 + VALUE_SIZE;
        } else {
            slot->data[0] = OP_GET;
            memcpy(slot->data + 1, &key, 8);
            slot->len = 9;
        }

        /* Mark as request ready */
        __atomic_store_n(&slot->state, SLOT_REQUEST, __ATOMIC_RELEASE);

        /* Wait for response — spin on slot state */
        while (__atomic_load_n(&slot->state, __ATOMIC_ACQUIRE) != SLOT_RESPONSE) {
#if defined(__aarch64__)
            __asm__ volatile("yield" ::: "memory");
#endif
        }

        /* Read response */
        if (is_write) {
            if (slot->len >= 1 && slot->data[0] == 0x00) t->put_ok++;
        } else {
            if (slot->len >= 1) {
                if (slot->data[0] == 0x00) t->get_ok++;
                else t->get_miss++;
            }
        }

        /* Release slot */
        __atomic_store_n(&slot->state, SLOT_EMPTY, __ATOMIC_RELEASE);
    }
    t->ns = now_ns() - start;
    return NULL;
}

static void run_shm(const char *label, shm_ring_t *shm, size_t ops, int nt,
                    int wpct, uint64_t maxk) {
    printf("\n  %s (Ops:%zu Thr:%d W%%:%d)\n", label, ops, nt, wpct);
    shm_bench_t *th = calloc(nt, sizeof(*th));
    pthread_t *pt = calloc(nt, sizeof(*pt));
    for (int i = 0; i < nt; i++) {
        th[i] = (shm_bench_t){i, ops/nt, wpct, maxk, shm, 0,0,0,0};
        pthread_create(&pt[i], NULL, shm_worker, &th[i]);
    }
    for (int i = 0; i < nt; i++) pthread_join(pt[i], NULL);
    uint64_t mx=0,p=0,g=0,m=0;
    for (int i=0;i<nt;i++){if(th[i].ns>mx)mx=th[i].ns;p+=th[i].put_ok;g+=th[i].get_ok;m+=th[i].get_miss;}
    double s=(double)mx/1e9;
    printf("    → %.0f QPS (%.2f M/s) Lat:%.3fμs PUT:%lu GET:%lu MISS:%lu\n",
           (double)ops/s, (double)ops/s/1e6, (double)mx/ops/1e3, p,g,m);
    free(th); free(pt);
}

/* ---- Fill via TCP ---- */
static void fill_tcp(int port, uint64_t count) {
    int fd = connect_tcp(port);
    if (fd < 0) { fprintf(stderr, "Cannot connect to TCP %d\n", port); return; }
    uint8_t req[9]; req[0] = OP_FILL; memcpy(req+1, &count, 8);
    write_full(fd, req, 9);
    uint8_t resp[16]; read_full(fd, resp, 9);
    printf("  Filled %lu entries\n", count);
    close(fd);
}

int main(int argc, char *argv[]) {
    size_t ops = 500000;
    int threads = 8;
    int port = 6381;

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--ops") && i+1 < argc) ops = (size_t)atol(argv[++i]);
        else if (!strcmp(argv[i], "--threads") && i+1 < argc) threads = atoi(argv[++i]);
    }

    uint64_t maxk = 1100000;

    printf("╔═══════════════════════════════════════════════════════════════════╗\n");
    printf("║  TLC Transport Benchmark: TCP vs UDS vs SHM                      ║\n");
    printf("║  Ops: %zu  Threads: %d  Value: %dB  Keys: %lu                  ║\n",
           ops, threads, VALUE_SIZE, maxk);
    printf("╚═══════════════════════════════════════════════════════════════════╝\n");

    /* Fill data via TCP */
    printf("\n  Filling via TCP...\n");
    fill_tcp(port, 1100000);

    /* Open SHM */
    int shm_fd = shm_open(SHM_PATH, O_RDWR, 0666);
    shm_ring_t *shm = NULL;
    if (shm_fd >= 0) {
        shm = mmap(NULL, SHM_TOTAL_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
        close(shm_fd);
        if (shm == MAP_FAILED) shm = NULL;
    }

    /* ============================================================ */
    printf("\n╔══════════════════════════════════════╗\n");
    printf("║  A. TCP (baseline)                   ║\n");
    printf("╚══════════════════════════════════════╝\n");
    run_sock("TCP 80R/20W (no pipeline)", port, 0, ops, threads, 20, maxk, 1);
    run_sock("TCP 80R/20W (P=16)", port, 0, ops, threads, 20, maxk, 16);
    run_sock("TCP 100%% GET (P=16)", port, 0, ops, threads, 0, maxk, 16);

    /* ============================================================ */
    printf("\n╔══════════════════════════════════════╗\n");
    printf("║  B. Unix Domain Socket               ║\n");
    printf("╚══════════════════════════════════════╝\n");
    run_sock("UDS 80R/20W (no pipeline)", port, 1, ops, threads, 20, maxk, 1);
    run_sock("UDS 80R/20W (P=16)", port, 1, ops, threads, 20, maxk, 16);
    run_sock("UDS 100%% GET (P=16)", port, 1, ops, threads, 0, maxk, 16);

    /* ============================================================ */
    if (shm) {
        printf("\n╔══════════════════════════════════════╗\n");
        printf("║  C. Shared Memory (zero-syscall)     ║\n");
        printf("╚══════════════════════════════════════╝\n");
        run_shm("SHM 80R/20W", shm, ops, threads, 20, maxk);
        run_shm("SHM 100%% GET", shm, ops, threads, 0, maxk);
    } else {
        printf("\n  SHM not available (server may not have initialized it)\n");
    }

    /* ============================================================ */
    printf("\n╔══════════════════════════════════════════════════════════════╗\n");
    printf("║  Summary                                                    ║\n");
    printf("╚══════════════════════════════════════════════════════════════╝\n");

    if (shm) munmap(shm, SHM_TOTAL_SIZE);
    printf("\n✅ Transport Benchmark complete.\n");
    return 0;
}
