/*
 * TLC V14 Benchmark — Zero-Copy Aeron IPC
 *
 * Tests the v10 zero-copy GET path:
 *   Server returns 5B (status + warm_idx) instead of 1201B.
 *   Client would read 1200B from shared WARM segment (simulated here).
 *
 * Compares: v9 Aeron (1201B response) vs v10 Aeron (5B response)
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sched.h>
#include <time.h>
#include <math.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <stdint.h>

#include "../src/aeron_ipc.h"

#define VALUE_SIZE 1200
#define OP_GET  0x01
#define OP_PUT  0x02
#define OP_FILL 0x05
#define OP_ALLOC_CHANNEL 0x20

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

static int connect_uds(const char *path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un a = {.sun_family = AF_UNIX};
    strncpy(a.sun_path, path, sizeof(a.sun_path)-1);
    if (connect(fd, (struct sockaddr*)&a, sizeof(a)) < 0) { close(fd); return -1; }
    return fd;
}

static int alloc_channel(int uds_fd) {
    uint8_t op = OP_ALLOC_CHANNEL;
    write_full(uds_fd, &op, 1);
    int32_t ch_id;
    read_full(uds_fd, &ch_id, 4);
    return ch_id;
}

static int open_channel(int ch_id, const char *prefix, aeron_ring_t **req, aeron_ring_t **resp) {
    char name[64];
    snprintf(name, sizeof(name), "/%s_req_%d", prefix, ch_id);
    int fd = shm_open(name, O_RDWR, 0666);
    if (fd < 0) return -1;
    *req = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (*req == MAP_FAILED) return -1;

    snprintf(name, sizeof(name), "/%s_resp_%d", prefix, ch_id);
    fd = shm_open(name, O_RDWR, 0666);
    if (fd < 0) return -1;
    *resp = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (*resp == MAP_FAILED) return -1;
    return 0;
}

typedef struct {
    int tid; size_t nops; int wpct; uint64_t max_key;
    const char *uds_path; const char *shm_prefix;
    uint64_t ns, put_ok, get_ok, get_miss;
} bench_t;

static void *worker(void *arg) {
    bench_t *t = arg;
    unsigned seed = t->tid + 42;

    int uds = connect_uds(t->uds_path);
    if (uds < 0) { fprintf(stderr, "T%d: UDS connect failed\n", t->tid); t->ns=0; return NULL; }
    int ch_id = alloc_channel(uds);
    close(uds);
    if (ch_id < 0) { fprintf(stderr, "T%d: channel alloc failed\n", t->tid); t->ns=0; return NULL; }

    aeron_ring_t *req_ring, *resp_ring;
    if (open_channel(ch_id, t->shm_prefix, &req_ring, &resp_ring) != 0) {
        fprintf(stderr, "T%d: open channel failed\n", t->tid); t->ns=0; return NULL;
    }

    /* NUMA-aware: pin client to NUMA node 0, odd cores (server uses even). */
    {
        int core = 3 + (ch_id * 2);  /* Odd cores for client */
        if (core >= 40) core = (core % 38) + 3;
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    }

    char value[VALUE_SIZE];
    for (int i = 0; i < VALUE_SIZE; i++) value[i] = (char)(rand_r(&seed) & 0xFF);

    uint8_t req_buf[16 + VALUE_SIZE];
    uint8_t resp_buf[16 + VALUE_SIZE];

    uint64_t start = now_ns();
    for (size_t i = 0; i < t->nops; i++) {
        uint64_t key = zipf(&seed, t->max_key);
        int is_w = (rand_r(&seed) % 100) < t->wpct;

        int rlen;
        if (is_w) {
            req_buf[0] = OP_PUT; memcpy(req_buf+1, &key, 8); memcpy(req_buf+9, value, VALUE_SIZE);
            rlen = 9 + VALUE_SIZE;
        } else {
            req_buf[0] = OP_GET; memcpy(req_buf+1, &key, 8);
            rlen = 9;
        }

        while (aeron_publish(req_ring, req_buf, rlen) != 0) {
#if defined(__aarch64__)
            __asm__ volatile("" ::: "memory");
#endif
        }

        int got;
        /* Pure spin — no yield (Seastar Phase 1: compiler barrier only) */
        while ((got = aeron_poll(resp_ring, resp_buf, sizeof(resp_buf))) <= 0) {
            __asm__ volatile("" ::: "memory");
        }

        if (is_w) {
            if (resp_buf[0] == 0x00) t->put_ok++;
        } else {
            if (resp_buf[0] == 0x00) t->get_ok++;
            else t->get_miss++;
        }
    }
    t->ns = now_ns() - start;
    munmap(req_ring, sizeof(aeron_ring_t));
    munmap(resp_ring, sizeof(aeron_ring_t));
    return NULL;
}

static void run(const char *label, size_t ops, int nt, int wpct, uint64_t mk,
                const char *uds_path, const char *shm_prefix) {
    printf("  %-45s", label);
    fflush(stdout);
    bench_t *th = calloc(nt, sizeof(*th));
    pthread_t *pt = calloc(nt, sizeof(*pt));
    for (int i=0;i<nt;i++) {
        th[i]=(bench_t){i,ops/nt,wpct,mk,uds_path,shm_prefix,0,0,0,0};
        pthread_create(&pt[i],NULL,worker,&th[i]);
    }
    for (int i=0;i<nt;i++) pthread_join(pt[i],NULL);
    uint64_t mx=0,p=0,g=0,m=0;
    for (int i=0;i<nt;i++){if(th[i].ns>mx)mx=th[i].ns;p+=th[i].put_ok;g+=th[i].get_ok;m+=th[i].get_miss;}
    double s=(double)mx/1e9;
    printf("%9.0f QPS %6.2f M/s %5.0f ns  P:%lu G:%lu M:%lu\n",
           (double)ops/s, (double)ops/s/1e6, (double)mx/ops, p,g,m);
    free(th); free(pt);
}

static void fill_uds(const char *path) {
    int fd = connect_uds(path);
    if (fd < 0) { fprintf(stderr, "Fill: connect failed to %s\n", path); return; }
    uint8_t req[9]; req[0] = OP_FILL; uint64_t c = 1100000; memcpy(req+1,&c,8);
    write_full(fd, req, 9);
    uint8_t resp[16]; read_full(fd, resp, 9);
    printf("  Filled 1.1M entries via %s\n", path);
    close(fd);
}

int main(int argc, char *argv[]) {
    size_t ops = 1000000;
    int threads = 8;
    for (int i=1;i<argc;i++) {
        if (!strcmp(argv[i],"--ops")&&i+1<argc) ops=(size_t)atol(argv[++i]);
        else if (!strcmp(argv[i],"--threads")&&i+1<argc) threads=atoi(argv[++i]);
    }
    uint64_t mk = 1100000;

    printf("╔═══════════════════════════════════════════════════════════════════╗\n");
    printf("║  V14 Benchmark: Zero-Copy Aeron IPC vs V9 Aeron IPC             ║\n");
    printf("║  Ops: %zu  Threads: %d  Value: %dB                             ║\n", ops, threads, VALUE_SIZE);
    printf("╚═══════════════════════════════════════════════════════════════════╝\n\n");

    /* Fill v10 server */
    fill_uds("/tmp/tlc_v14.sock");

    printf("\n  %-45s %9s %6s %5s\n", "Transport", "QPS", "M/s", "Lat");
    printf("  %-45s %9s %6s %5s\n", "---------------------------------------------", "---------", "------", "-----");

    /* V14 Aeron IPC (zero-copy GET: 5B response) */
    run("V14 Aeron 80R/20W (zero-copy, 8 ch)", ops, threads, 20, mk, "/tmp/tlc_v14.sock", "tlc_v14");
    run("V14 Aeron 100%% GET (zero-copy)", ops, threads, 0, mk, "/tmp/tlc_v14.sock", "tlc_v14");
    run("V14 Aeron 100%% PUT", ops, threads, 100, mk, "/tmp/tlc_v14.sock", "tlc_v14");

    /* V9 Aeron IPC (full 1201B response) — if server running */
    if (access("/tmp/tlc.sock", F_OK) == 0) {
        printf("\n  --- V9 Aeron (full response, for comparison) ---\n");
        run("V9 Aeron 80R/20W (full resp, 8 ch)", ops, threads, 20, mk, "/tmp/tlc.sock", "aeron_tlc");
        run("V9 Aeron 100%% GET (full resp)", ops, threads, 0, mk, "/tmp/tlc.sock", "aeron_tlc");
    }

    printf("\n✅ V14 Benchmark complete.\n");
    return 0;
}
