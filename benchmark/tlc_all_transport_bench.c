/*
 * TLC All-Transport Benchmark
 *
 * Tests 4 transport modes against the unified server:
 *   A. TCP loopback (baseline)
 *   B. Unix Domain Socket (UDS)
 *   C. Aeron IPC (shared memory SPSC rings, zero-lock)
 *   D. io_uring + UDS (SQPOLL, zero-syscall)
 *
 * Same workload: 80R/20W, 1200B values, 1.1M keys, Zipfian.
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
#include <linux/io_uring.h>

#include "../src/aeron_ipc.h"
#include "../src/iouring_lite.h"

#define VALUE_SIZE 1200
#define OP_GET  0x01
#define OP_PUT  0x02
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

/* ---- Generic socket benchmark (TCP or UDS) ---- */
typedef struct {
    int tid; size_t nops; int wpct; uint64_t max_key;
    int use_uds; int port; int pipeline;
    uint64_t ns, ok;
} sock_bench_t;

static int connect_tcp(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    struct sockaddr_in a = {.sin_family=AF_INET, .sin_port=htons(port)};
    inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
    if (connect(fd, (struct sockaddr*)&a, sizeof(a)) < 0) { close(fd); return -1; }
    return fd;
}
static int connect_uds(void) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un a = {.sun_family = AF_UNIX};
    strncpy(a.sun_path, "/tmp/tlc.sock", sizeof(a.sun_path)-1);
    if (connect(fd, (struct sockaddr*)&a, sizeof(a)) < 0) { close(fd); return -1; }
    return fd;
}

static void *sock_worker(void *arg) {
    sock_bench_t *t = arg;
    unsigned seed = t->tid + 42;
    int fd = t->use_uds ? connect_uds() : connect_tcp(t->port);
    if (fd < 0) { t->ns = 0; return NULL; }
    char value[VALUE_SIZE];
    for (int i = 0; i < VALUE_SIZE; i++) value[i] = (char)(rand_r(&seed) & 0xFF);
    int P = t->pipeline > 0 ? t->pipeline : 1;
    int *ops = malloc(P * sizeof(int));
    uint64_t start = now_ns();
    size_t i = 0;
    while (i < t->nops) {
        int batch = 0;
        for (; batch < P && i+batch < t->nops; batch++) {
            uint64_t key = zipf(&seed, t->max_key);
            int is_w = (rand_r(&seed)%100) < t->wpct;
            ops[batch] = is_w;
            if (is_w) { uint8_t op=OP_PUT; write_full(fd,&op,1); write_full(fd,&key,8); write_full(fd,value,VALUE_SIZE); }
            else { uint8_t op=OP_GET; write_full(fd,&op,1); write_full(fd,&key,8); }
        }
        for (int b = 0; b < batch; b++) {
            uint8_t st; if (read_full(fd,&st,1)!=0) goto done;
            if (!ops[b] && st==0) { char rb[VALUE_SIZE]; read_full(fd,rb,VALUE_SIZE); }
            t->ok++;
        }
        i += batch;
    }
done:
    t->ns = now_ns() - start;
    free(ops); close(fd);
    return NULL;
}

static void run_sock(const char *label, int port, int uds, size_t ops, int nt, int wpct, uint64_t mk, int P) {
    printf("  %-40s", label);
    fflush(stdout);
    sock_bench_t *th = calloc(nt, sizeof(*th));
    pthread_t *pt = calloc(nt, sizeof(*pt));
    for (int i=0;i<nt;i++) { th[i]=(sock_bench_t){i,ops/nt,wpct,mk,uds,port,P,0,0}; pthread_create(&pt[i],NULL,sock_worker,&th[i]); }
    for (int i=0;i<nt;i++) pthread_join(pt[i],NULL);
    uint64_t mx=0; for (int i=0;i<nt;i++) if(th[i].ns>mx) mx=th[i].ns;
    double s=(double)mx/1e9;
    printf("%8.0f QPS  %5.2f M/s  %5.2f μs\n", (double)ops/s, (double)ops/s/1e6, (double)mx/ops/1e3);
    free(th); free(pt);
}

/* ---- Aeron IPC benchmark ---- */
typedef struct {
    int tid; size_t nops; int wpct; uint64_t max_key;
    aeron_ring_t *req_ring; aeron_ring_t *resp_ring;
    uint64_t ns, ok;
} aeron_bench_t;

static void *aeron_worker(void *arg) {
    aeron_bench_t *t = arg;
    unsigned seed = t->tid + 42;
    char value[VALUE_SIZE];
    for (int i = 0; i < VALUE_SIZE; i++) value[i] = (char)(rand_r(&seed) & 0xFF);
    uint8_t req[16 + VALUE_SIZE];
    uint8_t resp[16 + VALUE_SIZE];

    uint64_t start = now_ns();
    for (size_t i = 0; i < t->nops; i++) {
        uint64_t key = zipf(&seed, t->max_key);
        int is_w = (rand_r(&seed)%100) < t->wpct;

        /* Build request */
        int rlen;
        if (is_w) {
            req[0] = OP_PUT; memcpy(req+1, &key, 8); memcpy(req+9, value, VALUE_SIZE);
            rlen = 9 + VALUE_SIZE;
        } else {
            req[0] = OP_GET; memcpy(req+1, &key, 8);
            rlen = 9;
        }

        /* Publish to request ring (zero-syscall) */
        while (aeron_publish(t->req_ring, req, rlen) != 0) {
#if defined(__aarch64__)
            __asm__ volatile("yield" ::: "memory");
#endif
        }

        /* Poll response ring (zero-syscall) */
        int got;
        while ((got = aeron_poll(t->resp_ring, resp, sizeof(resp))) <= 0) {
#if defined(__aarch64__)
            __asm__ volatile("yield" ::: "memory");
#endif
        }
        t->ok++;
    }
    t->ns = now_ns() - start;
    return NULL;
}

/* ---- io_uring + UDS benchmark ---- */
typedef struct {
    int tid; size_t nops; int wpct; uint64_t max_key;
    uint64_t ns, ok;
} iouring_bench_t;

static void *iouring_worker(void *arg) {
    iouring_bench_t *t = arg;
    unsigned seed = t->tid + 42;

    /* Connect UDS */
    int fd = connect_uds();
    if (fd < 0) { t->ns = 0; return NULL; }

    /* Init io_uring — try SQPOLL first, fall back to normal */
    iouring_ctx_t ring;
    int sqpoll = (iouring_init(&ring, 256, 1) == 0);
    if (!sqpoll) {
        if (iouring_init(&ring, 256, 0) != 0) {
            /* io_uring not available — fall back to regular read/write */
            close(fd);
            t->ns = 0;
            return NULL;
        }
    }

    char value[VALUE_SIZE];
    for (int i = 0; i < VALUE_SIZE; i++) value[i] = (char)(rand_r(&seed) & 0xFF);

    uint8_t req_buf[16 + VALUE_SIZE];
    uint8_t resp_buf[16 + VALUE_SIZE];

    uint64_t start = now_ns();
    for (size_t i = 0; i < t->nops; i++) {
        uint64_t key = zipf(&seed, t->max_key);
        int is_w = (rand_r(&seed)%100) < t->wpct;

        /* Build request */
        int rlen;
        if (is_w) {
            req_buf[0] = OP_PUT; memcpy(req_buf+1, &key, 8); memcpy(req_buf+9, value, VALUE_SIZE);
            rlen = 9 + VALUE_SIZE;
        } else {
            req_buf[0] = OP_GET; memcpy(req_buf+1, &key, 8);
            rlen = 9;
        }

        /* Submit write via io_uring */
        struct io_uring_sqe *sqe = iouring_get_sqe(&ring);
        if (sqe) {
            memset(sqe, 0, sizeof(*sqe));
            sqe->opcode = IORING_OP_WRITE;
            sqe->fd = fd;
            sqe->addr = (uint64_t)(uintptr_t)req_buf;
            sqe->len = rlen;
            sqe->user_data = i;
            iouring_submit(&ring);

            /* Wait for write completion */
            struct io_uring_cqe *cqe;
            while (!(cqe = iouring_peek_cqe(&ring))) {
#if defined(__aarch64__)
                __asm__ volatile("yield" ::: "memory");
#endif
            }
            iouring_cqe_seen(&ring);
        } else {
            /* Fallback to regular write */
            write_full(fd, req_buf, rlen);
        }

        /* Read response (regular read — io_uring read on UDS is complex) */
        uint8_t status;
        if (read_full(fd, &status, 1) == 0) {
            if (!is_w && status == 0) read_full(fd, resp_buf, VALUE_SIZE);
            t->ok++;
        }
    }
    t->ns = now_ns() - start;

    iouring_destroy(&ring);
    close(fd);
    return NULL;
}

static void run_iouring(const char *label, size_t ops, int nt, int wpct, uint64_t mk) {
    printf("  %-40s", label);
    fflush(stdout);
    iouring_bench_t *th = calloc(nt, sizeof(*th));
    pthread_t *pt = calloc(nt, sizeof(*pt));
    for (int i=0;i<nt;i++) { th[i]=(iouring_bench_t){i,ops/nt,wpct,mk,0,0}; pthread_create(&pt[i],NULL,iouring_worker,&th[i]); }
    for (int i=0;i<nt;i++) pthread_join(pt[i],NULL);
    uint64_t mx=0; for (int i=0;i<nt;i++) if(th[i].ns>mx) mx=th[i].ns;
    double s=(double)mx/1e9;
    if (mx > 0)
        printf("%8.0f QPS  %5.2f M/s  %5.2f μs\n", (double)ops/s, (double)ops/s/1e6, (double)mx/ops/1e3);
    else
        printf("FAILED (io_uring not available or SQPOLL denied)\n");
    free(th); free(pt);
}

/* ---- Fill helper ---- */
static void fill_tcp(int port) {
    int fd = connect_tcp(port);
    if (fd < 0) return;
    uint8_t req[9]; req[0] = OP_FILL; uint64_t c = 1100000; memcpy(req+1,&c,8);
    write_full(fd, req, 9);
    uint8_t resp[16]; read_full(fd, resp, 9);
    printf("  Filled 1.1M entries\n");
    close(fd);
}

int main(int argc, char *argv[]) {
    size_t ops = 500000;
    int threads = 8;
    for (int i=1;i<argc;i++) {
        if (!strcmp(argv[i],"--ops")&&i+1<argc) ops=(size_t)atol(argv[++i]);
        else if (!strcmp(argv[i],"--threads")&&i+1<argc) threads=atoi(argv[++i]);
    }
    uint64_t mk = 1100000;

    printf("╔═══════════════════════════════════════════════════════════════════╗\n");
    printf("║  TLC All-Transport Benchmark                                     ║\n");
    printf("║  Ops: %zu  Threads: %d  Value: %dB                             ║\n", ops, threads, VALUE_SIZE);
    printf("╚═══════════════════════════════════════════════════════════════════╝\n\n");

    fill_tcp(6381);

    printf("\n  %-40s %8s  %7s  %6s\n", "Transport", "QPS", "M/s", "Lat");
    printf("  %-40s %8s  %7s  %6s\n", "----------------------------------------", "--------", "-------", "------");

    /* A. TCP */
    run_sock("A1. TCP 80R/20W (no pipeline)", 6381, 0, ops, threads, 20, mk, 1);
    run_sock("A2. TCP 80R/20W (P=16)", 6381, 0, ops, threads, 20, mk, 16);
    run_sock("A3. TCP 100%% GET (P=16)", 6381, 0, ops, threads, 0, mk, 16);

    /* B. UDS */
    run_sock("B1. UDS 80R/20W (no pipeline)", 6381, 1, ops, threads, 20, mk, 1);
    run_sock("B2. UDS 80R/20W (P=16)", 6381, 1, ops, threads, 20, mk, 16);
    run_sock("B3. UDS 100%% GET (P=16)", 6381, 1, ops, threads, 0, mk, 16);

    /* C. Aeron IPC */
    printf("\n  --- Aeron IPC (shared memory SPSC rings) ---\n");
    {
        /* C0: Raw ring overhead (no server, just measure ring pub+poll) */
        aeron_ring_t *local_ring = calloc(1, sizeof(aeron_ring_t));
        uint8_t msg[16]; memset(msg, 0x42, 16);
        uint8_t out[16];
        uint64_t t0 = now_ns();
        int N = 1000000;
        for (int i = 0; i < N; i++) {
            aeron_publish(local_ring, msg, 16);
            aeron_poll(local_ring, out, 16);
        }
        uint64_t t1 = now_ns();
        double lat = (double)(t1 - t0) / N;
        printf("  C0. Aeron ring overhead (16B)             %8.0f QPS  %5.2f M/s  %5.1f ns\n",
               (double)N / ((double)(t1-t0)/1e9), (double)N / ((double)(t1-t0)/1e9) / 1e6, lat);

        uint8_t big[VALUE_SIZE]; memset(big, 0x42, VALUE_SIZE);
        uint8_t bigout[VALUE_SIZE];
        t0 = now_ns();
        for (int i = 0; i < N; i++) {
            aeron_publish(local_ring, big, VALUE_SIZE);
            aeron_poll(local_ring, bigout, VALUE_SIZE);
        }
        t1 = now_ns();
        lat = (double)(t1 - t0) / N;
        printf("  C1. Aeron ring overhead (1200B)           %8.0f QPS  %5.2f M/s  %5.1f ns\n",
               (double)N / ((double)(t1-t0)/1e9), (double)N / ((double)(t1-t0)/1e9) / 1e6, lat);
        free(local_ring);

        /* C2: End-to-end via server Aeron IPC */
        int afd_req = shm_open("/aeron_tlc_req", O_RDWR, 0666);
        int afd_resp = shm_open("/aeron_tlc_resp", O_RDWR, 0666);
        if (afd_req >= 0 && afd_resp >= 0) {
            aeron_ring_t *a_req = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, afd_req, 0);
            aeron_ring_t *a_resp = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, afd_resp, 0);
            close(afd_req); close(afd_resp);

            if (a_req != MAP_FAILED && a_resp != MAP_FAILED) {
                /* Single-threaded end-to-end: publish GET, poll response */
                unsigned seed = 777;
                uint8_t areq[16], aresp[16 + VALUE_SIZE];
                int aeron_ok = 0;
                int aeron_n = (int)(ops > 200000 ? 200000 : ops);

                t0 = now_ns();
                for (int i = 0; i < aeron_n; i++) {
                    uint64_t key = zipf(&seed, mk);
                    areq[0] = OP_GET;
                    memcpy(areq + 1, &key, 8);

                    while (aeron_publish(a_req, areq, 9) != 0) {
#if defined(__aarch64__)
                        __asm__ volatile("yield" ::: "memory");
#endif
                    }

                    int got;
                    while ((got = aeron_poll(a_resp, aresp, sizeof(aresp))) <= 0) {
#if defined(__aarch64__)
                        __asm__ volatile("yield" ::: "memory");
#endif
                    }
                    aeron_ok++;
                }
                t1 = now_ns();
                double as = (double)(t1 - t0) / 1e9;
                printf("  C2. Aeron IPC e2e GET (1 thread)          %8.0f QPS  %5.2f M/s  %5.0f ns\n",
                       (double)aeron_ok / as, (double)aeron_ok / as / 1e6,
                       (double)(t1 - t0) / aeron_ok);

                munmap(a_req, sizeof(aeron_ring_t));
                munmap(a_resp, sizeof(aeron_ring_t));
            }
        } else {
            if (afd_req >= 0) close(afd_req);
            if (afd_resp >= 0) close(afd_resp);
            printf("  C2. Aeron IPC: server rings not available\n");
        }
    }

    /* D. io_uring + UDS */
    printf("\n  --- io_uring + UDS (SQPOLL zero-syscall) ---\n");
    run_iouring("D1. io_uring+UDS 80R/20W", ops, threads, 20, mk);
    run_iouring("D2. io_uring+UDS 100%% GET", ops, threads, 0, mk);

    printf("\n✅ All-Transport Benchmark complete.\n");
    return 0;
}
