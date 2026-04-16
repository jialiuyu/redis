/*
 * TLC Aeron IPC Benchmark — Default Transport
 *
 * Each thread: allocate channel via UDS → GET/PUT via Aeron SPSC rings.
 * Zero syscalls in the data path.
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
#include <fcntl.h>
#include <stdint.h>

#include "../src/aeron_ipc.h"

#define VALUE_SIZE 1200
#define OP_GET  0x01
#define OP_PUT  0x02
#define OP_MGET 0x03
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

static int connect_uds(void) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un a = {.sun_family = AF_UNIX};
    strncpy(a.sun_path, "/tmp/tlc.sock", sizeof(a.sun_path)-1);
    if (connect(fd, (struct sockaddr*)&a, sizeof(a)) < 0) { close(fd); return -1; }
    return fd;
}

/* Allocate an Aeron channel via UDS control plane */
static int alloc_channel(int uds_fd) {
    uint8_t op = OP_ALLOC_CHANNEL;
    write_full(uds_fd, &op, 1);
    int32_t ch_id;
    read_full(uds_fd, &ch_id, 4);
    return ch_id;
}

/* Open Aeron channel rings */
static int open_channel(int ch_id, aeron_ring_t **req, aeron_ring_t **resp,
                        aeron_large_ring_t **large_resp) {
    char name[64];
    snprintf(name, sizeof(name), "/aeron_tlc_req_%d", ch_id);
    int fd = shm_open(name, O_RDWR, 0666);
    if (fd < 0) return -1;
    *req = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (*req == MAP_FAILED) return -1;

    snprintf(name, sizeof(name), "/aeron_tlc_resp_%d", ch_id);
    fd = shm_open(name, O_RDWR, 0666);
    if (fd < 0) return -1;
    *resp = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (*resp == MAP_FAILED) return -1;

    /* Open large response ring */
    *large_resp = NULL;
    snprintf(name, sizeof(name), "/aeron_tlc_lresp_%d", ch_id);
    fd = shm_open(name, O_RDWR, 0666);
    if (fd >= 0) {
        struct stat st;
        fstat(fd, &st);
        void *p = mmap(NULL, st.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
        close(fd);
        if (p != MAP_FAILED) *large_resp = (aeron_large_ring_t *)p;
    }

    return 0;
}

typedef struct {
    int tid; size_t nops; int wpct; uint64_t max_key;
    uint64_t ns, put_ok, get_ok, get_miss;
} bench_t;

static void *aeron_worker(void *arg) {
    bench_t *t = arg;
    unsigned seed = t->tid + 42;

    /* Allocate channel via UDS */
    int uds = connect_uds();
    if (uds < 0) { fprintf(stderr, "T%d: UDS connect failed\n", t->tid); t->ns=0; return NULL; }

    int ch_id = alloc_channel(uds);
    close(uds);
    if (ch_id < 0) { fprintf(stderr, "T%d: channel alloc failed\n", t->tid); t->ns=0; return NULL; }

    /* Open Aeron rings */
    aeron_ring_t *req_ring, *resp_ring;
    aeron_large_ring_t *large_resp;
    if (open_channel(ch_id, &req_ring, &resp_ring, &large_resp) != 0) {
        fprintf(stderr, "T%d: open channel %d failed\n", t->tid, ch_id); t->ns=0; return NULL;
    }

    char value[VALUE_SIZE];
    for (int i = 0; i < VALUE_SIZE; i++) value[i] = (char)(rand_r(&seed) & 0xFF);

    uint8_t req_buf[16 + VALUE_SIZE];
    uint8_t resp_buf[16 + VALUE_SIZE];

    uint64_t start = now_ns();
    for (size_t i = 0; i < t->nops; i++) {
        uint64_t key = zipf(&seed, t->max_key);
        int is_w = (rand_r(&seed) % 100) < t->wpct;

        /* Build request */
        int rlen;
        if (is_w) {
            req_buf[0] = OP_PUT; memcpy(req_buf+1, &key, 8); memcpy(req_buf+9, value, VALUE_SIZE);
            rlen = 9 + VALUE_SIZE;
        } else {
            req_buf[0] = OP_GET; memcpy(req_buf+1, &key, 8);
            rlen = 9;
        }

        /* Publish to request ring — ZERO SYSCALL */
        while (aeron_publish(req_ring, req_buf, rlen) != 0) {
#if defined(__aarch64__)
            __asm__ volatile("yield" ::: "memory");
#endif
        }

        /* Poll response ring — ZERO SYSCALL */
        int got;
        while ((got = aeron_poll(resp_ring, resp_buf, sizeof(resp_buf))) <= 0) {
#if defined(__aarch64__)
            __asm__ volatile("yield" ::: "memory");
#endif
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

/* UDS-based benchmark for comparison */
static void *uds_worker(void *arg) {
    bench_t *t = arg;
    unsigned seed = t->tid + 42;
    int fd = connect_uds();
    if (fd < 0) { t->ns=0; return NULL; }
    char value[VALUE_SIZE];
    for (int i=0;i<VALUE_SIZE;i++) value[i]=(char)(rand_r(&seed)&0xFF);
    uint64_t start = now_ns();
    for (size_t i = 0; i < t->nops; i++) {
        uint64_t key = zipf(&seed, t->max_key);
        int is_w = (rand_r(&seed)%100) < t->wpct;
        if (is_w) {
            uint8_t op=OP_PUT; write_full(fd,&op,1); write_full(fd,&key,8); write_full(fd,value,VALUE_SIZE);
            uint8_t st; read_full(fd,&st,1); if(st==0) t->put_ok++;
        } else {
            uint8_t op=OP_GET; write_full(fd,&op,1); write_full(fd,&key,8);
            uint8_t st; read_full(fd,&st,1);
            if(st==0){char rb[VALUE_SIZE];read_full(fd,rb,VALUE_SIZE);t->get_ok++;}
            else t->get_miss++;
        }
    }
    t->ns = now_ns() - start;
    close(fd);
    return NULL;
}

/* ---- MGET via Aeron IPC ---- */
#define OP_MGET 0x03

typedef struct {
    int tid; size_t n_batches; uint32_t batch_size; uint64_t max_key;
    uint64_t ns, total_keys, hits, misses;
} mget_bench_t;

static void *aeron_mget_worker(void *arg) {
    mget_bench_t *t = arg;
    unsigned seed = t->tid + 500;

    int uds = connect_uds();
    if (uds < 0) { t->ns = 0; return NULL; }
    int ch_id = alloc_channel(uds);
    close(uds);
    if (ch_id < 0) { t->ns = 0; return NULL; }

    aeron_ring_t *req_ring, *resp_ring;
    aeron_large_ring_t *large_resp;
    if (open_channel(ch_id, &req_ring, &resp_ring, &large_resp) != 0 || !large_resp) {
        fprintf(stderr, "T%d: open channel/large_resp failed\n", t->tid);
        t->ns = 0; return NULL;
    }

    /* Build MGET request buffer: [0x03][4B count][N × 8B key] */
    size_t req_size = 5 + t->batch_size * 8;
    uint8_t *req_buf = malloc(req_size);
    /* Large response buffer */
    size_t resp_max = 4 + t->batch_size * (1 + VALUE_SIZE) + 64;
    uint8_t *resp_buf = malloc(resp_max);

    uint64_t start = now_ns();
    for (size_t b = 0; b < t->n_batches; b++) {
        req_buf[0] = OP_MGET;
        memcpy(req_buf + 1, &t->batch_size, 4);
        uint64_t *keys = (uint64_t *)(req_buf + 5);
        for (uint32_t i = 0; i < t->batch_size; i++)
            keys[i] = zipf(&seed, t->max_key);

        /* Publish MGET request via Aeron ring — ZERO SYSCALL */
        while (aeron_publish(req_ring, req_buf, req_size) != 0) {
#if defined(__aarch64__)
            __asm__ volatile("yield" ::: "memory");
#endif
        }

        /* Poll large response ring — ZERO SYSCALL */
        int got;
        while ((got = aeron_large_poll(large_resp, resp_buf, resp_max)) <= 0) {
#if defined(__aarch64__)
            __asm__ volatile("yield" ::: "memory");
#endif
        }

        /* Parse response */
        uint32_t cnt;
        memcpy(&cnt, resp_buf, 4);
        int off = 4;
        for (uint32_t i = 0; i < cnt && off < got; i++) {
            if (resp_buf[off] == 0x00) { t->hits++; off += 1 + VALUE_SIZE; }
            else { t->misses++; off += 1; }
        }
        t->total_keys += cnt;
    }
    t->ns = now_ns() - start;

    free(req_buf); free(resp_buf);
    munmap(req_ring, sizeof(aeron_ring_t));
    munmap(resp_ring, sizeof(aeron_ring_t));
    return NULL;
}

static void run_mget(const char *label, size_t n_batches, int nt, uint32_t batch_size, uint64_t mk) {
    printf("  %-45s", label);
    fflush(stdout);
    mget_bench_t *th = calloc(nt, sizeof(*th));
    pthread_t *pt = calloc(nt, sizeof(*pt));
    for (int i = 0; i < nt; i++) {
        th[i] = (mget_bench_t){i, n_batches/nt, batch_size, mk, 0, 0, 0, 0};
        pthread_create(&pt[i], NULL, aeron_mget_worker, &th[i]);
    }
    for (int i = 0; i < nt; i++) pthread_join(pt[i], NULL);
    uint64_t mx = 0, tk = 0, h = 0, m = 0;
    for (int i = 0; i < nt; i++) {
        if (th[i].ns > mx) mx = th[i].ns;
        tk += th[i].total_keys; h += th[i].hits; m += th[i].misses;
    }
    double s = (double)mx / 1e9;
    if (mx > 0)
        printf("%9.0f k/s %6.2f M/s %6.0f ns/batch  H:%lu M:%lu\n",
               (double)tk/s, (double)tk/s/1e6, (double)mx/(n_batches > 0 ? n_batches : 1), h, m);
    else
        printf("FAILED\n");
    free(th); free(pt);
}

static void run(const char *label, size_t ops, int nt, int wpct, uint64_t mk,
                void*(*fn)(void*)) {
    printf("  %-45s", label);
    fflush(stdout);
    bench_t *th = calloc(nt, sizeof(*th));
    pthread_t *pt = calloc(nt, sizeof(*pt));
    for (int i=0;i<nt;i++) { th[i]=(bench_t){i,ops/nt,wpct,mk,0,0,0,0}; pthread_create(&pt[i],NULL,fn,&th[i]); }
    for (int i=0;i<nt;i++) pthread_join(pt[i],NULL);
    uint64_t mx=0,p=0,g=0,m=0;
    for (int i=0;i<nt;i++){if(th[i].ns>mx)mx=th[i].ns;p+=th[i].put_ok;g+=th[i].get_ok;m+=th[i].get_miss;}
    double s=(double)mx/1e9;
    printf("%9.0f QPS %6.2f M/s %6.0f ns  P:%lu G:%lu M:%lu\n",
           (double)ops/s, (double)ops/s/1e6, (double)mx/ops, p,g,m);
    free(th); free(pt);
}

static void fill_uds(void) {
    int fd = connect_uds();
    if (fd < 0) { fprintf(stderr, "Fill: UDS connect failed\n"); return; }
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
    printf("║  TLC Aeron IPC Benchmark — Default Transport                     ║\n");
    printf("║  Ops: %zu  Threads: %d  Value: %dB                             ║\n", ops, threads, VALUE_SIZE);
    printf("║  Data plane: Aeron SPSC rings (zero-syscall)                     ║\n");
    printf("║  Control: UDS /tmp/tlc.sock                                      ║\n");
    printf("╚═══════════════════════════════════════════════════════════════════╝\n\n");

    fill_uds();

    printf("\n  %-45s %9s %6s %6s\n", "Transport", "QPS", "M/s", "Lat");
    printf("  %-45s %9s %6s %6s\n", "---------------------------------------------", "---------", "------", "------");

    /* UDS baseline */
    run("UDS 80R/20W (no pipeline)", ops, threads, 20, mk, uds_worker);

    /* Aeron IPC */
    run("Aeron IPC 80R/20W (per-thread channel)", ops, threads, 20, mk, aeron_worker);
    run("Aeron IPC 100%% GET", ops, threads, 0, mk, aeron_worker);
    run("Aeron IPC 100%% PUT", ops, threads, 100, mk, aeron_worker);

    /* Scaling test */
    if (threads >= 4) {
        printf("\n  --- Scaling ---\n");
        for (int t = 1; t <= threads; t *= 2) {
            char label[64];
            snprintf(label, sizeof(label), "Aeron IPC 80R/20W (%d threads)", t);
            run(label, ops, t, 20, mk, aeron_worker);
        }
    }

    /* MGET batch via Aeron IPC */
    printf("\n  --- MGET Batch + SVE2 Gather via Aeron IPC ---\n");
    run_mget("Aeron MGET batch=100", 10000, threads, 100, mk);
    run_mget("Aeron MGET batch=500", 2000, threads, 500, mk);
    run_mget("Aeron MGET batch=1000", 1000, threads, 1000, mk);
    run_mget("Aeron MGET batch=3000", 400, threads, 3000, mk);

    printf("\n✅ Aeron IPC Benchmark complete.\n");
    return 0;
}
