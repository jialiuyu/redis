/*
 * TLC Hash Bench Client — Collision & Performance Testing
 *
 * Phases: fill → warmup → reset → measure → report
 *
 * Usage:
 *   ./tlc_hash_bench --ops 2000000 --threads 8 --max-key 1100000 [--sock /tmp/tlc_hash_bench.sock] [--skip-collision] [--csv out.csv]
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
#define OP_GET             0x01
#define OP_PUT             0x02
#define OP_FILL            0x05
#define OP_ALLOC_CHANNEL   0x20
#define OP_CLEAR_COUNTERS  0x07
#define OP_DUMP_COLLISION  0x08

static inline uint64_t now_ns(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec*1000000000ULL + ts.tv_nsec;
}
/* ---- Original (pseudo) Zipf: power-law transform ---- */
static uint64_t zipf_old(unsigned *s, uint64_t mx) {
    return (uint64_t)(pow((double)rand_r(s)/RAND_MAX,1.0/1.2)*(double)mx)%mx;
}

/* ---- Correct Zipf distribution (P(k) ∝ 1/k^s) ---- */
typedef struct {
    uint64_t n;        /* key range [0, n) */
    double   *cdf;     /* precomputed CDF: cdf[i] = P(X ≤ i+1) */
} zipf_gen_t;

static double zeta_sum(uint64_t n, double s) {
    double sum = 0.0;
    for (uint64_t i = 1; i <= n; i++)
        sum += 1.0 / pow((double)i, s);
    return sum;
}

static int zipf_init(zipf_gen_t *g, uint64_t n, double s) {
    g->n = n;
    g->cdf = (double *)malloc(n * sizeof(double));
    if (!g->cdf) return -1;
    double h = zeta_sum(n, s);
    double running = 0.0;
    for (uint64_t i = 0; i < n; i++) {
        running += (1.0 / pow((double)(i + 1), s)) / h;
        g->cdf[i] = running;
    }
    return 0;
}

static uint64_t zipf_sample(const zipf_gen_t *g, unsigned *seed) {
    double u = (double)rand_r(seed) / (double)RAND_MAX;
    uint64_t lo = 0, hi = g->n;
    while (lo < hi) {
        uint64_t mid = lo + (hi - lo) / 2;
        if (g->cdf[mid] < u) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

static void zipf_destroy(zipf_gen_t *g) {
    free(g->cdf); g->cdf = NULL;
}

static zipf_gen_t g_zipf;
static int g_use_correct_zipf = 0;
static double g_zipf_s = 1.20; /* YCSB default */
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

typedef struct {
    int tid; size_t nops; int wpct; uint64_t max_key;
    const char *uds_path; const char *shm_prefix;
    uint64_t ns, put_ok, get_ok, get_miss;
} bench_t;

typedef struct {
    uint64_t qps;
    double mops;
    uint64_t avg_ns;
    uint64_t put_ok;
    uint64_t get_ok;
    uint64_t get_miss;
} run_result_t;

static void *worker(void *arg) {
    bench_t *t = arg;
    unsigned seed = t->tid + 42;

    int uds = connect_uds(t->uds_path);
    if (uds < 0) { t->ns=0; return NULL; }
    uint8_t op = OP_ALLOC_CHANNEL; write_full(uds, &op, 1);
    int32_t ch_id; read_full(uds, &ch_id, 4);
    close(uds);
    if (ch_id < 0) { t->ns=0; return NULL; }

    aeron_ring_t *req_ring, *resp_ring;
    char name[64];
    snprintf(name, sizeof(name), "/%s_req_%d", t->shm_prefix, ch_id);
    int fd = shm_open(name, O_RDWR, 0666);
    if (fd < 0) return NULL;
    req_ring = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    snprintf(name, sizeof(name), "/%s_resp_%d", t->shm_prefix, ch_id);
    fd = shm_open(name, O_RDWR, 0666);
    if (fd < 0) return NULL;
    resp_ring = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    /* NUMA-aware: pin client to odd cores (same as v16 bench, server uses even). */
    {
        int core = 3 + (ch_id * 2);
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
        uint64_t key = g_use_correct_zipf ? zipf_sample(&g_zipf, &seed) : zipf_old(&seed, t->max_key);
        int is_w = (rand_r(&seed) % 100) < t->wpct;
        int rlen;
        if (is_w) {
            req_buf[0] = OP_PUT; memcpy(req_buf+1, &key, 8); memcpy(req_buf+9, value, VALUE_SIZE);
            rlen = 9 + VALUE_SIZE;
        } else {
            req_buf[0] = OP_GET; memcpy(req_buf+1, &key, 8);
            rlen = 9;
        }
        while (aeron_publish(req_ring, req_buf, rlen) != 0)
            __asm__ volatile("" ::: "memory");
        int got;
        while ((got = aeron_poll(resp_ring, resp_buf, sizeof(resp_buf))) <= 0)
            __asm__ volatile("" ::: "memory");
        if (is_w) { if (resp_buf[0]==0x00) t->put_ok++; }
        else { if (resp_buf[0]==0x00) t->get_ok++; else t->get_miss++; }
    }
    t->ns = now_ns() - start;
    munmap(req_ring, sizeof(aeron_ring_t));
    munmap(resp_ring, sizeof(aeron_ring_t));
    return NULL;
}

static run_result_t run(const char *label, size_t ops, int nt, int wpct, uint64_t mk,
                        const char *uds_path, const char *shm_prefix) {
    printf("  %-45s", label); fflush(stdout);
    bench_t *th = calloc(nt, sizeof(*th));
    pthread_t *pt = calloc(nt, sizeof(*pt));
    run_result_t result = {0};
    for (int i=0;i<nt;i++) {
        th[i]=(bench_t){i,ops/nt,100-wpct,mk,uds_path,shm_prefix,0,0,0,0};
        pthread_create(&pt[i],NULL,worker,&th[i]);
    }
    for (int i=0;i<nt;i++) pthread_join(pt[i],NULL);
    uint64_t mx=0,p=0,g=0,m=0;
    for (int i=0;i<nt;i++){if(th[i].ns>mx)mx=th[i].ns;p+=th[i].put_ok;g+=th[i].get_ok;m+=th[i].get_miss;}
    if (mx > 0) {
        double s=(double)mx/1e9;
        result.qps = (uint64_t)(((double)ops/s) + 0.5);
        result.mops = ((double)ops/s)/1e6;
        result.avg_ns = mx/ops;
    }
    result.put_ok = p;
    result.get_ok = g;
    result.get_miss = m;
    printf("%9lu QPS %6.2f M/s %5lu ns  P:%lu G:%lu M:%lu\n",
           (unsigned long)result.qps, result.mops, (unsigned long)result.avg_ns, p,g,m);
    free(th); free(pt);
    return result;
}

static void fill_uds(const char *path, uint64_t count) {
    int fd = connect_uds(path);
    if (fd < 0) { fprintf(stderr, "Fill: connect failed\n"); return; }
    uint8_t req[9]; req[0] = OP_FILL; memcpy(req+1, &count, 8);
    write_full(fd, req, 9);
    uint8_t resp[16]; read_full(fd, resp, 9);
    printf("  Filled %lu entries\n", (unsigned long)count);
    close(fd);
}

static void clear_counters(const char *path) {
    int fd = connect_uds(path);
    if (fd < 0) { fprintf(stderr, "Clear: connect failed\n"); return; }
    uint8_t req[1]; req[0] = OP_CLEAR_COUNTERS;
    write_full(fd, req, 1);
    uint8_t resp; read_full(fd, &resp, 1);
    close(fd);
}

static void dump_collision(const char *path) {
    int fd = connect_uds(path);
    if (fd < 0) { fprintf(stderr, "Dump: connect failed\n"); return; }
    uint8_t req[1]; req[0] = OP_DUMP_COLLISION;
    write_full(fd, req, 1);

    /* Read response: strategy_id(4) + name(32) + hot_get[5]*8 + hot_put[5]*8 + warm_get[7]*8 + total_hot*8 + total_warm*8 + hot_occ*8 + warm_occ*8 */
    uint32_t sid; read_full(fd, &sid, 4);
    char sname[33] = {0}; read_full(fd, sname, 32);
    uint64_t hot_get[5], hot_put[5], warm_get[7], total_hot, total_warm, hot_occ, warm_occ;
    for (int i = 0; i < 5; i++) read_full(fd, &hot_get[i], 8);
    for (int i = 0; i < 5; i++) read_full(fd, &hot_put[i], 8);
    for (int i = 0; i < 7; i++) read_full(fd, &warm_get[i], 8);
    read_full(fd, &total_hot, 8);
    read_full(fd, &total_warm, 8);
    read_full(fd, &hot_occ, 8);
    read_full(fd, &warm_occ, 8);
    close(fd);

    /* HOT stats */
    uint64_t hot_hits = 0;
    for (int i = 0; i < 4; i++) hot_hits += hot_get[i];
    uint64_t hot_queries = hot_hits + hot_get[4];
    printf("\n--- HOT Collision Stats (strategy: %s) ---\n", sname);
    printf("  Queries: %lu\n", (unsigned long)hot_queries);
    if (hot_queries > 0) {
        for (int i = 0; i < 4; i++)
            printf("  %d-probe: %.2f%%  (%lu)\n", i+1, 100.0*hot_get[i]/hot_queries, (unsigned long)hot_get[i]);
        printf("  Collision-miss: %.2f%%  (%lu)\n", 100.0*hot_get[4]/hot_queries, (unsigned long)hot_get[4]);
        double avg = 0;
        for (int i = 0; i < 4; i++) avg += (i+1)*(double)hot_get[i];
        avg += 4.0*(double)hot_get[4];
        printf("  Avg probes: %.2f\n", avg/hot_queries);
        printf("  Collision rate (probe>1): %.2f%%\n", 100.0*(hot_queries - hot_get[0])/hot_queries);
    }

    /* HOT PUT stats */
    uint64_t put_total = 0;
    for (int i = 0; i < 4; i++) put_total += hot_put[i];
    printf("\n--- HOT PUT Stats ---\n");
    printf("  Inserts: %lu  Evicts: %lu  (%.1f%%)\n",
        (unsigned long)put_total, (unsigned long)hot_put[4],
        (put_total+hot_put[4]) > 0 ? 100.0*hot_put[4]/(put_total+hot_put[4]) : 0);

    /* WARM stats */
    uint64_t warm_hits = 0;
    for (int i = 0; i < 6; i++) warm_hits += warm_get[i];
    uint64_t warm_queries = warm_hits + warm_get[6];
    printf("\n--- WARM Collision Stats ---\n");
    printf("  Queries: %lu\n", (unsigned long)warm_queries);
    if (warm_queries > 0) {
        for (int i = 0; i < 6; i++)
            printf("  %d-probe: %.2f%%  (%lu)\n", i+1, 100.0*warm_get[i]/warm_queries, (unsigned long)warm_get[i]);
        printf("  Collision-miss: %.2f%%  (%lu)\n", 100.0*warm_get[6]/warm_queries, (unsigned long)warm_get[6]);
        double avg = 0;
        for (int i = 0; i < 6; i++) avg += (i+1)*(double)warm_get[i];
        avg += 6.0*(double)warm_get[6];
        printf("  Avg probes: %.2f\n", avg/warm_queries);
        printf("  Collision rate (probe>1): %.2f%%\n", 100.0*(warm_queries - warm_get[0])/warm_queries);
    }

    /* Utilization */
    printf("\n--- Cache Utilization ---\n");
    printf("  HOT unique keys: %lu / %lu  (%.1f%%)\n",
        (unsigned long)hot_occ, (unsigned long)(1 << 17),
        100.0 * hot_occ / (1 << 17));
    printf("  WARM entries:    %lu / %lu  (%.1f%%)\n",
        (unsigned long)warm_occ, (unsigned long)(1 << 20),
        100.0 * warm_occ / (1 << 20));
}

int main(int argc, char *argv[]) {
    size_t ops = 2000000;
    int threads = 8;
    uint64_t max_key = 1100000;
    uint64_t fill = 0; /* 0 = auto: same as max_key */
    int rw = 80;
    int skip_collision = 0;
    const char *csv_file = NULL;
    const char *eviction = NULL;
    const char *strategy = NULL;
    const char *sock = "/tmp/tlc_hash_bench.sock";
    const char *shm_prefix = NULL;

    for (int i=1;i<argc;i++) {
        if (!strcmp(argv[i],"--ops")&&i+1<argc) ops=(size_t)atol(argv[++i]);
        else if (!strcmp(argv[i],"--threads")&&i+1<argc) threads=atoi(argv[++i]);
        else if (!strcmp(argv[i],"--max-key")&&i+1<argc) max_key=(uint64_t)atol(argv[++i]);
        else if (!strcmp(argv[i],"--fill")&&i+1<argc) fill=(uint64_t)atol(argv[++i]);
        else if (!strcmp(argv[i],"--rw")&&i+1<argc) rw=atoi(argv[++i]);
        else if (!strcmp(argv[i],"--csv")&&i+1<argc) csv_file=argv[++i];
        else if (!strcmp(argv[i],"--eviction")&&i+1<argc) eviction=argv[++i];
        else if (!strcmp(argv[i],"--strategy")&&i+1<argc) strategy=argv[++i];
        else if (!strcmp(argv[i],"--sock")&&i+1<argc) sock=argv[++i];
        else if (!strcmp(argv[i],"--skip-collision")) skip_collision=1;
        else if (!strcmp(argv[i],"--shm-prefix")&&i+1<argc) shm_prefix=argv[++i];
        else if (!strcmp(argv[i],"--zipf")) g_use_correct_zipf = 1;
        else if (!strcmp(argv[i],"--zipf-s")&&i+1<argc) { g_use_correct_zipf = 1; g_zipf_s = atof(argv[++i]); }
    }
    if (fill == 0) fill = max_key;

    if (g_use_correct_zipf) {
        if (zipf_init(&g_zipf, max_key, g_zipf_s) != 0) {
            fprintf(stderr, "zipf_init failed\n"); return 1;
        }
    }

    printf("=== TLC Hash Bench ===\n");
    printf("Ops: %zu  Threads: %d  Max-key: %lu  Fill: %lu  RW: %d/%d  Zipf: %s",
        ops, threads, (unsigned long)max_key, (unsigned long)fill, 100-rw, rw,
        g_use_correct_zipf ? "correct" : "legacy");
    if (g_use_correct_zipf) printf(" (s=%.2f)", g_zipf_s);
    printf("\n");

    const char *shm = shm_prefix ? shm_prefix : "tlc_hb";

    /* Phase 1: Pre-fill WARM */
    printf("\n--- Phase 1: Pre-fill ---\n");
    fill_uds(sock, fill);

    /* Phase 2: Warm-up */
    printf("\n--- Phase 2: Warm-up ---\n");
    run("[warmup] 80R/20W", ops, threads, rw, max_key, sock, shm);

    /* Phase 3: Reset counters */
    printf("\n--- Phase 3: Reset counters ---\n");
    if (skip_collision) {
        printf("  Skipped\n");
    } else {
        clear_counters(sock);
        printf("  Counters cleared\n");
    }

    /* Phase 4: Measurement */
    printf("\n--- Phase 4: Measurement ---\n");
    run_result_t measure_result = run("80R/20W (measure)", ops, threads, rw, max_key, sock, shm);

    /* Phase 5: Collect & report */
    printf("\n--- Phase 5: Collision Report ---\n");
    if (skip_collision) {
        printf("  Skipped\n");
    } else {
        dump_collision(sock);
    }

    /* CSV output */
    if (csv_file) {
        char sname[33] = {0};
        uint64_t hg[5] = {0}, hp[5] = {0}, wg[7] = {0}, th = 0, tw = 0, ho = 0, wo = 0;
        int have_csv_data = 1;

        if (skip_collision) {
            snprintf(sname, sizeof(sname), "%s",
                     strategy ? strategy : "SKIP_COLLISION");
        } else {
            /* Get collision data again for CSV */
            int fd = connect_uds(sock);
            if (fd >= 0) {
            uint8_t req[1]; req[0] = OP_DUMP_COLLISION;
            write_full(fd, req, 1);
            uint32_t sid; read_full(fd, &sid, 4);
                read_full(fd, sname, 32);
                for(int i=0;i<5;i++) read_full(fd,&hg[i],8);
                for(int i=0;i<5;i++) read_full(fd,&hp[i],8);
                for(int i=0;i<7;i++) read_full(fd,&wg[i],8);
                read_full(fd,&th,8); read_full(fd,&tw,8);
                read_full(fd,&ho,8); read_full(fd,&wo,8);
                close(fd);
            } else {
                fprintf(stderr, "CSV: connect failed\n");
                have_csv_data = 0;
            }
        }

        if (have_csv_data) {
            FILE *fp = fopen(csv_file, "a");
            if (fp) {
                fprintf(fp, "%s,%s,%zu,%d,%lu,%lu,%.2f,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%.1f,%.1f\n",
                    sname, eviction ? eviction : "", ops, threads, (unsigned long)max_key,
                    (unsigned long)measure_result.qps, measure_result.mops,
                    (unsigned long)measure_result.avg_ns,
                    (unsigned long)measure_result.put_ok,
                    (unsigned long)measure_result.get_ok,
                    (unsigned long)measure_result.get_miss,
                    (unsigned long)hg[0], (unsigned long)hg[1], (unsigned long)hg[2],
                    (unsigned long)hg[3], (unsigned long)hg[4],
                    (unsigned long)wg[0], (unsigned long)wg[6],
                    100.0 * ho / (1 << 17), 100.0 * wo / (1 << 20));
                fclose(fp);
                printf("  CSV appended to %s\n", csv_file);
            }
        }
    }

    if (g_use_correct_zipf) zipf_destroy(&g_zipf);

    printf("\nDone.\n");
    return 0;
}
