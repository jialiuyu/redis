/*
 * TLC V14 — Sharded Batch Workers
 *
 * Solves both V12 and V13 bottlenecks:
 *   V12 problem: 16 threads fighting for L3 cache lines
 *   V13 problem: single batch worker bottleneck
 *
 * Solution: N_WORKERS sharded batch workers, each:
 *   - Owns a shard of the key space (key % N_WORKERS)
 *   - Has a dedicated SPSC input ring (no MPSC contention)
 *   - Pinned to a dedicated core (no L3 cross-worker bouncing)
 *   - Processes keys in micro-batches with SVE2 prefetch
 *
 * Channel threads: read from Aeron → route to shard worker → spin-wait result
 * Worker threads: drain SPSC ring → batch lookup → set done flags
 */
#define _GNU_SOURCE
#include "aeron_ipc.h"
#include "three_layer_cache_ub.h"
#include "sve2_gemm.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sched.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/un.h>
#include <sys/mman.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>

#define TCP_PORT       6381
#define UDS_PATH       "/tmp/tlc_v14.sock"
#define N_WORKERS      4       /* Sharded batch workers */
#define WORKER_RING_SZ 4096   /* Per-worker SPSC ring size */
#define WORKER_RING_MASK (WORKER_RING_SZ - 1)
#define MICRO_BATCH    32     /* Process up to 32 keys per spin cycle */

#define OP_GET   0x01
#define OP_PUT   0x02
#define OP_FILL  0x05
#define OP_PING  0x06
#define OP_STATS 0x04
#define OP_ALLOC_CHANNEL 0x20

/* ---- Task: per-request, cache-line aligned ---- */
typedef struct {
    uint64_t key;
    int32_t  warm_idx;       /* Result: -1 = miss */
    volatile int done;       /* Atomic completion flag */
} __attribute__((aligned(64))) task_t;

/* ---- Per-worker SPSC ring (channel thread → worker) ---- */
typedef struct {
    task_t *slots[WORKER_RING_SZ];
    volatile uint64_t head __attribute__((aligned(64)));
    volatile uint64_t tail __attribute__((aligned(64)));
} worker_ring_t;

static inline int wring_push(worker_ring_t *r, task_t *t) {
    uint64_t tail = r->tail;
    if (tail - __atomic_load_n(&r->head, __ATOMIC_ACQUIRE) >= WORKER_RING_SZ) return -1;
    r->slots[tail & WORKER_RING_MASK] = t;
    __atomic_store_n(&r->tail, tail + 1, __ATOMIC_RELEASE);
    return 0;
}

static inline task_t *wring_pop(worker_ring_t *r) {
    uint64_t head = r->head;
    if (head == __atomic_load_n(&r->tail, __ATOMIC_ACQUIRE)) return NULL;
    task_t *t = r->slots[head & WORKER_RING_MASK];
    __atomic_store_n(&r->head, head + 1, __ATOMIC_RELEASE);
    return t;
}

/* ---- Global state ---- */
static three_layer_cache_t g_cache;
static volatile int g_running = 1;
static worker_ring_t g_worker_rings[N_WORKERS];
static pthread_t g_worker_threads[N_WORKERS];
static atomic_uint_fast64_t g_worker_ops[N_WORKERS];
static atomic_uint_fast64_t g_worker_batches[N_WORKERS];
static atomic_uint_fast64_t g_total_ops = 0;

typedef struct {
    int id;
    aeron_ring_t *req, *resp;
    pthread_t thread;
    int active;
    atomic_uint_fast64_t ops;
} channel_t;

#define MAX_CHANNELS 64
static channel_t g_channels[MAX_CHANNELS];
static atomic_int g_num_channels = 0;

static inline uint32_t hash_pf(uint64_t k, uint32_t mask) {
    k ^= k >> 33; k *= 0xff51afd7ed558ccdULL; k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL; k ^= k >> 33; return (uint32_t)(k & mask);
}

/* Key → worker shard */
static inline int key_to_worker(uint64_t key) {
    return (int)((key * 0x9E3779B97F4A7C15ULL) >> 62);  /* Top 2 bits of golden ratio hash */
}

/* ============================================================
 * Sharded Batch Worker
 *
 * Each worker: drain its SPSC ring, process micro-batch of
 * up to MICRO_BATCH keys with SVE2 prefetch, set done flags.
 * ============================================================ */
static void *shard_worker(void *arg) {
    int wid = *(int *)arg;

    /* Pin to dedicated core on NUMA node 0 */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(wid, &cpuset);  /* Cores 0-3 for workers */
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    worker_ring_t *ring = &g_worker_rings[wid];
    task_t *batch[MICRO_BATCH];
    uint32_t hot_mask = g_cache.hot.mask;

    while (g_running) {
        /* Drain up to MICRO_BATCH tasks */
        int count = 0;
        while (count < MICRO_BATCH) {
            task_t *t = wring_pop(ring);
            if (!t) break;
            batch[count++] = t;
        }

        if (count == 0) {
            __asm__ volatile("" ::: "memory");
            continue;
        }

        /* SVE2 prefetch gather across the micro-batch */
        for (int p = 0; p < count && p < 8; p++)
            __builtin_prefetch(&g_cache.hot.table[hash_pf(batch[p]->key, hot_mask)], 0, 3);

        for (int i = 0; i < count; i++) {
            if (i + 8 < count)
                __builtin_prefetch(&g_cache.hot.table[hash_pf(batch[i+8]->key, hot_mask)], 0, 3);

            uint64_t key = batch[i]->key;
            int32_t warm_idx = -1;

            /* HOT lookup */
            uint32_t slot = hash_pf(key, hot_mask);
            for (uint32_t j = 0; j < 4; j++) {
                uint32_t s = (slot + j) & hot_mask;
                hot_index_t e = g_cache.hot.table[s];
                if (e.warm_idx >= 0 && e.key == key) { warm_idx = e.warm_idx; break; }
                if (e.warm_idx < 0) break;
            }

            /* WARM lookup if HOT miss */
            if (warm_idx < 0) {
                uint32_t wslot = hash_pf(key, g_cache.warm.mask);
                uint32_t lock_id = wslot % (uint32_t)g_cache.warm.bmp.num_words;
                bmp_lock_acquire(&g_cache.warm.bmp, lock_id);
                for (uint32_t j = 0; j < 6; j++) {
                    int32_t idx = g_cache.warm.hash_table[(wslot + j) & g_cache.warm.mask];
                    if (idx < 0) break;
                    if (g_cache.warm.entries[idx].key == key &&
                        g_cache.warm.entries[idx].state != ENTRY_EMPTY) {
                        warm_idx = idx;
                        break;
                    }
                }
                bmp_lock_release(&g_cache.warm.bmp, lock_id);
                if (warm_idx >= 0) {
                    hot_index_t nv = {key, warm_idx, 0};
                    g_cache.hot.table[slot & hot_mask] = nv;
                }
            }

            batch[i]->warm_idx = warm_idx;
            __atomic_store_n(&batch[i]->done, 1, __ATOMIC_RELEASE);
        }

        atomic_fetch_add_explicit(&g_worker_ops[wid], count, memory_order_relaxed);
        atomic_fetch_add_explicit(&g_worker_batches[wid], 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&g_total_ops, count, memory_order_relaxed);
    }
    return NULL;
}

/* ---- Channel poll thread ---- */
static void *channel_poll(void *arg) {
    channel_t *ch = (channel_t *)arg;

    /* Pin to NUMA node 0, cores 4+ (workers use 0-3) */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(N_WORKERS + ch->id, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    uint8_t req_buf[AERON_MSG_SIZE];
    uint8_t resp_buf[8];
    task_t task __attribute__((aligned(64)));

    while (g_running && ch->active) {
        int rlen = aeron_poll(ch->req, req_buf, sizeof(req_buf));
        if (rlen <= 0) { __asm__ volatile("" ::: "memory"); continue; }

        uint8_t op = req_buf[0];

        if (op == OP_GET && rlen >= 9) {
            memcpy(&task.key, req_buf + 1, 8);
            task.warm_idx = -1;
            task.done = 0;

            /* Route to shard worker */
            int wid = key_to_worker(task.key);
            while (wring_push(&g_worker_rings[wid], &task) != 0)
                __asm__ volatile("" ::: "memory");

            /* Spin-wait for worker to complete */
            while (!__atomic_load_n(&task.done, __ATOMIC_ACQUIRE))
                __asm__ volatile("" ::: "memory");

            if (task.warm_idx >= 0) {
                resp_buf[0] = 0x00;
                memcpy(resp_buf + 1, &task.warm_idx, 4);
                while (aeron_publish(ch->resp, resp_buf, 5) != 0)
                    __asm__ volatile("" ::: "memory");
            } else {
                resp_buf[0] = 0x01;
                while (aeron_publish(ch->resp, resp_buf, 1) != 0)
                    __asm__ volatile("" ::: "memory");
            }
        } else if (op == OP_PUT && rlen >= 9 + TLC_VALUE_SIZE) {
            uint64_t key; memcpy(&key, req_buf + 1, 8);
            tlc_put(&g_cache, key, req_buf + 9);
            resp_buf[0] = 0x00;
            while (aeron_publish(ch->resp, resp_buf, 1) != 0)
                __asm__ volatile("" ::: "memory");
        } else if (op == OP_PING) {
            resp_buf[0] = 0x00;
            while (aeron_publish(ch->resp, resp_buf, 1) != 0)
                __asm__ volatile("" ::: "memory");
        }
        atomic_fetch_add_explicit(&ch->ops, 1, memory_order_relaxed);
    }
    return NULL;
}

static int create_channel(void) {
    int id = atomic_fetch_add(&g_num_channels, 1);
    if (id >= MAX_CHANNELS) return -1;
    channel_t *ch = &g_channels[id];
    ch->id = id;
    char name[64];
    snprintf(name, sizeof(name), "/tlc_v14_req_%d", id);
    int fd = shm_open(name, O_CREAT|O_RDWR, 0666); ftruncate(fd, sizeof(aeron_ring_t));
    ch->req = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd); memset(ch->req, 0, sizeof(aeron_ring_t));
    snprintf(name, sizeof(name), "/tlc_v14_resp_%d", id);
    fd = shm_open(name, O_CREAT|O_RDWR, 0666); ftruncate(fd, sizeof(aeron_ring_t));
    ch->resp = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd); memset(ch->resp, 0, sizeof(aeron_ring_t));
    atomic_store(&ch->ops, 0); ch->active = 1;
    pthread_create(&ch->thread, NULL, channel_poll, ch);
    return id;
}

/* ---- UDS control plane ---- */
static int read_full(int fd, void *buf, size_t n) {
    size_t d=0; while(d<n){ssize_t r=read(fd,(char*)buf+d,n-d);if(r<=0)return -1;d+=r;} return 0;
}
static int write_full(int fd, const void *buf, size_t n) {
    size_t d=0; while(d<n){ssize_t w=write(fd,(const char*)buf+d,n-d);if(w<=0)return -1;d+=w;} return 0;
}

typedef struct { int tid; int epfd; } io_ctx_t;

static void *io_thread(void *arg) {
    io_ctx_t *ctx = arg;
    struct epoll_event events[256];
    while (g_running) {
        int n = epoll_wait(ctx->epfd, events, 256, 10);
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            uint8_t op;
            if (read(fd, &op, 1) != 1) goto cfd;
            switch (op) {
            case OP_ALLOC_CHANNEL: { int ch = create_channel(); write_full(fd, &ch, 4); break; }
            case OP_FILL: {
                uint64_t count; read_full(fd, &count, 8);
                uint8_t fbuf[TLC_VALUE_SIZE]; unsigned seed = 12345;
                for (uint64_t j = 0; j < count; j++) {
                    for (int k = 0; k < (int)(TLC_VALUE_SIZE/4); k++) ((uint32_t*)fbuf)[k] = rand_r(&seed);
                    tlc_put(&g_cache, j, fbuf);
                }
                uint8_t ok = 0; write_full(fd, &ok, 1); write_full(fd, &count, 8); break;
            }
            case OP_STATS: {
                uint64_t stats[4+N_WORKERS];
                stats[0] = atomic_load(&g_total_ops);
                stats[1] = atomic_load(&g_num_channels);
                stats[2] = atomic_load(&g_cache.warm.count);
                stats[3] = N_WORKERS;
                for (int w = 0; w < N_WORKERS; w++) stats[4+w] = atomic_load(&g_worker_ops[w]);
                uint8_t ok = 0; write_full(fd, &ok, 1); write_full(fd, stats, sizeof(stats)); break;
            }
            case OP_PING: { uint8_t ok = 0; write_full(fd, &ok, 1); break; }
            default: goto cfd;
            }
            continue;
cfd:        epoll_ctl(ctx->epfd, EPOLL_CTL_DEL, fd, NULL); close(fd);
        }
    }
    return NULL;
}

static void sig_handler(int sig) { (void)sig; g_running = 0; }

int main(int argc, char *argv[]) {
    int port = TCP_PORT;
    for (int i = 1; i < argc; i++)
        if (!strcmp(argv[i], "--port") && i+1 < argc) port = atoi(argv[++i]);

    signal(SIGINT, sig_handler); signal(SIGTERM, sig_handler); signal(SIGPIPE, SIG_IGN);

    printf("╔═══════════════════════════════════════════════════════════════╗\n");
    printf("║  TLC V14 — Sharded Batch Workers                            ║\n");
    printf("║  %d workers (cores 0-%d) + channels (cores %d+)             ║\n",
           N_WORKERS, N_WORKERS-1, N_WORKERS);
    printf("║  Key sharding: key → worker[key %% %d]                       ║\n", N_WORKERS);
    printf("║  Micro-batch: up to %d keys per spin cycle                  ║\n", MICRO_BATCH);
    printf("╚═══════════════════════════════════════════════════════════════╝\n\n");

    if (tlc_init(&g_cache, 0, 0) != 0) { fprintf(stderr, "Cache init failed\n"); return 1; }
    tlc_emb_init(&g_cache, SVE2_EMB_TABLE_SIZE, SVE2_EMB_DIM_DEFAULT);

    memset(g_worker_rings, 0, sizeof(g_worker_rings));
    memset(g_worker_ops, 0, sizeof(g_worker_ops));
    memset(g_worker_batches, 0, sizeof(g_worker_batches));

    /* Start sharded workers */
    int wids[N_WORKERS];
    for (int w = 0; w < N_WORKERS; w++) {
        wids[w] = w;
        pthread_create(&g_worker_threads[w], NULL, shard_worker, &wids[w]);
    }

    /* UDS + TCP listen */
    unlink(UDS_PATH);
    int uds_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un ua = {.sun_family = AF_UNIX};
    strncpy(ua.sun_path, UDS_PATH, sizeof(ua.sun_path)-1);
    bind(uds_fd, (struct sockaddr*)&ua, sizeof(ua));
    listen(uds_fd, 4096);
    fcntl(uds_fd, F_SETFL, fcntl(uds_fd, F_GETFL, 0) | O_NONBLOCK);

    int tcp_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1; setsockopt(tcp_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in ta = {.sin_family=AF_INET, .sin_port=htons(port), .sin_addr.s_addr=htonl(INADDR_LOOPBACK)};
    bind(tcp_fd, (struct sockaddr*)&ta, sizeof(ta));
    listen(tcp_fd, 4096);
    fcntl(tcp_fd, F_SETFL, fcntl(tcp_fd, F_GETFL, 0) | O_NONBLOCK);

    io_ctx_t ctx = {0, epoll_create1(0)};
    pthread_t io_pt;
    pthread_create(&io_pt, NULL, io_thread, &ctx);

    int aepfd = epoll_create1(0);
    struct epoll_event ev;
    ev.events = EPOLLIN; ev.data.fd = uds_fd; epoll_ctl(aepfd, EPOLL_CTL_ADD, uds_fd, &ev);
    ev.data.fd = tcp_fd; epoll_ctl(aepfd, EPOLL_CTL_ADD, tcp_fd, &ev);

    printf("TCP: %d, UDS: %s\nServer ready.\n\n", port, UDS_PATH);

    while (g_running) {
        struct epoll_event aev[64];
        int n = epoll_wait(aepfd, aev, 64, 100);
        for (int i = 0; i < n; i++) {
            int lfd = aev[i].data.fd;
            struct sockaddr_storage sa; socklen_t sl = sizeof(sa);
            int cfd;
            while ((cfd = accept(lfd, (struct sockaddr*)&sa, &sl)) >= 0) {
                if (lfd == tcp_fd) { int one=1; setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)); }
                struct epoll_event cev = {.events=EPOLLIN, .data.fd=cfd};
                epoll_ctl(ctx.epfd, EPOLL_CTL_ADD, cfd, &cev);
            }
        }
    }

    printf("\n=== V14 Stats ===\n");
    printf("  Total ops: %lu\n", atomic_load(&g_total_ops));
    for (int w = 0; w < N_WORKERS; w++)
        printf("  Worker[%d]: %lu ops, %lu batches (avg %.1f)\n", w,
               atomic_load(&g_worker_ops[w]), atomic_load(&g_worker_batches[w]),
               atomic_load(&g_worker_batches[w]) > 0
               ? (double)atomic_load(&g_worker_ops[w]) / atomic_load(&g_worker_batches[w]) : 0);
    tlc_print_stats(&g_cache);
    unlink(UDS_PATH);
    tlc_destroy(&g_cache);
    return 0;
}
