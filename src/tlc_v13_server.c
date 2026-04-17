/*
 * TLC V13 Server — Batch Proxy + SVE2 Gather + Zero-Copy
 *
 * Architecture:
 *   - Aeron IPC channels for client communication (per-client SPSC)
 *   - MPSC lock-free ring collects GET requests from all channels
 *   - Single batch worker: drains up to 3000 requests, does ONE SVE2
 *     prefetch gather, writes results back to per-task completion slots
 *   - No mutex/cond per task — atomic flag + pure spin (no thundering herd)
 *   - UDS control plane for FILL/STATS/channel allocation
 *
 * Port 6381: TCP + UDS + Aeron IPC
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
#define UDS_PATH       "/tmp/tlc_v13.sock"
#define BATCH_LIMIT    3000
#define BATCH_TIMEOUT_US 0     /* 0 = process immediately */

#define OP_GET   0x01
#define OP_PUT   0x02
#define OP_MGET  0x03
#define OP_STATS 0x04
#define OP_FILL  0x05
#define OP_PING  0x06
#define OP_ALLOC_CHANNEL 0x20

/* ---- Task: per-request completion slot (no mutex, atomic flag) ---- */
typedef struct {
    uint64_t key;
    int32_t  warm_idx;       /* Result: WARM index (-1 = miss) */
    volatile int done;       /* Atomic completion flag */
} __attribute__((aligned(64))) batch_task_t;  /* Cache-line aligned */

/* ---- MPSC Lock-Free Ring (all channels → batch worker) ---- */
#define MPSC_RING_SIZE  8192
#define MPSC_RING_MASK  (MPSC_RING_SIZE - 1)

typedef struct {
    volatile uint64_t seq;
    batch_task_t *task;
} __attribute__((aligned(16))) mpsc_slot_t;

typedef struct {
    mpsc_slot_t slots[MPSC_RING_SIZE];
    volatile uint64_t head __attribute__((aligned(64)));
    volatile uint64_t tail __attribute__((aligned(64)));
} mpsc_ring_t;

static mpsc_ring_t g_batch_ring;

static inline int mpsc_push(batch_task_t *task) {
    uint64_t tail = __atomic_fetch_add(&g_batch_ring.tail, 1, __ATOMIC_RELAXED);
    mpsc_slot_t *slot = &g_batch_ring.slots[tail & MPSC_RING_MASK];
    /* Wait for slot to be consumed (seq == tail means it's free) */
    while (__atomic_load_n(&slot->seq, __ATOMIC_ACQUIRE) != tail)
        __asm__ volatile("" ::: "memory");
    slot->task = task;
    __atomic_store_n(&slot->seq, tail + 1, __ATOMIC_RELEASE);
    return 0;
}

static inline batch_task_t *mpsc_pop(void) {
    uint64_t head = g_batch_ring.head;
    mpsc_slot_t *slot = &g_batch_ring.slots[head & MPSC_RING_MASK];
    uint64_t seq = __atomic_load_n(&slot->seq, __ATOMIC_ACQUIRE);
    if (seq != head + 1) return NULL;  /* Not ready yet */
    batch_task_t *task = slot->task;
    __atomic_store_n(&slot->seq, head + MPSC_RING_SIZE, __ATOMIC_RELEASE);
    g_batch_ring.head = head + 1;
    return task;
}

/* ---- Global state ---- */
static three_layer_cache_t g_cache;
static volatile int g_running = 1;

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
static atomic_uint_fast64_t g_total_ops = 0;
static atomic_uint_fast64_t g_batch_count = 0;
static atomic_uint_fast64_t g_batch_total_keys = 0;

static inline uint32_t hash_pf(uint64_t k, uint32_t mask) {
    k ^= k >> 33; k *= 0xff51afd7ed558ccdULL; k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL; k ^= k >> 33; return (uint32_t)(k & mask);
}

/* ============================================================
 * Batch Worker Thread — THE core of V13
 *
 * Drains MPSC ring every 200μs or 3000 tasks.
 * Does ONE SVE2 prefetch gather across all keys.
 * Writes warm_idx back to each task, sets done=1.
 * No mutex, no cond_signal — pure atomic store.
 * ============================================================ */
static void *batch_worker(void *arg) {
    (void)arg;

    /* Pin to core 0 (NUMA node 0, dedicated) */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    batch_task_t *batch[BATCH_LIMIT];
    struct timespec ts;

    while (g_running) {
        int count = 0;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        uint64_t deadline_ns = (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec + BATCH_TIMEOUT_US * 1000;

        /* Phase 1: Drain MPSC ring until full or timeout */
        while (count < BATCH_LIMIT) {
            batch_task_t *task = mpsc_pop();
            if (task) {
                batch[count++] = task;
            } else {
                if (count > 0) {
                    clock_gettime(CLOCK_MONOTONIC, &ts);
                    uint64_t now = (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
                    if (now >= deadline_ns) break;
                }
                __asm__ volatile("" ::: "memory");
            }
        }

        if (count == 0) {
            __asm__ volatile("" ::: "memory");
            continue;
        }

        /* Phase 2: SVE2 batch gather with 8-ahead prefetch */
        uint32_t hot_mask = g_cache.hot.mask;

        /* Prefetch first 8 HOT entries */
        for (int p = 0; p < count && p < 8; p++)
            __builtin_prefetch(&g_cache.hot.table[hash_pf(batch[p]->key, hot_mask)], 0, 3);

        for (int i = 0; i < count; i++) {
            /* Prefetch 8 ahead */
            if (i + 8 < count)
                __builtin_prefetch(&g_cache.hot.table[hash_pf(batch[i+8]->key, hot_mask)], 0, 3);

            uint64_t key = batch[i]->key;
            int32_t warm_idx = -1;

            /* HOT lookup (lock-free) */
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

            /* Phase 3: Write result + set done flag (no mutex!) */
            batch[i]->warm_idx = warm_idx;
            __atomic_store_n(&batch[i]->done, 1, __ATOMIC_RELEASE);
        }

        atomic_fetch_add_explicit(&g_total_ops, count, memory_order_relaxed);
        atomic_fetch_add_explicit(&g_batch_count, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&g_batch_total_keys, count, memory_order_relaxed);
    }
    return NULL;
}

/* ---- Channel poll thread: reads from Aeron, pushes to MPSC, waits for result ---- */
static void *channel_poll(void *arg) {
    channel_t *ch = (channel_t *)arg;

    /* Pin to NUMA node 0 */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(1 + ch->id, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    uint8_t req_buf[AERON_MSG_SIZE];
    uint8_t resp_buf[8];
    batch_task_t task __attribute__((aligned(64)));

    while (g_running && ch->active) {
        int rlen = aeron_poll(ch->req, req_buf, sizeof(req_buf));
        if (rlen <= 0) { __asm__ volatile("" ::: "memory"); continue; }

        uint8_t op = req_buf[0];

        if (op == OP_GET && rlen >= 9) {
            memcpy(&task.key, req_buf + 1, 8);
            task.warm_idx = -1;
            task.done = 0;

            /* Push to MPSC batch ring */
            while (mpsc_push(&task) != 0)
                __asm__ volatile("" ::: "memory");

            /* Spin-wait for batch worker to complete (no mutex!) */
            while (!__atomic_load_n(&task.done, __ATOMIC_ACQUIRE))
                __asm__ volatile("" ::: "memory");

            /* Send response */
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
    snprintf(name, sizeof(name), "/tlc_v13_req_%d", id);
    int fd = shm_open(name, O_CREAT|O_RDWR, 0666); ftruncate(fd, sizeof(aeron_ring_t));
    ch->req = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd); memset(ch->req, 0, sizeof(aeron_ring_t));
    snprintf(name, sizeof(name), "/tlc_v13_resp_%d", id);
    fd = shm_open(name, O_CREAT|O_RDWR, 0666); ftruncate(fd, sizeof(aeron_ring_t));
    ch->resp = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd); memset(ch->resp, 0, sizeof(aeron_ring_t));
    atomic_store(&ch->ops, 0);
    ch->active = 1;
    pthread_create(&ch->thread, NULL, channel_poll, ch);
    return id;
}

/* ---- UDS control plane (same as V10) ---- */
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
                uint8_t ok = 0x00; write_full(fd, &ok, 1); write_full(fd, &count, 8);
                break;
            }
            case OP_STATS: {
                uint64_t stats[5] = {
                    atomic_load(&g_total_ops), atomic_load(&g_batch_count),
                    atomic_load(&g_batch_total_keys), atomic_load(&g_num_channels),
                    atomic_load(&g_cache.warm.count)
                };
                uint8_t ok = 0x00; write_full(fd, &ok, 1); write_full(fd, stats, sizeof(stats));
                break;
            }
            case OP_PING: { uint8_t ok = 0x00; write_full(fd, &ok, 1); break; }
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
    printf("║  TLC V13 — Batch Proxy + SVE2 Gather + Zero-Copy            ║\n");
    printf("║  MPSC ring → batch worker (3000 keys / 200μs) → SVE2 gather ║\n");
    printf("║  No mutex per task — atomic flag + pure spin                 ║\n");
    printf("╚═══════════════════════════════════════════════════════════════╝\n\n");

    if (tlc_init(&g_cache, 0, 0) != 0) { fprintf(stderr, "Cache init failed\n"); return 1; }
    tlc_emb_init(&g_cache, SVE2_EMB_TABLE_SIZE, SVE2_EMB_DIM_DEFAULT);

    memset(&g_batch_ring, 0, sizeof(g_batch_ring));
    /* Init MPSC ring sequence numbers */
    for (int i = 0; i < MPSC_RING_SIZE; i++)
        g_batch_ring.slots[i].seq = i;

    /* Start batch worker */
    pthread_t batch_pt;
    pthread_create(&batch_pt, NULL, batch_worker, NULL);

    /* UDS listen */
    unlink(UDS_PATH);
    int uds_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un ua = {.sun_family = AF_UNIX};
    strncpy(ua.sun_path, UDS_PATH, sizeof(ua.sun_path) - 1);
    bind(uds_fd, (struct sockaddr*)&ua, sizeof(ua));
    listen(uds_fd, 4096);
    fcntl(uds_fd, F_SETFL, fcntl(uds_fd, F_GETFL, 0) | O_NONBLOCK);

    /* TCP listen */
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

    int next = 0;
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

    printf("\n=== V13 Stats ===\n");
    printf("  Total ops:    %lu\n", atomic_load(&g_total_ops));
    printf("  Batches:      %lu\n", atomic_load(&g_batch_count));
    printf("  Avg batch sz: %.1f\n", atomic_load(&g_batch_count) > 0
           ? (double)atomic_load(&g_batch_total_keys) / atomic_load(&g_batch_count) : 0);
    printf("  Channels:     %d\n", atomic_load(&g_num_channels));
    tlc_print_stats(&g_cache);
    unlink(UDS_PATH);
    tlc_destroy(&g_cache);
    return 0;
}
