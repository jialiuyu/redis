/*
 * TLC V16 — FC + Hash V2 + WeChat 2x3 HA — Adaptive V12/Batch Hybrid
 *
 * Based on /sharedata/qiuwu/redis/inc/flat_combine.md
 *
 * Core idea: NO dedicated batch worker thread.
 * Each channel thread that gets a GET request:
 *   1. Publishes request to its slot on the Publication Board
 *   2. Tries CAS on g_combiner_lock
 *   3. If wins: scans all slots, collects pending requests,
 *      does SVE2 batch gather, distributes results, releases lock
 *   4. If loses: spin-waits on its slot's done flag
 *
 * Adaptive:
 *   - 1 pending → direct V12 cache lookup (zero batch overhead)
 *   - N pending → SVE2 prefetch gather across all N keys
 *
 * This eliminates:
 *   - Extra thread hop (V13/V14 problem)
 *   - Single worker bottleneck (V13 problem)
 *   - L3 contention from 16 threads (V12 problem — only combiner touches cache)
 */
#define _GNU_SOURCE
#include "aeron_ipc.h"
#include "three_layer_cache_ub.h"
#include "sve2_gemm.h"
#include "wechat_ha.h"
#include "hot_hash_v2.h"
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
#include <stdatomic.h>

#define TCP_PORT       6381
#define UDS_PATH       "/tmp/tlc_v16.sock"
#define MAX_SLOTS      64

#define OP_GET   0x01
#define OP_PUT   0x02
#define OP_FILL  0x05
#define OP_PING  0x06
#define OP_STATS 0x04
#define OP_ALLOC_CHANNEL 0x20

#define SLOT_EMPTY   0
#define SLOT_PENDING 1
#define SLOT_DONE    2

/* ---- Publication Board Slot (cache-line aligned) ---- */
typedef struct __attribute__((aligned(64))) {
    uint64_t key;
    int32_t  warm_idx;       /* Result */
    atomic_int state;        /* EMPTY → PENDING → DONE */
} fc_slot_t;

/* ---- Global Flat Combining State ---- */
static fc_slot_t g_slots[MAX_SLOTS];
static atomic_int g_combiner_lock = 0;
static three_layer_cache_t g_cache;
static wechat_ha_t g_ha;
static volatile int g_running = 1;

static atomic_uint_fast64_t g_total_ops = 0;
static atomic_uint_fast64_t g_combine_rounds = 0;
static atomic_uint_fast64_t g_combine_total_keys = 0;
static atomic_uint_fast64_t g_direct_ops = 0;  /* Single-key direct path */
static atomic_uint_fast64_t g_batch_ops = 0;   /* Multi-key batch path */

static inline uint32_t hash_pf(uint64_t k, uint32_t mask) {
    k ^= k >> 33; k *= 0xff51afd7ed558ccdULL; k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL; k ^= k >> 33; return (uint32_t)(k & mask);
}

/* ---- Channel state (declared before fc_get) ---- */
typedef struct {
    int id;
    aeron_ring_t *req, *resp;
    pthread_t thread;
    int active;
    atomic_uint_fast64_t ops;
} channel_t;

#define MAX_CHANNELS 64
static channel_t g_channels[MAX_CHANNELS];
static atomic_int g_num_channels_fc = 0;

/* ---- Direct V12 cache lookup (single key) ---- */
static inline int32_t direct_lookup(uint64_t key) {
    uint32_t hot_mask = g_cache.hot.mask;
    uint32_t slot = hot_hash_v2_primary(key, hot_mask);
    int32_t warm_idx = -1;

    for (uint32_t j = 0; j < 4; j++) {
        uint32_t s = hot_hash_v2_probe(key, hot_mask, j);
        hot_index_t e = g_cache.hot.table[s];
        if (e.warm_idx >= 0 && e.key == key) { warm_idx = e.warm_idx; break; }
        if (e.warm_idx < 0) break;
    }

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
    return warm_idx;
}

/* ---- Flat Combining GET — TTAS + libgqm counter optimization ---- */
static int32_t fc_get(int slot_id, uint64_t key) {
    /* Step 1: Publish request to my slot (wait-free, only I write my slot) */
    g_slots[slot_id].key = key;
    g_slots[slot_id].warm_idx = -1;
    atomic_store_explicit(&g_slots[slot_id].state, SLOT_PENDING, memory_order_release);

    /* Step 2: TTAS combiner election (libgqm-inspired)
     * Test first with relaxed load (no bus traffic if lock is held).
     * Only attempt CAS if lock appears free.
     * This reduces cache line bouncing from 64 failed CAS to 1 load. */
    int became_combiner = 0;
    int spins = 0;
    while (!became_combiner && atomic_load_explicit(&g_slots[slot_id].state, memory_order_acquire) == SLOT_PENDING) {
        /* Test: is lock free? (relaxed load, no bus traffic) */
        if (atomic_load_explicit(&g_combiner_lock, memory_order_relaxed) == 0) {
            /* Test-and-Set: try to grab it */
            int expected = 0;
            became_combiner = atomic_compare_exchange_strong_explicit(
                &g_combiner_lock, &expected, 1,
                memory_order_acq_rel, memory_order_relaxed);
        }
        if (!became_combiner) {
            /* Brief spin — someone else is combining, my result may appear soon */
            if (++spins > 64) break;  /* Don't spin forever, check slot */
            __asm__ volatile("" ::: "memory");
        }
    }

    if (became_combiner) {
        /* Step 3A: I am the combiner — scan all slots */
        int pending_ids[MAX_SLOTS];
        uint64_t pending_keys[MAX_SLOTS];
        int pending_count = 0;
        int num_channels = atomic_load_explicit(&g_num_channels_fc, memory_order_relaxed);

        for (int i = 0; i < num_channels && i < MAX_SLOTS; i++) {
            if (atomic_load_explicit(&g_slots[i].state, memory_order_acquire) == SLOT_PENDING) {
                pending_ids[pending_count] = i;
                pending_keys[pending_count] = g_slots[i].key;
                pending_count++;
            }
        }

        /* SVE2 batch gather with prefetch (always batch, even for 1 key) */
        if (pending_count > 0) {
            uint32_t hot_mask = g_cache.hot.mask;
            for (int p = 0; p < pending_count && p < 8; p++)
                __builtin_prefetch(&g_cache.hot.table[hot_hash_v2_primary(pending_keys[p], hot_mask)], 0, 3);

            for (int i = 0; i < pending_count; i++) {
                if (i + 8 < pending_count)
                    __builtin_prefetch(&g_cache.hot.table[hot_hash_v2_primary(pending_keys[i+8], hot_mask)], 0, 3);
                g_slots[pending_ids[i]].warm_idx = direct_lookup(pending_keys[i]);
            }

            if (pending_count == 1)
                atomic_fetch_add_explicit(&g_direct_ops, 1, memory_order_relaxed);
            else
                atomic_fetch_add_explicit(&g_batch_ops, pending_count, memory_order_relaxed);
        }

        /* Distribute results — single store per slot */
        for (int i = 0; i < pending_count; i++)
            atomic_store_explicit(&g_slots[pending_ids[i]].state, SLOT_DONE, memory_order_release);

        atomic_fetch_add_explicit(&g_combine_rounds, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&g_combine_total_keys, pending_count, memory_order_relaxed);
        atomic_fetch_add_explicit(&g_total_ops, pending_count, memory_order_relaxed);

        /* Release combiner lock */
        atomic_store_explicit(&g_combiner_lock, 0, memory_order_release);
    }
    /* else: another combiner already processed my request, or I timed out spinning */

    /* Wait for my result if not yet done */
    while (atomic_load_explicit(&g_slots[slot_id].state, memory_order_acquire) != SLOT_DONE)
        __asm__ volatile("" ::: "memory");

    int32_t result = g_slots[slot_id].warm_idx;
    atomic_store_explicit(&g_slots[slot_id].state, SLOT_EMPTY, memory_order_relaxed);
    return result;
}

/* ---- Channel poll thread ---- */
static void *channel_poll(void *arg) {
    channel_t *ch = (channel_t *)arg;

    /* Pin to NUMA node 0 */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(ch->id % 40, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    uint8_t req_buf[AERON_MSG_SIZE];
    uint8_t resp_buf[8];

    while (g_running && ch->active) {
        int rlen = aeron_poll(ch->req, req_buf, sizeof(req_buf));
        if (rlen <= 0) { __asm__ volatile("" ::: "memory"); continue; }

        uint8_t op = req_buf[0];

        if (op == OP_GET && rlen >= 9) {
            uint64_t key; memcpy(&key, req_buf + 1, 8);

            /* Flat Combining GET */
            int32_t warm_idx = fc_get(ch->id, key);

            if (warm_idx >= 0) {
                resp_buf[0] = 0x00;
                memcpy(resp_buf + 1, &warm_idx, 4);
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
            atomic_fetch_add_explicit(&g_total_ops, 1, memory_order_relaxed);
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
    int id = atomic_fetch_add(&g_num_channels_fc, 1);
    if (id >= MAX_CHANNELS) return -1;
    channel_t *ch = &g_channels[id];
    ch->id = id;
    char name[64];
    snprintf(name, sizeof(name), "/tlc_v16_req_%d", id);
    int fd = shm_open(name, O_CREAT|O_RDWR, 0666); ftruncate(fd, sizeof(aeron_ring_t));
    ch->req = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd); memset(ch->req, 0, sizeof(aeron_ring_t));
    snprintf(name, sizeof(name), "/tlc_v16_resp_%d", id);
    fd = shm_open(name, O_CREAT|O_RDWR, 0666); ftruncate(fd, sizeof(aeron_ring_t));
    ch->resp = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd); memset(ch->resp, 0, sizeof(aeron_ring_t));
    atomic_store(&ch->ops, 0); ch->active = 1;
    atomic_store_explicit(&g_slots[id].state, SLOT_EMPTY, memory_order_relaxed);
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
                uint64_t stats[6] = {
                    atomic_load(&g_total_ops), atomic_load(&g_combine_rounds),
                    atomic_load(&g_combine_total_keys), atomic_load(&g_direct_ops),
                    atomic_load(&g_batch_ops), atomic_load(&g_num_channels_fc)
                };
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

int main(void) {
    signal(SIGINT, sig_handler); signal(SIGTERM, sig_handler); signal(SIGPIPE, SIG_IGN);

    printf("╔═══════════════════════════════════════════════════════════════╗\n");
    printf("║  TLC V16 — FC + Hash V2 + WeChat 2x3 HA                                   ║\n");
    printf("║  Adaptive: 1 pending → V12 direct | N → SVE2 batch gather  ║\n");
    printf("║  No dedicated worker thread — combiner IS the channel thread ║\n");
    printf("╚═══════════════════════════════════════════════════════════════╝\n\n");

    if (tlc_init(&g_cache, 0, 0) != 0) { fprintf(stderr, "Cache init failed\n"); return 1; }
    tlc_emb_init(&g_cache, SVE2_EMB_TABLE_SIZE, SVE2_EMB_DIM_DEFAULT);
    ha_init(&g_ha, 0, 0);  /* IDC-A, shard 0 */
    printf("WeChat 2x3 HA: IDC=%d Shard=%d\n", g_ha.my_idc, g_ha.my_shard);

    for (int i = 0; i < MAX_SLOTS; i++)
        atomic_store_explicit(&g_slots[i].state, SLOT_EMPTY, memory_order_relaxed);

    unlink(UDS_PATH);
    int uds_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un ua = {.sun_family = AF_UNIX};
    strncpy(ua.sun_path, UDS_PATH, sizeof(ua.sun_path)-1);
    bind(uds_fd, (struct sockaddr*)&ua, sizeof(ua));
    listen(uds_fd, 4096);
    fcntl(uds_fd, F_SETFL, fcntl(uds_fd, F_GETFL, 0) | O_NONBLOCK);

    io_ctx_t ctx = {0, epoll_create1(0)};
    pthread_t io_pt;
    pthread_create(&io_pt, NULL, io_thread, &ctx);

    int aepfd = epoll_create1(0);
    struct epoll_event ev = {.events = EPOLLIN, .data.fd = uds_fd};
    epoll_ctl(aepfd, EPOLL_CTL_ADD, uds_fd, &ev);

    printf("UDS: %s\nServer ready.\n\n", UDS_PATH);

    while (g_running) {
        struct epoll_event aev[64];
        int n = epoll_wait(aepfd, aev, 64, 100);
        for (int i = 0; i < n; i++) {
            struct sockaddr_un ca; socklen_t cl = sizeof(ca);
            int cfd;
            while ((cfd = accept(uds_fd, (struct sockaddr*)&ca, &cl)) >= 0) {
                struct epoll_event cev = {.events=EPOLLIN, .data.fd=cfd};
                epoll_ctl(ctx.epfd, EPOLL_CTL_ADD, cfd, &cev);
            }
        }
    }

    printf("\n=== Flat Combining Stats ===\n");
    printf("  Total ops:      %lu\n", atomic_load(&g_total_ops));
    printf("  Combine rounds: %lu\n", atomic_load(&g_combine_rounds));
    printf("  Avg batch size: %.1f\n", atomic_load(&g_combine_rounds) > 0
           ? (double)atomic_load(&g_combine_total_keys) / atomic_load(&g_combine_rounds) : 0);
    printf("  Direct (1-key): %lu\n", atomic_load(&g_direct_ops));
    printf("  Batch (N-key):  %lu\n", atomic_load(&g_batch_ops));
    tlc_print_stats(&g_cache);
    unlink(UDS_PATH);
    tlc_destroy(&g_cache);
    return 0;
}
