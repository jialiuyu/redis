/*
 * TLC Aeron IPC Server — Default Transport
 *
 * Multi-channel Aeron IPC: each client gets a dedicated SPSC ring pair.
 * Server spawns one poll thread per channel — zero contention.
 * UDS kept for control plane (FILL, STATS, MGET batch).
 *
 * Data plane (Aeron): GET/PUT via shared memory rings, zero syscall.
 * Control plane (UDS): FILL, STATS, MGET batch via Unix socket.
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
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/un.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <errno.h>

#define UDS_PATH       "/tmp/tlc.sock"
#define MAX_CHANNELS   AERON_MAX_CHANNELS
#define BATCH_LIMIT    3000

#define OP_GET   0x01
#define OP_PUT   0x02
#define OP_MGET  0x03
#define OP_STATS 0x04
#define OP_FILL  0x05
#define OP_PING  0x06

static three_layer_cache_t g_cache;
static volatile int g_running = 1;

/* Per-channel state */
typedef struct {
    int           id;
    aeron_ring_t *req;
    aeron_ring_t *resp;
    aeron_large_ring_t *large_resp;  /* For MGET large responses */
    pthread_t     thread;
    int           active;
    atomic_uint_fast64_t ops;
    atomic_uint_fast64_t mget_ops;
} channel_t;

static channel_t g_channels[MAX_CHANNELS];
static atomic_int g_num_channels = 0;
static atomic_uint_fast64_t g_total_aeron_ops = 0;
static atomic_uint_fast64_t g_total_uds_ops = 0;
static atomic_uint_fast64_t g_mget_keys = 0;

static inline uint32_t hash_pf(uint64_t k, uint32_t mask) {
    k ^= k >> 33; k *= 0xff51afd7ed558ccdULL; k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL; k ^= k >> 33; return (uint32_t)(k & mask);
}

/* ---- Aeron channel poll thread ---- */
static void *channel_poll(void *arg) {
    channel_t *ch = (channel_t *)arg;
    uint8_t req_buf[AERON_MSG_SIZE];
    uint8_t resp_buf[1 + TLC_VALUE_SIZE];
    /* Pre-allocate MGET response buffer */
    size_t mget_resp_cap = 4 + BATCH_LIMIT * (1 + TLC_VALUE_SIZE);
    uint8_t *mget_resp = malloc(mget_resp_cap);

    while (g_running && ch->active) {
        int rlen = aeron_poll(ch->req, req_buf, sizeof(req_buf));
        if (rlen <= 0) {
#if defined(__aarch64__)
            __asm__ volatile("yield" ::: "memory");
#endif
            continue;
        }

        uint8_t op = req_buf[0];
        int resp_len = 0;

        switch (op) {
        case OP_GET: {
            if (rlen < 9) break;
            uint64_t key; memcpy(&key, req_buf + 1, 8);
            if (tlc_get(&g_cache, key, resp_buf + 1) == 0) {
                resp_buf[0] = 0x00;
                resp_len = 1 + TLC_VALUE_SIZE;
            } else {
                resp_buf[0] = 0x01;
                resp_len = 1;
            }
            if (resp_len > 0) {
                while (aeron_publish(ch->resp, resp_buf, resp_len) != 0) {
#if defined(__aarch64__)
                    __asm__ volatile("yield" ::: "memory");
#endif
                }
            }
            break;
        }
        case OP_PUT: {
            if (rlen < 9 + TLC_VALUE_SIZE) break;
            uint64_t key; memcpy(&key, req_buf + 1, 8);
            tlc_put(&g_cache, key, req_buf + 9);
            resp_buf[0] = 0x00;
            resp_len = 1;
            while (aeron_publish(ch->resp, resp_buf, resp_len) != 0) {
#if defined(__aarch64__)
                __asm__ volatile("yield" ::: "memory");
#endif
            }
            break;
        }
        case OP_MGET: {
            /* ============================================
             * MGET: merge N keys → SVE2 batch gather load
             * Request:  [0x03][4B count][N × 8B key]
             * Response: [4B count][N × (1B status + 1200B value)]
             * ============================================ */
            if (rlen < 5) break;
            uint32_t cnt; memcpy(&cnt, req_buf + 1, 4);
            if (cnt > BATCH_LIMIT) cnt = BATCH_LIMIT;
            if (rlen < (int)(5 + cnt * 8)) break;

            const uint64_t *keys = (const uint64_t *)(req_buf + 5);

            /* Build response with SVE2 prefetch gather */
            memcpy(mget_resp, &cnt, 4);
            int off = 4;

            /* Prefetch first 8 HOT entries */
            uint32_t hm = g_cache.hot.mask;
            for (uint32_t p = 0; p < cnt && p < 8; p++)
                __builtin_prefetch(&g_cache.hot.table[hash_pf(keys[p], hm)], 0, 3);

            for (uint32_t i = 0; i < cnt; i++) {
                /* Prefetch 8 ahead */
                if (i + 8 < cnt)
                    __builtin_prefetch(&g_cache.hot.table[hash_pf(keys[i+8], hm)], 0, 3);

                if (tlc_get(&g_cache, keys[i], mget_resp + off + 1) == 0) {
                    mget_resp[off] = 0x00;
                    off += 1 + TLC_VALUE_SIZE;
                } else {
                    mget_resp[off] = 0x01;
                    off += 1;
                }
            }

            /* Publish large response via large ring */
            if (ch->large_resp) {
                while (aeron_large_publish(ch->large_resp, mget_resp, off) != 0) {
#if defined(__aarch64__)
                    __asm__ volatile("yield" ::: "memory");
#endif
                }
            }

            atomic_fetch_add_explicit(&ch->mget_ops, cnt, memory_order_relaxed);
            atomic_fetch_add_explicit(&g_mget_keys, cnt, memory_order_relaxed);
            atomic_fetch_add_explicit(&g_total_aeron_ops, cnt, memory_order_relaxed);
            atomic_fetch_add_explicit(&ch->ops, 1, memory_order_relaxed);
            continue;  /* Skip the per-op counter below */
        }
        case OP_PING:
            resp_buf[0] = 0x00;
            while (aeron_publish(ch->resp, resp_buf, 1) != 0) {
#if defined(__aarch64__)
                __asm__ volatile("yield" ::: "memory");
#endif
            }
            break;
        default:
            resp_buf[0] = 0xFF;
            while (aeron_publish(ch->resp, resp_buf, 1) != 0) {
#if defined(__aarch64__)
                __asm__ volatile("yield" ::: "memory");
#endif
            }
        }

        atomic_fetch_add_explicit(&ch->ops, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&g_total_aeron_ops, 1, memory_order_relaxed);
    }

    free(mget_resp);
    return NULL;
}

/* Create a new Aeron channel (called when client requests one via UDS) */
static int create_channel(void) {
    int id = atomic_fetch_add(&g_num_channels, 1);
    if (id >= MAX_CHANNELS) return -1;

    channel_t *ch = &g_channels[id];
    ch->id = id;

    char name[64];
    snprintf(name, sizeof(name), "/aeron_tlc_req_%d", id);
    int fd = shm_open(name, O_CREAT | O_RDWR, 0666);
    if (fd < 0) return -1;
    if (ftruncate(fd, sizeof(aeron_ring_t)) < 0) { close(fd); return -1; }
    ch->req = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (ch->req == MAP_FAILED) return -1;
    memset(ch->req, 0, sizeof(aeron_ring_t));

    snprintf(name, sizeof(name), "/aeron_tlc_resp_%d", id);
    fd = shm_open(name, O_CREAT | O_RDWR, 0666);
    if (fd < 0) return -1;
    if (ftruncate(fd, sizeof(aeron_ring_t)) < 0) { close(fd); return -1; }
    ch->resp = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (ch->resp == MAP_FAILED) return -1;
    memset(ch->resp, 0, sizeof(aeron_ring_t));

    atomic_store(&ch->ops, 0);
    atomic_store(&ch->mget_ops, 0);

    /* Create large response ring for MGET */
    snprintf(name, sizeof(name), "/aeron_tlc_lresp_%d", id);
    fd = shm_open(name, O_CREAT | O_RDWR, 0666);
    if (fd >= 0) {
        uint32_t slot_data_size = 4 + BATCH_LIMIT * (1 + TLC_VALUE_SIZE) + 64;
        uint32_t slot_total = sizeof(aeron_large_slot_t) + slot_data_size;
        uint32_t num_slots = 4;
        size_t total = sizeof(aeron_large_ring_t) + num_slots * slot_total;
        if (ftruncate(fd, total) == 0) {
            void *p = mmap(NULL, total, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
            if (p != MAP_FAILED) {
                ch->large_resp = (aeron_large_ring_t *)p;
                ch->large_resp->head = 0;
                ch->large_resp->tail = 0;
                ch->large_resp->slot_size = slot_total;
                ch->large_resp->num_slots = num_slots;
                /* Init slot capacities */
                for (uint32_t s = 0; s < num_slots; s++) {
                    aeron_large_slot_t *slot = aeron_large_slot(ch->large_resp, s);
                    slot->len = 0;
                    slot->capacity = slot_data_size;
                }
            }
        }
        close(fd);
    }

    ch->active = 1;
    pthread_create(&ch->thread, NULL, channel_poll, ch);

    return id;
}

/* ---- UDS handler for control plane + MGET ---- */
static int read_full(int fd, void *buf, size_t n) {
    size_t d=0; while(d<n){ssize_t r=read(fd,(char*)buf+d,n-d);if(r<=0)return -1;d+=r;} return 0;
}
static int write_full(int fd, const void *buf, size_t n) {
    size_t d=0; while(d<n){ssize_t w=write(fd,(const char*)buf+d,n-d);if(w<=0)return -1;d+=w;} return 0;
}

#define OP_ALLOC_CHANNEL 0x20  /* Allocate Aeron channel, returns channel_id */

static __thread uint8_t t_resp[4 + BATCH_LIMIT * (1 + TLC_VALUE_SIZE)];

static void *uds_thread(void *arg) {
    int epfd = *(int *)arg;
    struct epoll_event events[256];
    uint8_t get_resp[1 + TLC_VALUE_SIZE];

    while (g_running) {
        int n = epoll_wait(epfd, events, 256, 10);
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            uint8_t op;
            if (read(fd, &op, 1) != 1) goto cfd;

            switch (op) {
            case OP_ALLOC_CHANNEL: {
                int ch_id = create_channel();
                int32_t resp = ch_id;
                write_full(fd, &resp, 4);
                if (ch_id >= 0)
                    printf("  Channel %d allocated\n", ch_id);
                break;
            }
            case OP_GET: {
                uint64_t key;
                if (read_full(fd, &key, 8) != 0) goto cfd;
                if (tlc_get(&g_cache, key, get_resp + 1) == 0) {
                    get_resp[0] = 0x00;
                    write_full(fd, get_resp, 1 + TLC_VALUE_SIZE);
                } else {
                    uint8_t m = 0x01; write_full(fd, &m, 1);
                }
                atomic_fetch_add_explicit(&g_total_uds_ops, 1, memory_order_relaxed);
                break;
            }
            case OP_PUT: {
                uint64_t key; uint8_t vbuf[TLC_VALUE_SIZE];
                if (read_full(fd, &key, 8) != 0) goto cfd;
                if (read_full(fd, vbuf, TLC_VALUE_SIZE) != 0) goto cfd;
                tlc_put(&g_cache, key, vbuf);
                uint8_t ok = 0x00; write_full(fd, &ok, 1);
                atomic_fetch_add_explicit(&g_total_uds_ops, 1, memory_order_relaxed);
                break;
            }
            case OP_MGET: {
                uint32_t cnt;
                if (read_full(fd, &cnt, 4) != 0) goto cfd;
                if (cnt > BATCH_LIMIT) cnt = BATCH_LIMIT;
                uint64_t keys[BATCH_LIMIT];
                if (read_full(fd, keys, cnt * 8) != 0) goto cfd;
                memcpy(t_resp, &cnt, 4);
                int off = 4;
                uint32_t hm = g_cache.hot.mask;
                for (uint32_t p = 0; p < cnt && p < 8; p++)
                    __builtin_prefetch(&g_cache.hot.table[hash_pf(keys[p], hm)], 0, 3);
                for (uint32_t j = 0; j < cnt; j++) {
                    if (j + 8 < cnt)
                        __builtin_prefetch(&g_cache.hot.table[hash_pf(keys[j+8], hm)], 0, 3);
                    if (tlc_get(&g_cache, keys[j], t_resp + off + 1) == 0) {
                        t_resp[off] = 0x00; off += 1 + TLC_VALUE_SIZE;
                    } else {
                        t_resp[off] = 0x01; off += 1;
                    }
                }
                write_full(fd, t_resp, off);
                atomic_fetch_add_explicit(&g_total_uds_ops, cnt, memory_order_relaxed);
                atomic_fetch_add_explicit(&g_mget_keys, cnt, memory_order_relaxed);
                break;
            }
            case OP_FILL: {
                uint64_t count;
                if (read_full(fd, &count, 8) != 0) goto cfd;
                uint8_t fbuf[TLC_VALUE_SIZE]; unsigned seed = 12345;
                for (uint64_t j = 0; j < count; j++) {
                    for (int k = 0; k < (int)(TLC_VALUE_SIZE/4); k++) ((uint32_t*)fbuf)[k] = rand_r(&seed);
                    tlc_put(&g_cache, j, fbuf);
                }
                uint8_t ok = 0x00; write_full(fd, &ok, 1);
                write_full(fd, &count, 8);
                break;
            }
            case OP_STATS: {
                uint64_t stats[6];
                stats[0] = atomic_load(&g_total_aeron_ops);
                stats[1] = atomic_load(&g_total_uds_ops);
                stats[2] = atomic_load(&g_num_channels);
                stats[3] = atomic_load(&g_cache.warm.count);
                stats[4] = atomic_load(&g_mget_keys);
                stats[5] = 0;
                for (int c = 0; c < atomic_load(&g_num_channels); c++)
                    stats[5] += atomic_load(&g_channels[c].ops);
                uint8_t ok = 0x00; write_full(fd, &ok, 1);
                write_full(fd, stats, sizeof(stats));
                break;
            }
            case OP_PING: { uint8_t ok = 0x00; write_full(fd, &ok, 1); break; }
            default: goto cfd;
            }
            continue;
cfd:        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL); close(fd);
        }
    }
    return NULL;
}

static void sig_handler(int sig) { (void)sig; g_running = 0; }

int main(int argc, char *argv[]) {
    (void)argc; (void)argv;
    signal(SIGINT, sig_handler); signal(SIGTERM, sig_handler); signal(SIGPIPE, SIG_IGN);

    printf("╔═══════════════════════════════════════════════════════════════╗\n");
    printf("║  TLC Aeron IPC Server — Default Transport                   ║\n");
    printf("║  Data plane:  Aeron SPSC rings (zero-syscall, per-client)   ║\n");
    printf("║  Control:     UDS /tmp/tlc.sock (FILL/STATS/MGET)           ║\n");
    printf("║  Max channels: %d                                           ║\n", MAX_CHANNELS);
    printf("╚═══════════════════════════════════════════════════════════════╝\n\n");

    if (tlc_init(&g_cache, 0, 0) != 0) { fprintf(stderr, "Cache init failed\n"); return 1; }
    tlc_emb_init(&g_cache, SVE2_EMB_TABLE_SIZE, SVE2_EMB_DIM_DEFAULT);

    /* UDS listen */
    unlink(UDS_PATH);
    int uds_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un ua = {.sun_family = AF_UNIX};
    strncpy(ua.sun_path, UDS_PATH, sizeof(ua.sun_path) - 1);
    bind(uds_fd, (struct sockaddr*)&ua, sizeof(ua));
    listen(uds_fd, 4096);
    fcntl(uds_fd, F_SETFL, fcntl(uds_fd, F_GETFL, 0) | O_NONBLOCK);

    int epfd = epoll_create1(0);

    /* UDS handler threads — each gets its own epoll */
    #define UDS_THREADS 4
    pthread_t uds_pts[UDS_THREADS];
    int epfds[UDS_THREADS];
    for (int i = 0; i < UDS_THREADS; i++) {
        epfds[i] = epoll_create1(0);
        pthread_create(&uds_pts[i], NULL, uds_thread, &epfds[i]);
    }

    /* Acceptor epoll — separate from UDS threads */
    struct epoll_event ev = {.events = EPOLLIN, .data.fd = uds_fd};
    epoll_ctl(epfd, EPOLL_CTL_ADD, uds_fd, &ev);

    printf("UDS: %s\nServer ready.\n\n", UDS_PATH);

    /* Acceptor */
    int next_uds = 0;
    while (g_running) {
        struct epoll_event aev[64];
        int n = epoll_wait(epfd, aev, 64, 100);
        for (int i = 0; i < n; i++) {
            if (aev[i].data.fd == uds_fd) {
                struct sockaddr_un ca; socklen_t cl = sizeof(ca);
                int cfd;
                while ((cfd = accept(uds_fd, (struct sockaddr*)&ca, &cl)) >= 0) {
                    struct epoll_event cev = {.events = EPOLLIN, .data.fd = cfd};
                    epoll_ctl(epfds[next_uds], EPOLL_CTL_ADD, cfd, &cev);
                    next_uds = (next_uds + 1) % UDS_THREADS;
                }
            }
        }
    }

    printf("\nShutting down...\n");
    for (int i = 0; i < atomic_load(&g_num_channels); i++) {
        g_channels[i].active = 0;
        pthread_join(g_channels[i].thread, NULL);
        char name[64];
        snprintf(name, sizeof(name), "/aeron_tlc_req_%d", i);
        munmap(g_channels[i].req, sizeof(aeron_ring_t)); shm_unlink(name);
        snprintf(name, sizeof(name), "/aeron_tlc_resp_%d", i);
        munmap(g_channels[i].resp, sizeof(aeron_ring_t)); shm_unlink(name);
        snprintf(name, sizeof(name), "/aeron_tlc_lresp_%d", i);
        if (g_channels[i].large_resp) {
            size_t lsz = sizeof(aeron_large_ring_t) + g_channels[i].large_resp->num_slots * g_channels[i].large_resp->slot_size;
            munmap(g_channels[i].large_resp, lsz);
        }
        shm_unlink(name);
    }
    for (int i = 0; i < UDS_THREADS; i++) { pthread_cancel(uds_pts[i]); pthread_join(uds_pts[i], NULL); }

    printf("\n=== Stats ===\n");
    printf("  Aeron ops:  %lu\n", atomic_load(&g_total_aeron_ops));
    printf("  UDS ops:    %lu\n", atomic_load(&g_total_uds_ops));
    printf("  Channels:   %d\n", atomic_load(&g_num_channels));
    printf("  MGET keys:  %lu\n", atomic_load(&g_mget_keys));
    for (int i = 0; i < atomic_load(&g_num_channels); i++)
        printf("  Ch[%d]: %lu ops\n", i, atomic_load(&g_channels[i].ops));

    tlc_print_stats(&g_cache);
    unlink(UDS_PATH);
    tlc_destroy(&g_cache);
    return 0;
}
