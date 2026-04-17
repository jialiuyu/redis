/*
 * TLC Unified Server — TCP + UDS + Shared Memory IPC + Aeron IPC
 *
 * Listens on:
 *   TCP  port 6381 (baseline, same as before)
 *   UDS  /tmp/tlc.sock (bypass TCP/IP stack)
 *   SHM  /dev/shm/tlc_shm_ring (zero-copy, zero-syscall data plane)
 *   Aeron /dev/shm/aeron_tlc_req + /dev/shm/aeron_tlc_resp (SPSC rings)
 *
 * All transports share the same three-layer cache + SVE2 gather.
 */
#define _GNU_SOURCE
#include "urma.h"
#include "tlc_transport.h"
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
#include <sys/stat.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <errno.h>

#define TCP_PORT       6381
#define UDS_PATH       "/tmp/tlc.sock"
#define IO_THREADS     8
#define EPOLL_EVENTS   256
#define BATCH_LIMIT    3000

#define OP_GET   0x01
#define OP_PUT   0x02
#define OP_MGET  0x03
#define OP_STATS 0x04
#define OP_FILL  0x05
#define OP_PING  0x06

static three_layer_cache_t g_cache;
static volatile int g_running = 1;
static shm_ring_t *g_shm = NULL;

static atomic_uint_fast64_t g_tcp_ops = 0;
static atomic_uint_fast64_t g_uds_ops = 0;
static atomic_uint_fast64_t g_shm_ops = 0;
static atomic_uint_fast64_t g_mget_batches = 0;
static atomic_uint_fast64_t g_mget_keys = 0;
static atomic_uint_fast64_t g_aeron_ops = 0;

/* Aeron IPC rings */
static aeron_ring_t *g_aeron_req = NULL;
static aeron_ring_t *g_aeron_resp = NULL;
#define AERON_REQ_SHM  "/aeron_tlc_req"
#define AERON_RESP_SHM "/aeron_tlc_resp"

static int read_full(int fd, void *buf, size_t n) {
    size_t d = 0; while (d < n) { ssize_t r = read(fd,(char*)buf+d,n-d); if (r<=0) return -1; d+=r; } return 0;
}
static int write_full(int fd, const void *buf, size_t n) {
    size_t d = 0; while (d < n) { ssize_t w = write(fd,(const char*)buf+d,n-d); if (w<=0) return -1; d+=w; } return 0;
}

static inline uint32_t hash_for_pf(uint64_t k, uint32_t mask) {
    k ^= k >> 33; k *= 0xff51afd7ed558ccdULL; k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL; k ^= k >> 33; return (uint32_t)(k & mask);
}

/* ---- Process request from any transport ---- */
/* Input: request data + length. Output: response written to resp_buf, return resp_len */
static int process_request(const uint8_t *req, int req_len, uint8_t *resp, int resp_max) {
    if (req_len < 1) return -1;
    uint8_t op = req[0];

    switch (op) {
    case OP_GET: {
        if (req_len < 9) return -1;
        uint64_t key; memcpy(&key, req + 1, 8);
        if (tlc_get(&g_cache, key, resp + 1) == 0) {
            resp[0] = 0x00;
            return 1 + TLC_VALUE_SIZE;
        } else {
            resp[0] = 0x01;
            return 1;
        }
    }
    case OP_PUT: {
        if (req_len < 9 + TLC_VALUE_SIZE) return -1;
        uint64_t key; memcpy(&key, req + 1, 8);
        tlc_put(&g_cache, key, req + 9);
        resp[0] = 0x00;
        return 1;
    }
    case OP_MGET: {
        if (req_len < 5) return -1;
        uint32_t cnt; memcpy(&cnt, req + 1, 4);
        if (cnt > BATCH_LIMIT) cnt = BATCH_LIMIT;
        if (req_len < (int)(5 + cnt * 8)) return -1;
        const uint64_t *keys = (const uint64_t *)(req + 5);

        memcpy(resp, &cnt, 4);
        int off = 4;
        uint32_t hm = g_cache.hot.mask;
        for (uint32_t p = 0; p < cnt && p < 8; p++)
            __builtin_prefetch(&g_cache.hot.table[hash_for_pf(keys[p], hm)], 0, 3);
        for (uint32_t i = 0; i < cnt; i++) {
            if (i + 8 < cnt)
                __builtin_prefetch(&g_cache.hot.table[hash_for_pf(keys[i+8], hm)], 0, 3);
            if (tlc_get(&g_cache, keys[i], resp + off + 1) == 0) {
                resp[off] = 0x00; off += 1 + TLC_VALUE_SIZE;
            } else {
                resp[off] = 0x01; off += 1;
            }
        }
        atomic_fetch_add_explicit(&g_mget_batches, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&g_mget_keys, cnt, memory_order_relaxed);
        return off;
    }
    case OP_PING:
        resp[0] = 0x00; return 1;
    case OP_FILL: {
        if (req_len < 9) return -1;
        uint64_t count; memcpy(&count, req + 1, 8);
        uint8_t fbuf[TLC_VALUE_SIZE]; unsigned seed = 12345;
        for (uint64_t i = 0; i < count; i++) {
            for (int j = 0; j < (int)(TLC_VALUE_SIZE/4); j++) ((uint32_t*)fbuf)[j] = rand_r(&seed);
            tlc_put(&g_cache, i, fbuf);
        }
        resp[0] = 0x00; memcpy(resp + 1, &count, 8); return 9;
    }
    default: return -1;
    }
}

/* ---- TCP/UDS IO thread (same handler for both) ---- */
typedef struct { int tid; int epfd; int is_uds; } io_ctx_t;

static __thread uint8_t t_resp[4 + BATCH_LIMIT * (1 + TLC_VALUE_SIZE)];

static void *io_thread(void *arg) {
    io_ctx_t *ctx = arg;
    struct epoll_event events[EPOLL_EVENTS];
    uint8_t get_resp[1 + TLC_VALUE_SIZE];

    while (g_running) {
        int n = epoll_wait(ctx->epfd, events, EPOLL_EVENTS, 10);
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            uint8_t op;
            if (read(fd, &op, 1) != 1) goto close_fd;

            if (op == OP_GET) {
                uint64_t key;
                if (read_full(fd, &key, 8) != 0) goto close_fd;
                if (tlc_get(&g_cache, key, get_resp + 1) == 0) {
                    get_resp[0] = 0x00;
                    if (write_full(fd, get_resp, 1 + TLC_VALUE_SIZE) != 0) goto close_fd;
                } else {
                    uint8_t miss = 0x01;
                    if (write_full(fd, &miss, 1) != 0) goto close_fd;
                }
                atomic_fetch_add_explicit(ctx->is_uds ? &g_uds_ops : &g_tcp_ops, 1, memory_order_relaxed);
            } else if (op == OP_PUT) {
                uint64_t key; uint8_t vbuf[TLC_VALUE_SIZE];
                if (read_full(fd, &key, 8) != 0) goto close_fd;
                if (read_full(fd, vbuf, TLC_VALUE_SIZE) != 0) goto close_fd;
                tlc_put(&g_cache, key, vbuf);
                uint8_t ok = 0x00;
                if (write_full(fd, &ok, 1) != 0) goto close_fd;
                atomic_fetch_add_explicit(ctx->is_uds ? &g_uds_ops : &g_tcp_ops, 1, memory_order_relaxed);
            } else if (op == OP_MGET) {
                uint32_t cnt;
                if (read_full(fd, &cnt, 4) != 0) goto close_fd;
                if (cnt > BATCH_LIMIT) cnt = BATCH_LIMIT;
                uint64_t keys_buf[BATCH_LIMIT];
                if (read_full(fd, keys_buf, cnt * 8) != 0) goto close_fd;

                uint8_t req_hdr[5]; req_hdr[0] = OP_MGET; memcpy(req_hdr + 1, &cnt, 4);
                /* Build full request for process_request */
                size_t full_req_len = 5 + cnt * 8;
                uint8_t *full_req = malloc(full_req_len);
                memcpy(full_req, req_hdr, 5);
                memcpy(full_req + 5, keys_buf, cnt * 8);
                int rlen = process_request(full_req, full_req_len, t_resp, sizeof(t_resp));
                free(full_req);
                if (rlen > 0) write_full(fd, t_resp, rlen);
                atomic_fetch_add_explicit(ctx->is_uds ? &g_uds_ops : &g_tcp_ops, cnt, memory_order_relaxed);
            } else if (op == OP_FILL) {
                uint64_t count;
                if (read_full(fd, &count, 8) != 0) goto close_fd;
                uint8_t req[9]; req[0] = OP_FILL; memcpy(req+1, &count, 8);
                uint8_t resp[16];
                int rlen = process_request(req, 9, resp, sizeof(resp));
                if (rlen > 0) write_full(fd, resp, rlen);
                atomic_fetch_add_explicit(ctx->is_uds ? &g_uds_ops : &g_tcp_ops, count, memory_order_relaxed);
            } else if (op == OP_PING) {
                uint8_t ok = 0x00; write_full(fd, &ok, 1);
            } else {
                goto close_fd;
            }
            continue;
close_fd:
            epoll_ctl(ctx->epfd, EPOLL_CTL_DEL, fd, NULL);
            close(fd);
        }
    }
    return NULL;
}

/* ---- SHM Poll Thread: zero-syscall data plane ---- */
static void *shm_poll_thread(void *arg) {
    (void)arg;
    uint8_t resp_buf[4 + BATCH_LIMIT * (1 + TLC_VALUE_SIZE)];
    uint64_t my_scan = 0;

    while (g_running) {
        int processed = 0;

        /* Scan a range of slots — each poll thread covers different slots */
        for (int s = 0; s < 256; s++) {
            uint64_t idx = (my_scan++) & SHM_RING_MASK;
            shm_slot_t *slot = &g_shm->slots[idx];

            /* Try to claim this slot with CAS: REQUEST → processing */
            uint32_t expected = SLOT_REQUEST;
            if (!__atomic_compare_exchange_n(&slot->state, &expected, 3 /* PROCESSING */,
                                             0, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED))
                continue;

            /* Process request */
            int rlen = process_request(slot->data, slot->len, resp_buf, sizeof(resp_buf));

            if (rlen > 0 && rlen <= (int)SHM_SLOT_SIZE) {
                memcpy(slot->data, resp_buf, rlen);
                slot->len = rlen;
            } else {
                slot->len = 0;
            }

            __atomic_store_n(&slot->state, SLOT_RESPONSE, __ATOMIC_RELEASE);
            processed++;
            atomic_fetch_add_explicit(&g_shm_ops, 1, memory_order_relaxed);
        }

        if (!processed) {
#if defined(__aarch64__)
            __asm__ volatile("yield" ::: "memory");
#endif
        }
    }
    return NULL;
}

/* ---- Init SHM ---- */
static shm_ring_t *init_shm(void) {
    int fd = shm_open(SHM_PATH, O_CREAT | O_RDWR, 0666);
    if (fd < 0) { perror("shm_open"); return NULL; }
    if (ftruncate(fd, SHM_TOTAL_SIZE) < 0) { perror("ftruncate"); close(fd); return NULL; }
    void *p = mmap(NULL, SHM_TOTAL_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror("mmap"); return NULL; }
    shm_ring_t *shm = (shm_ring_t *)p;
    atomic_store(&shm->req_head, 0);
    atomic_store(&shm->req_tail, 0);
    for (int i = 0; i < SHM_RING_SIZE; i++) shm->slots[i].state = SLOT_EMPTY;
    return shm;
}

/* ---- Aeron IPC poll thread ---- */
void *aeron_poll_thread_fn(void *arg) {
    (void)arg;
    uint8_t req_buf[AERON_MSG_SIZE];
    uint8_t resp_buf[AERON_MSG_SIZE];
    while (g_running) {
        int rlen = aeron_poll(g_aeron_req, req_buf, sizeof(req_buf));
        if (rlen > 0) {
            int resp_len = process_request(req_buf, rlen, resp_buf, sizeof(resp_buf));
            if (resp_len > 0) {
                while (aeron_publish(g_aeron_resp, resp_buf, resp_len) != 0) {
#if defined(__aarch64__)
                    __asm__ volatile("yield" ::: "memory");
#endif
                }
            }
            atomic_fetch_add_explicit(&g_aeron_ops, 1, memory_order_relaxed);
        } else {
#if defined(__aarch64__)
            __asm__ volatile("yield" ::: "memory");
#endif
        }
    }
    return NULL;
}

static void sig_handler(int sig) { (void)sig; g_running = 0; }

int main(int argc, char *argv[]) {
    int tcp_port = TCP_PORT;
    for (int i = 1; i < argc; i++)
        if (!strcmp(argv[i], "--port") && i+1 < argc) tcp_port = atoi(argv[++i]);

    signal(SIGINT, sig_handler); signal(SIGTERM, sig_handler); signal(SIGPIPE, SIG_IGN);

    printf("╔═══════════════════════════════════════════════════════════════╗\n");
    printf("║  TLC Unified Server — TCP + UDS + SHM Zero-Copy             ║\n");
    printf("║  TCP:  127.0.0.1:%d                                        ║\n", tcp_port);
    printf("║  UDS:  %s                                        ║\n", UDS_PATH);
    printf("║  SHM:  /dev/shm%s (zero-syscall polling)        ║\n", SHM_PATH);
    printf("╚═══════════════════════════════════════════════════════════════╝\n\n");

    /* Init cache */
    if (tlc_init(&g_cache, 0, 0) != 0) { fprintf(stderr, "Cache init failed\n"); return 1; }
    tlc_emb_init(&g_cache, SVE2_EMB_TABLE_SIZE, SVE2_EMB_DIM_DEFAULT);

    /* Init SHM */
    g_shm = init_shm();
    if (g_shm) printf("SHM ring: %d slots × %d bytes = %.1f MB\n",
                       SHM_RING_SIZE, (int)sizeof(shm_slot_t),
                       (double)SHM_TOTAL_SIZE / (1024*1024));

    /* TCP listen */
    int tcp_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(tcp_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in taddr = {.sin_family=AF_INET, .sin_port=htons(tcp_port),
                                .sin_addr.s_addr=htonl(INADDR_LOOPBACK)};
    bind(tcp_fd, (struct sockaddr*)&taddr, sizeof(taddr));
    listen(tcp_fd, 4096);
    printf("TCP listening on 127.0.0.1:%d\n", tcp_port);

    /* UDS listen */
    unlink(UDS_PATH);
    int uds_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un uaddr = {.sun_family = AF_UNIX};
    strncpy(uaddr.sun_path, UDS_PATH, sizeof(uaddr.sun_path) - 1);
    bind(uds_fd, (struct sockaddr*)&uaddr, sizeof(uaddr));
    listen(uds_fd, 4096);
    printf("UDS listening on %s\n", UDS_PATH);

    /* IO threads for TCP */
    io_ctx_t tcp_ctxs[IO_THREADS];
    pthread_t tcp_pts[IO_THREADS];
    for (int i = 0; i < IO_THREADS; i++) {
        tcp_ctxs[i] = (io_ctx_t){i, epoll_create1(0), 0};
        pthread_create(&tcp_pts[i], NULL, io_thread, &tcp_ctxs[i]);
    }

    /* IO threads for UDS */
    io_ctx_t uds_ctxs[IO_THREADS];
    pthread_t uds_pts[IO_THREADS];
    for (int i = 0; i < IO_THREADS; i++) {
        uds_ctxs[i] = (io_ctx_t){i + IO_THREADS, epoll_create1(0), 1};
        pthread_create(&uds_pts[i], NULL, io_thread, &uds_ctxs[i]);
    }

    /* SHM poll threads (multiple for throughput) */
    #define SHM_POLL_THREADS 4
    pthread_t shm_pts[SHM_POLL_THREADS];
    if (g_shm) {
        for (int i = 0; i < SHM_POLL_THREADS; i++)
            pthread_create(&shm_pts[i], NULL, shm_poll_thread, NULL);
    }

    /* Aeron IPC init */
    {
        int afd = shm_open(AERON_REQ_SHM, O_CREAT | O_RDWR, 0666);
        if (afd >= 0) {
            ftruncate(afd, sizeof(aeron_ring_t));
            g_aeron_req = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, afd, 0);
            close(afd);
            if (g_aeron_req == MAP_FAILED) g_aeron_req = NULL;
            else memset(g_aeron_req, 0, sizeof(aeron_ring_t));
        }
        afd = shm_open(AERON_RESP_SHM, O_CREAT | O_RDWR, 0666);
        if (afd >= 0) {
            ftruncate(afd, sizeof(aeron_ring_t));
            g_aeron_resp = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, afd, 0);
            close(afd);
            if (g_aeron_resp == MAP_FAILED) g_aeron_resp = NULL;
            else memset(g_aeron_resp, 0, sizeof(aeron_ring_t));
        }
        if (g_aeron_req && g_aeron_resp)
            printf("Aeron IPC: req=%s resp=%s (%.1f MB each)\n",
                   AERON_REQ_SHM, AERON_RESP_SHM, (double)sizeof(aeron_ring_t)/(1024*1024));
    }

    /* Aeron poll thread */
    pthread_t aeron_pt;
    if (g_aeron_req && g_aeron_resp) {
        extern void *aeron_poll_thread_fn(void *);
        pthread_create(&aeron_pt, NULL, aeron_poll_thread_fn, NULL);
    }

    /* Acceptor for TCP + UDS */
    /* Make listen sockets non-blocking for accept loop */
    fcntl(tcp_fd, F_SETFL, fcntl(tcp_fd, F_GETFL, 0) | O_NONBLOCK);
    fcntl(uds_fd, F_SETFL, fcntl(uds_fd, F_GETFL, 0) | O_NONBLOCK);

    int aepfd = epoll_create1(0);
    struct epoll_event ev;
    ev.events = EPOLLIN; ev.data.fd = tcp_fd;
    epoll_ctl(aepfd, EPOLL_CTL_ADD, tcp_fd, &ev);
    ev.data.fd = uds_fd;
    epoll_ctl(aepfd, EPOLL_CTL_ADD, uds_fd, &ev);

    int next_tcp = 0, next_uds = 0;
    printf("Server ready.\n\n");

    while (g_running) {
        struct epoll_event aev[64];
        int n = epoll_wait(aepfd, aev, 64, 100);
        for (int i = 0; i < n; i++) {
            int lfd = aev[i].data.fd;
            int cfd;
            struct sockaddr_storage sa; socklen_t sl = sizeof(sa);
            while ((cfd = accept(lfd, (struct sockaddr*)&sa, &sl)) >= 0) {
                if (lfd == tcp_fd) {
                    int one = 1; setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
                    struct epoll_event cev = {.events=EPOLLIN, .data.fd=cfd};
                    epoll_ctl(tcp_ctxs[next_tcp].epfd, EPOLL_CTL_ADD, cfd, &cev);
                    next_tcp = (next_tcp + 1) % IO_THREADS;
                } else {
                    struct epoll_event cev = {.events=EPOLLIN, .data.fd=cfd};
                    epoll_ctl(uds_ctxs[next_uds].epfd, EPOLL_CTL_ADD, cfd, &cev);
                    next_uds = (next_uds + 1) % IO_THREADS;
                }
            }
        }
    }

    printf("\nShutting down...\n");
    for (int i = 0; i < IO_THREADS; i++) { pthread_cancel(tcp_pts[i]); pthread_join(tcp_pts[i], NULL); }
    for (int i = 0; i < IO_THREADS; i++) { pthread_cancel(uds_pts[i]); pthread_join(uds_pts[i], NULL); }
    if (g_shm) { for (int i = 0; i < SHM_POLL_THREADS; i++) { pthread_cancel(shm_pts[i]); pthread_join(shm_pts[i], NULL); } }
    if (g_aeron_req && g_aeron_resp) { pthread_cancel(aeron_pt); pthread_join(aeron_pt, NULL); }

    printf("\n=== Stats ===\n");
    printf("  TCP ops:   %lu\n", atomic_load(&g_tcp_ops));
    printf("  UDS ops:   %lu\n", atomic_load(&g_uds_ops));
    printf("  SHM ops:   %lu\n", atomic_load(&g_shm_ops));
    printf("  Aeron ops: %lu\n", atomic_load(&g_aeron_ops));
    printf("  MGET:     %lu batches, %lu keys\n",
           atomic_load(&g_mget_batches), atomic_load(&g_mget_keys));

    tlc_print_stats(&g_cache);
    if (g_shm) { munmap(g_shm, SHM_TOTAL_SIZE); shm_unlink(SHM_PATH); }
    if (g_aeron_req) { munmap(g_aeron_req, sizeof(aeron_ring_t)); shm_unlink(AERON_REQ_SHM); }
    if (g_aeron_resp) { munmap(g_aeron_resp, sizeof(aeron_ring_t)); shm_unlink(AERON_RESP_SHM); }
    unlink(UDS_PATH);
    tlc_destroy(&g_cache);
    return 0;
}
