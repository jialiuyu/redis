#define _GNU_SOURCE

#include "../src/vemb_v16_client_ring.h"
#include "../src/vemb_v16_net.h"
#include "../src/vemb_v16_protocol.h"
#include "../src/zmalloc.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define VEMB_V16_BENCH_MAX_NODES 16
#define VEMB_V16_BENCH_HASH_VNODES 10
#define VEMB_V16_BENCH_PATH_MAX 256

typedef struct bench_hash_node {
    uint32_t hash_value;
    uint32_t node_index;
} bench_hash_node_t;

typedef struct bench_cfg {
    const char *socket_path;
    const char *tcp_host;
    char socket_paths[VEMB_V16_BENCH_MAX_NODES][VEMB_V16_BENCH_PATH_MAX];
    uint32_t node_count;
    bench_hash_node_t hash_nodes[
        VEMB_V16_BENCH_MAX_NODES * VEMB_V16_BENCH_HASH_VNODES];
    uint32_t hash_node_count;
    uint32_t dim;
    uint32_t prefill;
    uint32_t ops;
    int threads;
    const char *threads_arg;
    int mode;
    int hot_key_enabled;
    uint32_t hot_key_id;
    uint32_t timeout_ms;
    int pin_threads;
    uint32_t pipeline;
    uint32_t transport_type;
    uint16_t tcp_port;
} bench_cfg_t;

typedef struct bench_region_map {
    uint32_t region_id;
    uint32_t backend_type;
    uint32_t value_size;
    uint64_t region_bytes;
    uint64_t mmap_offset;
    uint64_t mmap_aligned_offset;
    uint64_t mapping_bytes;
    uint8_t *mapping_addr;
    uint8_t *mapped_addr;
} bench_region_map_t;

typedef struct bench_node_channel {
    vemb_v16_channel_desc_t desc;
    vemb_v16_client_ring_t *req_ring;
    vemb_v16_client_ring_t *resp_ring;
    int net_fd;
    uint32_t transport_type;
    const char *tcp_host;
    uint16_t tcp_port;
    uint32_t timeout_ms;
    bench_region_map_t warm_region;
} bench_node_channel_t;

typedef struct worker_arg {
    int tid;
    bench_cfg_t cfg;
    uint32_t node_count;
    bench_node_channel_t nodes[VEMB_V16_BENCH_MAX_NODES];
    uint64_t ok;
    uint64_t fail;
    uint64_t read_bytes;
    uint64_t vemb_sent;
    uint64_t vadd_sent;
    uint64_t request_publish_spins;
    uint64_t response_empty_polls;
    uint64_t ns;
    atomic_int stop;
    atomic_int done;
} worker_arg_t;

enum {
    MODE_PING = 0,
    MODE_VEMB_HANDLE = 1,
    MODE_VEMB_READ_VECTOR = 2,
    MODE_VADD_INLINE = 3,
    MODE_VEMB_SUPERNODE_READ = 4,
    MODE_MIXED_80R20W = 5,
    MODE_VEMB_INLINE_VECTOR = 6,
};

static uint32_t g_control_timeout_ms = 10000;

typedef struct pending_req {
    uint32_t op_index;
    uint32_t key_id;
    uint32_t node_index;
} pending_req_t;

static const char *mode_name(int mode);

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int wait_timed_out(uint64_t start_ns, uint32_t timeout_ms) {
    if (timeout_ms == 0) return 0;
    return now_ns() - start_ns >= (uint64_t)timeout_ms * 1000000ULL;
}

static void set_fd_timeout(int fd) {
    if (g_control_timeout_ms == 0) return;
    struct timeval tv = {
        .tv_sec = (time_t)(g_control_timeout_ms / 1000),
        .tv_usec = (suseconds_t)((g_control_timeout_ms % 1000) * 1000),
    };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

static int read_full(int fd, void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = read(fd, (char *)buf + done, n - done);
        if (r <= 0) return -1;
        done += (size_t)r;
    }
    return 0;
}

static int write_full(int fd, const void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = write(fd, (const char *)buf + done, n - done);
        if (r <= 0) return -1;
        done += (size_t)r;
    }
    return 0;
}

static int connect_uds(const char *path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    set_fd_timeout(fd);
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int alloc_channel(const bench_cfg_t *cfg, vemb_v16_channel_desc_t *desc) {
    int fd = connect_uds(cfg->socket_path);
    if (fd < 0) return -1;
    uint8_t op = VEMB_V16_CTRL_ALLOC_CHANNEL;
    vemb_v16_alloc_req_t req = {.vector_dim = cfg->dim};
    uint8_t status = VEMB_V16_STATUS_ERR;
    if (write_full(fd, &op, sizeof(op)) != 0 ||
        write_full(fd, &req, sizeof(req)) != 0 ||
        read_full(fd, &status, sizeof(status)) != 0 ||
        status != VEMB_V16_STATUS_OK ||
        read_full(fd, desc, sizeof(*desc)) != 0) {
        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}

static int alloc_channel_path(const char *socket_path,
                              const bench_cfg_t *cfg,
                              vemb_v16_channel_desc_t *desc) {
    bench_cfg_t node_cfg = *cfg;
    node_cfg.socket_path = socket_path;
    return alloc_channel(&node_cfg, desc);
}

static int alloc_tcp_channel(const bench_cfg_t *cfg,
                             vemb_v16_channel_desc_t *desc,
                             int *net_fd) {
    int fd = vemb_v16_net_connect(cfg->tcp_host,
                                  cfg->tcp_port,
                                  cfg->timeout_ms);
    if (fd < 0) return -1;
    vemb_v16_alloc_req_t req = {.vector_dim = cfg->dim};
    if (vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_HELLO,
                                 0,
                                 0,
                                 0,
                                 &req,
                                 sizeof(req)) != 0) {
        close(fd);
        return -1;
    }

    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_WELCOME ||
        hdr.payload_len != sizeof(*desc) ||
        vemb_v16_net_read_full(fd, desc, sizeof(*desc)) != 0 ||
        desc->magic != VEMB_V16_MAGIC ||
        desc->version != VEMB_V16_VERSION) {
        close(fd);
        return -1;
    }
    *net_fd = fd;
    return 0;
}

static int tcp_control_request(const char *host,
                               uint16_t port,
                               uint32_t timeout_ms,
                               uint16_t type,
                               uint64_t channel_id,
                               uint16_t expect_type,
                               void *payload,
                               uint32_t payload_len) {
    int fd = vemb_v16_net_connect(host, port, timeout_ms);
    if (fd < 0) return -1;
    if (vemb_v16_net_write_frame(fd,
                                 type,
                                 0,
                                 channel_id,
                                 0,
                                 NULL,
                                 0) != 0) {
        close(fd);
        return -1;
    }
    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != expect_type ||
        hdr.payload_len != payload_len ||
        (payload_len &&
         vemb_v16_net_read_full(fd, payload, payload_len) != 0)) {
        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}

static int fetch_stats_tcp(const bench_cfg_t *cfg, vemb_v16_stats_t *stats) {
    return tcp_control_request(cfg->tcp_host,
                               cfg->tcp_port,
                               cfg->timeout_ms,
                               VEMB_V16_NET_STATS,
                               0,
                               VEMB_V16_NET_STATS,
                               stats,
                               sizeof(*stats));
}

static int close_channel_tcp(const char *host,
                             uint16_t port,
                             uint32_t timeout_ms,
                             uint64_t channel_id) {
    vemb_v16_net_status_t status;
    if (tcp_control_request(host,
                            port,
                            timeout_ms,
                            VEMB_V16_NET_CLOSE_CHANNEL,
                            channel_id,
                            VEMB_V16_NET_CONTROL_STATUS,
                            &status,
                            sizeof(status)) != 0) {
        return -1;
    }
    return status.status == VEMB_V16_STATUS_OK ? 0 : -1;
}

static int close_all_channels_tcp(const bench_cfg_t *cfg, uint64_t *closed) {
    vemb_v16_net_status_t status;
    if (tcp_control_request(cfg->tcp_host,
                            cfg->tcp_port,
                            cfg->timeout_ms,
                            VEMB_V16_NET_CLOSE_ALL_CHANNELS,
                            0,
                            VEMB_V16_NET_CONTROL_STATUS,
                            &status,
                            sizeof(status)) != 0) {
        return -1;
    }
    if (status.status != VEMB_V16_STATUS_OK)
        return -1;
    if (closed) *closed = status.value;
    return 0;
}

static int fetch_stats(const char *socket_path, vemb_v16_stats_t *stats) {
    int fd = connect_uds(socket_path);
    if (fd < 0) return -1;
    uint8_t op = VEMB_V16_CTRL_STATS;
    uint8_t status = VEMB_V16_STATUS_ERR;
    if (write_full(fd, &op, sizeof(op)) != 0 ||
        read_full(fd, &status, sizeof(status)) != 0 ||
        status != VEMB_V16_STATUS_OK ||
        read_full(fd, stats, sizeof(*stats)) != 0) {
        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}

static int close_channel(const char *socket_path, uint64_t channel_id) {
    int fd = connect_uds(socket_path);
    if (fd < 0) return -1;
    uint8_t op = VEMB_V16_CTRL_CLOSE_CHANNEL;
    uint8_t status = VEMB_V16_STATUS_ERR;
    if (write_full(fd, &op, sizeof(op)) != 0 ||
        write_full(fd, &channel_id, sizeof(channel_id)) != 0 ||
        read_full(fd, &status, sizeof(status)) != 0) {
        close(fd);
        return -1;
    }
    close(fd);
    return status == VEMB_V16_STATUS_OK ? 0 : -1;
}

static int close_channel_path(const char *socket_path, uint64_t channel_id) {
    bench_cfg_t cfg = {.socket_path = socket_path};
    return close_channel(cfg.socket_path, channel_id);
}

static int close_all_channels(const char *socket_path, uint64_t *closed) {
    int fd = connect_uds(socket_path);
    if (fd < 0) return -1;
    uint8_t op = VEMB_V16_CTRL_CLOSE_ALL_CHANNELS;
    uint8_t status = VEMB_V16_STATUS_ERR;
    uint64_t n = 0;
    if (write_full(fd, &op, sizeof(op)) != 0 ||
        read_full(fd, &status, sizeof(status)) != 0 ||
        status != VEMB_V16_STATUS_OK ||
        read_full(fd, &n, sizeof(n)) != 0) {
        close(fd);
        return -1;
    }
    close(fd);
    if (closed) *closed = n;
    return 0;
}

static int open_ring(const char *name,
                     uint32_t slot_size,
                     vemb_v16_client_ring_t **ring) {
    int fd = shm_open(name, O_RDWR, 0666);
    if (fd < 0) return -1;
    size_t bytes = vemb_v16_client_ring_bytes(slot_size);
    void *ptr = mmap(NULL, bytes, PROT_READ | PROT_WRITE,
                     MAP_SHARED, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) return -1;
    *ring = ptr;
    return 0;
}

static int open_warm_region(const vemb_v16_channel_desc_t *desc,
                            bench_region_map_t *region) {
    if (!desc || !region)
        return -1;
    int fd = -1;
    if (desc->warm_backend_type == VEMB_V16_REGION_LOCAL_SHM) {
        fd = shm_open(desc->vector_region_name, O_RDONLY, 0666);
    } else if (desc->warm_backend_type == VEMB_V16_REGION_UB) {
        fd = open(desc->vector_region_name, O_RDONLY);
    } else {
        return -1;
    }
    if (fd < 0) return -1;
    size_t size = desc->warm_region_bytes ?
        (size_t)desc->warm_region_bytes :
        (size_t)desc->vector_stride * desc->max_vectors;
    long page_size = sysconf(_SC_PAGESIZE);
    uint64_t page_mask = (uint64_t)(page_size > 0 ? page_size : 4096) - 1u;
    uint64_t aligned_offset = desc->warm_mmap_offset & ~page_mask;
    size_t offset_delta = (size_t)(desc->warm_mmap_offset - aligned_offset);
    size_t map_size = size + offset_delta;
    void *ptr = mmap(NULL, map_size, PROT_READ, MAP_SHARED, fd,
                     (off_t)aligned_offset);
    close(fd);
    if (ptr == MAP_FAILED) return -1;
    region->region_id = desc->warm_region_id;
    region->backend_type = desc->warm_backend_type;
    region->value_size = desc->vector_stride;
    region->region_bytes = size;
    region->mmap_offset = desc->warm_mmap_offset;
    region->mmap_aligned_offset = aligned_offset;
    region->mapping_bytes = map_size;
    region->mapping_addr = ptr;
    region->mapped_addr = (uint8_t *)ptr + offset_delta;
    return 0;
}

static void close_warm_region(bench_region_map_t *region) {
    if (!region || !region->mapping_addr) return;
    munmap(region->mapping_addr, (size_t)region->mapping_bytes);
    memset(region, 0, sizeof(*region));
}

static void fill_vector(float *vector, uint32_t dim, uint32_t seed) {
    for (uint32_t i = 0; i < dim; i++)
        vector[i] = (float)((seed + i) & 1023) / 1024.0f;
}

static void make_key(char *buf, size_t len, uint32_t id) {
    snprintf(buf, len, "item:%u", id);
}

static int hash_node_cmp(const void *a, const void *b) {
    const bench_hash_node_t *ha = a;
    const bench_hash_node_t *hb = b;
    if (ha->hash_value < hb->hash_value) return -1;
    if (ha->hash_value > hb->hash_value) return 1;
    if (ha->node_index < hb->node_index) return -1;
    if (ha->node_index > hb->node_index) return 1;
    return 0;
}

static int build_hash_ring(bench_cfg_t *cfg) {
    if (!cfg || cfg->node_count == 0 ||
        cfg->node_count > VEMB_V16_BENCH_MAX_NODES) {
        return -1;
    }
    cfg->hash_node_count = 0;
    for (uint32_t node = 0; node < cfg->node_count; node++) {
        for (uint32_t vnode = 0; vnode < VEMB_V16_BENCH_HASH_VNODES; vnode++) {
            char vnode_key[64];
            uint32_t vnode_id = cfg->hash_node_count;
            snprintf(vnode_key, sizeof(vnode_key),
                     "supernode_%u_vnode_%u", node, vnode_id);
            cfg->hash_nodes[cfg->hash_node_count++] = (bench_hash_node_t){
                .hash_value = vemb_v16_murmur3(vnode_key, strlen(vnode_key)),
                .node_index = node,
            };
        }
    }
    qsort(cfg->hash_nodes,
          cfg->hash_node_count,
          sizeof(cfg->hash_nodes[0]),
          hash_node_cmp);
    return 0;
}

static uint32_t route_hash(const bench_cfg_t *cfg, uint32_t hash) {
    if (!cfg || cfg->node_count <= 1 || cfg->hash_node_count == 0)
        return 0;
    uint32_t left = 0;
    uint32_t right = cfg->hash_node_count;
    while (left < right) {
        uint32_t mid = left + (right - left) / 2;
        if (cfg->hash_nodes[mid].hash_value < hash)
            left = mid + 1;
        else
            right = mid;
    }
    if (left >= cfg->hash_node_count) left = 0;
    return cfg->hash_nodes[left].node_index;
}

static uint32_t route_key(const bench_cfg_t *cfg, const char *key) {
    return route_hash(cfg, vemb_v16_murmur3(key, strlen(key)));
}

static bench_region_map_t *find_warm_region(worker_arg_t *w,
                                             uint32_t region_id) {
    for (uint32_t i = 0; i < w->node_count; i++) {
        if (w->nodes[i].warm_region.mapped_addr &&
            w->nodes[i].warm_region.region_id == region_id) {
            return &w->nodes[i].warm_region;
        }
    }
    return NULL;
}

static void prepare_req(vemb_v16_req_t *req,
                        uint8_t op,
                        uint32_t req_id,
                        uint64_t channel_id,
                        const char *key,
                        uint32_t dim) {
    memset(req, 0, sizeof(*req));
    req->op = op;
    req->req_id = req_id;
    req->channel_id = channel_id;
    req->key_len = (uint32_t)strlen(key);
    req->key_hash = vemb_v16_murmur3(key, req->key_len);
    req->dim = dim;
    req->vector_bytes = dim * sizeof(float);
    memcpy(req->key, key, req->key_len);
}

static int send_req(vemb_v16_client_ring_t *ring, const vemb_v16_req_t *req, size_t len,
                    uint64_t *publish_spins, uint32_t timeout_ms) {
    uint64_t spins = 0;
    uint64_t start = now_ns();
    while (vemb_v16_client_publish(ring, req, (uint32_t)len) != 0) {
        spins++;
        if ((spins & 0xfffu) == 0 && wait_timed_out(start, timeout_ms)) {
            if (publish_spins) *publish_spins += spins;
            return -1;
        }
        __asm__ volatile("" ::: "memory");
    }
    if (publish_spins) *publish_spins += spins;
    return 0;
}

static int recv_resp(vemb_v16_client_ring_t *ring, vemb_v16_resp_t *resp,
                     uint64_t *empty_polls, uint32_t timeout_ms) {
    int got;
    uint64_t polls = 0;
    uint64_t start = now_ns();
    while ((got = vemb_v16_client_poll(ring, resp, sizeof(*resp))) <= 0) {
        polls++;
        if ((polls & 0xfffu) == 0 && wait_timed_out(start, timeout_ms)) {
            if (empty_polls) *empty_polls += polls;
            return -1;
        }
        __asm__ volatile("" ::: "memory");
    }
    if (empty_polls) *empty_polls += polls;
    return got == (int)sizeof(*resp) ? 0 : -1;
}

static int send_channel_req(bench_node_channel_t *node,
                            const vemb_v16_req_t *req,
                            size_t len,
                            uint64_t *publish_spins,
                            uint32_t timeout_ms) {
    if (node->transport_type == VEMB_V16_TRANSPORT_TCP) {
        (void)publish_spins;
        (void)timeout_ms;
        if (node->net_fd < 0) return -1;
        return vemb_v16_net_write_frame(node->net_fd,
                                        VEMB_V16_NET_REQUEST,
                                        0,
                                        node->desc.channel_id,
                                        req->req_id,
                                        req,
                                        (uint32_t)len);
    }
    return send_req(node->req_ring, req, len, publish_spins, timeout_ms);
}

static int recv_channel_resp(bench_node_channel_t *node,
                             vemb_v16_resp_t *resp,
                             uint8_t *inline_vector,
                             uint32_t inline_vector_cap,
                             uint32_t *inline_vector_bytes,
                             uint64_t *empty_polls,
                             uint32_t timeout_ms) {
    if (inline_vector_bytes) *inline_vector_bytes = 0;
    if (node->transport_type == VEMB_V16_TRANSPORT_TCP) {
        (void)empty_polls;
        (void)timeout_ms;
        if (node->net_fd < 0) return -1;
        vemb_v16_net_hdr_t hdr;
        if (vemb_v16_net_read_header(node->net_fd, &hdr) != 0 ||
            hdr.type != VEMB_V16_NET_RESPONSE ||
            hdr.channel_id != node->desc.channel_id ||
            hdr.payload_len < sizeof(*resp)) {
            return -1;
        }
        uint32_t extra = hdr.payload_len - (uint32_t)sizeof(*resp);
        if (extra) {
            if (!inline_vector || extra > inline_vector_cap)
                return -1;
            struct iovec iov[2] = {
                {.iov_base = resp, .iov_len = sizeof(*resp)},
                {.iov_base = inline_vector, .iov_len = extra},
            };
            if (vemb_v16_net_readv_full(node->net_fd, iov, 2) != 0)
                return -1;
            if (inline_vector_bytes) *inline_vector_bytes = extra;
        } else if (vemb_v16_net_read_full(node->net_fd, resp, sizeof(*resp)) != 0) {
            return -1;
        }
        return 0;
    }
    (void)inline_vector;
    (void)inline_vector_cap;
    return recv_resp(node->resp_ring, resp, empty_polls, timeout_ms);
}

static int prefill_multi(const bench_cfg_t *cfg,
                         bench_node_channel_t *nodes,
                         uint32_t node_count) {
    vemb_v16_req_t req;
    vemb_v16_resp_t resp;
    char key[VEMB_V16_MAX_KEY_LEN];
    uint64_t start = now_ns();
    for (uint32_t i = 0; i < cfg->prefill; i++) {
        make_key(key, sizeof(key), i);
        uint32_t node_index = route_key(cfg, key);
        if (node_index >= node_count)
            return -1;
        bench_node_channel_t *node = &nodes[node_index];
        prepare_req(&req, VEMB_V16_OP_VADD_INLINE, i + 1,
                    node->desc.channel_id, key, cfg->dim);
        fill_vector(req.vector, cfg->dim, i);
        if (send_channel_req(node, &req,
                             vemb_v16_req_inline_len(req.vector_bytes),
                             NULL, cfg->timeout_ms) != 0) {
            fprintf(stderr, "prefill send timeout at item=%u node=%u\n",
                    i, node_index);
            return -1;
        }
        if (recv_channel_resp(node, &resp, NULL, 0, NULL, NULL, cfg->timeout_ms) != 0 ||
            resp.status != VEMB_V16_STATUS_OK) {
            fprintf(stderr, "prefill response timeout/error at item=%u node=%u status=%u\n",
                    i, node_index, resp.status);
            return -1;
        }
        if ((i + 1) % 10000 == 0)
            printf("[prefill] inserted=%u elapsed=%.3fs\n",
                   i + 1, (double)(now_ns() - start) / 1e9);
    }
    if (cfg->prefill > 0)
        printf("[prefill] inserted=%u elapsed=%.3fs\n",
               cfg->prefill, (double)(now_ns() - start) / 1e9);
    return 0;
}

static void *worker_main(void *arg) {
    worker_arg_t *w = arg;
#ifdef __linux__
    if (w->cfg.pin_threads) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET((w->tid * 2 + 3) % 64, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
    }
#endif

    uint32_t pipeline = w->cfg.pipeline ? w->cfg.pipeline : 1;
    vemb_v16_req_t req;
    pending_req_t *pending = zcalloc_num(pipeline, sizeof(*pending));
    if (!pending) {
        zfree(pending);
        w->fail = w->cfg.ops;
        atomic_store_explicit(&w->done, 1, memory_order_release);
        return NULL;
    }
    uint32_t inline_vector_cap = w->cfg.dim * sizeof(float);
    uint8_t *inline_vector = NULL;
    if (w->cfg.mode == MODE_VEMB_INLINE_VECTOR) {
        inline_vector = zmalloc(inline_vector_cap);
        if (!inline_vector) {
            zfree(pending);
            w->fail = w->cfg.ops;
            atomic_store_explicit(&w->done, 1, memory_order_release);
            return NULL;
        }
    }
    char key[VEMB_V16_MAX_KEY_LEN];
    uint64_t start = now_ns();
    uint32_t sent = 0;
    uint32_t completed = 0;
    uint32_t pending_head = 0;
    uint32_t pending_tail = 0;
    uint32_t pending_count = 0;
    while (completed < w->cfg.ops &&
           !atomic_load_explicit(&w->stop, memory_order_acquire)) {
        while (sent < w->cfg.ops && pending_count < pipeline) {
            if (atomic_load_explicit(&w->stop, memory_order_acquire))
                break;
            uint32_t i = sent;
            uint32_t global_id = (uint32_t)(i + (uint32_t)w->tid * w->cfg.ops);
            uint32_t key_id = w->cfg.prefill ? global_id % w->cfg.prefill : global_id;
            if (w->cfg.hot_key_enabled) key_id = w->cfg.hot_key_id;
            size_t req_len = vemb_v16_req_handle_len();
            int send_failed = 0;
            int mixed_write = w->cfg.mode == MODE_MIXED_80R20W &&
                              (i % 5u) == 0;
            uint8_t op = (w->cfg.mode == MODE_VEMB_SUPERNODE_READ ||
                          w->cfg.mode == MODE_MIXED_80R20W) ?
                VEMB_V16_OP_VEMB_SUPERNODE_READ : VEMB_V16_OP_VEMB_HANDLE;
            if (w->cfg.mode == MODE_PING) {
                bench_node_channel_t *node = &w->nodes[0];
                memset(&req, 0, sizeof(req));
                req.op = VEMB_V16_OP_PING;
                req.req_id = i + 1;
                req.channel_id = node->desc.channel_id;
                if (send_channel_req(node, &req, req_len,
                                     &w->request_publish_spins,
                                     w->cfg.timeout_ms) != 0) {
                    fprintf(stderr, "worker %d request publish timeout at op=%u mode=ping\n",
                            w->tid, i);
                    send_failed = 1;
                }
            } else if (w->cfg.mode == MODE_VADD_INLINE || mixed_write) {
                uint32_t write_key_id = mixed_write ? key_id : global_id + 100000000u;
                make_key(key, sizeof(key), write_key_id);
                uint32_t node_index = route_key(&w->cfg, key);
                bench_node_channel_t *node = &w->nodes[node_index];
                prepare_req(&req, VEMB_V16_OP_VADD_INLINE, i + 1,
                            node->desc.channel_id, key, w->cfg.dim);
                fill_vector(req.vector, w->cfg.dim, global_id);
                req_len = vemb_v16_req_inline_len(req.vector_bytes);
                w->vadd_sent++;
                if (send_channel_req(node, &req, req_len,
                                     &w->request_publish_spins,
                                     w->cfg.timeout_ms) != 0) {
                    fprintf(stderr, "worker %d request publish timeout at op=%u mode=%s\n",
                            w->tid, i, mode_name(w->cfg.mode));
                    send_failed = 1;
                }
                pending[pending_tail].node_index = node_index;
            } else {
                make_key(key, sizeof(key), key_id);
                uint32_t node_index = route_key(&w->cfg, key);
                bench_node_channel_t *node = &w->nodes[node_index];
                prepare_req(&req, op, i + 1, node->desc.channel_id, key,
                            w->cfg.dim);
                if (w->cfg.mode == MODE_VEMB_INLINE_VECTOR)
                    req.flags |= VEMB_V16_REQ_F_INLINE_VECTOR;
                w->vemb_sent++;
                if (send_channel_req(node, &req, req_len,
                                     &w->request_publish_spins,
                                     w->cfg.timeout_ms) != 0) {
                    fprintf(stderr, "worker %d request publish timeout at op=%u key_id=%u\n",
                            w->tid, i, key_id);
                    send_failed = 1;
                }
                pending[pending_tail].node_index = node_index;
            }
            if (send_failed) {
                if (atomic_load_explicit(&w->stop, memory_order_acquire))
                    goto worker_done;
                w->fail += w->cfg.ops - completed;
                goto worker_done;
            }
            pending[pending_tail].op_index = i;
            pending[pending_tail].key_id = key_id;
            pending_tail = (pending_tail + 1) % pipeline;
            pending_count++;
            sent++;
        }

        vemb_v16_resp_t resp;
        uint32_t inline_vector_bytes = 0;
        uint32_t recv_node_index = pending[pending_head].node_index;
        bench_node_channel_t *recv_node = &w->nodes[recv_node_index];
        if (recv_channel_resp(recv_node,
                              &resp,
                              inline_vector,
                              inline_vector_cap,
                              &inline_vector_bytes,
                              &w->response_empty_polls,
                              w->cfg.timeout_ms) != 0) {
            if (atomic_load_explicit(&w->stop, memory_order_acquire))
                goto worker_done;
            uint32_t key_id = pending_count ? pending[pending_head].key_id : 0;
            uint32_t op_index = pending_count ? pending[pending_head].op_index : completed;
            fprintf(stderr, "worker %d response timeout at op=%u key_id=%u\n",
                    w->tid, op_index, key_id);
            w->fail += w->cfg.ops - completed;
            goto worker_done;
        }

        pending_req_t done_req = pending[pending_head];
        pending_head = (pending_head + 1) % pipeline;
        pending_count--;
        if (resp.status != VEMB_V16_STATUS_OK) {
            fprintf(stderr, "worker %d response error at op=%u status=%u key_id=%u\n",
                    w->tid, done_req.op_index, resp.status, done_req.key_id);
            w->fail++;
            completed++;
            continue;
        }
        if (w->cfg.mode == MODE_VEMB_READ_VECTOR) {
            bench_region_map_t *warm_region = find_warm_region(w, resp.region_id);
            if (!warm_region ||
                resp.vector_offset + resp.vector_bytes >
                    warm_region->region_bytes ||
                resp.vector_bytes != warm_region->value_size) {
                fprintf(stderr, "worker %d invalid vector handle at op=%u region=%u offset=%llu bytes=%u\n",
                        w->tid,
                        done_req.op_index,
                        resp.region_id,
                        (unsigned long long)resp.vector_offset,
                        resp.vector_bytes);
                w->fail += w->cfg.ops - completed;
                goto worker_done;
            }
            volatile const uint8_t *p =
                warm_region->mapped_addr + resp.vector_offset;
            uint8_t checksum = 0;
            for (uint32_t j = 0; j < resp.vector_bytes; j += 64)
                checksum ^= p[j];
            w->read_bytes += resp.vector_bytes + checksum * 0u;
        } else if (w->cfg.mode == MODE_VEMB_INLINE_VECTOR) {
            if (inline_vector_bytes != resp.vector_bytes ||
                resp.vector_bytes != inline_vector_cap) {
                fprintf(stderr, "worker %d invalid inline vector at op=%u bytes=%u expected=%u resp_bytes=%u\n",
                        w->tid,
                        done_req.op_index,
                        inline_vector_bytes,
                        inline_vector_cap,
                        resp.vector_bytes);
                w->fail += w->cfg.ops - completed;
                goto worker_done;
            }
            uint8_t checksum = 0;
            for (uint32_t j = 0; j < inline_vector_bytes; j += 64)
                checksum ^= inline_vector[j];
            w->read_bytes += inline_vector_bytes + checksum * 0u;
        }
        w->ok++;
        completed++;
    }
worker_done:
    w->ns = now_ns() - start;
    zfree(inline_vector);
    zfree(pending);
    atomic_store_explicit(&w->done, 1, memory_order_release);
    return NULL;
}

static int mode_from_string(const char *s) {
    if (!strcmp(s, "ping")) return MODE_PING;
    if (!strcmp(s, "vemb-handle")) return MODE_VEMB_HANDLE;
    if (!strcmp(s, "vemb-read-vector")) return MODE_VEMB_READ_VECTOR;
    if (!strcmp(s, "vemb-inline-vector")) return MODE_VEMB_INLINE_VECTOR;
    if (!strcmp(s, "vemb-supernode-read")) return MODE_VEMB_SUPERNODE_READ;
    if (!strcmp(s, "vadd-inline")) return MODE_VADD_INLINE;
    if (!strcmp(s, "mixed-80r20w")) return MODE_MIXED_80R20W;
    return -1;
}

static const char *mode_name(int mode) {
    switch (mode) {
    case MODE_PING: return "ping";
    case MODE_VEMB_HANDLE: return "vemb-handle";
    case MODE_VEMB_READ_VECTOR: return "vemb-read-vector";
    case MODE_VEMB_INLINE_VECTOR: return "vemb-inline-vector";
    case MODE_VEMB_SUPERNODE_READ: return "vemb-supernode-read";
    case MODE_VADD_INLINE: return "vadd-inline";
    case MODE_MIXED_80R20W: return "mixed-80r20w";
    default: return "unknown";
    }
}

static int append_thread_count(int **threads,
                               int *count,
                               int *capacity,
                               int value) {
    if (*count == *capacity) {
        int next_capacity = *capacity ? *capacity * 2 : 8;
        if (next_capacity < *capacity ||
            (size_t)next_capacity > SIZE_MAX / sizeof(**threads))
            return -1;
        int *next = zrealloc(*threads, sizeof(**threads) * (size_t)next_capacity);
        if (!next)
            return -1;
        *threads = next;
        *capacity = next_capacity;
    }
    (*threads)[(*count)++] = value;
    return 0;
}

static int parse_thread_list(const bench_cfg_t *cfg, int **threads_out) {
    int *threads = NULL;
    int count = 0;
    int capacity = 0;
    if (!cfg->threads_arg) {
        if (cfg->threads <= 0)
            return -1;
        if (append_thread_count(&threads, &count, &capacity, cfg->threads) != 0)
            return -1;
        *threads_out = threads;
        return count;
    }
    const char *p = cfg->threads_arg;
    while (*p) {
        char *end = NULL;
        errno = 0;
        long v = strtol(p, &end, 10);
        if (end == p || errno == ERANGE || v <= 0 || v > INT_MAX) {
            zfree(threads);
            return -1;
        }
        if (append_thread_count(&threads, &count, &capacity, (int)v) != 0) {
            zfree(threads);
            return -1;
        }
        if (*end == ',') {
            p = end + 1;
        } else if (*end == '\0') {
            break;
        } else {
            zfree(threads);
            return -1;
        }
    }
    if (count <= 0) {
        zfree(threads);
        return -1;
    }
    *threads_out = threads;
    return count;
}

static int parse_socket_list(bench_cfg_t *cfg, const char *arg) {
    if (!cfg || !arg || !arg[0])
        return -1;
    cfg->node_count = 0;
    const char *p = arg;
    while (*p) {
        if (cfg->node_count >= VEMB_V16_BENCH_MAX_NODES)
            return -1;
        const char *comma = strchr(p, ',');
        size_t len = comma ? (size_t)(comma - p) : strlen(p);
        if (len == 0 || len >= VEMB_V16_BENCH_PATH_MAX)
            return -1;
        memcpy(cfg->socket_paths[cfg->node_count], p, len);
        cfg->socket_paths[cfg->node_count][len] = '\0';
        cfg->node_count++;
        if (!comma) break;
        p = comma + 1;
    }
    if (cfg->node_count == 0)
        return -1;
    cfg->socket_path = cfg->socket_paths[0];
    return build_hash_ring(cfg);
}

static void print_stats_delta(const vemb_v16_stats_t *before,
                              const vemb_v16_stats_t *after) {
#define D(field) (unsigned long long)(after->field - before->field)
    printf("[stats] total=%llu vadd=%llu vemb=%llu not_found=%llu published=%llu completed=%llu active_channels=%llu\n",
           D(total_requests), D(vadd_requests), D(vemb_requests),
           D(not_found), D(published_jobs), D(completed_jobs),
           (unsigned long long)after->active_channels);
    printf("[stats] proxy request_poll=%llu completion_poll=%llu vemb_publish=%llu vadd_publish=%llu response_publish=%llu\n",
           D(proxy_request_poll), D(proxy_completion_poll),
           D(proxy_vemb_publish), D(proxy_vadd_publish),
           D(proxy_response_publish));
    printf("[stats] full vemb_ring=%llu vadd_ring=%llu response_ring=%llu completion_ring=%llu\n",
           D(proxy_vemb_ring_full), D(proxy_vadd_ring_full),
           D(proxy_response_ring_full), D(supernode_completion_ring_full));
    printf("[stats] supernode vemb_poll=%llu vadd_poll=%llu completion_publish=%llu\n",
           D(supernode_vemb_poll), D(supernode_vadd_poll),
           D(supernode_completion_publish));
    printf("[stats] bitmap lock_success=%llu lock_failure=%llu\n",
           D(bitmap_lock_success), D(bitmap_lock_failure));
    printf("[stats] depth request=%llu response=%llu vemb_shard=%llu vadd_shard=%llu completion=%llu channel_ops=%llu\n",
           (unsigned long long)after->request_ring_depth,
           (unsigned long long)after->response_ring_depth,
           (unsigned long long)after->vemb_shard_queue_depth,
           (unsigned long long)after->vadd_shard_queue_depth,
           (unsigned long long)after->completion_ring_depth,
           D(channel_ops));
    uint64_t samples = after->sample_count - before->sample_count;
    if (samples) {
        printf("[stats] samples=%llu table_lookup_avg_ns=%.1f bitmap_lock_avg_ns=%.1f bitmap_unlock_avg_ns=%.1f vector_load_avg_ns=%.1f completion_publish_avg_ns=%.1f\n",
               (unsigned long long)samples,
               (double)(after->sample_table_lookup_ns -
                        before->sample_table_lookup_ns) / (double)samples,
               (double)(after->sample_bitmap_lock_ns -
                        before->sample_bitmap_lock_ns) / (double)samples,
               (double)(after->sample_bitmap_unlock_ns -
                        before->sample_bitmap_unlock_ns) / (double)samples,
               (double)(after->sample_vector_load_ns -
                        before->sample_vector_load_ns) / (double)samples,
               (double)(after->sample_completion_publish_ns -
                        before->sample_completion_publish_ns) / (double)samples);
    }
#undef D
}

static void print_stats_delta_node(uint32_t node_index,
                                   const vemb_v16_stats_t *before,
                                   const vemb_v16_stats_t *after) {
    printf("[stats node=%u]\n", node_index);
    print_stats_delta(before, after);
}

static int fetch_stats_for_node(const bench_cfg_t *cfg,
                                uint32_t node_index,
                                vemb_v16_stats_t *stats) {
    if (cfg->transport_type == VEMB_V16_TRANSPORT_TCP)
        return fetch_stats_tcp(cfg, stats);
    return fetch_stats(cfg->socket_paths[node_index], stats);
}

static int close_all_for_node(const bench_cfg_t *cfg,
                              uint32_t node_index,
                              uint64_t *closed) {
    if (cfg->transport_type == VEMB_V16_TRANSPORT_TCP)
        return close_all_channels_tcp(cfg, closed);
    return close_all_channels(cfg->socket_paths[node_index], closed);
}

static void close_node_channel(const char *socket_path,
                               bench_node_channel_t *node) {
    if (!node) return;
    if (node->transport_type == VEMB_V16_TRANSPORT_TCP) {
        if (node->net_fd >= 0) {
            vemb_v16_net_write_frame(node->net_fd,
                                     VEMB_V16_NET_CLOSE,
                                     0,
                                     node->desc.channel_id,
                                     0,
                                     NULL,
                                     0);
            close(node->net_fd);
        }
        if (node->desc.channel_id)
            close_channel_tcp(node->tcp_host,
                              node->tcp_port,
                              node->timeout_ms,
                              node->desc.channel_id);
    } else if (node->desc.channel_id) {
        close_channel_path(socket_path, node->desc.channel_id);
    }
    if (node->req_ring) {
        munmap(node->req_ring,
               vemb_v16_client_ring_bytes(node->desc.request_ring_slot_size));
    }
    if (node->resp_ring) {
        munmap(node->resp_ring,
               vemb_v16_client_ring_bytes(node->desc.response_ring_slot_size));
    }
    close_warm_region(&node->warm_region);
    memset(node, 0, sizeof(*node));
    node->net_fd = -1;
}

static int setup_node_channel(const bench_cfg_t *cfg,
                              uint32_t node_index,
                              int open_region,
                              bench_node_channel_t *node) {
    if (node_index >= cfg->node_count)
        return -1;
    memset(node, 0, sizeof(*node));
    node->net_fd = -1;
    node->transport_type = cfg->transport_type;
    node->tcp_host = cfg->tcp_host;
    node->tcp_port = cfg->tcp_port;
    node->timeout_ms = cfg->timeout_ms;
    if (cfg->transport_type == VEMB_V16_TRANSPORT_TCP) {
        if (alloc_tcp_channel(cfg, &node->desc, &node->net_fd) != 0) {
            close_node_channel(cfg->socket_paths[node_index], node);
            return -1;
        }
        return 0;
    }
    const char *socket_path = cfg->socket_paths[node_index];
    if (alloc_channel_path(socket_path, cfg, &node->desc) != 0 ||
        open_ring(node->desc.request_ring_name,
                  node->desc.request_ring_slot_size,
                  &node->req_ring) != 0 ||
        open_ring(node->desc.response_ring_name,
                  node->desc.response_ring_slot_size,
                  &node->resp_ring) != 0) {
        close_node_channel(socket_path, node);
        return -1;
    }
    if (open_region && open_warm_region(&node->desc, &node->warm_region) != 0) {
        close_node_channel(socket_path, node);
        return -1;
    }
    return 0;
}

static int run_once(bench_cfg_t cfg) {
    bench_node_channel_t pre_nodes[VEMB_V16_BENCH_MAX_NODES];
    memset(pre_nodes, 0, sizeof(pre_nodes));
    if (cfg.transport_type == VEMB_V16_TRANSPORT_TCP &&
        cfg.mode == MODE_VEMB_READ_VECTOR) {
        fprintf(stderr, "vemb-read-vector requires shm vector-region mmap; use vemb-handle or vemb-supernode-read with --transport tcp\n");
        return 1;
    }
    if (cfg.transport_type != VEMB_V16_TRANSPORT_TCP &&
        cfg.mode == MODE_VEMB_INLINE_VECTOR) {
        fprintf(stderr, "vemb-inline-vector requires --transport tcp\n");
        return 1;
    }
    printf("[setup] transport=%s mode=%s dim=%u prefill=%u ops/thread=%u threads=%d pipeline=%u pin=%s\n",
           vemb_v16_transport_name(cfg.transport_type),
           mode_name(cfg.mode), cfg.dim, cfg.prefill, cfg.ops, cfg.threads,
           cfg.pipeline, cfg.pin_threads ? "yes" : "no");
    if (cfg.prefill && cfg.mode != MODE_PING) {
        for (uint32_t n = 0; n < cfg.node_count; n++) {
            if (setup_node_channel(&cfg, n, 0, &pre_nodes[n]) != 0) {
                fprintf(stderr, "failed to setup prefill channel node=%u\n", n);
                for (uint32_t c = 0; c < cfg.node_count; c++)
                    close_node_channel(cfg.socket_paths[c], &pre_nodes[c]);
                return 1;
            }
        }
        if (prefill_multi(&cfg, pre_nodes, cfg.node_count) != 0) {
            fprintf(stderr, "prefill failed\n");
            for (uint32_t n = 0; n < cfg.node_count; n++)
                close_node_channel(cfg.socket_paths[n], &pre_nodes[n]);
            return 1;
        }
    }
    for (uint32_t n = 0; n < cfg.node_count; n++)
        close_node_channel(cfg.socket_paths[n], &pre_nodes[n]);
    printf("[run] preparing mode=%s threads=%d ops/thread=%u timeout_ms=%u\n",
           mode_name(cfg.mode), cfg.threads, cfg.ops, cfg.timeout_ms);
    fflush(stdout);

    vemb_v16_stats_t before[VEMB_V16_BENCH_MAX_NODES];
    vemb_v16_stats_t after[VEMB_V16_BENCH_MAX_NODES];
    memset(before, 0, sizeof(before));
    memset(after, 0, sizeof(after));
    for (uint32_t n = 0; n < cfg.node_count; n++) {
        if (fetch_stats_for_node(&cfg, n, &before[n]) != 0)
            fprintf(stderr, "warning: fetch stats before run failed for node=%u\n", n);
    }

    worker_arg_t *args = zcalloc_num((size_t)cfg.threads, sizeof(*args));
    pthread_t *threads = zcalloc_num((size_t)cfg.threads, sizeof(*threads));
    if (!args || !threads) return 1;

    for (int i = 0; i < cfg.threads; i++) {
        args[i].tid = i;
        args[i].cfg = cfg;
        args[i].node_count = cfg.node_count;
        atomic_init(&args[i].stop, 0);
        atomic_init(&args[i].done, 0);
        for (uint32_t n = 0; n < cfg.node_count; n++) {
            if (setup_node_channel(&cfg,
                                   n,
                                   cfg.mode == MODE_VEMB_READ_VECTOR,
                                   &args[i].nodes[n]) != 0) {
                fprintf(stderr, "worker %d channel setup failed node=%u\n", i, n);
                return 1;
            }
        }
    }

    uint64_t start = now_ns();
    printf("[run] mode=%s threads=%d requests=%llu\n",
           mode_name(cfg.mode), cfg.threads,
           (unsigned long long)cfg.ops * (unsigned long long)cfg.threads);
    fflush(stdout);
    for (int i = 0; i < cfg.threads; i++)
        pthread_create(&threads[i], NULL, worker_main, &args[i]);
    uint64_t join_start = now_ns();
    int timed_out = 0;
    for (;;) {
        int done = 0;
        for (int i = 0; i < cfg.threads; i++)
            done += atomic_load_explicit(&args[i].done, memory_order_acquire);
        if (done == cfg.threads) break;
        if (wait_timed_out(join_start, cfg.timeout_ms ? cfg.timeout_ms : 0)) {
            fprintf(stderr, "run timeout: done_workers=%d/%d timeout_ms=%u\n",
                    done, cfg.threads, cfg.timeout_ms);
            for (uint32_t n = 0; n < cfg.node_count; n++) {
                if (fetch_stats_for_node(&cfg, n, &after[n]) == 0)
                    print_stats_delta_node(n, &before[n], &after[n]);
            }
            timed_out = 1;
            for (int i = 0; i < cfg.threads; i++)
                atomic_store_explicit(&args[i].stop, 1, memory_order_release);
            break;
        }
        struct timespec ts = {0, 1000000};
        nanosleep(&ts, NULL);
    }
    for (int i = 0; i < cfg.threads; i++)
        pthread_join(threads[i], NULL);
    uint64_t wall = now_ns() - start;

    uint64_t ok = 0, fail = 0, read_bytes = 0, vemb_sent = 0, vadd_sent = 0;
    uint64_t request_publish_spins = 0, response_empty_polls = 0;
    uint64_t max_ns = 0;
    for (int i = 0; i < cfg.threads; i++) {
        ok += args[i].ok;
        fail += args[i].fail;
        read_bytes += args[i].read_bytes;
        vemb_sent += args[i].vemb_sent;
        vadd_sent += args[i].vadd_sent;
        request_publish_spins += args[i].request_publish_spins;
        response_empty_polls += args[i].response_empty_polls;
        if (args[i].ns > max_ns) max_ns = args[i].ns;
    }
    uint64_t total_ops = ok + fail;
    double qps = (double)total_ops / ((double)wall / 1e9);
    double avg_ns = total_ops ? (double)max_ns / (double)total_ops : 0.0;
    printf("[done] mode=%s threads=%d ok=%llu fail=%llu qps=%.2f avg_thread_ns/op=%.1f read_bytes=%llu\n",
           mode_name(cfg.mode),
           cfg.threads,
           (unsigned long long)ok,
           (unsigned long long)fail,
           qps,
           avg_ns,
           (unsigned long long)read_bytes);
    printf("[client] request_publish_spins=%llu response_empty_polls=%llu\n",
           (unsigned long long)request_publish_spins,
           (unsigned long long)response_empty_polls);
    if (vemb_sent || vadd_sent) {
        printf("[client] sent_vemb=%llu sent_vadd=%llu write_ratio=%.2f%%\n",
               (unsigned long long)vemb_sent,
               (unsigned long long)vadd_sent,
               (double)vadd_sent * 100.0 / (double)(vemb_sent + vadd_sent));
    }
    for (uint32_t n = 0; n < cfg.node_count; n++) {
        if (fetch_stats_for_node(&cfg, n, &after[n]) == 0)
            print_stats_delta_node(n, &before[n], &after[n]);
        else
            fprintf(stderr, "warning: fetch stats after run failed for node=%u\n", n);
    }
    for (int i = 0; i < cfg.threads; i++) {
        for (uint32_t n = 0; n < cfg.node_count; n++)
            close_node_channel(cfg.socket_paths[n], &args[i].nodes[n]);
    }
    zfree(args);
    zfree(threads);
    return fail == 0 && !timed_out ? 0 : 1;
}

int main(int argc, char **argv) {
    bench_cfg_t cfg = {
        .socket_path = VEMB_V16_UDS_PATH,
        .tcp_host = VEMB_V16_TCP_HOST,
        .dim = VEMB_V16_DEFAULT_DIM,
        .prefill = 65536,
        .ops = 200000,
        .threads = 8,
        .mode = MODE_VEMB_HANDLE,
        .timeout_ms = 10000,
        .pipeline = 1,
        .transport_type = VEMB_V16_TRANSPORT_SHM,
        .tcp_port = VEMB_V16_TCP_PORT,
    };
    cfg.node_count = 1;
    strncpy(cfg.socket_paths[0], cfg.socket_path, sizeof(cfg.socket_paths[0]) - 1);
    build_hash_ring(&cfg);

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--socket") && i + 1 < argc) {
            cfg.socket_path = argv[++i];
            cfg.node_count = 1;
            strncpy(cfg.socket_paths[0], cfg.socket_path,
                    sizeof(cfg.socket_paths[0]) - 1);
            cfg.socket_paths[0][sizeof(cfg.socket_paths[0]) - 1] = '\0';
            build_hash_ring(&cfg);
        }
        else if (!strcmp(argv[i], "--sockets") && i + 1 < argc) {
            if (parse_socket_list(&cfg, argv[++i]) != 0) {
                fprintf(stderr, "invalid socket list\n");
                return 1;
            }
        }
        else if (!strcmp(argv[i], "--transport") && i + 1 < argc) {
            const char *transport = argv[++i];
            if (!strcmp(transport, "shm")) {
                cfg.transport_type = VEMB_V16_TRANSPORT_SHM;
            } else if (!strcmp(transport, "tcp")) {
                cfg.transport_type = VEMB_V16_TRANSPORT_TCP;
                cfg.node_count = 1;
            } else {
                fprintf(stderr, "invalid transport: %s\n", transport);
                return 1;
            }
        }
        else if ((!strcmp(argv[i], "--host") ||
                  !strcmp(argv[i], "--tcp-host")) && i + 1 < argc) {
            cfg.tcp_host = argv[++i];
        }
        else if ((!strcmp(argv[i], "--port") ||
                  !strcmp(argv[i], "--tcp-port")) && i + 1 < argc) {
            cfg.tcp_port = (uint16_t)strtoul(argv[++i], NULL, 10);
        }
        else if (!strcmp(argv[i], "--dim") && i + 1 < argc) cfg.dim = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--prefill") && i + 1 < argc) cfg.prefill = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--ops") && i + 1 < argc) cfg.ops = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--timeout-ms") && i + 1 < argc) cfg.timeout_ms = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--pipeline") && i + 1 < argc) cfg.pipeline = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--threads") && i + 1 < argc) {
            const char *arg = argv[++i];
            cfg.threads_arg = arg;
        }
        else if (!strcmp(argv[i], "--hot-key-id") && i + 1 < argc) {
            cfg.hot_key_enabled = 1;
            cfg.hot_key_id = (uint32_t)strtoul(argv[++i], NULL, 10);
        }
        else if (!strcmp(argv[i], "--pin")) {
            cfg.pin_threads = 1;
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                const char *v = argv[++i];
                cfg.pin_threads = !strcmp(v, "yes") || !strcmp(v, "1") ||
                                  !strcmp(v, "true") || !strcmp(v, "on");
            }
        }
        else if (!strcmp(argv[i], "--no-pin")) {
            cfg.pin_threads = 0;
        }
        else if (!strcmp(argv[i], "--mode") && i + 1 < argc) cfg.mode = mode_from_string(argv[++i]);
        else if (!strcmp(argv[i], "--help")) {
            printf("usage: %s [--transport shm|tcp] [--socket PATH | --sockets PATH[,PATH...]] [--host HOST] [--port PORT] [--dim N] [--prefill N] [--ops N] [--timeout-ms N] [--pipeline N] [--threads N[,N...]] [--pin [yes|no]] [--no-pin] [--hot-key-id N] [--mode ping|vemb-handle|vemb-read-vector|vemb-inline-vector|vemb-supernode-read|vadd-inline|mixed-80r20w]\n", argv[0]);
            return 0;
        }
    }
    if (cfg.transport_type == VEMB_V16_TRANSPORT_TCP)
        cfg.node_count = 1;
    g_control_timeout_ms = cfg.timeout_ms;
    for (uint32_t n = 0; n < cfg.node_count; n++) {
        uint64_t closed = 0;
        if (close_all_for_node(&cfg, n, &closed) == 0 && closed)
            printf("[setup] node=%u closed stale channels=%llu\n",
                   n, (unsigned long long)closed);
    }
    if (cfg.mode < 0 ||
        cfg.pipeline == 0 || cfg.pipeline > VEMB_V16_CLIENT_RING_SIZE ||
        cfg.node_count == 0 || cfg.node_count > VEMB_V16_BENCH_MAX_NODES) {
        fprintf(stderr, "invalid arguments\n");
        return 1;
    }

    int *thread_list = NULL;
    int thread_count = parse_thread_list(&cfg, &thread_list);
    if (thread_count <= 0) {
        fprintf(stderr, "invalid thread list\n");
        return 1;
    }
    int ret = 0;
    for (int i = 0; i < thread_count; i++) {
        bench_cfg_t run_cfg = cfg;
        run_cfg.threads = thread_list[i];
        ret = run_once(run_cfg);
        if (ret != 0) break;
    }
    zfree(thread_list);
    return ret;
}
