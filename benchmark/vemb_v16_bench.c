#define _GNU_SOURCE

#include "../src/vemb_v16_client_ring.h"
#include "../src/vemb_v16_protocol.h"

#include <errno.h>
#include <fcntl.h>
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

typedef struct bench_cfg {
    const char *socket_path;
    uint32_t dim;
    uint32_t prefill;
    uint32_t ops;
    int threads;
    char threads_arg[128];
    int mode;
    int hot_key_enabled;
    uint32_t hot_key_id;
    uint32_t timeout_ms;
    int pin_threads;
    uint32_t pipeline;
} bench_cfg_t;

typedef struct worker_arg {
    int tid;
    bench_cfg_t cfg;
    vemb_v16_channel_desc_t desc;
    vemb_v16_client_ring_t *req_ring;
    vemb_v16_client_ring_t *resp_ring;
    uint8_t *vector_region;
    uint64_t ok;
    uint64_t fail;
    uint64_t read_bytes;
    uint64_t request_publish_spins;
    uint64_t response_empty_polls;
    uint64_t ns;
    atomic_int done;
} worker_arg_t;

enum {
    MODE_PING = 0,
    MODE_VEMB_HANDLE = 1,
    MODE_VEMB_READ_VECTOR = 2,
    MODE_VADD_INLINE = 3,
    MODE_VEMB_SUPERNODE_READ = 4,
};

static uint32_t g_control_timeout_ms = 10000;

typedef struct pending_req {
    uint32_t op_index;
    uint32_t key_id;
} pending_req_t;

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

static uint8_t *open_vector_region(const vemb_v16_channel_desc_t *desc) {
    int fd = shm_open(desc->vector_region_name, O_RDONLY, 0666);
    if (fd < 0) return NULL;
    size_t size = (size_t)desc->vector_stride * desc->max_vectors;
    void *ptr = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) return NULL;
    return ptr;
}

static void fill_vector(float *vector, uint32_t dim, uint32_t seed) {
    for (uint32_t i = 0; i < dim; i++)
        vector[i] = (float)((seed + i) & 1023) / 1024.0f;
}

static void make_key(char *buf, size_t len, uint32_t id) {
    snprintf(buf, len, "item:%u", id);
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

static int recv_resp_batch(vemb_v16_client_ring_t *ring,
                           vemb_v16_resp_t *responses,
                           uint32_t max_count,
                           uint32_t *received,
                           uint64_t *empty_polls,
                           uint32_t timeout_ms) {
    uint64_t polls = 0;
    uint64_t start = now_ns();
    uint32_t got = 0;
    while ((got = vemb_v16_client_poll_batch(ring,
                                             responses,
                                             sizeof(*responses),
                                             max_count)) == 0) {
        polls++;
        if ((polls & 0xfffu) == 0 && wait_timed_out(start, timeout_ms)) {
            if (empty_polls) *empty_polls += polls;
            return -1;
        }
        __asm__ volatile("" ::: "memory");
    }
    if (empty_polls) *empty_polls += polls;
    *received = got;
    return 0;
}

static int prefill(const bench_cfg_t *cfg, const vemb_v16_channel_desc_t *desc,
                   vemb_v16_client_ring_t *req_ring,
                   vemb_v16_client_ring_t *resp_ring) {
    vemb_v16_req_t req;
    vemb_v16_resp_t resp;
    char key[VEMB_V16_MAX_KEY_LEN];
    uint64_t start = now_ns();
    for (uint32_t i = 0; i < cfg->prefill; i++) {
        make_key(key, sizeof(key), i);
        prepare_req(&req, VEMB_V16_OP_VADD_INLINE, i + 1, desc->channel_id,
                    key, cfg->dim);
        fill_vector(req.vector, cfg->dim, i);
        if (send_req(req_ring, &req, vemb_v16_req_inline_len(req.vector_bytes),
                     NULL, cfg->timeout_ms) != 0) {
            fprintf(stderr, "prefill send timeout at item=%u\n", i);
            return -1;
        }
        if (recv_resp(resp_ring, &resp, NULL, cfg->timeout_ms) != 0 ||
            resp.status != VEMB_V16_STATUS_OK) {
            fprintf(stderr, "prefill response timeout/error at item=%u status=%u\n",
                    i, resp.status);
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
    vemb_v16_resp_t *responses = calloc(pipeline, sizeof(*responses));
    pending_req_t *pending = calloc(pipeline, sizeof(*pending));
    if (!responses || !pending) {
        free(responses);
        free(pending);
        w->fail = w->cfg.ops;
        atomic_store_explicit(&w->done, 1, memory_order_release);
        return NULL;
    }
    char key[VEMB_V16_MAX_KEY_LEN];
    uint64_t start = now_ns();
    uint32_t sent = 0;
    uint32_t completed = 0;
    uint32_t pending_head = 0;
    uint32_t pending_tail = 0;
    uint32_t pending_count = 0;
    while (completed < w->cfg.ops) {
        while (sent < w->cfg.ops && pending_count < pipeline) {
            uint32_t i = sent;
            uint32_t global_id = (uint32_t)(i + (uint32_t)w->tid * w->cfg.ops);
            uint32_t key_id = w->cfg.prefill ? global_id % w->cfg.prefill : global_id;
            if (w->cfg.hot_key_enabled) key_id = w->cfg.hot_key_id;
            size_t req_len = vemb_v16_req_handle_len();
            int send_failed = 0;
            uint8_t op = w->cfg.mode == MODE_VEMB_SUPERNODE_READ ?
                VEMB_V16_OP_VEMB_SUPERNODE_READ : VEMB_V16_OP_VEMB_HANDLE;
            if (w->cfg.mode == MODE_PING) {
                memset(&req, 0, sizeof(req));
                req.op = VEMB_V16_OP_PING;
                req.req_id = i + 1;
                req.channel_id = w->desc.channel_id;
                if (send_req(w->req_ring, &req, req_len,
                             &w->request_publish_spins,
                             w->cfg.timeout_ms) != 0) {
                    fprintf(stderr, "worker %d request publish timeout at op=%u mode=ping\n",
                            w->tid, i);
                    send_failed = 1;
                }
            } else if (w->cfg.mode == MODE_VADD_INLINE) {
                make_key(key, sizeof(key), global_id + 100000000u);
                prepare_req(&req, VEMB_V16_OP_VADD_INLINE, i + 1,
                            w->desc.channel_id, key, w->cfg.dim);
                fill_vector(req.vector, w->cfg.dim, global_id);
                req_len = vemb_v16_req_inline_len(req.vector_bytes);
                if (send_req(w->req_ring, &req, req_len,
                             &w->request_publish_spins,
                             w->cfg.timeout_ms) != 0) {
                    fprintf(stderr, "worker %d request publish timeout at op=%u mode=vadd-inline\n",
                            w->tid, i);
                    send_failed = 1;
                }
            } else {
                make_key(key, sizeof(key), key_id);
                prepare_req(&req, op, i + 1, w->desc.channel_id, key,
                            w->cfg.dim);
                if (send_req(w->req_ring, &req, req_len,
                             &w->request_publish_spins,
                             w->cfg.timeout_ms) != 0) {
                    fprintf(stderr, "worker %d request publish timeout at op=%u key_id=%u\n",
                            w->tid, i, key_id);
                    send_failed = 1;
                }
            }
            if (send_failed) {
                w->fail += w->cfg.ops - completed;
                goto worker_done;
            }
            pending[pending_tail] = (pending_req_t){
                .op_index = i,
                .key_id = key_id,
            };
            pending_tail = (pending_tail + 1) % pipeline;
            pending_count++;
            sent++;
        }

        uint32_t got = 0;
        if (recv_resp_batch(w->resp_ring,
                            responses,
                            pipeline < pending_count ? pipeline : pending_count,
                            &got,
                            &w->response_empty_polls,
                            w->cfg.timeout_ms) != 0) {
            uint32_t key_id = pending_count ? pending[pending_head].key_id : 0;
            uint32_t op_index = pending_count ? pending[pending_head].op_index : completed;
            fprintf(stderr, "worker %d response timeout at op=%u key_id=%u\n",
                    w->tid, op_index, key_id);
            w->fail += w->cfg.ops - completed;
            goto worker_done;
        }

        for (uint32_t r = 0; r < got; r++) {
            pending_req_t done_req = pending[pending_head];
            pending_head = (pending_head + 1) % pipeline;
            pending_count--;
            vemb_v16_resp_t *resp = &responses[r];
            if (resp->status != VEMB_V16_STATUS_OK) {
                fprintf(stderr, "worker %d response error at op=%u status=%u key_id=%u\n",
                        w->tid, done_req.op_index, resp->status, done_req.key_id);
                w->fail += w->cfg.ops - completed;
                goto worker_done;
            }
            if (w->cfg.mode == MODE_VEMB_READ_VECTOR) {
                volatile const uint8_t *p = w->vector_region + resp->vector_offset;
                uint8_t checksum = 0;
                for (uint32_t j = 0; j < resp->vector_bytes; j += 64)
                    checksum ^= p[j];
                w->read_bytes += resp->vector_bytes + checksum * 0u;
            }
            w->ok++;
            completed++;
        }
    }
worker_done:
    w->ns = now_ns() - start;
    free(responses);
    free(pending);
    atomic_store_explicit(&w->done, 1, memory_order_release);
    return NULL;
}

static int mode_from_string(const char *s) {
    if (!strcmp(s, "ping")) return MODE_PING;
    if (!strcmp(s, "vemb-handle")) return MODE_VEMB_HANDLE;
    if (!strcmp(s, "vemb-read-vector")) return MODE_VEMB_READ_VECTOR;
    if (!strcmp(s, "vemb-supernode-read")) return MODE_VEMB_SUPERNODE_READ;
    if (!strcmp(s, "vadd-inline")) return MODE_VADD_INLINE;
    return -1;
}

static const char *mode_name(int mode) {
    switch (mode) {
    case MODE_PING: return "ping";
    case MODE_VEMB_HANDLE: return "vemb-handle";
    case MODE_VEMB_READ_VECTOR: return "vemb-read-vector";
    case MODE_VEMB_SUPERNODE_READ: return "vemb-supernode-read";
    case MODE_VADD_INLINE: return "vadd-inline";
    default: return "unknown";
    }
}

static int parse_thread_list(const bench_cfg_t *cfg, int *threads, int max_threads) {
    if (cfg->threads_arg[0] == '\0') {
        threads[0] = cfg->threads;
        return 1;
    }
    int count = 0;
    const char *p = cfg->threads_arg;
    while (*p && count < max_threads) {
        char *end = NULL;
        long v = strtol(p, &end, 10);
        if (end == p || v <= 0 || v > VEMB_V16_MAX_CHANNELS)
            return -1;
        threads[count++] = (int)v;
        if (*end == ',') {
            p = end + 1;
        } else if (*end == '\0') {
            break;
        } else {
            return -1;
        }
    }
    return count > 0 ? count : -1;
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
    printf("[stats] depth request=%llu response=%llu vemb_job=%llu vadd_job=%llu completion=%llu channel_ops=%llu\n",
           (unsigned long long)after->request_ring_depth,
           (unsigned long long)after->response_ring_depth,
           (unsigned long long)after->vemb_job_ring_depth,
           (unsigned long long)after->vadd_job_ring_depth,
           (unsigned long long)after->completion_ring_depth,
           (unsigned long long)after->channel_ops);
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

static int run_once(bench_cfg_t cfg) {
    vemb_v16_channel_desc_t pre_desc;
    if (alloc_channel(&cfg, &pre_desc) != 0) {
        fprintf(stderr, "failed to allocate prefill channel\n");
        return 1;
    }
    vemb_v16_client_ring_t *pre_req = NULL, *pre_resp = NULL;
    if (open_ring(pre_desc.request_ring_name,
                  pre_desc.request_ring_slot_size,
                  &pre_req) != 0 ||
        open_ring(pre_desc.response_ring_name,
                  pre_desc.response_ring_slot_size,
                  &pre_resp) != 0) {
        fprintf(stderr, "failed to open prefill rings\n");
        return 1;
    }
    printf("[setup] mode=%s dim=%u prefill=%u ops/thread=%u threads=%d pipeline=%u pin=%s\n",
           mode_name(cfg.mode), cfg.dim, cfg.prefill, cfg.ops, cfg.threads,
           cfg.pipeline, cfg.pin_threads ? "yes" : "no");
    if (cfg.prefill && cfg.mode != MODE_PING &&
        prefill(&cfg, &pre_desc, pre_req, pre_resp) != 0) {
        fprintf(stderr, "prefill failed\n");
        return 1;
    }
    close_channel(cfg.socket_path, pre_desc.channel_id);
    munmap(pre_req,
           vemb_v16_client_ring_bytes(pre_desc.request_ring_slot_size));
    munmap(pre_resp,
           vemb_v16_client_ring_bytes(pre_desc.response_ring_slot_size));
    pre_req = NULL;
    pre_resp = NULL;
    printf("[run] preparing mode=%s threads=%d ops/thread=%u timeout_ms=%u\n",
           mode_name(cfg.mode), cfg.threads, cfg.ops, cfg.timeout_ms);
    fflush(stdout);

    vemb_v16_stats_t before = {0};
    vemb_v16_stats_t after = {0};
    if (fetch_stats(cfg.socket_path, &before) != 0)
        fprintf(stderr, "warning: fetch stats before run failed\n");

    worker_arg_t *args = calloc((size_t)cfg.threads, sizeof(*args));
    pthread_t *threads = calloc((size_t)cfg.threads, sizeof(*threads));
    if (!args || !threads) return 1;

    for (int i = 0; i < cfg.threads; i++) {
        args[i].tid = i;
        args[i].cfg = cfg;
        atomic_init(&args[i].done, 0);
        if (alloc_channel(&cfg, &args[i].desc) != 0 ||
            open_ring(args[i].desc.request_ring_name,
                      args[i].desc.request_ring_slot_size,
                      &args[i].req_ring) != 0 ||
            open_ring(args[i].desc.response_ring_name,
                      args[i].desc.response_ring_slot_size,
                      &args[i].resp_ring) != 0) {
            fprintf(stderr, "worker %d channel setup failed\n", i);
            return 1;
        }
        if (cfg.mode == MODE_VEMB_READ_VECTOR) {
            args[i].vector_region = open_vector_region(&args[i].desc);
            if (!args[i].vector_region) {
                fprintf(stderr, "worker %d vector region setup failed\n", i);
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
            if (fetch_stats(cfg.socket_path, &after) == 0)
                print_stats_delta(&before, &after);
            timed_out = 1;
            break;
        }
        struct timespec ts = {0, 1000000};
        nanosleep(&ts, NULL);
    }
    if (!timed_out) {
        for (int i = 0; i < cfg.threads; i++)
            pthread_join(threads[i], NULL);
    } else {
        for (int i = 0; i < cfg.threads; i++)
            close_channel(cfg.socket_path, args[i].desc.channel_id);
        return 1;
    }
    uint64_t wall = now_ns() - start;

    uint64_t ok = 0, fail = 0, read_bytes = 0;
    uint64_t request_publish_spins = 0, response_empty_polls = 0;
    uint64_t max_ns = 0;
    for (int i = 0; i < cfg.threads; i++) {
        ok += args[i].ok;
        fail += args[i].fail;
        read_bytes += args[i].read_bytes;
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
    for (int i = 0; i < cfg.threads; i++) {
        close_channel(cfg.socket_path, args[i].desc.channel_id);
        if (args[i].req_ring) {
            munmap(args[i].req_ring,
                   vemb_v16_client_ring_bytes(
                       args[i].desc.request_ring_slot_size));
        }
        if (args[i].resp_ring) {
            munmap(args[i].resp_ring,
                   vemb_v16_client_ring_bytes(
                       args[i].desc.response_ring_slot_size));
        }
        if (args[i].vector_region) {
            munmap(args[i].vector_region,
                   (size_t)args[i].desc.vector_stride * args[i].desc.max_vectors);
        }
    }
    if (fetch_stats(cfg.socket_path, &after) == 0)
        print_stats_delta(&before, &after);
    else
        fprintf(stderr, "warning: fetch stats after run failed\n");
    free(args);
    free(threads);
    return fail == 0 ? 0 : 1;
}

int main(int argc, char **argv) {
    bench_cfg_t cfg = {
        .socket_path = VEMB_V16_UDS_PATH,
        .dim = VEMB_V16_DEFAULT_DIM,
        .prefill = 65536,
        .ops = 200000,
        .threads = 8,
        .mode = MODE_VEMB_HANDLE,
        .timeout_ms = 10000,
        .pipeline = 1,
    };

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--socket") && i + 1 < argc) cfg.socket_path = argv[++i];
        else if (!strcmp(argv[i], "--dim") && i + 1 < argc) cfg.dim = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--prefill") && i + 1 < argc) cfg.prefill = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--ops") && i + 1 < argc) cfg.ops = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--timeout-ms") && i + 1 < argc) cfg.timeout_ms = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--pipeline") && i + 1 < argc) cfg.pipeline = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--threads") && i + 1 < argc) {
            const char *arg = argv[++i];
            if (strchr(arg, ',')) {
                strncpy(cfg.threads_arg, arg, sizeof(cfg.threads_arg) - 1);
            } else {
                cfg.threads = atoi(arg);
            }
        }
        else if (!strcmp(argv[i], "--hot-key-id") && i + 1 < argc) {
            cfg.hot_key_enabled = 1;
            cfg.hot_key_id = (uint32_t)strtoul(argv[++i], NULL, 10);
        }
        else if (!strcmp(argv[i], "--pin") && i + 1 < argc) {
            const char *v = argv[++i];
            cfg.pin_threads = !strcmp(v, "yes") || !strcmp(v, "1") ||
                              !strcmp(v, "true");
        }
        else if (!strcmp(argv[i], "--mode") && i + 1 < argc) cfg.mode = mode_from_string(argv[++i]);
        else if (!strcmp(argv[i], "--help")) {
            printf("usage: %s [--socket PATH] [--dim N] [--prefill N] [--ops N] [--timeout-ms N] [--pipeline N] [--threads N] [--pin yes|no] [--hot-key-id N] [--mode ping|vemb-handle|vemb-read-vector|vemb-supernode-read|vadd-inline]\n", argv[0]);
            return 0;
        }
    }
    g_control_timeout_ms = cfg.timeout_ms;
    uint64_t closed = 0;
    if (close_all_channels(cfg.socket_path, &closed) == 0 && closed)
        printf("[setup] closed stale channels=%llu\n", (unsigned long long)closed);
    if (cfg.mode < 0 || cfg.threads <= 0 || cfg.threads > VEMB_V16_MAX_CHANNELS ||
        cfg.pipeline == 0 || cfg.pipeline > VEMB_V16_CLIENT_RING_SIZE) {
        fprintf(stderr, "invalid arguments\n");
        return 1;
    }

    int thread_list[64];
    int thread_count = parse_thread_list(&cfg, thread_list, 64);
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
    return ret;
}
