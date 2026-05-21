#define _GNU_SOURCE

#include "vemb_v16_proxy.h"
#include "vemb_v16_aeron_ring.h"
#include "vemb_v16_client_ring.h"
#include "vemb_v16_dataplane.h"
#include "vemb_v16_log.h"
#include "vemb_v16_supernode.h"
#include "vemb_v16_table.h"
#include "zmalloc.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define VEMB_V16_VEMB_JOB_RING_SIZE VEMB_V16_AERON_RING_SIZE
#define VEMB_V16_VADD_JOB_RING_SIZE 256u
#define VEMB_V16_COMPLETION_RING_SIZE VEMB_V16_AERON_RING_SIZE
#define VEMB_V16_PROXY_BATCH 32u

typedef struct vemb_v16_channel {
    uint64_t channel_id;
    uint32_t index;
    atomic_int active;
    char request_ring_name[64];
    char response_ring_name[64];
    vemb_v16_client_ring_t *request_ring;
    vemb_v16_client_ring_t *response_ring;
    size_t request_ring_bytes;
    size_t response_ring_bytes;
    pthread_t proxy_thread;
    pthread_t supernode_thread;
    vemb_v16_aeron_ring_t vemb_job_ring;
    vemb_v16_aeron_ring_t vadd_job_ring;
    vemb_v16_aeron_ring_t completion_ring;
    void *vemb_job_slots;
    void *vadd_job_slots;
    void *completion_slots;
    vemb_v16_supernode_ctx_t supernode_ctx;
    struct vemb_v16_proxy *proxy;
    vemb_v16_channel_counters_t stats;
} vemb_v16_channel_t;

struct vemb_v16_proxy {
    char uds_path[108];
    uint32_t vector_dim;
    uint32_t vector_stride;
    uint32_t request_ring_slot_size;
    uint32_t response_ring_slot_size;
    uint32_t max_vectors;
    uint8_t *vector_region;
    size_t vector_region_size;
    int vector_region_fd;
    char vector_region_name[64];
    vemb_v16_table_t *table;
    vemb_v16_channel_t channels[VEMB_V16_MAX_CHANNELS];
    atomic_uint_fast64_t next_channel_id;
    atomic_uint_fast32_t next_channel_index;
    atomic_int running;
    int uds_fd;
    pthread_mutex_t stats_lock;
    vemb_v16_stats_t closed_stats;
};

static inline void vemb_v16_cpu_relax(void) {
#if defined(__x86_64__)
    __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__)
    __asm__ volatile("yield" ::: "memory");
#else
    __asm__ volatile("" ::: "memory");
#endif
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

static int create_shared_ring(const char *name,
                              uint32_t slot_size,
                              vemb_v16_client_ring_t **ring,
                              size_t *ring_bytes) {
    shm_unlink(name);
    size_t bytes = vemb_v16_client_ring_bytes(slot_size);
    int fd = shm_open(name, O_CREAT | O_RDWR, 0666);
    if (fd < 0) return -1;
    if (ftruncate(fd, (off_t)bytes) != 0) {
        close(fd);
        shm_unlink(name);
        return -1;
    }
    void *ptr = mmap(NULL, bytes, PROT_READ | PROT_WRITE,
                     MAP_SHARED, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) {
        shm_unlink(name);
        return -1;
    }
    memset(ptr, 0, bytes);
    vemb_v16_client_ring_init(ptr, slot_size);
    *ring = ptr;
    if (ring_bytes) *ring_bytes = bytes;
    return 0;
}

static void destroy_shared_ring(const char *name,
                                vemb_v16_client_ring_t *ring,
                                size_t ring_bytes) {
    if (ring) munmap(ring, ring_bytes);
    if (name && name[0]) shm_unlink(name);
}

static uint64_t counter_load(atomic_uint_fast64_t *counter) {
    return atomic_load_explicit(counter, memory_order_relaxed);
}

static void stats_add_channel_counters(vemb_v16_stats_t *dst,
                                       vemb_v16_channel_counters_t *src) {
    dst->total_requests += counter_load(&src->total_requests);
    dst->vadd_requests += counter_load(&src->vadd_requests);
    dst->vemb_requests += counter_load(&src->vemb_requests);
    dst->not_found += counter_load(&src->not_found);
    dst->published_jobs += counter_load(&src->published_jobs);
    dst->completed_jobs += counter_load(&src->completed_jobs);
    dst->proxy_request_poll += counter_load(&src->proxy_request_poll);
    dst->proxy_completion_poll += counter_load(&src->proxy_completion_poll);
    dst->proxy_vemb_publish += counter_load(&src->proxy_vemb_publish);
    dst->proxy_vadd_publish += counter_load(&src->proxy_vadd_publish);
    dst->proxy_vemb_ring_full += counter_load(&src->proxy_vemb_ring_full);
    dst->proxy_vadd_ring_full += counter_load(&src->proxy_vadd_ring_full);
    dst->proxy_response_publish += counter_load(&src->proxy_response_publish);
    dst->proxy_response_ring_full += counter_load(&src->proxy_response_ring_full);
    dst->supernode_vemb_poll += counter_load(&src->supernode_vemb_poll);
    dst->supernode_vadd_poll += counter_load(&src->supernode_vadd_poll);
    dst->supernode_completion_publish += counter_load(&src->supernode_completion_publish);
    dst->supernode_completion_ring_full += counter_load(&src->supernode_completion_ring_full);
    dst->sample_count += counter_load(&src->sample_count);
    dst->sample_table_lookup_ns += counter_load(&src->sample_table_lookup_ns);
    dst->sample_bitmap_lock_ns += counter_load(&src->sample_bitmap_lock_ns);
    dst->sample_bitmap_unlock_ns += counter_load(&src->sample_bitmap_unlock_ns);
    dst->sample_vector_load_ns += counter_load(&src->sample_vector_load_ns);
    dst->sample_completion_publish_ns += counter_load(&src->sample_completion_publish_ns);
}

static void stats_add(vemb_v16_stats_t *dst, const vemb_v16_stats_t *src) {
    dst->total_requests += src->total_requests;
    dst->vadd_requests += src->vadd_requests;
    dst->vemb_requests += src->vemb_requests;
    dst->not_found += src->not_found;
    dst->published_jobs += src->published_jobs;
    dst->completed_jobs += src->completed_jobs;
    dst->proxy_request_poll += src->proxy_request_poll;
    dst->proxy_completion_poll += src->proxy_completion_poll;
    dst->proxy_vemb_publish += src->proxy_vemb_publish;
    dst->proxy_vadd_publish += src->proxy_vadd_publish;
    dst->proxy_vemb_ring_full += src->proxy_vemb_ring_full;
    dst->proxy_vadd_ring_full += src->proxy_vadd_ring_full;
    dst->proxy_response_publish += src->proxy_response_publish;
    dst->proxy_response_ring_full += src->proxy_response_ring_full;
    dst->supernode_vemb_poll += src->supernode_vemb_poll;
    dst->supernode_vadd_poll += src->supernode_vadd_poll;
    dst->supernode_completion_publish += src->supernode_completion_publish;
    dst->supernode_completion_ring_full += src->supernode_completion_ring_full;
    dst->sample_count += src->sample_count;
    dst->sample_table_lookup_ns += src->sample_table_lookup_ns;
    dst->sample_bitmap_lock_ns += src->sample_bitmap_lock_ns;
    dst->sample_bitmap_unlock_ns += src->sample_bitmap_unlock_ns;
    dst->sample_vector_load_ns += src->sample_vector_load_ns;
    dst->sample_completion_publish_ns += src->sample_completion_publish_ns;
}

static void vemb_v16_channel_free_slots(vemb_v16_channel_t *ch) {
    if (!ch) return;
    free(ch->vemb_job_slots);
    free(ch->vadd_job_slots);
    free(ch->completion_slots);
    ch->vemb_job_slots = NULL;
    ch->vadd_job_slots = NULL;
    ch->completion_slots = NULL;
}

static void publish_response(vemb_v16_channel_t *ch,
                             const vemb_v16_completion_t *completion) {
    vemb_v16_resp_t resp = {
        .status = completion->status,
        .op = completion->op,
        .flags = completion->flags,
        .req_id = completion->req_id,
        .key_hash = completion->key_hash,
        .vector_offset = completion->vector_offset,
        .vector_bytes = completion->vector_bytes,
        .dim = completion->dim,
    };
    while (vemb_v16_client_publish(ch->response_ring, &resp, sizeof(resp)) != 0 &&
           atomic_load_explicit(&ch->proxy->running, memory_order_relaxed) &&
           atomic_load_explicit(&ch->active, memory_order_acquire)) {
        atomic_fetch_add_explicit(&ch->stats.proxy_response_ring_full, 1,
                                  memory_order_relaxed);
        vemb_v16_cpu_relax();
    }
    atomic_fetch_add_explicit(&ch->stats.proxy_response_publish, 1,
                              memory_order_relaxed);
}

static void handle_request(vemb_v16_channel_t *ch, const vemb_v16_req_t *req, int req_len) {
    vemb_v16_proxy_t *proxy = ch->proxy;

    if (req->op == VEMB_V16_OP_PING) {
        vemb_v16_completion_t completion = {
            .status = VEMB_V16_STATUS_OK,
            .op = req->op,
            .req_id = req->req_id,
            .channel_index = ch->index,
            .channel_id = ch->channel_id,
        };
        publish_response(ch, &completion);
        return;
    }

    uint32_t key_len = req->key_len;
    if (key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN ||
        req->channel_id != ch->channel_id) {
        vemb_v16_completion_t completion = {
            .status = VEMB_V16_STATUS_ERR,
            .op = req->op,
            .req_id = req->req_id,
            .channel_index = ch->index,
            .channel_id = ch->channel_id,
        };
        publish_response(ch, &completion);
        return;
    }

    size_t min_len = req->op == VEMB_V16_OP_VADD_INLINE ?
        vemb_v16_req_inline_len(req->vector_bytes) : vemb_v16_req_handle_len();
    if ((size_t)req_len < min_len || req->dim > VEMB_V16_MAX_DIM ||
        req->vector_bytes > sizeof(req->vector)) {
        vemb_v16_completion_t completion = {
            .status = VEMB_V16_STATUS_ERR,
            .op = req->op,
            .req_id = req->req_id,
            .channel_index = ch->index,
            .channel_id = ch->channel_id,
        };
        publish_response(ch, &completion);
        return;
    }

    if (req->op == VEMB_V16_OP_VADD_INLINE) {
        vemb_v16_vadd_job_t job = {
            .base = {
                .op = req->op,
                .flags = req->flags,
                .req_id = req->req_id,
                .channel_index = ch->index,
                .key_len = key_len,
                .channel_id = ch->channel_id,
                .key_hash = req->key_hash,
                .dim = req->dim,
                .vector_bytes = req->vector_bytes,
            },
        };
        memcpy(job.base.key, req->key, key_len);
        memcpy(job.vector, req->vector, req->vector_bytes);
        while (vemb_v16_aeron_publish(&ch->vadd_job_ring, &job) != 0 &&
               atomic_load_explicit(&proxy->running, memory_order_relaxed) &&
               atomic_load_explicit(&ch->active, memory_order_acquire)) {
            atomic_fetch_add_explicit(&ch->stats.proxy_vadd_ring_full, 1,
                                      memory_order_relaxed);
            vemb_v16_cpu_relax();
        }
        atomic_fetch_add_explicit(&ch->stats.proxy_vadd_publish, 1,
                                  memory_order_relaxed);
    } else if (req->op == VEMB_V16_OP_VEMB_HANDLE ||
               req->op == VEMB_V16_OP_VEMB_SUPERNODE_READ) {
        vemb_v16_vemb_job_t job = {
            .base = {
                .op = req->op,
                .flags = req->flags,
                .req_id = req->req_id,
                .channel_index = ch->index,
                .key_len = key_len,
                .channel_id = ch->channel_id,
                .key_hash = req->key_hash,
                .dim = req->dim,
                .vector_bytes = req->vector_bytes,
            },
        };
        memcpy(job.base.key, req->key, key_len);
        while (vemb_v16_aeron_publish(&ch->vemb_job_ring, &job) != 0 &&
               atomic_load_explicit(&proxy->running, memory_order_relaxed) &&
               atomic_load_explicit(&ch->active, memory_order_acquire)) {
            atomic_fetch_add_explicit(&ch->stats.proxy_vemb_ring_full, 1,
                                      memory_order_relaxed);
            vemb_v16_cpu_relax();
        }
        atomic_fetch_add_explicit(&ch->stats.proxy_vemb_publish, 1,
                                  memory_order_relaxed);
    } else {
        vemb_v16_completion_t completion = {
            .status = VEMB_V16_STATUS_ERR,
            .op = req->op,
            .req_id = req->req_id,
            .channel_index = ch->index,
            .channel_id = ch->channel_id,
        };
        publish_response(ch, &completion);
        return;
    }
    atomic_fetch_add_explicit(&ch->stats.published_jobs, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&ch->stats.total_requests, 1, memory_order_relaxed);
}

static void drain_completions(vemb_v16_channel_t *ch) {
    vemb_v16_completion_t completions[VEMB_V16_PROXY_BATCH];
    uint32_t n;
    while ((n = vemb_v16_aeron_poll_batch(&ch->completion_ring,
                                          completions,
                                          VEMB_V16_PROXY_BATCH)) != 0) {
        atomic_fetch_add_explicit(&ch->stats.proxy_completion_poll, n,
                                  memory_order_relaxed);
        for (uint32_t i = 0; i < n; i++) {
            if (completions[i].channel_id == ch->channel_id &&
                atomic_load_explicit(&ch->active, memory_order_acquire)) {
                publish_response(ch, &completions[i]);
            }
        }
    }
}

static void *channel_thread_main(void *arg) {
    vemb_v16_channel_t *ch = arg;
    vemb_v16_proxy_t *proxy = ch->proxy;
    vemb_v16_req_t *req_buf = zmalloc(sizeof(*req_buf) * VEMB_V16_PROXY_BATCH);
    if (!req_buf) return NULL;

#ifdef __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET((int)((ch->index * 2 + 1) % 64), &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
#endif

    serverLog(LL_VERBOSE, "vemb_v16 proxy channel thread started: index=%u channel_id=%llu",
              ch->index, (unsigned long long)ch->channel_id);
    while (atomic_load_explicit(&proxy->running, memory_order_relaxed) &&
           atomic_load_explicit(&ch->active, memory_order_acquire)) {
        drain_completions(ch);

        uint32_t req_count = vemb_v16_client_poll_batch(ch->request_ring,
                                                        req_buf,
                                                        sizeof(req_buf[0]),
                                                        VEMB_V16_PROXY_BATCH);
        if (req_count > 0) {
            atomic_fetch_add_explicit(&ch->stats.proxy_request_poll, req_count,
                                      memory_order_relaxed);
            for (uint32_t i = 0; i < req_count; i++) {
                handle_request(ch, &req_buf[i], (int)ch->request_ring->slot_size);
            }
            atomic_fetch_add_explicit(&ch->stats.channel_ops, req_count,
                                      memory_order_relaxed);
        } else {
            vemb_v16_cpu_relax();
        }
    }
    serverLog(LL_VERBOSE, "vemb_v16 proxy channel thread stopped: index=%u channel_id=%llu ops=%llu",
              ch->index,
              (unsigned long long)ch->channel_id,
              (unsigned long long)atomic_load_explicit(&ch->stats.channel_ops,
                                                       memory_order_relaxed));
    zfree(req_buf);
    return NULL;
}

static int alloc_channel(vemb_v16_proxy_t *proxy, vemb_v16_channel_desc_t *desc) {
    uint32_t idx = VEMB_V16_MAX_CHANNELS;
    uint32_t start = atomic_fetch_add_explicit(&proxy->next_channel_index, 1,
                                               memory_order_relaxed);
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        uint32_t candidate = (start + i) % VEMB_V16_MAX_CHANNELS;
        if (!atomic_load_explicit(&proxy->channels[candidate].active,
                                  memory_order_acquire)) {
            idx = candidate;
            break;
        }
    }
    if (idx >= VEMB_V16_MAX_CHANNELS) return -1;

    vemb_v16_channel_t *ch = &proxy->channels[idx];
    memset(ch, 0, sizeof(*ch));
    ch->index = idx;
    ch->channel_id = atomic_fetch_add_explicit(&proxy->next_channel_id, 1,
                                               memory_order_relaxed);
    ch->proxy = proxy;
    atomic_store_explicit(&ch->active, 1, memory_order_release);
    if (posix_memalign(&ch->vemb_job_slots, 64,
                       sizeof(vemb_v16_vemb_job_t) *
                       VEMB_V16_VEMB_JOB_RING_SIZE) != 0 ||
        posix_memalign(&ch->vadd_job_slots, 64,
                       sizeof(vemb_v16_vadd_job_t) *
                       VEMB_V16_VADD_JOB_RING_SIZE) != 0 ||
        posix_memalign(&ch->completion_slots, 64,
                       sizeof(vemb_v16_completion_t) *
                       VEMB_V16_COMPLETION_RING_SIZE) != 0) {
        vemb_v16_channel_free_slots(ch);
        memset(ch, 0, sizeof(*ch));
        return -1;
    }
    if (vemb_v16_aeron_ring_init(&ch->vemb_job_ring,
                                 ch->vemb_job_slots,
                                 sizeof(vemb_v16_vemb_job_t),
                                 VEMB_V16_VEMB_JOB_RING_SIZE) != 0 ||
        vemb_v16_aeron_ring_init(&ch->vadd_job_ring,
                                 ch->vadd_job_slots,
                                 sizeof(vemb_v16_vadd_job_t),
                                 VEMB_V16_VADD_JOB_RING_SIZE) != 0 ||
        vemb_v16_aeron_ring_init(&ch->completion_ring,
                                 ch->completion_slots,
                                 sizeof(vemb_v16_completion_t),
                                 VEMB_V16_COMPLETION_RING_SIZE) != 0) {
        vemb_v16_channel_free_slots(ch);
        memset(ch, 0, sizeof(*ch));
        return -1;
    }

    snprintf(ch->request_ring_name, sizeof(ch->request_ring_name),
             "/%s_req_%llu", VEMB_V16_SHM_PREFIX,
             (unsigned long long)ch->channel_id);
    snprintf(ch->response_ring_name, sizeof(ch->response_ring_name),
             "/%s_resp_%llu", VEMB_V16_SHM_PREFIX,
             (unsigned long long)ch->channel_id);

    if (create_shared_ring(ch->request_ring_name,
                           proxy->request_ring_slot_size,
                           &ch->request_ring,
                           &ch->request_ring_bytes) != 0) {
        vemb_v16_channel_free_slots(ch);
        memset(ch, 0, sizeof(*ch));
        return -1;
    }
    if (create_shared_ring(ch->response_ring_name,
                           proxy->response_ring_slot_size,
                           &ch->response_ring,
                           &ch->response_ring_bytes) != 0) {
        destroy_shared_ring(ch->request_ring_name, ch->request_ring,
                            ch->request_ring_bytes);
        vemb_v16_channel_free_slots(ch);
        memset(ch, 0, sizeof(*ch));
        return -1;
    }

    ch->supernode_ctx = (vemb_v16_supernode_ctx_t){
        .worker_id = ch->index,
        .channel_active = &ch->active,
        .running = &proxy->running,
        .vemb_job_ring = &ch->vemb_job_ring,
        .vadd_job_ring = &ch->vadd_job_ring,
        .completion_ring = &ch->completion_ring,
        .table = proxy->table,
        .stats = &ch->stats,
    };

    if (pthread_create(&ch->supernode_thread, NULL,
                       vemb_v16_supernode_thread_main,
                       &ch->supernode_ctx) != 0 ||
        pthread_create(&ch->proxy_thread, NULL, channel_thread_main, ch) != 0) {
        atomic_store_explicit(&ch->active, 0, memory_order_release);
        destroy_shared_ring(ch->request_ring_name, ch->request_ring,
                            ch->request_ring_bytes);
        destroy_shared_ring(ch->response_ring_name, ch->response_ring,
                            ch->response_ring_bytes);
        vemb_v16_channel_free_slots(ch);
        memset(ch, 0, sizeof(*ch));
        return -1;
    }

    serverLog(LL_VERBOSE, "vemb_v16 channel allocated: index=%u channel_id=%llu req=%s resp=%s",
              ch->index,
              (unsigned long long)ch->channel_id,
              ch->request_ring_name,
              ch->response_ring_name);

    memset(desc, 0, sizeof(*desc));
    desc->magic = VEMB_V16_MAGIC;
    desc->version = VEMB_V16_VERSION;
    desc->channel_id = ch->channel_id;
    desc->channel_index = ch->index;
    desc->vector_dim = proxy->vector_dim;
    desc->vector_stride = proxy->vector_stride;
    desc->max_vectors = proxy->max_vectors;
    desc->request_ring_slot_size = proxy->request_ring_slot_size;
    desc->response_ring_slot_size = proxy->response_ring_slot_size;
    strncpy(desc->request_ring_name, ch->request_ring_name,
            sizeof(desc->request_ring_name) - 1);
    strncpy(desc->response_ring_name, ch->response_ring_name,
            sizeof(desc->response_ring_name) - 1);
    strncpy(desc->vector_region_name, proxy->vector_region_name,
            sizeof(desc->vector_region_name) - 1);
    return 0;
}

static void close_channel(vemb_v16_channel_t *ch) {
    if (!ch || !atomic_load_explicit(&ch->active, memory_order_acquire)) return;
    serverLog(LL_VERBOSE, "vemb_v16 channel closing: index=%u channel_id=%llu",
              ch->index, (unsigned long long)ch->channel_id);
    atomic_store_explicit(&ch->active, 0, memory_order_release);
    pthread_join(ch->proxy_thread, NULL);
    pthread_join(ch->supernode_thread, NULL);
    pthread_mutex_lock(&ch->proxy->stats_lock);
    stats_add_channel_counters(&ch->proxy->closed_stats, &ch->stats);
    pthread_mutex_unlock(&ch->proxy->stats_lock);
    destroy_shared_ring(ch->request_ring_name, ch->request_ring,
                        ch->request_ring_bytes);
    destroy_shared_ring(ch->response_ring_name, ch->response_ring,
                        ch->response_ring_bytes);
    vemb_v16_channel_free_slots(ch);
    memset(ch, 0, sizeof(*ch));
}

static int close_channel_by_id(vemb_v16_proxy_t *proxy, uint64_t channel_id) {
    if (!proxy || channel_id == 0) return -1;
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        vemb_v16_channel_t *ch = &proxy->channels[i];
        if (atomic_load_explicit(&ch->active, memory_order_acquire) &&
            ch->channel_id == channel_id) {
            close_channel(ch);
            return 0;
        }
    }
    return -1;
}

static uint64_t close_all_channels(vemb_v16_proxy_t *proxy) {
    if (!proxy) return 0;
    uint64_t closed = 0;
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        vemb_v16_channel_t *ch = &proxy->channels[i];
        if (atomic_load_explicit(&ch->active, memory_order_acquire)) {
            close_channel(ch);
            closed++;
        }
    }
    return closed;
}

static void handle_control_fd(vemb_v16_proxy_t *proxy, int fd) {
    uint8_t op = 0;
    if (read(fd, &op, 1) != 1) goto close_fd;

    if (op == VEMB_V16_CTRL_PING) {
        uint8_t ok = 0;
        write_full(fd, &ok, sizeof(ok));
    } else if (op == VEMB_V16_CTRL_ALLOC_CHANNEL) {
        vemb_v16_alloc_req_t req;
        if (read_full(fd, &req, sizeof(req)) != 0) goto close_fd;
        vemb_v16_channel_desc_t desc;
        uint8_t status = alloc_channel(proxy, &desc) == 0 ? VEMB_V16_STATUS_OK : VEMB_V16_STATUS_ERR;
        if (status != VEMB_V16_STATUS_OK)
            serverLog(LL_WARNING, "vemb_v16 alloc channel failed");
        write_full(fd, &status, sizeof(status));
        if (status == VEMB_V16_STATUS_OK)
            write_full(fd, &desc, sizeof(desc));
    } else if (op == VEMB_V16_CTRL_STATS) {
        uint8_t status = VEMB_V16_STATUS_OK;
        vemb_v16_stats_t stats;
        vemb_v16_proxy_get_stats(proxy, &stats);
        write_full(fd, &status, sizeof(status));
        write_full(fd, &stats, sizeof(stats));
    } else if (op == VEMB_V16_CTRL_CLOSE_CHANNEL) {
        uint64_t channel_id = 0;
        if (read_full(fd, &channel_id, sizeof(channel_id)) != 0) goto close_fd;
        uint8_t status = close_channel_by_id(proxy, channel_id) == 0 ?
            VEMB_V16_STATUS_OK : VEMB_V16_STATUS_ERR;
        write_full(fd, &status, sizeof(status));
    } else if (op == VEMB_V16_CTRL_CLOSE_ALL_CHANNELS) {
        uint64_t closed = close_all_channels(proxy);
        uint8_t status = VEMB_V16_STATUS_OK;
        write_full(fd, &status, sizeof(status));
        write_full(fd, &closed, sizeof(closed));
    }

close_fd:
    close(fd);
}

int vemb_v16_proxy_create(vemb_v16_proxy_t **out,
                          const char *uds_path,
                          uint32_t vector_dim,
                          uint32_t max_vectors,
                          const char *vector_region_name) {
    if (!out) return -1;
    if (vector_dim == 0 || vector_dim > VEMB_V16_MAX_DIM)
        vector_dim = VEMB_V16_DEFAULT_DIM;
    if (max_vectors == 0) max_vectors = VEMB_V16_DEFAULT_MAX_VECTORS;

    vemb_v16_proxy_t *proxy = zcalloc(sizeof(*proxy));
    if (!proxy) return -1;
    strncpy(proxy->uds_path, uds_path ? uds_path : VEMB_V16_UDS_PATH,
            sizeof(proxy->uds_path) - 1);
    proxy->vector_dim = vector_dim;
    proxy->vector_stride = vector_dim * sizeof(float);
    proxy->request_ring_slot_size =
        (uint32_t)vemb_v16_req_inline_len(proxy->vector_stride);
    proxy->response_ring_slot_size = sizeof(vemb_v16_resp_t);
    proxy->max_vectors = max_vectors;
    proxy->uds_fd = -1;
    proxy->vector_region_fd = -1;
    atomic_init(&proxy->running, 0);
    atomic_init(&proxy->next_channel_id, 1);
    atomic_init(&proxy->next_channel_index, 0);
    pthread_mutex_init(&proxy->stats_lock, NULL);

    proxy->vector_region_size = (size_t)proxy->vector_stride * proxy->max_vectors;
    if (!vector_region_name || !vector_region_name[0])
        vector_region_name = VEMB_V16_DEFAULT_VECTOR_REGION;
    if (vector_region_name[0] != '/' ||
        strlen(vector_region_name) >= sizeof(proxy->vector_region_name)) {
        serverLog(LL_WARNING, "invalid vemb_v16 vector region name: %s",
                  vector_region_name);
        vemb_v16_proxy_destroy(proxy);
        return -1;
    }
    strncpy(proxy->vector_region_name,
            vector_region_name,
            sizeof(proxy->vector_region_name) - 1);
    shm_unlink(proxy->vector_region_name);
    proxy->vector_region_fd = shm_open(proxy->vector_region_name,
                                       O_CREAT | O_RDWR, 0666);
    if (proxy->vector_region_fd < 0 ||
        ftruncate(proxy->vector_region_fd, (off_t)proxy->vector_region_size) != 0) {
        serverLog(LL_WARNING, "vemb_v16 vector region create failed: name=%s size=%zu error=%s",
                  proxy->vector_region_name,
                  proxy->vector_region_size,
                  strerror(errno));
        vemb_v16_proxy_destroy(proxy);
        return -1;
    }
    proxy->vector_region = mmap(NULL, proxy->vector_region_size,
                                PROT_READ | PROT_WRITE, MAP_SHARED,
                                proxy->vector_region_fd, 0);
    close(proxy->vector_region_fd);
    proxy->vector_region_fd = -1;
    if (proxy->vector_region == MAP_FAILED) {
        serverLog(LL_WARNING, "vemb_v16 vector region mmap failed: name=%s size=%zu error=%s",
                  proxy->vector_region_name,
                  proxy->vector_region_size,
                  strerror(errno));
        proxy->vector_region = NULL;
        vemb_v16_proxy_destroy(proxy);
        return -1;
    }

    if (vemb_v16_table_create(&proxy->table,
                              proxy->vector_dim,
                              proxy->max_vectors,
                              proxy->vector_region,
                              proxy->vector_region_size) != 0) {
        serverLog(LL_WARNING, "vemb_v16 table create failed: dim=%u max_vectors=%u",
                  proxy->vector_dim, proxy->max_vectors);
        vemb_v16_proxy_destroy(proxy);
        return -1;
    }

    serverLog(LL_NOTICE, "vemb_v16 proxy created: uds=%s dim=%u max_vectors=%u vector_region=%s size=%zu",
              proxy->uds_path,
              proxy->vector_dim,
              proxy->max_vectors,
              proxy->vector_region_name,
              proxy->vector_region_size);
    *out = proxy;
    return 0;
}

void vemb_v16_proxy_destroy(vemb_v16_proxy_t *proxy) {
    if (!proxy) return;
    vemb_v16_proxy_stop(proxy);
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++)
        close_channel(&proxy->channels[i]);
    if (proxy->uds_fd >= 0) close(proxy->uds_fd);
    if (proxy->uds_path[0]) unlink(proxy->uds_path);
    vemb_v16_table_destroy(proxy->table);
    if (proxy->vector_region) munmap(proxy->vector_region, proxy->vector_region_size);
    if (proxy->vector_region_name[0]) shm_unlink(proxy->vector_region_name);
    pthread_mutex_destroy(&proxy->stats_lock);
    zfree(proxy);
}

int vemb_v16_proxy_run(vemb_v16_proxy_t *proxy) {
    if (!proxy) return -1;
    atomic_store_explicit(&proxy->running, 1, memory_order_relaxed);
    unlink(proxy->uds_path);

    proxy->uds_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (proxy->uds_fd < 0) return -1;
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, proxy->uds_path, sizeof(addr.sun_path) - 1);
    if (bind(proxy->uds_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 ||
        listen(proxy->uds_fd, 4096) != 0) {
        return -1;
    }
    fcntl(proxy->uds_fd, F_SETFL, fcntl(proxy->uds_fd, F_GETFL, 0) | O_NONBLOCK);

    serverLog(LL_NOTICE, "vemb_v16 server ready: uds=%s dim=%u max_vectors=%u vector_region=%s",
              proxy->uds_path, proxy->vector_dim, proxy->max_vectors,
              proxy->vector_region_name);

    while (atomic_load_explicit(&proxy->running, memory_order_relaxed)) {
        int cfd = accept(proxy->uds_fd, NULL, NULL);
        if (cfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                struct timespec ts = {0, 1000000};
                nanosleep(&ts, NULL);
                continue;
            }
            if (errno == EINTR) continue;
            serverLog(LL_WARNING, "vemb_v16 accept failed: fd=%d errno=%d error=%s",
                      proxy->uds_fd, errno, strerror(errno));
            return -1;
        }
        handle_control_fd(proxy, cfd);
    }
    return 0;
}

void vemb_v16_proxy_stop(vemb_v16_proxy_t *proxy) {
    if (!proxy) return;
    int was_running = atomic_exchange_explicit(&proxy->running, 0,
                                               memory_order_relaxed);
    if (!was_running) return;
    if (proxy->uds_fd >= 0) shutdown(proxy->uds_fd, SHUT_RDWR);
}

void vemb_v16_proxy_get_stats(vemb_v16_proxy_t *proxy, vemb_v16_stats_t *stats) {
    if (!proxy || !stats) return;
    memset(stats, 0, sizeof(*stats));
    pthread_mutex_lock(&proxy->stats_lock);
    stats_add(stats, &proxy->closed_stats);
    pthread_mutex_unlock(&proxy->stats_lock);
    sve_operation_stats_t *sve_stats = vemb_v16_table_sve_stats(proxy->table);
    if (sve_stats) {
        stats->bitmap_lock_success =
            atomic_load_explicit(&sve_stats->lock_success, memory_order_relaxed);
        stats->bitmap_lock_failure =
            atomic_load_explicit(&sve_stats->lock_failure, memory_order_relaxed);
    }
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        vemb_v16_channel_t *ch = &proxy->channels[i];
        if (atomic_load_explicit(&ch->active, memory_order_acquire)) {
            stats->active_channels++;
            stats_add_channel_counters(stats, &ch->stats);
            stats->channel_ops += counter_load(&ch->stats.channel_ops);
            if (ch->request_ring)
                stats->request_ring_depth +=
                    vemb_v16_client_available(ch->request_ring);
            if (ch->response_ring)
                stats->response_ring_depth +=
                    vemb_v16_client_available(ch->response_ring);
            stats->vemb_job_ring_depth += vemb_v16_aeron_available(&ch->vemb_job_ring);
            stats->vadd_job_ring_depth += vemb_v16_aeron_available(&ch->vadd_job_ring);
            stats->completion_ring_depth += vemb_v16_aeron_available(&ch->completion_ring);
        }
    }
}
