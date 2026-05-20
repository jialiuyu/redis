#define _GNU_SOURCE

#include "vemb_v16_proxy.h"
#include "aeron_ipc.h"
#include "vemb_v16_aeron_ring.h"
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

typedef struct vemb_v16_channel {
    uint64_t channel_id;
    uint32_t index;
    atomic_int active;
    char request_ring_name[64];
    char response_ring_name[64];
    aeron_ring_t *request_ring;
    aeron_ring_t *response_ring;
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
    atomic_uint_fast64_t ops;
} vemb_v16_channel_t;

struct vemb_v16_proxy {
    char uds_path[108];
    uint32_t vector_dim;
    uint32_t vector_stride;
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
    atomic_uint_fast64_t total_requests;
    atomic_uint_fast64_t vadd_requests;
    atomic_uint_fast64_t vemb_requests;
    atomic_uint_fast64_t not_found;
    atomic_uint_fast64_t published_jobs;
    atomic_uint_fast64_t completed_jobs;
    atomic_uint_fast64_t proxy_request_poll;
    atomic_uint_fast64_t proxy_completion_poll;
    atomic_uint_fast64_t proxy_vemb_publish;
    atomic_uint_fast64_t proxy_vadd_publish;
    atomic_uint_fast64_t proxy_vemb_ring_full;
    atomic_uint_fast64_t proxy_vadd_ring_full;
    atomic_uint_fast64_t proxy_response_publish;
    atomic_uint_fast64_t proxy_response_ring_full;
    atomic_uint_fast64_t supernode_vemb_poll;
    atomic_uint_fast64_t supernode_vadd_poll;
    atomic_uint_fast64_t supernode_completion_publish;
    atomic_uint_fast64_t supernode_completion_ring_full;
    atomic_uint_fast64_t sample_count;
    atomic_uint_fast64_t sample_table_lookup_ns;
    atomic_uint_fast64_t sample_bitmap_lock_ns;
    atomic_uint_fast64_t sample_bitmap_unlock_ns;
    atomic_uint_fast64_t sample_vector_load_ns;
    atomic_uint_fast64_t sample_completion_publish_ns;
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

static int create_shared_ring(const char *name, aeron_ring_t **ring) {
    shm_unlink(name);
    int fd = shm_open(name, O_CREAT | O_RDWR, 0666);
    if (fd < 0) return -1;
    if (ftruncate(fd, (off_t)sizeof(aeron_ring_t)) != 0) {
        close(fd);
        shm_unlink(name);
        return -1;
    }
    void *ptr = mmap(NULL, sizeof(aeron_ring_t), PROT_READ | PROT_WRITE,
                     MAP_SHARED, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) {
        shm_unlink(name);
        return -1;
    }
    memset(ptr, 0, sizeof(aeron_ring_t));
    *ring = ptr;
    return 0;
}

static void destroy_shared_ring(const char *name, aeron_ring_t *ring) {
    if (ring) munmap(ring, sizeof(aeron_ring_t));
    if (name && name[0]) shm_unlink(name);
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
    while (aeron_publish(ch->response_ring, &resp, sizeof(resp)) != 0 &&
           atomic_load_explicit(&ch->proxy->running, memory_order_relaxed) &&
           atomic_load_explicit(&ch->active, memory_order_acquire)) {
        atomic_fetch_add_explicit(&ch->proxy->proxy_response_ring_full, 1,
                                  memory_order_relaxed);
        vemb_v16_cpu_relax();
    }
    atomic_fetch_add_explicit(&ch->proxy->proxy_response_publish, 1,
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
            atomic_fetch_add_explicit(&proxy->proxy_vadd_ring_full, 1,
                                      memory_order_relaxed);
            vemb_v16_cpu_relax();
        }
        atomic_fetch_add_explicit(&proxy->proxy_vadd_publish, 1,
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
            atomic_fetch_add_explicit(&proxy->proxy_vemb_ring_full, 1,
                                      memory_order_relaxed);
            vemb_v16_cpu_relax();
        }
        atomic_fetch_add_explicit(&proxy->proxy_vemb_publish, 1,
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
    atomic_fetch_add_explicit(&proxy->published_jobs, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&proxy->total_requests, 1, memory_order_relaxed);
}

static void drain_completions(vemb_v16_channel_t *ch) {
    vemb_v16_completion_t completion;
    while (vemb_v16_aeron_poll(&ch->completion_ring, &completion)) {
        atomic_fetch_add_explicit(&ch->proxy->proxy_completion_poll, 1,
                                  memory_order_relaxed);
        if (completion.channel_id == ch->channel_id &&
            atomic_load_explicit(&ch->active, memory_order_acquire)) {
            publish_response(ch, &completion);
        }
    }
}

static void *channel_thread_main(void *arg) {
    vemb_v16_channel_t *ch = arg;
    vemb_v16_proxy_t *proxy = ch->proxy;
    uint8_t req_buf[AERON_MSG_SIZE];

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

        int req_len = aeron_poll(ch->request_ring, req_buf, sizeof(req_buf));
        if (req_len > 0) {
            atomic_fetch_add_explicit(&proxy->proxy_request_poll, 1,
                                      memory_order_relaxed);
            if ((size_t)req_len >= vemb_v16_req_handle_len()) {
                handle_request(ch, (const vemb_v16_req_t *)(const void *)req_buf, req_len);
            }
            atomic_fetch_add_explicit(&ch->ops, 1, memory_order_relaxed);
        } else {
            vemb_v16_cpu_relax();
        }
    }
    serverLog(LL_VERBOSE, "vemb_v16 proxy channel thread stopped: index=%u channel_id=%llu ops=%llu",
              ch->index,
              (unsigned long long)ch->channel_id,
              (unsigned long long)atomic_load_explicit(&ch->ops, memory_order_relaxed));
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
    atomic_init(&ch->ops, 0);
    if (posix_memalign(&ch->vemb_job_slots, 64,
                       sizeof(vemb_v16_vemb_job_t) *
                       VEMB_V16_VEMB_JOB_RING_SIZE) != 0 ||
        posix_memalign(&ch->vadd_job_slots, 64,
                       sizeof(vemb_v16_vadd_job_t) *
                       VEMB_V16_VADD_JOB_RING_SIZE) != 0 ||
        posix_memalign(&ch->completion_slots, 64,
                       sizeof(vemb_v16_completion_t) *
                       VEMB_V16_COMPLETION_RING_SIZE) != 0) {
        free(ch->vemb_job_slots);
        free(ch->vadd_job_slots);
        free(ch->completion_slots);
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
        free(ch->vemb_job_slots);
        free(ch->vadd_job_slots);
        free(ch->completion_slots);
        memset(ch, 0, sizeof(*ch));
        return -1;
    }

    snprintf(ch->request_ring_name, sizeof(ch->request_ring_name),
             "/%s_req_%llu", VEMB_V16_SHM_PREFIX,
             (unsigned long long)ch->channel_id);
    snprintf(ch->response_ring_name, sizeof(ch->response_ring_name),
             "/%s_resp_%llu", VEMB_V16_SHM_PREFIX,
             (unsigned long long)ch->channel_id);

    if (create_shared_ring(ch->request_ring_name, &ch->request_ring) != 0) {
        free(ch->vemb_job_slots);
        free(ch->vadd_job_slots);
        free(ch->completion_slots);
        memset(ch, 0, sizeof(*ch));
        return -1;
    }
    if (create_shared_ring(ch->response_ring_name, &ch->response_ring) != 0) {
        destroy_shared_ring(ch->request_ring_name, ch->request_ring);
        free(ch->vemb_job_slots);
        free(ch->vadd_job_slots);
        free(ch->completion_slots);
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
        .vadd_requests = &proxy->vadd_requests,
        .vemb_requests = &proxy->vemb_requests,
        .not_found = &proxy->not_found,
        .completed_jobs = &proxy->completed_jobs,
        .supernode_vemb_poll = &proxy->supernode_vemb_poll,
        .supernode_vadd_poll = &proxy->supernode_vadd_poll,
        .supernode_completion_publish = &proxy->supernode_completion_publish,
        .supernode_completion_ring_full = &proxy->supernode_completion_ring_full,
        .sample_count = &proxy->sample_count,
        .sample_table_lookup_ns = &proxy->sample_table_lookup_ns,
        .sample_bitmap_lock_ns = &proxy->sample_bitmap_lock_ns,
        .sample_bitmap_unlock_ns = &proxy->sample_bitmap_unlock_ns,
        .sample_vector_load_ns = &proxy->sample_vector_load_ns,
        .sample_completion_publish_ns = &proxy->sample_completion_publish_ns,
    };

    if (pthread_create(&ch->supernode_thread, NULL,
                       vemb_v16_supernode_thread_main,
                       &ch->supernode_ctx) != 0 ||
        pthread_create(&ch->proxy_thread, NULL, channel_thread_main, ch) != 0) {
        atomic_store_explicit(&ch->active, 0, memory_order_release);
        destroy_shared_ring(ch->request_ring_name, ch->request_ring);
        destroy_shared_ring(ch->response_ring_name, ch->response_ring);
        free(ch->vemb_job_slots);
        free(ch->vadd_job_slots);
        free(ch->completion_slots);
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
    destroy_shared_ring(ch->request_ring_name, ch->request_ring);
    destroy_shared_ring(ch->response_ring_name, ch->response_ring);
    free(ch->vemb_job_slots);
    free(ch->vadd_job_slots);
    free(ch->completion_slots);
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
                          uint32_t max_vectors) {
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
    proxy->max_vectors = max_vectors;
    proxy->uds_fd = -1;
    proxy->vector_region_fd = -1;
    atomic_init(&proxy->running, 0);
    atomic_init(&proxy->next_channel_id, 1);
    atomic_init(&proxy->next_channel_index, 0);

    proxy->vector_region_size = (size_t)proxy->vector_stride * proxy->max_vectors;
    snprintf(proxy->vector_region_name, sizeof(proxy->vector_region_name),
             "/%s_vectors", VEMB_V16_SHM_PREFIX);
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
            serverLog(LL_WARNING, "vemb_v16 accept failed: %s", strerror(errno));
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
    stats->total_requests = atomic_load_explicit(&proxy->total_requests, memory_order_relaxed);
    stats->vadd_requests = atomic_load_explicit(&proxy->vadd_requests, memory_order_relaxed);
    stats->vemb_requests = atomic_load_explicit(&proxy->vemb_requests, memory_order_relaxed);
    stats->not_found = atomic_load_explicit(&proxy->not_found, memory_order_relaxed);
    stats->published_jobs = atomic_load_explicit(&proxy->published_jobs, memory_order_relaxed);
    stats->completed_jobs = atomic_load_explicit(&proxy->completed_jobs, memory_order_relaxed);
    stats->proxy_request_poll =
        atomic_load_explicit(&proxy->proxy_request_poll, memory_order_relaxed);
    stats->proxy_completion_poll =
        atomic_load_explicit(&proxy->proxy_completion_poll, memory_order_relaxed);
    stats->proxy_vemb_publish =
        atomic_load_explicit(&proxy->proxy_vemb_publish, memory_order_relaxed);
    stats->proxy_vadd_publish =
        atomic_load_explicit(&proxy->proxy_vadd_publish, memory_order_relaxed);
    stats->proxy_vemb_ring_full =
        atomic_load_explicit(&proxy->proxy_vemb_ring_full, memory_order_relaxed);
    stats->proxy_vadd_ring_full =
        atomic_load_explicit(&proxy->proxy_vadd_ring_full, memory_order_relaxed);
    stats->proxy_response_publish =
        atomic_load_explicit(&proxy->proxy_response_publish, memory_order_relaxed);
    stats->proxy_response_ring_full =
        atomic_load_explicit(&proxy->proxy_response_ring_full, memory_order_relaxed);
    stats->supernode_vemb_poll =
        atomic_load_explicit(&proxy->supernode_vemb_poll, memory_order_relaxed);
    stats->supernode_vadd_poll =
        atomic_load_explicit(&proxy->supernode_vadd_poll, memory_order_relaxed);
    stats->supernode_completion_publish =
        atomic_load_explicit(&proxy->supernode_completion_publish, memory_order_relaxed);
    stats->supernode_completion_ring_full =
        atomic_load_explicit(&proxy->supernode_completion_ring_full, memory_order_relaxed);
    stats->sample_count =
        atomic_load_explicit(&proxy->sample_count, memory_order_relaxed);
    stats->sample_table_lookup_ns =
        atomic_load_explicit(&proxy->sample_table_lookup_ns, memory_order_relaxed);
    stats->sample_bitmap_lock_ns =
        atomic_load_explicit(&proxy->sample_bitmap_lock_ns, memory_order_relaxed);
    stats->sample_bitmap_unlock_ns =
        atomic_load_explicit(&proxy->sample_bitmap_unlock_ns, memory_order_relaxed);
    stats->sample_vector_load_ns =
        atomic_load_explicit(&proxy->sample_vector_load_ns, memory_order_relaxed);
    stats->sample_completion_publish_ns =
        atomic_load_explicit(&proxy->sample_completion_publish_ns, memory_order_relaxed);
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
            if (ch->request_ring) {
                uint64_t req_head = __atomic_load_n(&ch->request_ring->head,
                                                    __ATOMIC_ACQUIRE);
                uint64_t req_tail = __atomic_load_n(&ch->request_ring->tail,
                                                    __ATOMIC_ACQUIRE);
                stats->request_ring_depth += req_tail - req_head;
            }
            if (ch->response_ring) {
                uint64_t resp_head = __atomic_load_n(&ch->response_ring->head,
                                                     __ATOMIC_ACQUIRE);
                uint64_t resp_tail = __atomic_load_n(&ch->response_ring->tail,
                                                     __ATOMIC_ACQUIRE);
                stats->response_ring_depth += resp_tail - resp_head;
            }
            stats->vemb_job_ring_depth += vemb_v16_aeron_available(&ch->vemb_job_ring);
            stats->vadd_job_ring_depth += vemb_v16_aeron_available(&ch->vadd_job_ring);
            stats->completion_ring_depth += vemb_v16_aeron_available(&ch->completion_ring);
            stats->channel_ops += atomic_load_explicit(&ch->ops, memory_order_relaxed);
        }
    }
}
