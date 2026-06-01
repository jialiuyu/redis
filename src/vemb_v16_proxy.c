#define _GNU_SOURCE

#include "cpu_relax.h"
#include "vemb_v16_proxy.h"
#include "vemb_v16_aeron_ring.h"
#include "vemb_v16_client_ring.h"
#include "vemb_v16_dataplane.h"
#include "vemb_v16_log.h"
#include "vemb_v16_net.h"
#include "vemb_v16_storage.h"
#include "vemb_v16_supernode.h"
#include "redisassert.h"
#include "zmalloc.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <poll.h>
#include <sched.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef __linux__
#include <sys/eventfd.h>
#include <sys/epoll.h>
#endif
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define VEMB_V16_VEMB_JOB_RING_SIZE VEMB_V16_AERON_RING_SIZE
#define VEMB_V16_VADD_JOB_RING_SIZE 256u
#define VEMB_V16_VADD_SHARD_RING_SIZE VEMB_V16_VADD_JOB_RING_SIZE
#define VEMB_V16_COMPLETION_RING_SIZE VEMB_V16_AERON_RING_SIZE
#define VEMB_V16_TCP_RESPONSE_BACKLOG_LIMIT (4u * 1024u * 1024u)
#define VEMB_V16_PROXY_BATCH 32u
#define VEMB_V16_SUPERNODE_STATE_CLOSING (1u << 31)
#define VEMB_V16_PROXY_IO_STATE_CLOSING (1u << 31)

struct vemb_v16_proxy;

typedef struct vemb_v16_proxy_io_worker {
    uint32_t worker_id;
    struct vemb_v16_proxy *proxy;
    pthread_t thread;
#ifdef __linux__
    int notify_fd;
#endif
} vemb_v16_proxy_io_worker_t;

typedef struct vemb_v16_supernode_pool_worker {
    uint32_t worker_id;
    struct vemb_v16_proxy *proxy;
    pthread_t thread;
#ifdef __linux__
    atomic_int job_notify_armed;
    int notify_fd;
#endif
} vemb_v16_supernode_pool_worker_t;

typedef struct vemb_v16_shard_queue {
    vemb_v16_aeron_ring_t ring;
    void *slots;
} vemb_v16_shard_queue_t;

typedef struct vemb_v16_channel {
    uint64_t channel_id;
    atomic_uint_fast64_t slot_channel_id;
    uint32_t index;
    atomic_int active;
    char request_ring_name[64];
    char response_ring_name[64];
    vemb_v16_client_ring_t *request_ring;
    vemb_v16_client_ring_t *response_ring;
    size_t request_ring_bytes;
    size_t response_ring_bytes;
    uint32_t transport_type;
    int net_fd;
    atomic_int proxy_io_registered;
    atomic_uint_fast32_t proxy_io_state;
    atomic_uint_fast32_t supernode_state;
    atomic_int completion_notify_armed;
    int tcp_backpressure_enabled;
    uint8_t *tcp_response_backlog;
    size_t tcp_response_backlog_cap;
    size_t tcp_response_backlog_len;
    size_t tcp_response_backlog_sent;
    vemb_v16_aeron_ring_t completion_ring;
    void *completion_slots;
    vemb_v16_supernode_ctx_t supernode_ctx;
    struct vemb_v16_proxy *proxy;
    vemb_v16_channel_counters_t stats;
} vemb_v16_channel_t;

struct vemb_v16_proxy {
    char uds_path[108];
    char tcp_host[64];
    uint32_t vector_dim;
    uint32_t vector_stride;
    uint32_t request_ring_slot_size;
    uint32_t response_ring_slot_size;
    uint32_t max_vectors;
    vemb_v16_storage_ctx_t *storage;
    vemb_v16_channel_t channels[VEMB_V16_MAX_CHANNELS];
    atomic_uint_fast64_t next_channel_id;
    atomic_uint_fast32_t next_channel_index;
    atomic_int running;
    int uds_fd;
    int tcp_fd;
    uint16_t tcp_port;
    int tcp_enabled;
    uint32_t proxy_io_worker_count;
    int proxy_io_pool_started;
    vemb_v16_proxy_io_worker_t proxy_io_workers[VEMB_V16_MAX_CHANNELS];
    uint32_t supernode_worker_count;
    int supernode_pool_started;
    vemb_v16_supernode_pool_worker_t supernode_workers[VEMB_V16_MAX_CHANNELS];
    uint32_t vemb_shard_proxy_count;
    uint32_t vemb_shard_supernode_count;
    vemb_v16_shard_queue_t *vemb_shard_queues;
    vemb_v16_shard_queue_t *vadd_shard_queues;
    pthread_mutex_t stats_lock;
    vemb_v16_stats_t closed_stats;
};

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

/// UB/SHM control plane: create the per-channel client request/response rings.
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
    dst->channel_ops += counter_load(&src->channel_ops);
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
    dst->channel_ops += src->channel_ops;
}

static void vemb_v16_channel_free_slots(vemb_v16_channel_t *ch) {
    assert(ch != NULL);
    free(ch->completion_slots);
    zfree(ch->tcp_response_backlog);
    ch->completion_slots = NULL;
    ch->tcp_response_backlog = NULL;
    ch->tcp_response_backlog_cap = 0;
    ch->tcp_response_backlog_len = 0;
    ch->tcp_response_backlog_sent = 0;
}

static void free_shard_queue_array(vemb_v16_shard_queue_t *queues,
                                   uint32_t count) {
    if (!queues)
        return;
    for (uint32_t i = 0; i < count; i++) {
        free(queues[i].slots);
        queues[i].slots = NULL;
    }
    zfree(queues);
}

static int init_shard_queue_array(vemb_v16_shard_queue_t **out,
                                  uint32_t count,
                                  size_t slot_size,
                                  uint32_t ring_size) {
    if (!out)
        return -1;
    *out = NULL;

    vemb_v16_shard_queue_t *queues = zcalloc(sizeof(*queues) * count);
    if (!queues)
        return -1;

    for (uint32_t i = 0; i < count; i++) {
        if (posix_memalign(&queues[i].slots, 64, slot_size * ring_size) != 0 ||
            vemb_v16_aeron_ring_init(&queues[i].ring,
                                     queues[i].slots,
                                     (uint32_t)slot_size,
                                     ring_size) != 0) {
            free_shard_queue_array(queues, count);
            return -1;
        }
    }

    *out = queues;
    return 0;
}

static void free_vemb_shard_queues(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    uint32_t count = proxy->vemb_shard_proxy_count *
        proxy->vemb_shard_supernode_count;
    free_shard_queue_array(proxy->vemb_shard_queues, count);
    free_shard_queue_array(proxy->vadd_shard_queues, count);
    proxy->vemb_shard_queues = NULL;
    proxy->vadd_shard_queues = NULL;
    proxy->vemb_shard_proxy_count = 0;
    proxy->vemb_shard_supernode_count = 0;
}

static int validate_pooled_worker_config(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    if (proxy->proxy_io_worker_count == 0)
        return -1;
    if (proxy->supernode_worker_count == 0)
        return -1;
    return 0;
}

static int shard_queue_topology_ready(vemb_v16_proxy_t *proxy) {
    if (validate_pooled_worker_config(proxy) != 0)
        return 0;
    if (!proxy->vemb_shard_queues || !proxy->vadd_shard_queues)
        return 0;
    if (proxy->vemb_shard_proxy_count != proxy->proxy_io_worker_count)
        return 0;
    if (proxy->vemb_shard_supernode_count != proxy->supernode_worker_count)
        return 0;
    return 1;
}

static uint32_t shard_queue_index(vemb_v16_proxy_t *proxy,
                                  uint32_t proxy_worker_id,
                                  uint32_t supernode_worker_id) {
    assert(proxy != NULL);
    assert(shard_queue_topology_ready(proxy));
    assert(proxy_worker_id < proxy->vemb_shard_proxy_count);
    assert(supernode_worker_id < proxy->vemb_shard_supernode_count);
    return proxy_worker_id * proxy->vemb_shard_supernode_count +
        supernode_worker_id;
}

static vemb_v16_storage_ctx_t *proxy_storage(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    assert(proxy->storage != NULL);
    return proxy->storage;
}

/// Scheduling plane: allocate the proxy-IO -> SuperNode shard queues.
static int init_vemb_shard_queues(vemb_v16_proxy_t *proxy) {
    if (validate_pooled_worker_config(proxy) != 0)
        return -1;
    if (proxy->vemb_shard_queues)
        return 0;

    uint32_t proxy_count = proxy->proxy_io_worker_count;
    uint32_t supernode_count = proxy->supernode_worker_count;
    uint32_t count = proxy_count * supernode_count;
    proxy->vemb_shard_proxy_count = proxy_count;
    proxy->vemb_shard_supernode_count = supernode_count;
    if (init_shard_queue_array(&proxy->vemb_shard_queues,
                               count,
                               sizeof(vemb_v16_vemb_job_t),
                               VEMB_V16_VEMB_JOB_RING_SIZE) != 0 ||
        init_shard_queue_array(&proxy->vadd_shard_queues,
                               count,
                               sizeof(vemb_v16_vadd_job_t),
                               VEMB_V16_VADD_SHARD_RING_SIZE) != 0) {
        free_vemb_shard_queues(proxy);
        return -1;
    }
    return 0;
}

/// Lifecycle synchronization: reset a channel after TCP or UB/SHM teardown.
static void reset_closed_channel(vemb_v16_channel_t *ch) {
    assert(ch != NULL);
    atomic_store_explicit(&ch->slot_channel_id, 0, memory_order_release);
    atomic_store_explicit(&ch->active, 0, memory_order_release);
    atomic_store_explicit(&ch->proxy_io_registered, 0, memory_order_release);
    atomic_store_explicit(&ch->proxy_io_state, 0, memory_order_release);
    atomic_store_explicit(&ch->supernode_state, 0, memory_order_release);
    atomic_store_explicit(&ch->completion_notify_armed, 0,
                          memory_order_release);
    ch->channel_id = 0;
    ch->index = 0;
    memset(ch->request_ring_name, 0, sizeof(ch->request_ring_name));
    memset(ch->response_ring_name, 0, sizeof(ch->response_ring_name));
    ch->request_ring = NULL;
    ch->response_ring = NULL;
    ch->request_ring_bytes = 0;
    ch->response_ring_bytes = 0;
    ch->transport_type = 0;
    ch->net_fd = -1;
    ch->tcp_backpressure_enabled = 0;
    memset(&ch->completion_ring, 0, sizeof(ch->completion_ring));
    ch->completion_slots = NULL;
    memset(&ch->supernode_ctx, 0, sizeof(ch->supernode_ctx));
    ch->proxy = NULL;
    memset(&ch->stats, 0, sizeof(ch->stats));
}

/// Execution-side ownership: let one SuperNode worker safely touch a channel.
static int supernode_channel_acquire(vemb_v16_channel_t *ch) {
    if (atomic_load_explicit(&ch->slot_channel_id, memory_order_acquire) == 0 ||
        !atomic_load_explicit(&ch->active, memory_order_acquire)) {
        return 0;
    }
    uint_fast32_t state =
        atomic_load_explicit(&ch->supernode_state, memory_order_acquire);
    for (;;) {
        if (state & VEMB_V16_SUPERNODE_STATE_CLOSING)
            return 0;
        if (atomic_compare_exchange_weak_explicit(&ch->supernode_state,
                                                  &state,
                                                  state + 1,
                                                  memory_order_acquire,
                                                  memory_order_relaxed)) {
            return 1;
        }
    }
}

/// Scheduling-side ownership: let one proxy IO worker safely touch a channel.
static int proxy_io_channel_acquire(vemb_v16_channel_t *ch) {
    if (atomic_load_explicit(&ch->slot_channel_id, memory_order_acquire) == 0 ||
        !atomic_load_explicit(&ch->active, memory_order_acquire)) {
        return 0;
    }
    uint_fast32_t state =
        atomic_load_explicit(&ch->proxy_io_state, memory_order_acquire);
    for (;;) {
        if (state & VEMB_V16_PROXY_IO_STATE_CLOSING)
            return 0;
        if (atomic_compare_exchange_weak_explicit(&ch->proxy_io_state,
                                                  &state,
                                                  state + 1,
                                                  memory_order_acquire,
                                                  memory_order_relaxed)) {
            return 1;
        }
    }
}

static void proxy_io_channel_release(vemb_v16_channel_t *ch) {
    atomic_fetch_sub_explicit(&ch->proxy_io_state, 1, memory_order_release);
}

static void proxy_io_channel_disarm_completion_notify(vemb_v16_channel_t *ch) {
    assert(ch != NULL);
    atomic_store_explicit(&ch->completion_notify_armed, 0,
                          memory_order_release);
}

#ifdef __linux__
static int proxy_io_channel_arm_completion_notify(vemb_v16_channel_t *ch) {
    if (!ch || !atomic_load_explicit(&ch->active, memory_order_acquire)) {
        return 0;
    }

    atomic_store_explicit(&ch->completion_notify_armed, 1,
                          memory_order_release);
    if (vemb_v16_aeron_available(&ch->completion_ring) != 0) {
        atomic_store_explicit(&ch->completion_notify_armed, 0,
                              memory_order_release);
        return -1;
    }
    return 1;
}
#endif

static void proxy_io_channel_close_begin(vemb_v16_channel_t *ch) {
    atomic_fetch_or_explicit(&ch->proxy_io_state,
                             VEMB_V16_PROXY_IO_STATE_CLOSING,
                             memory_order_acq_rel);
}

static void proxy_io_channel_wait_closed(vemb_v16_channel_t *ch) {
    for (;;) {
        uint_fast32_t state =
            atomic_load_explicit(&ch->proxy_io_state, memory_order_acquire);
        if ((state & ~VEMB_V16_PROXY_IO_STATE_CLOSING) == 0 &&
            !atomic_load_explicit(&ch->proxy_io_registered,
                                  memory_order_acquire)) {
            return;
        }
        cpu_relax();
    }
}

static void supernode_channel_release(vemb_v16_channel_t *ch) {
    atomic_fetch_sub_explicit(&ch->supernode_state, 1, memory_order_release);
}

static void supernode_channel_close_begin(vemb_v16_channel_t *ch) {
    atomic_fetch_or_explicit(&ch->supernode_state,
                             VEMB_V16_SUPERNODE_STATE_CLOSING,
                             memory_order_acq_rel);
}

static void supernode_channel_wait_closed(vemb_v16_channel_t *ch) {
    for (;;) {
        uint_fast32_t state =
            atomic_load_explicit(&ch->supernode_state, memory_order_acquire);
        if ((state & ~VEMB_V16_SUPERNODE_STATE_CLOSING) == 0)
            return;
        cpu_relax();
    }
}

static int tcp_response_vector_slice(vemb_v16_channel_t *ch,
                                     vemb_v16_resp_t *resp,
                                     const uint8_t **vector,
                                     uint32_t *vector_bytes) {
    return vemb_v16_storage_vector_slice(proxy_storage(ch->proxy),
                                         resp,
                                         vector,
                                         vector_bytes);
}

static void fill_response_from_completion(vemb_v16_resp_t *resp,
                                          const vemb_v16_completion_t *completion);

#ifdef __linux__
/// TCP transport: queue partial response writes when clients apply backpressure.
static int tcp_response_backlog_pending(vemb_v16_channel_t *ch) {
    return ch && ch->tcp_response_backlog_len > ch->tcp_response_backlog_sent;
}

static int ensure_tcp_response_backlog_capacity(vemb_v16_channel_t *ch,
                                                size_t append_bytes) {
    if (!ch)
        return -1;
    size_t pending = ch->tcp_response_backlog_len - ch->tcp_response_backlog_sent;
    if (append_bytes > VEMB_V16_TCP_RESPONSE_BACKLOG_LIMIT ||
        pending > VEMB_V16_TCP_RESPONSE_BACKLOG_LIMIT - append_bytes) {
        return -1;
    }
    if (ch->tcp_response_backlog_sent != 0 && pending != 0) {
        memmove(ch->tcp_response_backlog,
                ch->tcp_response_backlog + ch->tcp_response_backlog_sent,
                pending);
    }
    ch->tcp_response_backlog_len = pending;
    ch->tcp_response_backlog_sent = 0;
    if (ch->tcp_response_backlog_cap >= pending + append_bytes)
        return 0;

    size_t next_cap = ch->tcp_response_backlog_cap ? ch->tcp_response_backlog_cap : 4096u;
    while (next_cap < pending + append_bytes) {
        next_cap <<= 1;
        if (next_cap > VEMB_V16_TCP_RESPONSE_BACKLOG_LIMIT) {
            next_cap = VEMB_V16_TCP_RESPONSE_BACKLOG_LIMIT;
            break;
        }
    }
    if (next_cap < pending + append_bytes)
        return -1;
    uint8_t *next = zrealloc(ch->tcp_response_backlog, next_cap);
    if (!next)
        return -1;
    ch->tcp_response_backlog = next;
    ch->tcp_response_backlog_cap = next_cap;
    return 0;
}

static int append_tcp_response_backlog(vemb_v16_channel_t *ch,
                                       const void *buf,
                                       size_t len) {
    if (!ch || (!buf && len != 0))
        return -1;
    if (ensure_tcp_response_backlog_capacity(ch, len) != 0)
        return -1;
    memcpy(ch->tcp_response_backlog + ch->tcp_response_backlog_len, buf, len);
    ch->tcp_response_backlog_len += len;
    return 0;
}

static ssize_t tcp_send_nonblocking(int fd, const void *buf, size_t len) {
    if (len == 0)
        return 0;
    ssize_t n = send(fd, buf, len, MSG_DONTWAIT);
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        return 0;
    return n;
}

static int flush_tcp_response_backlog(vemb_v16_channel_t *ch) {
    if (!ch || ch->net_fd < 0 || !tcp_response_backlog_pending(ch))
        return 1;

    size_t pending = ch->tcp_response_backlog_len - ch->tcp_response_backlog_sent;
    ssize_t n = tcp_send_nonblocking(ch->net_fd,
                                     ch->tcp_response_backlog + ch->tcp_response_backlog_sent,
                                     pending);
    if (n < 0)
        return -1;
    ch->tcp_response_backlog_sent += (size_t)n;
    if (ch->tcp_response_backlog_sent == ch->tcp_response_backlog_len) {
        ch->tcp_response_backlog_len = 0;
        ch->tcp_response_backlog_sent = 0;
        return 1;
    }
    return 0;
}

/// TCP transport: compute one response frame size.
static size_t tcp_response_wire_size(vemb_v16_resp_t *resp,
                                     uint32_t vector_bytes) {
    (void)resp;
    return sizeof(vemb_v16_net_hdr_t) + sizeof(vemb_v16_resp_t) + vector_bytes;
}

/// TCP transport: encode one response frame, optionally including inline vector bytes.
static uint8_t *encode_tcp_response_bytes(vemb_v16_channel_t *ch,
                                          vemb_v16_resp_t *resp,
                                          size_t *out_len) {
    const uint8_t *vector = NULL;
    uint32_t vector_bytes = 0;
    tcp_response_vector_slice(ch, resp, &vector, &vector_bytes);

    size_t bytes = tcp_response_wire_size(resp, vector_bytes);
    uint8_t *buf = zmalloc(bytes);
    if (!buf)
        return NULL;

    vemb_v16_net_hdr_t hdr = {
        .magic = VEMB_V16_MAGIC,
        .version = VEMB_V16_VERSION,
        .type = VEMB_V16_NET_RESPONSE,
        .flags = vector_bytes ? VEMB_V16_NET_F_INLINE_VECTOR : 0,
        .payload_len = (uint32_t)sizeof(*resp) + vector_bytes,
        .channel_id = ch->channel_id,
        .req_id = resp->req_id,
    };
    size_t off = 0;
    memcpy(buf + off, &hdr, sizeof(hdr));
    off += sizeof(hdr);
    memcpy(buf + off, resp, sizeof(*resp));
    off += sizeof(*resp);
    if (vector_bytes) {
        memcpy(buf + off, vector, vector_bytes);
        off += vector_bytes;
    }
    if (out_len) *out_len = off;
    return buf;
}

/// TCP transport: encode a batch of response frames for nonblocking writes.
static uint8_t *encode_tcp_response_batch(vemb_v16_channel_t *ch,
                                          const vemb_v16_completion_t *completions,
                                          uint32_t n,
                                          uint32_t *published,
                                          size_t *out_len) {
    vemb_v16_resp_t responses[VEMB_V16_PROXY_BATCH];
    const uint8_t *vectors[VEMB_V16_PROXY_BATCH];
    uint32_t vector_bytes[VEMB_V16_PROXY_BATCH];
    vemb_v16_net_hdr_t headers[VEMB_V16_PROXY_BATCH];
    uint32_t out = 0;
    size_t total_bytes = 0;

    for (uint32_t i = 0; i < n; i++) {
        if (completions[i].channel_id != ch->channel_id ||
            !atomic_load_explicit(&ch->active, memory_order_acquire)) {
            continue;
        }
        fill_response_from_completion(&responses[out], &completions[i]);
        vectors[out] = NULL;
        vector_bytes[out] = 0;
        tcp_response_vector_slice(ch, &responses[out], &vectors[out], &vector_bytes[out]);
        headers[out] = (vemb_v16_net_hdr_t){
            .magic = VEMB_V16_MAGIC,
            .version = VEMB_V16_VERSION,
            .type = VEMB_V16_NET_RESPONSE,
            .flags = vector_bytes[out] ? VEMB_V16_NET_F_INLINE_VECTOR : 0,
            .payload_len = (uint32_t)sizeof(responses[out]) + vector_bytes[out],
            .channel_id = ch->channel_id,
            .req_id = responses[out].req_id,
        };
        total_bytes += sizeof(headers[out]) + sizeof(responses[out]) + vector_bytes[out];
        out++;
    }

    if (published) *published = out;
    if (out_len) *out_len = total_bytes;
    if (out == 0)
        return NULL;

    uint8_t *buf = zmalloc(total_bytes);
    if (!buf)
        return NULL;
    size_t off = 0;
    for (uint32_t i = 0; i < out; i++) {
        memcpy(buf + off, &headers[i], sizeof(headers[i]));
        off += sizeof(headers[i]);
        memcpy(buf + off, &responses[i], sizeof(responses[i]));
        off += sizeof(responses[i]);
        if (vector_bytes[i]) {
            memcpy(buf + off, vectors[i], vector_bytes[i]);
            off += vector_bytes[i];
        }
    }
    return buf;
}
#endif

/// TCP transport: write one completion response to the socket.
static int publish_tcp_response(vemb_v16_channel_t *ch, vemb_v16_resp_t *resp) {
    if (!ch->tcp_backpressure_enabled) {
        const uint8_t *vector = NULL;
        uint32_t vector_bytes = 0;
        tcp_response_vector_slice(ch, resp, &vector, &vector_bytes);
        return vemb_v16_net_write_frame2(ch->net_fd,
                                         VEMB_V16_NET_RESPONSE,
                                         vector_bytes ? VEMB_V16_NET_F_INLINE_VECTOR : 0,
                                         ch->channel_id,
                                         resp->req_id,
                                         resp,
                                         (uint32_t)sizeof(*resp),
                                         vector,
                                         vector_bytes);
    }

#ifdef __linux__
    size_t bytes = 0;
    uint8_t *buf = encode_tcp_response_bytes(ch, resp, &bytes);
    if (!buf)
        return -1;
    int rc = 0;
    if (tcp_response_backlog_pending(ch)) {
        rc = append_tcp_response_backlog(ch, buf, bytes);
    } else {
        ssize_t n = tcp_send_nonblocking(ch->net_fd, buf, bytes);
        if (n < 0) {
            rc = -1;
        } else if ((size_t)n < bytes) {
            rc = append_tcp_response_backlog(ch, buf + n, bytes - (size_t)n);
        }
    }
    zfree(buf);
    return rc;
#else
    return -1;
#endif
}

static void fill_response_from_completion(vemb_v16_resp_t *resp,
                                          const vemb_v16_completion_t *completion) {
    *resp = (vemb_v16_resp_t){
        .status = completion->status,
        .op = completion->op,
        .flags = completion->flags,
        .req_id = completion->req_id,
        .key_hash = completion->key_hash,
        .vector_offset = completion->vector_offset,
        .vector_bytes = completion->vector_bytes,
        .dim = completion->dim,
        .region_id = completion->region_id,
    };
}

/// TCP transport: batch completion responses into writev/backlog output.
static int publish_tcp_response_batch(vemb_v16_channel_t *ch,
                                      const vemb_v16_completion_t *completions,
                                      uint32_t n,
                                      uint32_t *published) {
    if (!ch->tcp_backpressure_enabled) {
        vemb_v16_resp_t *responses =
            zmalloc(sizeof(*responses) * VEMB_V16_PROXY_BATCH);
        vemb_v16_net_hdr_t *headers =
            zmalloc(sizeof(*headers) * VEMB_V16_PROXY_BATCH);
        struct iovec *iov =
            zmalloc(sizeof(*iov) * VEMB_V16_PROXY_BATCH * 3u);
        if (!responses || !headers || !iov) {
            zfree(responses);
            zfree(headers);
            zfree(iov);
            if (published) *published = 0;
            return -1;
        }
        int iovcnt = 0;
        uint32_t out = 0;

        for (uint32_t i = 0; i < n; i++) {
            if (completions[i].channel_id != ch->channel_id ||
                !atomic_load_explicit(&ch->active, memory_order_acquire)) {
                continue;
            }

            fill_response_from_completion(&responses[out], &completions[i]);
            const uint8_t *vector = NULL;
            uint32_t vector_bytes = 0;
            tcp_response_vector_slice(ch, &responses[out], &vector, &vector_bytes);

            headers[out] = (vemb_v16_net_hdr_t){
                .magic = VEMB_V16_MAGIC,
                .version = VEMB_V16_VERSION,
                .type = VEMB_V16_NET_RESPONSE,
                .flags = vector_bytes ? VEMB_V16_NET_F_INLINE_VECTOR : 0,
                .payload_len = (uint32_t)sizeof(responses[out]) + vector_bytes,
                .channel_id = ch->channel_id,
                .req_id = responses[out].req_id,
            };
            iov[iovcnt++] = (struct iovec){ .iov_base = &headers[out], .iov_len = sizeof(headers[out]) };
            iov[iovcnt++] = (struct iovec){ .iov_base = &responses[out], .iov_len = sizeof(responses[out]) };
            if (vector_bytes) {
                iov[iovcnt++] = (struct iovec){ .iov_base = (void *)vector, .iov_len = vector_bytes };
            }
            out++;
        }

        if (published) *published = out;
        if (out == 0) {
            zfree(responses);
            zfree(headers);
            zfree(iov);
            return 0;
        }
        int rc = vemb_v16_net_writev_full(ch->net_fd, iov, iovcnt);
        zfree(responses);
        zfree(headers);
        zfree(iov);
        return rc;
    }

#ifdef __linux__
    size_t bytes = 0;
    uint32_t out = 0;
    uint8_t *buf = encode_tcp_response_batch(ch, completions, n, &out, &bytes);
    if (published) *published = out;
    if (out == 0)
        return 0;
    if (!buf)
        return -1;

    int rc = 0;
    if (tcp_response_backlog_pending(ch)) {
        rc = append_tcp_response_backlog(ch, buf, bytes);
    } else {
        ssize_t sent = tcp_send_nonblocking(ch->net_fd, buf, bytes);
        if (sent < 0) {
            rc = -1;
        } else if ((size_t)sent < bytes) {
            rc = append_tcp_response_backlog(ch, buf + sent, bytes - (size_t)sent);
        }
    }
    zfree(buf);
    return rc;
#else
    if (published) *published = 0;
    return -1;
#endif
}

/// Response scheduling: route a completion back to TCP or UB/SHM clients.
static void publish_response(vemb_v16_channel_t *ch,
                             const vemb_v16_completion_t *completion) {
    vemb_v16_resp_t resp;
    fill_response_from_completion(&resp, completion);
    if (ch->transport_type == VEMB_V16_TRANSPORT_TCP) {
        if (ch->net_fd < 0 ||
            publish_tcp_response(ch, &resp) != 0) {
            if (ch->net_fd >= 0) {
                shutdown(ch->net_fd, SHUT_RDWR);
                close(ch->net_fd);
                ch->net_fd = -1;
            }
            return;
        }
    } else {
        while (vemb_v16_client_publish(ch->response_ring, &resp, sizeof(resp)) != 0 &&
               atomic_load_explicit(&ch->proxy->running, memory_order_relaxed) &&
               atomic_load_explicit(&ch->active, memory_order_acquire)) {
            atomic_fetch_add_explicit(&ch->stats.proxy_response_ring_full, 1,
                                      memory_order_relaxed);
            cpu_relax();
        }
    }
    atomic_fetch_add_explicit(&ch->stats.proxy_response_publish, 1,
                              memory_order_relaxed);
}

/// Job scheduling: route VADD/VEMB work from proxy IO to a SuperNode shard queue.
static int publish_shard_job(vemb_v16_channel_t *ch,
                             const void *job,
                             uint32_t proxy_io_worker_id,
                             vemb_v16_shard_queue_t *queues,
                             atomic_uint_fast64_t *ring_full_counter) {
    assert(ch != NULL);
    assert(job != NULL);
    vemb_v16_proxy_t *proxy = ch->proxy;
    assert(shard_queue_topology_ready(proxy));
    assert(queues != NULL);
    assert(ring_full_counter != NULL);
    assert(proxy_io_worker_id < proxy->vemb_shard_proxy_count);

    uint32_t supernode_id = ch->index % proxy->vemb_shard_supernode_count;
    uint32_t queue_index =
        shard_queue_index(proxy, proxy_io_worker_id, supernode_id);
    vemb_v16_aeron_ring_t *ring = &queues[queue_index].ring;

    while (vemb_v16_aeron_publish(ring, job) != 0 &&
           atomic_load_explicit(&proxy->running, memory_order_relaxed) &&
           atomic_load_explicit(&ch->active, memory_order_acquire)) {
        atomic_fetch_add_explicit(ring_full_counter, 1,
                                  memory_order_relaxed);
        cpu_relax();
    }
#ifdef __linux__
    vemb_v16_supernode_pool_worker_t *worker =
        &proxy->supernode_workers[supernode_id];
    int expected = 1;
    if (atomic_compare_exchange_strong_explicit(&worker->job_notify_armed,
                                                &expected,
                                                0,
                                                memory_order_acq_rel,
                                                memory_order_relaxed) &&
        worker->notify_fd >= 0) {
        uint64_t one = 1;
        (void)write(worker->notify_fd, &one, sizeof(one));
    }
#endif
    return atomic_load_explicit(&ch->active, memory_order_acquire) ? 0 : -1;
}

/// Request scheduling: validate protocol input and enqueue execution jobs.
static void handle_request(vemb_v16_channel_t *ch,
                           const vemb_v16_req_t *req,
                           int req_len,
                           uint32_t proxy_io_worker_id) {
    if (req->op == VEMB_V16_OP_PING) {
        vemb_v16_completion_t completion = {
            .status = VEMB_V16_STATUS_OK,
            .op = req->op,
            .req_id = req->req_id,
            .channel_index = ch->index,
            .channel_id = ch->channel_id,
        };
        atomic_fetch_add_explicit(&ch->stats.total_requests, 1,
                                  memory_order_relaxed);
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
        vemb_v16_vadd_job_t *job = zmalloc(sizeof(*job));
        if (!job) {
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
        *job = (vemb_v16_vadd_job_t){
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
        memcpy(job->base.key, req->key, key_len);
        memcpy(job->vector, req->vector, req->vector_bytes);
        if (publish_shard_job(ch,
                              job,
                              proxy_io_worker_id,
                              ch->proxy->vadd_shard_queues,
                              &ch->stats.proxy_vadd_ring_full) != 0) {
            zfree(job);
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
        zfree(job);
        atomic_fetch_add_explicit(&ch->stats.proxy_vadd_publish, 1,
                                  memory_order_relaxed);
    } else if (req->op == VEMB_V16_OP_VEMB_HANDLE ||
               req->op == VEMB_V16_OP_VEMB_SUPERNODE_READ) {
        vemb_v16_vemb_job_t *job = zmalloc(sizeof(*job));
        if (!job) {
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
        *job = (vemb_v16_vemb_job_t){
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
        memcpy(job->base.key, req->key, key_len);
        if (publish_shard_job(ch,
                              job,
                              proxy_io_worker_id,
                              ch->proxy->vemb_shard_queues,
                              &ch->stats.proxy_vemb_ring_full) != 0) {
            zfree(job);
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
        zfree(job);
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

/// Response scheduling: drain SuperNode completions and publish by transport.
static int drain_completions(vemb_v16_channel_t *ch) {
    vemb_v16_completion_t completions[VEMB_V16_PROXY_BATCH];
    uint32_t n;
    uint32_t total = 0;
    while ((n = vemb_v16_aeron_poll_batch(&ch->completion_ring,
                                          completions,
                                          VEMB_V16_PROXY_BATCH)) != 0) {
        total += n;
        atomic_fetch_add_explicit(&ch->stats.proxy_completion_poll, n,
                                  memory_order_relaxed);
        if (ch->transport_type == VEMB_V16_TRANSPORT_TCP) {
            uint32_t published = 0;
            if (ch->net_fd < 0 ||
                publish_tcp_response_batch(ch, completions, n, &published) != 0) {
                if (ch->net_fd >= 0) {
                    shutdown(ch->net_fd, SHUT_RDWR);
                    close(ch->net_fd);
                    ch->net_fd = -1;
                }
                return -1;
            }
            atomic_fetch_add_explicit(&ch->stats.proxy_response_publish,
                                      published,
                                      memory_order_relaxed);
            continue;
        }
        for (uint32_t i = 0; i < n; i++) {
            if (completions[i].channel_id == ch->channel_id &&
                atomic_load_explicit(&ch->active, memory_order_acquire)) {
                publish_response(ch, &completions[i]);
            }
        }
    }
    return (int)total;
}

static int tcp_poll_input(int fd) {
    if (fd < 0) return -1;
    struct pollfd pfd = {
        .fd = fd,
        .events = POLLIN,
    };
    int pr = poll(&pfd, 1, 0);
    if (pr < 0) return errno == EINTR ? 0 : -1;
    if (pr == 0) return 0;
    if (pfd.revents & (POLLERR | POLLNVAL))
        return -1;
    if (pfd.revents & POLLIN)
        return 1;
    if (pfd.revents & POLLHUP)
        return -1;
    return 0;
}

/// TCP transport: read one request frame and hand it to the scheduler.
static int channel_read_tcp_request(vemb_v16_channel_t *ch,
                                    uint32_t proxy_io_worker_id) {
    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(ch->net_fd, &hdr) != 0)
        return -1;
    if (hdr.type == VEMB_V16_NET_CLOSE)
        return -1;
    if (hdr.type != VEMB_V16_NET_REQUEST ||
        hdr.channel_id != ch->channel_id ||
        hdr.payload_len == 0 ||
        hdr.payload_len > sizeof(vemb_v16_req_t)) {
        return -1;
    }

    vemb_v16_req_t *req = zmalloc(sizeof(*req));
    if (!req)
        return -1;
    memset(req, 0, sizeof(*req));
    if (vemb_v16_net_read_full(ch->net_fd, req, hdr.payload_len) != 0) {
        zfree(req);
        return -1;
    }
    handle_request(ch, req, (int)hdr.payload_len, proxy_io_worker_id);
    zfree(req);
    if (ch->net_fd < 0)
        return -1;
    return 1;
}

/// UB/SHM transport: poll client request ring and hand jobs to the scheduler.
static int channel_poll_shm_requests(vemb_v16_channel_t *ch,
                                     uint32_t proxy_io_worker_id) {
    if (!ch || !ch->request_ring)
        return -1;

    vemb_v16_req_t *req_buf =
        zmalloc(sizeof(*req_buf) * VEMB_V16_PROXY_BATCH);
    if (!req_buf)
        return -1;

    uint32_t req_count = vemb_v16_client_poll_batch(ch->request_ring,
                                                    req_buf,
                                                    sizeof(req_buf[0]),
                                                    VEMB_V16_PROXY_BATCH);
    if (req_count == 0) {
        zfree(req_buf);
        return 0;
    }

    atomic_fetch_add_explicit(&ch->stats.proxy_request_poll, req_count,
                              memory_order_relaxed);
    for (uint32_t i = 0; i < req_count; i++) {
        handle_request(ch, &req_buf[i],
                       (int)ch->request_ring->slot_size,
                       proxy_io_worker_id);
    }
    zfree(req_buf);
    atomic_fetch_add_explicit(&ch->stats.channel_ops, req_count,
                              memory_order_relaxed);
    return (int)req_count;
}

/// TCP transport: read a bounded batch of already-ready request frames.
static int channel_read_ready_tcp_requests(vemb_v16_channel_t *ch,
                                           uint32_t proxy_io_worker_id) {
    uint32_t count = 0;
    int ready = 0;
    while (count < VEMB_V16_PROXY_BATCH) {
        if (channel_read_tcp_request(ch, proxy_io_worker_id) < 0)
            return -1;
        count++;
        if (count >= VEMB_V16_PROXY_BATCH)
            break;
        ready = tcp_poll_input(ch->net_fd);
        if (ready < 0)
            return -1;
        if (ready == 0)
            break;
    }

    atomic_fetch_add_explicit(&ch->stats.proxy_request_poll, count,
                              memory_order_relaxed);
    atomic_fetch_add_explicit(&ch->stats.channel_ops, count,
                              memory_order_relaxed);
    return (int)count;
}

/// Control plane: fill the channel descriptor returned to TCP or UB/SHM clients.
static void fill_channel_desc(vemb_v16_proxy_t *proxy,
                              vemb_v16_channel_t *ch,
                              vemb_v16_channel_desc_t *desc) {
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
    vemb_v16_storage_fill_channel_desc(proxy_storage(proxy), desc);
}

static void cleanup_unstarted_channel(vemb_v16_channel_t *ch) {
    assert(ch != NULL);
    atomic_store_explicit(&ch->slot_channel_id, 0, memory_order_release);
    atomic_store_explicit(&ch->active, 0, memory_order_release);
    destroy_shared_ring(ch->request_ring_name, ch->request_ring,
                        ch->request_ring_bytes);
    destroy_shared_ring(ch->response_ring_name, ch->response_ring,
                        ch->response_ring_bytes);
    if (ch->net_fd >= 0) {
        shutdown(ch->net_fd, SHUT_RDWR);
        close(ch->net_fd);
    }
    vemb_v16_channel_free_slots(ch);
    reset_closed_channel(ch);
}

/// Control plane: allocate channel state for either TCP sockets or UB/SHM rings.
static int alloc_channel_common(vemb_v16_proxy_t *proxy,
                                uint32_t transport_type,
                                int net_fd,
                                vemb_v16_channel_desc_t *desc) {
    uint32_t idx = VEMB_V16_MAX_CHANNELS;
    uint32_t start = atomic_fetch_add_explicit(&proxy->next_channel_index, 1,
                                               memory_order_relaxed);
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        uint32_t candidate = (start + i) % VEMB_V16_MAX_CHANNELS;
        if (atomic_load_explicit(&proxy->channels[candidate].slot_channel_id,
                                 memory_order_acquire) == 0) {
            idx = candidate;
            break;
        }
    }
    if (idx >= VEMB_V16_MAX_CHANNELS) {
        if (transport_type == VEMB_V16_TRANSPORT_TCP && net_fd >= 0)
            close(net_fd);
        return -1;
    }

    vemb_v16_channel_t *ch = &proxy->channels[idx];
    reset_closed_channel(ch);
    ch->index = idx;
    ch->channel_id = atomic_fetch_add_explicit(&proxy->next_channel_id, 1,
                                               memory_order_relaxed);
    ch->proxy = proxy;
    ch->transport_type = transport_type;
    ch->net_fd = net_fd;
    atomic_store_explicit(&ch->active, 0, memory_order_release);
    atomic_store_explicit(&ch->proxy_io_registered, 0, memory_order_release);
    atomic_store_explicit(&ch->proxy_io_state, 0, memory_order_release);
    atomic_store_explicit(&ch->supernode_state, 0, memory_order_release);
    if (posix_memalign(&ch->completion_slots, 64,
                       sizeof(vemb_v16_completion_t) *
                       VEMB_V16_COMPLETION_RING_SIZE) != 0) {
        cleanup_unstarted_channel(ch);
        return -1;
    }
    if (vemb_v16_aeron_ring_init(&ch->completion_ring,
                                 ch->completion_slots,
                                 sizeof(vemb_v16_completion_t),
                                 VEMB_V16_COMPLETION_RING_SIZE) != 0) {
        cleanup_unstarted_channel(ch);
        return -1;
    }

    if (transport_type == VEMB_V16_TRANSPORT_SHM) {
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
            cleanup_unstarted_channel(ch);
            return -1;
        }
        if (create_shared_ring(ch->response_ring_name,
                               proxy->response_ring_slot_size,
                               &ch->response_ring,
                               &ch->response_ring_bytes) != 0) {
            cleanup_unstarted_channel(ch);
            return -1;
        }
    }

    ch->supernode_ctx = (vemb_v16_supernode_ctx_t){
        .worker_id = ch->index,
        .channel_active = &ch->active,
        .running = &proxy->running,
        .completion_notify_armed = NULL,
        .completion_notify_fd = NULL,
        .completion_ring = &ch->completion_ring,
        .storage = proxy->storage,
        .stats = &ch->stats,
    };

    atomic_store_explicit(&ch->active, 1, memory_order_release);

    int pooled_proxy_io = proxy->proxy_io_worker_count != 0;
#ifdef __linux__
    ch->tcp_backpressure_enabled =
        transport_type == VEMB_V16_TRANSPORT_TCP && pooled_proxy_io;
#else
    ch->tcp_backpressure_enabled = 0;
#endif
    if (pooled_proxy_io) {
        ch->supernode_ctx.completion_notify_armed =
            &ch->completion_notify_armed;
#ifdef __linux__
        uint32_t worker_id = ch->index % proxy->proxy_io_worker_count;
        ch->supernode_ctx.completion_notify_fd =
            &proxy->proxy_io_workers[worker_id].notify_fd;
#endif
    }
    atomic_store_explicit(&ch->slot_channel_id, ch->channel_id,
                          memory_order_release);

    serverLog(LL_VERBOSE, "vemb_v16 channel allocated: index=%u channel_id=%llu transport=%s req=%s resp=%s",
              ch->index,
              (unsigned long long)ch->channel_id,
              vemb_v16_transport_name(ch->transport_type),
              ch->request_ring_name,
              ch->response_ring_name);

    if (desc) fill_channel_desc(proxy, ch, desc);
    return 0;
}

/// UB/SHM control plane: allocate a shared-memory client channel.
static int alloc_channel(vemb_v16_proxy_t *proxy, vemb_v16_channel_desc_t *desc) {
    return alloc_channel_common(proxy, VEMB_V16_TRANSPORT_SHM, -1, desc);
}

/// TCP control plane: attach an accepted socket to a channel.
static int alloc_tcp_channel(vemb_v16_proxy_t *proxy,
                             int net_fd,
                             vemb_v16_channel_desc_t *desc) {
    return alloc_channel_common(proxy, VEMB_V16_TRANSPORT_TCP, net_fd, desc);
}

/// Control plane: close a channel and wait for proxy IO/SuperNode users to leave.
static void close_channel(vemb_v16_channel_t *ch) {
    if (!ch ||
        atomic_load_explicit(&ch->slot_channel_id, memory_order_acquire) == 0) {
        return;
    }
    serverLog(LL_VERBOSE, "vemb_v16 channel closing: index=%u channel_id=%llu",
              ch->index, (unsigned long long)ch->channel_id);
    int pooled_proxy_io = ch->proxy->proxy_io_worker_count != 0;
    int pooled_supernode = ch->proxy->supernode_worker_count != 0;
    atomic_exchange_explicit(&ch->active, 0, memory_order_acq_rel);
    if (pooled_proxy_io)
        proxy_io_channel_close_begin(ch);
    if (pooled_supernode)
        supernode_channel_close_begin(ch);
    if (ch->transport_type == VEMB_V16_TRANSPORT_TCP && ch->net_fd >= 0)
        shutdown(ch->net_fd, SHUT_RDWR);
    if (pooled_proxy_io)
        proxy_io_channel_wait_closed(ch);
    if (pooled_supernode) {
        supernode_channel_wait_closed(ch);
    }
    pthread_mutex_lock(&ch->proxy->stats_lock);
    stats_add_channel_counters(&ch->proxy->closed_stats, &ch->stats);
    pthread_mutex_unlock(&ch->proxy->stats_lock);
    destroy_shared_ring(ch->request_ring_name, ch->request_ring,
                        ch->request_ring_bytes);
    destroy_shared_ring(ch->response_ring_name, ch->response_ring,
                        ch->response_ring_bytes);
    if (ch->net_fd >= 0) {
        close(ch->net_fd);
        ch->net_fd = -1;
    }
    vemb_v16_channel_free_slots(ch);
    reset_closed_channel(ch);
}

static int close_channel_by_id(vemb_v16_proxy_t *proxy, uint64_t channel_id) {
    assert(proxy != NULL);
    if (channel_id == 0) return -1;
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        vemb_v16_channel_t *ch = &proxy->channels[i];
        if (atomic_load_explicit(&ch->slot_channel_id,
                                 memory_order_acquire) == channel_id) {
            close_channel(ch);
            return 0;
        }
    }
    return -1;
}

static uint64_t close_all_channels(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    uint64_t closed = 0;
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        vemb_v16_channel_t *ch = &proxy->channels[i];
        if (atomic_load_explicit(&ch->slot_channel_id,
                                 memory_order_acquire) != 0) {
            close_channel(ch);
            closed++;
        }
    }
    return closed;
}

static void reap_inactive_tcp_channels(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        vemb_v16_channel_t *ch = &proxy->channels[i];
        if (atomic_load_explicit(&ch->slot_channel_id,
                                 memory_order_acquire) != 0 &&
            ch->transport_type == VEMB_V16_TRANSPORT_TCP &&
            !atomic_load_explicit(&ch->active, memory_order_acquire)) {
            close_channel(ch);
        }
    }
}

/// Scheduling cleanup: mark a TCP or UB/SHM channel inactive from proxy IO.
static void proxy_io_channel_deactivate(vemb_v16_channel_t *ch) {
    assert(ch != NULL);
    proxy_io_channel_disarm_completion_notify(ch);
    atomic_store_explicit(&ch->active, 0, memory_order_release);
    if (ch->transport_type == VEMB_V16_TRANSPORT_TCP && ch->net_fd >= 0)
        shutdown(ch->net_fd, SHUT_RDWR);
}

static void proxy_io_set_affinity(uint32_t worker_id) {
#ifdef __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET((int)((worker_id * 2 + 1) % 64), &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
#else
    (void)worker_id;
#endif
}

/// Proxy IO scheduling: poll-based fallback for TCP sockets and UB/SHM rings.
static void *proxy_io_poll_thread_main(void *arg) {
    vemb_v16_proxy_io_worker_t *worker = arg;
    vemb_v16_proxy_t *proxy = worker->proxy;

    proxy_io_set_affinity(worker->worker_id);

    serverLog(LL_VERBOSE, "vemb_v16 proxy io poll worker started: worker_id=%u",
              worker->worker_id);
    while (atomic_load_explicit(&proxy->running, memory_order_relaxed)) {
        struct pollfd pfds[VEMB_V16_MAX_CHANNELS];
        vemb_v16_channel_t *poll_channels[VEMB_V16_MAX_CHANNELS];
        nfds_t nfds = 0;
        int did_work = 0;
        uint32_t worker_count = proxy->proxy_io_worker_count;
        if (worker_count == 0)
            break;

        for (uint32_t i = worker->worker_id;
             i < VEMB_V16_MAX_CHANNELS;
             i += worker_count) {
            vemb_v16_channel_t *ch = &proxy->channels[i];
            if (!proxy_io_channel_acquire(ch))
                continue;

            int n = drain_completions(ch);
            if (n < 0) {
                proxy_io_channel_deactivate(ch);
                did_work = 1;
                proxy_io_channel_release(ch);
                continue;
            }
            if (n > 0)
                did_work = 1;

            if (atomic_load_explicit(&ch->active, memory_order_acquire) &&
                ch->transport_type == VEMB_V16_TRANSPORT_SHM) {
                int rc = channel_poll_shm_requests(ch, worker->worker_id);
                if (rc < 0) {
                    proxy_io_channel_deactivate(ch);
                    did_work = 1;
                } else if (rc > 0) {
                    did_work = 1;
                }
                proxy_io_channel_release(ch);
                continue;
            }

            if (atomic_load_explicit(&ch->active, memory_order_acquire) &&
                ch->net_fd >= 0) {
                pfds[nfds] = (struct pollfd){
                    .fd = ch->net_fd,
                    .events = POLLIN,
                    .revents = 0,
                };
                poll_channels[nfds] = ch;
                nfds++;
            } else {
                proxy_io_channel_release(ch);
            }
        }

        int pr = 0;
        if (nfds > 0) {
            pr = poll(pfds, nfds, did_work ? 0 : 1);
            if (pr < 0 && errno == EINTR)
                pr = 0;
        }

        for (nfds_t i = 0; i < nfds; i++) {
            vemb_v16_channel_t *ch = poll_channels[i];
            short revents = pfds[i].revents;
            if (pr < 0) {
                proxy_io_channel_deactivate(ch);
                did_work = 1;
            } else if (revents & POLLIN) {
                int rc = channel_read_ready_tcp_requests(ch,
                                                         worker->worker_id);
                if (rc < 0) {
                    proxy_io_channel_deactivate(ch);
                } else if (rc > 0) {
                    did_work = 1;
                }
            } else if (revents & (POLLERR | POLLHUP | POLLNVAL)) {
                proxy_io_channel_deactivate(ch);
                did_work = 1;
            }
            proxy_io_channel_release(ch);
        }

        if (!did_work && nfds == 0)
            cpu_relax();
    }
    serverLog(LL_VERBOSE, "vemb_v16 proxy io poll worker stopped: worker_id=%u",
              worker->worker_id);
    return NULL;
}

#ifdef __linux__
/// Proxy IO scheduling: unregister one TCP fd from the Linux epoll worker.
static void proxy_io_epoll_unregister(vemb_v16_channel_t *ch,
                                      int epfd,
                                      uint64_t *registered_ids,
                                      int *registered_fds,
                                      uint32_t *registered_events,
                                      uint32_t index) {
    if (registered_ids[index] == 0)
        return;
    if (registered_fds[index] >= 0)
        epoll_ctl(epfd, EPOLL_CTL_DEL, registered_fds[index], NULL);
    registered_ids[index] = 0;
    registered_fds[index] = -1;
    registered_events[index] = 0;
    atomic_store_explicit(&ch->proxy_io_registered, 0, memory_order_release);
}

/// Proxy IO scheduling: epoll TCP sockets, poll UB/SHM rings, and drain completions.
static void *proxy_io_epoll_thread_main(void *arg) {
    vemb_v16_proxy_io_worker_t *worker = arg;
    vemb_v16_proxy_t *proxy = worker->proxy;
    uint64_t registered_ids[VEMB_V16_MAX_CHANNELS] = {0};
    int registered_fds[VEMB_V16_MAX_CHANNELS];
    uint32_t registered_events[VEMB_V16_MAX_CHANNELS];
    struct epoll_event events[VEMB_V16_MAX_CHANNELS];
    const uint64_t notify_token = UINT64_MAX;
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        registered_fds[i] = -1;
        registered_events[i] = 0;
    }

    int epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0)
        return proxy_io_poll_thread_main(arg);
    worker->notify_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (worker->notify_fd >= 0) {
        struct epoll_event ev = {
            .events = EPOLLIN,
            .data = {.u64 = notify_token},
        };
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, worker->notify_fd, &ev) != 0) {
            close(worker->notify_fd);
            worker->notify_fd = -1;
        }
    }

    proxy_io_set_affinity(worker->worker_id);

    serverLog(LL_VERBOSE, "vemb_v16 proxy io epoll worker started: worker_id=%u",
              worker->worker_id);
    while (atomic_load_explicit(&proxy->running, memory_order_relaxed)) {
        int did_work = 0;
        uint32_t worker_count = proxy->proxy_io_worker_count;
        if (worker_count == 0)
            break;

        for (uint32_t i = worker->worker_id;
             i < VEMB_V16_MAX_CHANNELS;
             i += worker_count) {
            vemb_v16_channel_t *ch = &proxy->channels[i];
            uint64_t channel_id =
                atomic_load_explicit(&ch->slot_channel_id,
                                     memory_order_acquire);
            int active = channel_id != 0 &&
                ch->transport_type == VEMB_V16_TRANSPORT_TCP &&
                atomic_load_explicit(&ch->active, memory_order_acquire) &&
                ch->net_fd >= 0;
            int fd = active ? ch->net_fd : -1;

            if (registered_ids[i] != 0 &&
                (!active ||
                 registered_ids[i] != channel_id ||
                 registered_fds[i] != fd)) {
                proxy_io_epoll_unregister(ch,
                                          epfd,
                                          registered_ids,
                                          registered_fds,
                                          registered_events,
                                          i);
                did_work = 1;
            }

            if (!active)
                continue;

            if (!proxy_io_channel_acquire(ch))
                continue;
            proxy_io_channel_disarm_completion_notify(ch);

            channel_id = atomic_load_explicit(&ch->slot_channel_id,
                                              memory_order_acquire);
            int channel_active = channel_id != 0 &&
                atomic_load_explicit(&ch->active, memory_order_acquire);
            if (!channel_active) {
                proxy_io_channel_release(ch);
                continue;
            }

            if (ch->transport_type == VEMB_V16_TRANSPORT_SHM) {
                int n = drain_completions(ch);
                if (n < 0) {
                    proxy_io_channel_deactivate(ch);
                    did_work = 1;
                } else if (n > 0) {
                    did_work = 1;
                }
                int rc = channel_poll_shm_requests(ch, worker->worker_id);
                if (rc < 0) {
                    proxy_io_channel_deactivate(ch);
                    did_work = 1;
                } else if (rc > 0) {
                    did_work = 1;
                } else if (!did_work) {
                    if (proxy_io_channel_arm_completion_notify(ch) < 0)
                        did_work = 1;
                }
                proxy_io_channel_release(ch);
                continue;
            }

            active = ch->transport_type == VEMB_V16_TRANSPORT_TCP &&
                ch->net_fd >= 0;
            fd = active ? ch->net_fd : -1;
            if (!active) {
                proxy_io_channel_release(ch);
                continue;
            }

            if (registered_ids[i] == 0) {
                uint32_t desired_events = EPOLLIN | EPOLLERR | EPOLLHUP;
                if (tcp_response_backlog_pending(ch))
                    desired_events |= EPOLLOUT;
                struct epoll_event ev = {
                    .events = desired_events,
                    .data = {.u32 = i},
                };
                if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) == 0) {
                    registered_ids[i] = channel_id;
                    registered_fds[i] = fd;
                    registered_events[i] = desired_events;
                    atomic_store_explicit(&ch->proxy_io_registered, 1,
                                          memory_order_release);
                } else {
                    proxy_io_channel_deactivate(ch);
                    did_work = 1;
                    proxy_io_channel_release(ch);
                    continue;
                }
            }

            uint32_t desired_events = EPOLLIN | EPOLLERR | EPOLLHUP;
            if (tcp_response_backlog_pending(ch))
                desired_events |= EPOLLOUT;
            if (registered_ids[i] != 0 &&
                registered_events[i] != desired_events) {
                struct epoll_event ev = {
                    .events = desired_events,
                    .data = {.u32 = i},
                };
                if (epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev) == 0) {
                    registered_events[i] = desired_events;
                } else {
                    proxy_io_channel_deactivate(ch);
                    did_work = 1;
                    proxy_io_channel_release(ch);
                    continue;
                }
            }

            if (tcp_response_backlog_pending(ch)) {
                int flush = flush_tcp_response_backlog(ch);
                if (flush < 0) {
                    proxy_io_channel_deactivate(ch);
                    did_work = 1;
                    proxy_io_channel_release(ch);
                    continue;
                }
                if (flush == 1)
                    did_work = 1;
            }
            int n = drain_completions(ch);
            if (n < 0) {
                proxy_io_channel_deactivate(ch);
                did_work = 1;
            } else if (n > 0) {
                did_work = 1;
            } else if (proxy_io_channel_arm_completion_notify(ch) < 0) {
                did_work = 1;
            }
            proxy_io_channel_release(ch);
        }

        int timeout_ms = did_work ? 0 : 10;
        int nready = epoll_wait(epfd,
                                events,
                                VEMB_V16_MAX_CHANNELS,
                                timeout_ms);
        if (nready < 0) {
            if (errno == EINTR)
                continue;
            break;
        }

        for (int i = 0; i < nready; i++) {
            if (events[i].data.u64 == notify_token) {
                uint64_t value = 0;
                if (worker->notify_fd >= 0)
                    (void)read(worker->notify_fd, &value, sizeof(value));
                did_work = 1;
                continue;
            }

            uint32_t index = events[i].data.u32;
            if (index >= VEMB_V16_MAX_CHANNELS)
                continue;
            vemb_v16_channel_t *ch = &proxy->channels[index];
            if (!proxy_io_channel_acquire(ch))
                continue;
            proxy_io_channel_disarm_completion_notify(ch);
            uint64_t channel_id =
                atomic_load_explicit(&ch->slot_channel_id,
                                     memory_order_acquire);
            if (registered_ids[index] != channel_id) {
                proxy_io_channel_release(ch);
                continue;
            }

            uint32_t revents = events[i].events;
            if (revents & EPOLLIN) {
                int rc = channel_read_ready_tcp_requests(ch,
                                                         worker->worker_id);
                if (rc < 0) {
                    proxy_io_channel_deactivate(ch);
                }
            }
            if ((revents & EPOLLOUT) && atomic_load_explicit(&ch->active, memory_order_acquire)) {
                if (flush_tcp_response_backlog(ch) < 0)
                    proxy_io_channel_deactivate(ch);
            }
            if (revents & (EPOLLERR | EPOLLHUP)) {
                proxy_io_channel_deactivate(ch);
            }
            proxy_io_channel_release(ch);
        }
    }

    for (uint32_t i = worker->worker_id;
         i < VEMB_V16_MAX_CHANNELS;
         i += proxy->proxy_io_worker_count) {
        proxy_io_epoll_unregister(&proxy->channels[i],
                                  epfd,
                                  registered_ids,
                                  registered_fds,
                                  registered_events,
                                  i);
    }
    if (worker->notify_fd >= 0) {
        close(worker->notify_fd);
        worker->notify_fd = -1;
    }
    close(epfd);
    serverLog(LL_VERBOSE, "vemb_v16 proxy io epoll worker stopped: worker_id=%u",
              worker->worker_id);
    return NULL;
}
#endif

static void *proxy_io_pool_thread_main(void *arg) {
#ifdef __linux__
    return proxy_io_epoll_thread_main(arg);
#else
    return proxy_io_poll_thread_main(arg);
#endif
}

static int start_proxy_io_pool(vemb_v16_proxy_t *proxy) {
    if (validate_pooled_worker_config(proxy) != 0)
        return -1;
    for (uint32_t i = 0; i < proxy->proxy_io_worker_count; i++) {
        proxy->proxy_io_workers[i] = (vemb_v16_proxy_io_worker_t){
            .worker_id = i,
            .proxy = proxy,
#ifdef __linux__
            .notify_fd = -1,
#endif
        };
        if (pthread_create(&proxy->proxy_io_workers[i].thread,
                           NULL,
                           proxy_io_pool_thread_main,
                           &proxy->proxy_io_workers[i]) != 0) {
            atomic_store_explicit(&proxy->running, 0, memory_order_relaxed);
            for (uint32_t j = 0; j < i; j++)
                pthread_join(proxy->proxy_io_workers[j].thread, NULL);
            proxy->proxy_io_pool_started = 0;
            return -1;
        }
    }
    proxy->proxy_io_pool_started = 1;
    return 0;
}

static void stop_proxy_io_pool(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    if (!proxy->proxy_io_pool_started)
        return;
    for (uint32_t i = 0; i < proxy->proxy_io_worker_count; i++)
        pthread_join(proxy->proxy_io_workers[i].thread, NULL);
    proxy->proxy_io_pool_started = 0;
}

typedef void (*shard_job_apply_fn)(vemb_v16_supernode_ctx_t *ctx,
                                   void *job,
                                   vemb_v16_supernode_scratch_t *scratch,
                                   vemb_v16_channel_t *ch);

/// Execution: run one VEMB job on the SuperNode storage/backend path.
static void apply_vemb_shard_job(vemb_v16_supernode_ctx_t *ctx,
                                 void *job,
                                 vemb_v16_supernode_scratch_t *scratch,
                                 vemb_v16_channel_t *ch) {
    atomic_fetch_add_explicit(&ch->stats.supernode_vemb_poll, 1,
                              memory_order_relaxed);
    vemb_v16_supernode_handle_vemb_job(ctx,
                                       job,
                                       &scratch->read_result,
                                       &scratch->read_result_bytes);
}

/// Execution: run one VADD job on the SuperNode storage/backend path.
static void apply_vadd_shard_job(vemb_v16_supernode_ctx_t *ctx,
                                 void *job,
                                 vemb_v16_supernode_scratch_t *scratch,
                                 vemb_v16_channel_t *ch) {
    (void)scratch;
    atomic_fetch_add_explicit(&ch->stats.supernode_vadd_poll, 1,
                              memory_order_relaxed);
    vemb_v16_supernode_handle_vadd_job(ctx, job);
}

/// Execution scheduling: drain shard queues assigned to one SuperNode worker.
static int drain_shard_queues(vemb_v16_proxy_t *proxy,
                              uint32_t supernode_worker_id,
                              vemb_v16_supernode_scratch_t *scratch,
                              vemb_v16_shard_queue_t *queues,
                              void *job_buf,
                              shard_job_apply_fn apply_job) {
    assert(proxy != NULL);
    assert(scratch != NULL);
    assert(queues != NULL);
    assert(job_buf != NULL);
    assert(apply_job != NULL);
    if (!shard_queue_topology_ready(proxy) ||
        supernode_worker_id >= proxy->vemb_shard_supernode_count) {
        return 0;
    }

    int did_work = 0;
    for (uint32_t proxy_id = 0;
         proxy_id < proxy->vemb_shard_proxy_count;
         proxy_id++) {
        uint32_t queue_index =
            shard_queue_index(proxy, proxy_id, supernode_worker_id);
        vemb_v16_aeron_ring_t *ring = &queues[queue_index].ring;
        uint32_t n = vemb_v16_aeron_poll_batch(ring, job_buf,
                                               VEMB_V16_PROXY_BATCH);
        if (!n)
            continue;
        did_work += (int)n;

        for (uint32_t i = 0; i < n; i++) {
            uint8_t *slot = (uint8_t *)job_buf + (size_t)i * ring->slot_size;
            vemb_v16_job_base_t *job_base = (vemb_v16_job_base_t *)slot;
            if (job_base->channel_index >= VEMB_V16_MAX_CHANNELS)
                continue;
            vemb_v16_channel_t *ch = &proxy->channels[job_base->channel_index];
            if (!supernode_channel_acquire(ch))
                continue;
            if (atomic_load_explicit(&ch->slot_channel_id,
                                     memory_order_acquire) ==
                    job_base->channel_id &&
                atomic_load_explicit(&ch->active, memory_order_acquire)) {
                vemb_v16_supernode_ctx_t ctx = ch->supernode_ctx;
                ctx.worker_id = supernode_worker_id;
                apply_job(&ctx, slot, scratch, ch);
            }
            supernode_channel_release(ch);
        }
    }
    return did_work;
}

/// Execution scheduling: drain VEMB read queues for one SuperNode worker.
static int drain_vemb_shard_queues(vemb_v16_proxy_t *proxy,
                                   uint32_t supernode_worker_id,
                                   vemb_v16_supernode_scratch_t *scratch) {
    assert(proxy != NULL);
    assert(scratch != NULL);
    return drain_shard_queues(proxy,
                              supernode_worker_id,
                              scratch,
                              proxy->vemb_shard_queues,
                              scratch->vemb_jobs,
                              apply_vemb_shard_job);
}

/// Execution scheduling: drain VADD write queues for one SuperNode worker.
static int drain_vadd_shard_queues(vemb_v16_proxy_t *proxy,
                                   uint32_t supernode_worker_id,
                                   vemb_v16_supernode_scratch_t *scratch) {
    assert(proxy != NULL);
    assert(scratch != NULL);
    return drain_shard_queues(proxy,
                              supernode_worker_id,
                              scratch,
                              proxy->vadd_shard_queues,
                              scratch->vadd_jobs,
                              apply_vadd_shard_job);
}

#ifdef __linux__
/// Execution scheduling: check whether a SuperNode worker can sleep.
static int supernode_worker_has_pending(vemb_v16_proxy_t *proxy,
                                        uint32_t worker_id) {
    if (!shard_queue_topology_ready(proxy) ||
        worker_id >= proxy->vemb_shard_supernode_count)
        return 0;

    for (uint32_t proxy_id = 0;
         proxy_id < proxy->vemb_shard_proxy_count;
         proxy_id++) {
        uint32_t queue_index = shard_queue_index(proxy, proxy_id, worker_id);
        if (vemb_v16_aeron_available(&proxy->vemb_shard_queues[queue_index].ring) != 0)
            return 1;
    }
    for (uint32_t proxy_id = 0;
         proxy_id < proxy->vemb_shard_proxy_count;
         proxy_id++) {
        uint32_t queue_index = shard_queue_index(proxy, proxy_id, worker_id);
        if (vemb_v16_aeron_available(&proxy->vadd_shard_queues[queue_index].ring) != 0)
            return 1;
    }

    return 0;
}

static void supernode_worker_wait_for_jobs(vemb_v16_supernode_pool_worker_t *worker) {
    if (!worker || worker->notify_fd < 0)
        return;

    atomic_store_explicit(&worker->job_notify_armed, 1, memory_order_release);
    if (supernode_worker_has_pending(worker->proxy, worker->worker_id)) {
        atomic_store_explicit(&worker->job_notify_armed, 0, memory_order_release);
        return;
    }

    struct pollfd pfd = {
        .fd = worker->notify_fd,
        .events = POLLIN,
    };
    int rc = poll(&pfd, 1, 10);
    if (rc > 0 && (pfd.revents & POLLIN)) {
        uint64_t value = 0;
        (void)read(worker->notify_fd, &value, sizeof(value));
    }
    atomic_store_explicit(&worker->job_notify_armed, 0, memory_order_release);
}
#endif

/// Execution worker: consume scheduled VEMB/VADD jobs and publish completions.
static void *supernode_pool_thread_main(void *arg) {
    vemb_v16_supernode_pool_worker_t *worker = arg;
    vemb_v16_proxy_t *proxy = worker->proxy;
    vemb_v16_supernode_scratch_t scratch;
    if (vemb_v16_supernode_scratch_init(&scratch) != 0)
        return NULL;
#ifdef __linux__
    worker->notify_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    atomic_store_explicit(&worker->job_notify_armed, 0, memory_order_release);
#endif

#ifdef __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET((int)((worker->worker_id * 2 + 2) % 64), &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
#endif

    serverLog(LL_VERBOSE, "vemb_v16 pooled supernode worker started: worker_id=%u",
              worker->worker_id);
    while (atomic_load_explicit(&proxy->running, memory_order_relaxed)) {
        int did_work = 0;
        if (drain_vemb_shard_queues(proxy, worker->worker_id, &scratch) > 0)
            did_work = 1;
        if (drain_vadd_shard_queues(proxy, worker->worker_id, &scratch) > 0)
            did_work = 1;
        if (!did_work) {
#ifdef __linux__
            supernode_worker_wait_for_jobs(worker);
#else
            cpu_relax();
#endif
        }
    }
    serverLog(LL_VERBOSE, "vemb_v16 pooled supernode worker stopped: worker_id=%u",
              worker->worker_id);
#ifdef __linux__
    if (worker->notify_fd >= 0) {
        close(worker->notify_fd);
        worker->notify_fd = -1;
    }
#endif
    vemb_v16_supernode_scratch_cleanup(&scratch);
    return NULL;
}

static int start_supernode_pool(vemb_v16_proxy_t *proxy) {
    if (validate_pooled_worker_config(proxy) != 0)
        return -1;
    for (uint32_t i = 0; i < proxy->supernode_worker_count; i++) {
        proxy->supernode_workers[i] = (vemb_v16_supernode_pool_worker_t){
            .worker_id = i,
            .proxy = proxy,
#ifdef __linux__
            .job_notify_armed = 0,
            .notify_fd = -1,
#endif
        };
        if (pthread_create(&proxy->supernode_workers[i].thread,
                           NULL,
                           supernode_pool_thread_main,
                           &proxy->supernode_workers[i]) != 0) {
            atomic_store_explicit(&proxy->running, 0, memory_order_relaxed);
            for (uint32_t j = 0; j < i; j++)
                pthread_join(proxy->supernode_workers[j].thread, NULL);
            proxy->supernode_pool_started = 0;
            return -1;
        }
    }
    proxy->supernode_pool_started = 1;
    return 0;
}

static void stop_supernode_pool(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    if (!proxy->supernode_pool_started)
        return;
    for (uint32_t i = 0; i < proxy->supernode_worker_count; i++)
        pthread_join(proxy->supernode_workers[i].thread, NULL);
    proxy->supernode_pool_started = 0;
}

/// TCP control plane: write a small status response frame.
static void tcp_write_status(int fd, uint8_t status, uint64_t value) {
    vemb_v16_net_status_t st = {
        .status = status,
        .value = value,
    };
    vemb_v16_net_write_frame(fd,
                             VEMB_V16_NET_CONTROL_STATUS,
                             0,
                             0,
                             0,
                             &st,
                             sizeof(st));
}

/// TCP control plane: process one accepted TCP control or channel setup socket.
static void handle_tcp_fd(vemb_v16_proxy_t *proxy, int fd) {
    assert(proxy != NULL);
    if (fd < 0) return;
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0)
        fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
    vemb_v16_net_set_tcp_nodelay(fd);
    vemb_v16_net_set_timeouts(fd, 10000);

    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0) {
        close(fd);
        return;
    }

    if (hdr.type == VEMB_V16_NET_STATS) {
        if (hdr.payload_len != 0) {
            close(fd);
            return;
        }
        vemb_v16_stats_t stats;
        vemb_v16_proxy_get_stats(proxy, &stats);
        vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_STATS,
                                 0,
                                 0,
                                 0,
                                 &stats,
                                 sizeof(stats));
        close(fd);
        return;
    }

    if (hdr.type == VEMB_V16_NET_CLOSE_CHANNEL) {
        if (hdr.payload_len != 0) {
            close(fd);
            return;
        }
        uint8_t status = close_channel_by_id(proxy, hdr.channel_id) == 0 ?
            VEMB_V16_STATUS_OK : VEMB_V16_STATUS_ERR;
        tcp_write_status(fd, status, 0);
        close(fd);
        return;
    }

    if (hdr.type == VEMB_V16_NET_CLOSE_ALL_CHANNELS) {
        if (hdr.payload_len != 0) {
            close(fd);
            return;
        }
        uint64_t closed = close_all_channels(proxy);
        tcp_write_status(fd, VEMB_V16_STATUS_OK, closed);
        close(fd);
        return;
    }

    vemb_v16_alloc_req_t req;
    if (hdr.type != VEMB_V16_NET_HELLO ||
        hdr.payload_len != sizeof(req) ||
        vemb_v16_net_read_full(fd, &req, sizeof(req)) != 0) {
        close(fd);
        return;
    }
    (void)req;

    vemb_v16_channel_desc_t desc;
    if (alloc_tcp_channel(proxy, fd, &desc) != 0) {
        return;
    }
    if (vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_WELCOME,
                                 0,
                                 desc.channel_id,
                                 0,
                                 &desc,
                                 sizeof(desc)) != 0) {
        close_channel_by_id(proxy, desc.channel_id);
    }
}

/// UDS control plane: allocate UB/SHM channels and serve stats/close commands.
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
                          vemb_v16_storage_ctx_t *storage) {
    assert(out != NULL);
    assert(uds_path != NULL);
    assert(uds_path[0] != '\0');
    assert(storage != NULL);
    assert(vector_dim != 0);
    assert(vector_dim <= VEMB_V16_MAX_DIM);
    assert(max_vectors != 0);

    vemb_v16_proxy_t *proxy = zcalloc(sizeof(*proxy));
    if (!proxy) return -1;
    strncpy(proxy->uds_path, uds_path, sizeof(proxy->uds_path) - 1);
    proxy->vector_dim = vector_dim;
    proxy->vector_stride = vector_dim * sizeof(float);
    proxy->request_ring_slot_size =
        (uint32_t)vemb_v16_req_inline_len(proxy->vector_stride);
    proxy->response_ring_slot_size = sizeof(vemb_v16_resp_t);
    proxy->max_vectors = max_vectors;
    proxy->uds_fd = -1;
    proxy->tcp_fd = -1;
    proxy->tcp_port = VEMB_V16_TCP_PORT;
    strncpy(proxy->tcp_host, VEMB_V16_TCP_HOST, sizeof(proxy->tcp_host) - 1);
    atomic_init(&proxy->running, 0);
    atomic_init(&proxy->next_channel_id, 1);
    atomic_init(&proxy->next_channel_index, 0);
    pthread_mutex_init(&proxy->stats_lock, NULL);
    proxy->storage = storage;

    serverLog(LL_NOTICE, "vemb_v16 proxy created: uds=%s dim=%u max_vectors=%u vector_region=%s size=%zu",
              proxy->uds_path,
              proxy->vector_dim,
              proxy->max_vectors,
              vemb_v16_storage_vector_region_name(proxy_storage(proxy)),
              vemb_v16_storage_vector_region_size(proxy_storage(proxy)));
    *out = proxy;
    return 0;
}

int vemb_v16_proxy_enable_tcp(vemb_v16_proxy_t *proxy,
                              const char *host,
                              uint16_t port) {
    assert(proxy != NULL);
    assert(host != NULL);
    assert(host[0] != '\0');
    assert(strlen(host) < sizeof(proxy->tcp_host));
    strncpy(proxy->tcp_host, host, sizeof(proxy->tcp_host) - 1);
    proxy->tcp_host[sizeof(proxy->tcp_host) - 1] = '\0';
    proxy->tcp_port = port ? port : VEMB_V16_TCP_PORT;
    proxy->tcp_enabled = 1;
    return 0;
}

static int proxy_set_worker_count(vemb_v16_proxy_t *proxy,
                                  int pool_started,
                                  uint32_t count,
                                  uint32_t *dst) {
    assert(proxy != NULL);
    assert(dst != NULL);
    assert(!pool_started);
    if (count == 0)
        return -1;
    if (count > VEMB_V16_MAX_CHANNELS)
        return -1;
    *dst = count;
    return 0;
}

int vemb_v16_proxy_set_supernode_workers(vemb_v16_proxy_t *proxy,
                                         uint32_t workers) {
    return proxy_set_worker_count(proxy,
                                  proxy->supernode_pool_started,
                                  workers,
                                  &proxy->supernode_worker_count);
}

int vemb_v16_proxy_set_proxy_io_threads(vemb_v16_proxy_t *proxy,
                                        uint32_t threads) {
    return proxy_set_worker_count(proxy,
                                  proxy->proxy_io_pool_started,
                                  threads,
                                  &proxy->proxy_io_worker_count);
}

void vemb_v16_proxy_destroy(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    vemb_v16_proxy_stop(proxy);
    stop_proxy_io_pool(proxy);
    stop_supernode_pool(proxy);
    free_vemb_shard_queues(proxy);
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++)
        close_channel(&proxy->channels[i]);
    if (proxy->uds_fd >= 0) close(proxy->uds_fd);
    if (proxy->tcp_fd >= 0) close(proxy->tcp_fd);
    if (proxy->uds_path[0]) unlink(proxy->uds_path);
    pthread_mutex_destroy(&proxy->stats_lock);
    zfree(proxy);
}

/// Top-level control loop: accept UDS UB/SHM control and optional TCP control.
int vemb_v16_proxy_run(vemb_v16_proxy_t *proxy) {
    if (validate_pooled_worker_config(proxy) != 0)
        return -1;
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
    if (proxy->tcp_enabled) {
        proxy->tcp_fd = vemb_v16_net_listen(proxy->tcp_host,
                                            proxy->tcp_port,
                                            4096);
        if (proxy->tcp_fd < 0) {
            serverLog(LL_WARNING, "vemb_v16 tcp listen failed: %s:%u errno=%d error=%s",
                      proxy->tcp_host, proxy->tcp_port, errno, strerror(errno));
            return -1;
        }
        fcntl(proxy->tcp_fd, F_SETFL,
              fcntl(proxy->tcp_fd, F_GETFL, 0) | O_NONBLOCK);
    }
    if (init_vemb_shard_queues(proxy) != 0) {
        serverLog(LL_WARNING, "vemb_v16 vemb shard queue init failed: proxy_io_threads=%u supernode_workers=%u",
                  proxy->proxy_io_worker_count,
                  proxy->supernode_worker_count);
        atomic_store_explicit(&proxy->running, 0, memory_order_relaxed);
        return -1;
    }
    if (start_supernode_pool(proxy) != 0) {
        serverLog(LL_WARNING, "vemb_v16 pooled supernode start failed: workers=%u",
                  proxy->supernode_worker_count);
        free_vemb_shard_queues(proxy);
        return -1;
    }
    if (start_proxy_io_pool(proxy) != 0) {
        serverLog(LL_WARNING, "vemb_v16 proxy io pool start failed: threads=%u",
                  proxy->proxy_io_worker_count);
        atomic_store_explicit(&proxy->running, 0, memory_order_relaxed);
        stop_supernode_pool(proxy);
        free_vemb_shard_queues(proxy);
        return -1;
    }

    serverLog(LL_NOTICE, "vemb_v16 server ready: uds=%s tcp=%s:%u tcp_enabled=%s proxy_io_threads=%u supernode_workers=%u dim=%u max_vectors=%u vector_region=%s",
              proxy->uds_path,
              proxy->tcp_host,
              proxy->tcp_port,
              proxy->tcp_enabled ? "yes" : "no",
              proxy->proxy_io_worker_count,
              proxy->supernode_worker_count,
              proxy->vector_dim,
              proxy->max_vectors,
              vemb_v16_storage_vector_region_name(proxy_storage(proxy)));

    while (atomic_load_explicit(&proxy->running, memory_order_relaxed)) {
        int did_work = 0;
        for (;;) {
            int cfd = accept(proxy->uds_fd, NULL, NULL);
            if (cfd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                    break;
                serverLog(LL_WARNING, "vemb_v16 uds accept failed: fd=%d errno=%d error=%s",
                          proxy->uds_fd, errno, strerror(errno));
                atomic_store_explicit(&proxy->running, 0, memory_order_relaxed);
                stop_proxy_io_pool(proxy);
                stop_supernode_pool(proxy);
                free_vemb_shard_queues(proxy);
                return -1;
            }
            did_work = 1;
            handle_control_fd(proxy, cfd);
        }
        if (proxy->tcp_enabled && proxy->tcp_fd >= 0) {
            for (;;) {
                int cfd = accept(proxy->tcp_fd, NULL, NULL);
                if (cfd < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                        break;
                    serverLog(LL_WARNING, "vemb_v16 tcp accept failed: fd=%d errno=%d error=%s",
                              proxy->tcp_fd, errno, strerror(errno));
                    atomic_store_explicit(&proxy->running, 0, memory_order_relaxed);
                    stop_proxy_io_pool(proxy);
                    stop_supernode_pool(proxy);
                    free_vemb_shard_queues(proxy);
                    return -1;
                }
                did_work = 1;
                handle_tcp_fd(proxy, cfd);
            }
        }
        if (!did_work) {
            struct timespec ts = {0, 1000000};
            nanosleep(&ts, NULL);
        }
        reap_inactive_tcp_channels(proxy);
    }
    stop_proxy_io_pool(proxy);
    stop_supernode_pool(proxy);
    free_vemb_shard_queues(proxy);
    return 0;
}

void vemb_v16_proxy_stop(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    int was_running = atomic_exchange_explicit(&proxy->running, 0,
                                               memory_order_relaxed);
    if (!was_running) return;
    if (proxy->uds_fd >= 0) shutdown(proxy->uds_fd, SHUT_RDWR);
    if (proxy->tcp_fd >= 0) shutdown(proxy->tcp_fd, SHUT_RDWR);
}

void vemb_v16_proxy_get_stats(vemb_v16_proxy_t *proxy, vemb_v16_stats_t *stats) {
    assert(proxy != NULL);
    assert(stats != NULL);
    memset(stats, 0, sizeof(*stats));
    pthread_mutex_lock(&proxy->stats_lock);
    stats_add(stats, &proxy->closed_stats);
    pthread_mutex_unlock(&proxy->stats_lock);
    sve_operation_stats_t *sve_stats =
        vemb_v16_storage_sve_stats(proxy_storage(proxy));
    if (sve_stats) {
        stats->bitmap_lock_success =
            atomic_load_explicit(&sve_stats->lock_success, memory_order_relaxed);
        stats->bitmap_lock_failure =
            atomic_load_explicit(&sve_stats->lock_failure, memory_order_relaxed);
    }
    if (proxy->vemb_shard_queues) {
        uint32_t count = proxy->vemb_shard_proxy_count *
            proxy->vemb_shard_supernode_count;
        for (uint32_t i = 0; i < count; i++) {
            stats->vemb_shard_queue_depth +=
                vemb_v16_aeron_available(&proxy->vemb_shard_queues[i].ring);
        }
    }
    if (proxy->vadd_shard_queues) {
        uint32_t count = proxy->vemb_shard_proxy_count *
            proxy->vemb_shard_supernode_count;
        for (uint32_t i = 0; i < count; i++) {
            stats->vadd_shard_queue_depth +=
                vemb_v16_aeron_available(&proxy->vadd_shard_queues[i].ring);
        }
    }
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        vemb_v16_channel_t *ch = &proxy->channels[i];
        if (atomic_load_explicit(&ch->active, memory_order_acquire)) {
            stats->active_channels++;
            stats_add_channel_counters(stats, &ch->stats);
            if (ch->request_ring)
                stats->request_ring_depth +=
                    vemb_v16_client_available(ch->request_ring);
            if (ch->response_ring)
                stats->response_ring_depth +=
                    vemb_v16_client_available(ch->response_ring);
            stats->completion_ring_depth += vemb_v16_aeron_available(&ch->completion_ring);
        }
    }
}
