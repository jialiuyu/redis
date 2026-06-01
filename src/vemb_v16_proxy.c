#define _GNU_SOURCE

#include "cpu_relax.h"
#include "vemb_v16_aeron_transport.h"
#include "vemb_v16_proxy_types.h"
#include "vemb_v16_tcp_transport.h"
#include "vemb_v16_log.h"
#include "vemb_v16_net.h"
#include "macro.h"
#include "redisassert.h"
#include "zmalloc.h"

#include <errno.h>
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
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define VEMB_V16_VEMB_JOB_RING_SIZE VEMB_V16_AERON_RING_SIZE
#define VEMB_V16_VADD_JOB_RING_SIZE 256u
#define VEMB_V16_VADD_SHARD_RING_SIZE VEMB_V16_VADD_JOB_RING_SIZE
#define VEMB_V16_COMPLETION_RING_SIZE VEMB_V16_AERON_RING_SIZE

uint64_t vemb_v16_channel_id(vemb_v16_channel_t *ch) {
    return ch->channel_id;
}

int vemb_v16_channel_active(vemb_v16_channel_t *ch) {
    return atomic_load_explicit(&ch->active, memory_order_acquire);
}

int vemb_v16_channel_net_fd(vemb_v16_channel_t *ch) {
    return ch->net_fd;
}

int vemb_v16_channel_tcp_backpressure_enabled(vemb_v16_channel_t *ch) {
    return ch->tcp_backpressure_enabled;
}

int vemb_v16_channel_proxy_running(vemb_v16_channel_t *ch) {
    return atomic_load_explicit(&ch->proxy->running, memory_order_relaxed);
}

vemb_v16_client_ring_t *vemb_v16_channel_request_ring(vemb_v16_channel_t *ch) {
    return ch->request_ring;
}

vemb_v16_client_ring_t *vemb_v16_channel_response_ring(vemb_v16_channel_t *ch) {
    return ch->response_ring;
}

uint32_t vemb_v16_channel_request_slot_size(vemb_v16_channel_t *ch) {
    return ch->request_ring->slot_size;
}

void vemb_v16_channel_add_proxy_request_poll(vemb_v16_channel_t *ch,
                                             uint64_t n) {
    atomic_fetch_add_explicit(&ch->stats.proxy_request_poll, n,
                              memory_order_relaxed);
}

void vemb_v16_channel_add_proxy_response_ring_full(vemb_v16_channel_t *ch,
                                                   uint64_t n) {
    atomic_fetch_add_explicit(&ch->stats.proxy_response_ring_full, n,
                              memory_order_relaxed);
}

void vemb_v16_channel_add_channel_ops(vemb_v16_channel_t *ch, uint64_t n) {
    atomic_fetch_add_explicit(&ch->stats.channel_ops, n,
                              memory_order_relaxed);
}

const char *vemb_v16_proxy_uds_path(vemb_v16_proxy_t *proxy) {
    return proxy->uds_path;
}

const char *vemb_v16_proxy_tcp_host(vemb_v16_proxy_t *proxy) {
    return proxy->tcp_host;
}

uint16_t vemb_v16_proxy_tcp_port(vemb_v16_proxy_t *proxy) {
    return proxy->tcp_port;
}

#ifdef __linux__
int vemb_v16_tcp_backlog_pending(vemb_v16_channel_t *ch) {
    return ch && ch->tcp_response_backlog_len > ch->tcp_response_backlog_sent;
}

size_t vemb_v16_tcp_backlog_pending_bytes(vemb_v16_channel_t *ch) {
    return ch->tcp_response_backlog_len - ch->tcp_response_backlog_sent;
}

uint8_t *vemb_v16_tcp_backlog_pending_ptr(vemb_v16_channel_t *ch) {
    return ch->tcp_response_backlog + ch->tcp_response_backlog_sent;
}

void vemb_v16_tcp_backlog_compact(vemb_v16_channel_t *ch, size_t pending) {
    if (ch->tcp_response_backlog_sent != 0 && pending != 0) {
        memmove(ch->tcp_response_backlog,
                ch->tcp_response_backlog + ch->tcp_response_backlog_sent,
                pending);
    }
    ch->tcp_response_backlog_len = pending;
    ch->tcp_response_backlog_sent = 0;
}

size_t vemb_v16_tcp_backlog_capacity(vemb_v16_channel_t *ch) {
    return ch->tcp_response_backlog_cap;
}

uint8_t *vemb_v16_tcp_backlog_buffer(vemb_v16_channel_t *ch) {
    return ch->tcp_response_backlog;
}

void vemb_v16_tcp_backlog_set_buffer(vemb_v16_channel_t *ch,
                                     uint8_t *buf,
                                     size_t cap) {
    ch->tcp_response_backlog = buf;
    ch->tcp_response_backlog_cap = cap;
}

void vemb_v16_tcp_backlog_append_done(vemb_v16_channel_t *ch, size_t len) {
    ch->tcp_response_backlog_len += len;
}

void vemb_v16_tcp_backlog_consume(vemb_v16_channel_t *ch, size_t len) {
    ch->tcp_response_backlog_sent += len;
}

void vemb_v16_tcp_backlog_reset(vemb_v16_channel_t *ch) {
    ch->tcp_response_backlog_len = 0;
    ch->tcp_response_backlog_sent = 0;
}
#endif

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

int vemb_v16_proxy_tcp_response_vector_slice(vemb_v16_channel_t *ch,
                              vemb_v16_resp_t *resp,
                              const uint8_t **vector,
                              uint32_t *vector_bytes) {
    return vemb_v16_storage_vector_slice(proxy_storage(ch->proxy),
                                         resp,
                                         vector,
                                         vector_bytes);
}

void vemb_v16_make_response_from(vemb_v16_resp_t *resp,
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

/// Response scheduling: route a completion back to TCP or UB/SHM clients.
static void publish_response(vemb_v16_channel_t *ch,
                             const vemb_v16_completion_t *completion) {
    vemb_v16_resp_t resp;
    vemb_v16_make_response_from(&resp, completion);
    if (ch->transport_type == VEMB_V16_TRANSPORT_TCP) {
        if (ch->net_fd < 0 ||
            vemb_v16_tcp_publish_response(ch, &resp) != 0) {
            if (ch->net_fd >= 0) {
                shutdown(ch->net_fd, SHUT_RDWR);
                close(ch->net_fd);
                ch->net_fd = -1;
            }
            return;
        }
    } else {
        if (vemb_v16_aeron_publish_response(ch, &resp) != 0)
            return;
    }
    atomic_fetch_add_explicit(&ch->stats.proxy_response_publish, 1,
                              memory_order_relaxed);
}

static vemb_v16_completion_t make_completion(vemb_v16_channel_t *ch,
                                             const vemb_v16_req_t *req,
                                             uint8_t status) {
    return (vemb_v16_completion_t){
        .status = status,
        .op = req->op,
        .req_id = req->req_id,
        .channel_index = ch->index,
        .channel_id = ch->channel_id,
    };
}

static void publish_status_response(vemb_v16_channel_t *ch,
                                    const vemb_v16_req_t *req,
                                    uint8_t status) {
    vemb_v16_completion_t completion = make_completion(ch, req, status);
    publish_response(ch, &completion);
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

static void fill_job_base(vemb_v16_job_base_t *base,
                          vemb_v16_channel_t *ch,
                          const vemb_v16_req_t *req,
                          uint32_t key_len) {
    *base = (vemb_v16_job_base_t){
        .op = req->op,
        .flags = req->flags,
        .req_id = req->req_id,
        .channel_index = ch->index,
        .key_len = key_len,
        .channel_id = ch->channel_id,
        .key_hash = req->key_hash,
        .dim = req->dim,
        .vector_bytes = req->vector_bytes,
    };
    memcpy(base->key, req->key, key_len);
}

static int publish_request_job(vemb_v16_channel_t *ch,
                               const vemb_v16_req_t *req,
                               uint32_t key_len,
                               uint32_t proxy_io_worker_id) {
    int is_vadd = req->op == VEMB_V16_OP_VADD_INLINE;
    int is_vemb = req->op == VEMB_V16_OP_VEMB_HANDLE ||
        req->op == VEMB_V16_OP_VEMB_SUPERNODE_READ;
    RETURN_IF(!is_vadd && !is_vemb, -1);

    void *job = zmalloc(is_vadd ?
        sizeof(vemb_v16_vadd_job_t) : sizeof(vemb_v16_vemb_job_t));
    RETURN_IF(!job, -1);

    fill_job_base((vemb_v16_job_base_t *)job, ch, req, key_len);
    if (is_vadd) {
        memcpy(((vemb_v16_vadd_job_t *)job)->vector,
               req->vector,
               req->vector_bytes);
    }

    vemb_v16_shard_queue_t *queues = is_vadd ?
        ch->proxy->vadd_shard_queues : ch->proxy->vemb_shard_queues;
    atomic_uint_fast64_t *ring_full_counter = is_vadd ?
        &ch->stats.proxy_vadd_ring_full : &ch->stats.proxy_vemb_ring_full;
    atomic_uint_fast64_t *publish_counter = is_vadd ?
        &ch->stats.proxy_vadd_publish : &ch->stats.proxy_vemb_publish;

    int rc = publish_shard_job(ch,
                               job,
                               proxy_io_worker_id,
                               queues,
                               ring_full_counter);
    zfree(job);
    RETURN_IF(rc != 0, -1);
    atomic_fetch_add_explicit(publish_counter, 1, memory_order_relaxed);
    return 0;
}

/// Request scheduling: validate protocol input and enqueue execution jobs.
void vemb_v16_proxy_handle_request(vemb_v16_channel_t *ch,
                    const vemb_v16_req_t *req,
                    int req_len,
                    uint32_t proxy_io_worker_id) {
    if (req->op == VEMB_V16_OP_PING) {
        atomic_fetch_add_explicit(&ch->stats.total_requests, 1,
                                  memory_order_relaxed);
        publish_status_response(ch, req, VEMB_V16_STATUS_OK);
        return;
    }

    uint32_t key_len = req->key_len;
    if (key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN ||
        req->channel_id != ch->channel_id) {
        goto error_response;
    }

    size_t min_len = req->op == VEMB_V16_OP_VADD_INLINE ?
        vemb_v16_req_inline_len(req->vector_bytes) : vemb_v16_req_handle_len();
    if ((size_t)req_len < min_len || req->dim > VEMB_V16_MAX_DIM ||
        req->vector_bytes > sizeof(req->vector)) {
        goto error_response;
    }

    if (publish_request_job(ch, req, key_len, proxy_io_worker_id) != 0) {
        goto error_response;
    }

    atomic_fetch_add_explicit(&ch->stats.published_jobs, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&ch->stats.total_requests, 1, memory_order_relaxed);
    return;

error_response:
    publish_status_response(ch, req, VEMB_V16_STATUS_ERR);
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
                vemb_v16_tcp_publish_response_batch(ch, completions, n, &published) != 0) {
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
    vemb_v16_aeron_destroy_shared_ring(ch->request_ring_name, ch->request_ring,
                        ch->request_ring_bytes);
    vemb_v16_aeron_destroy_shared_ring(ch->response_ring_name, ch->response_ring,
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

    if (transport_type == VEMB_V16_TRANSPORT_AERON) {
        snprintf(ch->request_ring_name, sizeof(ch->request_ring_name),
                 "/%s_req_%llu", VEMB_V16_SHM_PREFIX,
                 (unsigned long long)ch->channel_id);
        snprintf(ch->response_ring_name, sizeof(ch->response_ring_name),
                 "/%s_resp_%llu", VEMB_V16_SHM_PREFIX,
                 (unsigned long long)ch->channel_id);

        if (vemb_v16_aeron_create_shared_ring(ch->request_ring_name,
                               proxy->request_ring_slot_size,
                               &ch->request_ring,
                               &ch->request_ring_bytes) != 0) {
            cleanup_unstarted_channel(ch);
            return -1;
        }
        if (vemb_v16_aeron_create_shared_ring(ch->response_ring_name,
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
int vemb_v16_proxy_alloc_shm_channel(vemb_v16_proxy_t *proxy, vemb_v16_channel_desc_t *desc) {
    return alloc_channel_common(proxy, VEMB_V16_TRANSPORT_AERON, -1, desc);
}

/// TCP control plane: attach an accepted socket to a channel.
int vemb_v16_proxy_alloc_tcp_channel(vemb_v16_proxy_t *proxy,
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
    vemb_v16_aeron_destroy_shared_ring(ch->request_ring_name, ch->request_ring,
                        ch->request_ring_bytes);
    vemb_v16_aeron_destroy_shared_ring(ch->response_ring_name, ch->response_ring,
                        ch->response_ring_bytes);
    if (ch->net_fd >= 0) {
        close(ch->net_fd);
        ch->net_fd = -1;
    }
    vemb_v16_channel_free_slots(ch);
    reset_closed_channel(ch);
}

int vemb_v16_proxy_close_channel_by_id(vemb_v16_proxy_t *proxy, uint64_t channel_id) {
    assert(proxy != NULL);
    RETURN_IF(channel_id == 0, -1);
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

uint64_t vemb_v16_proxy_close_all_channels(vemb_v16_proxy_t *proxy) {
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
                ch->transport_type == VEMB_V16_TRANSPORT_AERON) {
                int rc = vemb_v16_aeron_poll_shm_requests(ch, worker->worker_id);
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
                int rc = vemb_v16_tcp_read_ready_requests(ch,
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
    if (epfd < 0) {
        serverLog(LL_WARNING, "vemb_v16 proxy io epoll create failed: worker_id=%u errno=%d error=%s",
                  worker->worker_id, errno, strerror(errno));
        atomic_store_explicit(&proxy->running, 0, memory_order_relaxed);
        return NULL;
    }
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

            if (ch->transport_type == VEMB_V16_TRANSPORT_AERON) {
                int n = drain_completions(ch);
                if (n < 0) {
                    proxy_io_channel_deactivate(ch);
                    did_work = 1;
                } else if (n > 0) {
                    did_work = 1;
                }
                int rc = vemb_v16_aeron_poll_shm_requests(ch, worker->worker_id);
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
                if (vemb_v16_tcp_backlog_pending(ch))
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
            if (vemb_v16_tcp_backlog_pending(ch))
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

            if (vemb_v16_tcp_backlog_pending(ch)) {
                int flush = vemb_v16_tcp_flush_response_backlog(ch);
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
                int rc = vemb_v16_tcp_read_ready_requests(ch,
                                                         worker->worker_id);
                if (rc < 0) {
                    proxy_io_channel_deactivate(ch);
                }
            }
            if ((revents & EPOLLOUT) && atomic_load_explicit(&ch->active, memory_order_acquire)) {
                if (vemb_v16_tcp_flush_response_backlog(ch) < 0)
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
    RETURN_IF(!proxy, -1);
    strncpy(proxy->uds_path, uds_path, sizeof(proxy->uds_path) - 1);
    proxy->vector_dim = vector_dim;
    proxy->vector_stride = vector_dim * sizeof(float);
    proxy->request_ring_slot_size =
        (uint32_t)vemb_v16_req_inline_len(proxy->vector_stride);
    proxy->response_ring_slot_size = sizeof(vemb_v16_resp_t);
    proxy->max_vectors = max_vectors;
    proxy->listen_fd = -1;
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

int vemb_v16_proxy_enable_uds(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    proxy->uds_enabled = 1;
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
    if (proxy->listen_fd >= 0) close(proxy->listen_fd);
    if (proxy->uds_enabled && proxy->uds_path[0]) unlink(proxy->uds_path);
    pthread_mutex_destroy(&proxy->stats_lock);
    zfree(proxy);
}

/// Top-level control loop: accept either UDS UB/SHM control or TCP control/data.
int vemb_v16_proxy_run(vemb_v16_proxy_t *proxy) {
    int rc = -1;
    if (validate_pooled_worker_config(proxy) != 0)
        return -1;
    if (!proxy->uds_enabled && !proxy->tcp_enabled)
        return -1;
    assert(proxy->uds_enabled != proxy->tcp_enabled);
    atomic_store_explicit(&proxy->running, 1, memory_order_relaxed);

    vemb_v16_transport_listener_t listener = {0};
    if (proxy->uds_enabled) {
        if (vemb_v16_aeron_listen(proxy, 4096, &listener) != 0)
            goto cleanup;
    } else {
        if (vemb_v16_tcp_listen(proxy, 4096, &listener) != 0)
            goto cleanup;
    }
    proxy->listen_fd = listener.fd;
    if (init_vemb_shard_queues(proxy) != 0) {
        serverLog(LL_WARNING, "vemb_v16 vemb shard queue init failed: proxy_io_threads=%u supernode_workers=%u",
                  proxy->proxy_io_worker_count,
                  proxy->supernode_worker_count);
        goto cleanup;
    }
    if (start_supernode_pool(proxy) != 0) {
        serverLog(LL_WARNING, "vemb_v16 pooled supernode start failed: workers=%u",
                  proxy->supernode_worker_count);
        goto cleanup;
    }
    if (start_proxy_io_pool(proxy) != 0) {
        serverLog(LL_WARNING, "vemb_v16 proxy io pool start failed: threads=%u",
                  proxy->proxy_io_worker_count);
        goto cleanup;
    }

    serverLog(LL_NOTICE, "vemb_v16 server ready: uds_enabled=%s uds=%s tcp_enabled=%s tcp=%s:%u proxy_io_threads=%u supernode_workers=%u dim=%u max_vectors=%u vector_region=%s",
              proxy->uds_enabled ? "yes" : "no",
              proxy->uds_path,
              proxy->tcp_enabled ? "yes" : "no",
              proxy->tcp_host,
              proxy->tcp_port,
              proxy->proxy_io_worker_count,
              proxy->supernode_worker_count,
              proxy->vector_dim,
              proxy->max_vectors,
              vemb_v16_storage_vector_region_name(proxy_storage(proxy)));

    assert(listener.fd >= 0);
    assert(listener.name != NULL);
    assert(listener.handle_fd != NULL);

    while (atomic_load_explicit(&proxy->running, memory_order_relaxed)) {
        int did_work = 0;
        for (;;) {
            int cfd = accept(listener.fd, NULL, NULL);
            if (cfd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                    break;
                serverLog(LL_WARNING, "vemb_v16 %s accept failed: fd=%d errno=%d error=%s",
                          listener.name, listener.fd, errno, strerror(errno));
                goto cleanup;
            }
            did_work = 1;
            listener.handle_fd(proxy, cfd);
        }
        if (!did_work) {
            struct timespec ts = {0, 1000000};
            nanosleep(&ts, NULL);
        }
        reap_inactive_tcp_channels(proxy);
    }
    rc = 0;

cleanup:
    atomic_store_explicit(&proxy->running, 0, memory_order_relaxed);
    stop_proxy_io_pool(proxy);
    stop_supernode_pool(proxy);
    free_vemb_shard_queues(proxy);
    if (proxy->listen_fd >= 0) {
        close(proxy->listen_fd);
        proxy->listen_fd = -1;
    }
    if (proxy->uds_enabled && proxy->uds_path[0])
        unlink(proxy->uds_path);
    return rc;
}

void vemb_v16_proxy_stop(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    int was_running = atomic_exchange_explicit(&proxy->running, 0,
                                               memory_order_relaxed);
    if (!was_running) return;
    if (proxy->listen_fd >= 0) shutdown(proxy->listen_fd, SHUT_RDWR);
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
