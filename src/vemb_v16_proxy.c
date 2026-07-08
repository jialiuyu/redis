#define _GNU_SOURCE

#include "vemb_v16_hash.h"

#include "cpu_relax.h"
#include "vemb_v16_aeron_transport.h"
#include "vemb_v16_proxy_types.h"
#include "vemb_v16_tcp_transport.h"
#include "vemb_v16_log.h"
#include "vemb_v16_net.h"
#include "vemb_v16_stats.h"
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
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define VEMB_V16_JOB_SHARD_RING_SIZE 256u
#define VEMB_V16_JOB_RETURN_RING_SIZE 256u
#define VEMB_V16_COMPLETION_RING_SIZE VEMB_V16_AERON_RING_SIZE
#define VEMB_V16_DIAG_REQ_ID_LIMIT 80u
#define VEMB_V16_SCALEOUT_NOTIFY_INTERVAL_US 100000u
#define VEMB_V16_SCALEOUT_NOTIFY_TIMEOUT_MS 1000u
#define VEMB_V16_READ_JOB_POOL_SLOTS 1024u
#define VEMB_V16_VSIM_JOB_POOL_SLOTS 256u
#define VEMB_V16_INLINE_JOB_POOL_SLOTS 512u

static int diag_should_log_req(uint32_t req_id) {
    return req_id != 0 && req_id <= VEMB_V16_DIAG_REQ_ID_LIMIT;
}

static void completion_release_payload(vemb_v16_completion_t *completion) {
    vemb_v16_completion_release_inline_snapshot(completion);
}

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

void vemb_v16_channel_add_proxy_response_ring_full(vemb_v16_channel_t *ch,
                                                   uint64_t n) {
    atomic_fetch_add_explicit(&ch->stats.proxy_response_ring_full, n,
                              memory_order_relaxed);
}

static void channel_note_response_status(vemb_v16_channel_t *ch,
                                         uint8_t status) {
    atomic_uint_fast64_t *counter = NULL;
    switch (status) {
    case VEMB_V16_STATUS_MOVED:
        counter = &ch->stats.moved_count;
        break;
    case VEMB_V16_STATUS_STALE_TOPOLOGY:
        counter = &ch->stats.stale_count;
        break;
    case VEMB_V16_STATUS_ASK:
        counter = &ch->stats.ask_count;
        break;
    default:
        return;
    }
    atomic_fetch_add_explicit(counter, 1, memory_order_relaxed);
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
    return ch->tcp_response_backlog_len > ch->tcp_response_backlog_sent;
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

static size_t align64_size(size_t value) {
    return (value + 63u) & ~(size_t)63u;
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

static void free_job_shard_queues(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    uint32_t count = proxy->job_shard_proxy_count *
        proxy->job_shard_supernode_count;
    free_shard_queue_array(proxy->job_shard_queues, count);
    proxy->job_shard_queues = NULL;
    proxy->job_shard_proxy_count = 0;
    proxy->job_shard_supernode_count = 0;
}

static int validate_pooled_worker_config(vemb_v16_proxy_t *proxy);
static int drain_job_return_queues(vemb_v16_proxy_t *proxy,
                                   uint32_t proxy_worker_id);
static int tcp_vemb_read_requires_inline_op(vemb_v16_channel_t *ch,
                                            const vemb_v16_req_t *req);

static void free_job_return_queues(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    uint32_t count = proxy->job_shard_proxy_count *
        proxy->job_shard_supernode_count;
    free_shard_queue_array(proxy->job_return_queues, count);
    proxy->job_return_queues = NULL;
}

static int init_job_return_queues(vemb_v16_proxy_t *proxy) {
    if (validate_pooled_worker_config(proxy) != 0)
        return -1;
    if (proxy->job_return_queues)
        return 0;

    uint32_t count = proxy->proxy_io_worker_count *
        proxy->supernode_worker_count;
    if (init_shard_queue_array(&proxy->job_return_queues,
                               count,
                               sizeof(vemb_v16_job_return_t),
                               VEMB_V16_JOB_RETURN_RING_SIZE) != 0) {
        free_job_return_queues(proxy);
        return -1;
    }
    return 0;
}

static uint32_t job_pool_slot_count(const vemb_v16_proxy_t *proxy,
                                    uint16_t pool_type) {
    assert(proxy != NULL);
    switch (pool_type) {
    case VEMB_V16_JOB_POOL_READ:
        return VEMB_V16_READ_JOB_POOL_SLOTS;
    case VEMB_V16_JOB_POOL_VSIM_KEY_KEY:
        return VEMB_V16_VSIM_JOB_POOL_SLOTS;
    case VEMB_V16_JOB_POOL_INLINE_VECTOR:
        return VEMB_V16_INLINE_JOB_POOL_SLOTS;
    default:
        return 0;
    }
}

static size_t job_pool_slot_stride(uint16_t pool_type) {
    switch (pool_type) {
    case VEMB_V16_JOB_POOL_READ:
        return offsetof(vemb_v16_job_slot_t, u) + sizeof(vemb_v16_vemb_job_t);
    case VEMB_V16_JOB_POOL_VSIM_KEY_KEY:
        return offsetof(vemb_v16_job_slot_t, u) + sizeof(vemb_v16_vsim_key_key_job_t);
    case VEMB_V16_JOB_POOL_INLINE_VECTOR:
        return offsetof(vemb_v16_job_slot_t, u) + sizeof(vemb_v16_vadd_job_t);
    default:
        return 0;
    }
}

static vemb_v16_job_slot_t *job_pool_slot(vemb_v16_job_pool_t *pool,
                                          uint32_t slot_id) {
    assert(pool != NULL);
    assert(slot_id < pool->slot_count);
    return (vemb_v16_job_slot_t *)(
        (uint8_t *)pool->slots + (size_t)slot_id * pool->slot_stride);
}

static void job_pool_slot_reset(vemb_v16_job_pool_t *pool, uint32_t slot_id) {
    vemb_v16_job_slot_t *slot = job_pool_slot(pool, slot_id);
    memset(&slot->u, 0, pool->slot_stride - offsetof(vemb_v16_job_slot_t, u));
    slot->hdr.op = 0;
}

static size_t job_pool_slots_region_bytes(const vemb_v16_proxy_t *proxy,
                                          uint16_t pool_type) {
    return align64_size((size_t)job_pool_slot_count(proxy, pool_type) *
                        job_pool_slot_stride(pool_type));
}

static uint64_t job_pool_slots_region_offset(const vemb_v16_proxy_t *proxy,
                                             uint32_t worker_id,
                                             uint16_t pool_type) {
    uint64_t offset = proxy->job_pool_slots_mmap_offset;
    for (uint32_t i = 0; i < worker_id; i++) {
        for (uint16_t t = 0; t < VEMB_V16_JOB_POOL_COUNT; t++)
            offset += job_pool_slots_region_bytes(proxy, t);
    }
    for (uint16_t t = 0; t < pool_type; t++)
        offset += job_pool_slots_region_bytes(proxy, t);
    return offset;
}

static int init_worker_job_pool(vemb_v16_job_pool_t *pool,
                                vemb_v16_proxy_t *proxy,
                                uint32_t worker_id,
                                uint16_t pool_type) {
    memset(pool, 0, sizeof(*pool));
    pool->pool_type = pool_type;
    pool->slot_count = job_pool_slot_count(proxy, pool_type);
    pool->slot_stride = (uint32_t)job_pool_slot_stride(pool_type);
    RETURN_IF(pool->slot_count == 0 || pool->slot_stride == 0, -1);
    if (proxy->job_pool_slots_path[0] != '\0') {
        vemb_v16_mapped_region_t *region =
            &proxy->proxy_io_workers[worker_id].job_pool_slot_regions[pool_type];
        if (vemb_v16_mapped_region_open(region,
                                        proxy->job_pool_slots_backend_type,
                                        proxy->job_pool_slots_path,
                                        job_pool_slots_region_offset(proxy,
                                                                     worker_id,
                                                                     pool_type),
                                        job_pool_slots_region_bytes(proxy,
                                                                    pool_type)) != 0) {
            return -1;
        }
        pool->slots = region->mapped_addr;
    } else if (posix_memalign(&pool->slots,
                              64,
                              (size_t)pool->slot_count * pool->slot_stride) != 0) {
        return -1;
    }
    pool->free_stack = zmalloc(sizeof(*pool->free_stack) * pool->slot_count);
    if (pool->free_stack == NULL) {
        if (proxy->job_pool_slots_path[0] != '\0') {
            vemb_v16_mapped_region_close(
                &proxy->proxy_io_workers[worker_id].job_pool_slot_regions[pool_type]);
        } else {
            zfree(pool->slots);
        }
        pool->slots = NULL;
        return -1;
    }
    pool->free_count = pool->slot_count;
    for (uint32_t i = 0; i < pool->slot_count; i++) {
        vemb_v16_job_slot_t *slot = job_pool_slot(pool, i);
        memset(slot, 0, pool->slot_stride);
        atomic_init(&slot->hdr.state, VEMB_V16_JOB_SLOT_FREE);
        slot->hdr.generation = 1;
        pool->free_stack[i] = pool->slot_count - 1 - i;
    }
    return 0;
}

static void cleanup_worker_job_pool(vemb_v16_job_pool_t *pool) {
    RETURN_IF(!pool);
    zfree(pool->free_stack);
    memset(pool, 0, sizeof(*pool));
}

static int init_proxy_io_job_pools(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    for (uint32_t i = 0; i < proxy->proxy_io_worker_count; i++) {
        for (uint16_t pool_type = 0; pool_type < VEMB_V16_JOB_POOL_COUNT; pool_type++) {
            if (init_worker_job_pool(
                    &proxy->proxy_io_workers[i].job_pools[pool_type],
                    proxy,
                    i,
                    pool_type) != 0) {
                return -1;
            }
        }
    }
    return 0;
}

static void cleanup_proxy_io_job_pools(vemb_v16_proxy_t *proxy) {
    RETURN_IF(!proxy);
    for (uint32_t i = 0; i < proxy->proxy_io_worker_count; i++) {
        for (uint16_t pool_type = 0; pool_type < VEMB_V16_JOB_POOL_COUNT; pool_type++) {
            vemb_v16_mapped_region_close(
                &proxy->proxy_io_workers[i].job_pool_slot_regions[pool_type]);
            if (proxy->job_pool_slots_path[0] == '\0') {
                zfree(proxy->proxy_io_workers[i].job_pools[pool_type].slots);
                proxy->proxy_io_workers[i].job_pools[pool_type].slots = NULL;
            }
            cleanup_worker_job_pool(&proxy->proxy_io_workers[i].job_pools[pool_type]);
        }
    }
}

/// Scheduling plane: allocate the proxy-IO -> SuperNode shard queues.
static int init_job_shard_queues(vemb_v16_proxy_t *proxy) {
    if (validate_pooled_worker_config(proxy) != 0)
        return -1;
    if (proxy->job_shard_queues)
        return 0;

    uint32_t proxy_count = proxy->proxy_io_worker_count;
    uint32_t supernode_count = proxy->supernode_worker_count;
    uint32_t count = proxy_count * supernode_count;
    proxy->job_shard_proxy_count = proxy_count;
    proxy->job_shard_supernode_count = supernode_count;
    if (init_shard_queue_array(&proxy->job_shard_queues,
                               count,
                               sizeof(vemb_v16_job_ref_t),
                               VEMB_V16_JOB_SHARD_RING_SIZE) != 0) {
        free_job_shard_queues(proxy);
        return -1;
    }
    return 0;
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
    if (!proxy->job_shard_queues)
        return 0;
    if (proxy->job_shard_proxy_count != proxy->proxy_io_worker_count)
        return 0;
    if (proxy->job_shard_supernode_count != proxy->supernode_worker_count)
        return 0;
    return 1;
}

static uint32_t shard_queue_index(vemb_v16_proxy_t *proxy,
                                  uint32_t proxy_worker_id,
                                  uint32_t sn_work_id) {
    assert(proxy != NULL);
    assert(shard_queue_topology_ready(proxy));
    assert(proxy_worker_id < proxy->job_shard_proxy_count);
    assert(sn_work_id < proxy->job_shard_supernode_count);
    return proxy_worker_id * proxy->job_shard_supernode_count + sn_work_id;
}

static vemb_v16_storage_ctx_t *proxy_storage(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    assert(proxy->storage != NULL);
    return proxy->storage;
}

static int migration_control_req_valid(
        const vemb_v16_migration_control_req_t *req) {
    return req &&
           req->key_len > 0 &&
           req->key_len <= VEMB_V16_MAX_KEY_LEN &&
           req->target_owner != UINT32_MAX;
}

static void migration_control_fill_resp(
        vemb_v16_migration_control_resp_t *resp,
        uint8_t status,
        const tlc_core_key_migration_info_t *info) {
    memset(resp, 0, sizeof(*resp));
    resp->status = status;
    if (!info)
        return;
    resp->key_hash = info->key_hash;
    resp->key_version = info->key_version;
    resp->topology_epoch = info->topology_epoch;
    resp->owner_epoch = info->owner_epoch;
    resp->migration_state = info->migration_state;
    resp->target_owner = info->target_owner;
    resp->tombstone = info->tombstone;
    resp->shard_id = info->shard_id;
}

static void migration_control_fill_outbox(
        vemb_v16_migration_control_resp_t *resp,
        const vemb_v16_migration_outbox_stats_t *stats) {
    resp->applied_seq = stats->acked_seq;
    resp->barrier_seq = stats->barrier_seq;
    resp->pending_delta = stats->pending_count;
    resp->outbox_state = stats->state;
}

static void epoch_control_fill_resp(vemb_v16_proxy_t *proxy,
                                    vemb_v16_epoch_control_resp_t *resp,
                                    uint8_t status) {
    memset(resp, 0, sizeof(*resp));
    resp->status = status;
    vemb_v16_storage_epoch_get(proxy_storage(proxy),
                               &resp->current_topology_epoch,
                               &resp->min_write_epoch);
}

static void topology_resp_add_local_endpoint(
        vemb_v16_proxy_t *proxy,
        vemb_v16_topology_control_resp_t *resp);

static void topology_control_fill_resp(
        vemb_v16_proxy_t *proxy,
        vemb_v16_topology_control_resp_t *resp,
        uint8_t status) {
    memset(resp, 0, sizeof(*resp));
    vemb_v16_storage_topology_get(proxy_storage(proxy), resp);
    topology_resp_add_local_endpoint(proxy, resp);
    resp->status = status;
}

static void topology_resp_upsert_endpoint(
        vemb_v16_topology_control_resp_t *resp,
        const vemb_v16_topology_endpoint_t *endpoint) {
    if (endpoint->owner_id == UINT32_MAX)
        return;
    for (uint32_t i = 0; i < resp->endpoint_count; i++) {
        if (resp->endpoints[i].owner_id == endpoint->owner_id) {
            resp->endpoints[i] = *endpoint;
            return;
        }
    }
    if (resp->endpoint_count >= VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS)
        return;
    resp->endpoints[resp->endpoint_count++] = *endpoint;
}

static void topology_resp_add_local_endpoint(
        vemb_v16_proxy_t *proxy,
        vemb_v16_topology_control_resp_t *resp) {
    vemb_v16_topology_endpoint_t endpoint = {
        .owner_id = proxy_storage(proxy)->local_owner_id,
    };
    if (proxy->tcp_enabled) {
        endpoint.transport_type = VEMB_V16_TRANSPORT_TCP;
        endpoint.tcp_port = proxy->tcp_port;
        strncpy(endpoint.host, proxy->tcp_host, sizeof(endpoint.host) - 1);
        endpoint.host[sizeof(endpoint.host) - 1] = '\0';
    } else if (proxy->uds_enabled) {
        endpoint.transport_type = VEMB_V16_TRANSPORT_AERON;
        strncpy(endpoint.uds_path,
                proxy->uds_path,
                sizeof(endpoint.uds_path) - 1);
        endpoint.uds_path[sizeof(endpoint.uds_path) - 1] = '\0';
    } else {
        return;
    }
    topology_resp_upsert_endpoint(resp, &endpoint);
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
    ch->tcp_response_backlog = NULL;
    ch->tcp_response_backlog_cap = 0;
    ch->tcp_response_backlog_len = 0;
    ch->tcp_response_backlog_sent = 0;
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
    if (!atomic_load_explicit(&ch->active, memory_order_acquire)) {
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
        .local_slot = completion->local_slot,
        .owner_generation = completion->owner_generation,
        .redirect_owner = completion->redirect_owner,
        .score = completion->score,
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
            serverLog(LL_WARNING,
                      "vemb_v16 tcp publish response failed: channel_index=%u channel_id=%llu req_id=%u op=%u status=%u flags=%u net_fd=%d active=%d",
                      ch->index,
                      (unsigned long long)ch->channel_id,
                      resp.req_id,
                      resp.op,
                      resp.status,
                      resp.flags,
                      ch->net_fd,
                      atomic_load_explicit(&ch->active, memory_order_acquire));
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
    channel_note_response_status(ch, completion->status);
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

static int publish_completion_batch(vemb_v16_channel_t *ch,
                                    vemb_v16_completion_t *completions,
                                    const uint16_t *ready_indices,
                                    uint32_t ready_count) {
    if (ready_count == 0)
        return 0;

    if (ch->transport_type == VEMB_V16_TRANSPORT_TCP) {
        uint32_t published = 0;
        if (ch->net_fd < 0 ||
            vemb_v16_tcp_publish_response_batch(ch,
                                                completions,
                                                ready_indices,
                                                ready_count,
                                                &published) != 0) {
            uint16_t first = ready_count ? ready_indices[0] : 0;
            const vemb_v16_completion_t *completion =
                ready_count ? &completions[first] : NULL;
            serverLog(LL_WARNING,
                      "vemb_v16 tcp publish response batch failed: channel_index=%u channel_id=%llu ready_count=%u published=%u first_req_id=%u first_op=%u first_status=%u net_fd=%d active=%d",
                      ch->index,
                      (unsigned long long)ch->channel_id,
                      ready_count,
                      published,
                      completion ? completion->req_id : 0,
                      completion ? completion->op : 0,
                      completion ? completion->status : 0,
                      ch->net_fd,
                      atomic_load_explicit(&ch->active, memory_order_acquire));
            for (uint32_t i = 0; i < ready_count; i++)
                completion_release_payload(&completions[ready_indices[i]]);
            if (ch->net_fd >= 0) {
                shutdown(ch->net_fd, SHUT_RDWR);
                close(ch->net_fd);
                ch->net_fd = -1;
            }
            return -1;
        }
        for (uint32_t i = 0; i < ready_count; i++)
            channel_note_response_status(ch,
                                         completions[ready_indices[i]].status);
        for (uint32_t i = 0; i < ready_count; i++)
            completion_release_payload(&completions[ready_indices[i]]);
    } else {
        for (uint32_t i = 0; i < ready_count; i++) {
            uint16_t idx = ready_indices[i];
            publish_response(ch, &completions[idx]);
            completion_release_payload(&completions[idx]);
        }
    }
    return 0;
}

/// Job scheduling: route VADD/VEMB work from proxy IO to a SuperNode shard queue.
static int publish_shard_job(vemb_v16_channel_t *ch,
                             const vemb_v16_job_ref_t *ref,
                             uint32_t proxy_io_worker_id,
                             vemb_v16_shard_queue_t *queues,
                             atomic_uint_fast64_t *ring_full_counter) {
    assert(ch != NULL);
    assert(ref != NULL);
    vemb_v16_proxy_t *proxy = ch->proxy;
    assert(shard_queue_topology_ready(proxy));
    assert(queues != NULL);
    assert(ring_full_counter != NULL);
    assert(proxy_io_worker_id < proxy->job_shard_proxy_count);

    uint32_t supernode_id = ch->index % proxy->job_shard_supernode_count;
    uint32_t queue_index =
        shard_queue_index(proxy, proxy_io_worker_id, supernode_id);
    vemb_v16_aeron_ring_t *ring = &queues[queue_index].ring;

    while (vemb_v16_aeron_publish(ring, ref) != 0 &&
           atomic_load_explicit(&proxy->running, memory_order_relaxed) &&
           atomic_load_explicit(&ch->active, memory_order_acquire)) {
        atomic_fetch_add_explicit(ring_full_counter, 1, memory_order_relaxed);
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
                          vemb_v16_job_kind_t kind,
                          vemb_v16_channel_t *ch,
                          const vemb_v16_req_t *req) {
    *base = (vemb_v16_job_base_t){
        .kind = (uint8_t)kind,
        .op = req->op,
        .flags = req->flags,
        .req_id = req->req_id,
        .channel_index = ch->index,
        .channel_id = ch->channel_id,
        .key_hash = req->key_hash,
        .topology_epoch = req->topology_epoch,
    };
}

static uint16_t job_pool_type_for_op(uint8_t op) {
    switch (op) {
    case VEMB_V16_OP_PING:
    case VEMB_V16_OP_VEMB_HANDLE:
    case VEMB_V16_OP_VEMB_INLINE:
    case VEMB_V16_OP_VREM:
        return VEMB_V16_JOB_POOL_READ;
    case VEMB_V16_OP_VSIM_KEY_KEY:
        return VEMB_V16_JOB_POOL_VSIM_KEY_KEY;
    case VEMB_V16_OP_VADD:
    case VEMB_V16_OP_VSIM_INLINE:
        return VEMB_V16_JOB_POOL_INLINE_VECTOR;
    default:
        return UINT16_MAX;
    }
}

static int job_pool_alloc_slot(vemb_v16_job_pool_t *pool, uint32_t *slot_id) {
    assert(pool != NULL);
    assert(slot_id != NULL);
    RETURN_IF(pool->free_count == 0, -1);
    *slot_id = pool->free_stack[--pool->free_count];
    vemb_v16_job_slot_t *slot = job_pool_slot(pool, *slot_id);
    atomic_store_explicit(&slot->hdr.state,
                          VEMB_V16_JOB_SLOT_RESERVED,
                          memory_order_relaxed);
    job_pool_slot_reset(pool, *slot_id);
    return 0;
}

static void job_pool_release_slot(vemb_v16_job_pool_t *pool,
                                  uint32_t slot_id,
                                  int bump_generation) {
    assert(pool != NULL);
    assert(slot_id < pool->slot_count);
    vemb_v16_job_slot_t *slot = job_pool_slot(pool, slot_id);
    if (bump_generation)
        slot->hdr.generation++;
    slot->hdr.op = 0;
    atomic_store_explicit(&slot->hdr.state,
                          VEMB_V16_JOB_SLOT_FREE,
                          memory_order_release);
    assert(pool->free_count < pool->slot_count);
    pool->free_stack[pool->free_count++] = slot_id;
}

static const vemb_v16_job_base_t *job_slot_payload_base(
    const vemb_v16_job_pool_t *pool,
    const vemb_v16_job_slot_t *slot) {
    switch (pool->pool_type) {
    case VEMB_V16_JOB_POOL_READ:
        return &slot->u.read_job.base;
    case VEMB_V16_JOB_POOL_VSIM_KEY_KEY:
        return &slot->u.vsim_job.base;
    case VEMB_V16_JOB_POOL_INLINE_VECTOR:
        return &slot->u.inline_job.base;
    default:
        return &slot->u.base_job;
    }
}

static int fill_job_slot(vemb_v16_job_pool_t *pool,
                         uint32_t slot_id,
                         vemb_v16_channel_t *ch,
                         const vemb_v16_req_t *req,
                         uint32_t key_len,
                         int *has_inline_vector,
                         int *is_vemb_like) {
    vemb_v16_job_slot_t *slot = job_pool_slot(pool, slot_id);
    assert(has_inline_vector != NULL);
    assert(is_vemb_like != NULL);
    *has_inline_vector = 0;
    *is_vemb_like = 0;

    switch (req->op) {
    case VEMB_V16_OP_PING:
        fill_job_base(&slot->u.base_job, VEMB_V16_JOB_KIND_BASE, ch, req);
        break;
    case VEMB_V16_OP_VEMB_HANDLE:
    case VEMB_V16_OP_VEMB_INLINE:
    case VEMB_V16_OP_VREM:
        fill_job_base(&slot->u.read_job.base, VEMB_V16_JOB_KIND_READ, ch, req);
        slot->u.read_job.key_len = key_len;
        slot->u.read_job.dim = req->dim;
        slot->u.read_job.vector_bytes = req->vector_bytes;
        memcpy(slot->u.read_job.key, req->key, key_len);
        *is_vemb_like = 1;
        break;
    case VEMB_V16_OP_VSIM_KEY_KEY:
        fill_job_base(&slot->u.vsim_job.base,
                      VEMB_V16_JOB_KIND_VSIM_KEY_KEY,
                      ch,
                      req);
        slot->u.vsim_job.key_len = key_len;
        slot->u.vsim_job.key2_len = req->key2_len;
        slot->u.vsim_job.dim = req->dim;
        slot->u.vsim_job.vector_bytes = req->vector_bytes;
        slot->u.vsim_job.key2_hash = req->key2_hash;
        memcpy(slot->u.vsim_job.key, req->key, key_len);
        memcpy(slot->u.vsim_job.key2, req->key2, req->key2_len);
        *is_vemb_like = 1;
        break;
    case VEMB_V16_OP_VADD:
    case VEMB_V16_OP_VSIM_INLINE:
        fill_job_base(&slot->u.inline_job.base,
                      VEMB_V16_JOB_KIND_INLINE_VECTOR,
                      ch,
                      req);
        slot->u.inline_job.key_len = key_len;
        slot->u.inline_job.dim = req->dim;
        slot->u.inline_job.vector_bytes = req->vector_bytes;
        memcpy(slot->u.inline_job.key, req->key, key_len);
        memcpy(slot->u.inline_job.vector, req->vector, req->vector_bytes);
        *has_inline_vector = 1;
        break;
    default:
        return -1;
    }

    slot->hdr.op = req->op;
    return 0;
}

static int publish_request_job(vemb_v16_channel_t *ch,
                               const vemb_v16_req_t *req,
                               uint32_t key_len,
                               uint32_t proxy_io_worker_id) {
    int is_ping = req->op == VEMB_V16_OP_PING;
    int has_inline_vector = 0;
    int is_vemb_like = 0;
    uint16_t pool_type = job_pool_type_for_op(req->op);
    RETURN_IF(pool_type == UINT16_MAX, -1);
    if (pool_type == VEMB_V16_JOB_POOL_READ &&
        !is_ping &&
        tcp_vemb_read_requires_inline_op(ch, req) != 0) {
        return -1;
    }
    vemb_v16_job_pool_t *pool =
        &ch->proxy->proxy_io_workers[proxy_io_worker_id].job_pools[pool_type];
    uint32_t slot_id = 0;
    if (job_pool_alloc_slot(pool, &slot_id) != 0) {
        return -1;
    }
    GOTO_IF(fill_job_slot(pool,
                          slot_id,
                          ch,
                          req,
                          key_len,
                          &has_inline_vector,
                          &is_vemb_like) != 0, release_slot);
    GOTO_IF(!is_ping && !has_inline_vector && !is_vemb_like, release_slot);
    vemb_v16_job_slot_t *slot = job_pool_slot(pool, slot_id);
    vemb_v16_job_ref_t ref = {
        .proxy_worker_id = (uint16_t)proxy_io_worker_id,
        .pool_type = pool_type,
        .slot_id = slot_id,
        .generation = slot->hdr.generation,
        .req_id = req->req_id,
        .op = req->op,
    };
    atomic_store_explicit(&slot->hdr.state, VEMB_V16_JOB_SLOT_PUBLISHED, memory_order_release);
    vemb_v16_shard_queue_t *queues = ch->proxy->job_shard_queues;
    atomic_uint_fast64_t *ring_full_counter = has_inline_vector ?
        &ch->stats.proxy_vadd_ring_full : &ch->stats.proxy_vemb_ring_full;
    if (diag_should_log_req(req->req_id)) {
        uint32_t supernode_id = ch->index % ch->proxy->job_shard_supernode_count;
        uint32_t queue_index = shard_queue_index(ch->proxy,
            proxy_io_worker_id, supernode_id);
        serverLog(LL_DEBUG,
                  "vemb_v16 diag proxy enqueue: proxy_worker=%u channel_index=%u channel_id=%llu req_id=%u op=%u flags=%u key_hash=%llu key_len=%u queue=%s queue_index=%u supernode_worker=%u vector_bytes=%u",
                  proxy_io_worker_id,
                  ch->index,
                  (unsigned long long)ch->channel_id,
                  ref.req_id,
                  ref.op,
                  req->flags,
                  (unsigned long long)req->key_hash,
                  key_len,
                  "job",
                  queue_index,
                  supernode_id,
                  req->vector_bytes);
    }

    int rc = publish_shard_job(ch,
                           &ref,
                           proxy_io_worker_id,
                           queues,
                           ring_full_counter);
    if (likely(rc == 0)) {
        return 0;
    }
release_slot:
    job_pool_release_slot(pool, slot_id, 0);
    return -1;
}

static int tcp_vemb_read_requires_inline_op(vemb_v16_channel_t *ch,
                                            const vemb_v16_req_t *req) {
    if (ch->transport_type != VEMB_V16_TRANSPORT_TCP)
        return 0;
    if (req->op != VEMB_V16_OP_VEMB_HANDLE)
        return 0;
    return -1;
}

/// Request scheduling: validate protocol input and enqueue execution jobs.
void vemb_v16_proxy_handle_request(vemb_v16_channel_t *ch,
                    const vemb_v16_req_t *req,
                    int req_len,
                    uint32_t proxy_io_worker_id) {
    if (req->op == VEMB_V16_OP_PING) {
        if (publish_request_job(ch, req, 0, proxy_io_worker_id) != 0)
            goto error_response;
        atomic_fetch_add_explicit(&ch->stats.total_requests, 1,
                                  memory_order_relaxed);
        return;
    }

    uint32_t key_len = req->key_len;
    if (key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN ||
        req->channel_id != ch->channel_id) {
        goto error_response;
    }
    if (req->op == VEMB_V16_OP_VSIM_KEY_KEY &&
        (req->key2_len == 0 || req->key2_len > VEMB_V16_MAX_KEY_LEN)) {
        goto error_response;
    }

    size_t min_len = (req->op == VEMB_V16_OP_VADD ||
                      req->op == VEMB_V16_OP_VSIM_INLINE) ?
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
    uint16_t ready_indices[VEMB_V16_PROXY_BATCH];
    uint32_t n;
    uint32_t total = 0;
    while ((n = vemb_v16_aeron_poll_batch(&ch->completion_ring,
                                          completions,
                                          VEMB_V16_PROXY_BATCH)) != 0) {
        total += n;
        uint32_t ready_count = 0;
        for (uint32_t i = 0; i < n; i++) {
            if (diag_should_log_req(completions[i].req_id)) {
                serverLog(LL_DEBUG,
                          "vemb_v16 diag proxy completion drain: channel_index=%u channel_id=%llu batch_index=%u batch_count=%u req_id=%u op=%u status=%u flags=%u vector_bytes=%u region_id=%u local_slot=%u owner_generation=%llu",
                          ch->index,
                          (unsigned long long)ch->channel_id,
                          i,
                          n,
                          completions[i].req_id,
                          completions[i].op,
                          completions[i].status,
                          completions[i].flags,
                          completions[i].vector_bytes,
                          completions[i].region_id,
                          completions[i].local_slot,
                          (unsigned long long)completions[i].owner_generation);
            }
            if (completions[i].channel_id != ch->channel_id ||
                !atomic_load_explicit(&ch->active, memory_order_acquire)) {
                completion_release_payload(&completions[i]);
                continue;
            }
            ready_indices[ready_count++] = (uint16_t)i;
        }
        if (publish_completion_batch(ch,
                                     completions,
                                     ready_indices,
                                     ready_count) != 0) {
            return -1;
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
    return alloc_channel_common(proxy,
                                VEMB_V16_TRANSPORT_TCP,
                                net_fd,
                                desc);
}

/// Control plane: close a channel and wait for proxy IO/SuperNode users to leave.
static void close_channel(vemb_v16_channel_t *ch) {
    if (atomic_load_explicit(&ch->slot_channel_id, memory_order_acquire) == 0) {
        return;
    }
    serverLog(LL_NOTICE,
              "vemb_v16 channel closing: index=%u channel_id=%llu transport=%u net_fd=%d active=%d",
              ch->index,
              (unsigned long long)ch->channel_id,
              ch->transport_type,
              ch->net_fd,
              atomic_load_explicit(&ch->active, memory_order_acquire));
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
    vemb_v16_stats_add_channel_counters(&ch->proxy->closed_stats, &ch->stats);
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

        if (drain_job_return_queues(proxy, worker->worker_id) > 0)
            did_work = 1;

        for (uint32_t i = worker->worker_id; i < VEMB_V16_MAX_CHANNELS; i += worker_count) {
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

        if (drain_job_return_queues(proxy, worker->worker_id) > 0)
            did_work = 1;

        for (uint32_t i = worker->worker_id;
             i < VEMB_V16_MAX_CHANNELS;
             i += worker_count) {
            vemb_v16_channel_t *ch = &proxy->channels[i];
            uint64_t channel_id =
                atomic_load_explicit(&ch->slot_channel_id,
                                     memory_order_acquire);
            int channel_active = channel_id != 0 &&
                atomic_load_explicit(&ch->active, memory_order_acquire);
            int tcp_active = channel_active &&
                ch->transport_type == VEMB_V16_TRANSPORT_TCP &&
                ch->net_fd >= 0;
            int aeron_active = channel_active &&
                ch->transport_type == VEMB_V16_TRANSPORT_AERON;
            int active = tcp_active;
            int fd = tcp_active ? ch->net_fd : -1;

            if (registered_ids[i] != 0 &&
                (!tcp_active ||
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

            if (!tcp_active && !aeron_active)
                continue;

            if (!proxy_io_channel_acquire(ch))
                continue;
            proxy_io_channel_disarm_completion_notify(ch);

            channel_id = atomic_load_explicit(&ch->slot_channel_id,
                                              memory_order_acquire);
            channel_active = channel_id != 0 &&
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
        proxy->proxy_io_workers[i].worker_id = i;
        proxy->proxy_io_workers[i].proxy = proxy;
#ifdef __linux__
        proxy->proxy_io_workers[i].notify_fd = -1;
#endif
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

/// Execution: run one VEMB job on the SuperNode storage/backend path.
static void apply_vemb_job(vemb_v16_supernode_ctx_t *ctx,
                           const vemb_v16_vemb_job_t *job,
                           vemb_v16_supernode_scratch_t *scratch,
                           vemb_v16_channel_t *ch) {
    (void)scratch;
    (void)ch;
    vemb_v16_supernode_handle_vemb_job(ctx, job);
}

static void apply_vsim_key_key_job(vemb_v16_supernode_ctx_t *ctx,
                                   const vemb_v16_vsim_key_key_job_t *job,
                                   vemb_v16_supernode_scratch_t *scratch,
                                   vemb_v16_channel_t *ch) {
    (void)scratch;
    (void)ch;
    vemb_v16_supernode_handle_vsim_key_key_job(ctx, job);
}

static void apply_vrem_job(vemb_v16_supernode_ctx_t *ctx,
                           const vemb_v16_vemb_job_t *job,
                           vemb_v16_supernode_scratch_t *scratch,
                           vemb_v16_channel_t *ch) {
    (void)scratch;
    (void)ch;
    vemb_v16_supernode_handle_vrem_job(ctx, job);
}

/// Execution: run one VADD job on the SuperNode storage/backend path.
static void apply_vadd_job(vemb_v16_supernode_ctx_t *ctx,
                           const vemb_v16_vadd_job_t *job,
                           vemb_v16_supernode_scratch_t *scratch,
                           vemb_v16_channel_t *ch) {
    (void)scratch;
    (void)ch;
    vemb_v16_supernode_handle_vadd_job(ctx, job);
}

static void notify_completion_consumer_from_proxy(vemb_v16_supernode_ctx_t *ctx) {
#ifdef __linux__
    assert(ctx->completion_notify_armed != NULL);
    assert(ctx->completion_notify_fd != NULL);
    int expected = 1;
    if (!atomic_compare_exchange_strong_explicit(ctx->completion_notify_armed,
                                                 &expected,
                                                 0,
                                                 memory_order_acq_rel,
                                                 memory_order_relaxed)) {
        return;
    }
    int notify_fd = *ctx->completion_notify_fd;
    if (notify_fd >= 0) {
        uint64_t one = 1;
        (void)write(notify_fd, &one, sizeof(one));
    }
#else
    (void)ctx;
#endif
}

static void publish_synthetic_completion(vemb_v16_supernode_ctx_t *ctx,
                                         const vemb_v16_completion_t *completion) {
    while (vemb_v16_aeron_publish(ctx->completion_ring, completion) != 0 &&
           atomic_load_explicit(ctx->running, memory_order_relaxed) &&
           atomic_load_explicit(ctx->channel_active, memory_order_acquire)) {
        atomic_fetch_add_explicit(&ctx->stats->supernode_completion_ring_full, 1,
                                  memory_order_relaxed);
        cpu_relax();
    }
    atomic_fetch_add_explicit(&ctx->stats->supernode_completion_publish, 1,
                              memory_order_relaxed);
    atomic_fetch_add_explicit(&ctx->stats->completed_jobs, 1,
                              memory_order_relaxed);
    notify_completion_consumer_from_proxy(ctx);
}

static void apply_unified_shard_job(vemb_v16_supernode_ctx_t *ctx,
                                    const vemb_v16_job_base_t *job,
                                    vemb_v16_supernode_scratch_t *scratch,
                                    vemb_v16_channel_t *ch) {
    (void)scratch;
    switch (job->kind) {
    case VEMB_V16_JOB_KIND_BASE:
        vemb_v16_supernode_handle_base_job(ctx, job);
        break;
    case VEMB_V16_JOB_KIND_READ:
        if (job->op == VEMB_V16_OP_VREM) {
            apply_vrem_job(ctx, (const vemb_v16_vemb_job_t *)job, scratch, ch);
        } else {
            apply_vemb_job(ctx, (const vemb_v16_vemb_job_t *)job, scratch, ch);
        }
        break;
    case VEMB_V16_JOB_KIND_VSIM_KEY_KEY:
        apply_vsim_key_key_job(ctx,
                               (const vemb_v16_vsim_key_key_job_t *)job,
                               scratch,
                               ch);
        break;
    case VEMB_V16_JOB_KIND_INLINE_VECTOR:
        apply_vadd_job(ctx, (const vemb_v16_vadd_job_t *)job, scratch, ch);
        break;
    default: {
        vemb_v16_completion_t completion = {
            .op = job->op,
            .req_id = job->req_id,
            .channel_id = job->channel_id,
            .channel_index = job->channel_index,
            .status = job->op == VEMB_V16_OP_PING ?
                VEMB_V16_STATUS_OK :
                VEMB_V16_STATUS_ERR,
        };
        publish_synthetic_completion(ctx, &completion);
        break;
    }
    }
}

static void publish_job_return(vemb_v16_proxy_t *proxy,
                               uint32_t proxy_worker_id,
                               uint32_t sn_work_id,
                               const vemb_v16_job_return_t *ret) {
    assert(proxy != NULL);
    assert(ret != NULL);
    uint32_t queue_index = shard_queue_index(proxy, proxy_worker_id, sn_work_id);
    vemb_v16_aeron_ring_t *ring = &proxy->job_return_queues[queue_index].ring;
    while (vemb_v16_aeron_publish(ring, ret) != 0 &&
           atomic_load_explicit(&proxy->running, memory_order_relaxed)) {
        cpu_relax();
    }
}

/// Execution scheduling: drain shard queues assigned to one SuperNode worker.
static int drain_shard_queues(vemb_v16_proxy_t *proxy,
                              uint32_t sn_work_id,
                              vemb_v16_supernode_scratch_t *scratch,
                              vemb_v16_shard_queue_t *queues,
                              vemb_v16_job_ref_t *job_refs) {
    assert(proxy != NULL);
    assert(scratch != NULL);
    assert(queues != NULL);
    assert(job_refs != NULL);
    RETURN_IF(!shard_queue_topology_ready(proxy) ||
              sn_work_id >= proxy->job_shard_supernode_count, 0);

    int did_work = 0;
    for (uint32_t work_id = 0; work_id < proxy->job_shard_proxy_count; work_id++) {
        uint32_t queue_index = shard_queue_index(proxy, work_id, sn_work_id);
        vemb_v16_aeron_ring_t *ring = &queues[queue_index].ring;
        uint32_t n = vemb_v16_aeron_poll_batch(ring, job_refs, VEMB_V16_PROXY_BATCH);
        if (!n) continue;

        did_work += (int)n;
        for (uint32_t i = 0; i < n; i++) {
            vemb_v16_job_ref_t *ref = &job_refs[i];
            if (ref->proxy_worker_id >= proxy->proxy_io_worker_count ||
                ref->pool_type >= VEMB_V16_JOB_POOL_COUNT)
                continue;
            vemb_v16_job_pool_t *pool =
                &proxy->proxy_io_workers[ref->proxy_worker_id].job_pools[ref->pool_type];
            if (ref->slot_id >= pool->slot_count)
                continue;
            vemb_v16_job_slot_t *slot = job_pool_slot(pool, ref->slot_id);
            if (slot->hdr.generation != ref->generation ||
                atomic_load_explicit(&slot->hdr.state, memory_order_acquire) !=
                    VEMB_V16_JOB_SLOT_PUBLISHED)
                continue;
            const vemb_v16_job_base_t *job_base = job_slot_payload_base(pool, slot);
            if (!job_base || job_base->op != ref->op)
                continue;
            atomic_store_explicit(&slot->hdr.state,
                                  VEMB_V16_JOB_SLOT_RUNNING,
                                  memory_order_release);
            vemb_v16_job_return_t job_return = {
                .pool_type = ref->pool_type,
                .slot_id = ref->slot_id,
                .generation = ref->generation,
            };
            if (job_base->channel_index >= VEMB_V16_MAX_CHANNELS) {
                publish_job_return(proxy, ref->proxy_worker_id, sn_work_id, &job_return);
                continue;
            }
            vemb_v16_channel_t *ch = &proxy->channels[job_base->channel_index];
            if (!supernode_channel_acquire(ch)) {
                publish_job_return(proxy, ref->proxy_worker_id, sn_work_id, &job_return);
                continue;
            }
            if (atomic_load_explicit(
                &ch->slot_channel_id, memory_order_acquire) == job_base->channel_id &&
                atomic_load_explicit(&ch->active, memory_order_acquire)) {
                if (diag_should_log_req(job_base->req_id)) {
                    serverLog(LL_DEBUG,
                              "vemb_v16 diag supernode dequeue: worker_id=%u queue=%s proxy_worker=%u queue_index=%u batch_index=%u batch_count=%u channel_index=%u channel_id=%llu req_id=%u op=%u flags=%u key_hash=%llu",
                              sn_work_id,
                              "job",
                              work_id,
                              queue_index,
                              i,
                              n,
                              job_base->channel_index,
                              (unsigned long long)job_base->channel_id,
                              job_base->req_id,
                              job_base->op,
                              job_base->flags,
                              (unsigned long long)job_base->key_hash);
                }
                vemb_v16_supernode_ctx_t ctx = ch->supernode_ctx;
                ctx.worker_id = sn_work_id;
                apply_unified_shard_job(&ctx, job_base, scratch, ch);
            }
            supernode_channel_release(ch);
            publish_job_return(proxy, ref->proxy_worker_id, sn_work_id, &job_return);
        }
    }
    return did_work;
}

/// Execution scheduling: drain FIFO job queues for one SuperNode worker.
static int drain_job_shard_queues(vemb_v16_proxy_t *proxy,
                                  uint32_t sn_work_id,
                                  vemb_v16_supernode_scratch_t *scratch) {
    assert(proxy != NULL);
    assert(scratch != NULL);
    return drain_shard_queues(proxy,
                              sn_work_id,
                              scratch,
                              proxy->job_shard_queues,
                              scratch->job_refs);
}

static int drain_job_return_queues(vemb_v16_proxy_t *proxy,
                                   uint32_t proxy_worker_id) {
    assert(proxy != NULL);
    RETURN_IF(!shard_queue_topology_ready(proxy) ||
              !proxy->job_return_queues ||
              proxy_worker_id >= proxy->job_shard_proxy_count,
              0);

    int reclaimed = 0;
    vemb_v16_job_return_t returns[VEMB_V16_PROXY_BATCH];
    for (uint32_t sn_id = 0; sn_id < proxy->job_shard_supernode_count; sn_id++) {
        uint32_t queue_index = shard_queue_index(proxy, proxy_worker_id, sn_id);
        vemb_v16_aeron_ring_t *ring = &proxy->job_return_queues[queue_index].ring;
        uint32_t n;
        while ((n = vemb_v16_aeron_poll_batch(ring,
                                              returns,
                                              VEMB_V16_PROXY_BATCH)) != 0) {
            reclaimed += (int)n;
            for (uint32_t i = 0; i < n; i++) {
                vemb_v16_job_return_t *ret = &returns[i];
                if (ret->pool_type >= VEMB_V16_JOB_POOL_COUNT)
                    continue;
                vemb_v16_job_pool_t *pool =
                    &proxy->proxy_io_workers[proxy_worker_id].job_pools[ret->pool_type];
                if (ret->slot_id >= pool->slot_count)
                    continue;
                vemb_v16_job_slot_t *slot = job_pool_slot(pool, ret->slot_id);
                if (slot->hdr.generation != ret->generation)
                    continue;
                if (atomic_load_explicit(&slot->hdr.state, memory_order_acquire) !=
                    VEMB_V16_JOB_SLOT_RUNNING) {
                    continue;
                }
                job_pool_release_slot(pool, ret->slot_id, 1);
            }
        }
    }
    return reclaimed;
}

#ifdef __linux__
/// Execution scheduling: check whether a SuperNode worker can sleep.
static int supernode_worker_has_pending(vemb_v16_proxy_t *proxy,
                                        uint32_t worker_id) {
    RETURN_IF(!shard_queue_topology_ready(proxy) ||
        worker_id >= proxy->job_shard_supernode_count, 0);

    for (uint32_t proxy_id = 0;
         proxy_id < proxy->job_shard_proxy_count;
         proxy_id++) {
        uint32_t queue_index = shard_queue_index(proxy, proxy_id, worker_id);
        if (vemb_v16_aeron_available(&proxy->job_shard_queues[queue_index].ring) != 0)
            return 1;
    }

    return 0;
}

static void supernode_worker_wait_for_jobs(vemb_v16_supernode_pool_worker_t *worker) {
    if (worker->notify_fd < 0)
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
        if (drain_job_shard_queues(proxy, worker->worker_id, &scratch) > 0)
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

static void scaleout_notify_sleep(uint32_t interval_us) {
    struct timespec ts = {
        .tv_sec = interval_us / 1000000u,
        .tv_nsec = (long)(interval_us % 1000000u) * 1000L,
    };
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
    }
}

static int scaleout_notify_connect_uds(const char *path, uint32_t timeout_ms) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    vemb_v16_net_set_timeouts(fd, timeout_ms);
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    if (!path || strlen(path) >= sizeof(addr.sun_path)) {
        close(fd);
        return -1;
    }
    strcpy(addr.sun_path, path);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int scaleout_notify_send_tcp(
        const vemb_v16_topology_endpoint_t *endpoint,
        const vemb_v16_scaleout_local_done_req_t *req,
        vemb_v16_scaleout_local_done_resp_t *resp) {
    int fd = vemb_v16_net_connect(endpoint->host,
                                  endpoint->tcp_port,
                                  VEMB_V16_SCALEOUT_NOTIFY_TIMEOUT_MS);
    if (fd < 0)
        return -1;
    uint8_t req_buf[64];
    size_t req_len = 0;
    if (vemb_v16_scaleout_local_done_req_encode(req_buf,
                                                sizeof(req_buf),
                                                req,
                                                &req_len) != 0 ||
        vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_SCALEOUT_LOCAL_DONE,
                                 0,
                                 0,
                                 0,
                                 req_buf,
                                 (uint32_t)req_len) != 0) {
        close(fd);
        return -1;
    }
    vemb_v16_net_hdr_t hdr;
    uint8_t resp_buf[64];
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_SCALEOUT_LOCAL_DONE_RESPONSE ||
        hdr.flags != 0 ||
        hdr.payload_len != vemb_v16_scaleout_local_done_resp_encoded_len() ||
        vemb_v16_net_read_full(fd, resp_buf, hdr.payload_len) != 0 ||
        vemb_v16_scaleout_local_done_resp_decode(resp,
                                                 resp_buf,
                                                 hdr.payload_len) != 0) {
        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}

static int scaleout_notify_send_uds(
        const vemb_v16_topology_endpoint_t *endpoint,
        const vemb_v16_scaleout_local_done_req_t *req,
        vemb_v16_scaleout_local_done_resp_t *resp) {
    int fd = scaleout_notify_connect_uds(
        endpoint->uds_path,
        VEMB_V16_SCALEOUT_NOTIFY_TIMEOUT_MS);
    if (fd < 0)
        return -1;
    uint8_t op = VEMB_V16_CTRL_SCALEOUT_LOCAL_DONE;
    if (vemb_v16_net_write_full(fd, &op, sizeof(op)) != 0 ||
        vemb_v16_net_write_full(fd, req, sizeof(*req)) != 0 ||
        vemb_v16_net_read_full(fd, resp, sizeof(*resp)) != 0) {
        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}

static int scaleout_notify_send(
        const vemb_v16_topology_endpoint_t *endpoint,
        const vemb_v16_scaleout_local_done_req_t *req,
        vemb_v16_scaleout_local_done_resp_t *resp) {
    memset(resp, 0, sizeof(*resp));
    if (endpoint->transport_type == VEMB_V16_TRANSPORT_TCP)
        return scaleout_notify_send_tcp(endpoint, req, resp);
    if (endpoint->transport_type == VEMB_V16_TRANSPORT_AERON)
        return scaleout_notify_send_uds(endpoint, req, resp);
    return -1;
}

static int scaleout_notify_resp_matches(
        const vemb_v16_scaleout_local_done_req_t *req,
        const vemb_v16_scaleout_local_done_resp_t *resp) {
    return resp->status == VEMB_V16_STATUS_OK &&
           resp->migration_topology_epoch ==
               req->migration_topology_epoch &&
           resp->cutover_topology_epoch ==
               req->cutover_topology_epoch &&
           resp->source_owner == req->source_owner &&
           resp->notify_seq == req->notify_seq;
}

static void *scaleout_notify_main(void *arg) {
    vemb_v16_proxy_t *proxy = arg;
    serverLog(LL_NOTICE,
              "vemb_v16 scaleout notify worker started: interval_us=%u",
              proxy->scaleout_notify_interval_us);
    while (atomic_load_explicit(&proxy->running, memory_order_relaxed) &&
           !atomic_load_explicit(&proxy->scaleout_notify_stop,
                                 memory_order_acquire)) {
        vemb_v16_storage_scaleout_auto_status_t status;
        memset(&status, 0, sizeof(status));
        if (vemb_v16_storage_scaleout_auto_get_status(proxy_storage(proxy),
                                                      &status) == 0 &&
            status.enabled &&
            status.coordinated &&
            status.phase == VEMB_V16_STORAGE_SCALEOUT_NOTIFY_PENDING &&
            status.coordinator_endpoint_valid) {
            vemb_v16_scaleout_local_done_req_t req = {
                .migration_topology_epoch = status.migration_epoch,
                .cutover_topology_epoch = status.cutover_epoch,
                .notify_seq = status.notify_seq,
                .source_owner = status.source_owner,
                .phase = status.phase,
                .error_code = status.last_error,
                .pending_delta = status.pending_delta,
                .baseline_retry_pending = status.baseline_retry_pending,
                .migrating_key_count = status.migrating_key_count,
                .range_count = status.range_count,
            };
            vemb_v16_scaleout_local_done_resp_t resp;
            memset(&resp, 0, sizeof(resp));
            if (scaleout_notify_send(&status.coordinator_endpoint,
                                     &req,
                                     &resp) == 0 &&
                scaleout_notify_resp_matches(&req, &resp)) {
                (void)vemb_v16_storage_scaleout_auto_mark_notified(
                    proxy_storage(proxy),
                    req.migration_topology_epoch,
                    req.source_owner,
                    req.notify_seq);
            }
        }
        scaleout_notify_sleep(proxy->scaleout_notify_interval_us);
    }
    serverLog(LL_NOTICE, "vemb_v16 scaleout notify worker stopped");
    return NULL;
}

static int start_scaleout_notify_worker(vemb_v16_proxy_t *proxy) {
    if (proxy->scaleout_notify_thread_started)
        return 0;
    proxy->scaleout_notify_interval_us =
        proxy->scaleout_notify_interval_us ?
            proxy->scaleout_notify_interval_us :
            VEMB_V16_SCALEOUT_NOTIFY_INTERVAL_US;
    atomic_store_explicit(&proxy->scaleout_notify_stop,
                          0,
                          memory_order_release);
    if (pthread_create(&proxy->scaleout_notify_thread,
                       NULL,
                       scaleout_notify_main,
                       proxy) != 0) {
        return -1;
    }
    proxy->scaleout_notify_thread_started = 1;
    return 0;
}

static void stop_scaleout_notify_worker(vemb_v16_proxy_t *proxy) {
    if (!proxy->scaleout_notify_thread_started)
        return;
    atomic_store_explicit(&proxy->scaleout_notify_stop,
                          1,
                          memory_order_release);
    pthread_join(proxy->scaleout_notify_thread, NULL);
    proxy->scaleout_notify_thread_started = 0;
}

/// TCP control plane: write a small status response frame.
int vemb_v16_proxy_create(vemb_v16_proxy_t **out,
                          const char *uds_path,
                          uint32_t vector_dim,
                          uint32_t max_vectors,
                          vemb_v16_storage_ctx_t *storage,
                          const vemb_v16_warm_regions_manifest_t *manifest) {
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
    atomic_init(&proxy->scaleout_notify_stop, 0);
    proxy->scaleout_notify_interval_us =
        VEMB_V16_SCALEOUT_NOTIFY_INTERVAL_US;
    pthread_mutex_init(&proxy->stats_lock, NULL);
    proxy->storage = storage;
    if (manifest &&
        manifest->has_job_plane_backend_type &&
        manifest->job_plane_path[0] != '\0') {
        proxy->job_pool_slots_backend_type = manifest->job_plane_backend_type;
        proxy->job_pool_slots_mmap_offset = manifest->job_plane_mmap_offset;
        strncpy(proxy->job_pool_slots_path,
                manifest->job_plane_path,
                sizeof(proxy->job_pool_slots_path) - 1);
        proxy->job_pool_slots_path[sizeof(proxy->job_pool_slots_path) - 1] = '\0';
    }

    serverLog(LL_NOTICE, "vemb_v16 proxy created: uds=%s dim=%u max_vectors=%u vector_region=%s size=%zu job_pool_slots=%s backend=%u offset=%llu",
              proxy->uds_path,
              proxy->vector_dim,
              proxy->max_vectors,
              vemb_v16_storage_vector_region_name(proxy_storage(proxy)),
              vemb_v16_storage_vector_region_size(proxy_storage(proxy)),
              proxy->job_pool_slots_path[0] ? proxy->job_pool_slots_path : "(heap)",
              proxy->job_pool_slots_backend_type,
              (unsigned long long)proxy->job_pool_slots_mmap_offset);
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
    stop_scaleout_notify_worker(proxy);
    stop_proxy_io_pool(proxy);
    stop_supernode_pool(proxy);
    free_job_shard_queues(proxy);
    free_job_return_queues(proxy);
    cleanup_proxy_io_job_pools(proxy);
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
    if (init_job_shard_queues(proxy) != 0) {
        serverLog(LL_WARNING, "vemb_v16 job shard queue init failed: proxy_io_threads=%u supernode_workers=%u",
                  proxy->proxy_io_worker_count,
                  proxy->supernode_worker_count);
        goto cleanup;
    }
    if (init_job_return_queues(proxy) != 0) {
        serverLog(LL_WARNING, "vemb_v16 job return queue init failed: proxy_io_threads=%u supernode_workers=%u",
                  proxy->proxy_io_worker_count,
                  proxy->supernode_worker_count);
        goto cleanup;
    }
    if (init_proxy_io_job_pools(proxy) != 0) {
        serverLog(LL_WARNING, "vemb_v16 job pool init failed: proxy_io_threads=%u",
                  proxy->proxy_io_worker_count);
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
    if (start_scaleout_notify_worker(proxy) != 0) {
        serverLog(LL_WARNING, "vemb_v16 scaleout notify worker start failed");
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
    stop_scaleout_notify_worker(proxy);
    stop_proxy_io_pool(proxy);
    stop_supernode_pool(proxy);
    free_job_shard_queues(proxy);
    free_job_return_queues(proxy);
    cleanup_proxy_io_job_pools(proxy);
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

int vemb_v16_proxy_migration_mark_migrating(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_control_req_t *req,
    vemb_v16_migration_control_resp_t *resp) {
    if (!migration_control_req_valid(req)) {
        migration_control_fill_resp(resp, VEMB_V16_STATUS_ERR, NULL);
        return -1;
    }

    tlc_core_key_migration_info_t info = {0};
    int rc = vemb_v16_storage_migration_mark_migrating_in_shard(
        proxy_storage(proxy),
        req->key,
        req->key_len,
        req->key_hash,
        req->topology_epoch,
        req->target_owner,
        req->shard_id,
        &info);
    migration_control_fill_resp(resp,
                                rc == 0 ? VEMB_V16_STATUS_OK :
                                    VEMB_V16_STATUS_ERR,
                                rc == 0 ? &info : NULL);
    return rc;
}

int vemb_v16_proxy_migration_mark_cutover(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_control_req_t *req,
    vemb_v16_migration_control_resp_t *resp) {
    if (!migration_control_req_valid(req)) {
        migration_control_fill_resp(resp, VEMB_V16_STATUS_ERR, NULL);
        return -1;
    }

    tlc_core_key_migration_info_t info = {0};
    int rc = vemb_v16_storage_migration_mark_cutover_in_shard(
        proxy_storage(proxy),
        req->key,
        req->key_len,
        req->key_hash,
        req->topology_epoch,
        req->target_owner,
        req->shard_id,
        &info);
    migration_control_fill_resp(resp,
                                rc == 0 ? VEMB_V16_STATUS_OK :
                                    VEMB_V16_STATUS_ERR,
                                rc == 0 ? &info : NULL);
    return rc;
}

int vemb_v16_proxy_migration_mark_source_gc(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_control_req_t *req,
    vemb_v16_migration_control_resp_t *resp) {
    if (!migration_control_req_valid(req)) {
        migration_control_fill_resp(resp, VEMB_V16_STATUS_ERR, NULL);
        return -1;
    }

    tlc_core_key_migration_info_t info = {0};
    int rc = vemb_v16_storage_migration_mark_source_gc_in_shard(
        proxy_storage(proxy),
        req->key,
        req->key_len,
        req->key_hash,
        req->topology_epoch,
        req->target_owner,
        req->shard_id,
        &info);
    migration_control_fill_resp(resp,
                                rc == 0 ? VEMB_V16_STATUS_OK :
                                    VEMB_V16_STATUS_ERR,
                                rc == 0 ? &info : NULL);
    return rc;
}

int vemb_v16_proxy_migration_barrier(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_control_req_t *req,
    vemb_v16_migration_control_resp_t *resp) {
    if (!migration_control_req_valid(req)) {
        migration_control_fill_resp(resp, VEMB_V16_STATUS_ERR, NULL);
        return -1;
    }

    tlc_core_key_migration_info_t info = {0};
    vemb_v16_migration_outbox_stats_t outbox_stats = {0};
    int rc = vemb_v16_storage_migration_barrier_in_shard(
        proxy_storage(proxy),
        req->key,
        req->key_len,
        req->key_hash,
        req->topology_epoch,
        req->target_owner,
        req->shard_id,
        &info,
        &outbox_stats);
    migration_control_fill_resp(resp,
                                rc == 0 ? VEMB_V16_STATUS_OK :
                                    VEMB_V16_STATUS_ERR,
                                rc == 0 ? &info : NULL);
    if (rc == 0)
        migration_control_fill_outbox(resp, &outbox_stats);
    return rc;
}

int vemb_v16_proxy_migration_mark_migrating_batch(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_control_batch_req_t *req,
    vemb_v16_migration_control_batch_resp_t *resp) {
    memset(resp, 0, sizeof(*resp));
    resp->status = VEMB_V16_STATUS_ERR;

    if (!req ||
        req->entry_count == 0 ||
        req->entry_count > VEMB_V16_MIGRATION_CONTROL_MAX_BATCH) {
        return -1;
    }

    resp->entry_count = req->entry_count;
    int rc = 0;
    for (uint32_t i = 0; i < req->entry_count; i++) {
        if (vemb_v16_proxy_migration_mark_migrating(proxy,
                                                    &req->entries[i],
                                                    &resp->entries[i]) == 0) {
            resp->success_count++;
        } else {
            resp->error_count++;
            rc = -1;
        }
    }
    resp->status = rc == 0 ? VEMB_V16_STATUS_OK : VEMB_V16_STATUS_ERR;
    return rc;
}

static void migration_range_control_fill_error(
        vemb_v16_migration_range_control_resp_t *resp,
        const vemb_v16_migration_range_control_req_t *req) {
    memset(resp, 0, sizeof(*resp));
    resp->status = VEMB_V16_STATUS_ERR;
    if (!req)
        return;
    resp->migration_topology_epoch = req->migration_topology_epoch;
    resp->cutover_topology_epoch = req->cutover_topology_epoch;
    resp->owner_epoch = req->cutover_topology_epoch;
    resp->target_owner = req->target_owner;
    resp->shard_id = req->shard_id;
    resp->page_limit = req->page_limit;
}

static int migration_range_control_req_valid(
        const vemb_v16_migration_range_control_req_t *req,
        int need_cutover_epoch) {
    return req &&
           req->migration_topology_epoch != 0 &&
           req->target_owner != UINT32_MAX &&
           (!need_cutover_epoch ||
            req->cutover_topology_epoch >= req->migration_topology_epoch);
}

int vemb_v16_proxy_migration_range_barrier(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_range_control_req_t *req,
    vemb_v16_migration_range_control_resp_t *resp) {
    if (!migration_range_control_req_valid(req, 0)) {
        migration_range_control_fill_error(resp, req);
        return -1;
    }
    return vemb_v16_storage_migration_range_barrier(
        proxy_storage(proxy),
        req->migration_topology_epoch,
        req->target_owner,
        req->shard_id,
        req->page_limit,
        resp);
}

int vemb_v16_proxy_migration_range_mark_cutover(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_range_control_req_t *req,
    vemb_v16_migration_range_control_resp_t *resp) {
    if (!migration_range_control_req_valid(req, 1)) {
        migration_range_control_fill_error(resp, req);
        return -1;
    }
    return vemb_v16_storage_migration_range_mark_cutover(
        proxy_storage(proxy),
        req->migration_topology_epoch,
        req->cutover_topology_epoch,
        req->target_owner,
        req->shard_id,
        req->page_limit,
        resp);
}

int vemb_v16_proxy_migration_range_mark_source_gc(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_range_control_req_t *req,
    vemb_v16_migration_range_control_resp_t *resp) {
    if (!migration_range_control_req_valid(req, 1)) {
        migration_range_control_fill_error(resp, req);
        return -1;
    }
    return vemb_v16_storage_migration_range_mark_source_gc(
        proxy_storage(proxy),
        req->migration_topology_epoch,
        req->cutover_topology_epoch,
        req->target_owner,
        req->shard_id,
        req->page_limit,
        resp);
}

int vemb_v16_proxy_epoch_set(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_epoch_control_req_t *req,
    vemb_v16_epoch_control_resp_t *resp) {
    int rc = (!req || req->min_write_epoch > req->current_topology_epoch) ?
        -1 :
        vemb_v16_storage_epoch_set(proxy_storage(proxy),
                                   req->current_topology_epoch,
                                   req->min_write_epoch);
    epoch_control_fill_resp(proxy,
                            resp,
                            rc == 0 ? VEMB_V16_STATUS_OK :
                                VEMB_V16_STATUS_ERR);
    return rc;
}

int vemb_v16_proxy_epoch_get(
    vemb_v16_proxy_t *proxy,
    vemb_v16_epoch_control_resp_t *resp) {
    epoch_control_fill_resp(proxy, resp, VEMB_V16_STATUS_OK);
    return 0;
}

int vemb_v16_proxy_topology_set(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_topology_control_req_t *req,
    vemb_v16_topology_control_resp_t *resp) {
    int rc = vemb_v16_storage_topology_set(proxy_storage(proxy), req);
    topology_control_fill_resp(proxy,
                               resp,
                               rc == 0 ? VEMB_V16_STATUS_OK :
                                   VEMB_V16_STATUS_ERR);
    return rc;
}

int vemb_v16_proxy_topology_get(
    vemb_v16_proxy_t *proxy,
    vemb_v16_topology_control_resp_t *resp) {
    topology_control_fill_resp(proxy, resp, VEMB_V16_STATUS_OK);
    return 0;
}

void vemb_v16_proxy_get_stats(vemb_v16_proxy_t *proxy, vemb_v16_stats_t *stats) {
    assert(proxy != NULL);
    assert(stats != NULL);
    memset(stats, 0, sizeof(*stats));
    pthread_mutex_lock(&proxy->stats_lock);
    vemb_v16_stats_add(stats, &proxy->closed_stats);
    pthread_mutex_unlock(&proxy->stats_lock);
    sve_operation_stats_t *sve_stats =
        vemb_v16_storage_sve_stats(proxy_storage(proxy));
    if (sve_stats) {
        stats->bitmap_lock_success =
            atomic_load_explicit(&sve_stats->lock_success, memory_order_relaxed);
        stats->bitmap_lock_failure =
            atomic_load_explicit(&sve_stats->lock_failure, memory_order_relaxed);
    }
    tlc_core_stats_t core_stats;
    tlc_core_get_stats(proxy_storage(proxy)->tlc->core, &core_stats);
    stats->warm_region_count = core_stats.warm_region_count;
    stats->warm_region_full_count = core_stats.warm_region_full_count;
    stats->warm_alloc_local = core_stats.warm_alloc_local;
    stats->warm_alloc_remote = core_stats.warm_alloc_remote;
    stats->warm_alloc_fallback = core_stats.warm_alloc_fallback;
    stats->warm_alloc_cold_spill = core_stats.warm_alloc_cold_spill;
    stats->warm_alloc_fail = core_stats.warm_alloc_fail;
    stats->warm_eviction_success = core_stats.warm_eviction_success;
    stats->warm_eviction_fail = core_stats.warm_eviction_fail;
    stats->warm_same_key_overwrite = core_stats.warm_same_key_overwrite;
    stats->warm_stale_handle_reject = core_stats.warm_stale_handle_reject;
    stats->remote_meta_stale = core_stats.remote_meta_stale;
    stats->warm_region_hash_local_pct = core_stats.warm_region_hash_local_pct;
    vemb_v16_stats_t tlc_runtime_stats;
    memset(&tlc_runtime_stats, 0, sizeof(tlc_runtime_stats));
    vemb_v16_tlc_get_runtime_stats(proxy_storage(proxy)->tlc,
                                   &tlc_runtime_stats);
    vemb_v16_stats_add(stats, &tlc_runtime_stats);
    stats->source_gc_count += atomic_load_explicit(
        &proxy_storage(proxy)->migration_source_gc_count,
        memory_order_relaxed);
    uint64_t gc_safe_watermark = atomic_load_explicit(
        &proxy_storage(proxy)->migration_gc_safe_watermark,
        memory_order_relaxed);
    if (stats->gc_safe_watermark < gc_safe_watermark)
        stats->gc_safe_watermark = gc_safe_watermark;
    stats->migration_baseline_sent += atomic_load_explicit(
        &proxy_storage(proxy)->migration_baseline_sent_count,
        memory_order_relaxed);
    stats->migration_baseline_skipped += atomic_load_explicit(
        &proxy_storage(proxy)->migration_baseline_skipped_count,
        memory_order_relaxed);
    stats->migration_baseline_error += atomic_load_explicit(
        &proxy_storage(proxy)->migration_baseline_error_count,
        memory_order_relaxed);
    stats->migration_baseline_retry_queued += atomic_load_explicit(
        &proxy_storage(proxy)->migration_baseline_retry_queued_count,
        memory_order_relaxed);
    stats->migration_baseline_retry_sent += atomic_load_explicit(
        &proxy_storage(proxy)->migration_baseline_retry_sent_count,
        memory_order_relaxed);
    pthread_mutex_lock(&proxy_storage(proxy)->migration_outbox_lock);
    stats->migration_baseline_retry_pending +=
        proxy_storage(proxy)->migration_baseline_retry_count;
    pthread_mutex_unlock(&proxy_storage(proxy)->migration_outbox_lock);
    if (proxy->job_shard_queues) {
        uint32_t count = proxy->job_shard_proxy_count *
            proxy->job_shard_supernode_count;
        for (uint32_t i = 0; i < count; i++) {
            stats->job_shard_queue_depth +=
                vemb_v16_aeron_available(&proxy->job_shard_queues[i].ring);
        }
    }
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        vemb_v16_channel_t *ch = &proxy->channels[i];
        if (atomic_load_explicit(&ch->active, memory_order_acquire)) {
            stats->active_channels++;
            vemb_v16_stats_add_channel_counters(stats, &ch->stats);
            if (ch->request_ring)
                stats->request_ring_depth += vemb_v16_client_available(ch->request_ring);
            if (ch->response_ring)
                stats->response_ring_depth += vemb_v16_client_available(ch->response_ring);
            stats->completion_ring_depth += vemb_v16_aeron_available(&ch->completion_ring);
        }
    }
}
