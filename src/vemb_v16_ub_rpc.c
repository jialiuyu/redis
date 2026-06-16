#define _GNU_SOURCE

#include "vemb_v16_ub_rpc.h"

#include "cpu_relax.h"
#include "vemb_v16_log.h"
#include "vemb_v16_mapped_region.h"
#include "zmalloc.h"

#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <string.h>
#include <time.h>

#define VEMB_V16_UB_RPC_MAGIC 0x56315552u
#define VEMB_V16_UB_RPC_VERSION 1u
#define VEMB_V16_UB_RPC_RING_BITS 12u
#define VEMB_V16_UB_RPC_RING_SIZE (1u << VEMB_V16_UB_RPC_RING_BITS)
#define VEMB_V16_UB_RPC_RING_MASK (VEMB_V16_UB_RPC_RING_SIZE - 1u)
#define VEMB_V16_UB_RPC_PENDING_BITS 12u
#define VEMB_V16_UB_RPC_PENDING_SIZE (1u << VEMB_V16_UB_RPC_PENDING_BITS)
#define VEMB_V16_UB_RPC_PENDING_MASK (VEMB_V16_UB_RPC_PENDING_SIZE - 1u)
#define VEMB_V16_UB_RPC_DEFAULT_TIMEOUT_MS 100u
#define VEMB_V16_UB_RPC_LOG_LIMIT 32u
#define VEMB_V16_UB_RPC_LISTENER_BATCH 64u

typedef struct vemb_v16_ub_rpc_wire_req {
    uint32_t magic;
    uint32_t version;
    vemb_v16_ub_lookup_rpc_req_t req;
} vemb_v16_ub_rpc_wire_req_t;

typedef struct vemb_v16_ub_rpc_wire_resp {
    uint32_t magic;
    uint32_t version;
    vemb_v16_ub_lookup_rpc_resp_t resp;
} vemb_v16_ub_rpc_wire_resp_t;

typedef struct vemb_v16_ub_rpc_ring_slot {
    _Alignas(64) atomic_uint_fast64_t sequence;
    uint8_t payload[];
} vemb_v16_ub_rpc_ring_slot_t;

typedef struct vemb_v16_ub_rpc_shared_ring {
    uint32_t magic;
    uint32_t version;
    uint32_t slot_size;
    uint32_t slot_count;
    uint32_t slot_mask;
    uint32_t slot_stride;
    uint32_t reserved0[10];
    _Alignas(64) atomic_uint_fast64_t head;
    _Alignas(64) atomic_uint_fast64_t tail;
    uint8_t slots[];
} vemb_v16_ub_rpc_shared_ring_t;

typedef struct vemb_v16_ub_rpc_ring {
    vemb_v16_ub_rpc_ring_config_t config;
    vemb_v16_mapped_region_t mapping;
    vemb_v16_ub_rpc_shared_ring_t *ring;
    size_t bytes;
    uint32_t slot_size;
    uint32_t slot_stride;
} vemb_v16_ub_rpc_ring_t;

typedef enum vemb_v16_ub_rpc_pending_state {
    VEMB_V16_UB_RPC_PENDING_EMPTY = 0,
    VEMB_V16_UB_RPC_PENDING_CLAIMING = 1,
    VEMB_V16_UB_RPC_PENDING_WAITING = 2,
    VEMB_V16_UB_RPC_PENDING_WRITING = 3,
    VEMB_V16_UB_RPC_PENDING_READY = 4,
} vemb_v16_ub_rpc_pending_state_t;

typedef struct vemb_v16_ub_rpc_pending {
    atomic_uint state;
    uint32_t reserved0;
    atomic_uint_fast64_t request_id;
    vemb_v16_ub_lookup_rpc_resp_t resp;
} vemb_v16_ub_rpc_pending_t;

typedef struct vemb_v16_ub_rpc_peer_state {
    uint32_t owner_id;
    vemb_v16_ub_rpc_ring_t request;
    vemb_v16_ub_rpc_ring_t response;
    vemb_v16_ub_rpc_ring_t inbound_request;
    vemb_v16_ub_rpc_ring_t outbound_response;
    vemb_v16_ub_rpc_pending_t pending[VEMB_V16_UB_RPC_PENDING_SIZE];
} vemb_v16_ub_rpc_peer_state_t;

struct vemb_v16_ub_rpc {
    vemb_v16_tlc_t *tlc;
    uint32_t local_owner_id;
    uint32_t timeout_ms;
    uint32_t peer_count;
    vemb_v16_ub_rpc_peer_state_t peers[VEMB_V16_UB_RPC_MAX_PEERS];
    pthread_t thread;
    int thread_started;
    atomic_int running;
    atomic_uint_fast32_t timeout_logs;
    atomic_uint_fast32_t ring_full_logs;
    atomic_uint_fast32_t error_logs;
};

static uint64_t rpc_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint64_t timeout_from_req_ns(const vemb_v16_ub_rpc_t *rpc,
                                    const vemb_v16_ub_lookup_rpc_req_t *req) {
    if (req && req->timeout_ns)
        return req->timeout_ns;
    uint32_t timeout_ms = rpc->timeout_ms ? rpc->timeout_ms :
        VEMB_V16_UB_RPC_DEFAULT_TIMEOUT_MS;
    return (uint64_t)timeout_ms * 1000000ull;
}

static void tiny_pause(void) {
    for (uint32_t i = 0; i < 64; i++)
        cpu_relax();
    sched_yield();
}

static size_t align64_size(size_t value) {
    return (value + 63u) & ~(size_t)63u;
}

static uint32_t rpc_ring_slot_stride(uint32_t slot_size) {
    return (uint32_t)align64_size(sizeof(vemb_v16_ub_rpc_ring_slot_t) +
                                  (size_t)slot_size);
}

static size_t rpc_ring_bytes(uint32_t slot_size) {
    uint32_t stride = rpc_ring_slot_stride(slot_size);
    return sizeof(vemb_v16_ub_rpc_shared_ring_t) +
           (size_t)stride * VEMB_V16_UB_RPC_RING_SIZE;
}

static vemb_v16_ub_rpc_ring_slot_t *rpc_ring_slot(
        vemb_v16_ub_rpc_shared_ring_t *ring,
        uint64_t pos) {
    return (vemb_v16_ub_rpc_ring_slot_t *)
        (ring->slots + (pos & ring->slot_mask) * ring->slot_stride);
}

static void rpc_ring_init(vemb_v16_ub_rpc_shared_ring_t *ring,
                          uint32_t slot_size) {
    uint32_t stride = rpc_ring_slot_stride(slot_size);
    memset(ring, 0, rpc_ring_bytes(slot_size));
    ring->magic = VEMB_V16_UB_RPC_MAGIC;
    ring->version = VEMB_V16_UB_RPC_VERSION;
    ring->slot_size = slot_size;
    ring->slot_count = VEMB_V16_UB_RPC_RING_SIZE;
    ring->slot_mask = VEMB_V16_UB_RPC_RING_MASK;
    ring->slot_stride = stride;
    atomic_init(&ring->head, 0);
    atomic_init(&ring->tail, 0);
    for (uint32_t i = 0; i < VEMB_V16_UB_RPC_RING_SIZE; i++) {
        vemb_v16_ub_rpc_ring_slot_t *slot = rpc_ring_slot(ring, i);
        atomic_init(&slot->sequence, i);
    }
}

static int rpc_ring_ready(const vemb_v16_ub_rpc_ring_t *ring) {
    return ring &&
           ring->ring &&
           ring->ring->magic == VEMB_V16_UB_RPC_MAGIC &&
           ring->ring->version == VEMB_V16_UB_RPC_VERSION &&
           ring->ring->slot_size == ring->slot_size &&
           ring->ring->slot_count == VEMB_V16_UB_RPC_RING_SIZE &&
           ring->ring->slot_mask == VEMB_V16_UB_RPC_RING_MASK &&
           ring->ring->slot_stride == ring->slot_stride;
}

static int rpc_ring_publish(vemb_v16_ub_rpc_shared_ring_t *ring,
                            const void *payload) {
    uint64_t pos = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    vemb_v16_ub_rpc_ring_slot_t *slot = NULL;
    for (;;) {
        slot = rpc_ring_slot(ring, pos);
        uint64_t seq = atomic_load_explicit(&slot->sequence,
                                            memory_order_acquire);
        int64_t diff = (int64_t)seq - (int64_t)pos;
        if (diff == 0) {
            uint64_t desired = pos + 1;
            if (atomic_compare_exchange_weak_explicit(
                    &ring->tail,
                    &pos,
                    desired,
                    memory_order_relaxed,
                    memory_order_relaxed)) {
                break;
            }
        } else if (diff < 0) {
            return -1;
        } else {
            pos = atomic_load_explicit(&ring->tail,
                                       memory_order_relaxed);
        }
    }
    memcpy(slot->payload, payload, ring->slot_size);
    atomic_store_explicit(&slot->sequence, pos + 1, memory_order_release);
    return 0;
}

static int rpc_ring_poll(vemb_v16_ub_rpc_shared_ring_t *ring,
                         void *payload) {
    uint64_t pos = atomic_load_explicit(&ring->head, memory_order_relaxed);
    vemb_v16_ub_rpc_ring_slot_t *slot = NULL;
    for (;;) {
        slot = rpc_ring_slot(ring, pos);
        uint64_t seq = atomic_load_explicit(&slot->sequence,
                                            memory_order_acquire);
        int64_t diff = (int64_t)seq - (int64_t)(pos + 1);
        if (diff == 0) {
            uint64_t desired = pos + 1;
            if (atomic_compare_exchange_weak_explicit(
                    &ring->head,
                    &pos,
                    desired,
                    memory_order_relaxed,
                    memory_order_relaxed)) {
                break;
            }
        } else if (diff < 0) {
            return 0;
        } else {
            pos = atomic_load_explicit(&ring->head,
                                       memory_order_relaxed);
        }
    }
    memcpy(payload, slot->payload, ring->slot_size);
    atomic_store_explicit(&slot->sequence,
                          pos + ring->slot_count,
                          memory_order_release);
    return 1;
}

static void log_limited(atomic_uint_fast32_t *counter,
                        int level,
                        const char *fmt,
                        uint32_t owner_id,
                        uint64_t request_id,
                        uint64_t key_hash,
                        const char *path,
                        int err) {
    uint32_t n = atomic_fetch_add_explicit(counter, 1,
                                          memory_order_relaxed);
    if (n >= VEMB_V16_UB_RPC_LOG_LIMIT)
        return;
    serverLog(level,
              fmt,
              owner_id,
              (unsigned long long)request_id,
              (unsigned long long)key_hash,
              path ? path : "(none)",
              err,
              strerror(err));
}

static int ring_config_valid(const vemb_v16_ub_rpc_ring_config_t *config) {
    return config &&
           (config->backend_type == VEMB_V16_REGION_LOCAL_SHM ||
            config->backend_type == VEMB_V16_REGION_UB) &&
           config->path[0] != '\0';
}

static int ring_open(vemb_v16_ub_rpc_ring_t *ring,
                     const vemb_v16_ub_rpc_ring_config_t *config,
                     uint32_t slot_size,
                     int init_on_open) {
    if (!ring_config_valid(config) || slot_size == 0)
        return -1;
    memset(ring, 0, sizeof(*ring));
    ring->mapping.fd = -1;
    ring->config = *config;
    ring->slot_size = slot_size;
    ring->slot_stride = rpc_ring_slot_stride(slot_size);
    ring->bytes = rpc_ring_bytes(slot_size);
    if (vemb_v16_mapped_region_open(&ring->mapping,
                                    config->backend_type,
                                    config->path,
                                    config->mmap_offset,
                                    ring->bytes) != 0) {
        return -1;
    }
    ring->ring = (vemb_v16_ub_rpc_shared_ring_t *)ring->mapping.mapped_addr;
    if (init_on_open && !rpc_ring_ready(ring))
        rpc_ring_init(ring->ring, slot_size);
    return 0;
}

static void ring_close(vemb_v16_ub_rpc_ring_t *ring) {
    if (!ring)
        return;
    vemb_v16_mapped_region_close(&ring->mapping);
    ring->ring = NULL;
    ring->bytes = 0;
}

static vemb_v16_ub_rpc_peer_state_t *find_peer(vemb_v16_ub_rpc_t *rpc,
                                               uint32_t owner_id) {
    for (uint32_t i = 0; i < rpc->peer_count; i++) {
        if (rpc->peers[i].owner_id == owner_id)
            return &rpc->peers[i];
    }
    return NULL;
}

static vemb_v16_ub_rpc_pending_t *pending_claim(
        vemb_v16_ub_rpc_peer_state_t *peer,
        uint64_t request_id) {
    uint32_t start = (uint32_t)request_id & VEMB_V16_UB_RPC_PENDING_MASK;
    for (uint32_t i = 0; i < VEMB_V16_UB_RPC_PENDING_SIZE; i++) {
        vemb_v16_ub_rpc_pending_t *slot =
            &peer->pending[(start + i) & VEMB_V16_UB_RPC_PENDING_MASK];
        uint32_t expected = VEMB_V16_UB_RPC_PENDING_EMPTY;
        if (atomic_compare_exchange_strong_explicit(
                &slot->state,
                &expected,
                VEMB_V16_UB_RPC_PENDING_CLAIMING,
                memory_order_acquire,
                memory_order_relaxed)) {
            atomic_store_explicit(&slot->request_id,
                                  request_id,
                                  memory_order_relaxed);
            memset(&slot->resp, 0, sizeof(slot->resp));
            atomic_store_explicit(&slot->state,
                                  VEMB_V16_UB_RPC_PENDING_WAITING,
                                  memory_order_release);
            return slot;
        }
    }
    return NULL;
}

static void pending_release_waiting(vemb_v16_ub_rpc_pending_t *slot) {
    if (!slot)
        return;
    uint32_t expected = VEMB_V16_UB_RPC_PENDING_WAITING;
    if (atomic_compare_exchange_strong_explicit(
            &slot->state,
            &expected,
            VEMB_V16_UB_RPC_PENDING_EMPTY,
            memory_order_acq_rel,
            memory_order_relaxed)) {
    }
}

static vemb_v16_ub_rpc_pending_t *pending_find(
        vemb_v16_ub_rpc_peer_state_t *peer,
        uint64_t request_id) {
    uint32_t start = (uint32_t)request_id & VEMB_V16_UB_RPC_PENDING_MASK;
    for (uint32_t i = 0; i < VEMB_V16_UB_RPC_PENDING_SIZE; i++) {
        vemb_v16_ub_rpc_pending_t *slot =
            &peer->pending[(start + i) & VEMB_V16_UB_RPC_PENDING_MASK];
        uint32_t state = atomic_load_explicit(&slot->state,
                                              memory_order_acquire);
        if (state == VEMB_V16_UB_RPC_PENDING_WAITING &&
            atomic_load_explicit(&slot->request_id,
                                 memory_order_acquire) == request_id) {
            return slot;
        }
    }
    return NULL;
}

static int pending_complete(vemb_v16_ub_rpc_peer_state_t *peer,
                            const vemb_v16_ub_lookup_rpc_resp_t *resp) {
    vemb_v16_ub_rpc_pending_t *slot =
        pending_find(peer, resp->request_id);
    if (!slot)
        return -1;
    uint32_t expected = VEMB_V16_UB_RPC_PENDING_WAITING;
    if (!atomic_compare_exchange_strong_explicit(
            &slot->state,
            &expected,
            VEMB_V16_UB_RPC_PENDING_WRITING,
            memory_order_acq_rel,
            memory_order_relaxed)) {
        return -1;
    }
    slot->resp = *resp;
    atomic_store_explicit(&slot->state,
                          VEMB_V16_UB_RPC_PENDING_READY,
                          memory_order_release);
    return 0;
}

static int pending_wait(vemb_v16_ub_rpc_t *rpc,
                        vemb_v16_ub_rpc_peer_state_t *peer,
                        vemb_v16_ub_rpc_pending_t *slot,
                        const vemb_v16_ub_lookup_rpc_req_t *req,
                        vemb_v16_ub_lookup_rpc_resp_t *resp,
                        uint64_t deadline_ns) {
    (void)peer;
    while (atomic_load_explicit(&rpc->running, memory_order_acquire)) {
        uint32_t state = atomic_load_explicit(&slot->state,
                                              memory_order_acquire);
        if (state == VEMB_V16_UB_RPC_PENDING_READY) {
            *resp = slot->resp;
            atomic_store_explicit(&slot->state,
                                  VEMB_V16_UB_RPC_PENDING_EMPTY,
                                  memory_order_release);
            return 0;
        }
        if (rpc_now_ns() >= deadline_ns) {
            uint32_t expected = VEMB_V16_UB_RPC_PENDING_WAITING;
            if (atomic_compare_exchange_strong_explicit(
                    &slot->state,
                    &expected,
                    VEMB_V16_UB_RPC_PENDING_EMPTY,
                    memory_order_acq_rel,
                    memory_order_relaxed)) {
                resp->status = VEMB_V16_UB_LOOKUP_RPC_TIMEOUT;
                log_limited(&rpc->timeout_logs,
                            LL_NOTICE,
                            "vemb_v16 ub rpc response timeout: owner=%u request_id=%llu key_hash=%llu path=%s errno=%d error=%s",
                            req->dst_owner_id,
                            req->request_id,
                            req->key_hash,
                            NULL,
                            ETIMEDOUT);
                return -1;
            }
        }
        tiny_pause();
    }
    resp->status = VEMB_V16_UB_LOOKUP_RPC_ERROR;
    return -1;
}

static int publish_with_deadline(vemb_v16_ub_rpc_t *rpc,
                                 vemb_v16_ub_rpc_ring_t *ring,
                                 const void *slot,
                                 uint64_t deadline_ns,
                                 uint32_t owner_id,
                                 uint64_t request_id,
                                 uint64_t key_hash,
                                 uint32_t *status) {
    int saw_full = 0;
    while (atomic_load_explicit(&rpc->running, memory_order_acquire)) {
        if (!rpc_ring_ready(ring)) {
            if (rpc_now_ns() >= deadline_ns) {
                if (status)
                    *status = VEMB_V16_UB_LOOKUP_RPC_TIMEOUT;
                log_limited(&rpc->timeout_logs,
                            LL_NOTICE,
                            "vemb_v16 ub rpc ring not ready: owner=%u request_id=%llu key_hash=%llu path=%s errno=%d error=%s",
                            owner_id,
                            request_id,
                            key_hash,
                            ring ? ring->config.path : NULL,
                            ETIMEDOUT);
                return -1;
            }
            tiny_pause();
            continue;
        }

        int rc = rpc_ring_publish(ring->ring, slot);
        if (rc == 0)
            return 0;
        saw_full = 1;
        if (rpc_now_ns() >= deadline_ns) {
            if (status)
                *status = VEMB_V16_UB_LOOKUP_RPC_BUSY;
            log_limited(&rpc->ring_full_logs,
                        LL_NOTICE,
                        "vemb_v16 ub rpc ring full: owner=%u request_id=%llu key_hash=%llu path=%s errno=%d error=%s",
                        owner_id,
                        request_id,
                        key_hash,
                        ring->config.path,
                        EAGAIN);
            return -1;
        }
        tiny_pause();
    }

    if (status)
        *status = saw_full ? VEMB_V16_UB_LOOKUP_RPC_BUSY :
            VEMB_V16_UB_LOOKUP_RPC_ERROR;
    return -1;
}

int vemb_v16_ub_rpc_lookup(void *arg,
                           const vemb_v16_ub_lookup_rpc_req_t *req,
                           vemb_v16_ub_lookup_rpc_resp_t *resp) {
    vemb_v16_ub_rpc_t *rpc = arg;
    if (!rpc || !req || !resp)
        return -1;
    memset(resp, 0, sizeof(*resp));
    resp->request_id = req->request_id;
    resp->key_hash = req->key_hash;

    if (req->dst_owner_id == rpc->local_owner_id)
        return vemb_v16_tlc_lookup_rpc_local_handler(rpc->tlc, req, resp);

    vemb_v16_ub_rpc_peer_state_t *peer = find_peer(rpc, req->dst_owner_id);
    if (!peer) {
        resp->status = VEMB_V16_UB_LOOKUP_RPC_ERROR;
        log_limited(&rpc->error_logs,
                    LL_WARNING,
                    "vemb_v16 ub rpc peer missing: owner=%u request_id=%llu key_hash=%llu path=%s errno=%d error=%s",
                    req->dst_owner_id,
                    req->request_id,
                    req->key_hash,
                    NULL,
                    ENOENT);
        return 0;
    }

    vemb_v16_ub_rpc_pending_t *pending =
        pending_claim(peer, req->request_id);
    if (!pending) {
        resp->status = VEMB_V16_UB_LOOKUP_RPC_BUSY;
        log_limited(&rpc->ring_full_logs,
                    LL_NOTICE,
                    "vemb_v16 ub rpc pending full: owner=%u request_id=%llu key_hash=%llu path=%s errno=%d error=%s",
                    req->dst_owner_id,
                    req->request_id,
                    req->key_hash,
                    NULL,
                    EAGAIN);
        return 0;
    }

    vemb_v16_ub_rpc_wire_req_t wire_req = {
        .magic = VEMB_V16_UB_RPC_MAGIC,
        .version = VEMB_V16_UB_RPC_VERSION,
        .req = *req,
    };
    uint64_t timeout_ns = timeout_from_req_ns(rpc, req);
    uint64_t deadline_ns = rpc_now_ns() + timeout_ns;
    uint32_t status = VEMB_V16_UB_LOOKUP_RPC_OK;
    int rc = publish_with_deadline(rpc,
                                   &peer->request,
                                   &wire_req,
                                   deadline_ns,
                                   req->dst_owner_id,
                                   req->request_id,
                                   req->key_hash,
                                   &status);
    if (rc != 0) {
        pending_release_waiting(pending);
        resp->status = status;
        return 0;
    }

    (void)pending_wait(rpc, peer, pending, req, resp, deadline_ns);
    return 0;
}

static void process_response(vemb_v16_ub_rpc_t *rpc,
                             vemb_v16_ub_rpc_peer_state_t *peer,
                             const vemb_v16_ub_rpc_wire_resp_t *wire_resp) {
    if (wire_resp->magic != VEMB_V16_UB_RPC_MAGIC ||
        wire_resp->version != VEMB_V16_UB_RPC_VERSION) {
        log_limited(&rpc->error_logs,
                    LL_WARNING,
                    "vemb_v16 ub rpc invalid response frame: owner=%u request_id=%llu key_hash=%llu path=%s errno=%d error=%s",
                    peer->owner_id,
                    wire_resp->resp.request_id,
                    wire_resp->resp.key_hash,
                    peer->response.config.path,
                    EPROTO);
        return;
    }
    if (pending_complete(peer, &wire_resp->resp) != 0) {
        log_limited(&rpc->error_logs,
                    LL_NOTICE,
                    "vemb_v16 ub rpc unmatched response dropped: owner=%u request_id=%llu key_hash=%llu path=%s errno=%d error=%s",
                    peer->owner_id,
                    wire_resp->resp.request_id,
                    wire_resp->resp.key_hash,
                    peer->response.config.path,
                    EAGAIN);
    }
}

static void process_request(vemb_v16_ub_rpc_t *rpc,
                            vemb_v16_ub_rpc_peer_state_t *peer,
                            const vemb_v16_ub_rpc_wire_req_t *wire_req) {
    vemb_v16_ub_rpc_wire_resp_t wire_resp;
    memset(&wire_resp, 0, sizeof(wire_resp));
    wire_resp.magic = VEMB_V16_UB_RPC_MAGIC;
    wire_resp.version = VEMB_V16_UB_RPC_VERSION;
    wire_resp.resp.request_id = wire_req->req.request_id;
    wire_resp.resp.key_hash = wire_req->req.key_hash;

    if (wire_req->magic != VEMB_V16_UB_RPC_MAGIC ||
        wire_req->version != VEMB_V16_UB_RPC_VERSION ||
        wire_req->req.dst_owner_id != rpc->local_owner_id ||
        wire_req->req.src_owner_id != peer->owner_id) {
        wire_resp.resp.status = VEMB_V16_UB_LOOKUP_RPC_ERROR;
        wire_resp.resp.kind = VEMB_V16_UB_LOOKUP_RPC_KIND_NONE;
    } else if (vemb_v16_tlc_lookup_rpc_local_handler(rpc->tlc,
                                                     &wire_req->req,
                                                     &wire_resp.resp) != 0) {
        wire_resp.resp.status = VEMB_V16_UB_LOOKUP_RPC_ERROR;
        wire_resp.resp.kind = VEMB_V16_UB_LOOKUP_RPC_KIND_NONE;
    }

    uint64_t timeout_ns = timeout_from_req_ns(rpc, &wire_req->req);
    uint64_t deadline_ns = rpc_now_ns() + timeout_ns;
    uint32_t status = VEMB_V16_UB_LOOKUP_RPC_OK;
    (void)publish_with_deadline(rpc,
                                &peer->outbound_response,
                                &wire_resp,
                                deadline_ns,
                                peer->owner_id,
                                wire_req->req.request_id,
                                wire_req->req.key_hash,
                                &status);
}

static void *listener_main(void *arg) {
    vemb_v16_ub_rpc_t *rpc = arg;
    serverLog(LL_NOTICE,
              "vemb_v16 ub rpc listener started: owner=%u peers=%u",
              rpc->local_owner_id,
              rpc->peer_count);
    while (atomic_load_explicit(&rpc->running, memory_order_acquire)) {
        uint32_t handled = 0;
        for (uint32_t i = 0; i < rpc->peer_count; i++) {
            vemb_v16_ub_rpc_peer_state_t *peer = &rpc->peers[i];
            if (rpc_ring_ready(&peer->inbound_request)) {
                for (uint32_t n = 0; n < VEMB_V16_UB_RPC_LISTENER_BATCH; n++) {
                    vemb_v16_ub_rpc_wire_req_t wire_req;
                    int got = rpc_ring_poll(peer->inbound_request.ring,
                                            &wire_req);
                    if (got <= 0)
                        break;
                    process_request(rpc, peer, &wire_req);
                    handled++;
                }
            }
            if (rpc_ring_ready(&peer->response)) {
                for (uint32_t n = 0; n < VEMB_V16_UB_RPC_LISTENER_BATCH; n++) {
                    vemb_v16_ub_rpc_wire_resp_t wire_resp;
                    int got = rpc_ring_poll(peer->response.ring,
                                            &wire_resp);
                    if (got <= 0)
                        break;
                    process_response(rpc, peer, &wire_resp);
                    handled++;
                }
            }
        }
        if (handled == 0)
            tiny_pause();
    }
    serverLog(LL_NOTICE,
              "vemb_v16 ub rpc listener stopped: owner=%u",
              rpc->local_owner_id);
    return NULL;
}

static int peer_open(vemb_v16_ub_rpc_peer_state_t *dst,
                     const vemb_v16_ub_rpc_peer_t *src) {
    memset(dst, 0, sizeof(*dst));
    dst->request.mapping.fd = -1;
    dst->response.mapping.fd = -1;
    dst->inbound_request.mapping.fd = -1;
    dst->outbound_response.mapping.fd = -1;
    dst->owner_id = src->owner_id;
    for (uint32_t i = 0; i < VEMB_V16_UB_RPC_PENDING_SIZE; i++) {
        atomic_init(&dst->pending[i].state,
                    VEMB_V16_UB_RPC_PENDING_EMPTY);
        atomic_init(&dst->pending[i].request_id, 0);
    }

    if (ring_open(&dst->request,
                  &src->request,
                  sizeof(vemb_v16_ub_rpc_wire_req_t),
                  0) != 0 ||
        ring_open(&dst->response,
                  &src->response,
                  sizeof(vemb_v16_ub_rpc_wire_resp_t),
                  1) != 0 ||
        ring_open(&dst->inbound_request,
                  &src->inbound_request,
                  sizeof(vemb_v16_ub_rpc_wire_req_t),
                  1) != 0 ||
        ring_open(&dst->outbound_response,
                  &src->outbound_response,
                  sizeof(vemb_v16_ub_rpc_wire_resp_t),
                  0) != 0) {
        return -1;
    }
    return 0;
}

static void peer_close(vemb_v16_ub_rpc_peer_state_t *peer) {
    if (!peer)
        return;
    ring_close(&peer->request);
    ring_close(&peer->response);
    ring_close(&peer->inbound_request);
    ring_close(&peer->outbound_response);
}

int vemb_v16_ub_rpc_create(vemb_v16_ub_rpc_t **out,
                           vemb_v16_tlc_t *tlc,
                           uint32_t local_owner_id,
                           uint32_t timeout_ms,
                           const vemb_v16_ub_rpc_peer_t *peers,
                           uint32_t peer_count) {
    if (!out || !tlc || peer_count > VEMB_V16_UB_RPC_MAX_PEERS ||
        (peer_count && !peers))
        return -1;
    *out = NULL;
    if (peer_count == 0)
        return 0;

    vemb_v16_ub_rpc_t *rpc = zcalloc(sizeof(*rpc));
    if (!rpc)
        return -1;
    rpc->tlc = tlc;
    rpc->local_owner_id = local_owner_id;
    rpc->timeout_ms = timeout_ms ? timeout_ms :
        VEMB_V16_UB_RPC_DEFAULT_TIMEOUT_MS;
    rpc->peer_count = 0;
    atomic_init(&rpc->running, 0);
    atomic_init(&rpc->timeout_logs, 0);
    atomic_init(&rpc->ring_full_logs, 0);
    atomic_init(&rpc->error_logs, 0);

    for (uint32_t i = 0; i < peer_count; i++) {
        if (peer_open(&rpc->peers[rpc->peer_count], &peers[i]) != 0) {
            serverLog(LL_WARNING,
                      "failed to open vemb_v16 ub rpc peer rings: local_owner=%u peer_owner=%u",
                      local_owner_id,
                      peers[i].owner_id);
            vemb_v16_ub_rpc_destroy(rpc);
            return -1;
        }
        rpc->peer_count++;
    }

    atomic_store_explicit(&rpc->running, 1, memory_order_release);
    if (pthread_create(&rpc->thread, NULL, listener_main, rpc) != 0) {
        atomic_store_explicit(&rpc->running, 0, memory_order_release);
        vemb_v16_ub_rpc_destroy(rpc);
        return -1;
    }
    rpc->thread_started = 1;
    vemb_v16_tlc_set_lookup_rpc(tlc, vemb_v16_ub_rpc_lookup, rpc);
    *out = rpc;
    return 0;
}

void vemb_v16_ub_rpc_destroy(vemb_v16_ub_rpc_t *rpc) {
    if (!rpc)
        return;
    atomic_store_explicit(&rpc->running, 0, memory_order_release);
    if (rpc->thread_started)
        pthread_join(rpc->thread, NULL);
    if (rpc->tlc)
        vemb_v16_tlc_set_lookup_rpc(rpc->tlc, NULL, NULL);
    for (uint32_t i = 0; i < rpc->peer_count; i++)
        peer_close(&rpc->peers[i]);
    zfree(rpc);
}
