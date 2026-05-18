/*
 * FC-only VEMB proxy.
 *
 * This file intentionally does not use the legacy proxy batching stack:
 * no active bucket heap, no proxy_batch_bucket, no flush scheduler, no
 * flush executor, no completion dict, and no response result thread.
 *
 * Redis command threads publish VEMB requests to a per-worker FC board. The
 * winning combiner writes one pointer-carrying packet to the worker request
 * ring. The supernode worker loads UB vectors, writes the result directly into
 * the blocked request, and calls RedisModule_UnblockClient().
 */

#define REDISMODULE_CORE_MODULE

#include "proxy_aggregator.h"
#include "batch_latency_trace.h"
#include "macro.h"
#include "monotonic.h"
#include "ring_buffer_mgr.h"
#include "server.h"
#include "supernode_protocol.h"
#include "ub_metadata.h"

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define PROXY_FC_MIN_SLOTS 64
#define PROXY_FC_DEFAULT_SLOTS 256
#define PROXY_FC_DEFAULT_BATCH_LIMIT 32

typedef enum proxyFcSlotState {
    PROXY_FC_SLOT_EMPTY = 0,
    PROXY_FC_SLOT_WRITING = 1,
    PROXY_FC_SLOT_PENDING = 2,
    PROXY_FC_SLOT_CLAIMED = 3,
} proxyFcSlotState;

typedef struct proxy_fc_slot {
    _Alignas(64) atomic_int state;
    uint64_t row_id;
    uint64_t publish_time_us;
    proxy_vector_request_t *owner;
} proxy_fc_slot_t;

typedef struct proxy_fc_board {
    proxy_fc_slot_t *slots;
    proxy_vector_request_t **scratch_owners;
    size_t *scratch_indexes;
    ring_buffer_t *request_ring;
    size_t slot_count;
    size_t scratch_capacity;
    size_t scan_cursor;
    int supernode_id;
    int worker_id;
    atomic_int combiner_lock;
    atomic_size_t pending_count;
} proxy_fc_board_t;

typedef struct proxy_aggregator {
    proxy_fc_board_t *boards;
    size_t num_boards;
    size_t workers_per_node;
    size_t active_workers;
    size_t batch_limit;
    size_t fc_slots;
    size_t fc_max_scan;
    uint64_t time_limit_us;
    atomic_uint_fast64_t next_batch_id;

    atomic_uint_fast64_t total_requests;
    atomic_uint_fast64_t published;
    atomic_uint_fast64_t combine_rounds;
    atomic_uint_fast64_t combined_requests;
    atomic_uint_fast64_t direct_rounds;
    atomic_uint_fast64_t batch_rounds;
    atomic_uint_fast64_t slot_busy;
    atomic_uint_fast64_t ring_busy;
    atomic_uint_fast64_t submit_failures;
    atomic_uint_fast64_t pending_max;
} proxy_aggregator_t;

static proxy_aggregator_t *proxy = NULL;
static atomic_uint_fast64_t next_request_id = 1;

static size_t proxy_fc_effective_slots(void) {
    size_t slots = server.proxy.vemb_fc_slots > 0 ?
        server.proxy.vemb_fc_slots : PROXY_FC_DEFAULT_SLOTS;
    if (slots < PROXY_FC_MIN_SLOTS) slots = PROXY_FC_MIN_SLOTS;
    return slots;
}

static size_t proxy_fc_effective_batch_limit(size_t slots) {
    size_t limit = server.proxy.batch_limit > 0 ?
        server.proxy.batch_limit : PROXY_FC_DEFAULT_BATCH_LIMIT;
    if (limit == 0) limit = 1;
    if (limit > slots) limit = slots;
    return limit;
}

static uint64_t proxy_fc_effective_time_limit_us(void) {
    return server.proxy.time_limit_us > 0 ?
        server.proxy.time_limit_us : PROXY_TIME_LIMIT_US;
}

static inline size_t proxy_fc_slot_index(uint64_t request_id, size_t slot_count) {
    uint64_t x = request_id;
    x ^= x >> 33;
    x *= UINT64_C(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x *= UINT64_C(0xc4ceb9fe1a85ec53);
    x ^= x >> 33;
    return (size_t)(x % slot_count);
}

static inline size_t proxy_fc_worker_for_row(uint64_t row_id) {
    return proxy && proxy->active_workers > 0 ?
        (size_t)(row_id % proxy->active_workers) : 0;
}

static int proxy_fc_board_init(proxy_fc_board_t *board,
                               size_t slot_count,
                               size_t batch_limit,
                               int supernode_id,
                               int worker_id,
                               ring_buffer_t *request_ring) {
    RETURN_IF(!board || slot_count == 0 || batch_limit == 0 || !request_ring, C_ERR);

    board->slots = zcalloc(sizeof(*board->slots) * slot_count);
    RETURN_IF(!board->slots, C_ERR);
    board->scratch_owners = zmalloc(sizeof(*board->scratch_owners) * batch_limit);
    if (!board->scratch_owners) {
        zfree(board->slots);
        memset(board, 0, sizeof(*board));
        return C_ERR;
    }
    board->scratch_indexes = zmalloc(sizeof(*board->scratch_indexes) * batch_limit);
    if (!board->scratch_indexes) {
        zfree(board->scratch_owners);
        zfree(board->slots);
        memset(board, 0, sizeof(*board));
        return C_ERR;
    }

    board->request_ring = request_ring;
    board->slot_count = slot_count;
    board->scratch_capacity = batch_limit;
    board->supernode_id = supernode_id;
    board->worker_id = worker_id;
    atomic_init(&board->combiner_lock, 0);
    atomic_init(&board->pending_count, 0);
    for (size_t i = 0; i < slot_count; i++)
        atomic_init(&board->slots[i].state, PROXY_FC_SLOT_EMPTY);
    return C_OK;
}

static void proxy_fc_board_cleanup(proxy_fc_board_t *board) {
    RETURN_IF(!board);
    zfree(board->scratch_indexes);
    zfree(board->scratch_owners);
    zfree(board->slots);
    memset(board, 0, sizeof(*board));
}

static int proxy_fc_claim_slot(proxy_fc_board_t *board,
                               size_t slot_index,
                               proxy_vector_request_t **owners,
                               size_t *indexes,
                               size_t *count,
                               size_t max_count) {
    RETURN_IF(!board || !owners || !indexes || !count || *count >= max_count, 0);
    RETURN_IF(slot_index >= board->slot_count, 0);

    proxy_fc_slot_t *slot = &board->slots[slot_index];
    int state = PROXY_FC_SLOT_PENDING;
    if (!atomic_compare_exchange_strong_explicit(&slot->state, &state,
                                                 PROXY_FC_SLOT_CLAIMED,
                                                 memory_order_acq_rel,
                                                 memory_order_relaxed)) {
        return 0;
    }
    owners[*count] = slot->owner;
    indexes[*count] = slot_index;
    (*count)++;
    return 1;
}

static int proxy_fc_publish_packet(proxy_fc_board_t *board,
                                   proxy_vector_request_t **owners,
                                   size_t count,
                                   uint64_t now_us) {
    RETURN_IF(!proxy || !board || !owners || count == 0, C_ERR);
    RETURN_IF(count > UINT32_MAX, C_ERR);
    RETURN_IF(count > (SIZE_MAX - sizeof(fc_vemb_packet_t)) /
              sizeof(((fc_vemb_packet_t *)0)->requests[0]), C_ERR);

    size_t packet_size = sizeof(fc_vemb_packet_t) +
                         count * sizeof(((fc_vemb_packet_t *)0)->requests[0]);
    RETURN_IF(packet_size > UINT32_MAX, C_ERR);

    uint64_t wait_sum_us = 0;
    uint64_t wait_max_us = 0;
    for (size_t i = 0; i < count; i++) {
        proxy_vector_request_t *owner = owners[i];
        RETURN_IF(!owner || owner->op_type != PROXY_VECTOR_OP_VEMB, C_ERR);
        uint64_t wait_us = owner->submit_time_us > 0 ?
            (uint64_t)(now_us - owner->submit_time_us) : 0;
        wait_sum_us += wait_us;
        if (wait_us > wait_max_us) wait_max_us = wait_us;
    }

    fc_vemb_packet_t *packet = NULL;
    if (ring_buffer_reserve(board->request_ring, packet_size, (void **)&packet) != C_OK)
        return C_ERR;

    uint64_t batch_id =
        atomic_fetch_add_explicit(&proxy->next_batch_id, 1, memory_order_relaxed) + 1;
    packet->hdr.magic = BATCH_PACKET_MAGIC;
    packet->hdr.packet_size = (uint32_t)packet_size;
    packet->hdr.num_requests = (uint32_t)count;
    packet->hdr.op_type = BATCH_PACKET_OP_VEMB | BATCH_PACKET_FLAG_FC_POINTERS;
    packet->hdr.supernode_id = (uint32_t)board->supernode_id;
    packet->hdr.worker_id = (uint32_t)board->worker_id;
    packet->hdr.timestamp_us = now_us;
    packet->hdr.batch_id = batch_id;

    for (size_t i = 0; i < count; i++) {
        proxy_vector_request_t *owner = owners[i];
        owner->batch_id = batch_id;
        packet->requests[i].row_id = owner->row_id;
        packet->requests[i].owner = owner;
    }

    if (ring_buffer_commit_write(board->request_ring, packet_size) != C_OK) {
        ring_buffer_cancel_write(board->request_ring);
        for (size_t i = 0; i < count; i++) owners[i]->batch_id = 0;
        return C_ERR;
    }

    batch_latency_trace_proxy_meta_t proxy_meta = {
        .proxy_path = count == 1 ? BATCH_TRACE_PROXY_PATH_DIRECT :
                                   BATCH_TRACE_PROXY_PATH_BATCH,
        .flush_reason = count == 1 ? BATCH_TRACE_FLUSH_REASON_DIRECT :
                                     BATCH_TRACE_FLUSH_REASON_FULL,
        .flush_trigger = BATCH_TRACE_FLUSH_TRIGGER_DIRECT,
        .bucket_depth = (uint32_t)count,
        .bucket_gap_us = 0,
        .bucket_ewma_gap_us = 0,
    };
    (void)batch_latency_trace_begin(batch_id,
                                    BATCH_PACKET_OP_VEMB,
                                    (uint32_t)count,
                                    wait_sum_us,
                                    wait_max_us,
                                    elapsedUs(now_us),
                                    &proxy_meta);
    return C_OK;
}

static int proxy_fc_try_combine(proxy_fc_board_t *board,
                                size_t preferred_slot,
                                uint64_t now_us,
                                int force) {
    RETURN_IF(!proxy || !board || !board->slots || !board->request_ring, C_ERR);

    size_t pending_snapshot =
        atomic_load_explicit(&board->pending_count, memory_order_acquire);
    if (pending_snapshot == 0)
        return C_OK;
    if (!force && pending_snapshot < proxy->batch_limit) {
        uint64_t oldest_us = UINT64_MAX;
        for (size_t i = 0; i < board->slot_count; i++) {
            int state = atomic_load_explicit(&board->slots[i].state,
                                             memory_order_acquire);
            if (state != PROXY_FC_SLOT_PENDING) continue;
            uint64_t publish_us = board->slots[i].publish_time_us;
            if (publish_us > 0 && publish_us < oldest_us) oldest_us = publish_us;
        }
        if (oldest_us == UINT64_MAX ||
            now_us < oldest_us ||
            now_us - oldest_us < proxy->time_limit_us) {
            return C_OK;
        }
    }

    if (atomic_load_explicit(&board->combiner_lock, memory_order_relaxed) != 0)
        return C_OK;

    int expected = 0;
    if (!atomic_compare_exchange_strong_explicit(&board->combiner_lock, &expected, 1,
                                                 memory_order_acq_rel,
                                                 memory_order_relaxed)) {
        return C_OK;
    }

    proxy_vector_request_t **owners = board->scratch_owners;
    size_t *indexes = board->scratch_indexes;
    size_t count = 0;
    size_t max_count = board->scratch_capacity;
    size_t scan_limit = board->slot_count;
    if (proxy->fc_max_scan > 0 && scan_limit > proxy->fc_max_scan)
        scan_limit = proxy->fc_max_scan;
    if (scan_limit == 0) scan_limit = 1;
    if (max_count > scan_limit) max_count = scan_limit;

    if (preferred_slot < board->slot_count) {
        (void)proxy_fc_claim_slot(board, preferred_slot, owners, indexes,
                                  &count, max_count);
    }

    if (pending_snapshot > count && count < max_count) {
        size_t start = board->scan_cursor % board->slot_count;
        for (size_t scanned = 0; scanned < scan_limit && count < max_count; scanned++) {
            size_t idx = (start + scanned) % board->slot_count;
            if (idx == preferred_slot) continue;
            (void)proxy_fc_claim_slot(board, idx, owners, indexes, &count, max_count);
        }
        board->scan_cursor = (start + scan_limit) % board->slot_count;
    }

    int ret = C_OK;
    if (count > 0) {
        ret = proxy_fc_publish_packet(board, owners, count, now_us);
        if (ret == C_OK) {
            atomic_fetch_add_explicit(&proxy->combine_rounds, 1, memory_order_relaxed);
            atomic_fetch_add_explicit(&proxy->combined_requests, count,
                                      memory_order_relaxed);
            if (count == 1)
                atomic_fetch_add_explicit(&proxy->direct_rounds, 1, memory_order_relaxed);
            else
                atomic_fetch_add_explicit(&proxy->batch_rounds, 1, memory_order_relaxed);
        } else {
            atomic_fetch_add_explicit(&proxy->ring_busy, 1, memory_order_relaxed);
        }
    }

    for (size_t i = 0; i < count; i++) {
        proxy_fc_slot_t *slot = &board->slots[indexes[i]];
        if (ret == C_OK) {
            slot->owner = NULL;
            slot->row_id = 0;
            slot->publish_time_us = 0;
            atomic_fetch_sub_explicit(&board->pending_count, 1, memory_order_acq_rel);
            atomic_store_explicit(&slot->state, PROXY_FC_SLOT_EMPTY,
                                  memory_order_release);
        } else {
            atomic_store_explicit(&slot->state, PROXY_FC_SLOT_PENDING,
                                  memory_order_release);
        }
    }

    atomic_store_explicit(&board->combiner_lock, 0, memory_order_release);
    return ret;
}

static int proxy_fc_submit_vemb(proxy_vector_request_t *owner) {
    RETURN_IF(!proxy || !owner || owner->op_type != PROXY_VECTOR_OP_VEMB, C_ERR);

    size_t worker = proxy_fc_worker_for_row(owner->row_id);
    RETURN_IF(worker >= proxy->num_boards, C_ERR);
    proxy_fc_board_t *board = &proxy->boards[worker];

    size_t start = proxy_fc_slot_index(owner->request_id, board->slot_count);
    proxy_fc_slot_t *slot = NULL;
    for (size_t i = 0; i < board->slot_count; i++) {
        size_t idx = (start + i) % board->slot_count;
        proxy_fc_slot_t *candidate = &board->slots[idx];
        int empty = PROXY_FC_SLOT_EMPTY;
        if (atomic_compare_exchange_strong_explicit(&candidate->state, &empty,
                                                    PROXY_FC_SLOT_WRITING,
                                                    memory_order_acq_rel,
                                                    memory_order_relaxed)) {
            slot = candidate;
            break;
        }
    }
    if (!slot) {
        atomic_fetch_add_explicit(&proxy->slot_busy, 1, memory_order_relaxed);
        return C_ERR;
    }

    owner->submit_time_us = getMonotonicUs();
    slot->row_id = owner->row_id;
    slot->publish_time_us = owner->submit_time_us;
    slot->owner = owner;
    atomic_store_explicit(&slot->state, PROXY_FC_SLOT_PENDING,
                          memory_order_release);
    size_t pending = atomic_fetch_add_explicit(&board->pending_count, 1,
                                               memory_order_acq_rel) + 1;
    atomic_fetch_add_explicit(&proxy->published, 1, memory_order_relaxed);
    uint64_t old_max = atomic_load_explicit(&proxy->pending_max, memory_order_relaxed);
    while (pending > old_max &&
           !atomic_compare_exchange_weak_explicit(&proxy->pending_max, &old_max, pending,
                                                  memory_order_relaxed,
                                                  memory_order_relaxed)) {
    }

    return C_OK;
}

int proxy_aggregator_drain_worker(int worker_id, uint64_t now_us) {
    RETURN_IF(!proxy || worker_id < 0, C_ERR);
    RETURN_IF((size_t)worker_id >= proxy->active_workers, C_OK);
    proxy_fc_board_t *board = &proxy->boards[worker_id];
    return proxy_fc_try_combine(board, board->slot_count, now_us, 0);
}

int proxy_aggregator_init(int num_supernodes) {
    if (proxy) return C_OK;
    if (num_supernodes <= 0) num_supernodes = 1;

    proxy = zcalloc(sizeof(*proxy));
    RETURN_IF(!proxy, C_ERR);
    proxy->workers_per_node = server.supernode_workers > 0 ?
        (size_t)server.supernode_workers : (size_t)max((int)sysconf(_SC_NPROCESSORS_ONLN), 1);
    proxy->active_workers = server.proxy.vemb_fc_workers > 0 ?
        server.proxy.vemb_fc_workers : proxy->workers_per_node;
    if (proxy->active_workers > proxy->workers_per_node)
        proxy->active_workers = proxy->workers_per_node;
    if (proxy->active_workers == 0) proxy->active_workers = 1;
    proxy->fc_slots = proxy_fc_effective_slots();
    proxy->batch_limit = proxy_fc_effective_batch_limit(proxy->fc_slots);
    proxy->fc_max_scan = server.proxy.vemb_fc_max_scan;
    proxy->time_limit_us = proxy_fc_effective_time_limit_us();
    proxy->num_boards = proxy->workers_per_node;

    if (ring_buffer_mgr_init(proxy->workers_per_node, RING_BUFFER_SIZE) != C_OK)
        goto failed;
    if (ring_buffer_mgr_ensure_supernodes((size_t)num_supernodes) != C_OK)
        goto failed;
    if (batch_latency_trace_init() != C_OK)
        goto failed;

    proxy->boards = zcalloc(sizeof(*proxy->boards) * proxy->num_boards);
    if (!proxy->boards) goto failed;
    for (size_t worker = 0; worker < proxy->num_boards; worker++) {
        ring_buffer_t *request_ring = ring_buffer_mgr_get_request(0, (int)worker);
        if (!request_ring) goto failed;
        if (proxy_fc_board_init(&proxy->boards[worker],
                                proxy->fc_slots,
                                proxy->batch_limit,
                                0,
                                (int)worker,
                                request_ring) != C_OK) {
            goto failed;
        }
    }

    atomic_init(&proxy->total_requests, 0);
    atomic_init(&proxy->published, 0);
    atomic_init(&proxy->combine_rounds, 0);
    atomic_init(&proxy->combined_requests, 0);
    atomic_init(&proxy->direct_rounds, 0);
    atomic_init(&proxy->batch_rounds, 0);
    atomic_init(&proxy->slot_busy, 0);
    atomic_init(&proxy->ring_busy, 0);
    atomic_init(&proxy->submit_failures, 0);
    atomic_init(&proxy->pending_max, 0);
    atomic_init(&proxy->next_batch_id, 0);

    serverLog(LL_NOTICE,
              "FC-only VEMB proxy initialized: workers=%zu active_workers=%zu slots=%zu batch_limit=%zu time_limit_us=%llu",
              proxy->workers_per_node,
              proxy->active_workers,
              proxy->fc_slots,
              proxy->batch_limit,
              (unsigned long long)proxy->time_limit_us);
    return C_OK;

failed:
    proxy_aggregator_shutdown();
    return C_ERR;
}

void proxy_aggregator_shutdown(void) {
    if (!proxy) return;
    if (proxy->boards) {
        for (size_t i = 0; i < proxy->num_boards; i++)
            proxy_fc_board_cleanup(&proxy->boards[i]);
        zfree(proxy->boards);
    }
    batch_latency_trace_cleanup();
    ring_buffer_mgr_shutdown();
    zfree(proxy);
    proxy = NULL;
    serverLog(LL_NOTICE, "FC-only VEMB proxy shutdown");
}

int proxy_enqueue_request(const char *key, void *client_ctx,
                          float *result_buffer, size_t vector_dim) {
    UNUSED(key);
    UNUSED(client_ctx);
    UNUSED(result_buffer);
    UNUSED(vector_dim);
    return C_ERR;
}

int proxy_enqueue_vector_request(const char *key, proxy_vector_request_t *req) {
    UNUSED(key);
    RETURN_IF(!req, C_ERR);
    if (req->op_type != PROXY_VECTOR_OP_VEMB) return C_ERR;
    return proxy_fc_submit_vemb(req);
}

static int proxy_vemb_reply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    proxy_vector_request_t *req =
        (proxy_vector_request_t *)RedisModule_GetBlockedClientPrivateData(ctx);
    if (!req) return RedisModule_ReplyWithError(ctx, "ERR missing VEMB proxy request");

    if (req->error_code != C_OK) {
        proxy_vector_request_free(req);
        return RedisModule_ReplyWithError(ctx, "ERR UB engine vemb failed");
    }
    if (!req->result_vector || req->result_dim == 0) {
        proxy_vector_request_free(req);
        return RedisModule_ReplyWithNull(ctx);
    }

    if (req->raw_output) {
        RedisModule_ReplyWithArray(ctx, 3);
        RedisModule_ReplyWithSimpleString(ctx, "fp32");
        RedisModule_ReplyWithStringBuffer(ctx,
            (const char *)req->result_vector,
            req->result_dim * sizeof(float));
        RedisModule_ReplyWithDouble(ctx, 1.0);
    } else {
        RedisModule_ReplyWithArray(ctx, req->result_dim);
        for (size_t i = 0; i < req->result_dim; i++)
            RedisModule_ReplyWithDouble(ctx, req->result_vector[i]);
    }

    RedisModule_BlockedClientMeasureTimeEnd(RedisModule_GetBlockedClientHandle(ctx));
    proxy_vector_request_free(req);
    return REDISMODULE_OK;
}

int proxy_submit_vemb(RedisModuleCtx *ctx, void *key, void *element, int raw_output) {
    RETURN_IF(!ctx || !key || !element, REDISMODULE_ERR);
    RETURN_IF(!proxy, RedisModule_ReplyWithError(ctx, "ERR FC proxy not initialized"));

    RedisModuleString *key_obj = key;
    RedisModuleString *element_obj = element;
    size_t key_len = 0;
    size_t element_len = 0;
    const char *key_cstr = RedisModule_StringPtrLen(key_obj, &key_len);
    const char *element_cstr = RedisModule_StringPtrLen(element_obj, &element_len);
    if (!key_cstr || !element_cstr)
        return RedisModule_ReplyWithError(ctx, "ERR invalid key or element");

    sds key_tmp = sdsnewlen(key_cstr, key_len);
    sds element_tmp = sdsnewlen(element_cstr, element_len);
    if (!key_tmp || !element_tmp) {
        sdsfree(key_tmp);
        sdsfree(element_tmp);
        return RedisModule_ReplyWithError(ctx, "ERR oom");
    }

    ub_vector_set_meta_t *set = ub_metadata_get_set(key_tmp);
    uint64_t row_id = 0;
    if (!set || ub_metadata_lookup_row(set, element_tmp, &row_id) != C_OK) {
        sdsfree(key_tmp);
        sdsfree(element_tmp);
        return RedisModule_ReplyWithNull(ctx);
    }

    uint64_t request_id =
        atomic_fetch_add_explicit(&next_request_id, 1, memory_order_relaxed);
    RedisModuleBlockedClient *bc =
        RedisModule_BlockClient(ctx, proxy_vemb_reply, NULL, NULL, 0);
    if (!bc) {
        sdsfree(key_tmp);
        sdsfree(element_tmp);
        return RedisModule_ReplyWithError(ctx, "ERR failed to block client");
    }

    proxy_vector_request_t *req =
        proxy_vector_request_create_vemb(request_id, row_id, raw_output, bc);
    if (!req) {
        RedisModule_AbortBlock(bc);
        sdsfree(key_tmp);
        sdsfree(element_tmp);
        return RedisModule_ReplyWithError(ctx, "ERR oom");
    }

    RedisModule_BlockClientSetPrivateData(bc, req);
    RedisModule_BlockedClientMeasureTimeStart(bc);
    if (proxy_fc_submit_vemb(req) != C_OK) {
        RedisModule_AbortBlock(bc);
        proxy_vector_request_free(req);
        sdsfree(key_tmp);
        sdsfree(element_tmp);
        return RedisModule_ReplyWithError(ctx, "ERR failed to publish VEMB request");
    }

    atomic_fetch_add_explicit(&proxy->total_requests, 1, memory_order_relaxed);
    sdsfree(key_tmp);
    sdsfree(element_tmp);
    return REDISMODULE_OK;
}

int proxy_submit_vsim(RedisModuleCtx *ctx,
                      void *key,
                      float *query_vector,
                      size_t query_dim,
                      size_t requested_count,
                      int withscores) {
    UNUSED(key);
    UNUSED(query_dim);
    UNUSED(requested_count);
    UNUSED(withscores);
    zfree(query_vector);
    return RedisModule_ReplyWithError(ctx, "ERR VSIM proxy path removed in FC-only proxy");
}

sds proxy_aggregator_get_stats(void) {
    sds stats = sdsempty();
    if (!proxy)
        return sdscat(stats, "FC Proxy: Not initialized\n");

    size_t pending_total = 0;
    size_t pending_max_now = 0;
    size_t ring_data_total = 0;
    for (size_t i = 0; i < proxy->num_boards; i++) {
        size_t pending =
            atomic_load_explicit(&proxy->boards[i].pending_count, memory_order_relaxed);
        pending_total += pending;
        if (pending > pending_max_now) pending_max_now = pending;
        ring_data_total += ring_buffer_available_data(proxy->boards[i].request_ring);
    }

    uint64_t combine_rounds =
        atomic_load_explicit(&proxy->combine_rounds, memory_order_relaxed);
    uint64_t combined_requests =
        atomic_load_explicit(&proxy->combined_requests, memory_order_relaxed);

    stats = sdscatprintf(stats, "FC Proxy Stats:\n");
    stats = sdscatprintf(stats, "  Workers: %zu\n", proxy->workers_per_node);
    stats = sdscatprintf(stats, "  Active FC workers: %zu\n", proxy->active_workers);
    stats = sdscatprintf(stats, "  FC slots per worker: %zu\n", proxy->fc_slots);
    stats = sdscatprintf(stats, "  FC batch limit: %zu\n", proxy->batch_limit);
    stats = sdscatprintf(stats, "  FC max scan: %zu\n", proxy->fc_max_scan);
    stats = sdscatprintf(stats, "  FC time limit us: %llu\n",
                         (unsigned long long)proxy->time_limit_us);
    stats = sdscatprintf(stats, "  Total requests: %llu\n",
                         (unsigned long long)atomic_load_explicit(&proxy->total_requests,
                                                                  memory_order_relaxed));
    stats = sdscatprintf(stats, "  Published: %llu\n",
                         (unsigned long long)atomic_load_explicit(&proxy->published,
                                                                  memory_order_relaxed));
    stats = sdscatprintf(stats, "  Combine rounds: %llu\n",
                         (unsigned long long)combine_rounds);
    stats = sdscatprintf(stats, "  Combined requests: %llu\n",
                         (unsigned long long)combined_requests);
    stats = sdscatprintf(stats, "  Direct rounds: %llu\n",
                         (unsigned long long)atomic_load_explicit(&proxy->direct_rounds,
                                                                  memory_order_relaxed));
    stats = sdscatprintf(stats, "  Batch rounds: %llu\n",
                         (unsigned long long)atomic_load_explicit(&proxy->batch_rounds,
                                                                  memory_order_relaxed));
    stats = sdscatprintf(stats, "  Slot busy: %llu\n",
                         (unsigned long long)atomic_load_explicit(&proxy->slot_busy,
                                                                  memory_order_relaxed));
    stats = sdscatprintf(stats, "  Ring busy: %llu\n",
                         (unsigned long long)atomic_load_explicit(&proxy->ring_busy,
                                                                  memory_order_relaxed));
    stats = sdscatprintf(stats, "  Submit failures: %llu\n",
                         (unsigned long long)atomic_load_explicit(&proxy->submit_failures,
                                                                  memory_order_relaxed));
    stats = sdscatprintf(stats, "  Pending total: %zu\n", pending_total);
    stats = sdscatprintf(stats, "  Pending max now: %zu\n", pending_max_now);
    stats = sdscatprintf(stats, "  Pending max observed: %llu\n",
                         (unsigned long long)atomic_load_explicit(&proxy->pending_max,
                                                                  memory_order_relaxed));
    stats = sdscatprintf(stats, "  Request ring bytes: %zu\n", ring_data_total);
    if (combine_rounds > 0) {
        stats = sdscatprintf(stats, "  Average FC batch size: %.2f\n",
                             (double)combined_requests / (double)combine_rounds);
    }

    if (batch_latency_trace_enabled()) {
        sds traces = batch_latency_trace_dump_recent("  Recent FC traces", 16);
        stats = sdscatsds(stats, traces);
        sdsfree(traces);
    } else {
        stats = sdscat(stats, "  Recent FC traces: disabled unless loglevel debug\n");
    }
    return stats;
}
