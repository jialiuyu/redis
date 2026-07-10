#define _GNU_SOURCE

#include "cpu_relax.h"
#include "vemb_v16_supernode.h"
#include "vemb_v16_dataplane.h"
#include "vemb_v16_log.h"
#include "vemb_v16_protocol.h"
#include "vemb_v16_stats.h"
#include "redisassert.h"
#include "sve_similarity.h"
#include "zmalloc.h"
#include "macro.h"
#include "monotonic.h"

#include <stdlib.h>
#include <stdatomic.h>
#include <string.h>
#ifdef __linux__
#include <unistd.h>
#endif

#define VEMB_V16_SAMPLE_MASK 1023u
#define VEMB_V16_SUPERNODE_BATCH 32u
#define VEMB_V16_DIAG_REQ_ID_LIMIT 80u

typedef struct vemb_v16_inline_snapshot {
    uint32_t payload_bytes;
    uint32_t reserved;
    uint8_t payload[];
} vemb_v16_inline_snapshot_t;

static int diag_should_log_req(uint32_t req_id) {
    return req_id != 0 && req_id <= VEMB_V16_DIAG_REQ_ID_LIMIT;
}

static int request_should_sample(uint32_t req_id) {
    return (req_id & VEMB_V16_SAMPLE_MASK) == 0;
}

static monotime timing_start_if_sampled(int sample) {
    monotime start = 0;
    if (sample)
        elapsedStartNs(&start);
    return start;
}

static void timing_acc_add_ns_if_sampled(vemb_v16_timing_acc_t *acc,
                                         int sample,
                                         uint64_t ns) {
    if (sample) vemb_v16_timing_acc_add(acc, ns);
}

static uint64_t timing_acc_add_if_sampled(vemb_v16_timing_acc_t *acc,
                                          int sample,
                                          monotime start) {
    if (!sample) return 0;
    uint64_t ns = 0;
    ns = elapsedNs(start);
    vemb_v16_timing_acc_add(acc, ns);
    return ns;
}

static const char *op_name(uint8_t op) {
    switch (op) {
    case VEMB_V16_OP_VADD:
        return "vadd";
    case VEMB_V16_OP_VREM:
        return "vrem";
    case VEMB_V16_OP_VEMB_HANDLE:
        return "vemb-handle";
    case VEMB_V16_OP_VEMB_INLINE:
        return "vemb-inline";
    case VEMB_V16_OP_VSIM_INLINE:
        return "vsim-inline";
    case VEMB_V16_OP_VSIM_KEY_KEY:
        return "vsim-key1-key2";
    default:
        return "unknown";
    }
}

static int vector_handle_is_local(vemb_v16_tlc_t *tlc,
                                  const vemb_v16_vector_handle_t *handle) {
    const vemb_v16_tlc_warm_region_t *region =
        vemb_v16_tlc_find_region(tlc, handle->region_id);
    return region && region->is_local;
}

static void timing_acc_add_vector_locality_if_sampled(
    vemb_v16_tlc_t *tlc,
    const vemb_v16_vector_handle_t *handle,
    vemb_v16_timing_acc_t *payload_local_slice,
    vemb_v16_timing_acc_t *payload_remote_slice,
    int sample,
    monotime start) {
    if (!sample) return;
    vemb_v16_timing_acc_t *acc = payload_remote_slice;
    if (vector_handle_is_local(tlc, handle))
        acc = payload_local_slice;
    vemb_v16_timing_acc_add(acc, elapsedNs(start));
}

static int job_shape_matches_tlc(uint32_t dim,
                                 uint32_t vector_bytes,
                                 const vemb_v16_tlc_t *tlc) {
    return dim == tlc->vector_dim &&
        vector_bytes == tlc->value_size;
}

static int job_key_is_source_cutover(vemb_v16_tlc_t *tlc,
                                     const char *key,
                                     uint32_t key_len,
                                     uint64_t key_hash,
                                     tlc_core_key_migration_info_t *info) {
    int rc = tlc_core_key_is_source_cutover(tlc->core,
                                            key,
                                            key_len,
                                            key_hash,
                                            info);
    return rc > 0;
}

static void completion_set_moved(vemb_v16_completion_t *completion,
                                 const tlc_core_key_migration_info_t *info) {
    completion->status = VEMB_V16_STATUS_MOVED;
    completion->redirect_owner = info ? info->target_owner : UINT32_MAX;
}

static void completion_set_ask(vemb_v16_completion_t *completion,
                               const tlc_core_key_migration_info_t *info) {
    completion->status = VEMB_V16_STATUS_ASK;
    completion->redirect_owner = info ? info->target_owner : UINT32_MAX;
}

static void completion_set_vector_handle(
    vemb_v16_completion_t *completion,
    const vemb_v16_vector_handle_t *handle) {
    completion->vector_offset = handle->offset;
    completion->vector_bytes = handle->bytes;
    completion->region_id = handle->region_id;
    completion->local_slot = handle->local_slot;
    completion->owner_generation = handle->owner_generation;
}

static vemb_v16_inline_snapshot_t *inline_snapshot_from_payload(
    const uint8_t *payload) {
    assert(payload != NULL);
    return (vemb_v16_inline_snapshot_t *)(payload -
                                          offsetof(vemb_v16_inline_snapshot_t,
                                                   payload));
}

static uint8_t *completion_alloc_inline_snapshot(uint32_t vector_bytes) {
    size_t bytes = sizeof(vemb_v16_inline_snapshot_t) + vector_bytes;
    vemb_v16_inline_snapshot_t *snapshot = zmalloc(bytes);
    RETURN_IF(!snapshot, NULL);
    snapshot->payload_bytes = vector_bytes;
    snapshot->reserved = 0;
    return snapshot->payload;
}

void vemb_v16_completion_release_inline_snapshot(
    vemb_v16_completion_t *completion) {
    RETURN_IF(!completion || !completion->inline_vector);
    vemb_v16_inline_snapshot_t *snapshot =
        inline_snapshot_from_payload(completion->inline_vector);
    assert(snapshot->payload_bytes == completion->inline_vector_bytes);
    zfree(snapshot);
    completion->inline_vector = NULL;
    completion->inline_vector_bytes = 0;
}

static int snapshot_vemb_payload(vemb_v16_supernode_ctx_t *ctx,
                                 vemb_v16_tlc_t *tlc,
                                 uint8_t op,
                                 uint32_t vector_bytes,
                                 const vemb_v16_vector_handle_t *handle,
                                 vemb_v16_completion_t *completion,
                                 vemb_v16_timing_acc_t *payload_local_slice,
                                 vemb_v16_timing_acc_t *payload_remote_slice,
                                 int sample) {
    RETURN_IF(op != VEMB_V16_OP_VEMB_INLINE, -1);
    completion->inline_vector =
        completion_alloc_inline_snapshot(vector_bytes);
    RETURN_IF(!completion->inline_vector, -1);

    uint32_t vector_len = 0;
    monotime vector_load_start = timing_start_if_sampled(sample);
    int copy_rc = vemb_v16_tlc_load_vector(tlc,
                                           handle,
                                           completion->inline_vector,
                                           vector_bytes,
                                           &vector_len);
    timing_acc_add_vector_locality_if_sampled(tlc,
                                              handle,
                                              payload_local_slice,
                                              payload_remote_slice,
                                              sample,
                                              vector_load_start);
    if (copy_rc != 0 || vector_len != vector_bytes) {
        vemb_v16_completion_release_inline_snapshot(completion);
        return -1;
    }

    completion->inline_vector_bytes = vector_len;
    if (sample) {
        atomic_fetch_add_explicit(&ctx->stats->sample_vector_load_ns,
                                  payload_local_slice->ns +
                                  payload_remote_slice->ns,
                                  memory_order_relaxed);
    }
    return 0;
}

static void log_request_timing(vemb_v16_supernode_ctx_t *ctx,
                               const vemb_v16_job_base_t *job,
                               uint64_t key2_hash,
                               const vemb_v16_completion_t *completion,
                               const vemb_v16_timing_acc_t *job_total,
                               const vemb_v16_timing_acc_t *primary_lookup,
                               const vemb_v16_timing_acc_t *secondary_lookup,
                               const vemb_v16_timing_acc_t *remote_meta_lookup,
                               const vemb_v16_timing_acc_t *payload_local_slice,
                               const vemb_v16_timing_acc_t *payload_remote_slice,
                               const vemb_v16_timing_acc_t *compute) {
    serverLog(LL_DEBUG,
              "vemb_v16 supernode request timing: worker_id=%u op=%s op_code=%u req_id=%u channel_index=%u channel_id=%llu status=%u key_hash=%llu key2_hash=%llu region_id=%u offset=%llu bytes=%u score=%f job_total_count=%llu job_total_ns=%llu job_total_max_ns=%llu primary_lookup_count=%llu primary_lookup_ns=%llu primary_lookup_max_ns=%llu secondary_lookup_count=%llu secondary_lookup_ns=%llu secondary_lookup_max_ns=%llu remote_meta_lookup_count=%llu remote_meta_lookup_ns=%llu remote_meta_lookup_max_ns=%llu payload_local_slice_count=%llu payload_local_slice_ns=%llu payload_local_slice_max_ns=%llu payload_remote_slice_count=%llu payload_remote_slice_ns=%llu payload_remote_slice_max_ns=%llu compute_count=%llu compute_ns=%llu compute_max_ns=%llu",
              ctx->worker_id,
              op_name(job->op),
              job->op,
              job->req_id,
              job->channel_index,
              (unsigned long long)job->channel_id,
              completion->status,
              (unsigned long long)job->key_hash,
              (unsigned long long)key2_hash,
              completion->region_id,
              (unsigned long long)completion->vector_offset,
              completion->vector_bytes,
              completion->score,
              (unsigned long long)job_total->count,
              (unsigned long long)job_total->ns,
              (unsigned long long)job_total->max_ns,
              (unsigned long long)primary_lookup->count,
              (unsigned long long)primary_lookup->ns,
              (unsigned long long)primary_lookup->max_ns,
              (unsigned long long)secondary_lookup->count,
              (unsigned long long)secondary_lookup->ns,
              (unsigned long long)secondary_lookup->max_ns,
              (unsigned long long)remote_meta_lookup->count,
              (unsigned long long)remote_meta_lookup->ns,
              (unsigned long long)remote_meta_lookup->max_ns,
              (unsigned long long)payload_local_slice->count,
              (unsigned long long)payload_local_slice->ns,
              (unsigned long long)payload_local_slice->max_ns,
              (unsigned long long)payload_remote_slice->count,
              (unsigned long long)payload_remote_slice->ns,
              (unsigned long long)payload_remote_slice->max_ns,
              (unsigned long long)compute->count,
              (unsigned long long)compute->ns,
              (unsigned long long)compute->max_ns);
}

static void vemb_v16_notify_completion_consumer(vemb_v16_supernode_ctx_t *ctx) {
#ifdef __linux__
    if (!ctx->completion_notify_armed || !ctx->completion_notify_fd)
        return;
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

static void vemb_v16_publish_completion(vemb_v16_supernode_ctx_t *ctx,
                                        const vemb_v16_completion_t *completion,
                                        monotime completion_start,
                                        int sample) {
    while (vemb_v16_aeron_publish(ctx->completion_ring, completion) != 0 &&
           atomic_load_explicit(ctx->running, memory_order_relaxed) &&
           atomic_load_explicit(ctx->channel_active, memory_order_acquire)) {
        atomic_fetch_add_explicit(&ctx->stats->supernode_completion_ring_full, 1, memory_order_relaxed);
        cpu_relax();
    }
    if (sample) {
        atomic_fetch_add_explicit(&ctx->stats->sample_completion_publish_ns, elapsedNs(completion_start), memory_order_relaxed);
    }
    atomic_fetch_add_explicit(&ctx->stats->supernode_completion_publish, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&ctx->stats->completed_jobs, 1, memory_order_relaxed);
    if (diag_should_log_req(completion->req_id)) {
        serverLog(LL_DEBUG,
                  "vemb_v16 diag supernode completion publish: worker_id=%u channel_index=%u channel_id=%llu req_id=%u op=%u status=%u flags=%u key_hash=%llu vector_bytes=%u inline_vector_bytes=%u region_id=%u local_slot=%u owner_generation=%llu offset=%llu",
                  ctx->worker_id,
                  completion->channel_index,
                  (unsigned long long)completion->channel_id,
                  completion->req_id,
                  completion->op,
                  completion->status,
                  completion->flags,
                  (unsigned long long)completion->key_hash,
                  completion->vector_bytes,
                  completion->inline_vector_bytes,
                  completion->region_id,
                  completion->local_slot,
                  (unsigned long long)completion->owner_generation,
                  (unsigned long long)completion->vector_offset);
    }
    vemb_v16_notify_completion_consumer(ctx);
}

void vemb_v16_supernode_handle_base_job(vemb_v16_supernode_ctx_t *ctx,
                                        const vemb_v16_job_base_t *job) {
    vemb_v16_completion_t completion = {
        .status = job->op == VEMB_V16_OP_PING ?
            VEMB_V16_STATUS_OK :
            VEMB_V16_STATUS_ERR,
        .op = job->op,
        .flags = job->flags,
        .req_id = job->req_id,
        .channel_index = job->channel_index,
        .channel_id = job->channel_id,
        .key_hash = job->key_hash,
    };
    vemb_v16_publish_completion(ctx, &completion, 0, 0);
}

void vemb_v16_supernode_handle_vemb_job(vemb_v16_supernode_ctx_t *ctx,
                                        const vemb_v16_vemb_job_t *vemb_job) {
    vemb_v16_storage_ctx_t *storage = ctx->storage;
    vemb_v16_tlc_t *tlc = storage->tlc;
    const vemb_v16_job_base_t *job = &vemb_job->base;
    vemb_v16_completion_t completion = {
        .status = VEMB_V16_STATUS_OK,
        .op = job->op,
        .flags = job->flags,
        .req_id = job->req_id,
        .channel_index = job->channel_index,
        .channel_id = job->channel_id,
        .key_hash = job->key_hash,
        .dim = vemb_job->dim,
        .vector_bytes = vemb_job->vector_bytes,
    };
    uint32_t warm_slot = 0;
    vemb_v16_vector_handle_t handle = {0};
    int sample = request_should_sample(job->req_id);
    monotime job_start = timing_start_if_sampled(sample);
    monotime lookup_start = timing_start_if_sampled(sample);
    uint64_t lookup_ns = 0;
    vemb_v16_timing_acc_t primary_lookup = {0};
    vemb_v16_timing_acc_t secondary_lookup = {0};
    vemb_v16_timing_acc_t remote_meta_lookup = {0};
    vemb_v16_timing_acc_t payload_local_slice = {0};
    vemb_v16_timing_acc_t payload_remote_slice = {0};
    vemb_v16_timing_acc_t compute = {0};
    const char *err_reason = NULL;
    int migration_active = vemb_v16_storage_migration_active(storage);
    int needs_payload_snapshot = job->op == VEMB_V16_OP_VEMB_INLINE;
    if (!job_shape_matches_tlc(vemb_job->dim, vemb_job->vector_bytes, tlc)) {
        completion.status = VEMB_V16_STATUS_ERR;
        err_reason = "shape_mismatch";
        goto finish_vemb_job;
    }

    if (needs_payload_snapshot &&
        vemb_v16_tlc_get_cached_handle(tlc,
                                       vemb_job->key,
                                       vemb_job->key_len,
                                       job->key_hash,
                                       &handle,
                                       &warm_slot) == 0) {
        lookup_ns = timing_acc_add_if_sampled(&primary_lookup,
                                              sample,
                                              lookup_start);
        completion_set_vector_handle(&completion, &handle);
        if (snapshot_vemb_payload(ctx,
                                  tlc,
                                  job->op,
                                  vemb_job->vector_bytes,
                                  &handle,
                                  &completion,
                                  &payload_local_slice,
                                  &payload_remote_slice,
                                  sample) == 0) {
            goto finish_vemb_job;
        }
        completion.status = VEMB_V16_STATUS_OK;
        completion.vector_bytes = vemb_job->vector_bytes;
        lookup_start = timing_start_if_sampled(sample);
    }

    if (vemb_v16_tlc_get_handle(tlc, vemb_job->key, vemb_job->key_len,
                                job->key_hash, &handle, &warm_slot) != 0) {
        lookup_ns = timing_acc_add_if_sampled(&primary_lookup,
                                              sample,
                                              lookup_start);
        tlc_core_key_migration_info_t redirect_info = {0};
        if (migration_active &&
            job_key_is_source_cutover(tlc,
                                      vemb_job->key,
                                      vemb_job->key_len,
                                      job->key_hash,
                                      &redirect_info)) {
            completion_set_moved(&completion, &redirect_info);
        } else {
            completion.status = VEMB_V16_STATUS_NOT_FOUND;
            atomic_fetch_add_explicit(&ctx->stats->not_found, 1, memory_order_relaxed);
        }
    } else {
        lookup_ns = timing_acc_add_if_sampled(&primary_lookup,
                                              sample,
                                              lookup_start);
        completion_set_vector_handle(&completion, &handle);
        if (needs_payload_snapshot) {
            if (snapshot_vemb_payload(ctx,
                                      tlc,
                                      job->op,
                                      vemb_job->vector_bytes,
                                      &handle,
                                      &completion,
                                      &payload_local_slice,
                                      &payload_remote_slice,
                                      sample) != 0) {
                completion.status = VEMB_V16_STATUS_ERR;
                completion.vector_bytes = 0;
                err_reason = "snapshot_payload_failed";
            }
        }
    }

finish_vemb_job:
    if (completion.status == VEMB_V16_STATUS_ERR) {
        uint64_t current_epoch = 0;
        uint64_t min_write_epoch = 0;
        tlc_core_key_migration_info_t info = {
            .source_owner = UINT32_MAX,
            .target_owner = UINT32_MAX,
        };
        int info_rc = tlc_core_get_migration_info(tlc->core,
                                                  vemb_job->key,
                                                  vemb_job->key_len,
                                                  job->key_hash,
                                                  &info);
        vemb_v16_storage_epoch_get(storage, &current_epoch, &min_write_epoch);
        serverLog(LL_WARNING,
                  "vemb_v16 vemb request failed: req_id=%u op=%u key_hash=%llu key_len=%u reason=%s dim=%u/%u vector_bytes=%u/%u request_epoch=%llu current_epoch=%llu min_write_epoch=%llu migration_active=%d needs_payload_snapshot=%d info_rc=%d state=%u info_epoch=%llu owner_epoch=%llu source=%u target=%u shard=%u",
                  job->req_id,
                  job->op,
                  (unsigned long long)job->key_hash,
                  vemb_job->key_len,
                  err_reason ? err_reason : "unknown",
                  vemb_job->dim,
                  tlc->vector_dim,
                  vemb_job->vector_bytes,
                  tlc->value_size,
                  (unsigned long long)job->topology_epoch,
                  (unsigned long long)current_epoch,
                  (unsigned long long)min_write_epoch,
                  migration_active,
                  needs_payload_snapshot,
                  info_rc,
                  info.migration_state,
                  (unsigned long long)info.topology_epoch,
                  (unsigned long long)info.owner_epoch,
                  info.source_owner,
                  info.target_owner,
                  info.shard_id);
    }
    if (sample) {
        vemb_v16_timing_acc_t job_total = {0};
        atomic_fetch_add_explicit(&ctx->stats->sample_count, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&ctx->stats->sample_table_lookup_ns, lookup_ns, memory_order_relaxed);
        vemb_v16_timing_acc_add(&job_total, elapsedNs(job_start));
        log_request_timing(ctx,
                           job,
                           0,
                           &completion,
                           &job_total,
                           &primary_lookup,
                           &secondary_lookup,
                           &remote_meta_lookup,
                           &payload_local_slice,
                           &payload_remote_slice,
                           &compute);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_JOB_TOTAL, &job_total);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_PRIMARY_LOOKUP, &primary_lookup);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_SECONDARY_LOOKUP, &secondary_lookup);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_REMOTE_META_LOOKUP, &remote_meta_lookup);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_PAYLOAD_LOCAL_SLICE, &payload_local_slice);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_PAYLOAD_REMOTE_SLICE, &payload_remote_slice);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_COMPUTE, &compute);
    }
    atomic_fetch_add_explicit(&ctx->stats->vemb_requests, 1, memory_order_relaxed);
    vemb_v16_publish_completion(ctx,
                                &completion,
                                timing_start_if_sampled(sample),
                                sample);
}

void vemb_v16_supernode_handle_vsim_key_key_job(
    vemb_v16_supernode_ctx_t *ctx,
    const vemb_v16_vsim_key_key_job_t *vsim_job) {
    vemb_v16_storage_ctx_t *storage = ctx->storage;
    vemb_v16_tlc_t *tlc = storage->tlc;
    const vemb_v16_job_base_t *job = &vsim_job->base;
    vemb_v16_completion_t completion = {
        .status = VEMB_V16_STATUS_OK,
        .op = job->op,
        .flags = job->flags,
        .req_id = job->req_id,
        .channel_index = job->channel_index,
        .channel_id = job->channel_id,
        .key_hash = job->key_hash,
        .dim = vsim_job->dim,
        .vector_bytes = vsim_job->vector_bytes,
    };
    uint32_t warm_slot = 0;
    vemb_v16_vector_handle_t handle = {0};
    int sample = request_should_sample(job->req_id);
    monotime job_start = timing_start_if_sampled(sample);
    monotime lookup_start = timing_start_if_sampled(sample);
    uint64_t lookup_ns = 0;
    vemb_v16_timing_acc_t primary_lookup = {0};
    vemb_v16_timing_acc_t secondary_lookup = {0};
    vemb_v16_timing_acc_t remote_meta_lookup = {0};
    vemb_v16_timing_acc_t payload_local_slice = {0};
    vemb_v16_timing_acc_t payload_remote_slice = {0};
    vemb_v16_timing_acc_t compute = {0};
    int migration_active = vemb_v16_storage_migration_active(storage);

    if (!job_shape_matches_tlc(vsim_job->dim, vsim_job->vector_bytes, tlc)) {
        completion.status = VEMB_V16_STATUS_ERR;
        goto finish_vsim_job;
    }

    if (vemb_v16_tlc_get_handle(tlc, vsim_job->key, vsim_job->key_len,
                                job->key_hash, &handle, &warm_slot) != 0) {
        lookup_ns = timing_acc_add_if_sampled(&primary_lookup,
                                              sample,
                                              lookup_start);
        tlc_core_key_migration_info_t redirect_info = {0};
        if (migration_active &&
            job_key_is_source_cutover(tlc,
                                      vsim_job->key,
                                      vsim_job->key_len,
                                      job->key_hash,
                                      &redirect_info)) {
            completion_set_moved(&completion, &redirect_info);
        } else {
            completion.status = VEMB_V16_STATUS_NOT_FOUND;
            atomic_fetch_add_explicit(&ctx->stats->not_found, 1, memory_order_relaxed);
        }
        goto finish_vsim_job;
    }

    lookup_ns = timing_acc_add_if_sampled(&primary_lookup, sample, lookup_start);
    completion_set_vector_handle(&completion, &handle);

    vemb_v16_vector_handle_t handle2 = {0};
    vemb_v16_tlc_lookup_source_t key2_source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    vemb_v16_tlc_lookup_timing_t key2_timing = {0};
    if (vemb_v16_tlc_lookup_vsim_key2(tlc,
                                      vsim_job->key2,
                                      vsim_job->key2_len,
                                      vsim_job->key2_hash,
                                      &handle2,
                                      &key2_source,
                                      &key2_timing) != 0) {
        timing_acc_add_ns_if_sampled(&secondary_lookup,
                                     sample,
                                     key2_timing.local_lookup_ns);
        if (key2_timing.remote_meta_lookup_count)
            timing_acc_add_ns_if_sampled(
                &remote_meta_lookup,
                sample,
                key2_timing.remote_meta_lookup_ns);
        tlc_core_key_migration_info_t redirect_info = {0};
        if (migration_active &&
            job_key_is_source_cutover(tlc,
                                      vsim_job->key2,
                                      vsim_job->key2_len,
                                      vsim_job->key2_hash,
                                      &redirect_info)) {
            completion_set_moved(&completion, &redirect_info);
        } else {
            serverLog(LL_NOTICE,
                      "vemb_v16 vsim key-key key2 lookup miss: req_id=%u key_hash=%llu key2_hash=%llu key2_len=%u",
                      job->req_id,
                      (unsigned long long)job->key_hash,
                      (unsigned long long)vsim_job->key2_hash,
                      vsim_job->key2_len);
            completion.status = VEMB_V16_STATUS_NOT_FOUND;
            atomic_fetch_add_explicit(&ctx->stats->not_found, 1, memory_order_relaxed);
        }
        goto finish_vsim_job;
    }

    const uint8_t *v1_bytes = NULL;
    const uint8_t *v2_bytes = NULL;
    uint32_t v1_len = 0;
    uint32_t v2_len = 0;
    timing_acc_add_ns_if_sampled(&secondary_lookup,
                                 sample,
                                 key2_timing.local_lookup_ns);
    if (key2_timing.remote_meta_lookup_count)
        timing_acc_add_ns_if_sampled(
            &remote_meta_lookup,
            sample,
            key2_timing.remote_meta_lookup_ns);
    monotime slice_start = timing_start_if_sampled(sample);
    int v1_rc = vemb_v16_tlc_vector_slice(tlc,
                                          &handle,
                                          &v1_bytes,
                                          &v1_len);
    timing_acc_add_vector_locality_if_sampled(
        tlc,
        &handle,
        &payload_local_slice,
        &payload_remote_slice,
        sample,
        slice_start);
    slice_start = timing_start_if_sampled(sample);
    int v2_rc = vemb_v16_tlc_vector_slice(tlc,
                                          &handle2,
                                          &v2_bytes,
                                          &v2_len);
    timing_acc_add_vector_locality_if_sampled(
        tlc,
        &handle2,
        &payload_local_slice,
        &payload_remote_slice,
        sample,
        slice_start);
    if (v1_rc != 0 || v2_rc != 0 ||
        v1_len != vsim_job->vector_bytes ||
        v2_len != vsim_job->vector_bytes) {
        serverLog(LL_WARNING,
                  "vemb_v16 vsim key-key vector slice failed: req_id=%u key_hash=%llu key2_hash=%llu region1=%u offset1=%llu bytes1=%u region2=%u offset2=%llu bytes2=%u expected_bytes=%u",
                  job->req_id,
                  (unsigned long long)job->key_hash,
                  (unsigned long long)vsim_job->key2_hash,
                  handle.region_id,
                  (unsigned long long)handle.offset,
                  handle.bytes,
                  handle2.region_id,
                  (unsigned long long)handle2.offset,
                  handle2.bytes,
                  vsim_job->vector_bytes);
        completion.status = VEMB_V16_STATUS_ERR;
    } else {
        const float *v1 = (const float *)(const void *)v1_bytes;
        const float *v2 = (const float *)(const void *)v2_bytes;
        monotime compute_start = timing_start_if_sampled(sample);
        completion.score = sve_cosine_similarity_f32(v1, v2, vsim_job->dim);
        timing_acc_add_if_sampled(&compute, sample, compute_start);
        serverLog(LL_DEBUG,
                  "vemb_v16 vsim key-key ok: req_id=%u key_hash=%llu key2_hash=%llu key2_source=%u region1=%u offset1=%llu region2=%u offset2=%llu score=%f",
                  job->req_id,
                  (unsigned long long)job->key_hash,
                  (unsigned long long)vsim_job->key2_hash,
                  key2_source,
                  handle.region_id,
                  (unsigned long long)handle.offset,
                  handle2.region_id,
                  (unsigned long long)handle2.offset,
                  completion.score);
    }

finish_vsim_job:
    if (sample) {
        vemb_v16_timing_acc_t job_total = {0};
        atomic_fetch_add_explicit(&ctx->stats->sample_count, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&ctx->stats->sample_table_lookup_ns, lookup_ns, memory_order_relaxed);
        vemb_v16_timing_acc_add(&job_total, elapsedNs(job_start));
        log_request_timing(ctx,
                           job,
                           vsim_job->key2_hash,
                           &completion,
                           &job_total,
                           &primary_lookup,
                           &secondary_lookup,
                           &remote_meta_lookup,
                           &payload_local_slice,
                           &payload_remote_slice,
                           &compute);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_JOB_TOTAL, &job_total);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_PRIMARY_LOOKUP, &primary_lookup);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_SECONDARY_LOOKUP, &secondary_lookup);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_REMOTE_META_LOOKUP, &remote_meta_lookup);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_PAYLOAD_LOCAL_SLICE, &payload_local_slice);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_PAYLOAD_REMOTE_SLICE, &payload_remote_slice);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_COMPUTE, &compute);
    }
    atomic_fetch_add_explicit(&ctx->stats->vsim_requests, 1, memory_order_relaxed);
    vemb_v16_publish_completion(ctx,
                                &completion,
                                timing_start_if_sampled(sample),
                                sample);
}

void vemb_v16_supernode_handle_vrem_job(vemb_v16_supernode_ctx_t *ctx,
                                        const vemb_v16_vemb_job_t *vrem_job) {
    vemb_v16_storage_ctx_t *storage = ctx->storage;
    vemb_v16_tlc_t *tlc = storage->tlc;
    const vemb_v16_job_base_t *job = &vrem_job->base;
    vemb_v16_completion_t completion = {
        .status = VEMB_V16_STATUS_OK,
        .op = job->op,
        .flags = job->flags,
        .req_id = job->req_id,
        .channel_index = job->channel_index,
        .channel_id = job->channel_id,
        .key_hash = job->key_hash,
        .dim = vrem_job->dim,
        .vector_bytes = vrem_job->vector_bytes,
    };
    int stale_topology = 0;
    tlc_core_key_migration_info_t redirect_info = {0};
    uint64_t write_topology_epoch = job->topology_epoch;
    int ask_redirect = (job->flags & VEMB_V16_REQ_F_ASK_REDIRECT) != 0;
    vemb_v16_vector_handle_t existing = {0};
    uint32_t warm_slot = 0;
    int migration_active = vemb_v16_storage_migration_active(storage);

    if (ask_redirect) {
        if (vemb_v16_storage_ask_redirect_write_ready(
                storage,
                vrem_job->key,
                vrem_job->key_len,
                job->key_hash,
                &redirect_info) != 0) {
            completion_set_ask(&completion, &redirect_info);
        } else {
            write_topology_epoch = redirect_info.owner_epoch ?
                redirect_info.owner_epoch : redirect_info.topology_epoch;
        }
    } else if (migration_active) {
        if (vemb_v16_storage_write_epoch_is_stale(storage,
                                                  job->topology_epoch)) {
            stale_topology = 1;
        } else if (job_key_is_source_cutover(tlc,
                                             vrem_job->key,
                                             vrem_job->key_len,
                                             job->key_hash,
                                             &redirect_info)) {
            completion_set_moved(&completion, &redirect_info);
        } else if (vemb_v16_storage_migration_write_blocked_info(
                       storage,
                       vrem_job->key,
                       vrem_job->key_len,
                       job->key_hash,
                       &redirect_info,
                       NULL)) {
            completion_set_ask(&completion, &redirect_info);
        }
    }

    if (completion.status == VEMB_V16_STATUS_OK && !stale_topology) {
        if (vemb_v16_tlc_get_handle(tlc,
                                    vrem_job->key,
                                    vrem_job->key_len,
                                    job->key_hash,
                                    &existing,
                                    &warm_slot) != 0) {
            memset(&redirect_info, 0, sizeof(redirect_info));
            if (!ask_redirect &&
                migration_active &&
                job_key_is_source_cutover(tlc,
                                          vrem_job->key,
                                          vrem_job->key_len,
                                          job->key_hash,
                                          &redirect_info)) {
                completion_set_moved(&completion, &redirect_info);
            } else {
                completion.status = VEMB_V16_STATUS_NOT_FOUND;
                completion.vector_bytes = 0;
                atomic_fetch_add_explicit(&ctx->stats->not_found, 1, memory_order_relaxed);
            }
        } else {
            tlc_core_key_migration_info_t delete_info = {0};
            if (vemb_v16_storage_delete_with_epoch(storage,
                                                   vrem_job->key,
                                                   vrem_job->key_len,
                                                   job->key_hash,
                                                   write_topology_epoch,
                                                   &delete_info,
                                                   NULL) == 0) {
                completion.vector_bytes = 0;
                completion.dim = 0;
                completion.region_id = UINT32_MAX;
                completion.local_slot = UINT32_MAX;
            } else {
                memset(&redirect_info, 0, sizeof(redirect_info));
                if (!ask_redirect &&
                    migration_active &&
                    job_key_is_source_cutover(tlc,
                                              vrem_job->key,
                                              vrem_job->key_len,
                                              job->key_hash,
                                              &redirect_info)) {
                    completion_set_moved(&completion, &redirect_info);
                } else {
                    completion.status = VEMB_V16_STATUS_ERR;
                }
            }
        }
    }
    if (stale_topology)
        completion.status = VEMB_V16_STATUS_STALE_TOPOLOGY;

    atomic_fetch_add_explicit(&ctx->stats->vadd_requests, 1, memory_order_relaxed);
    vemb_v16_publish_completion(ctx, &completion, 0, 0);
}

void vemb_v16_supernode_handle_vadd_job(vemb_v16_supernode_ctx_t *ctx,
                                        const vemb_v16_vadd_job_t *vadd_job) {
    vemb_v16_storage_ctx_t *storage = ctx->storage;
    vemb_v16_tlc_t *tlc = storage->tlc;
    const vemb_v16_job_base_t *job = &vadd_job->base;
    vemb_v16_completion_t completion = {
        .status = VEMB_V16_STATUS_OK,
        .op = job->op,
        .flags = job->flags,
        .req_id = job->req_id,
        .channel_index = job->channel_index,
        .channel_id = job->channel_id,
        .key_hash = job->key_hash,
        .dim = vadd_job->dim,
        .vector_bytes = vadd_job->vector_bytes,
    };
    int sample = request_should_sample(job->req_id);
    monotime job_start = timing_start_if_sampled(sample);
    vemb_v16_timing_acc_t primary_lookup = {0};
    vemb_v16_timing_acc_t payload_local_slice = {0};
    vemb_v16_timing_acc_t payload_remote_slice = {0};
    vemb_v16_timing_acc_t compute = {0};
    const char *err_reason = NULL;

    if ((job->op == VEMB_V16_OP_VADD ||
         job->op == VEMB_V16_OP_VSIM_INLINE) &&
        !job_shape_matches_tlc(vadd_job->dim, vadd_job->vector_bytes, tlc)) {
        completion.status = VEMB_V16_STATUS_ERR;
        err_reason = "shape_mismatch";
        goto finish_vadd_job;
    }

    if (job->op == VEMB_V16_OP_VADD) {
        uint32_t warm_slot = 0;
        vemb_v16_vector_handle_t handle = {0};
        int put_rc = -1;
        int stale_topology = 0;
        uint64_t write_topology_epoch = job->topology_epoch;
        int ask_redirect = (job->flags & VEMB_V16_REQ_F_ASK_REDIRECT) != 0;
        int migration_active = vemb_v16_storage_migration_active(storage);
        tlc_core_key_migration_info_t redirect_info = {0};
        if (ask_redirect) {
            if (vemb_v16_storage_ask_redirect_write_ready(
                    storage,
                    vadd_job->key,
                    vadd_job->key_len,
                    job->key_hash,
                    &redirect_info) != 0) {
                completion_set_ask(&completion, &redirect_info);
            } else {
                write_topology_epoch = redirect_info.owner_epoch ?
                    redirect_info.owner_epoch : redirect_info.topology_epoch;
            }
        } else if (migration_active) {
            if (vemb_v16_storage_write_epoch_is_stale(storage,
                                                      job->topology_epoch)) {
                stale_topology = 1;
            } else if (job_key_is_source_cutover(tlc,
                                                 vadd_job->key,
                                                 vadd_job->key_len,
                                                 job->key_hash,
                                                 &redirect_info)) {
                completion_set_moved(&completion, &redirect_info);
            } else if (vemb_v16_storage_migration_write_blocked_info(
                           storage,
                           vadd_job->key,
                           vadd_job->key_len,
                           job->key_hash,
                           &redirect_info,
                           NULL)) {
                completion_set_ask(&completion, &redirect_info);
            }
        }

        // ASK is resolved before this point; entering here means local put can proceed.
        // normal vadd put if not redirected or moved, and topology is not stale
        if (completion.status == VEMB_V16_STATUS_OK && !stale_topology) {
            monotime put_start = timing_start_if_sampled(sample);
            put_rc = vemb_v16_tlc_put_with_epoch(tlc,
                                                 vadd_job->key,
                                                 vadd_job->key_len,
                                                 job->key_hash,
                                                 vadd_job->vector,
                                                 vadd_job->vector_bytes,
                                                 write_topology_epoch,
                                                 &handle,
                                                 &warm_slot);
            if (put_rc == 0 && handle.bytes > 0) {
                timing_acc_add_vector_locality_if_sampled(
                    tlc,
                    &handle,
                    &payload_local_slice,
                    &payload_remote_slice,
                    sample,
                    put_start);
            } else if (put_rc != 0 && !ask_redirect) {
                tlc_core_key_migration_info_t redirect_info = {0};
                if (migration_active &&
                    job_key_is_source_cutover(tlc,
                                              vadd_job->key,
                                              vadd_job->key_len,
                                              job->key_hash,
                                              &redirect_info)) {
                    completion_set_moved(&completion, &redirect_info);
                }
            }
            // MOVED after the local put attempt must not continue local success completion.
            if (completion.status != VEMB_V16_STATUS_MOVED) {
                if (put_rc != 0) {
                    if (diag_should_log_req(job->req_id)) {
                        uint64_t current_epoch = 0;
                        uint64_t min_write_epoch = 0;
                        tlc_core_key_migration_info_t info = {
                            .source_owner = UINT32_MAX,
                            .target_owner = UINT32_MAX,
                        };
                        int info_rc = tlc_core_get_migration_info(tlc->core,
                            vadd_job->key,
                            vadd_job->key_len,
                            job->key_hash,
                            &info);
                        vemb_v16_storage_epoch_get(storage,
                                                   &current_epoch,
                                                   &min_write_epoch);
                        serverLog(LL_WARNING,
                                  "vemb_v16 vadd put failed: req_id=%u key_hash=%llu key_len=%u status_reason=%s put_rc=%d dim=%u/%u vector_bytes=%u/%u request_epoch=%llu write_epoch=%llu current_epoch=%llu min_write_epoch=%llu migration_active=%d ask_redirect=%d stale_topology=%d info_rc=%d state=%u info_epoch=%llu owner_epoch=%llu source=%u target=%u shard=%u",
                                  job->req_id,
                                  (unsigned long long)job->key_hash,
                                  vadd_job->key_len,
                                  "tlc_put",
                                  put_rc,
                                  vadd_job->dim,
                                  tlc->vector_dim,
                                  vadd_job->vector_bytes,
                                  tlc->value_size,
                                  (unsigned long long)job->topology_epoch,
                                  (unsigned long long)write_topology_epoch,
                                  (unsigned long long)current_epoch,
                                  (unsigned long long)min_write_epoch,
                                  migration_active,
                                  ask_redirect,
                                  stale_topology,
                                  info_rc,
                                  info.migration_state,
                                  (unsigned long long)info.topology_epoch,
                                  (unsigned long long)info.owner_epoch,
                                  info.source_owner,
                                  info.target_owner,
                                  info.shard_id);
                    }
                    completion.status = VEMB_V16_STATUS_ERR;
                    err_reason = "tlc_put";
                } else {
                    completion_set_vector_handle(&completion, &handle);
                    if (!ask_redirect &&
                        migration_active &&
                        vemb_v16_storage_migration_delta_put_after_local_write(
                            storage,
                            vadd_job->key,
                            vadd_job->key_len,
                            job->key_hash,
                            &handle,
                            NULL) != 0) {
                        completion.status = VEMB_V16_STATUS_ERR;
                        if (diag_should_log_req(job->req_id)) {
                            serverLog(LL_WARNING,
                                      "vemb_v16 vadd migration delta publish failed: req_id=%u key_hash=%llu region_id=%u offset=%llu bytes=%u",
                                      job->req_id,
                                      (unsigned long long)job->key_hash,
                                      handle.region_id,
                                      (unsigned long long)handle.offset,
                                      handle.bytes);
                        }
                        err_reason = "delta_publish";
                    }
                    if (handle.bytes > 0 &&
                        completion.status == VEMB_V16_STATUS_OK &&
                        vemb_v16_tlc_remote_meta_owner_view_count(tlc) > 1 &&
                        enqueue_remote_meta_publish(tlc,
                                                    tlc->remote_meta_view,
                                                    vadd_job->key,
                                                    vadd_job->key_len,
                                                    job->key_hash,
                                                    &handle,
                                                    0) != 0 &&
                        diag_should_log_req(job->req_id)) {
                        serverLog(LL_WARNING,
                                  "vemb_v16 vadd remote meta publish enqueue failed: req_id=%u key_hash=%llu region_id=%u offset=%llu bytes=%u",
                                  job->req_id,
                                  (unsigned long long)job->key_hash,
                                  handle.region_id,
                                  (unsigned long long)handle.offset,
                                  handle.bytes);
                    }
                }
            }
        }
        if (stale_topology)
            completion.status = VEMB_V16_STATUS_STALE_TOPOLOGY;
    } else if (job->op == VEMB_V16_OP_VSIM_INLINE) {
        uint32_t warm_slot = 0;
        vemb_v16_vector_handle_t handle = {0};
        monotime lookup_start = timing_start_if_sampled(sample);
        int migration_active = vemb_v16_storage_migration_active(storage);
        if (vemb_v16_tlc_get_handle(tlc, vadd_job->key, vadd_job->key_len,
                                    job->key_hash, &handle, &warm_slot) != 0) {
            timing_acc_add_if_sampled(&primary_lookup, sample, lookup_start);
            tlc_core_key_migration_info_t redirect_info = {0};
            if (migration_active &&
                job_key_is_source_cutover(tlc,
                                          vadd_job->key,
                                          vadd_job->key_len,
                                          job->key_hash,
                                          &redirect_info)) {
                completion_set_moved(&completion, &redirect_info);
            } else {
                completion.status = VEMB_V16_STATUS_NOT_FOUND;
                atomic_fetch_add_explicit(&ctx->stats->not_found, 1, memory_order_relaxed);
            }
        } else {
            timing_acc_add_if_sampled(&primary_lookup, sample, lookup_start);
            const uint8_t *stored_bytes = NULL;
            uint32_t stored_len = 0;
            monotime slice_start = timing_start_if_sampled(sample);
            if (vemb_v16_tlc_vector_slice(tlc, &handle,
                                          &stored_bytes,
                                          &stored_len) != 0 ||
                stored_len != vadd_job->vector_bytes) {
                timing_acc_add_vector_locality_if_sampled(
                    tlc,
                    &handle,
                    &payload_local_slice,
                    &payload_remote_slice,
                    sample,
                    slice_start);
                completion.status = VEMB_V16_STATUS_ERR;
            } else {
                timing_acc_add_vector_locality_if_sampled(
                    tlc,
                    &handle,
                    &payload_local_slice,
                    &payload_remote_slice,
                    sample,
                    slice_start);
                const float *stored = (const float *)(const void *)stored_bytes;
                completion_set_vector_handle(&completion, &handle);
                monotime compute_start = timing_start_if_sampled(sample);
                completion.score = sve_cosine_similarity_f32(stored, vadd_job->vector, vadd_job->dim);
                timing_acc_add_if_sampled(&compute, sample, compute_start);
            }
        }
    } else {
        completion.status = VEMB_V16_STATUS_ERR;
    }

finish_vadd_job:
    if (completion.status == VEMB_V16_STATUS_ERR &&
        job->op == VEMB_V16_OP_VADD) {
        uint64_t current_epoch = 0;
        uint64_t min_write_epoch = 0;
        tlc_core_key_migration_info_t info = {
            .source_owner = UINT32_MAX,
            .target_owner = UINT32_MAX,
        };
        int info_rc = tlc_core_get_migration_info(tlc->core,
                                                  vadd_job->key,
                                                  vadd_job->key_len,
                                                  job->key_hash,
                                                  &info);
        vemb_v16_storage_epoch_get(storage, &current_epoch, &min_write_epoch);
        serverLog(LL_WARNING,
                  "vemb_v16 vadd request failed: req_id=%u key_hash=%llu key_len=%u reason=%s dim=%u/%u vector_bytes=%u/%u request_epoch=%llu current_epoch=%llu min_write_epoch=%llu info_rc=%d state=%u info_epoch=%llu owner_epoch=%llu source=%u target=%u shard=%u",
                  job->req_id,
                  (unsigned long long)job->key_hash,
                  vadd_job->key_len,
                  err_reason ? err_reason : "unknown",
                  vadd_job->dim,
                  tlc->vector_dim,
                  vadd_job->vector_bytes,
                  tlc->value_size,
                  (unsigned long long)job->topology_epoch,
                  (unsigned long long)current_epoch,
                  (unsigned long long)min_write_epoch,
                  info_rc,
                  info.migration_state,
                  (unsigned long long)info.topology_epoch,
                  (unsigned long long)info.owner_epoch,
                  info.source_owner,
                  info.target_owner,
                  info.shard_id);
    }
    if (job->op == VEMB_V16_OP_VSIM_INLINE) {
        atomic_fetch_add_explicit(&ctx->stats->vsim_requests, 1, memory_order_relaxed);
    } else if (job->op == VEMB_V16_OP_VADD) {
        atomic_fetch_add_explicit(&ctx->stats->vadd_requests, 1, memory_order_relaxed);
    }

    if (sample) {
        vemb_v16_timing_acc_t job_total = {0};
        vemb_v16_timing_acc_add(&job_total, elapsedNs(job_start));
        vemb_v16_timing_acc_t secondary_lookup = {0};
        vemb_v16_timing_acc_t remote_meta_lookup = {0};
        log_request_timing(ctx,
                           job,
                           0,
                           &completion,
                           &job_total,
                           &primary_lookup,
                           &secondary_lookup,
                           &remote_meta_lookup,
                           &payload_local_slice,
                           &payload_remote_slice,
                           &compute);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_JOB_TOTAL, &job_total);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_PRIMARY_LOOKUP, &primary_lookup);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_PAYLOAD_LOCAL_SLICE, &payload_local_slice);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_PAYLOAD_REMOTE_SLICE, &payload_remote_slice);
        vemb_v16_channel_counters_add_timing(ctx->stats, VEMB_V16_TIMING_COMPUTE, &compute);
    }
    vemb_v16_publish_completion(ctx, &completion, 0, 0);
}

int vemb_v16_supernode_scratch_init(vemb_v16_supernode_scratch_t *scratch) {
    assert(scratch != NULL);
    memset(scratch, 0, sizeof(*scratch));
    scratch->job_refs = zmalloc(sizeof(*scratch->job_refs) *
                                VEMB_V16_SUPERNODE_BATCH);
    if (!scratch->job_refs) {
        vemb_v16_supernode_scratch_cleanup(scratch);
        return -1;
    }
    return 0;
}

void vemb_v16_supernode_scratch_cleanup(vemb_v16_supernode_scratch_t *scratch) {
    assert(scratch != NULL);
    zfree(scratch->job_refs);
    memset(scratch, 0, sizeof(*scratch));
}
