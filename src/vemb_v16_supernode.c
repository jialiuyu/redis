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

#include <stdlib.h>
#include <stdatomic.h>
#include <string.h>
#include <time.h>
#ifdef __linux__
#include <unistd.h>
#endif

#define VEMB_V16_SAMPLE_MASK 1023u
#define VEMB_V16_SUPERNODE_BATCH 32u

static uint64_t monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static const char *op_name(uint8_t op) {
    switch (op) {
    case VEMB_V16_OP_VADD_INLINE:
        return "vadd-inline";
    case VEMB_V16_OP_VEMB_SUPERNODE_READ:
        return "vemb-supernode-read";
    case VEMB_V16_OP_VSIM_INLINE:
        return "vsim-inline";
    case VEMB_V16_OP_VSIM_KEY_KEY:
        return "vsim-key-key";
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

static void log_request_timing(vemb_v16_supernode_ctx_t *ctx,
                               const vemb_v16_job_base_t *job,
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
              (unsigned long long)job->key2_hash,
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
    if (!ctx || !ctx->completion_notify_armed || !ctx->completion_notify_fd)
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
                                        uint64_t completion_start,
                                        int sample) {
    while (vemb_v16_aeron_publish(ctx->completion_ring, completion) != 0 &&
           atomic_load_explicit(ctx->running, memory_order_relaxed) &&
           atomic_load_explicit(ctx->channel_active, memory_order_acquire)) {
        atomic_fetch_add_explicit(&ctx->stats->supernode_completion_ring_full, 1,
                                  memory_order_relaxed);
        cpu_relax();
    }
    if (sample) {
        atomic_fetch_add_explicit(&ctx->stats->sample_completion_publish_ns,
                                  monotonic_ns() - completion_start,
                                  memory_order_relaxed);
    }
    atomic_fetch_add_explicit(&ctx->stats->supernode_completion_publish, 1,
                              memory_order_relaxed);
    atomic_fetch_add_explicit(&ctx->stats->completed_jobs, 1,
                              memory_order_relaxed);
    vemb_v16_notify_completion_consumer(ctx);
}

void vemb_v16_supernode_handle_vemb_job(vemb_v16_supernode_ctx_t *ctx,
                                        vemb_v16_vemb_job_t *vemb_job,
                                        float **read_result,
                                        size_t *read_result_bytes) {
    vemb_v16_storage_ctx_t *storage = ctx->storage;
    vemb_v16_tlc_t *tlc = storage->tlc;
    vemb_v16_job_base_t *job = &vemb_job->base;
    vemb_v16_completion_t completion = {
        .status = VEMB_V16_STATUS_OK,
        .op = job->op,
        .flags = job->flags,
        .req_id = job->req_id,
        .channel_index = job->channel_index,
        .channel_id = job->channel_id,
        .key_hash = job->key_hash,
        .dim = job->dim,
        .vector_bytes = job->vector_bytes,
    };
    uint32_t warm_slot = 0;
    vemb_v16_vector_handle_t handle = {0};
    int sample = ((job->req_id & VEMB_V16_SAMPLE_MASK) == 0);
    uint64_t job_start = monotonic_ns();
    uint64_t lookup_start = monotonic_ns();
    uint64_t lookup_ns = 0;
    vemb_v16_timing_acc_t primary_lookup = {0};
    vemb_v16_timing_acc_t secondary_lookup = {0};
    vemb_v16_timing_acc_t remote_meta_lookup = {0};
    vemb_v16_timing_acc_t payload_local_slice = {0};
    vemb_v16_timing_acc_t payload_remote_slice = {0};
    vemb_v16_timing_acc_t compute = {0};

    if (vemb_v16_tlc_get_handle(tlc, job->key, job->key_len,
                                job->key_hash, &handle, &warm_slot) != 0) {
        lookup_ns = monotonic_ns() - lookup_start;
        vemb_v16_timing_acc_add(&primary_lookup, lookup_ns);
        completion.status = VEMB_V16_STATUS_NOT_FOUND;
        atomic_fetch_add_explicit(&ctx->stats->not_found, 1,
                                  memory_order_relaxed);
    } else {
        lookup_ns = monotonic_ns() - lookup_start;
        vemb_v16_timing_acc_add(&primary_lookup, lookup_ns);
        if (job->dim != tlc->vector_dim ||
            job->vector_bytes != tlc->value_size) {
            completion.status = VEMB_V16_STATUS_ERR;
        } else {
            completion.vector_offset = handle.offset;
            completion.vector_bytes = handle.bytes;
            completion.region_id = handle.region_id;
            if (job->op == VEMB_V16_OP_VSIM_KEY_KEY) {
                vemb_v16_vector_handle_t handle2 = {0};
                vemb_v16_tlc_lookup_source_t key2_source =
                    VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
                vemb_v16_tlc_lookup_timing_t key2_timing = {0};
                if (vemb_v16_tlc_lookup_vsim_key2_timed(tlc,
                                                        job->key2,
                                                        job->key2_len,
                                                        job->key2_hash,
                                                        &handle2,
                                                        &key2_source,
                    &key2_timing) != 0) {
                    if (key2_timing.local_lookup_count)
                        vemb_v16_timing_acc_add(&secondary_lookup, key2_timing.local_lookup_ns);
                    if (key2_timing.remote_meta_lookup_count)
                        vemb_v16_timing_acc_add(&remote_meta_lookup, key2_timing.remote_meta_lookup_ns);
                    serverLog(LL_NOTICE,
                              "vemb_v16 vsim key-key key2 lookup miss: req_id=%u key_hash=%llu key2_hash=%llu key2_len=%u",
                              job->req_id,
                              (unsigned long long)job->key_hash,
                              (unsigned long long)job->key2_hash,
                              job->key2_len);
                    completion.status = VEMB_V16_STATUS_NOT_FOUND;
                    atomic_fetch_add_explicit(&ctx->stats->not_found, 1,
                                              memory_order_relaxed);
                } else {
                    const uint8_t *v1_bytes = NULL;
                    const uint8_t *v2_bytes = NULL;
                    uint32_t v1_len = 0;
                    uint32_t v2_len = 0;
                    if (key2_timing.local_lookup_count)
                        vemb_v16_timing_acc_add(&secondary_lookup, key2_timing.local_lookup_ns);
                    if (key2_timing.remote_meta_lookup_count)
                        vemb_v16_timing_acc_add(&remote_meta_lookup, key2_timing.remote_meta_lookup_ns);
                    uint64_t slice_start = monotonic_ns();
                    int v1_rc = vemb_v16_tlc_vector_slice(tlc,
                                                          &handle,
                                                          &v1_bytes,
                                                          &v1_len);
                    uint64_t slice_ns = monotonic_ns() - slice_start;
                    if (vector_handle_is_local(tlc, &handle)) {
                        vemb_v16_timing_acc_add(&payload_local_slice, slice_ns);
                    } else {
                        vemb_v16_timing_acc_add(&payload_remote_slice, slice_ns);
                    }
                    slice_start = monotonic_ns();
                    int v2_rc = vemb_v16_tlc_vector_slice(tlc,
                                                          &handle2,
                                                          &v2_bytes,
                                                          &v2_len);
                    slice_ns = monotonic_ns() - slice_start;
                    if (vector_handle_is_local(tlc, &handle2)) {
                        vemb_v16_timing_acc_add(&payload_local_slice, slice_ns);
                    } else {
                        vemb_v16_timing_acc_add(&payload_remote_slice, slice_ns);
                    }
                    if (v1_rc != 0 || v2_rc != 0 ||
                        v1_len != job->vector_bytes ||
                        v2_len != job->vector_bytes) {
                        serverLog(LL_WARNING,
                                  "vemb_v16 vsim key-key vector slice failed: req_id=%u key_hash=%llu key2_hash=%llu region1=%u offset1=%llu bytes1=%u region2=%u offset2=%llu bytes2=%u expected_bytes=%u",
                                  job->req_id,
                                  (unsigned long long)job->key_hash,
                                  (unsigned long long)job->key2_hash,
                                  handle.region_id,
                                  (unsigned long long)handle.offset,
                                  handle.bytes,
                                  handle2.region_id,
                                  (unsigned long long)handle2.offset,
                                  handle2.bytes,
                                  job->vector_bytes);
                        completion.status = VEMB_V16_STATUS_ERR;
                    } else {
                        const float *v1 = (const float *)(const void *)v1_bytes;
                        const float *v2 = (const float *)(const void *)v2_bytes;
                        uint64_t compute_start = monotonic_ns();
                        completion.score = sve_cosine_similarity_f32(v1, v2, job->dim);
                        vemb_v16_timing_acc_add(&compute, monotonic_ns() - compute_start);
                        serverLog(LL_DEBUG,
                                  "vemb_v16 vsim key-key ok: req_id=%u key_hash=%llu key2_hash=%llu key2_source=%u region1=%u offset1=%llu region2=%u offset2=%llu score=%f",
                                  job->req_id,
                                  (unsigned long long)job->key_hash,
                                  (unsigned long long)job->key2_hash,
                                  key2_source,
                                  handle.region_id,
                                  (unsigned long long)handle.offset,
                                  handle2.region_id,
                                  (unsigned long long)handle2.offset,
                                  completion.score);
                    }
                }
            } else if (job->op == VEMB_V16_OP_VEMB_SUPERNODE_READ) {
                if (*read_result_bytes < job->vector_bytes) {
                    float *next = zrealloc(*read_result, job->vector_bytes);
                    if (!next) {
                        completion.status = VEMB_V16_STATUS_ERR;
                        goto vemb_read_done;
                    }
                    *read_result = next;
                    *read_result_bytes = job->vector_bytes;
                }

                const uint8_t *vector_bytes = NULL;
                uint32_t vector_len = 0;
                uint64_t vector_load_start = monotonic_ns();
                if (vemb_v16_tlc_vector_slice(tlc, &handle,
                                              &vector_bytes,
                                              &vector_len) != 0 ||
                    vector_len != job->vector_bytes) {
                    uint64_t vector_load_ns = monotonic_ns() - vector_load_start;
                    if (vector_handle_is_local(tlc, &handle))
                        vemb_v16_timing_acc_add(&payload_local_slice, vector_load_ns);
                    else
                        vemb_v16_timing_acc_add(&payload_remote_slice, vector_load_ns);
                    completion.status = VEMB_V16_STATUS_ERR;
                } else {
                    uint64_t vector_load_ns = monotonic_ns() - vector_load_start;
                    if (vector_handle_is_local(tlc, &handle))
                        vemb_v16_timing_acc_add(&payload_local_slice, vector_load_ns);
                    else
                        vemb_v16_timing_acc_add(&payload_remote_slice, vector_load_ns);
                    memcpy(*read_result, vector_bytes, vector_len);
                }
                if (sample) {
                    atomic_fetch_add_explicit(&ctx->stats->sample_vector_load_ns,
                                              payload_local_slice.ns +
                                              payload_remote_slice.ns,
                                              memory_order_relaxed);
                }
            }
        }
    }

vemb_read_done:
    if (sample) {
        if (!lookup_ns) lookup_ns = monotonic_ns() - lookup_start;
        atomic_fetch_add_explicit(&ctx->stats->sample_count, 1,
                                  memory_order_relaxed);
        atomic_fetch_add_explicit(&ctx->stats->sample_table_lookup_ns,
                                  lookup_ns,
                                  memory_order_relaxed);
    }
    vemb_v16_timing_acc_t job_total = {0};
    vemb_v16_timing_acc_add(&job_total, monotonic_ns() - job_start);
    log_request_timing(ctx,
                       job,
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
    if (job->op == VEMB_V16_OP_VSIM_KEY_KEY) {
        atomic_fetch_add_explicit(&ctx->stats->vsim_requests, 1, memory_order_relaxed);
    } else {
        atomic_fetch_add_explicit(&ctx->stats->vemb_requests, 1, memory_order_relaxed);
    }
    vemb_v16_publish_completion(ctx, &completion, sample ? monotonic_ns() : 0, sample);
}

void vemb_v16_supernode_handle_vadd_job(vemb_v16_supernode_ctx_t *ctx,
                                        vemb_v16_vadd_job_t *vadd_job) {
    vemb_v16_storage_ctx_t *storage = ctx->storage;
    vemb_v16_tlc_t *tlc = storage->tlc;
    vemb_v16_job_base_t *job = &vadd_job->base;
    vemb_v16_completion_t completion = {
        .status = VEMB_V16_STATUS_OK,
        .op = job->op,
        .flags = job->flags,
        .req_id = job->req_id,
        .channel_index = job->channel_index,
        .channel_id = job->channel_id,
        .key_hash = job->key_hash,
        .dim = job->dim,
        .vector_bytes = job->vector_bytes,
    };
    uint64_t job_start = monotonic_ns();
    vemb_v16_timing_acc_t primary_lookup = {0};
    vemb_v16_timing_acc_t payload_local_slice = {0};
    vemb_v16_timing_acc_t payload_remote_slice = {0};
    vemb_v16_timing_acc_t compute = {0};

    if (job->op == VEMB_V16_OP_VADD_INLINE) {
        uint32_t warm_slot = 0;
        vemb_v16_vector_handle_t handle = {0};
        int put_rc = -1;
        uint64_t put_ns = 0;
        if (job->dim == tlc->vector_dim &&
            job->vector_bytes == tlc->value_size) {
            uint64_t put_start = monotonic_ns();
            put_rc = vemb_v16_tlc_put(tlc,
                                      job->key,
                                      job->key_len,
                                      job->key_hash,
                                      vadd_job->vector,
                                      job->vector_bytes,
                                      &handle,
                                      &warm_slot);
            put_ns = monotonic_ns() - put_start;
            if (put_rc == 0 && handle.bytes > 0) {
                if (vector_handle_is_local(tlc, &handle)) {
                    vemb_v16_timing_acc_add(&payload_local_slice, put_ns);
                } else {
                    vemb_v16_timing_acc_add(&payload_remote_slice, put_ns);
                }
            }
        }
        if (job->dim != tlc->vector_dim ||
            job->vector_bytes != tlc->value_size ||
            put_rc != 0) {
            completion.status = VEMB_V16_STATUS_ERR;
        } else {
            completion.vector_offset = handle.offset;
            completion.vector_bytes = handle.bytes;
            completion.region_id = handle.region_id;
            if (vemb_v16_tlc_publish_remote_meta(tlc,
                                                 job->key,
                                                 job->key_len,
                                                 job->key_hash,
                                                 &handle) != 0) {
                serverLog(LL_WARNING,
                          "vemb_v16 vadd remote meta publish failed: req_id=%u key_hash=%llu region_id=%u offset=%llu bytes=%u",
                          job->req_id,
                          (unsigned long long)job->key_hash,
                          handle.region_id,
                          (unsigned long long)handle.offset,
                          handle.bytes);
            }
        }
        atomic_fetch_add_explicit(&ctx->stats->vadd_requests, 1,
                                  memory_order_relaxed);
    } else if (job->op == VEMB_V16_OP_VSIM_INLINE) {
        uint32_t warm_slot = 0;
        vemb_v16_vector_handle_t handle = {0};
        uint64_t lookup_start = monotonic_ns();
        if (job->dim != tlc->vector_dim ||
            job->vector_bytes != tlc->value_size ||
            vemb_v16_tlc_get_handle(tlc, job->key, job->key_len,
                                    job->key_hash, &handle, &warm_slot) != 0) {
            vemb_v16_timing_acc_add(&primary_lookup, monotonic_ns() - lookup_start);
            completion.status = VEMB_V16_STATUS_NOT_FOUND;
            atomic_fetch_add_explicit(&ctx->stats->not_found, 1,
                                      memory_order_relaxed);
        } else {
            vemb_v16_timing_acc_add(&primary_lookup, monotonic_ns() - lookup_start);
            const uint8_t *stored_bytes = NULL;
            uint32_t stored_len = 0;
            uint64_t slice_start = monotonic_ns();
            if (vemb_v16_tlc_vector_slice(tlc, &handle,
                                          &stored_bytes,
                                          &stored_len) != 0 ||
                stored_len != job->vector_bytes) {
                uint64_t slice_ns = monotonic_ns() - slice_start;
                if (vector_handle_is_local(tlc, &handle))
                    vemb_v16_timing_acc_add(&payload_local_slice, slice_ns);
                else
                    vemb_v16_timing_acc_add(&payload_remote_slice, slice_ns);
                completion.status = VEMB_V16_STATUS_ERR;
            } else {
                uint64_t slice_ns = monotonic_ns() - slice_start;
                if (vector_handle_is_local(tlc, &handle))
                    vemb_v16_timing_acc_add(&payload_local_slice, slice_ns);
                else
                    vemb_v16_timing_acc_add(&payload_remote_slice, slice_ns);
                const float *stored = (const float *)(const void *)stored_bytes;
                completion.vector_offset = handle.offset;
                completion.vector_bytes = handle.bytes;
                completion.region_id = handle.region_id;
                uint64_t compute_start = monotonic_ns();
                completion.score = sve_cosine_similarity_f32(stored,
                                                             vadd_job->vector,
                                                             job->dim);
                vemb_v16_timing_acc_add(&compute, monotonic_ns() - compute_start);
            }
        }
        atomic_fetch_add_explicit(&ctx->stats->vsim_requests, 1,
                                  memory_order_relaxed);
    } else {
        completion.status = VEMB_V16_STATUS_ERR;
    }

    vemb_v16_timing_acc_t job_total = {0};
    vemb_v16_timing_acc_add(&job_total, monotonic_ns() - job_start);
    vemb_v16_timing_acc_t secondary_lookup = {0};
    vemb_v16_timing_acc_t remote_meta_lookup = {0};
    log_request_timing(ctx,
                       job,
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
    vemb_v16_publish_completion(ctx, &completion, 0, 0);
}

int vemb_v16_supernode_scratch_init(vemb_v16_supernode_scratch_t *scratch) {
    assert(scratch != NULL);
    memset(scratch, 0, sizeof(*scratch));
    scratch->vemb_jobs =
        zmalloc(sizeof(*scratch->vemb_jobs) * VEMB_V16_SUPERNODE_BATCH);
    scratch->vadd_jobs =
        zmalloc(sizeof(*scratch->vadd_jobs) * VEMB_V16_SUPERNODE_BATCH);
    if (!scratch->vemb_jobs || !scratch->vadd_jobs) {
        vemb_v16_supernode_scratch_cleanup(scratch);
        return -1;
    }
    return 0;
}

void vemb_v16_supernode_scratch_cleanup(vemb_v16_supernode_scratch_t *scratch) {
    assert(scratch != NULL);
    zfree(scratch->read_result);
    zfree(scratch->vemb_jobs);
    zfree(scratch->vadd_jobs);
    memset(scratch, 0, sizeof(*scratch));
}
