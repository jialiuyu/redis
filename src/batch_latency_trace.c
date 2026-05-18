#include "batch_latency_trace.h"

#include "macro.h"
#include "server.h"
#include "zmalloc.h"

#include <pthread.h>
#include <string.h>

#ifndef C_OK
#define C_OK 0
#endif
#ifndef C_ERR
#define C_ERR -1
#endif

typedef struct batch_latency_trace_registry {
    batch_latency_trace_t traces[BATCH_TRACE_RING_CAPACITY];
    size_t next_slot;
    size_t count;
    size_t refcount;
    pthread_mutex_t lock;
    int initialized;
} batch_latency_trace_registry_t;

static batch_latency_trace_registry_t g_batch_traces = {0};

static const char *batch_latency_trace_proxy_path_name(uint32_t path) {
    switch (path) {
    case BATCH_TRACE_PROXY_PATH_BATCH:
        return "batch";
    case BATCH_TRACE_PROXY_PATH_DIRECT:
        return "direct";
    default:
        return "unknown";
    }
}

static const char *batch_latency_trace_flush_reason_name(uint32_t reason) {
    switch (reason) {
    case BATCH_TRACE_FLUSH_REASON_DIRECT:
        return "direct";
    case BATCH_TRACE_FLUSH_REASON_FULL:
        return "limit";
    case BATCH_TRACE_FLUSH_REASON_TIMEOUT:
        return "timeout";
    default:
        return "unknown";
    }
}

static const char *batch_latency_trace_flush_trigger_name(uint32_t trigger) {
    switch (trigger) {
    case BATCH_TRACE_FLUSH_TRIGGER_DIRECT:
        return "direct";
    case BATCH_TRACE_FLUSH_TRIGGER_BACKGROUND:
        return "background";
    case BATCH_TRACE_FLUSH_TRIGGER_IMMEDIATE_CAPACITY:
        return "capacity";
    case BATCH_TRACE_FLUSH_TRIGGER_IMMEDIATE_APPEND:
        return "append";
    default:
        return "unknown";
    }
}

static void batch_latency_trace_log_completed(const batch_latency_trace_t *trace) {
    RETURN_IF(!trace);

    double avg_wait = trace->num_requests > 0 ?
        (double)trace->proxy_batch_wait_total_us / (double)trace->num_requests : 0.0;
    double avg_result_queue = trace->completed_requests > 0 ?
        (double)trace->proxy_result_queue_total_us / (double)trace->completed_requests : 0.0;
    double avg_e2e = trace->completed_requests > 0 ?
        (double)trace->request_e2e_total_us / (double)trace->completed_requests : 0.0;

    serverLog(LL_NOTICE,
              "batch-trace batch=%llu op=%u req=%u done=%u proxy_wait_avg_us=%.1f proxy_wait_max_us=%llu proxy_flush_us=%llu "
              "queue_us=%llu bitmap_lock_ns=%llu bitmap_ublock_ns=%llu vector_load_ns=%llu compute_ns=%llu response_ns=%llu result_queue_avg_us=%.1f "
              "result_queue_max_us=%llu e2e_avg_us=%.1f e2e_max_us=%llu "
              "path=%s flush_reason=%s flush_trigger=%s bucket_depth=%u gap_us=%llu ewma_gap_us=%llu",
              (unsigned long long)trace->batch_id,
              trace->op_type,
              trace->num_requests,
              trace->completed_requests,
              avg_wait,
              (unsigned long long)trace->proxy_batch_wait_max_us,
              (unsigned long long)trace->proxy_flush_us,
              (unsigned long long)trace->supernode_queue_us,
              (unsigned long long)trace->supernode_bitmap_lock_ns,
              (unsigned long long)trace->supernode_bitmap_unlock_ns,
              (unsigned long long)trace->supernode_vector_load_ns,
              (unsigned long long)trace->supernode_compute_ns,
              (unsigned long long)trace->supernode_response_ns,
              avg_result_queue,
              (unsigned long long)trace->proxy_result_queue_max_us,
              avg_e2e,
              (unsigned long long)trace->request_e2e_max_us,
              batch_latency_trace_proxy_path_name(trace->proxy_meta.proxy_path),
              batch_latency_trace_flush_reason_name(trace->proxy_meta.flush_reason),
              batch_latency_trace_flush_trigger_name(trace->proxy_meta.flush_trigger),
              trace->proxy_meta.bucket_depth,
              (unsigned long long)trace->proxy_meta.bucket_gap_us,
              (unsigned long long)trace->proxy_meta.bucket_ewma_gap_us);
}

static batch_latency_trace_t *batch_latency_trace_find_locked(uint64_t batch_id) {
    for (size_t i = 0; i < g_batch_traces.count; i++) {
        batch_latency_trace_t *trace = &g_batch_traces.traces[i];
        if (trace->batch_id == batch_id) return trace;
    }
    return NULL;
}

static batch_latency_trace_t *batch_latency_trace_alloc_locked(uint64_t batch_id) {
    batch_latency_trace_t *trace = batch_latency_trace_find_locked(batch_id);
    if (trace) {
        memset(trace, 0, sizeof(*trace));
        trace->batch_id = batch_id;
        return trace;
    }

    size_t slot = g_batch_traces.next_slot;
    trace = &g_batch_traces.traces[slot];
    memset(trace, 0, sizeof(*trace));
    trace->batch_id = batch_id;

    g_batch_traces.next_slot = (slot + 1) % BATCH_TRACE_RING_CAPACITY;
    if (g_batch_traces.count < BATCH_TRACE_RING_CAPACITY) {
        g_batch_traces.count++;
    }
    return trace;
}

int batch_latency_trace_init(void) {
    if (g_batch_traces.initialized) {
        g_batch_traces.refcount++;
        return C_OK;
    }
    if (pthread_mutex_init(&g_batch_traces.lock, NULL) != 0) {
        return C_ERR;
    }
    memset(g_batch_traces.traces, 0, sizeof(g_batch_traces.traces));
    g_batch_traces.next_slot = 0;
    g_batch_traces.count = 0;
    g_batch_traces.refcount = 1;
    g_batch_traces.initialized = 1;
    return C_OK;
}

void batch_latency_trace_cleanup(void) {
    if (!g_batch_traces.initialized) return;
    if (g_batch_traces.refcount > 1) {
        g_batch_traces.refcount--;
        return;
    }
    pthread_mutex_destroy(&g_batch_traces.lock);
    memset(&g_batch_traces, 0, sizeof(g_batch_traces));
}

int batch_latency_trace_begin(uint64_t batch_id,
                              uint32_t op_type,
                              uint32_t num_requests,
                              uint64_t proxy_batch_wait_total_us,
                              uint64_t proxy_batch_wait_max_us,
                              uint64_t proxy_flush_us,
                              const batch_latency_trace_proxy_meta_t *proxy_meta) {
    RETURN_IF(batch_id == 0 || num_requests == 0, C_ERR);
    if (!g_batch_traces.initialized && batch_latency_trace_init() != C_OK) return C_ERR;

    pthread_mutex_lock(&g_batch_traces.lock);
    batch_latency_trace_t *trace = batch_latency_trace_alloc_locked(batch_id);
    trace->op_type = op_type;
    trace->num_requests = num_requests;
    trace->proxy_batch_wait_total_us = proxy_batch_wait_total_us;
    trace->proxy_batch_wait_max_us = proxy_batch_wait_max_us;
    trace->proxy_flush_us = proxy_flush_us;
    if (proxy_meta) {
        trace->proxy_meta = *proxy_meta;
    } else {
        memset(&trace->proxy_meta, 0, sizeof(trace->proxy_meta));
    }
    pthread_mutex_unlock(&g_batch_traces.lock);
    return C_OK;
}

int batch_latency_trace_record_supernode(uint64_t batch_id,
                                         uint64_t supernode_queue_us,
                                         uint64_t supernode_bitmap_lock_ns,
                                         uint64_t supernode_bitmap_unlock_ns,
                                         uint64_t supernode_vector_load_ns,
                                         uint64_t supernode_compute_ns,
                                         uint64_t supernode_response_ns) {
    if (!g_batch_traces.initialized) return C_ERR;

    pthread_mutex_lock(&g_batch_traces.lock);
    batch_latency_trace_t *trace = batch_latency_trace_find_locked(batch_id);
    if (!trace) {
        pthread_mutex_unlock(&g_batch_traces.lock);
        return C_ERR;
    }
    trace->supernode_queue_us = supernode_queue_us;
    trace->supernode_bitmap_lock_ns = supernode_bitmap_lock_ns;
    trace->supernode_bitmap_unlock_ns = supernode_bitmap_unlock_ns;
    trace->supernode_vector_load_ns = supernode_vector_load_ns;
    trace->supernode_compute_ns = supernode_compute_ns;
    trace->supernode_response_ns = supernode_response_ns;
    pthread_mutex_unlock(&g_batch_traces.lock);
    return C_OK;
}

int batch_latency_trace_record_request_completion(uint64_t batch_id,
                                                  uint64_t result_queue_us,
                                                  uint64_t request_e2e_us) {
    if (!g_batch_traces.initialized) return C_ERR;

    batch_latency_trace_t snapshot = {0};
    int should_log = 0;

    pthread_mutex_lock(&g_batch_traces.lock);
    batch_latency_trace_t *trace = batch_latency_trace_find_locked(batch_id);
    if (!trace) {
        pthread_mutex_unlock(&g_batch_traces.lock);
        return C_ERR;
    }

    trace->completed_requests++;
    trace->proxy_result_queue_total_us += result_queue_us;
    trace->request_e2e_total_us += request_e2e_us;
    if (result_queue_us > trace->proxy_result_queue_max_us) {
        trace->proxy_result_queue_max_us = result_queue_us;
    }
    if (request_e2e_us > trace->request_e2e_max_us) {
        trace->request_e2e_max_us = request_e2e_us;
    }
    if (trace->completed_requests >= trace->num_requests) {
        snapshot = *trace;
        should_log = 1;
    }
    pthread_mutex_unlock(&g_batch_traces.lock);

    if (should_log) {
        batch_latency_trace_log_completed(&snapshot);
    }
    return C_OK;
}

sds batch_latency_trace_dump_recent(const char *title, size_t limit) {
    sds out = sdsempty();
    if (!title) title = "Batch latency traces";

    out = sdscatprintf(out, "%s:\n", title);
    if (!g_batch_traces.initialized) {
        return sdscat(out, "  Not initialized\n");
    }

    pthread_mutex_lock(&g_batch_traces.lock);
    size_t available = g_batch_traces.count;
    if (available == 0) {
        pthread_mutex_unlock(&g_batch_traces.lock);
        return sdscat(out, "  No traces\n");
    }

    if (limit == 0 || limit > available) limit = available;

    for (size_t i = 0; i < limit; i++) {
        size_t idx = (g_batch_traces.next_slot + BATCH_TRACE_RING_CAPACITY - 1 - i) %
                     BATCH_TRACE_RING_CAPACITY;
        batch_latency_trace_t *trace = &g_batch_traces.traces[idx];
        if (trace->batch_id == 0) continue;

        double avg_wait = trace->num_requests > 0 ?
            (double)trace->proxy_batch_wait_total_us / (double)trace->num_requests : 0.0;
        double avg_result_queue = trace->completed_requests > 0 ?
            (double)trace->proxy_result_queue_total_us / (double)trace->completed_requests : 0.0;
        double avg_e2e = trace->completed_requests > 0 ?
            (double)trace->request_e2e_total_us / (double)trace->completed_requests : 0.0;

        out = sdscatprintf(
            out,
            "  batch=%llu op=%u req=%u done=%u proxy_wait_avg_us=%.1f proxy_wait_max_us=%llu proxy_flush_us=%llu "
            "queue_us=%llu bitmap_lock_ns=%llu bitmap_ublock_ns=%llu vector_load_ns=%llu compute_ns=%llu response_ns=%llu result_queue_avg_us=%.1f "
            "result_queue_max_us=%llu e2e_avg_us=%.1f e2e_max_us=%llu "
            "path=%s flush_reason=%s flush_trigger=%s bucket_depth=%u gap_us=%llu ewma_gap_us=%llu\n",
            (unsigned long long)trace->batch_id,
            trace->op_type,
            trace->num_requests,
            trace->completed_requests,
            avg_wait,
            (unsigned long long)trace->proxy_batch_wait_max_us,
            (unsigned long long)trace->proxy_flush_us,
            (unsigned long long)trace->supernode_queue_us,
            (unsigned long long)trace->supernode_bitmap_lock_ns,
            (unsigned long long)trace->supernode_bitmap_unlock_ns,
            (unsigned long long)trace->supernode_vector_load_ns,
            (unsigned long long)trace->supernode_compute_ns,
            (unsigned long long)trace->supernode_response_ns,
            avg_result_queue,
            (unsigned long long)trace->proxy_result_queue_max_us,
            avg_e2e,
            (unsigned long long)trace->request_e2e_max_us,
            batch_latency_trace_proxy_path_name(trace->proxy_meta.proxy_path),
            batch_latency_trace_flush_reason_name(trace->proxy_meta.flush_reason),
            batch_latency_trace_flush_trigger_name(trace->proxy_meta.flush_trigger),
            trace->proxy_meta.bucket_depth,
            (unsigned long long)trace->proxy_meta.bucket_gap_us,
            (unsigned long long)trace->proxy_meta.bucket_ewma_gap_us);
    }
    pthread_mutex_unlock(&g_batch_traces.lock);
    return out;
}
