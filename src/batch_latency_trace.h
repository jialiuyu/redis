#ifndef __BATCH_LATENCY_TRACE_H
#define __BATCH_LATENCY_TRACE_H

#include "sds.h"
#include "supernode_protocol.h"

#include <stddef.h>
#include <stdint.h>

#define BATCH_TRACE_RING_CAPACITY 128

typedef enum batchTraceProxyPath {
    BATCH_TRACE_PROXY_PATH_UNKNOWN = 0,
    BATCH_TRACE_PROXY_PATH_BATCH = 1,
    BATCH_TRACE_PROXY_PATH_DIRECT = 2,
} batchTraceProxyPath;

typedef enum batchTraceFlushReason {
    BATCH_TRACE_FLUSH_REASON_UNKNOWN = 0,
    BATCH_TRACE_FLUSH_REASON_DIRECT = 1,
    BATCH_TRACE_FLUSH_REASON_FULL = 2,
    BATCH_TRACE_FLUSH_REASON_TIMEOUT = 3,
} batchTraceFlushReason;

typedef enum batchTraceFlushTrigger {
    BATCH_TRACE_FLUSH_TRIGGER_UNKNOWN = 0,
    BATCH_TRACE_FLUSH_TRIGGER_DIRECT = 1,
    BATCH_TRACE_FLUSH_TRIGGER_BACKGROUND = 2,
    BATCH_TRACE_FLUSH_TRIGGER_IMMEDIATE_CAPACITY = 3,
    BATCH_TRACE_FLUSH_TRIGGER_IMMEDIATE_APPEND = 4,
} batchTraceFlushTrigger;

typedef struct batch_latency_trace_proxy_meta {
    uint32_t proxy_path;
    uint32_t flush_reason;
    uint32_t flush_trigger;
    uint32_t bucket_depth;
    uint64_t bucket_gap_us;
    uint64_t bucket_ewma_gap_us;
} batch_latency_trace_proxy_meta_t;

typedef struct batch_latency_trace {
    uint64_t batch_id;
    uint32_t op_type;
    uint32_t num_requests;
    uint32_t completed_requests;
    batch_latency_trace_proxy_meta_t proxy_meta;
    uint64_t proxy_batch_wait_total_us;
    uint64_t proxy_batch_wait_max_us;
    uint64_t proxy_flush_us;
    uint64_t supernode_queue_us;
    uint64_t supernode_bitmap_lock_ns;
    uint64_t supernode_bitmap_unlock_ns;
    uint64_t supernode_vector_load_ns;
    uint64_t supernode_compute_ns;
    uint64_t supernode_response_ns;
    uint64_t proxy_result_queue_total_us;
    uint64_t proxy_result_queue_max_us;
    uint64_t request_e2e_total_us;
    uint64_t request_e2e_max_us;
} batch_latency_trace_t;

int batch_latency_trace_init(void);
void batch_latency_trace_cleanup(void);

int batch_latency_trace_begin(uint64_t batch_id,
                              uint32_t op_type,
                              uint32_t num_requests,
                              uint64_t proxy_batch_wait_total_us,
                              uint64_t proxy_batch_wait_max_us,
                              uint64_t proxy_flush_us,
                              const batch_latency_trace_proxy_meta_t *proxy_meta);

int batch_latency_trace_record_supernode(uint64_t batch_id,
                                         uint64_t supernode_queue_us,
                                         uint64_t supernode_bitmap_lock_ns,
                                         uint64_t supernode_bitmap_unlock_ns,
                                         uint64_t supernode_vector_load_ns,
                                         uint64_t supernode_compute_ns,
                                         uint64_t supernode_response_ns);

int batch_latency_trace_record_request_completion(uint64_t batch_id,
                                                  uint64_t result_queue_us,
                                                  uint64_t request_e2e_us);

sds batch_latency_trace_dump_recent(const char *title, size_t limit);

#endif /* __BATCH_LATENCY_TRACE_H */
