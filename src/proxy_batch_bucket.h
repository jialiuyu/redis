#ifndef __PROXY_BATCH_BUCKET_H
#define __PROXY_BATCH_BUCKET_H

#include "supernode_protocol.h"
#include "ring_buffer.h"
#include "vector_proxy_request.h"

#include <pthread.h>
#include <stdint.h>

typedef struct proxy_request {
    uint64_t request_id;
    uint32_t key_hash;                  /* 预计算 hash，避免重复计算 */
    uint64_t row_id;                    /* UB row id, copied from owner for VEMB packets */
    int target_supernode_id;            /* 目标超节点 */
    int target_worker_id;               /* 目标 worker */
    uint64_t submit_time_us;
    proxy_vector_request_t *owner;      /* 业务请求对象 */
} proxy_request_t;

typedef struct proxy_batch_bucket {
    proxy_request_t **requests;         /* 请求数组 */
    size_t count;                       /* 当前请求数 */
    size_t capacity;                    /* 容量 */
    int target_supernode_id;            /* 目标超节点 ID */
    int target_worker_id;               /* 目标 Worker ID */
    ring_buffer_t *rb;                  /* 目标 worker 对应的 ring buffer */
    uint64_t last_flush_time_us;        /* 上次刷新时间 */
    uint64_t last_append_time_us;       /* 上次 VEMB/VISM 到达时间 */
    uint64_t recent_gap_ewma_us;        /* 请求到达间隔 EWMA */
    pthread_mutex_t mutex;
    int mutex_initialized;
} proxy_batch_bucket_t;

proxy_batch_bucket_t *proxy_batch_bucket_create_array(size_t num_buckets);
void proxy_batch_bucket_destroy_array(proxy_batch_bucket_t *buckets, size_t num_buckets);
int proxy_batch_bucket_init(proxy_batch_bucket_t *bucket, size_t capacity,
                            int target_supernode_id, int target_worker_id,
                            uint64_t now_us);
void proxy_batch_bucket_cleanup(proxy_batch_bucket_t *bucket);
void proxy_batch_bucket_reset(proxy_batch_bucket_t *bucket, uint64_t flush_time_us);
void proxy_batch_bucket_note_arrival(proxy_batch_bucket_t *bucket, uint64_t now_us);

proxy_request_t *proxy_request_create(uint64_t request_id, uint32_t key_hash,
                                      int target_supernode_id, int target_worker_id,
                                      uint64_t submit_time_us,
                                      proxy_vector_request_t *owner);
void proxy_request_destroy(proxy_request_t *req);

int proxy_batch_bucket_append(proxy_batch_bucket_t *bucket, proxy_request_t *req);

size_t proxy_batch_bucket_packet_size(const proxy_batch_bucket_t *bucket);
int proxy_batch_bucket_fill_packet(const proxy_batch_bucket_t *bucket,
                                   batch_packet_t *packet,
                                   size_t packet_size,
                                   uint64_t timestamp_us,
                                   uint64_t batch_id);

#endif /* __PROXY_BATCH_BUCKET_H */
