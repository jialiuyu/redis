#ifndef __PROXY_BATCH_BUCKET_H
#define __PROXY_BATCH_BUCKET_H

#include "supernode_protocol.h"

#include <pthread.h>
#include <stdint.h>

typedef struct proxy_request {
    uint64_t request_id;
    uint32_t key_hash;                  /* 预计算 hash，避免重复计算 */
    int target_supernode_id;            /* 目标超节点 */
    int target_worker_id;               /* 目标 worker */
    uint64_t submit_time_us;
    void *client_context;               /* 客户端上下文 */
    int completed;
    int error_code;
    float *result_vector;               /* 结果向量 */
    size_t vector_dim;
} proxy_request_t;

typedef struct proxy_batch_bucket {
    proxy_request_t **requests;         /* 请求数组 */
    size_t count;                       /* 当前请求数 */
    size_t capacity;                    /* 容量 */
    int target_supernode_id;            /* 目标超节点 ID */
    int target_worker_id;               /* 目标 Worker ID */
    uint64_t last_flush_time_us;        /* 上次刷新时间 */
    pthread_mutex_t mutex;
    int mutex_initialized;
} proxy_batch_bucket_t;

proxy_batch_bucket_t *proxy_batch_bucket_create_array(size_t num_buckets);
void proxy_batch_bucket_destroy_array(proxy_batch_bucket_t *buckets, size_t num_buckets);
int proxy_batch_bucket_init(proxy_batch_bucket_t *bucket, size_t capacity,
                            int target_supernode_id, int target_worker_id,
                            uint64_t now_us);
void proxy_batch_bucket_cleanup(proxy_batch_bucket_t *bucket);

void proxy_batch_bucket_lock(proxy_batch_bucket_t *bucket);
void proxy_batch_bucket_unlock(proxy_batch_bucket_t *bucket);

size_t proxy_batch_bucket_count(const proxy_batch_bucket_t *bucket);
size_t proxy_batch_bucket_capacity(const proxy_batch_bucket_t *bucket);
uint64_t proxy_batch_bucket_age_us(const proxy_batch_bucket_t *bucket, uint64_t now_us);
void proxy_batch_bucket_reset(proxy_batch_bucket_t *bucket, uint64_t flush_time_us);

proxy_request_t *proxy_request_create(uint64_t request_id, uint32_t key_hash,
                                      int target_supernode_id, int target_worker_id,
                                      uint64_t submit_time_us, void *client_context,
                                      float *result_buffer, size_t vector_dim);
void proxy_request_destroy(proxy_request_t *req);

int proxy_batch_bucket_append(proxy_batch_bucket_t *bucket, proxy_request_t *req);

size_t proxy_batch_bucket_packet_size(const proxy_batch_bucket_t *bucket);
int proxy_batch_bucket_fill_packet(const proxy_batch_bucket_t *bucket,
                                   batch_packet_t *packet,
                                   size_t packet_size,
                                   uint64_t timestamp_us,
                                   uint64_t batch_id);

int proxy_batch_bucket_target_supernode_id(const proxy_batch_bucket_t *bucket);

#endif /* __PROXY_BATCH_BUCKET_H */
