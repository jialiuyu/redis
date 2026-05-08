#include "macro.h"
#include "proxy_batch_bucket.h"
#include "server.h"

proxy_batch_bucket_t *proxy_batch_bucket_create_array(size_t num_buckets) {
    RETURN_IF(num_buckets == 0, NULL);
    return zcalloc(sizeof(proxy_batch_bucket_t) * num_buckets);
}

void proxy_batch_bucket_destroy_array(proxy_batch_bucket_t *buckets, size_t num_buckets) {
    RETURN_IF(!buckets);

    for (size_t i = 0; i < num_buckets; i++) {
        proxy_batch_bucket_cleanup(&buckets[i]);
    }
    zfree(buckets);
}

int proxy_batch_bucket_init(proxy_batch_bucket_t *bucket, size_t capacity,
                            int target_supernode_id, int target_worker_id,
                            uint64_t now_us) {
    RETURN_IF(!bucket || capacity == 0, C_ERR);

    bucket->requests = zcalloc(sizeof(proxy_request_t *) * capacity);
    RETURN_IF(!bucket->requests, C_ERR);

    bucket->count = 0;
    bucket->capacity = capacity;
    bucket->target_supernode_id = target_supernode_id;
    bucket->target_worker_id = target_worker_id;
    bucket->last_flush_time_us = now_us;
    if (pthread_mutex_init(&bucket->mutex, NULL) != 0) {
        zfree(bucket->requests);
        bucket->requests = NULL;
        return C_ERR;
    }
    bucket->mutex_initialized = 1;
    return C_OK;
}

void proxy_batch_bucket_cleanup(proxy_batch_bucket_t *bucket) {
    RETURN_IF(!bucket);

    if (bucket->requests) {
        for (size_t i = 0; i < bucket->count; i++) {
            proxy_request_destroy(bucket->requests[i]);
        }
        zfree(bucket->requests);
        bucket->requests = NULL;
    }
    bucket->count = 0;
    bucket->capacity = 0;
    if (bucket->mutex_initialized) {
        pthread_mutex_destroy(&bucket->mutex);
        bucket->mutex_initialized = 0;
    }
}

void proxy_batch_bucket_lock(proxy_batch_bucket_t *bucket) {
    RETURN_IF(!bucket || !bucket->mutex_initialized);
    pthread_mutex_lock(&bucket->mutex);
}

void proxy_batch_bucket_unlock(proxy_batch_bucket_t *bucket) {
    RETURN_IF(!bucket || !bucket->mutex_initialized);
    pthread_mutex_unlock(&bucket->mutex);
}

size_t proxy_batch_bucket_count(const proxy_batch_bucket_t *bucket) {
    RETURN_IF(!bucket, 0);
    return bucket->count;
}

size_t proxy_batch_bucket_capacity(const proxy_batch_bucket_t *bucket) {
    RETURN_IF(!bucket, 0);
    return bucket->capacity;
}

uint64_t proxy_batch_bucket_age_us(const proxy_batch_bucket_t *bucket, uint64_t now_us) {
    RETURN_IF(!bucket || now_us < bucket->last_flush_time_us, 0);
    return now_us - bucket->last_flush_time_us;
}

void proxy_batch_bucket_reset(proxy_batch_bucket_t *bucket, uint64_t flush_time_us) {
    RETURN_IF(!bucket);

    for (size_t i = 0; i < bucket->count; i++) {
        proxy_request_destroy(bucket->requests[i]);
        bucket->requests[i] = NULL;
    }

    bucket->count = 0;
    bucket->last_flush_time_us = flush_time_us;
}

proxy_request_t *proxy_request_create(uint64_t request_id, uint32_t key_hash,
                                      int target_supernode_id, int target_worker_id,
                                      uint64_t submit_time_us, void *client_context,
                                      float *result_buffer, size_t vector_dim) {
    proxy_request_t *req = zmalloc(sizeof(proxy_request_t));
    RETURN_IF(!req, NULL);

    req->request_id = request_id;
    req->key_hash = key_hash;
    req->target_supernode_id = target_supernode_id;
    req->target_worker_id = target_worker_id;
    req->submit_time_us = submit_time_us;
    req->client_context = client_context;
    req->completed = 0;
    req->error_code = 0;
    req->result_vector = result_buffer;
    req->vector_dim = vector_dim;
    return req;
}

void proxy_request_destroy(proxy_request_t *req) {
    RETURN_IF(!req);
    zfree(req);
}

int proxy_batch_bucket_append(proxy_batch_bucket_t *bucket, proxy_request_t *req) {
    RETURN_IF(!bucket || !req || !bucket->requests, C_ERR);
    RETURN_IF(bucket->count >= bucket->capacity, C_ERR);

    bucket->requests[bucket->count++] = req;
    if (bucket->count == 1) {
        bucket->last_flush_time_us = req->submit_time_us;
    }
    return C_OK;
}

size_t proxy_batch_bucket_packet_size(const proxy_batch_bucket_t *bucket) {
    RETURN_IF(!bucket || bucket->count == 0, 0);
    return sizeof(batch_packet_t) + bucket->count * sizeof(((batch_packet_t *)0)->requests[0]);
}

int proxy_batch_bucket_fill_packet(const proxy_batch_bucket_t *bucket,
                                   batch_packet_t *packet,
                                   size_t packet_size,
                                   uint64_t timestamp_us,
                                   uint64_t batch_id) {
    RETURN_IF(!bucket || !packet || packet_size < proxy_batch_bucket_packet_size(bucket), C_ERR);

    packet->magic = BATCH_PACKET_MAGIC;
    packet->packet_size = packet_size;
    packet->num_requests = bucket->count;
    packet->supernode_id = bucket->target_supernode_id;
    packet->worker_id = bucket->target_worker_id;
    packet->timestamp_us = timestamp_us;
    packet->batch_id = batch_id;

    for (size_t i = 0; i < bucket->count; i++) {
        proxy_request_t *req = bucket->requests[i];
        packet->requests[i].request_id = req->request_id;
        packet->requests[i].key_hash = req->key_hash;
    }

    return C_OK;
}

int proxy_batch_bucket_target_supernode_id(const proxy_batch_bucket_t *bucket) {
    RETURN_IF(!bucket, -1);
    return bucket->target_supernode_id;
}
