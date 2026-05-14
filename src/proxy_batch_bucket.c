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
    bucket->rb = NULL;
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
                                      uint64_t submit_time_us,
                                      proxy_vector_request_t *owner) {
    proxy_request_t *req = zmalloc(sizeof(proxy_request_t));
    RETURN_IF(!req, NULL);

    req->request_id = request_id;
    req->key_hash = key_hash;
    req->target_supernode_id = target_supernode_id;
    req->target_worker_id = target_worker_id;
    req->submit_time_us = submit_time_us;
    req->owner = owner;
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
    if (bucket->count == 1 &&
        bucket->requests[0] &&
        bucket->requests[0]->owner &&
        bucket->requests[0]->owner->op_type == PROXY_VECTOR_OP_VSIM) {
        proxy_vector_request_t *owner = bucket->requests[0]->owner;
        return sizeof(batch_vsim_packet_t) +
               sizeof(float) * owner->query_dim +
               sizeof(uint64_t) * owner->candidate_count;
    }
    return sizeof(batch_packet_t) + bucket->count * sizeof(((batch_packet_t *)0)->requests[0]);
}

int proxy_batch_bucket_fill_packet(const proxy_batch_bucket_t *bucket,
                                   batch_packet_t *packet,
                                   size_t packet_size,
                                   uint64_t timestamp_us,
                                   uint64_t batch_id) {
    RETURN_IF(!bucket || !packet || packet_size < proxy_batch_bucket_packet_size(bucket), C_ERR);

    if (bucket->count == 1 &&
        bucket->requests[0] &&
        bucket->requests[0]->owner &&
        bucket->requests[0]->owner->op_type == PROXY_VECTOR_OP_VSIM) {
        proxy_vector_request_t *owner = bucket->requests[0]->owner;
        batch_vsim_packet_t *vsim = (batch_vsim_packet_t *)packet;
        vsim->hdr.magic = BATCH_PACKET_MAGIC;
        vsim->hdr.packet_size = (uint32_t)packet_size;
        vsim->hdr.num_requests = 1;
        vsim->hdr.op_type = BATCH_PACKET_OP_VSIM;
        vsim->hdr.supernode_id = bucket->target_supernode_id;
        vsim->hdr.worker_id = bucket->target_worker_id;
        vsim->hdr.timestamp_us = timestamp_us;
        vsim->hdr.batch_id = batch_id;
        vsim->flags = owner->withscores ? 1u : 0u;
        vsim->request_id = owner->request_id;
        vsim->query_dim = (uint32_t)owner->query_dim;
        vsim->requested_count = (uint32_t)owner->requested_count;
        vsim->candidate_count = (uint32_t)owner->candidate_count;
        vsim->reserved = 0;
        memcpy(vsim->payload, owner->query_vector, sizeof(float) * owner->query_dim);
        memcpy((uint8_t *)(vsim->payload + owner->query_dim),
               owner->candidate_rows,
               sizeof(uint64_t) * owner->candidate_count);
        return C_OK;
    }

    packet->hdr.magic = BATCH_PACKET_MAGIC;
    packet->hdr.packet_size = packet_size;
    packet->hdr.num_requests = bucket->count;
    packet->hdr.op_type = BATCH_PACKET_OP_VEMB;
    packet->hdr.supernode_id = bucket->target_supernode_id;
    packet->hdr.worker_id = bucket->target_worker_id;
    packet->hdr.timestamp_us = timestamp_us;
    packet->hdr.batch_id = batch_id;

    for (size_t i = 0; i < bucket->count; i++) {
        proxy_request_t *req = bucket->requests[i];
        packet->requests[i].request_id = req->request_id;
        packet->requests[i].row_id = req->owner ? req->owner->row_id : 0;
    }

    return C_OK;
}
