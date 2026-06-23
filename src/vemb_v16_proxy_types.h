#ifndef __VEMB_V16_PROXY_TYPES_H
#define __VEMB_V16_PROXY_TYPES_H

#include "vemb_v16_proxy_internal.h"
#include "vemb_v16_aeron_ring.h"
#include "vemb_v16_client_ring.h"
#include "vemb_v16_storage.h"
#include "vemb_v16_supernode.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

typedef struct vemb_v16_proxy_io_worker {
    uint32_t worker_id;
    struct vemb_v16_proxy *proxy;
    pthread_t thread;
#ifdef __linux__
    int notify_fd;
#endif
} vemb_v16_proxy_io_worker_t;

typedef struct vemb_v16_supernode_pool_worker {
    uint32_t worker_id;
    struct vemb_v16_proxy *proxy;
    pthread_t thread;
#ifdef __linux__
    atomic_int job_notify_armed;
    int notify_fd;
#endif
} vemb_v16_supernode_pool_worker_t;

typedef struct vemb_v16_shard_queue {
    vemb_v16_aeron_ring_t ring;
    void *slots;
} vemb_v16_shard_queue_t;

struct vemb_v16_channel {
    uint64_t channel_id;
    atomic_uint_fast64_t slot_channel_id;
    uint32_t index;
    atomic_int active;
    uint32_t transport_type;
    int net_fd;
    atomic_int proxy_io_registered;
    atomic_uint_fast32_t proxy_io_state;
    atomic_uint_fast32_t supernode_state;
    atomic_int completion_notify_armed;
    int tcp_backpressure_enabled;
    uint8_t *tcp_response_backlog;
    size_t tcp_response_backlog_cap;
    size_t tcp_response_backlog_len;
    size_t tcp_response_backlog_sent;
    vemb_v16_aeron_ring_t completion_ring;
    void *completion_slots;
    char request_ring_name[64];
    char response_ring_name[64];
    vemb_v16_client_ring_t *request_ring;
    vemb_v16_client_ring_t *response_ring;
    size_t request_ring_bytes;
    size_t response_ring_bytes;
    vemb_v16_supernode_ctx_t supernode_ctx;
    struct vemb_v16_proxy *proxy;
    vemb_v16_channel_counters_t stats;
};

struct vemb_v16_proxy {
    uint32_t vector_dim;
    uint32_t vector_stride;
    uint32_t max_vectors;
    vemb_v16_storage_ctx_t *storage;
    vemb_v16_channel_t channels[VEMB_V16_MAX_CHANNELS];
    atomic_uint_fast64_t next_channel_id;
    atomic_uint_fast32_t next_channel_index;
    atomic_int running;
    int listen_fd;
    char uds_path[108];
    uint32_t request_ring_slot_size;
    uint32_t response_ring_slot_size;
    int uds_enabled;
    uint32_t proxy_io_worker_count;
    int proxy_io_pool_started;
    vemb_v16_proxy_io_worker_t proxy_io_workers[VEMB_V16_MAX_CHANNELS];
    uint32_t supernode_worker_count;
    int supernode_pool_started;
    vemb_v16_supernode_pool_worker_t supernode_workers[VEMB_V16_MAX_CHANNELS];
    uint32_t vemb_shard_proxy_count;
    uint32_t vemb_shard_supernode_count;
    vemb_v16_shard_queue_t *vemb_shard_queues;
    vemb_v16_shard_queue_t *vadd_shard_queues;
    pthread_mutex_t stats_lock;
    vemb_v16_stats_t closed_stats;
    int inject_pipe_rd;   /* read by proxy thread to receive injected fds */
    int inject_pipe_wr;   /* written by Redis main thread to inject fds  */
};

#endif
