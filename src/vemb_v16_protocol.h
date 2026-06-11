#ifndef __VEMB_V16_PROTOCOL_H
#define __VEMB_V16_PROTOCOL_H

#include "vemb_v16_hash.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define VEMB_V16_MAGIC 0x56313645u
#define VEMB_V16_VERSION 1u

#define VEMB_V16_UDS_PATH "/tmp/vemb_v16.sock"
#define VEMB_V16_TCP_HOST "127.0.0.1"
#define VEMB_V16_TCP_PORT 6391
#define VEMB_V16_SHM_PREFIX "vemb_v16"
#define VEMB_V16_DEFAULT_VECTOR_REGION "/vemb_v16_vectors"

#ifndef VEMB_V16_MAX_CHANNELS
#define VEMB_V16_MAX_CHANNELS 64
#endif

#define VEMB_V16_MAX_KEY_LEN 128
#define VEMB_V16_MAX_DIM 4096
#define VEMB_V16_DEFAULT_DIM 300
#define VEMB_V16_DEFAULT_MAX_VECTORS 131072
#define VEMB_V16_MAX_DESC_WARM_REGIONS 16u

#define VEMB_V16_STATUS_OK 0u
#define VEMB_V16_STATUS_NOT_FOUND 1u
#define VEMB_V16_STATUS_ERR 2u

#define VEMB_V16_REGION_LOCAL_SHM 1u
#define VEMB_V16_REGION_UB 2u

#define VEMB_V16_TRANSPORT_AERON 1u
#define VEMB_V16_TRANSPORT_TCP 2u

#define VEMB_V16_REQ_F_INLINE_VECTOR 0x01u
#define VEMB_V16_NET_F_INLINE_VECTOR 0x01u

enum vemb_v16_ctrl_op {
    VEMB_V16_CTRL_PING = 0x01,
    VEMB_V16_CTRL_ALLOC_CHANNEL = 0x20,
    VEMB_V16_CTRL_CLOSE_CHANNEL = 0x21,
    VEMB_V16_CTRL_STATS = 0x22,
    VEMB_V16_CTRL_CLOSE_ALL_CHANNELS = 0x23,
};

enum vemb_v16_net_frame_type {
    VEMB_V16_NET_HELLO = 0x01,
    VEMB_V16_NET_WELCOME = 0x02,
    VEMB_V16_NET_REQUEST = 0x03,
    VEMB_V16_NET_RESPONSE = 0x04,
    VEMB_V16_NET_CLOSE = 0x05,
    VEMB_V16_NET_STATS = 0x06,
    VEMB_V16_NET_CLOSE_CHANNEL = 0x07,
    VEMB_V16_NET_CLOSE_ALL_CHANNELS = 0x08,
    VEMB_V16_NET_CONTROL_STATUS = 0x09,
};

enum vemb_v16_data_op {
    VEMB_V16_OP_PING = 0x01,
    VEMB_V16_OP_VADD_INLINE = 0x10,
    VEMB_V16_OP_VEMB_HANDLE = 0x20,
    VEMB_V16_OP_VEMB_SUPERNODE_READ = 0x21,
    VEMB_V16_OP_VSIM_INLINE = 0x30,
    VEMB_V16_OP_VSIM_KEY_KEY = 0x31,
};

typedef struct vemb_v16_alloc_req {
    uint32_t vector_dim;
    uint32_t flags;
} vemb_v16_alloc_req_t;

typedef struct vemb_v16_channel_desc {
    uint32_t magic;
    uint32_t version;
    uint64_t channel_id;
    uint32_t channel_index;
    uint32_t vector_dim;
    uint32_t vector_stride;
    uint32_t max_vectors;
    uint32_t request_ring_slot_size;
    uint32_t response_ring_slot_size;
    uint32_t warm_region_id;
    uint32_t warm_backend_type;
    uint64_t warm_region_bytes;
    uint64_t warm_mmap_offset;
    char request_ring_name[64];
    char response_ring_name[64];
    char vector_region_name[256];
    uint32_t warm_region_count;
    uint32_t reserved0;
    struct {
        uint32_t region_id;
        uint32_t backend_type;
        uint64_t region_bytes;
        uint64_t mmap_offset;
        char path[256];
    } warm_regions[VEMB_V16_MAX_DESC_WARM_REGIONS];
} vemb_v16_channel_desc_t;

typedef struct vemb_v16_net_hdr {
    uint32_t magic;
    uint16_t version;
    uint16_t type;
    uint32_t flags;
    uint32_t payload_len;
    uint64_t channel_id;
    uint32_t req_id;
    uint32_t reserved;
} vemb_v16_net_hdr_t;

typedef struct vemb_v16_net_status {
    uint8_t status;
    uint8_t reserved0[7];
    uint64_t value;
} vemb_v16_net_status_t;

typedef struct vemb_v16_req {
    uint8_t op;
    uint8_t flags;
    uint16_t reserved0;
    uint32_t req_id;
    uint64_t channel_id;
    uint64_t key_hash;
    uint32_t key_len;
    uint32_t key2_len;
    uint64_t key2_hash;
    uint32_t dim;
    uint32_t vector_bytes;
    uint32_t reserved1;
    char key[VEMB_V16_MAX_KEY_LEN];
    char key2[VEMB_V16_MAX_KEY_LEN];
    float vector[VEMB_V16_MAX_DIM];
} vemb_v16_req_t;

typedef struct vemb_v16_resp {
    uint8_t status;
    uint8_t op;
    uint16_t flags;
    uint32_t req_id;
    uint64_t key_hash;
    uint64_t vector_offset;
    uint32_t vector_bytes;
    uint32_t dim;
    uint32_t region_id;
    float score;
} vemb_v16_resp_t;

typedef struct vemb_v16_stats {
    uint64_t total_requests;
    uint64_t vadd_requests;
    uint64_t vemb_requests;
    uint64_t vsim_requests;
    uint64_t not_found;
    uint64_t published_jobs;
    uint64_t completed_jobs;
    uint64_t active_channels;
    uint64_t proxy_request_poll;
    uint64_t proxy_completion_poll;
    uint64_t proxy_vemb_publish;
    uint64_t proxy_vadd_publish;
    uint64_t proxy_vemb_ring_full;
    uint64_t proxy_vadd_ring_full;
    uint64_t proxy_response_publish;
    uint64_t proxy_response_ring_full;
    uint64_t supernode_vemb_poll;
    uint64_t supernode_vadd_poll;
    uint64_t supernode_completion_publish;
    uint64_t supernode_completion_ring_full;
    uint64_t sample_count;
    uint64_t sample_table_lookup_ns;
    uint64_t sample_bitmap_lock_ns;
    uint64_t sample_bitmap_unlock_ns;
    uint64_t sample_vector_load_ns;
    uint64_t sample_completion_publish_ns;
    uint64_t bitmap_lock_success;
    uint64_t bitmap_lock_failure;
    uint64_t request_ring_depth;
    uint64_t response_ring_depth;
    uint64_t vemb_shard_queue_depth;
    uint64_t vadd_shard_queue_depth;
    uint64_t completion_ring_depth;
    uint64_t channel_ops;
    uint64_t warm_region_count;
    uint64_t warm_region_full_count;
    uint64_t warm_alloc_local;
    uint64_t warm_alloc_remote;
    uint64_t warm_alloc_fallback;
    uint64_t warm_alloc_cold_spill;
    uint64_t warm_alloc_fail;
    uint64_t warm_region_hash_local_pct;
    uint64_t timing_job_count;
    uint64_t timing_job_total_ns;
    uint64_t timing_job_total_max_ns;
    uint64_t timing_primary_lookup_count;
    uint64_t timing_primary_lookup_ns;
    uint64_t timing_primary_lookup_max_ns;
    uint64_t timing_secondary_lookup_count;
    uint64_t timing_secondary_lookup_ns;
    uint64_t timing_secondary_lookup_max_ns;
    uint64_t timing_remote_meta_lookup_count;
    uint64_t timing_remote_meta_lookup_ns;
    uint64_t timing_remote_meta_lookup_max_ns;
    uint64_t timing_payload_local_slice_count;
    uint64_t timing_payload_local_slice_ns;
    uint64_t timing_payload_local_slice_max_ns;
    uint64_t timing_payload_remote_slice_count;
    uint64_t timing_payload_remote_slice_ns;
    uint64_t timing_payload_remote_slice_max_ns;
    uint64_t timing_compute_count;
    uint64_t timing_compute_ns;
    uint64_t timing_compute_max_ns;
} vemb_v16_stats_t;

static inline size_t vemb_v16_req_handle_len(void) {
    return offsetof(vemb_v16_req_t, vector);
}

static inline size_t vemb_v16_req_inline_len(uint32_t vector_bytes) {
    return offsetof(vemb_v16_req_t, vector) + (size_t)vector_bytes;
}

#endif
