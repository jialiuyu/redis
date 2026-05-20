#ifndef __VEMB_V16_PROTOCOL_H
#define __VEMB_V16_PROTOCOL_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define VEMB_V16_MAGIC 0x56313645u
#define VEMB_V16_VERSION 1u

#define VEMB_V16_UDS_PATH "/tmp/vemb_v16.sock"
#define VEMB_V16_SHM_PREFIX "vemb_v16"

#define VEMB_V16_MAX_CHANNELS 64
#define VEMB_V16_MAX_KEY_LEN 128
#define VEMB_V16_MAX_DIM 4096
#define VEMB_V16_DEFAULT_DIM 300
#define VEMB_V16_DEFAULT_MAX_VECTORS 131072

#define VEMB_V16_STATUS_OK 0u
#define VEMB_V16_STATUS_NOT_FOUND 1u
#define VEMB_V16_STATUS_ERR 2u

enum vemb_v16_ctrl_op {
    VEMB_V16_CTRL_PING = 0x01,
    VEMB_V16_CTRL_ALLOC_CHANNEL = 0x20,
    VEMB_V16_CTRL_CLOSE_CHANNEL = 0x21,
    VEMB_V16_CTRL_STATS = 0x22,
};

enum vemb_v16_data_op {
    VEMB_V16_OP_PING = 0x01,
    VEMB_V16_OP_VADD_INLINE = 0x10,
    VEMB_V16_OP_VEMB_HANDLE = 0x20,
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
    char request_ring_name[64];
    char response_ring_name[64];
    char vector_region_name[64];
} vemb_v16_channel_desc_t;

typedef struct vemb_v16_req {
    uint8_t op;
    uint8_t flags;
    uint16_t reserved0;
    uint32_t req_id;
    uint64_t channel_id;
    uint64_t key_hash;
    uint32_t key_len;
    uint32_t dim;
    uint32_t vector_bytes;
    uint32_t reserved1;
    char key[VEMB_V16_MAX_KEY_LEN];
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
    uint64_t reserved;
} vemb_v16_resp_t;

typedef struct vemb_v16_stats {
    uint64_t total_requests;
    uint64_t vadd_requests;
    uint64_t vemb_requests;
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
    uint64_t vemb_job_ring_depth;
    uint64_t vadd_job_ring_depth;
    uint64_t completion_ring_depth;
    uint64_t channel_ops;
} vemb_v16_stats_t;

static inline uint32_t vemb_v16_murmur3(const char *key, size_t len) {
    const uint32_t c1 = 0xcc9e2d51u;
    const uint32_t c2 = 0x1b873593u;
    const uint32_t seed = 0x5bd1e995u;

    uint32_t h = seed;
    const uint8_t *data = (const uint8_t *)key;
    const int nblocks = (int)(len / 4);
    const uint32_t *blocks = (const uint32_t *)(const void *)(data + nblocks * 4);

    for (int i = -nblocks; i; i++) {
        uint32_t k = blocks[i];
        k *= c1;
        k = (k << 15) | (k >> 17);
        k *= c2;

        h ^= k;
        h = (h << 13) | (h >> 19);
        h = h * 5 + 0xe6546b64u;
    }

    const uint8_t *tail = data + nblocks * 4;
    uint32_t k = 0;
    switch (len & 3u) {
    case 3: k ^= (uint32_t)tail[2] << 16; /* fall through */
    case 2: k ^= (uint32_t)tail[1] << 8;  /* fall through */
    case 1:
        k ^= (uint32_t)tail[0];
        k *= c1;
        k = (k << 15) | (k >> 17);
        k *= c2;
        h ^= k;
        break;
    default:
        break;
    }

    h ^= (uint32_t)len;
    h ^= h >> 16;
    h *= 0x85ebca6bu;
    h ^= h >> 13;
    h *= 0xc2b2ae35u;
    h ^= h >> 16;
    return h;
}

static inline size_t vemb_v16_req_handle_len(void) {
    return offsetof(vemb_v16_req_t, vector);
}

static inline size_t vemb_v16_req_inline_len(uint32_t vector_bytes) {
    return offsetof(vemb_v16_req_t, vector) + (size_t)vector_bytes;
}

#endif
