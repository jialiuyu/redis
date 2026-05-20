#ifndef __VEMB_V16_DATAPLANE_H
#define __VEMB_V16_DATAPLANE_H

#include "vemb_v16_protocol.h"

#include <stdint.h>

typedef struct vemb_v16_job_base {
    uint8_t op;
    uint8_t flags;
    uint16_t reserved0;
    uint32_t req_id;
    uint32_t channel_index;
    uint32_t key_len;
    uint64_t channel_id;
    uint64_t key_hash;
    uint32_t dim;
    uint32_t vector_bytes;
    uint64_t vector_offset_or_staging_offset;
    char key[VEMB_V16_MAX_KEY_LEN];
} vemb_v16_job_base_t;

typedef struct vemb_v16_vemb_job {
    vemb_v16_job_base_t base;
} vemb_v16_vemb_job_t;

typedef struct vemb_v16_vadd_job {
    vemb_v16_job_base_t base;
    float vector[VEMB_V16_MAX_DIM];
} vemb_v16_vadd_job_t;

typedef struct vemb_v16_completion {
    uint8_t status;
    uint8_t op;
    uint16_t flags;
    uint32_t req_id;
    uint32_t channel_index;
    uint32_t dim;
    uint64_t channel_id;
    uint64_t key_hash;
    uint64_t vector_offset;
    uint32_t vector_bytes;
    uint32_t reserved;
} vemb_v16_completion_t;

#endif
