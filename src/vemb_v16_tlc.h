#ifndef __VEMB_V16_TLC_H
#define __VEMB_V16_TLC_H

#include <stddef.h>
#include <stdint.h>

#include "sve_operation.h"
#include "vemb_v16_protocol.h"

typedef enum vemb_v16_region_backend {
    VEMB_V16_TLC_REGION_LOCAL_SHM = VEMB_V16_REGION_LOCAL_SHM,
    VEMB_V16_TLC_REGION_UB = VEMB_V16_REGION_UB,
} vemb_v16_region_backend_t;

typedef struct vemb_v16_region_desc {
    uint32_t region_id;
    uint32_t supernode_id;
    uint32_t storage_class;
    uint32_t backend_type;
    uint32_t dim;
    uint32_t value_size;
    uint64_t mmap_offset;
    uint64_t region_bytes;
    char path[256];
} vemb_v16_region_desc_t;

typedef struct vemb_v16_vector_handle {
    uint32_t region_id;
    uint32_t bytes;
    uint64_t offset;
    uint64_t key_hash;
} vemb_v16_vector_handle_t;

typedef struct vemb_v16_tlc_warm_region {
    uint32_t region_id;
    uint32_t backend_type;
    void *mapped_addr;
    uint64_t region_bytes;
    uint64_t mmap_offset;
    uint32_t value_size;
} vemb_v16_tlc_warm_region_t;

typedef struct tlc_core tlc_core_t;

typedef struct vemb_v16_tlc {
    uint32_t vector_dim;
    uint32_t value_size;
    uint32_t max_vectors;
    uint32_t region_id;
    uint32_t backend_type;
    ub_address_space_t ubas;
    state_bitmap_t bitmap;
    sve_operation_stats_t sve_stats;
    sve_gather_ctx_t gather_ctx;
    tlc_core_t *core;
} vemb_v16_tlc_t;

int vemb_v16_tlc_create(vemb_v16_tlc_t **out,
                        uint32_t vector_dim,
                        uint32_t max_vectors,
                        const vemb_v16_tlc_warm_region_t *warm_region);
void vemb_v16_tlc_destroy(vemb_v16_tlc_t *tlc);

int vemb_v16_tlc_get_handle(vemb_v16_tlc_t *tlc,
                            const char *key,
                            uint32_t key_len,
                            uint64_t key_hash,
                            vemb_v16_vector_handle_t *handle,
                            uint32_t *warm_slot);
int vemb_v16_tlc_put(vemb_v16_tlc_t *tlc,
                     const char *key,
                     uint32_t key_len,
                     uint64_t key_hash,
                     const float *vector,
                     uint32_t vector_bytes,
                     vemb_v16_vector_handle_t *handle,
                     uint32_t *warm_slot);
int vemb_v16_tlc_cold_append(vemb_v16_tlc_t *tlc,
                             const char *key,
                             uint32_t key_len,
                             uint64_t key_hash,
                             const float *vector,
                             uint32_t vector_bytes);

#endif
