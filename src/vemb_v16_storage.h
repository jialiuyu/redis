#ifndef __VEMB_V16_STORAGE_H
#define __VEMB_V16_STORAGE_H

#include "vemb_v16_protocol.h"
#include "vemb_v16_tlc.h"
#include "vemb_v16_warm_provider.h"

#include <stddef.h>
#include <stdint.h>

typedef struct vemb_v16_storage_ctx {
    uint32_t vector_dim;
    uint32_t vector_stride;
    uint32_t max_vectors;
    uint32_t warm_region_id;
    uint32_t warm_backend_type;
    uint64_t warm_mmap_offset;
    uint8_t *vector_region;
    size_t vector_region_size;
    char vector_region_name[256];
    vemb_v16_warm_provider_t warm_provider;
    vemb_v16_tlc_t *tlc;
} vemb_v16_storage_ctx_t;

int vemb_v16_storage_ctx_create(vemb_v16_storage_ctx_t **out,
                                uint32_t vector_dim,
                                uint32_t vector_stride,
                                uint32_t max_vectors,
                                const char *vector_region_name,
                                uint32_t warm_region_id,
                                uint32_t warm_backend_type,
                                uint64_t warm_mmap_offset);
void vemb_v16_storage_ctx_destroy(vemb_v16_storage_ctx_t *storage);
const char *vemb_v16_storage_vector_region_name(vemb_v16_storage_ctx_t *storage);
size_t vemb_v16_storage_vector_region_size(vemb_v16_storage_ctx_t *storage);
sve_operation_stats_t *vemb_v16_storage_sve_stats(vemb_v16_storage_ctx_t *storage);
void vemb_v16_storage_fill_channel_desc(vemb_v16_storage_ctx_t *storage,
                                        vemb_v16_channel_desc_t *desc);
int vemb_v16_storage_vector_slice(vemb_v16_storage_ctx_t *storage,
                                  vemb_v16_resp_t *resp,
                                  const uint8_t **vector,
                                  uint32_t *vector_bytes);

#endif
