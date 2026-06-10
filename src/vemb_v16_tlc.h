#ifndef __VEMB_V16_TLC_H
#define __VEMB_V16_TLC_H

#include <stddef.h>
#include <stdint.h>

#include "sve_operation.h"
#include "tlc_core.h"
#include "vemb_v16_protocol.h"

#define VEMB_V16_TLC_MAX_REMOTE_META_VIEWS 16u

typedef struct vemb_v16_remote_meta_view vemb_v16_remote_meta_view_t;
typedef uint32_t (*vemb_v16_tlc_owner_resolver_fn)(uint64_t key_hash,
                                                   const char *key,
                                                   uint32_t key_len,
                                                   void *arg);

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

typedef enum vemb_v16_tlc_lookup_source {
    VEMB_V16_TLC_LOOKUP_SOURCE_NONE = 0,
    VEMB_V16_TLC_LOOKUP_SOURCE_LOCAL = 1,
    VEMB_V16_TLC_LOOKUP_SOURCE_REMOTE = 2,
} vemb_v16_tlc_lookup_source_t;

typedef struct vemb_v16_tlc_warm_region {
    uint32_t region_id;
    uint32_t backend_type;
    uint32_t home_ub_node_id;
    uint32_t is_local;
    uint32_t weight;
    void *mapped_addr;
    uint64_t region_bytes;
    uint64_t mmap_offset;
    uint32_t value_size;
    void *shared_allocator;
} vemb_v16_tlc_warm_region_t;

typedef struct vemb_v16_tlc_remote_meta_owner_view {
    uint32_t owner_id;
    vemb_v16_remote_meta_view_t *view;
} vemb_v16_tlc_remote_meta_owner_view_t;

typedef struct vemb_v16_tlc {
    uint32_t vector_dim;
    uint32_t value_size;
    uint32_t max_vectors;
    uint32_t region_id;
    uint32_t backend_type;
    uint32_t warm_region_count;
    vemb_v16_tlc_warm_region_t *warm_regions;
    state_bitmap_t bitmap;
    sve_operation_stats_t sve_stats;
    tlc_core_t *core;
    vemb_v16_remote_meta_view_t *remote_meta_view;
    uint32_t remote_meta_retry_budget;
    uint32_t remote_meta_view_count;
    vemb_v16_tlc_remote_meta_owner_view_t remote_meta_views[VEMB_V16_TLC_MAX_REMOTE_META_VIEWS];
    vemb_v16_tlc_owner_resolver_fn owner_resolver;
    void *owner_resolver_arg;
} vemb_v16_tlc_t;

int vemb_v16_tlc_create(vemb_v16_tlc_t **out,
                        uint32_t vector_dim,
                        uint32_t max_vectors,
                        const vemb_v16_tlc_warm_region_t *warm_regions,
                        uint32_t warm_region_count,
                        uint32_t local_region_weight);
void vemb_v16_tlc_destroy(vemb_v16_tlc_t *tlc);

int vemb_v16_tlc_get_handle(vemb_v16_tlc_t *tlc,
                            const char *key,
                            uint32_t key_len,
                            uint64_t key_hash,
                            vemb_v16_vector_handle_t *handle,
                            uint32_t *warm_slot);
int vemb_v16_tlc_lookup_vsim_key2(vemb_v16_tlc_t *tlc,
                                  const char *key2,
                                  uint32_t key2_len,
                                  uint64_t key2_hash,
                                  vemb_v16_vector_handle_t *handle,
                                  vemb_v16_tlc_lookup_source_t *source);
void vemb_v16_tlc_set_remote_meta_view(vemb_v16_tlc_t *tlc,
                                       vemb_v16_remote_meta_view_t *view,
                                       uint32_t retry_budget);
int vemb_v16_tlc_set_remote_meta_owner_view(vemb_v16_tlc_t *tlc,
                                            uint32_t owner_id,
                                            vemb_v16_remote_meta_view_t *view);
void vemb_v16_tlc_set_owner_resolver(vemb_v16_tlc_t *tlc,
                                     vemb_v16_tlc_owner_resolver_fn resolver,
                                     void *arg);
int vemb_v16_tlc_publish_remote_meta(vemb_v16_tlc_t *tlc,
                                     const char *key,
                                     uint32_t key_len,
                                     uint64_t key_hash,
                                     const vemb_v16_vector_handle_t *handle);
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
const vemb_v16_tlc_warm_region_t *vemb_v16_tlc_find_region(
    const vemb_v16_tlc_t *tlc,
    uint32_t region_id);
int vemb_v16_tlc_vector_slice(const vemb_v16_tlc_t *tlc,
                              const vemb_v16_vector_handle_t *handle,
                              const uint8_t **vector,
                              uint32_t *vector_bytes);
void vemb_v16_tlc_get_core_stats(vemb_v16_tlc_t *tlc,
                                 tlc_core_stats_t *stats);
uint32_t vemb_v16_tlc_get_region_stats(vemb_v16_tlc_t *tlc,
                                       tlc_core_region_stats_t *regions,
                                       uint32_t max_regions);

#endif
