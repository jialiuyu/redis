#ifndef __VEMB_V16_STORAGE_H
#define __VEMB_V16_STORAGE_H

#include "vemb_v16_protocol.h"
#include "vemb_v16_remote_meta.h"
#include "vemb_v16_shared_allocator.h"
#include "vemb_v16_tlc.h"
#include "vemb_v16_warm_provider.h"

#include <stddef.h>
#include <stdint.h>

#define VEMB_V16_MAX_MANIFEST_REGIONS VEMB_V16_MAX_DESC_WARM_REGIONS
#define VEMB_V16_MAX_MANIFEST_REMOTE_META_VIEWS VEMB_V16_TLC_MAX_REMOTE_META_VIEWS
#define VEMB_V16_STORAGE_OWNER_HASH_VNODES 10u
#define VEMB_V16_STORAGE_MAX_OWNER_HASH_NODES \
    ((VEMB_V16_MAX_MANIFEST_REMOTE_META_VIEWS + 1u) * \
     VEMB_V16_STORAGE_OWNER_HASH_VNODES)

typedef struct vemb_v16_manifest_region {
    uint32_t region_id;
    uint32_t backend_type;
    uint32_t home_ub_node_id;
    uint32_t is_local;
    uint32_t has_is_local;
    uint32_t weight;
    uint32_t value_size;
    uint64_t mmap_offset;
    uint64_t region_bytes;
    char path[256];
} vemb_v16_manifest_region_t;

typedef struct vemb_v16_manifest_remote_meta_view {
    uint32_t owner_id;
    uint32_t has_owner_id;
    uint32_t backend_type;
    uint32_t has_backend_type;
    uint32_t entry_count;
    uint32_t bucket_count;
    uint64_t mmap_offset;
    char path[256];
} vemb_v16_manifest_remote_meta_view_t;

typedef struct vemb_v16_warm_regions_manifest {
    uint32_t local_ub_node_id;
    uint32_t has_local_ub_node_id;
    uint32_t local_region_weight;
    uint32_t remote_meta_backend_type;
    uint32_t has_remote_meta_backend_type;
    uint32_t remote_meta_entry_count;
    uint32_t remote_meta_bucket_count;
    uint64_t remote_meta_mmap_offset;
    char remote_meta_path[256];
    uint32_t remote_meta_view_count;
    vemb_v16_manifest_remote_meta_view_t
        remote_meta_views[VEMB_V16_MAX_MANIFEST_REMOTE_META_VIEWS];
    uint32_t region_count;
    vemb_v16_manifest_region_t regions[VEMB_V16_MAX_MANIFEST_REGIONS];
} vemb_v16_warm_regions_manifest_t;

typedef struct vemb_v16_storage_remote_meta_view {
    uint32_t owner_id;
    uint32_t is_mapped;
    uint32_t backend_type;
    uint64_t mmap_offset;
    char path[256];
    void *base;
    size_t bytes;
    uint32_t entry_count;
    uint32_t bucket_count;
    vemb_v16_mapped_region_t mapping;
    vemb_v16_remote_meta_view_t view;
} vemb_v16_storage_remote_meta_view_t;

typedef struct vemb_v16_storage_owner_hash_node {
    uint32_t hash_value;
    uint32_t owner_id;
} vemb_v16_storage_owner_hash_node_t;

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
    uint32_t warm_region_count;
    uint32_t local_region_weight;
    vemb_v16_mapped_region_t *warm_data_mappings;
    vemb_v16_mapped_region_t *warm_allocator_mappings;
    vemb_v16_warm_provider_t *warm_providers;
    vemb_v16_shared_allocator_mapping_t *warm_allocators;
    vemb_v16_warm_provider_t warm_provider;
    vemb_v16_tlc_t *tlc;
    vemb_v16_mapped_region_t remote_meta_mapping;
    uint32_t remote_meta_is_mapped;
    uint32_t remote_meta_backend_type;
    uint64_t remote_meta_mmap_offset;
    char remote_meta_path[256];
    void *remote_meta_base;
    size_t remote_meta_bytes;
    uint32_t remote_meta_entry_count;
    uint32_t remote_meta_bucket_count;
    vemb_v16_remote_meta_view_t remote_meta_view;
    uint32_t remote_meta_owner_view_count;
    vemb_v16_storage_remote_meta_view_t
        remote_meta_owner_views[VEMB_V16_MAX_MANIFEST_REMOTE_META_VIEWS];
    uint32_t owner_hash_node_count;
    vemb_v16_storage_owner_hash_node_t
        owner_hash_nodes[VEMB_V16_STORAGE_MAX_OWNER_HASH_NODES];
} vemb_v16_storage_ctx_t;

int vemb_v16_storage_ctx_create_from_manifest(vemb_v16_storage_ctx_t **out,
                                              uint32_t vector_dim,
                                              uint32_t vector_stride,
                                              uint32_t max_vectors,
                                              const vemb_v16_warm_regions_manifest_t *manifest);
int vemb_v16_parse_warm_regions_manifest(const char *path,
                                         uint32_t value_size,
                                         vemb_v16_warm_regions_manifest_t *manifest);
int vemb_v16_storage_reset_manifest_regions(const vemb_v16_warm_regions_manifest_t *manifest);
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
