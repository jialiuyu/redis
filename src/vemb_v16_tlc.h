#ifndef __VEMB_V16_TLC_H
#define __VEMB_V16_TLC_H

#include <stddef.h>
#include <stdatomic.h>
#include <pthread.h>
#include <stdint.h>

#include "sve_operation.h"
#include "tlc_core.h"
#include "vemb_v16_protocol.h"

#define VEMB_V16_TLC_MAX_REMOTE_META_VIEWS 16u
#define VEMB_V16_TLC_MAX_MIGRATION_PROGRESS 64u

typedef struct vemb_v16_remote_meta_view vemb_v16_remote_meta_view_t;
typedef struct vemb_v16_tlc vemb_v16_tlc_t;
typedef struct vemb_v16_tlc_remote_meta_publisher
    vemb_v16_tlc_remote_meta_publisher_t;
typedef uint32_t (*vemb_v16_tlc_owner_resolver_fn)(uint64_t key_hash,
                                                   const char *key,
                                                   uint32_t key_len,
                                                   void *arg);
typedef int (*vemb_v16_tlc_lookup_rpc_fn)(
    void *arg,
    const vemb_v16_ub_lookup_rpc_req_t *req,
    vemb_v16_ub_lookup_rpc_resp_t *resp);

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
    uint32_t local_slot;
    uint32_t reserved0;
    uint64_t offset;
    uint64_t key_hash;
    uint64_t owner_generation;
} vemb_v16_vector_handle_t;

typedef enum vemb_v16_tlc_lookup_source {
    VEMB_V16_TLC_LOOKUP_SOURCE_NONE = 0,
    VEMB_V16_TLC_LOOKUP_SOURCE_LOCAL = 1,
    VEMB_V16_TLC_LOOKUP_SOURCE_REMOTE = 2,
    VEMB_V16_TLC_LOOKUP_SOURCE_UB_RPC = 3,
} vemb_v16_tlc_lookup_source_t;

typedef struct vemb_v16_tlc_lookup_timing {
    uint64_t local_lookup_ns;
    uint64_t remote_meta_lookup_ns;
    uint32_t local_lookup_count;
    uint32_t remote_meta_lookup_count;
} vemb_v16_tlc_lookup_timing_t;

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

typedef struct vemb_v16_tlc_migration_progress {
    uint32_t valid;
    uint32_t source_owner;
    uint32_t target_owner;
    uint32_t shard_id;
    uint64_t topology_epoch;
    uint64_t applied_seq;
    uint64_t barrier_seq;
} vemb_v16_tlc_migration_progress_t;

struct vemb_v16_tlc {
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
    vemb_v16_tlc_lookup_rpc_fn lookup_rpc;
    void *lookup_rpc_arg;
    pthread_mutex_t migration_progress_lock;
    uint32_t migration_progress_lock_init;
    uint32_t migration_progress_count;
    vemb_v16_tlc_migration_progress_t
        migration_progress[VEMB_V16_TLC_MAX_MIGRATION_PROGRESS];
    atomic_uint_fast64_t ub_lookup_rpc_next_request_id;
    _Atomic(vemb_v16_tlc_remote_meta_publisher_t *) remote_meta_publisher;
    atomic_uint_fast64_t remote_meta_lookup_hit;
    atomic_uint_fast64_t remote_meta_lookup_miss;
    atomic_uint_fast64_t remote_meta_lookup_busy;
    atomic_uint_fast64_t remote_meta_lookup_way_probe;
    atomic_uint_fast64_t remote_meta_lookup_set_conflict;
    atomic_uint_fast64_t remote_meta_publish_async_enqueue;
    atomic_uint_fast64_t remote_meta_publish_async_drop;
    atomic_uint_fast64_t remote_meta_publish_async_coalesce;
    atomic_uint_fast64_t remote_meta_publish_ok;
    atomic_uint_fast64_t remote_meta_publish_busy;
    atomic_uint_fast64_t remote_meta_publish_insert;
    atomic_uint_fast64_t remote_meta_publish_update;
    atomic_uint_fast64_t remote_meta_publish_evict;
    atomic_uint_fast64_t remote_meta_publish_ns;
    atomic_uint_fast64_t ub_lookup_rpc_count;
    atomic_uint_fast64_t ub_lookup_rpc_ok;
    atomic_uint_fast64_t ub_lookup_rpc_not_found;
    atomic_uint_fast64_t ub_lookup_rpc_busy;
    atomic_uint_fast64_t ub_lookup_rpc_timeout;
    atomic_uint_fast64_t ub_lookup_rpc_error;
    atomic_uint_fast64_t ub_lookup_rpc_handle;
    atomic_uint_fast64_t ub_lookup_rpc_snapshot;
    atomic_uint_fast64_t ub_lookup_rpc_ns;
    atomic_uint_fast64_t remote_meta_repair_enqueue;
    atomic_uint_fast64_t remote_meta_repair_ok;
    atomic_uint_fast64_t remote_meta_repair_drop;
};

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
                                  vemb_v16_tlc_lookup_source_t *source,
                                  vemb_v16_tlc_lookup_timing_t *timing);
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
int vemb_v16_tlc_publish_remote_meta_async(vemb_v16_tlc_t *tlc,
                                           const char *key,
                                           uint32_t key_len,
                                           uint64_t key_hash,
                                           const vemb_v16_vector_handle_t *handle);
uint32_t vemb_v16_tlc_flush_remote_meta_publishes(vemb_v16_tlc_t *tlc,
                                                  uint32_t budget);
void vemb_v16_tlc_set_lookup_rpc(vemb_v16_tlc_t *tlc,
                                 vemb_v16_tlc_lookup_rpc_fn fn,
                                 void *arg);
int vemb_v16_tlc_lookup_rpc_local_handler(
    void *arg,
    const vemb_v16_ub_lookup_rpc_req_t *req,
    vemb_v16_ub_lookup_rpc_resp_t *resp);
int vemb_v16_tlc_migration_rpc_local_handler(
    void *arg,
    const vemb_v16_ub_migration_rpc_req_t *req,
    vemb_v16_ub_migration_rpc_resp_t *resp);
int vemb_v16_tlc_put(vemb_v16_tlc_t *tlc,
                     const char *key,
                     uint32_t key_len,
                     uint64_t key_hash,
                     const float *vector,
                     uint32_t vector_bytes,
                     vemb_v16_vector_handle_t *handle,
                     uint32_t *warm_slot);
int vemb_v16_tlc_put_with_epoch(vemb_v16_tlc_t *tlc,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                const float *vector,
                                uint32_t vector_bytes,
                                uint64_t topology_epoch,
                                vemb_v16_vector_handle_t *handle,
                                uint32_t *warm_slot);
int vemb_v16_tlc_delete_with_epoch(vemb_v16_tlc_t *tlc,
                                   const char *key,
                                   uint32_t key_len,
                                   uint64_t key_hash,
                                   uint64_t topology_epoch,
                                   tlc_core_key_migration_info_t *info);
int vemb_v16_tlc_cold_append(vemb_v16_tlc_t *tlc,
                             const char *key,
                             uint32_t key_len,
                             uint64_t key_hash,
                             const float *vector,
                             uint32_t vector_bytes);
int vemb_v16_tlc_get_migration_info(vemb_v16_tlc_t *tlc,
                                    const char *key,
                                    uint32_t key_len,
                                    uint64_t key_hash,
                                    tlc_core_key_migration_info_t *info);
int vemb_v16_tlc_mark_migrating(vemb_v16_tlc_t *tlc,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                uint64_t topology_epoch,
                                uint32_t target_owner,
                                tlc_core_key_migration_info_t *info);
int vemb_v16_tlc_mark_migrating_in_shard(
                                vemb_v16_tlc_t *tlc,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                uint64_t topology_epoch,
                                uint32_t target_owner,
                                uint32_t shard_id,
                                tlc_core_key_migration_info_t *info);
int vemb_v16_tlc_mark_cutover(vemb_v16_tlc_t *tlc,
                              const char *key,
                              uint32_t key_len,
                              uint64_t key_hash,
                              uint64_t topology_epoch,
                              uint32_t target_owner,
                              tlc_core_key_migration_info_t *info);
int vemb_v16_tlc_mark_source_gc(vemb_v16_tlc_t *tlc,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                uint64_t topology_epoch,
                                uint32_t target_owner,
                                tlc_core_key_migration_info_t *info);
int vemb_v16_tlc_accept_owner_lease(vemb_v16_tlc_t *tlc,
                                    const char *key,
                                    uint32_t key_len,
                                    uint64_t key_hash,
                                    uint64_t topology_epoch,
                                    uint64_t owner_epoch,
                                    uint32_t target_owner,
                                    tlc_core_key_migration_info_t *info);
int vemb_v16_tlc_migration_progress_ready(vemb_v16_tlc_t *tlc,
                                          uint32_t source_owner,
                                          uint32_t target_owner,
                                          uint32_t shard_id,
                                          uint64_t max_topology_epoch,
                                          uint64_t *topology_epoch,
                                          uint64_t *applied_seq,
                                          uint64_t *barrier_seq);
int vemb_v16_tlc_key_is_source_cutover(vemb_v16_tlc_t *tlc,
                                       const char *key,
                                       uint32_t key_len,
                                       uint64_t key_hash,
                                       tlc_core_key_migration_info_t *info);
int vemb_v16_tlc_has_uncommitted_source_migrations(vemb_v16_tlc_t *tlc);
int vemb_v16_tlc_collect_migration_keys(
                                vemb_v16_tlc_t *tlc,
                                uint64_t topology_epoch,
                                uint32_t target_owner,
                                uint32_t shard_id,
                                uint32_t migration_state,
                                tlc_core_migration_key_ref_t *keys,
                                uint32_t max_keys,
                                uint32_t *key_count);
int vemb_v16_tlc_collect_migration_keys_page(
                                vemb_v16_tlc_t *tlc,
                                uint64_t topology_epoch,
                                uint32_t target_owner,
                                uint32_t shard_id,
                                uint32_t migration_state,
                                tlc_core_migration_key_ref_t *keys,
                                uint32_t max_keys,
                                uint32_t *key_count,
                                uint32_t *remaining_count);
int vemb_v16_tlc_count_migration_keys(
                                vemb_v16_tlc_t *tlc,
                                uint64_t topology_epoch,
                                uint32_t target_owner,
                                uint32_t shard_id,
                                uint32_t migration_state,
                                uint32_t *key_count);
int vemb_v16_tlc_collect_migration_ranges(
                                vemb_v16_tlc_t *tlc,
                                uint64_t topology_epoch,
                                uint32_t migration_state,
                                tlc_core_migration_range_ref_t *ranges,
                                uint32_t max_ranges,
                                uint32_t *range_count);
int vemb_v16_tlc_collect_source_active_keys(
                                vemb_v16_tlc_t *tlc,
                                uint32_t *cursor,
                                tlc_core_migration_key_ref_t *keys,
                                uint32_t max_keys,
                                uint32_t *key_count,
                                int *done);
int vemb_v16_tlc_snapshot(vemb_v16_tlc_t *tlc,
                          const char *key,
                          uint32_t key_len,
                          uint64_t key_hash,
                          uint32_t source_owner,
                          uint32_t target_owner,
                          tlc_core_migration_snapshot_t *snapshot,
                          void *value_out,
                          uint32_t value_out_size);
int vemb_v16_tlc_apply_migration(vemb_v16_tlc_t *tlc,
                                 const tlc_core_migration_snapshot_t *snapshot,
                                 const void *value,
                                 uint32_t value_size,
                                 tlc_core_migration_apply_status_t *status,
                                 vemb_v16_vector_handle_t *handle);
const vemb_v16_tlc_warm_region_t *vemb_v16_tlc_find_region(
    const vemb_v16_tlc_t *tlc,
    uint32_t region_id);
int vemb_v16_tlc_vector_slice(const vemb_v16_tlc_t *tlc,
                              const vemb_v16_vector_handle_t *handle,
                              const uint8_t **vector,
                              uint32_t *vector_bytes);
void vemb_v16_tlc_get_core_stats(vemb_v16_tlc_t *tlc,
                                 tlc_core_stats_t *stats);
void vemb_v16_tlc_get_runtime_stats(vemb_v16_tlc_t *tlc,
                                    vemb_v16_stats_t *stats);
uint32_t vemb_v16_tlc_get_region_stats(vemb_v16_tlc_t *tlc,
                                       tlc_core_region_stats_t *regions,
                                       uint32_t max_regions);

#endif
