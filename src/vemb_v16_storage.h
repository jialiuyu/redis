#ifndef __VEMB_V16_STORAGE_H
#define __VEMB_V16_STORAGE_H

#include "vemb_v16_protocol.h"
#include "vemb_v16_peer_view_map.h"
#include "vemb_v16_migration_outbox.h"
#include "vemb_v16_remote_meta.h"
#include "vemb_v16_warm_region_layout.h"
#include "vemb_v16_tlc.h"
#include "vemb_v16_topology.h"
#include "vemb_v16_ub_rpc.h"
#include "vemb_v16_warm_provider.h"

#include <stddef.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>

#define VEMB_V16_MAX_MANIFEST_REGIONS VEMB_V16_MAX_DESC_WARM_REGIONS
#define VEMB_V16_MAX_MANIFEST_REMOTE_META_VIEWS VEMB_V16_TLC_MAX_REMOTE_META_VIEWS
#define VEMB_V16_MAX_MANIFEST_UB_RPC_PEERS VEMB_V16_UB_RPC_MAX_PEERS
#define VEMB_V16_STORAGE_OWNER_HASH_VNODES 10u
#define VEMB_V16_STORAGE_MAX_OWNER_COUNT \
    (VEMB_V16_MAX_MANIFEST_REMOTE_META_VIEWS + \
     VEMB_V16_MAX_MANIFEST_UB_RPC_PEERS + 1u)
#define VEMB_V16_STORAGE_MAX_OWNER_HASH_NODES \
    (VEMB_V16_STORAGE_MAX_OWNER_COUNT * VEMB_V16_STORAGE_OWNER_HASH_VNODES)
#define VEMB_V16_STORAGE_MAX_MIGRATION_OUTBOXES 16u
#define VEMB_V16_STORAGE_MAX_MIGRATION_BASELINE_RETRIES 4096u
#define VEMB_V16_STORAGE_MAX_SCALEOUT_RANGES 64u

#define VEMB_V16_STORAGE_SCALEOUT_IDLE 0u
#define VEMB_V16_STORAGE_SCALEOUT_DRAINING 1u
#define VEMB_V16_STORAGE_SCALEOUT_CUTOVER_LOCAL 2u
#define VEMB_V16_STORAGE_SCALEOUT_LOCAL_DONE 3u
#define VEMB_V16_STORAGE_SCALEOUT_NOTIFY_PENDING 4u
#define VEMB_V16_STORAGE_SCALEOUT_NOTIFIED 5u
#define VEMB_V16_STORAGE_SCALEOUT_GLOBAL_CUTOVER_WAIT 6u
#define VEMB_V16_STORAGE_SCALEOUT_PUBLISH_FULL_ACTIVE 7u
#define VEMB_V16_STORAGE_SCALEOUT_SOURCE_GC 8u
#define VEMB_V16_STORAGE_SCALEOUT_DONE 9u
#define VEMB_V16_STORAGE_SCALEOUT_ERROR 10u

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
    uint32_t set_count;
    uint32_t ways;
    uint64_t mmap_offset;
    char path[256];
} vemb_v16_manifest_remote_meta_view_t;

typedef struct vemb_v16_manifest_ub_rpc_peer {
    uint32_t owner_id;
    uint32_t has_owner_id;
    vemb_v16_ub_rpc_ring_config_t request;
    vemb_v16_ub_rpc_ring_config_t response;
    vemb_v16_ub_rpc_ring_config_t inbound_request;
    vemb_v16_ub_rpc_ring_config_t outbound_response;
} vemb_v16_manifest_ub_rpc_peer_t;

typedef struct vemb_v16_warm_regions_manifest {
    uint32_t local_ub_node_id;
    uint32_t has_local_ub_node_id;
    uint32_t local_region_weight;
    uint32_t remote_meta_backend_type;
    uint32_t has_remote_meta_backend_type;
    uint32_t remote_meta_entry_count;
    uint32_t remote_meta_bucket_count;
    uint32_t remote_meta_set_count;
    uint32_t remote_meta_ways;
    uint64_t remote_meta_mmap_offset;
    char remote_meta_path[256];
    uint32_t job_plane_backend_type;
    uint32_t has_job_plane_backend_type;
    uint64_t job_plane_mmap_offset;
    char job_plane_path[256];
    uint32_t remote_meta_view_count;
    vemb_v16_manifest_remote_meta_view_t
        remote_meta_views[VEMB_V16_MAX_MANIFEST_REMOTE_META_VIEWS];
    uint32_t ub_rpc_timeout_ms;
    uint32_t ub_rpc_peer_count;
    vemb_v16_manifest_ub_rpc_peer_t
        ub_rpc_peers[VEMB_V16_MAX_MANIFEST_UB_RPC_PEERS];
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
    uint32_t set_count;
    uint32_t ways;
    vemb_v16_mapped_region_t mapping;
    vemb_v16_remote_meta_view_t view;
} vemb_v16_storage_remote_meta_view_t;

typedef struct vemb_v16_storage_owner_hash_node {
    uint32_t hash_value;
    uint32_t owner_id;
} vemb_v16_storage_owner_hash_node_t;

typedef struct vemb_v16_storage_owner_resolver_snapshot {
    uint32_t owner_hash_node_count;
    vemb_v16_storage_owner_hash_node_t
        owner_hash_nodes[VEMB_V16_STORAGE_MAX_OWNER_HASH_NODES];
    vemb_v16_topology_ring_t owner_ring;
} vemb_v16_storage_owner_resolver_snapshot_t;

typedef struct vemb_v16_storage_migration_baseline_retry {
    uint32_t valid;
    uint32_t key_len;
    uint32_t target_owner;
    uint32_t shard_id;
    uint64_t key_hash;
    uint64_t topology_epoch;
    uint32_t attempts;
    uint32_t last_status;
    char key[VEMB_V16_MAX_KEY_LEN];
} vemb_v16_storage_migration_baseline_retry_t;

typedef struct vemb_v16_storage_scaleout_auto_status {
    uint32_t enabled;
    uint32_t phase;
    uint32_t last_error;
    uint32_t coordinated;
    uint32_t source_owner;
    uint32_t coordinator_endpoint_valid;
    uint32_t pending_delta;
    uint32_t baseline_retry_pending;
    uint32_t migrating_key_count;
    uint32_t range_count;
    uint32_t reserved0;
    uint64_t migration_epoch;
    uint64_t cutover_epoch;
    uint64_t notify_seq;
    vemb_v16_topology_endpoint_t coordinator_endpoint;
} vemb_v16_storage_scaleout_auto_status_t;

typedef struct vemb_v16_storage_ctx {
    uint32_t vector_dim;
    uint32_t vector_stride;
    uint32_t max_vectors;
    uint32_t local_owner_id;
    uint32_t warm_region_id;
    uint32_t warm_backend_type;
    atomic_uint_fast64_t current_topology_epoch;
    atomic_uint_fast64_t min_write_epoch;
    atomic_uint_fast64_t migration_active_count;
    atomic_uint_fast64_t migration_delta_request_id;
    atomic_uint_fast64_t migration_source_gc_count;
    atomic_uint_fast64_t migration_gc_safe_watermark;
    atomic_uint_fast64_t migration_baseline_sent_count;
    atomic_uint_fast64_t migration_baseline_skipped_count;
    atomic_uint_fast64_t migration_baseline_error_count;
    atomic_uint_fast64_t migration_baseline_retry_queued_count;
    atomic_uint_fast64_t migration_baseline_retry_sent_count;
    pthread_mutex_t topology_lock;
    pthread_mutex_t migration_outbox_lock;
    pthread_t migration_retry_thread;
    atomic_int migration_retry_stop;
    uint32_t published_topology_valid;
    uint32_t published_topology_flags;
    uint32_t published_endpoint_count;
    uint32_t migration_retry_thread_started;
    uint32_t migration_retry_interval_us;
    uint32_t migration_retry_batch_size;
    uint32_t scaleout_auto_enabled;
    uint32_t scaleout_auto_phase;
    uint32_t scaleout_auto_last_error;
    uint32_t scaleout_auto_endpoint_count;
    uint32_t scaleout_auto_coordinated;
    uint32_t scaleout_auto_coordinator_endpoint_valid;
    uint64_t scaleout_auto_migration_epoch;
    uint64_t scaleout_auto_cutover_epoch;
    uint64_t scaleout_auto_notify_seq;
    uint64_t warm_mmap_offset;
    uint8_t *vector_region;
    size_t vector_region_size;
    char vector_region_name[256];
    uint32_t warm_region_count;
    uint32_t local_region_weight;
    vemb_v16_mapped_region_t *warm_data_mappings;
    vemb_v16_mapped_region_t *warm_allocator_mappings;
    vemb_v16_warm_provider_t *warm_providers;
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
    uint32_t remote_meta_set_count;
    uint32_t remote_meta_ways;
    vemb_v16_remote_meta_view_t remote_meta_view;
    vemb_v16_ub_rpc_t *ub_rpc;
    uint32_t remote_meta_owner_view_count;
    vemb_v16_storage_remote_meta_view_t
        remote_meta_owner_views[VEMB_V16_MAX_MANIFEST_REMOTE_META_VIEWS];
    uint32_t peer_region_config_count;
    vemb_v16_manifest_region_t
        peer_region_configs[VEMB_V16_PEER_VIEW_MAP_MAX_REGIONS];
    uint32_t peer_remote_meta_view_config_count;
    vemb_v16_manifest_remote_meta_view_t
        peer_remote_meta_view_configs
            [VEMB_V16_PEER_VIEW_MAP_MAX_REMOTE_META_VIEWS];
    uint32_t ub_rpc_timeout_ms;
    uint32_t ub_rpc_peer_config_count;
    vemb_v16_manifest_ub_rpc_peer_t
        ub_rpc_peer_configs[VEMB_V16_MAX_MANIFEST_UB_RPC_PEERS];
    atomic_uint owner_resolver_active_snapshot;
    uint32_t owner_resolver_pending_valid;
    uint32_t owner_resolver_pending_snapshot;
    vemb_v16_storage_owner_resolver_snapshot_t owner_resolver_snapshots[2];
    vemb_v16_topology_ring_t active_topology_ring;
    vemb_v16_topology_ring_t standby_topology_ring;
    vemb_v16_topology_endpoint_t
        published_endpoints[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
    uint32_t published_coordinator_endpoint_valid;
    vemb_v16_topology_endpoint_t published_coordinator_endpoint;
    vemb_v16_topology_ring_t scaleout_auto_full_ring;
    vemb_v16_topology_endpoint_t
        scaleout_auto_endpoints[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
    vemb_v16_topology_endpoint_t scaleout_auto_coordinator_endpoint;
    uint32_t migration_outbox_count;
    vemb_v16_migration_outbox_t
        *migration_outboxes[VEMB_V16_STORAGE_MAX_MIGRATION_OUTBOXES];
    uint32_t migration_baseline_retry_count;
    vemb_v16_storage_migration_baseline_retry_t
        migration_baseline_retries[VEMB_V16_STORAGE_MAX_MIGRATION_BASELINE_RETRIES];
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
void vemb_v16_storage_epoch_get(
    const vemb_v16_storage_ctx_t *storage,
    uint64_t *current_topology_epoch,
    uint64_t *min_write_epoch);
int vemb_v16_storage_epoch_set(
    vemb_v16_storage_ctx_t *storage,
    uint64_t current_topology_epoch,
    uint64_t min_write_epoch);
int vemb_v16_storage_write_epoch_is_stale(
    const vemb_v16_storage_ctx_t *storage,
    uint64_t request_topology_epoch);
int vemb_v16_storage_migration_active(
    const vemb_v16_storage_ctx_t *storage);
int vemb_v16_storage_ask_redirect_write_ready(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    tlc_core_key_migration_info_t *info);
int vemb_v16_storage_topology_set(
    vemb_v16_storage_ctx_t *storage,
    const vemb_v16_topology_control_req_t *req);
void vemb_v16_storage_topology_get(
    vemb_v16_storage_ctx_t *storage,
    vemb_v16_topology_control_resp_t *resp);
int vemb_v16_storage_apply_peer_view_map(
    vemb_v16_storage_ctx_t *storage,
    const vemb_v16_peer_view_map_req_t *req,
    vemb_v16_peer_view_map_resp_t *resp);
int vemb_v16_storage_migration_mark_cutover(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint32_t target_owner,
    tlc_core_key_migration_info_t *info);
int vemb_v16_storage_migration_mark_cutover_in_shard(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    tlc_core_key_migration_info_t *info);
int vemb_v16_storage_migration_mark_source_gc(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint32_t target_owner,
    tlc_core_key_migration_info_t *info);
int vemb_v16_storage_migration_mark_source_gc_in_shard(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    tlc_core_key_migration_info_t *info);
int vemb_v16_storage_migration_barrier(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint32_t target_owner,
    tlc_core_key_migration_info_t *info,
    vemb_v16_migration_outbox_stats_t *outbox_stats);
int vemb_v16_storage_migration_barrier_in_shard(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    tlc_core_key_migration_info_t *info,
    vemb_v16_migration_outbox_stats_t *outbox_stats);
int vemb_v16_storage_migration_range_barrier(
    vemb_v16_storage_ctx_t *storage,
    uint64_t migration_topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    uint32_t page_limit,
    vemb_v16_migration_range_control_resp_t *resp);
int vemb_v16_storage_migration_range_mark_cutover(
    vemb_v16_storage_ctx_t *storage,
    uint64_t migration_topology_epoch,
    uint64_t cutover_topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    uint32_t page_limit,
    vemb_v16_migration_range_control_resp_t *resp);
int vemb_v16_storage_migration_range_mark_source_gc(
    vemb_v16_storage_ctx_t *storage,
    uint64_t migration_topology_epoch,
    uint64_t cutover_topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    uint32_t page_limit,
    vemb_v16_migration_range_control_resp_t *resp);
int vemb_v16_storage_migration_mark_migrating(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint32_t target_owner,
    tlc_core_key_migration_info_t *info);
int vemb_v16_storage_migration_mark_migrating_in_shard(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    tlc_core_key_migration_info_t *info);
int vemb_v16_storage_migration_delta_put_after_local_write(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    const vemb_v16_vector_handle_t *handle,
    vemb_v16_ub_migration_delta_ack_desc_t *ack);
int vemb_v16_storage_delete_with_epoch(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    tlc_core_key_migration_info_t *info,
    vemb_v16_ub_migration_delta_ack_desc_t *ack);
int vemb_v16_storage_migration_drain_outboxes(
    vemb_v16_storage_ctx_t *storage,
    uint32_t max_delta,
    uint32_t *sent_count,
    uint32_t *acked_count);
int vemb_v16_storage_migration_drain_baselines(
    vemb_v16_storage_ctx_t *storage,
    uint32_t max_snapshot,
    uint32_t *sent_count,
    uint32_t *acked_count);
int vemb_v16_storage_migration_retry_start(
    vemb_v16_storage_ctx_t *storage,
    uint32_t interval_us,
    uint32_t batch_size);
void vemb_v16_storage_migration_retry_stop(
    vemb_v16_storage_ctx_t *storage);
int vemb_v16_storage_scaleout_auto_step(
    vemb_v16_storage_ctx_t *storage);
int vemb_v16_storage_scaleout_auto_get_status(
    vemb_v16_storage_ctx_t *storage,
    vemb_v16_storage_scaleout_auto_status_t *status);
int vemb_v16_storage_scaleout_auto_mark_notified(
    vemb_v16_storage_ctx_t *storage,
    uint64_t migration_epoch,
    uint32_t source_owner,
    uint64_t notify_seq);
int vemb_v16_storage_migration_write_blocked(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash);
int vemb_v16_storage_migration_write_blocked_info(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    tlc_core_key_migration_info_t *info,
    vemb_v16_migration_outbox_stats_t *outbox_stats);

#endif
