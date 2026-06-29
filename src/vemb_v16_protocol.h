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
#define VEMB_V16_MIGRATION_CONTROL_MAX_BATCH 16u
#define VEMB_V16_MIGRATION_CONTROL_MAX_RANGE_KEYS 64u
#define VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS 64u
#define VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS 64u
#define VEMB_V16_TOPOLOGY_ENDPOINT_HOST_LEN 64u
#define VEMB_V16_TOPOLOGY_ENDPOINT_PATH_LEN 108u

#define VEMB_V16_STATUS_OK 0u
#define VEMB_V16_STATUS_NOT_FOUND 1u
#define VEMB_V16_STATUS_ERR 2u
#define VEMB_V16_STATUS_STALE_TOPOLOGY 3u
#define VEMB_V16_STATUS_MOVED 4u
#define VEMB_V16_STATUS_ASK 5u

#define VEMB_V16_REGION_LOCAL_SHM 1u
#define VEMB_V16_REGION_UB 2u

#define VEMB_V16_TRANSPORT_AERON 1u
#define VEMB_V16_TRANSPORT_TCP 2u

#define VEMB_V16_NET_F_ENCODED_PAYLOAD 0x01u

#define VEMB_V16_REQ_F_ASK_REDIRECT 0x02u
#define VEMB_V16_TOPOLOGY_CONTROL_F_PUBLISHED 0x01u
#define VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED 0x02u
#define VEMB_V16_TOPOLOGY_CONTROL_F_AUTO_SCALEOUT 0x04u
#define VEMB_V16_TOPOLOGY_CONTROL_F_COORDINATED_SCALEOUT 0x08u

enum vemb_v16_ctrl_op {
    VEMB_V16_CTRL_PING = 0x01,
    VEMB_V16_CTRL_ALLOC_CHANNEL = 0x20,
    VEMB_V16_CTRL_CLOSE_CHANNEL = 0x21,
    VEMB_V16_CTRL_STATS = 0x22,
    VEMB_V16_CTRL_CLOSE_ALL_CHANNELS = 0x23,
    VEMB_V16_CTRL_MIGRATION_MARK_MIGRATING = 0x30,
    VEMB_V16_CTRL_MIGRATION_MARK_CUTOVER = 0x31,
    VEMB_V16_CTRL_MIGRATION_MARK_MIGRATING_BATCH = 0x32,
    VEMB_V16_CTRL_MIGRATION_BARRIER = 0x33,
    VEMB_V16_CTRL_MIGRATION_RANGE_BARRIER = 0x34,
    VEMB_V16_CTRL_MIGRATION_RANGE_MARK_CUTOVER = 0x35,
    VEMB_V16_CTRL_MIGRATION_MARK_SOURCE_GC = 0x36,
    VEMB_V16_CTRL_MIGRATION_RANGE_SOURCE_GC = 0x37,
    VEMB_V16_CTRL_EPOCH_SET = 0x40,
    VEMB_V16_CTRL_EPOCH_GET = 0x41,
    VEMB_V16_CTRL_TOPOLOGY_SET = 0x42,
    VEMB_V16_CTRL_TOPOLOGY_GET = 0x43,
    VEMB_V16_CTRL_SCALEOUT_LOCAL_DONE = 0x44,
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
    VEMB_V16_NET_MIGRATION_MARK_MIGRATING = 0x0a,
    VEMB_V16_NET_MIGRATION_MARK_CUTOVER = 0x0b,
    VEMB_V16_NET_MIGRATION_CONTROL_RESPONSE = 0x0c,
    VEMB_V16_NET_MIGRATION_MARK_MIGRATING_BATCH = 0x0d,
    VEMB_V16_NET_MIGRATION_CONTROL_BATCH_RESPONSE = 0x0e,
    VEMB_V16_NET_EPOCH_SET = 0x0f,
    VEMB_V16_NET_EPOCH_GET = 0x10,
    VEMB_V16_NET_EPOCH_CONTROL_RESPONSE = 0x11,
    VEMB_V16_NET_TOPOLOGY_SET = 0x12,
    VEMB_V16_NET_TOPOLOGY_GET = 0x13,
    VEMB_V16_NET_TOPOLOGY_RESPONSE = 0x14,
    VEMB_V16_NET_MIGRATION_BARRIER = 0x15,
    VEMB_V16_NET_MIGRATION_RANGE_BARRIER = 0x16,
    VEMB_V16_NET_MIGRATION_RANGE_MARK_CUTOVER = 0x17,
    VEMB_V16_NET_MIGRATION_RANGE_CONTROL_RESPONSE = 0x18,
    VEMB_V16_NET_MIGRATION_MARK_SOURCE_GC = 0x19,
    VEMB_V16_NET_MIGRATION_RANGE_SOURCE_GC = 0x1a,
    VEMB_V16_NET_SCALEOUT_LOCAL_DONE = 0x1b,
    VEMB_V16_NET_SCALEOUT_LOCAL_DONE_RESPONSE = 0x1c,
};

enum vemb_v16_data_op {
    VEMB_V16_OP_PING = 0x01,
    VEMB_V16_OP_VADD = 0x10,
    VEMB_V16_OP_VREM = 0x11,
    VEMB_V16_OP_VEMB_HANDLE = 0x20,
    VEMB_V16_OP_VEMB_INLINE = 0x21,
    VEMB_V16_OP_VSIM_INLINE = 0x30,
    VEMB_V16_OP_VSIM_KEY_KEY = 0x31,
};

enum vemb_v16_ub_lookup_rpc_op {
    VEMB_V16_UB_LOOKUP_RPC_LOOKUP_HANDLE = 0x01,
};

enum vemb_v16_ub_lookup_rpc_status {
    VEMB_V16_UB_LOOKUP_RPC_OK = 0,
    VEMB_V16_UB_LOOKUP_RPC_NOT_FOUND = 1,
    VEMB_V16_UB_LOOKUP_RPC_BUSY = 2,
    VEMB_V16_UB_LOOKUP_RPC_ERROR = 3,
    VEMB_V16_UB_LOOKUP_RPC_TIMEOUT = 4,
};

enum vemb_v16_ub_lookup_rpc_kind {
    VEMB_V16_UB_LOOKUP_RPC_KIND_NONE = 0,
    VEMB_V16_UB_LOOKUP_RPC_KIND_HANDLE = 1,
    VEMB_V16_UB_LOOKUP_RPC_KIND_SNAPSHOT = 2,
};

enum vemb_v16_ub_rpc_frame_kind {
    VEMB_V16_UB_RPC_FRAME_LOOKUP = 1,
    VEMB_V16_UB_RPC_FRAME_MIGRATION = 2,
};

enum vemb_v16_ub_migration_rpc_op {
    VEMB_V16_UB_MIGRATION_RPC_SNAPSHOT_REQ = 0x01,
    VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT = 0x02,
    VEMB_V16_UB_MIGRATION_RPC_DELTA_DELETE = 0x03,
    VEMB_V16_UB_MIGRATION_RPC_DELTA_ACK = 0x04,
    VEMB_V16_UB_MIGRATION_RPC_BARRIER_REQ = 0x05,
    VEMB_V16_UB_MIGRATION_RPC_BARRIER_RESP = 0x06,
    VEMB_V16_UB_MIGRATION_RPC_LEASE_COMMIT_REQ = 0x07,
    VEMB_V16_UB_MIGRATION_RPC_LEASE_COMMIT_RESP = 0x08,
    VEMB_V16_UB_MIGRATION_RPC_BASELINE_PUT = 0x09,
};

enum vemb_v16_ub_migration_rpc_status {
    VEMB_V16_UB_MIGRATION_RPC_OK = 0,
    VEMB_V16_UB_MIGRATION_RPC_NOT_FOUND = 1,
    VEMB_V16_UB_MIGRATION_RPC_ERROR = 2,
    VEMB_V16_UB_MIGRATION_RPC_TIMEOUT = 3,
    VEMB_V16_UB_MIGRATION_RPC_BUSY = 4,
    VEMB_V16_UB_MIGRATION_RPC_DUPLICATE = 5,
    VEMB_V16_UB_MIGRATION_RPC_STALE_REJECTED = 6,
    VEMB_V16_UB_MIGRATION_RPC_RETRY = 7,
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

typedef struct vemb_v16_epoch_control_req {
    uint64_t current_topology_epoch;
    uint64_t min_write_epoch;
    uint32_t flags;
    uint32_t reserved0;
} vemb_v16_epoch_control_req_t;

typedef struct vemb_v16_epoch_control_resp {
    uint8_t status;
    uint8_t reserved0[7];
    uint64_t current_topology_epoch;
    uint64_t min_write_epoch;
} vemb_v16_epoch_control_resp_t;

typedef struct vemb_v16_topology_endpoint {
    uint32_t owner_id;
    uint32_t transport_type;
    uint16_t tcp_port;
    uint16_t reserved0;
    char host[VEMB_V16_TOPOLOGY_ENDPOINT_HOST_LEN];
    char uds_path[VEMB_V16_TOPOLOGY_ENDPOINT_PATH_LEN];
} vemb_v16_topology_endpoint_t;

typedef struct vemb_v16_topology_control_req {
    uint64_t current_topology_epoch;
    uint64_t min_write_epoch;
    uint32_t active_owner_count;
    uint32_t standby_owner_count;
    uint32_t vnode_count;
    uint32_t flags;
    uint32_t active_owners[VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS];
    uint32_t standby_owners[VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS];
    uint32_t endpoint_count;
    uint32_t coordinator_endpoint_valid;
    vemb_v16_topology_endpoint_t coordinator_endpoint;
    vemb_v16_topology_endpoint_t
        endpoints[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
} vemb_v16_topology_control_req_t;

typedef struct vemb_v16_topology_control_resp {
    uint8_t status;
    uint8_t reserved0[7];
    uint64_t current_topology_epoch;
    uint64_t min_write_epoch;
    uint32_t active_owner_count;
    uint32_t standby_owner_count;
    uint32_t vnode_count;
    uint32_t flags;
    uint32_t active_owners[VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS];
    uint32_t standby_owners[VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS];
    uint32_t endpoint_count;
    uint32_t coordinator_endpoint_valid;
    vemb_v16_topology_endpoint_t coordinator_endpoint;
    vemb_v16_topology_endpoint_t
        endpoints[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
} vemb_v16_topology_control_resp_t;

typedef struct vemb_v16_scaleout_local_done_req {
    uint64_t migration_topology_epoch;
    uint64_t cutover_topology_epoch;
    uint64_t notify_seq;
    uint32_t source_owner;
    uint32_t phase;
    uint32_t error_code;
    uint32_t pending_delta;
    uint32_t baseline_retry_pending;
    uint32_t migrating_key_count;
    uint32_t range_count;
    uint32_t flags;
    uint32_t reserved0;
} vemb_v16_scaleout_local_done_req_t;

typedef struct vemb_v16_scaleout_local_done_resp {
    uint8_t status;
    uint8_t reserved0[7];
    uint64_t migration_topology_epoch;
    uint64_t cutover_topology_epoch;
    uint64_t notify_seq;
    uint32_t source_owner;
    uint32_t reserved1;
} vemb_v16_scaleout_local_done_resp_t;

typedef struct vemb_v16_migration_control_req {
    uint64_t key_hash;
    uint64_t topology_epoch;
    uint32_t key_len;
    uint32_t target_owner;
    uint32_t shard_id;
    uint32_t reserved1;
    char key[VEMB_V16_MAX_KEY_LEN];
} vemb_v16_migration_control_req_t;

typedef struct vemb_v16_migration_control_resp {
    uint8_t status;
    uint8_t reserved0[7];
    uint64_t key_hash;
    uint64_t key_version;
    uint64_t topology_epoch;
    uint64_t owner_epoch;
    uint64_t applied_seq;
    uint64_t barrier_seq;
    uint32_t migration_state;
    uint32_t target_owner;
    uint32_t tombstone;
    uint32_t pending_delta;
    uint32_t outbox_state;
    uint32_t shard_id;
} vemb_v16_migration_control_resp_t;

typedef struct vemb_v16_migration_control_batch_req {
    uint32_t entry_count;
    uint32_t reserved0;
    vemb_v16_migration_control_req_t
        entries[VEMB_V16_MIGRATION_CONTROL_MAX_BATCH];
} vemb_v16_migration_control_batch_req_t;

typedef struct vemb_v16_migration_control_batch_resp {
    uint8_t status;
    uint8_t reserved0[7];
    uint32_t entry_count;
    uint32_t success_count;
    uint32_t error_count;
    uint32_t reserved1;
    vemb_v16_migration_control_resp_t
        entries[VEMB_V16_MIGRATION_CONTROL_MAX_BATCH];
} vemb_v16_migration_control_batch_resp_t;

typedef struct vemb_v16_migration_range_control_req {
    uint64_t migration_topology_epoch;
    uint64_t cutover_topology_epoch;
    uint32_t target_owner;
    uint32_t shard_id;
    uint32_t flags;
    uint32_t page_limit;
} vemb_v16_migration_range_control_req_t;

typedef struct vemb_v16_migration_range_control_resp {
    uint8_t status;
    uint8_t reserved0[7];
    uint64_t migration_topology_epoch;
    uint64_t cutover_topology_epoch;
    uint64_t owner_epoch;
    uint64_t applied_seq;
    uint64_t barrier_seq;
    uint64_t source_seq;
    uint64_t retry_delta;
    uint32_t target_owner;
    uint32_t shard_id;
    uint32_t key_count;
    uint32_t success_count;
    uint32_t error_count;
    uint32_t pending_delta;
    uint32_t outbox_state;
    uint32_t remaining_keys;
    uint32_t page_key_count;
    uint32_t range_done;
    uint32_t range_ready;
    uint32_t page_limit;
    uint32_t reserved1;
} vemb_v16_migration_range_control_resp_t;

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
    uint64_t topology_epoch;
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
    uint32_t local_slot;
    uint64_t owner_generation;
    uint32_t redirect_owner;
    uint32_t reserved2;
    float score;
} vemb_v16_resp_t;

typedef struct vemb_v16_ub_lookup_rpc_req {
    uint64_t request_id;
    uint32_t src_owner_id;
    uint32_t dst_owner_id;
    uint32_t op;
    uint32_t flags;
    uint64_t key_hash;
    uint32_t key_len;
    uint32_t timeout_ns;
    char key[VEMB_V16_MAX_KEY_LEN];
} vemb_v16_ub_lookup_rpc_req_t;

typedef struct vemb_v16_ub_lookup_rpc_resp {
    uint64_t request_id;
    uint32_t status;
    uint32_t kind;
    uint64_t key_hash;
    uint32_t region_id;
    uint32_t local_slot;
    uint64_t offset;
    uint32_t bytes;
    uint32_t snapshot_bytes;
    uint64_t owner_generation;
} vemb_v16_ub_lookup_rpc_resp_t;

typedef struct vemb_v16_ub_migration_delta_desc {
    uint64_t key_hash;
    uint64_t key_version;
    uint64_t topology_epoch;
    uint64_t owner_epoch;
    uint64_t delta_seq;
    uint32_t op;
    uint32_t flags;
    uint32_t shard_id;
    uint32_t key_len;
    uint32_t source_owner;
    uint32_t target_owner;
    uint32_t tombstone;
    uint32_t value_size;
    uint32_t region_id;
    uint32_t local_slot;
    uint32_t bytes;
    uint32_t reserved0;
    uint64_t offset;
    uint64_t owner_generation;
    char key[VEMB_V16_MAX_KEY_LEN];
} vemb_v16_ub_migration_delta_desc_t;

typedef struct vemb_v16_ub_migration_delta_ack_desc {
    uint64_t topology_epoch;
    uint64_t applied_seq;
    uint64_t barrier_seq;
    uint32_t source_owner;
    uint32_t target_owner;
    uint32_t shard_id;
    uint32_t status;
} vemb_v16_ub_migration_delta_ack_desc_t;

typedef struct vemb_v16_ub_migration_barrier_desc {
    uint64_t topology_epoch;
    uint64_t barrier_seq;
    uint32_t source_owner;
    uint32_t target_owner;
    uint32_t shard_id;
    uint32_t flags;
} vemb_v16_ub_migration_barrier_desc_t;

typedef struct vemb_v16_ub_migration_lease_desc {
    uint64_t topology_epoch;
    uint64_t owner_epoch;
    uint32_t source_owner;
    uint32_t target_owner;
    uint32_t shard_id;
    uint32_t flags;
} vemb_v16_ub_migration_lease_desc_t;

typedef struct vemb_v16_ub_migration_snapshot_desc {
    uint64_t key_hash;
    uint64_t key_version;
    uint64_t topology_epoch;
    uint64_t owner_epoch;
    uint32_t key_len;
    uint32_t migration_state;
    uint32_t source_owner;
    uint32_t target_owner;
    uint32_t tombstone;
    uint32_t value_size;
    uint32_t region_id;
    uint32_t local_slot;
    uint32_t bytes;
    uint32_t shard_id;
    uint64_t offset;
    uint64_t owner_generation;
    char key[VEMB_V16_MAX_KEY_LEN];
} vemb_v16_ub_migration_snapshot_desc_t;

typedef struct vemb_v16_ub_migration_rpc_req {
    uint64_t request_id;
    uint32_t src_owner_id;
    uint32_t dst_owner_id;
    uint32_t op;
    uint32_t flags;
    uint64_t key_hash;
    uint64_t topology_epoch;
    uint32_t key_len;
    uint32_t timeout_ns;
    uint32_t target_owner_id;
    uint32_t reserved0;
    char key[VEMB_V16_MAX_KEY_LEN];
    vemb_v16_ub_migration_delta_desc_t delta;
    vemb_v16_ub_migration_delta_ack_desc_t delta_ack;
    vemb_v16_ub_migration_barrier_desc_t barrier;
    vemb_v16_ub_migration_lease_desc_t lease;
    vemb_v16_ub_migration_snapshot_desc_t snapshot;
} vemb_v16_ub_migration_rpc_req_t;

typedef struct vemb_v16_ub_migration_rpc_resp {
    uint64_t request_id;
    uint32_t status;
    uint32_t op;
    uint64_t key_hash;
    uint64_t topology_epoch;
    uint64_t key_version;
    uint32_t source_owner_id;
    uint32_t target_owner_id;
    vemb_v16_ub_migration_snapshot_desc_t snapshot;
    vemb_v16_ub_migration_delta_ack_desc_t delta_ack;
    vemb_v16_ub_migration_barrier_desc_t barrier;
    vemb_v16_ub_migration_lease_desc_t lease;
} vemb_v16_ub_migration_rpc_resp_t;

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
    uint64_t job_shard_queue_depth;
    uint64_t reserved_shard_queue_depth;
    uint64_t completion_ring_depth;
    uint64_t channel_ops;
    uint64_t warm_region_count;
    uint64_t warm_region_full_count;
    uint64_t warm_alloc_local;
    uint64_t warm_alloc_remote;
    uint64_t warm_alloc_fallback;
    uint64_t warm_alloc_cold_spill;
    uint64_t warm_alloc_fail;
    uint64_t warm_eviction_success;
    uint64_t warm_eviction_fail;
    uint64_t warm_same_key_overwrite;
    uint64_t warm_stale_handle_reject;
    uint64_t remote_meta_stale;
    uint64_t remote_meta_lookup_hit;
    uint64_t remote_meta_lookup_miss;
    uint64_t remote_meta_lookup_busy;
    uint64_t remote_meta_lookup_way_probe;
    uint64_t remote_meta_lookup_set_conflict;
    uint64_t remote_meta_publish_async_enqueue;
    uint64_t remote_meta_publish_async_drop;
    uint64_t remote_meta_publish_async_coalesce;
    uint64_t remote_meta_publish_ok;
    uint64_t remote_meta_publish_busy;
    uint64_t remote_meta_publish_insert;
    uint64_t remote_meta_publish_update;
    uint64_t remote_meta_publish_evict;
    uint64_t remote_meta_publish_ns;
    uint64_t ub_lookup_rpc_count;
    uint64_t ub_lookup_rpc_ok;
    uint64_t ub_lookup_rpc_not_found;
    uint64_t ub_lookup_rpc_busy;
    uint64_t ub_lookup_rpc_timeout;
    uint64_t ub_lookup_rpc_error;
    uint64_t ub_lookup_rpc_handle;
    uint64_t ub_lookup_rpc_snapshot;
    uint64_t ub_lookup_rpc_ns;
    uint64_t remote_meta_repair_enqueue;
    uint64_t remote_meta_repair_ok;
    uint64_t remote_meta_repair_drop;
    uint64_t warm_region_hash_local_pct;
    uint64_t moved_count;
    uint64_t stale_count;
    uint64_t ask_count;
    uint64_t forward_count;
    uint64_t duplicate_request_count;
    uint64_t source_gc_count;
    uint64_t gc_safe_watermark;
    uint64_t migration_baseline_sent;
    uint64_t migration_baseline_skipped;
    uint64_t migration_baseline_error;
    uint64_t migration_baseline_retry_queued;
    uint64_t migration_baseline_retry_sent;
    uint64_t migration_baseline_retry_pending;
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

static inline void vemb_v16_proto_put_u16(uint8_t **p, uint16_t v) {
    uint8_t *dst = *p;
    dst[0] = (uint8_t)(v >> 8);
    dst[1] = (uint8_t)v;
    *p = dst + 2;
}

static inline void vemb_v16_proto_put_u32(uint8_t **p, uint32_t v) {
    uint8_t *dst = *p;
    dst[0] = (uint8_t)(v >> 24);
    dst[1] = (uint8_t)(v >> 16);
    dst[2] = (uint8_t)(v >> 8);
    dst[3] = (uint8_t)v;
    *p = dst + 4;
}

static inline void vemb_v16_proto_put_u64(uint8_t **p, uint64_t v) {
    uint8_t *dst = *p;
    dst[0] = (uint8_t)(v >> 56);
    dst[1] = (uint8_t)(v >> 48);
    dst[2] = (uint8_t)(v >> 40);
    dst[3] = (uint8_t)(v >> 32);
    dst[4] = (uint8_t)(v >> 24);
    dst[5] = (uint8_t)(v >> 16);
    dst[6] = (uint8_t)(v >> 8);
    dst[7] = (uint8_t)v;
    *p = dst + 8;
}

static inline uint16_t vemb_v16_proto_get_u16(const uint8_t **p) {
    const uint8_t *src = *p;
    uint16_t v = (uint16_t)(((uint16_t)src[0] << 8) | (uint16_t)src[1]);
    *p = src + 2;
    return v;
}

static inline uint32_t vemb_v16_proto_get_u32(const uint8_t **p) {
    const uint8_t *src = *p;
    uint32_t v = ((uint32_t)src[0] << 24) |
                 ((uint32_t)src[1] << 16) |
                 ((uint32_t)src[2] << 8) |
                 (uint32_t)src[3];
    *p = src + 4;
    return v;
}

static inline uint64_t vemb_v16_proto_get_u64(const uint8_t **p) {
    const uint8_t *src = *p;
    uint64_t v = ((uint64_t)src[0] << 56) |
                 ((uint64_t)src[1] << 48) |
                 ((uint64_t)src[2] << 40) |
                 ((uint64_t)src[3] << 32) |
                 ((uint64_t)src[4] << 24) |
                 ((uint64_t)src[5] << 16) |
                 ((uint64_t)src[6] << 8) |
                 (uint64_t)src[7];
    *p = src + 8;
    return v;
}

static inline void vemb_v16_proto_put_bytes(uint8_t **p,
                                            const void *src,
                                            size_t len) {
    if (len != 0)
        memcpy(*p, src, len);
    *p += len;
}

static inline void vemb_v16_proto_get_bytes(const uint8_t **p,
                                            void *dst,
                                            size_t len) {
    if (len != 0)
        memcpy(dst, *p, len);
    *p += len;
}

static inline void vemb_v16_proto_put_f32(uint8_t **p, float v) {
    uint32_t bits = 0;
    memcpy(&bits, &v, sizeof(bits));
    vemb_v16_proto_put_u32(p, bits);
}

static inline float vemb_v16_proto_get_f32(const uint8_t **p) {
    uint32_t bits = vemb_v16_proto_get_u32(p);
    float v = 0;
    memcpy(&v, &bits, sizeof(v));
    return v;
}

static inline size_t vemb_v16_alloc_req_encoded_len(void) {
    return 8u;
}

static inline int vemb_v16_alloc_req_encode(uint8_t *dst,
                                            size_t cap,
                                            const vemb_v16_alloc_req_t *req,
                                            size_t *out_len) {
    if (!dst || !req || cap < vemb_v16_alloc_req_encoded_len())
        return -1;
    uint8_t *p = dst;
    vemb_v16_proto_put_u32(&p, req->vector_dim);
    vemb_v16_proto_put_u32(&p, req->flags);
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_alloc_req_decode(vemb_v16_alloc_req_t *req,
                                            const uint8_t *src,
                                            size_t len) {
    if (!req || !src || len != vemb_v16_alloc_req_encoded_len())
        return -1;
    const uint8_t *p = src;
    memset(req, 0, sizeof(*req));
    req->vector_dim = vemb_v16_proto_get_u32(&p);
    req->flags = vemb_v16_proto_get_u32(&p);
    return 0;
}

static inline size_t vemb_v16_channel_desc_encoded_len(
    const vemb_v16_channel_desc_t *desc) {
    uint32_t warm_region_count = desc ? desc->warm_region_count : 0;
    if (warm_region_count > VEMB_V16_MAX_DESC_WARM_REGIONS)
        warm_region_count = VEMB_V16_MAX_DESC_WARM_REGIONS;
    return 452u + (size_t)warm_region_count * 280u;
}

static inline int vemb_v16_channel_desc_encode(
    uint8_t *dst,
    size_t cap,
    const vemb_v16_channel_desc_t *desc,
    size_t *out_len) {
    if (!dst || !desc)
        return -1;
    size_t need = vemb_v16_channel_desc_encoded_len(desc);
    if (cap < need)
        return -1;
    uint8_t *p = dst;
    uint32_t warm_region_count = desc->warm_region_count;
    if (warm_region_count > VEMB_V16_MAX_DESC_WARM_REGIONS)
        warm_region_count = VEMB_V16_MAX_DESC_WARM_REGIONS;
    vemb_v16_proto_put_u32(&p, desc->magic);
    vemb_v16_proto_put_u32(&p, desc->version);
    vemb_v16_proto_put_u64(&p, desc->channel_id);
    vemb_v16_proto_put_u32(&p, desc->channel_index);
    vemb_v16_proto_put_u32(&p, desc->vector_dim);
    vemb_v16_proto_put_u32(&p, desc->vector_stride);
    vemb_v16_proto_put_u32(&p, desc->max_vectors);
    vemb_v16_proto_put_u32(&p, desc->request_ring_slot_size);
    vemb_v16_proto_put_u32(&p, desc->response_ring_slot_size);
    vemb_v16_proto_put_u32(&p, desc->warm_region_id);
    vemb_v16_proto_put_u32(&p, desc->warm_backend_type);
    vemb_v16_proto_put_u64(&p, desc->warm_region_bytes);
    vemb_v16_proto_put_u64(&p, desc->warm_mmap_offset);
    vemb_v16_proto_put_bytes(&p,
                             desc->request_ring_name,
                             sizeof(desc->request_ring_name));
    vemb_v16_proto_put_bytes(&p,
                             desc->response_ring_name,
                             sizeof(desc->response_ring_name));
    vemb_v16_proto_put_bytes(&p,
                             desc->vector_region_name,
                             sizeof(desc->vector_region_name));
    vemb_v16_proto_put_u32(&p, warm_region_count);
    for (uint32_t i = 0; i < warm_region_count; i++) {
        vemb_v16_proto_put_u32(&p, desc->warm_regions[i].region_id);
        vemb_v16_proto_put_u32(&p, desc->warm_regions[i].backend_type);
        vemb_v16_proto_put_u64(&p, desc->warm_regions[i].region_bytes);
        vemb_v16_proto_put_u64(&p, desc->warm_regions[i].mmap_offset);
        vemb_v16_proto_put_bytes(&p,
                                 desc->warm_regions[i].path,
                                 sizeof(desc->warm_regions[i].path));
    }
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_channel_desc_decode(vemb_v16_channel_desc_t *desc,
                                               const uint8_t *src,
                                               size_t len) {
    if (!desc || !src || len < 452u)
        return -1;
    const uint8_t *p = src;
    memset(desc, 0, sizeof(*desc));
    desc->magic = vemb_v16_proto_get_u32(&p);
    desc->version = vemb_v16_proto_get_u32(&p);
    desc->channel_id = vemb_v16_proto_get_u64(&p);
    desc->channel_index = vemb_v16_proto_get_u32(&p);
    desc->vector_dim = vemb_v16_proto_get_u32(&p);
    desc->vector_stride = vemb_v16_proto_get_u32(&p);
    desc->max_vectors = vemb_v16_proto_get_u32(&p);
    desc->request_ring_slot_size = vemb_v16_proto_get_u32(&p);
    desc->response_ring_slot_size = vemb_v16_proto_get_u32(&p);
    desc->warm_region_id = vemb_v16_proto_get_u32(&p);
    desc->warm_backend_type = vemb_v16_proto_get_u32(&p);
    desc->warm_region_bytes = vemb_v16_proto_get_u64(&p);
    desc->warm_mmap_offset = vemb_v16_proto_get_u64(&p);
    vemb_v16_proto_get_bytes(&p,
                             desc->request_ring_name,
                             sizeof(desc->request_ring_name));
    vemb_v16_proto_get_bytes(&p,
                             desc->response_ring_name,
                             sizeof(desc->response_ring_name));
    vemb_v16_proto_get_bytes(&p,
                             desc->vector_region_name,
                             sizeof(desc->vector_region_name));
    desc->warm_region_count = vemb_v16_proto_get_u32(&p);
    if (desc->warm_region_count > VEMB_V16_MAX_DESC_WARM_REGIONS)
        return -1;
    if (len != vemb_v16_channel_desc_encoded_len(desc))
        return -1;
    for (uint32_t i = 0; i < desc->warm_region_count; i++) {
        desc->warm_regions[i].region_id = vemb_v16_proto_get_u32(&p);
        desc->warm_regions[i].backend_type = vemb_v16_proto_get_u32(&p);
        desc->warm_regions[i].region_bytes = vemb_v16_proto_get_u64(&p);
        desc->warm_regions[i].mmap_offset = vemb_v16_proto_get_u64(&p);
        vemb_v16_proto_get_bytes(&p,
                                 desc->warm_regions[i].path,
                                 sizeof(desc->warm_regions[i].path));
    }
    return 0;
}

static inline size_t vemb_v16_req_encoded_len(const vemb_v16_req_t *req) {
    return 54u + (size_t)req->key_len + (size_t)req->key2_len +
           (size_t)req->vector_bytes;
}

static inline int vemb_v16_req_encode(uint8_t *dst,
                                      size_t cap,
                                      const vemb_v16_req_t *req,
                                      size_t *out_len) {
    if (!dst || !req)
        return -1;
    if (req->key_len > VEMB_V16_MAX_KEY_LEN ||
        req->key2_len > VEMB_V16_MAX_KEY_LEN ||
        req->vector_bytes > sizeof(req->vector)) {
        return -1;
    }
    size_t need = vemb_v16_req_encoded_len(req);
    if (cap < need)
        return -1;
    uint8_t *p = dst;
    *p++ = req->op;
    *p++ = req->flags;
    vemb_v16_proto_put_u32(&p, req->req_id);
    vemb_v16_proto_put_u64(&p, req->channel_id);
    vemb_v16_proto_put_u64(&p, req->key_hash);
    vemb_v16_proto_put_u32(&p, req->key_len);
    vemb_v16_proto_put_u32(&p, req->key2_len);
    vemb_v16_proto_put_u64(&p, req->key2_hash);
    vemb_v16_proto_put_u64(&p, req->topology_epoch);
    vemb_v16_proto_put_u32(&p, req->dim);
    vemb_v16_proto_put_u32(&p, req->vector_bytes);
    vemb_v16_proto_put_bytes(&p, req->key, req->key_len);
    vemb_v16_proto_put_bytes(&p, req->key2, req->key2_len);
    vemb_v16_proto_put_bytes(&p, req->vector, req->vector_bytes);
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_req_decode(vemb_v16_req_t *req,
                                      const uint8_t *src,
                                      size_t len) {
    if (!req || !src || len < 54u)
        return -1;
    const uint8_t *p = src;
    memset(req, 0, sizeof(*req));
    req->op = *p++;
    req->flags = *p++;
    req->req_id = vemb_v16_proto_get_u32(&p);
    req->channel_id = vemb_v16_proto_get_u64(&p);
    req->key_hash = vemb_v16_proto_get_u64(&p);
    req->key_len = vemb_v16_proto_get_u32(&p);
    req->key2_len = vemb_v16_proto_get_u32(&p);
    req->key2_hash = vemb_v16_proto_get_u64(&p);
    req->topology_epoch = vemb_v16_proto_get_u64(&p);
    req->dim = vemb_v16_proto_get_u32(&p);
    req->vector_bytes = vemb_v16_proto_get_u32(&p);
    if (req->key_len > VEMB_V16_MAX_KEY_LEN ||
        req->key2_len > VEMB_V16_MAX_KEY_LEN ||
        req->vector_bytes > sizeof(req->vector))
        return -1;
    if (len != vemb_v16_req_encoded_len(req))
        return -1;
    vemb_v16_proto_get_bytes(&p, req->key, req->key_len);
    vemb_v16_proto_get_bytes(&p, req->key2, req->key2_len);
    vemb_v16_proto_get_bytes(&p, req->vector, req->vector_bytes);
    return 0;
}

static inline size_t vemb_v16_resp_encoded_len(void) {
    return 56u;
}

static inline int vemb_v16_resp_encode(uint8_t *dst,
                                       size_t cap,
                                       const vemb_v16_resp_t *resp,
                                       size_t *out_len) {
    if (!dst || !resp || cap < vemb_v16_resp_encoded_len())
        return -1;
    uint8_t *p = dst;
    *p++ = resp->status;
    *p++ = resp->op;
    vemb_v16_proto_put_u16(&p, resp->flags);
    vemb_v16_proto_put_u32(&p, resp->req_id);
    vemb_v16_proto_put_u64(&p, resp->key_hash);
    vemb_v16_proto_put_u64(&p, resp->vector_offset);
    vemb_v16_proto_put_u32(&p, resp->vector_bytes);
    vemb_v16_proto_put_u32(&p, resp->dim);
    vemb_v16_proto_put_u32(&p, resp->region_id);
    vemb_v16_proto_put_u32(&p, resp->local_slot);
    vemb_v16_proto_put_u64(&p, resp->owner_generation);
    vemb_v16_proto_put_u32(&p, resp->redirect_owner);
    vemb_v16_proto_put_f32(&p, resp->score);
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_resp_decode(vemb_v16_resp_t *resp,
                                       const uint8_t *src,
                                       size_t len) {
    if (!resp || !src || len != vemb_v16_resp_encoded_len())
        return -1;
    const uint8_t *p = src;
    memset(resp, 0, sizeof(*resp));
    resp->status = *p++;
    resp->op = *p++;
    resp->flags = vemb_v16_proto_get_u16(&p);
    resp->req_id = vemb_v16_proto_get_u32(&p);
    resp->key_hash = vemb_v16_proto_get_u64(&p);
    resp->vector_offset = vemb_v16_proto_get_u64(&p);
    resp->vector_bytes = vemb_v16_proto_get_u32(&p);
    resp->dim = vemb_v16_proto_get_u32(&p);
    resp->region_id = vemb_v16_proto_get_u32(&p);
    resp->local_slot = vemb_v16_proto_get_u32(&p);
    resp->owner_generation = vemb_v16_proto_get_u64(&p);
    resp->redirect_owner = vemb_v16_proto_get_u32(&p);
    resp->score = vemb_v16_proto_get_f32(&p);
    return 0;
}

#endif
