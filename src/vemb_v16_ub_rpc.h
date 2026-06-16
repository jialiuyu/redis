#ifndef __VEMB_V16_UB_RPC_H
#define __VEMB_V16_UB_RPC_H

#include "vemb_v16_protocol.h"
#include "vemb_v16_tlc.h"

#include <stdint.h>

#define VEMB_V16_UB_RPC_MAX_PEERS VEMB_V16_TLC_MAX_REMOTE_META_VIEWS

typedef struct vemb_v16_ub_rpc_ring_config {
    uint32_t backend_type;
    uint64_t mmap_offset;
    char path[256];
} vemb_v16_ub_rpc_ring_config_t;

typedef struct vemb_v16_ub_rpc_peer {
    uint32_t owner_id;
    vemb_v16_ub_rpc_ring_config_t request;
    vemb_v16_ub_rpc_ring_config_t response;
    vemb_v16_ub_rpc_ring_config_t inbound_request;
    vemb_v16_ub_rpc_ring_config_t outbound_response;
} vemb_v16_ub_rpc_peer_t;

typedef struct vemb_v16_ub_rpc vemb_v16_ub_rpc_t;

int vemb_v16_ub_rpc_create(vemb_v16_ub_rpc_t **out,
                           vemb_v16_tlc_t *tlc,
                           uint32_t local_owner_id,
                           uint32_t timeout_ms,
                           const vemb_v16_ub_rpc_peer_t *peers,
                           uint32_t peer_count);
void vemb_v16_ub_rpc_destroy(vemb_v16_ub_rpc_t *rpc);
int vemb_v16_ub_rpc_lookup(void *arg,
                           const vemb_v16_ub_lookup_rpc_req_t *req,
                           vemb_v16_ub_lookup_rpc_resp_t *resp);

#endif
