#ifndef __SUPERNODE_PROTOCOL_H
#define __SUPERNODE_PROTOCOL_H

#include <stdint.h>

#define BATCH_PACKET_MAGIC 0xCAC0BEEF
#define BATCH_PACKET_FLAG_FC_POINTERS UINT32_C(0x80000000)

typedef struct proxy_vector_request proxy_vector_request_t;

typedef enum batchPacketOpType {
    BATCH_PACKET_OP_VEMB = 1,
    BATCH_PACKET_OP_VSIM = 2,
} batchPacketOpType;

typedef struct batch_result_packet {
    uint32_t magic;
    uint32_t packet_size;
    uint32_t op_type;
    uint32_t status;
    uint64_t batch_id;
    uint64_t request_id;
    uint32_t dim;
    uint32_t reserved;
    float data[];
} __attribute__((packed)) batch_result_packet_t;

typedef struct batch_vsim_result_entry {
    uint64_t row_id;
    float score;
} __attribute__((packed)) batch_vsim_result_entry_t;

typedef struct batch_vsim_result_packet {
    uint32_t magic;
    uint32_t packet_size;
    uint32_t op_type;
    uint32_t status;
    uint64_t batch_id;
    uint64_t request_id;
    uint32_t num_results;
    uint32_t reserved;
    batch_vsim_result_entry_t results[];
} __attribute__((packed)) batch_vsim_result_packet_t;

typedef struct batch_request_header {
    uint32_t magic;                     /* 魔数：BATCH_PACKET_MAGIC */
    uint32_t packet_size;               /* 包大小 */
    uint32_t num_requests;              /* 请求数量 */
    uint32_t op_type;                   /* 批次操作类型 */
    uint32_t supernode_id;              /* 目标超节点 ID */
    uint32_t worker_id;                 /* 目标 Worker ID */
    uint64_t timestamp_us;              /* 时间戳 */
    uint64_t batch_id;                  /* 批次 ID */
} __attribute__((packed)) batch_request_header_t;

typedef struct batch_packet {
    batch_request_header_t hdr;
    struct {
        uint64_t request_id;
        uint64_t row_id;                /* UB row id */
    } requests[];
} __attribute__((packed)) batch_packet_t;

typedef struct fc_vemb_packet {
    batch_request_header_t hdr;
    struct {
        uint64_t row_id;
        proxy_vector_request_t *owner;  /* In-process Redis blocked request */
    } requests[];
} __attribute__((packed)) fc_vemb_packet_t;

typedef struct batch_vsim_packet {
    batch_request_header_t hdr;
    uint32_t flags;
    uint64_t request_id;
    uint32_t query_dim;
    uint32_t requested_count;
    uint32_t candidate_count;
    uint32_t reserved;
    float payload[];
} __attribute__((packed)) batch_vsim_packet_t;

#endif /* __SUPERNODE_PROTOCOL_H */
