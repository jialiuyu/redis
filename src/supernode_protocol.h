#ifndef __SUPERNODE_PROTOCOL_H
#define __SUPERNODE_PROTOCOL_H

#include <stdint.h>

#define BATCH_PACKET_MAGIC 0xCAC0BEEF

typedef enum batchPacketOpType {
    BATCH_PACKET_OP_VEMB = 1,
} batchPacketOpType;

typedef struct batch_result_packet {
    uint32_t magic;
    uint32_t packet_size;
    uint32_t op_type;
    uint32_t status;
    uint64_t request_id;
    uint32_t dim;
    uint32_t reserved;
    float data[];
} __attribute__((packed)) batch_result_packet_t;

typedef struct batch_packet {
    uint32_t magic;                     /* 魔数：BATCH_PACKET_MAGIC */
    uint32_t packet_size;               /* 包大小 */
    uint32_t num_requests;              /* 请求数量 */
    uint32_t op_type;                   /* 批次操作类型 */
    uint32_t supernode_id;              /* 目标超节点 ID */
    uint32_t worker_id;                 /* 目标 Worker ID */
    uint64_t timestamp_us;              /* 时间戳 */
    uint64_t batch_id;                  /* 批次 ID */

    struct {
        uint64_t request_id;
        uint64_t row_id;                /* UB row id */
    } requests[];
} __attribute__((packed)) batch_packet_t;

#endif /* __SUPERNODE_PROTOCOL_H */
