#ifndef __SUPERNODE_PROTOCOL_H
#define __SUPERNODE_PROTOCOL_H

#include <stdint.h>

#define BATCH_PACKET_MAGIC 0xCAC0BEEF

typedef struct batch_packet {
    uint32_t magic;                     /* 魔数：BATCH_PACKET_MAGIC */
    uint32_t packet_size;               /* 包大小 */
    uint32_t num_requests;              /* 请求数量 */
    uint32_t supernode_id;              /* 目标超节点 ID */
    uint32_t worker_id;                 /* 目标 Worker ID */
    uint64_t timestamp_us;              /* 时间戳 */
    uint64_t batch_id;                  /* 批次 ID */

    struct {
        uint64_t request_id;
        uint64_t key_hash;              /* Key 的哈希值 */
    } requests[];
} __attribute__((packed)) batch_packet_t;

#endif /* __SUPERNODE_PROTOCOL_H */
