/*
 * Proxy Aggregator - Smart Batching Layer
 * 实现微秒级等待积攒批量请求，通过一致性哈希分发到超节点
 * 
 * 设计目标：
 * - 积攒 3000-6000 个请求为一批
 * - 微秒级等待（200-500μs）
 * - 一致性哈希分片
 * - 零拷贝 Ring Buffer 通信
 */

#ifndef __PROXY_AGGREGATOR_H
#define __PROXY_AGGREGATOR_H

#include "ring_buffer.h"
#include <stdint.h>

/* 聚合配置 */
#define PROXY_BATCH_LIMIT 3000          /* 默认批量大小限制 */
#define PROXY_TIME_LIMIT_US 200         /* 默认微秒级等待时间 */
#define PROXY_MAX_SUPERNODES 150        /* 默认最大超节点数量 */
/* 请求结构 */


/* ========== API 函数 ========== */

/* 初始化和清理 */
int proxy_aggregator_init(int num_supernodes);
void proxy_aggregator_shutdown(void);

/* 请求提交 */
int proxy_enqueue_request(const char *key, void *client_ctx, 
                         float *result_buffer, size_t vector_dim);

/* 查询接口 */
ring_buffer_t *proxy_aggregator_get_worker_rb(int supernode_id, int worker_id);
int proxy_aggregator_get_workers_per_supernode(void);

/* 统计信息 */
sds proxy_aggregator_get_stats(void);

#endif /* __PROXY_AGGREGATOR_H */
