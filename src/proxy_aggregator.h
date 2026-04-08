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

#include "server.h"
#include <stdint.h>
#include <pthread.h>
#include <stdatomic.h>

/* 聚合配置 */
#define PROXY_BATCH_LIMIT 3000          /* 批量大小限制 */
#define PROXY_TIME_LIMIT_US 200         /* 微秒级等待时间 */
#define PROXY_MAX_SUPERNODES 150        /* 最大超节点数量 */
#define PROXY_HASH_RING_SIZE 1024       /* 一致性哈希环大小 */

/* Ring Buffer 配置 */
#define RING_BUFFER_SIZE (16 * 1024 * 1024)  /* 16MB Ring Buffer */
#define RING_BUFFER_BATCH_SIZE (256 * 1024)  /* 256KB per batch packet */

/* 请求结构 */
typedef struct proxy_request {
    uint64_t request_id;
    char *key;                          /* embedding key */
    uint64_t submit_time_us;
    void *client_context;               /* 客户端上下文 */
    int completed;
    int error_code;
    float *result_vector;               /* 结果向量 */
    size_t vector_dim;
} proxy_request_t;

/* 批量桶结构 - 对应不同超节点 */
typedef struct batch_bucket {
    proxy_request_t **requests;         /* 请求数组 */
    size_t count;                       /* 当前请求数 */
    size_t capacity;                    /* 容量 */
    int target_supernode_id;            /* 目标超节点 ID */
    uint64_t last_flush_time_us;       /* 上次刷新时间 */
    pthread_mutex_t mutex;
} batch_bucket_t;

/* 批量数据包 - 用于 Ring Buffer 传输 */
typedef struct batch_packet {
    uint32_t magic;                     /* 魔数：0xCAC0BEEF */
    uint32_t packet_size;               /* 包大小 */
    uint32_t num_requests;              /* 请求数量 */
    uint32_t supernode_id;              /* 目标超节点 ID */
    uint64_t timestamp_us;              /* 时间戳 */
    uint64_t batch_id;                  /* 批次 ID */
    
    /* 请求数据 */
    struct {
        uint64_t request_id;
        uint64_t key_hash;              /* Key 的哈希值 */
        uint32_t key_len;
        char key_data[256];             /* Key 数据 */
    } requests[];
} __attribute__((packed)) batch_packet_t;

/* Ring Buffer 结构 */
typedef struct ring_buffer {
    volatile uint64_t head;             /* 读指针（原子操作）*/
    volatile uint64_t tail;             /* 写指针（原子操作）*/
    uint8_t *buffer;                    /* 缓冲区 */
    size_t size;                        /* 缓冲区大小 */
    int fd;                             /* 共享内存 fd */
    pthread_mutex_t write_mutex;        /* 写锁 */
} ring_buffer_t;

/* 一致性哈希节点 */
typedef struct hash_node {
    uint32_t hash_value;                /* 哈希值 */
    int supernode_id;                   /* 超节点 ID */
} hash_node_t;

/* 一致性哈希环 */
typedef struct consistent_hash_ring {
    hash_node_t *nodes;                 /* 哈希节点数组 */
    size_t num_nodes;                   /* 节点数量 */
    size_t capacity;                    /* 容量 */
    pthread_rwlock_t lock;              /* 读写锁 */
} consistent_hash_ring_t;

/* Proxy 聚合器主结构 */
typedef struct proxy_aggregator {
    /* 批量桶 - 每个超节点一个 */
    batch_bucket_t *buckets;
    size_t num_buckets;
    
    /* 一致性哈希环 */
    consistent_hash_ring_t *hash_ring;
    
    /* Ring Buffer - 每个超节点一个 */
    ring_buffer_t **ring_buffers;
    size_t num_ring_buffers;
    
    /* 工作线程 */
    pthread_t flush_thread;
    int running;
    
    /* 统计信息 */
    atomic_uint_fast64_t total_requests;
    atomic_uint_fast64_t total_batches;
    atomic_uint_fast64_t total_flushes;
    atomic_uint_fast64_t batch_full_flushes;
    atomic_uint_fast64_t timeout_flushes;
    
} proxy_aggregator_t;

/* 全局 Proxy 聚合器实例 */
extern proxy_aggregator_t *global_proxy_aggregator;

/* ========== API 函数 ========== */

/* 初始化和清理 */
int proxy_aggregator_init(int num_supernodes);
void proxy_aggregator_shutdown(void);

/* 请求提交 */
int proxy_enqueue_request(const char *key, void *client_ctx, 
                         float *result_buffer, size_t vector_dim);

/* 一致性哈希 */
int consistent_hash_init(consistent_hash_ring_t **ring, int num_supernodes);
void consistent_hash_destroy(consistent_hash_ring_t *ring);
int consistent_hash_get_node(consistent_hash_ring_t *ring, const char *key);
int consistent_hash_add_node(consistent_hash_ring_t *ring, int supernode_id);
int consistent_hash_remove_node(consistent_hash_ring_t *ring, int supernode_id);

/* Ring Buffer 操作 */
ring_buffer_t *ring_buffer_create(size_t size, const char *name);
void ring_buffer_destroy(ring_buffer_t *rb);
int ring_buffer_push(ring_buffer_t *rb, const void *data, size_t len);
int ring_buffer_pop(ring_buffer_t *rb, void *data, size_t max_len, size_t *actual_len);
size_t ring_buffer_available_space(ring_buffer_t *rb);
size_t ring_buffer_available_data(ring_buffer_t *rb);

/* 批量处理 */
int flush_batch(batch_bucket_t *bucket, ring_buffer_t *rb);
void *flush_thread_func(void *arg);

/* 序列化 */
batch_packet_t *serialize_batch_for_sve(batch_bucket_t *bucket);

/* 工具函数 */
uint32_t murmur3_hash(const char *key, size_t len);
uint64_t get_time_us(void);

/* 统计信息 */
sds proxy_aggregator_get_stats(void);

#endif /* __PROXY_AGGREGATOR_H */
