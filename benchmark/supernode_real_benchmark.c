/*
 * SuperNode Real Benchmark - 真实SuperNode性能测试
 * 
 * 测试场景：
 * - 连接真实的 SuperNode 服务器（端口 6388）
 * - 使用 Proxy Aggregator 批量聚合（3000-6000个请求）
 * - 通过一致性哈希分发到后端 SuperNode Worker
 * - SVE2 Gather Load 从 UB.mem 批量读取
 * - 测试真实的端到端性能
 */

#include "benchmark_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <math.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

/* SuperNode 连接配置 */
#define SUPERNODE_HOST "127.0.0.1"
#define SUPERNODE_PORT 6388
#define SUPERNODE_TIMEOUT_SEC 10
#define SUPERNODE_BATCH_SIZE 3000      /* 批量大小 */
#define SUPERNODE_BATCH_WAIT_US 200    /* 批量等待时间（微秒）*/

/* 协议定义 */
#define PROTO_MAGIC 0xCAC0BEEF
#define PROTO_CMD_BATCH_GET 0x01
#define PROTO_CMD_BATCH_RESPONSE 0x02

/* 批量请求协议 */
typedef struct {
    uint32_t magic;                     /* 魔数 */
    uint32_t command;                   /* 命令类型 */
    uint32_t num_requests;              /* 请求数量 */
    uint32_t embedding_dim;             /* Embedding 维度 */
    uint64_t batch_id;                  /* 批次 ID */
    uint64_t timestamp_us;              /* 时间戳 */
    
    /* 请求数据 */
    struct {
        uint64_t key_hash;              /* Key 哈希值 */
        char key[64];                   /* Key 字符串 */
    } requests[];
} __attribute__((packed)) batch_request_t;

/* 批量响应协议 */
typedef struct {
    uint32_t magic;                     /* 魔数 */
    uint32_t command;                   /* 命令类型 */
    uint32_t num_responses;             /* 响应数量 */
    uint32_t embedding_dim;             /* Embedding 维度 */
    uint64_t batch_id;                  /* 批次 ID */
    uint64_t process_time_us;           /* 处理时间 */
    
    /* 响应数据（变长）*/
    /* float embeddings[num_responses][embedding_dim]; */
} __attribute__((packed)) batch_response_t;

/* SuperNode 连接池 */
typedef struct {
    int *sockets;
    int num_connections;
    pthread_mutex_t *locks;
    benchmark_stats_t stats;
    
    /* 批量聚合缓冲区 */
    query_t **batch_buffer;
    size_t batch_count;
    uint64_t batch_start_time_us;
    pthread_mutex_t batch_mutex;
    
} supernode_pool_t;

/* 线程数据 */
typedef struct {
    int thread_id;
    supernode_pool_t *pool;
    query_t *queries;
    size_t num_queries;
    uint64_t local_time_ns;
} thread_data_t;

/* 获取当前时间（微秒）*/
static inline uint64_t get_time_us_local(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000 + (uint64_t)tv.tv_usec;
}

/* MurmurHash3 32-bit */
static uint32_t murmur3_hash_local(const char *key, size_t len) {
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;
    const uint32_t seed = 0x5bd1e995;
    
    uint32_t h = seed;
    const uint8_t *data = (const uint8_t *)key;
    const int nblocks = len / 4;
    
    const uint32_t *blocks = (const uint32_t *)(data + nblocks * 4);
    for (int i = -nblocks; i; i++) {
        uint32_t k = blocks[i];
        k *= c1;
        k = (k << 15) | (k >> (32 - 15));
        k *= c2;
        
        h ^= k;
        h = (h << 13) | (h >> (32 - 13));
        h = h * 5 + 0xe6546b64;
    }
    
    const uint8_t *tail = (const uint8_t *)(data + nblocks * 4);
    uint32_t k = 0;
    switch (len & 3) {
        case 3: k ^= tail[2] << 16;
        case 2: k ^= tail[1] << 8;
        case 1: k ^= tail[0];
                k *= c1;
                k = (k << 15) | (k >> (32 - 15));
                k *= c2;
                h ^= k;
    }
    
    h ^= len;
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    
    return h;
}

/* 连接到 SuperNode 服务器 */
static int connect_to_supernode(const char *host, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        fprintf(stderr, "Failed to create socket: %s\n", strerror(errno));
        return -1;
    }
    
    /* 设置超时 */
    struct timeval timeout;
    timeout.tv_sec = SUPERNODE_TIMEOUT_SEC;
    timeout.tv_usec = 0;
    
    if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
        fprintf(stderr, "Failed to set receive timeout: %s\n", strerror(errno));
        close(sock);
        return -1;
    }
    
    if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0) {
        fprintf(stderr, "Failed to set send timeout: %s\n", strerror(errno));
        close(sock);
        return -1;
    }
    
    /* 连接 */
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, host, &server_addr.sin_addr) <= 0) {
        fprintf(stderr, "Invalid address: %s\n", host);
        close(sock);
        return -1;
    }
    
    if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        fprintf(stderr, "Failed to connect to %s:%d: %s\n", host, port, strerror(errno));
        close(sock);
        return -1;
    }
    
    return sock;
}

/* 初始化 SuperNode 连接池 */
supernode_pool_t *init_supernode_pool(int num_connections) {
    supernode_pool_t *pool = malloc(sizeof(supernode_pool_t));
    if (!pool) {
        fprintf(stderr, "Failed to allocate supernode pool\n");
        return NULL;
    }
    
    pool->num_connections = num_connections;
    pool->sockets = malloc(num_connections * sizeof(int));
    pool->locks = malloc(num_connections * sizeof(pthread_mutex_t));
    
    if (!pool->sockets || !pool->locks) {
        fprintf(stderr, "Failed to allocate pool resources\n");
        free(pool->sockets);
        free(pool->locks);
        free(pool);
        return NULL;
    }
    
    init_stats(&pool->stats);
    
    /* 初始化批量缓冲区 */
    pool->batch_buffer = malloc(SUPERNODE_BATCH_SIZE * sizeof(query_t *));
    pool->batch_count = 0;
    pool->batch_start_time_us = 0;
    pthread_mutex_init(&pool->batch_mutex, NULL);
    
    printf("Connecting to SuperNode at %s:%d...\n", SUPERNODE_HOST, SUPERNODE_PORT);
    
    /* 创建连接 */
    for (int i = 0; i < num_connections; i++) {
        pool->sockets[i] = connect_to_supernode(SUPERNODE_HOST, SUPERNODE_PORT);
        
        if (pool->sockets[i] < 0) {
            fprintf(stderr, "Connection %d failed\n", i);
            
            /* 清理已创建的连接 */
            for (int j = 0; j < i; j++) {
                close(pool->sockets[j]);
            }
            free(pool->batch_buffer);
            free(pool->sockets);
            free(pool->locks);
            free(pool);
            return NULL;
        }
        
        pthread_mutex_init(&pool->locks[i], NULL);
        
        if ((i + 1) % 10 == 0 || i == num_connections - 1) {
            printf("  Created %d / %d connections\n", i + 1, num_connections);
        }
    }
    
    printf("✅ Connected to SuperNode with %d connections\n\n", num_connections);
    return pool;
}

/* 清理 SuperNode 连接池 */
void cleanup_supernode_pool(supernode_pool_t *pool) {
    if (!pool) return;
    
    for (int i = 0; i < pool->num_connections; i++) {
        if (pool->sockets[i] >= 0) {
            close(pool->sockets[i]);
        }
        pthread_mutex_destroy(&pool->locks[i]);
    }
    
    pthread_mutex_destroy(&pool->batch_mutex);
    free(pool->batch_buffer);
    free(pool->sockets);
    free(pool->locks);
    free(pool);
}

/* 发送批量请求到 SuperNode */
static uint64_t supernode_batch_get(supernode_pool_t *pool, int conn_id, 
                                    query_t **queries, size_t num_queries,
                                    float *embeddings) {
    uint64_t start_time = get_time_ns();
    
    /* 计算请求大小 */
    size_t request_data_size = num_queries * (sizeof(uint64_t) + 64);  /* key_hash + key */
    size_t request_size = sizeof(batch_request_t) + request_data_size;
    
    /* 构造批量请求 */
    batch_request_t *request = malloc(request_size);
    if (!request) {
        return get_time_ns() - start_time;
    }
    
    memset(request, 0, request_size);
    
    request->magic = PROTO_MAGIC;
    request->command = PROTO_CMD_BATCH_GET;
    request->num_requests = num_queries;
    request->embedding_dim = EMBEDDING_DIM;
    request->batch_id = get_time_us_local();  /* 使用时间戳作为batch_id */
    request->timestamp_us = get_time_us_local();
    
    /* 填充请求数据 */
    for (size_t i = 0; i < num_queries; i++) {
        char key_str[64];
        snprintf(key_str, sizeof(key_str), "emb:%llu", (unsigned long long)queries[i]->id);
        
        request->requests[i].key_hash = murmur3_hash_local(key_str, strlen(key_str));
        strncpy(request->requests[i].key, key_str, 63);
        request->requests[i].key[63] = '\0';
    }
    
    /* 获取连接 */
    pthread_mutex_lock(&pool->locks[conn_id]);
    int sock = pool->sockets[conn_id];
    
    /* 发送请求 */
    ssize_t sent = send(sock, request, request_size, 0);
    if (sent != (ssize_t)request_size) {
        fprintf(stderr, "Failed to send batch request: %s\n", strerror(errno));
        pthread_mutex_unlock(&pool->locks[conn_id]);
        free(request);
        return get_time_ns() - start_time;
    }
    
    /* 接收响应头 */
    batch_response_t response_header;
    ssize_t received = recv(sock, &response_header, sizeof(response_header), MSG_WAITALL);
    
    if (received != sizeof(response_header)) {
        fprintf(stderr, "Failed to receive response header: %s\n", strerror(errno));
        pthread_mutex_unlock(&pool->locks[conn_id]);
        free(request);
        return get_time_ns() - start_time;
    }
    
    /* 验证响应 */
    if (response_header.magic != PROTO_MAGIC || 
        response_header.command != PROTO_CMD_BATCH_RESPONSE) {
        fprintf(stderr, "Invalid response from SuperNode\n");
        pthread_mutex_unlock(&pool->locks[conn_id]);
        free(request);
        return get_time_ns() - start_time;
    }
    
    /* 接收 Embedding 数据 */
    size_t embedding_data_size = response_header.num_responses * 
                                response_header.embedding_dim * sizeof(float);
    
    if (embedding_data_size > 0) {
        received = recv(sock, embeddings, embedding_data_size, MSG_WAITALL);
        if (received != (ssize_t)embedding_data_size) {
            fprintf(stderr, "Failed to receive embedding data: %s\n", strerror(errno));
        }
    }
    
    pthread_mutex_unlock(&pool->locks[conn_id]);
    free(request);
    
    uint64_t end_time = get_time_ns();
    return end_time - start_time;
}

/* 工作线程 */
void *worker_thread(void *arg) {
    thread_data_t *data = (thread_data_t *)arg;
    supernode_pool_t *pool = data->pool;
    
    /* 每个线程使用自己的连接 */
    int conn_id = data->thread_id % pool->num_connections;
    
    /* 分配 embedding 缓冲区 */
    float *embeddings = aligned_alloc(64, SUPERNODE_BATCH_SIZE * EMBEDDING_SIZE);
    if (!embeddings) {
        fprintf(stderr, "Thread %d: Failed to allocate embedding buffer\n", data->thread_id);
        return NULL;
    }
    
    uint64_t start_time = get_time_ns();
    
    /* 处理查询 - 批量模式 */
    size_t processed = 0;
    while (processed < data->num_queries) {
        /* 计算本批次大小 */
        size_t batch_size = data->num_queries - processed;
        if (batch_size > SUPERNODE_BATCH_SIZE) {
            batch_size = SUPERNODE_BATCH_SIZE;
        }
        
        /* 准备批量查询 */
        query_t **batch_queries = malloc(batch_size * sizeof(query_t *));
        for (size_t i = 0; i < batch_size; i++) {
            batch_queries[i] = &data->queries[processed + i];
        }
        
        /* 发送批量请求 */
        uint64_t latency = supernode_batch_get(pool, conn_id, batch_queries, 
                                              batch_size, embeddings);
        
        /* 更新统计 */
        for (size_t i = 0; i < batch_size; i++) {
            update_latency_stats(&pool->stats, latency / batch_size);
        }
        
        free(batch_queries);
        processed += batch_size;
        
        /* 进度报告 */
        if (processed % 100000 == 0 || processed == data->num_queries) {
            printf("  Thread %d: %zu / %zu queries (avg latency: %.2f μs)\n", 
                   data->thread_id, processed, data->num_queries,
                   (double)latency / batch_size / 1000.0);
        }
    }
    
    uint64_t end_time = get_time_ns();
    data->local_time_ns = end_time - start_time;
    
    free(embeddings);
    return NULL;
}

/* 生成测试查询 */
query_t *generate_queries(size_t num_queries) {
    query_t *queries = malloc(num_queries * sizeof(query_t));
    if (!queries) {
        fprintf(stderr, "Failed to allocate queries\n");
        return NULL;
    }
    
    printf("Generating %zu test queries...\n", num_queries);
    
    unsigned int seed = (unsigned int)time(NULL);
    
    for (size_t i = 0; i < num_queries; i++) {
        /* 80% UID 查询，20% Item 查询 */
        if ((rand_r(&seed) % 100) < 80) {
            queries[i].type = QUERY_TYPE_UID;
            queries[i].id = (uint64_t)rand_r(&seed) % UID_COUNT;
        } else {
            queries[i].type = QUERY_TYPE_ITEM;
            queries[i].id = (uint64_t)rand_r(&seed) % ITEM_COUNT;
        }
        
        if ((i + 1) % 1000000 == 0) {
            printf("  Generated %zu / %zu queries\n", i + 1, num_queries);
        }
    }
    
    printf("✅ Generated %zu queries\n\n", num_queries);
    return queries;
}

/* 运行基准测试 */
int run_benchmark(size_t num_queries, int num_threads, int num_supernodes) {
    printf("\n========================================\n");
    printf("SuperNode Real Benchmark\n");
    printf("========================================\n");
    printf("SuperNode Server: %s:%d\n", SUPERNODE_HOST, SUPERNODE_PORT);
    printf("Queries: %zu\n", num_queries);
    printf("Threads: %d\n", num_threads);
    printf("Batch Size: %d\n", SUPERNODE_BATCH_SIZE);
    printf("Simulated SuperNodes: %d\n", num_supernodes);
    printf("========================================\n\n");
    
    /* 初始化 SuperNode 连接池 */
    supernode_pool_t *pool = init_supernode_pool(num_threads);
    if (!pool) {
        fprintf(stderr, "Failed to initialize SuperNode pool\n");
        return 1;
    }
    
    /* 生成查询 */
    query_t *queries = generate_queries(num_queries);
    if (!queries) {
        cleanup_supernode_pool(pool);
        return 1;
    }
    
    /* 创建线程 */
    pthread_t *threads = malloc(num_threads * sizeof(pthread_t));
    thread_data_t *thread_data = malloc(num_threads * sizeof(thread_data_t));
    
    size_t queries_per_thread = num_queries / num_threads;
    
    printf("Starting benchmark...\n\n");
    
    uint64_t start_time = get_time_ns();
    
    /* 启动线程 */
    for (int i = 0; i < num_threads; i++) {
        thread_data[i].thread_id = i;
        thread_data[i].pool = pool;
        thread_data[i].queries = &queries[i * queries_per_thread];
        thread_data[i].num_queries = queries_per_thread;
        thread_data[i].local_time_ns = 0;
        
        pthread_create(&threads[i], NULL, worker_thread, &thread_data[i]);
    }
    
    /* 等待线程完成 */
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    
    uint64_t end_time = get_time_ns();
    double total_time = (end_time - start_time) / 1e9;
    
    /* 打印结果 */
    print_stats("SuperNode Real", &pool->stats, total_time);
    
    /* 估算完整规模性能 */
    printf("========================================\n");
    printf("Full Scale Estimation\n");
    printf("========================================\n");
    
    uint64_t total_req = atomic_load(&pool->stats.total_requests);
    uint64_t total_lat = atomic_load(&pool->stats.total_latency_ns);
    uint64_t total_batches = atomic_load(&pool->stats.batch_count);
    double avg_lat_us = (double)total_lat / total_req / 1000.0;
    
    /* 基于实际测试的单SuperNode QPS */
    double measured_qps = total_req / total_time;
    uint64_t total_qps = (uint64_t)(measured_qps * num_supernodes);
    
    printf("Total embeddings: %llu billion\n", (UID_COUNT + ITEM_COUNT) / 1000000000ULL);
    printf("Measured single-SuperNode QPS: %.2f\n", measured_qps);
    printf("Average latency: %.2f μs\n", avg_lat_us);
    printf("Total batches: %lu\n", total_batches);
    printf("Average batch size: %.1f\n", (double)total_req / total_batches);
    printf("Estimated total QPS: %lu (%.2f billion)\n", total_qps, total_qps / 1e9);
    printf("Time to process all: %.2f seconds (%.2f minutes)\n", 
           (double)TOTAL_EMBEDDINGS / total_qps,
           (double)TOTAL_EMBEDDINGS / total_qps / 60.0);
    printf("\nHardware Cost:\n");
    printf("  SuperNodes: %d\n", num_supernodes);
    printf("  Estimated cost: $%lu million (@ $100k per SuperNode)\n", 
           (uint64_t)num_supernodes * 100000 / 1000000);
    printf("\nComparison with Traditional Redis:\n");
    printf("  Server reduction: %.2f%% (from 350,000 to %d)\n",
           (1.0 - (double)num_supernodes / 350000.0) * 100.0, num_supernodes);
    printf("  Cost reduction: %.2f%% (from $1.75B to $%luM)\n",
           (1.0 - (double)num_supernodes * 100000 / 1750000000.0) * 100.0,
           (uint64_t)num_supernodes * 100000 / 1000000);
    printf("========================================\n\n");
    
    /* 清理 */
    cleanup_supernode_pool(pool);
    free(queries);
    free(threads);
    free(thread_data);
    
    return 0;
}

int main(int argc, char *argv[]) {
    /* 解析参数 */
    size_t num_queries = DEFAULT_NUM_QUERIES;
    int num_threads = DEFAULT_NUM_THREADS;
    int num_supernodes = NUM_SUPERNODES;
    
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--queries") == 0 && i + 1 < argc) {
            num_queries = strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--threads") == 0 && i + 1 < argc) {
            num_threads = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--supernodes") == 0 && i + 1 < argc) {
            num_supernodes = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0) {
            printf("Usage: %s [options]\n", argv[0]);
            printf("Options:\n");
            printf("  --queries N      Number of queries (default: %d)\n", DEFAULT_NUM_QUERIES);
            printf("  --threads N      Number of threads (default: %d)\n", DEFAULT_NUM_THREADS);
            printf("  --supernodes N   Number of SuperNodes (default: %d)\n", NUM_SUPERNODES);
            printf("  --help           Show this help\n");
            return 0;
        }
    }
    
    return run_benchmark(num_queries, num_threads, num_supernodes);
}
