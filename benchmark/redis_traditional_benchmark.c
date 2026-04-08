/*
 * 传统 Redis 集群性能测试（C 实现）
 * 
 * 真实 Redis 服务器测试：
 * - 连接到真实的 Redis 服务器
 * - 测试实际的读写性能
 * - 获取真实的 baseline 数据
 */

#include "benchmark_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <math.h>
#include "../deps/hiredis/hiredis.h"

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

/* Redis 连接配置 */
#define REDIS_HOST "127.0.0.1"
#define REDIS_PORT 6381
#define REDIS_TIMEOUT_SEC 5

/* Redis 连接池 */
typedef struct {
    redisContext **contexts;
    int num_connections;
    pthread_mutex_t *locks;
    benchmark_stats_t stats;
} redis_pool_t;

/* 线程数据 */
typedef struct {
    int thread_id;
    redis_pool_t *pool;
    query_t *queries;
    size_t num_queries;
    uint64_t local_time_ns;
} thread_data_t;

/* 初始化 Redis 连接池 */
redis_pool_t *init_redis_pool(int num_connections) {
    redis_pool_t *pool = malloc(sizeof(redis_pool_t));
    if (!pool) {
        fprintf(stderr, "Failed to allocate redis pool\n");
        return NULL;
    }
    
    pool->num_connections = num_connections;
    pool->contexts = malloc(num_connections * sizeof(redisContext *));
    pool->locks = malloc(num_connections * sizeof(pthread_mutex_t));
    
    if (!pool->contexts || !pool->locks) {
        fprintf(stderr, "Failed to allocate pool resources\n");
        free(pool->contexts);
        free(pool->locks);
        free(pool);
        return NULL;
    }
    
    init_stats(&pool->stats);
    
    printf("Connecting to Redis at %s:%d...\n", REDIS_HOST, REDIS_PORT);
    
    /* 创建连接 */
    for (int i = 0; i < num_connections; i++) {
        struct timeval timeout = { REDIS_TIMEOUT_SEC, 0 };
        pool->contexts[i] = redisConnectWithTimeout(REDIS_HOST, REDIS_PORT, timeout);
        
        if (pool->contexts[i] == NULL || pool->contexts[i]->err) {
            if (pool->contexts[i]) {
                fprintf(stderr, "Connection %d error: %s\n", i, pool->contexts[i]->errstr);
                redisFree(pool->contexts[i]);
            } else {
                fprintf(stderr, "Connection %d error: can't allocate redis context\n", i);
            }
            
            /* 清理已创建的连接 */
            for (int j = 0; j < i; j++) {
                redisFree(pool->contexts[j]);
            }
            free(pool->contexts);
            free(pool->locks);
            free(pool);
            return NULL;
        }
        
        pthread_mutex_init(&pool->locks[i], NULL);
        
        if ((i + 1) % 10 == 0 || i == num_connections - 1) {
            printf("  Created %d / %d connections\n", i + 1, num_connections);
        }
    }
    
    printf("✅ Connected to Redis with %d connections\n\n", num_connections);
    return pool;
}

/* 清理 Redis 连接池 */
void cleanup_redis_pool(redis_pool_t *pool) {
    if (!pool) return;
    
    for (int i = 0; i < pool->num_connections; i++) {
        if (pool->contexts[i]) {
            redisFree(pool->contexts[i]);
        }
        pthread_mutex_destroy(&pool->locks[i]);
    }
    
    free(pool->contexts);
    free(pool->locks);
    free(pool);
}

/* 从 Redis 获取 Embedding */
static uint64_t redis_get_embedding(redis_pool_t *pool, int conn_id, uint64_t key, float *embedding) {
    uint64_t start_time = get_time_ns();
    
    /* 构造 key */
    char key_str[64];
    snprintf(key_str, sizeof(key_str), "emb:%llu", (unsigned long long)key);
    
    /* 获取连接 */
    pthread_mutex_lock(&pool->locks[conn_id]);
    redisContext *ctx = pool->contexts[conn_id];
    
    /* 执行 GET 命令 */
    redisReply *reply = redisCommand(ctx, "GET %s", key_str);
    
    uint64_t end_time = get_time_ns();
    uint64_t latency = end_time - start_time;
    
    if (reply == NULL) {
        fprintf(stderr, "Redis command failed: %s\n", ctx->errstr);
        pthread_mutex_unlock(&pool->locks[conn_id]);
        return latency;
    }
    
    /* 如果 key 不存在，创建一个随机 embedding 并存储 */
    if (reply->type == REDIS_REPLY_NIL) {
        freeReplyObject(reply);
        
        /* 生成随机 embedding */
        generate_random_embedding(key, embedding);
        
        /* 存储到 Redis */
        reply = redisCommand(ctx, "SET %s %b", key_str, embedding, EMBEDDING_SIZE);
        if (reply) {
            freeReplyObject(reply);
        }
    } else if (reply->type == REDIS_REPLY_STRING) {
        /* 解析 embedding 数据 */
        if (reply->len >= EMBEDDING_SIZE) {
            memcpy(embedding, reply->str, EMBEDDING_SIZE);
        } else {
            /* 数据不完整，生成随机数据 */
            generate_random_embedding(key, embedding);
        }
        freeReplyObject(reply);
    } else {
        freeReplyObject(reply);
        generate_random_embedding(key, embedding);
    }
    
    pthread_mutex_unlock(&pool->locks[conn_id]);
    
    return latency;
}

/* 工作线程 */
void *worker_thread(void *arg) {
    thread_data_t *data = (thread_data_t *)arg;
    redis_pool_t *pool = data->pool;
    
    /* 每个线程使用自己的连接 */
    int conn_id = data->thread_id % pool->num_connections;
    
    /* 分配 embedding 缓冲区 */
    float *embedding = aligned_alloc(64, EMBEDDING_SIZE);
    if (!embedding) {
        fprintf(stderr, "Thread %d: Failed to allocate embedding buffer\n", data->thread_id);
        return NULL;
    }
    
    uint64_t start_time = get_time_ns();
    
    /* 处理查询 */
    for (size_t i = 0; i < data->num_queries; i++) {
        query_t *query = &data->queries[i];
        
        /* 获取 Embedding */
        uint64_t latency = redis_get_embedding(pool, conn_id, query->id, embedding);
        
        /* 更新统计 */
        update_latency_stats(&pool->stats, latency);
        
        /* 进度报告 */
        if ((i + 1) % 100000 == 0) {
            printf("  Thread %d: %zu / %zu queries (avg latency: %.2f μs)\n", 
                   data->thread_id, i + 1, data->num_queries,
                   (double)latency / 1000.0);
        }
    }
    
    uint64_t end_time = get_time_ns();
    data->local_time_ns = end_time - start_time;
    
    free(embedding);
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
int run_benchmark(size_t num_queries, int num_threads, int num_servers) {
    printf("\n========================================\n");
    printf("Redis Baseline Benchmark (Real Server)\n");
    printf("========================================\n");
    printf("Redis Server: %s:%d\n", REDIS_HOST, REDIS_PORT);
    printf("Queries: %zu\n", num_queries);
    printf("Threads: %d\n", num_threads);
    printf("Simulated Servers: %d\n", num_servers);
    printf("========================================\n\n");
    
    /* 初始化 Redis 连接池 */
    redis_pool_t *pool = init_redis_pool(num_threads);
    if (!pool) {
        fprintf(stderr, "Failed to initialize Redis pool\n");
        return 1;
    }
    
    /* 生成查询 */
    query_t *queries = generate_queries(num_queries);
    if (!queries) {
        cleanup_redis_pool(pool);
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
    print_stats("Redis Baseline", &pool->stats, total_time);
    
    /* 估算完整规模性能 */
    printf("========================================\n");
    printf("Full Scale Estimation\n");
    printf("========================================\n");
    
    uint64_t total_req = atomic_load(&pool->stats.total_requests);
    uint64_t total_lat = atomic_load(&pool->stats.total_latency_ns);
    double avg_lat_us = (double)total_lat / total_req / 1000.0;
    
    /* 基于实际测试的单服务器 QPS */
    double measured_qps = total_req / total_time;
    uint64_t total_qps = (uint64_t)(measured_qps * num_servers);
    
    printf("Total embeddings: %llu billion\n", (UID_COUNT + ITEM_COUNT) / 1000000000ULL);
    printf("Measured single-server QPS: %.2f\n", measured_qps);
    printf("Average latency: %.2f μs\n", avg_lat_us);
    printf("Estimated total QPS: %lu (%.2f billion)\n", total_qps, total_qps / 1e9);
    printf("Time to process all: %.2f seconds (%.2f minutes)\n", 
           (double)TOTAL_EMBEDDINGS / total_qps,
           (double)TOTAL_EMBEDDINGS / total_qps / 60.0);
    printf("\nHardware Cost:\n");
    printf("  Servers: %d\n", num_servers);
    printf("  Estimated cost: $%lu million (@ $5k per server)\n", 
           (uint64_t)num_servers * 5000 / 1000000);
    printf("========================================\n\n");
    
    /* 清理 */
    cleanup_redis_pool(pool);
    free(queries);
    free(threads);
    free(thread_data);
    
    return 0;
}

int main(int argc, char *argv[]) {
    /* 解析参数 */
    size_t num_queries = DEFAULT_NUM_QUERIES;
    int num_threads = DEFAULT_NUM_THREADS;
    int num_servers = NUM_REDIS_SERVERS;
    
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--queries") == 0 && i + 1 < argc) {
            num_queries = strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--threads") == 0 && i + 1 < argc) {
            num_threads = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--servers") == 0 && i + 1 < argc) {
            num_servers = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0) {
            printf("Usage: %s [options]\n", argv[0]);
            printf("Options:\n");
            printf("  --queries N    Number of queries (default: %d)\n", DEFAULT_NUM_QUERIES);
            printf("  --threads N    Number of threads (default: %d)\n", DEFAULT_NUM_THREADS);
            printf("  --servers N    Number of Redis servers (default: %d)\n", NUM_REDIS_SERVERS);
            printf("  --help         Show this help\n");
            return 0;
        }
    }
    
    return run_benchmark(num_queries, num_threads, num_servers);
}
