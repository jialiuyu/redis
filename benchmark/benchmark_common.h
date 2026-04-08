/*
 * Benchmark Common Definitions
 * 端到端性能测试的公共定义
 */

#ifndef __BENCHMARK_COMMON_H
#define __BENCHMARK_COMMON_H

#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <sys/time.h>
#include <stdio.h>
#include <math.h>
#include <time.h>

/* 配置常量 */
#define EMBEDDING_DIM 300
#define EMBEDDING_SIZE (EMBEDDING_DIM * sizeof(float))

/* 规模配置 */
#define UID_COUNT_BILLION 10ULL        /* 100 亿 UID */
#define ITEM_COUNT_BILLION 100ULL      /* 1000 亿 Item */
#define UID_COUNT (UID_COUNT_BILLION * 1000000000ULL)
#define ITEM_COUNT (ITEM_COUNT_BILLION * 1000000000ULL)
#define TOTAL_EMBEDDINGS (UID_COUNT + ITEM_COUNT)

/* 传统 Redis 配置 */
#define NUM_REDIS_SERVERS 350000       /* 35 万台 */
#define QPS_PER_REDIS_SERVER 10000     /* 每台 1 万 QPS */

/* SuperNode 配置 */
#define NUM_SUPERNODES 150
#define SUPERNODE_CAPACITY_TB 4
#define SUPERNODE_NUM_WORKERS 16

/* 测试配置 */
#define DEFAULT_NUM_QUERIES 10000000   /* 1000 万查询 */
#define DEFAULT_NUM_THREADS 16
#define BATCH_SIZE 3000                /* 批量大小 */

/* Embedding 结构 */
typedef struct {
    uint64_t id;
    float data[EMBEDDING_DIM];
} __attribute__((aligned(64))) embedding_t;

/* 查询类型 */
typedef enum {
    QUERY_TYPE_UID = 0,
    QUERY_TYPE_ITEM = 1
} query_type_t;

/* 查询结构 */
typedef struct {
    query_type_t type;
    uint64_t id;
} query_t;

/* 统计信息 */
typedef struct {
    atomic_uint_fast64_t total_requests;
    atomic_uint_fast64_t total_latency_ns;
    atomic_uint_fast64_t min_latency_ns;
    atomic_uint_fast64_t max_latency_ns;
    atomic_uint_fast64_t network_latency_ns;
    atomic_uint_fast64_t disk_io_latency_ns;
    atomic_uint_fast64_t cpu_latency_ns;
    atomic_uint_fast64_t batch_count;
} benchmark_stats_t;

/* 工具函数 */

/* 获取当前时间（纳秒）*/
static inline uint64_t get_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* 获取当前时间（微秒）*/
static inline uint64_t get_time_us(void) {
    return get_time_ns() / 1000;
}

/* 初始化统计信息 */
static inline void init_stats(benchmark_stats_t *stats) {
    atomic_init(&stats->total_requests, 0);
    atomic_init(&stats->total_latency_ns, 0);
    atomic_init(&stats->min_latency_ns, UINT64_MAX);
    atomic_init(&stats->max_latency_ns, 0);
    atomic_init(&stats->network_latency_ns, 0);
    atomic_init(&stats->disk_io_latency_ns, 0);
    atomic_init(&stats->cpu_latency_ns, 0);
    atomic_init(&stats->batch_count, 0);
}

/* 更新延迟统计 */
static inline void update_latency_stats(benchmark_stats_t *stats, uint64_t latency_ns) {
    atomic_fetch_add(&stats->total_requests, 1);
    atomic_fetch_add(&stats->total_latency_ns, latency_ns);
    
    /* 更新最小延迟 */
    uint64_t min = atomic_load(&stats->min_latency_ns);
    while (latency_ns < min && 
           !atomic_compare_exchange_weak(&stats->min_latency_ns, &min, latency_ns)) {
        min = atomic_load(&stats->min_latency_ns);
    }
    
    /* 更新最大延迟 */
    uint64_t max = atomic_load(&stats->max_latency_ns);
    while (latency_ns > max && 
           !atomic_compare_exchange_weak(&stats->max_latency_ns, &max, latency_ns)) {
        max = atomic_load(&stats->max_latency_ns);
    }
}

/* 打印统计信息 */
static inline void print_stats(const char *name, benchmark_stats_t *stats, double total_time_sec) {
    uint64_t total_req = atomic_load(&stats->total_requests);
    uint64_t total_lat = atomic_load(&stats->total_latency_ns);
    uint64_t min_lat = atomic_load(&stats->min_latency_ns);
    uint64_t max_lat = atomic_load(&stats->max_latency_ns);
    uint64_t net_lat = atomic_load(&stats->network_latency_ns);
    uint64_t disk_lat = atomic_load(&stats->disk_io_latency_ns);
    uint64_t cpu_lat = atomic_load(&stats->cpu_latency_ns);
    uint64_t batch_cnt = atomic_load(&stats->batch_count);
    
    double avg_lat_us = total_req > 0 ? (double)total_lat / total_req / 1000.0 : 0;
    double throughput_qps = total_req / total_time_sec;
    
    printf("\n========================================\n");
    printf("%s Results\n", name);
    printf("========================================\n");
    printf("Total time: %.2f seconds\n", total_time_sec);
    printf("Total requests: %lu\n", total_req);
    printf("Throughput: %.2f QPS (%.2f M QPS)\n", throughput_qps, throughput_qps / 1000000.0);
    
    if (batch_cnt > 0) {
        printf("Total batches: %lu\n", batch_cnt);
        printf("Average batch size: %.1f\n", (double)total_req / batch_cnt);
    }
    
    printf("\nLatency:\n");
    printf("  Average: %.2f μs\n", avg_lat_us);
    printf("  Min: %.2f μs\n", min_lat / 1000.0);
    printf("  Max: %.2f μs\n", max_lat / 1000.0);
    
    if (net_lat > 0 || disk_lat > 0 || cpu_lat > 0) {
        printf("\nLatency Breakdown:\n");
        if (net_lat > 0) {
            double avg_net = (double)net_lat / total_req / 1000.0;
            printf("  Network: %.2f μs (%.1f%%)\n", avg_net, avg_net / avg_lat_us * 100);
        }
        if (disk_lat > 0) {
            double avg_disk = (double)disk_lat / total_req / 1000.0;
            printf("  Disk I/O: %.2f μs (%.1f%%)\n", avg_disk, avg_disk / avg_lat_us * 100);
        }
        if (cpu_lat > 0) {
            double avg_cpu = (double)cpu_lat / total_req / 1000.0;
            printf("  CPU: %.2f μs (%.1f%%)\n", avg_cpu, avg_cpu / avg_lat_us * 100);
        }
    }
    
    printf("========================================\n\n");
}

/* 生成随机 Embedding */
static inline void generate_random_embedding(uint64_t seed, float *embedding) {
    /* 使用简单的线性同余生成器 */
    uint64_t state = seed;
    float sum_sq = 0.0f;
    
    for (int i = 0; i < EMBEDDING_DIM; i++) {
        state = state * 1103515245ULL + 12345ULL;
        float val = ((float)(state >> 32) / (float)UINT32_MAX) * 2.0f - 1.0f;
        embedding[i] = val;
        sum_sq += val * val;
    }
    
    /* L2 归一化 */
    float norm = sqrtf(sum_sq);
    if (norm > 0.0f) {
        for (int i = 0; i < EMBEDDING_DIM; i++) {
            embedding[i] /= norm;
        }
    }
}

/* 哈希函数 */
static inline uint64_t hash_key(uint64_t key) {
    /* MurmurHash3 简化版 */
    key ^= key >> 33;
    key *= 0xff51afd7ed558ccdULL;
    key ^= key >> 33;
    key *= 0xc4ceb9fe1a85ec53ULL;
    key ^= key >> 33;
    return key;
}

#endif /* __BENCHMARK_COMMON_H */
