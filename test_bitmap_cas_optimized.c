/*
 * Bitmap CAS 优化版本测试程序
 * 验证高性能无锁并发控制
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdalign.h>
#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include <string.h>

/* 配置 */
#define NUM_THREADS 8
#define OPS_PER_THREAD 100000
#define TOTAL_OPS (NUM_THREADS * OPS_PER_THREAD)
#define NUM_BITS (TOTAL_OPS * 2)
#define HIGH_CONTENTION_BITS 64

/* 对齐的原子字 - 防止伪共享 */
typedef struct {
    _Alignas(64) atomic_uint_fast64_t word;
} aligned_atomic_word_t;

/* Bitmap 结构 */
typedef struct {
    aligned_atomic_word_t *bits;
    size_t num_words;
} bitmap_t;

/* 统计信息 */
typedef struct {
    atomic_uint_fast64_t total_acquires;
    atomic_uint_fast64_t failed_acquires;
    atomic_uint_fast64_t total_releases;
    atomic_uint_fast64_t total_retries;
} stats_t;

typedef struct {
    const char *name;
    stats_t stats;
} bitmap_impl_t;

typedef enum {
    ACCESS_MODE_PARTITIONED = 0,
    ACCESS_MODE_HIGH_CONTENTION = 1,
} access_mode_t;

static bitmap_impl_t *current_impl = NULL;

/* 获取当前时间（微秒）*/
static inline uint64_t get_time_us(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000 + (uint64_t)tv.tv_usec;
}

/* 创建 Bitmap */
bitmap_t *bitmap_create(size_t num_bits) {
    bitmap_t *bitmap = malloc(sizeof(bitmap_t));
    if (!bitmap) return NULL;
    
    bitmap->num_words = (num_bits + 63) / 64;
    bitmap->bits = aligned_alloc(64, bitmap->num_words * sizeof(aligned_atomic_word_t));
    
    if (!bitmap->bits) {
        free(bitmap);
        return NULL;
    }
    
    /* 初始化为 0 */
    for (size_t i = 0; i < bitmap->num_words; i++) {
        atomic_init(&bitmap->bits[i].word, 0);
    }
    
    return bitmap;
}

/* 销毁 Bitmap */
void bitmap_destroy(bitmap_t *bitmap) {
    if (bitmap) {
        free(bitmap->bits);
        free(bitmap);
    }
}

/* 尝试获取锁（CAS 优化版本）*/
int bitmap_try_acquire_cas(bitmap_t *bitmap, uint64_t bit_index) {
    uint64_t word_index = bit_index / 64;
    uint64_t bit_offset = bit_index % 64;
    const uint64_t mask = 1ULL << bit_offset;
    
    if (word_index >= bitmap->num_words) return 0;
    
    atomic_uint_fast64_t *target_word = &bitmap->bits[word_index].word;
    
    /* 读取当前值（relaxed）*/
    uint64_t old_val = atomic_load_explicit(target_word, memory_order_relaxed);
    
    int retry_count = 0;
    
    /* CAS 循环 */
    do {
        /* 检查是否已被占用 */
        if ((old_val & mask) != 0) {
            atomic_fetch_add_explicit(&current_impl->stats.failed_acquires, 1,
                                      memory_order_relaxed);
            return 0;  /* 已被占用 */
        }
        
        /* 计算新值 */
        uint64_t new_val = old_val | mask;
        
        /* CAS 操作 */
        if (atomic_compare_exchange_weak_explicit(target_word, &old_val, new_val,
                                                  memory_order_acquire,
                                                  memory_order_relaxed)) {
            atomic_fetch_add_explicit(&current_impl->stats.total_acquires, 1,
                                      memory_order_relaxed);
            atomic_fetch_add_explicit(&current_impl->stats.total_retries, retry_count,
                                      memory_order_relaxed);
            return 1;  /* 成功 */
        }
        
        /* CAS 失败，重试 */
        retry_count++;
        
        /* CPU Pause（减少竞争）*/
#if defined(__x86_64__) || defined(__i386__)
        __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(__arm__)
        __asm__ __volatile__("yield" ::: "memory");
#endif
        
    } while (1);
}

/* 尝试获取锁（当前 fetch_or 版本）*/
int bitmap_try_acquire_fetch_or(bitmap_t *bitmap, uint64_t bit_index) {
    uint64_t word_index = bit_index / 64;
    uint64_t bit_offset = bit_index % 64;
    const uint64_t mask = 1ULL << bit_offset;

    if (word_index >= bitmap->num_words) return 0;

    uint64_t prev = atomic_fetch_or_explicit(&bitmap->bits[word_index].word,
                                             mask, memory_order_acquire);
    if (prev & mask) {
        atomic_fetch_add_explicit(&current_impl->stats.failed_acquires, 1,
                                  memory_order_relaxed);
        return 0;
    }

    atomic_fetch_add_explicit(&current_impl->stats.total_acquires, 1,
                              memory_order_relaxed);
    return 1;
}

/* 释放锁（通用实现）*/
void bitmap_release(bitmap_t *bitmap, uint64_t bit_index) {
    uint64_t word_index = bit_index / 64;
    uint64_t bit_offset = bit_index % 64;
    const uint64_t mask_complement = ~(1ULL << bit_offset);
    
    if (word_index >= bitmap->num_words) return;
    
    atomic_uint_fast64_t *target_word = &bitmap->bits[word_index].word;
    
    /* 原子按位与操作（release 内存序）*/
    atomic_fetch_and_explicit(target_word, mask_complement, memory_order_release);
    
    atomic_fetch_add_explicit(&current_impl->stats.total_releases, 1, memory_order_relaxed);
}

/* 测试位状态 */
int bitmap_test_bit(bitmap_t *bitmap, uint64_t bit_index) {
    uint64_t word_index = bit_index / 64;
    uint64_t bit_offset = bit_index % 64;
    
    if (word_index >= bitmap->num_words) return 0;
    
    uint64_t word = atomic_load_explicit(&bitmap->bits[word_index].word, 
                                        memory_order_relaxed);
    return (word & (1ULL << bit_offset)) != 0;
}

/* 线程数据 */
typedef struct {
    int thread_id;
    bitmap_t *bitmap;
    uint64_t start_bit;
    uint64_t end_bit;
    access_mode_t access_mode;
    uint64_t local_ops;
    uint64_t local_time_us;
} thread_data_t;

/* 工作线程 */
void *worker_thread(void *arg) {
    thread_data_t *data = (thread_data_t *)arg;
    
    uint64_t start_time = get_time_us();
    
    /* 执行操作 */
    for (uint64_t i = 0; i < OPS_PER_THREAD; i++) {
        uint64_t bit_range = data->end_bit - data->start_bit;
        uint64_t bit = data->start_bit + (i % bit_range);

        if (data->access_mode == ACCESS_MODE_HIGH_CONTENTION) {
            bit = i % HIGH_CONTENTION_BITS;
        }
        
        /* 尝试获取 */
        if (current_impl && strcmp(current_impl->name, "fetch_or current implementation") == 0) {
            if (!bitmap_try_acquire_fetch_or(data->bitmap, bit)) continue;
        } else {
            if (!bitmap_try_acquire_cas(data->bitmap, bit)) continue;
        }

        {
            /* 模拟使用资源 */
            volatile int dummy = 0;
            for (int j = 0; j < 10; j++) {
                dummy += j;
            }
            
            /* 释放 */
            bitmap_release(data->bitmap, bit);
            data->local_ops++;
        }
    }
    
    uint64_t end_time = get_time_us();
    data->local_time_us = end_time - start_time;
    
    return NULL;
}

/* 运行测试 */
void run_test(bitmap_impl_t *impl, const char *test_name, int num_threads,
              access_mode_t access_mode) {
    printf("\n========================================\n");
    printf("Test: %s (%s)\n", test_name, impl->name);
    printf("Threads: %d\n", num_threads);
    printf("Operations per thread: %d\n", OPS_PER_THREAD);
    printf("Total operations: %d\n", num_threads * OPS_PER_THREAD);
    printf("Access mode: %s\n",
           access_mode == ACCESS_MODE_HIGH_CONTENTION ? "High contention" : "Partitioned");
    printf("========================================\n\n");
    
    /* 创建 Bitmap */
    bitmap_t *bitmap = bitmap_create(NUM_BITS);
    if (!bitmap) {
        printf("Failed to create bitmap\n");
        return;
    }
    
    /* 重置统计 */
    current_impl = impl;
    atomic_store(&impl->stats.total_acquires, 0);
    atomic_store(&impl->stats.failed_acquires, 0);
    atomic_store(&impl->stats.total_releases, 0);
    atomic_store(&impl->stats.total_retries, 0);
    
    /* 创建线程 */
    pthread_t *threads = malloc(num_threads * sizeof(pthread_t));
    thread_data_t *thread_data = malloc(num_threads * sizeof(thread_data_t));
    
    uint64_t bits_per_thread = NUM_BITS / num_threads;
    
    uint64_t test_start_time = get_time_us();
    
    /* 启动线程 */
    for (int i = 0; i < num_threads; i++) {
        thread_data[i].thread_id = i;
        thread_data[i].bitmap = bitmap;
        thread_data[i].start_bit = i * bits_per_thread;
        thread_data[i].end_bit = (i + 1) * bits_per_thread;
        thread_data[i].access_mode = access_mode;
        thread_data[i].local_ops = 0;
        thread_data[i].local_time_us = 0;
        
        pthread_create(&threads[i], NULL, worker_thread, &thread_data[i]);
    }
    
    /* 等待线程完成 */
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    
    uint64_t test_end_time = get_time_us();
    uint64_t total_time_us = test_end_time - test_start_time;
    
    /* 统计结果 */
    uint64_t total_ops = 0;
    for (int i = 0; i < num_threads; i++) {
        total_ops += thread_data[i].local_ops;
    }
    
    uint64_t total_acquires = atomic_load(&impl->stats.total_acquires);
    uint64_t failed_acquires = atomic_load(&impl->stats.failed_acquires);
    uint64_t total_releases = atomic_load(&impl->stats.total_releases);
    uint64_t total_retries = atomic_load(&impl->stats.total_retries);
    
    /* 打印结果 */
    printf("Results:\n");
    printf("  Total time: %.2f ms\n", total_time_us / 1000.0);
    printf("  Successful operations: %llu\n", (unsigned long long)total_ops);
    printf("  Total acquires: %llu\n", (unsigned long long)total_acquires);
    printf("  Failed acquires: %llu\n", (unsigned long long)failed_acquires);
    printf("  Total releases: %llu\n", (unsigned long long)total_releases);
    printf("  Total retries: %llu\n", (unsigned long long)total_retries);
    printf("  Success rate: %.2f%%\n", 
           100.0 * total_acquires / (total_acquires + failed_acquires));
    printf("\n");
    
    printf("Performance:\n");
    printf("  Throughput: %.2f M ops/sec\n", 
           total_ops / (total_time_us / 1000000.0) / 1000000.0);
    printf("  Average latency: %.0f ns/op\n", 
           (double)total_time_us * 1000.0 / total_ops);
    printf("\n");
    
    /* 验证 */
    int errors = 0;
    for (uint64_t i = 0; i < NUM_BITS; i++) {
        if (bitmap_test_bit(bitmap, i)) {
            errors++;
        }
    }
    
    if (errors == 0) {
        printf("✅ Verification: All bits correctly released\n");
    } else {
        printf("❌ Verification: %d bits still set (should be 0)\n", errors);
    }
    
    /* 清理 */
    bitmap_destroy(bitmap);
    free(threads);
    free(thread_data);
}

/* 主函数 */
int main(void) {
    bitmap_impl_t cas_impl = {.name = "CAS optimized"};
    bitmap_impl_t fetch_or_impl = {.name = "fetch_or current implementation"};

    printf("========================================\n");
    printf("Bitmap CAS Optimization Test\n");
    printf("========================================\n");
    printf("\n");
    printf("Configuration:\n");
    printf("  Alignment: 64 bytes (cache line)\n");
    printf("  Atomic type: atomic_uint_fast64_t\n");
    printf("  Memory order: acquire/release/relaxed\n");
    printf("  Compared implementations: compare_exchange_weak vs fetch_or\n");
    printf("  High-contention window: %d bits shared by all threads\n", HIGH_CONTENTION_BITS);
    printf("\n");
    
    /* 运行不同线程数的测试 */
    run_test(&cas_impl, "Low Concurrency", 2, ACCESS_MODE_PARTITIONED);
    run_test(&fetch_or_impl, "Low Concurrency", 2, ACCESS_MODE_PARTITIONED);
    run_test(&cas_impl, "Medium Concurrency", 4, ACCESS_MODE_PARTITIONED);
    run_test(&fetch_or_impl, "Medium Concurrency", 4, ACCESS_MODE_PARTITIONED);
    run_test(&cas_impl, "High Concurrency", 8, ACCESS_MODE_PARTITIONED);
    run_test(&fetch_or_impl, "High Concurrency", 8, ACCESS_MODE_PARTITIONED);
    run_test(&cas_impl, "Very High Concurrency", 16, ACCESS_MODE_PARTITIONED);
    run_test(&fetch_or_impl, "Very High Concurrency", 16, ACCESS_MODE_PARTITIONED);

    run_test(&cas_impl, "Hotspot Concurrency", 8, ACCESS_MODE_HIGH_CONTENTION);
    run_test(&fetch_or_impl, "Hotspot Concurrency", 8, ACCESS_MODE_HIGH_CONTENTION);
    run_test(&cas_impl, "Hotspot Very High Concurrency", 16, ACCESS_MODE_HIGH_CONTENTION);
    run_test(&fetch_or_impl, "Hotspot Very High Concurrency", 16, ACCESS_MODE_HIGH_CONTENTION);
    
    printf("\n========================================\n");
    printf("All tests completed!\n");
    printf("========================================\n");
    
    return 0;
}
