/*
 * Three-Layer Cache Benchmark
 *
 * Tests: HOT(L3) → WARM(mem) → COLD(append-only) with
 * ring buffer notifications, Paxos, and 2x3 HA.
 *
 * Workload: 80% reads, 20% writes, Zipfian key distribution
 * (hot keys accessed much more frequently → tests LRU promotion)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <math.h>
#include "../src/three_layer_cache.h"

#define DEFAULT_QUERIES   1000000
#define DEFAULT_THREADS   8
#define DEFAULT_WARM_FILL 100000  /* pre-fill warm layer */

static inline uint64_t bench_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

/* Zipfian distribution for realistic hot/cold key access */
static uint64_t zipfian_key(unsigned int *seed, uint64_t max_key) {
    double u = (double)rand_r(seed) / RAND_MAX;
    /* Zipf with s=1.2: heavily skewed toward small keys */
    double z = pow(u, 1.0 / 1.2);
    return (uint64_t)(z * (double)max_key) % max_key;
}

typedef struct {
    three_layer_cache_t *cache;
    int thread_id;
    size_t num_ops;
    int write_pct;  /* 0-100 */
    uint64_t elapsed_ns;
    uint64_t read_hits;
    uint64_t read_misses;
    uint64_t writes;
} bench_thread_t;

static void *bench_worker(void *arg) {
    bench_thread_t *t = (bench_thread_t *)arg;
    unsigned int seed = (unsigned int)(t->thread_id + 42);
    uint8_t value_buf[TLC_VALUE_SIZE];
    uint64_t max_key = TLC_WARM_CAPACITY;

    uint64_t start = bench_now_ns();

    for (size_t i = 0; i < t->num_ops; i++) {
        uint64_t key = zipfian_key(&seed, max_key);
        int is_write = (rand_r(&seed) % 100) < t->write_pct;

        if (is_write) {
            /* Generate random value */
            for (int j = 0; j < (int)(TLC_VALUE_SIZE / sizeof(uint32_t)); j++) {
                ((uint32_t *)value_buf)[j] = rand_r(&seed);
            }
            tlc_put(t->cache, key, value_buf);
            t->writes++;
        } else {
            int rc = tlc_get(t->cache, key, value_buf);
            if (rc == 0) t->read_hits++;
            else t->read_misses++;
        }
    }

    t->elapsed_ns = bench_now_ns() - start;
    tlc_flush_tls_stats(t->cache);
    return NULL;
}

static void run_benchmark(three_layer_cache_t *cache,
                          size_t num_ops, int num_threads, int write_pct,
                          const char *label) {
    printf("\n========================================\n");
    printf("  %s\n", label);
    printf("  Ops: %zu, Threads: %d, Write%%: %d\n", num_ops, num_threads, write_pct);
    printf("========================================\n");

    bench_thread_t *threads = calloc(num_threads, sizeof(bench_thread_t));
    pthread_t *pthreads = calloc(num_threads, sizeof(pthread_t));
    size_t ops_per_thread = num_ops / num_threads;

    for (int i = 0; i < num_threads; i++) {
        threads[i].cache = cache;
        threads[i].thread_id = i;
        threads[i].num_ops = ops_per_thread;
        threads[i].write_pct = write_pct;
        pthread_create(&pthreads[i], NULL, bench_worker, &threads[i]);
    }

    for (int i = 0; i < num_threads; i++) {
        pthread_join(pthreads[i], NULL);
    }

    /* Aggregate results */
    uint64_t max_elapsed = 0;
    uint64_t total_hits = 0, total_misses = 0, total_writes = 0;
    for (int i = 0; i < num_threads; i++) {
        if (threads[i].elapsed_ns > max_elapsed)
            max_elapsed = threads[i].elapsed_ns;
        total_hits += threads[i].read_hits;
        total_misses += threads[i].read_misses;
        total_writes += threads[i].writes;
    }

    double elapsed_s = (double)max_elapsed / 1e9;
    double qps = (double)num_ops / elapsed_s;
    double avg_latency_us = (double)max_elapsed / (double)num_ops / 1000.0;

    printf("\nResults:\n");
    printf("  Total time:    %.3f seconds\n", elapsed_s);
    printf("  Throughput:    %.2f QPS (%.2f M QPS)\n", qps, qps / 1e6);
    printf("  Avg latency:   %.3f μs\n", avg_latency_us);
    printf("  Read hits:     %lu\n", total_hits);
    printf("  Read misses:   %lu\n", total_misses);
    printf("  Writes:        %lu\n", total_writes);
    printf("  Hit rate:      %.1f%%\n",
           total_hits + total_misses > 0
           ? 100.0 * total_hits / (total_hits + total_misses) : 0);

    free(threads);
    free(pthreads);
}

int main(int argc, char *argv[]) {
    size_t num_ops = DEFAULT_QUERIES;
    int num_threads = DEFAULT_THREADS;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--queries") == 0 && i + 1 < argc)
            num_ops = (size_t)atol(argv[++i]);
        else if (strcmp(argv[i], "--threads") == 0 && i + 1 < argc)
            num_threads = atoi(argv[++i]);
    }

    printf("Three-Layer Cache Benchmark\n");
    printf("HOT(L3 %dK) → WARM(mem %dK) → COLD(append-only)\n",
           TLC_HOT_CAPACITY / 1024, TLC_WARM_CAPACITY / 1024);
    printf("HA: 2x3 WeChat-style, Paxos conflict resolution\n\n");

    three_layer_cache_t cache;
    if (tlc_init(&cache, 0, 0) != 0) {
        fprintf(stderr, "Failed to init cache\n");
        return 1;
    }

    /* Pre-fill WARM layer with some data */
    printf("Pre-filling WARM layer with %d entries...\n", DEFAULT_WARM_FILL);
    uint8_t fill_val[TLC_VALUE_SIZE];
    unsigned int fill_seed = 12345;
    for (int i = 0; i < DEFAULT_WARM_FILL; i++) {
        for (int j = 0; j < (int)(TLC_VALUE_SIZE / sizeof(uint32_t)); j++)
            ((uint32_t *)fill_val)[j] = rand_r(&fill_seed);
        tlc_put(&cache, (uint64_t)i, fill_val);
    }
    /* Also seed some into COLD */
    for (int i = 0; i < DEFAULT_WARM_FILL / 10; i++) {
        for (int j = 0; j < (int)(TLC_VALUE_SIZE / sizeof(uint32_t)); j++)
            ((uint32_t *)fill_val)[j] = rand_r(&fill_seed);
        cold_append(&cache.cold, (uint64_t)(DEFAULT_WARM_FILL + i), fill_val);
    }
    printf("Pre-fill done.\n");

    /* Reset stats after pre-fill */
    atomic_store(&cache.total_reads, 0);
    atomic_store(&cache.total_writes, 0);
    atomic_store(&cache.read_throughs, 0);
    atomic_store(&cache.write_throughs, 0);
    atomic_store(&cache.hot.hits, 0);
    atomic_store(&cache.hot.misses, 0);
    atomic_store(&cache.warm.hits, 0);
    atomic_store(&cache.warm.misses, 0);
    atomic_store(&cache.cold.hits, 0);
    atomic_store(&cache.cold.misses, 0);

    /* Benchmark 1: Read-heavy (80% read, 20% write) */
    run_benchmark(&cache, num_ops, num_threads, 20, "Read-Heavy (80R/20W)");
    tlc_print_stats(&cache);

    /* Benchmark 2: Write-heavy (20% read, 80% write) */
    run_benchmark(&cache, num_ops / 2, num_threads, 80, "Write-Heavy (20R/80W)");

    /* Benchmark 3: HA failover test */
    printf("\n--- Simulating IDC 1 failure ---\n");
    tlc_ha_failover(&cache, 1);
    run_benchmark(&cache, num_ops / 4, num_threads, 20, "After IDC-1 Failover");

    printf("\n--- Recovering IDC 1 ---\n");
    tlc_ha_recover(&cache, 1);
    run_benchmark(&cache, num_ops / 4, num_threads, 20, "After IDC-1 Recovery");

    tlc_print_stats(&cache);
    tlc_destroy(&cache);

    printf("\n✅ Benchmark complete.\n");
    return 0;
}
