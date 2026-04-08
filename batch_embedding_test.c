/*
 * Batch Embedding Test - High-performance UB vector queries
 * Simulates massive concurrent requests for embedding vectors
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>
#include <limits.h>
#include "deps/hiredis/hiredis.h"
#include <stdatomic.h>

// Test configuration
#define NUM_THREADS 4               // Number of concurrent client threads
#define REQUESTS_PER_THREAD 100     // Requests per thread
#define TOTAL_REQUESTS (NUM_THREADS * REQUESTS_PER_THREAD)
#define VECTOR_DIM 300              // Embedding dimension
#define BATCH_SIZE 16               // Batch size for UB processing
#define WARMUP_REQUESTS 10          // Warmup requests before timing

// Performance targets
#define TARGET_LATENCY_US 100       // Target: 100 microseconds per request
#define TARGET_THROUGHPUT_QPS 50000 // Target: 50k queries per second

// Test data
static char *test_keys[10000];      // Pre-generated test keys
static int num_test_keys = 0;
static atomic_long total_requests = 0;
static atomic_long total_responses = 0;
static atomic_long total_latency_us = 0;
static atomic_long min_latency_us = LONG_MAX;
static atomic_long max_latency_us = 0;

// Thread data structure
typedef struct {
    int thread_id;
    redisContext *redis_ctx;
    long local_requests;
    long local_latency_us;
    long local_min_latency;
    long local_max_latency;
} thread_data_t;

// Timing utilities
static inline long get_time_us() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long)tv.tv_sec * 1000000 + (long)tv.tv_usec;
}

// Generate test keys
void generate_test_keys() {
    for (int i = 0; i < 10000; i++) {
        char key[32];
        snprintf(key, sizeof(key), "emb:%08d", i);
        test_keys[num_test_keys++] = strdup(key);
    }
    printf("Generated %d test keys\n", num_test_keys);
}

// Worker thread function
void *worker_thread(void *arg) {
    thread_data_t *data = (thread_data_t *)arg;
    redisContext *ctx = data->redis_ctx;

    data->local_requests = 0;
    data->local_latency_us = 0;
    data->local_min_latency = LONG_MAX;
    data->local_max_latency = 0;

    // Warmup phase
    for (int i = 0; i < WARMUP_REQUESTS; i++) {
        int key_idx = rand() % num_test_keys;
        redisReply *reply = redisCommand(ctx, "VEMB test_vectors %s", test_keys[key_idx]);
        if (reply) freeReplyObject(reply);
    }

    printf("Thread %d: Warmup complete, starting benchmark\n", data->thread_id);

    // Benchmark phase
    for (int i = 0; i < REQUESTS_PER_THREAD; i++) {
        int key_idx = rand() % num_test_keys;

        long start_time = get_time_us();

        // Send VEMB request
        redisReply *reply = redisCommand(ctx, "VEMB test_vectors %s", test_keys[key_idx]);

        long end_time = get_time_us();
        long latency = end_time - start_time;

        data->local_requests++;
        data->local_latency_us += latency;

        if (latency < data->local_min_latency) data->local_min_latency = latency;
        if (latency > data->local_max_latency) data->local_max_latency = latency;

        // Update global stats atomically
        atomic_fetch_add(&total_requests, 1);
        atomic_fetch_add(&total_responses, 1);
        atomic_fetch_add(&total_latency_us, latency);

        long global_min = atomic_load(&min_latency_us);
        while (latency < global_min &&
               !atomic_compare_exchange_weak(&min_latency_us, &global_min, latency)) {
            global_min = atomic_load(&min_latency_us);
        }

        long global_max = atomic_load(&max_latency_us);
        while (latency > global_max &&
               !atomic_compare_exchange_weak(&max_latency_us, &global_max, latency)) {
            global_max = atomic_load(&max_latency_us);
        }

        if (reply) freeReplyObject(reply);

        // Small delay to simulate real-world request patterns
        usleep(10); // 10 microseconds
    }

    printf("Thread %d: Completed %ld requests\n", data->thread_id, data->local_requests);
    return NULL;
}

// Initialize Redis connection
redisContext *create_redis_connection() {
    redisContext *ctx = redisConnect("127.0.0.1", 6379);
    if (ctx == NULL || ctx->err) {
        fprintf(stderr, "Failed to connect to Redis: %s\n",
                ctx ? ctx->errstr : "Unknown error");
        return NULL;
    }

    // Set UB engine for testing
    redisReply *reply = redisCommand(ctx, "VENGINE SET ub");
    if (reply) {
        printf("Set vector engine to UB: %s\n", reply->str);
        freeReplyObject(reply);
    }

    return ctx;
}

// Setup test data
void setup_test_data(redisContext *ctx) {
    printf("Setting up test data...\n");

    // Create test vectors
    for (int i = 0; i < 1000 && i < num_test_keys; i++) {
        // Generate a simple test vector
        char vector_str[1024] = "VALUES 300";
        char *ptr = vector_str + strlen(vector_str);

        for (int d = 0; d < VECTOR_DIM; d++) {
            ptr += sprintf(ptr, " %.6f", (float)(rand() % 2000 - 1000) / 1000.0f);
        }

        redisReply *reply = redisCommand(ctx, "VADD test_vectors %s %s",
                                        vector_str, test_keys[i]);
        if (reply) freeReplyObject(reply);
    }

    printf("Test data setup complete\n");
}

// Run batch embedding test
int run_batch_test() {
    printf("=== Redis UB Vector Engine Batch Test ===\n");
    printf("Target: %d concurrent threads, %d requests each\n", NUM_THREADS, REQUESTS_PER_THREAD);
    printf("Total requests: %d\n", TOTAL_REQUESTS);
    printf("Target latency: %d μs per request\n", TARGET_LATENCY_US);
    printf("Target throughput: %d QPS\n", TARGET_THROUGHPUT_QPS);

    // Initialize test data
    generate_test_keys();

    // Create main Redis connection for setup
    redisContext *main_ctx = create_redis_connection();
    if (!main_ctx) {
        fprintf(stderr, "Failed to create Redis connection\n");
        return 1;
    }

    setup_test_data(main_ctx);

    // Create worker threads
    pthread_t threads[NUM_THREADS];
    thread_data_t thread_data[NUM_THREADS];

    long test_start_time = get_time_us();

    // Start worker threads
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_data[i].thread_id = i;
        thread_data[i].redis_ctx = create_redis_connection();
        if (!thread_data[i].redis_ctx) {
            fprintf(stderr, "Failed to create connection for thread %d\n", i);
            continue;
        }

        if (pthread_create(&threads[i], NULL, worker_thread, &thread_data[i]) != 0) {
            fprintf(stderr, "Failed to create thread %d\n", i);
            continue;
        }
    }

    // Wait for all threads to complete
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
        if (thread_data[i].redis_ctx) {
            redisFree(thread_data[i].redis_ctx);
        }
    }

    long test_end_time = get_time_us();
    long total_test_time_us = test_end_time - test_start_time;

    // Calculate results
    long total_req = atomic_load(&total_requests);
    long total_lat = atomic_load(&total_latency_us);
    long min_lat = atomic_load(&min_latency_us);
    long max_lat = atomic_load(&max_latency_us);

    double avg_latency_us = total_req > 0 ? (double)total_lat / total_req : 0;
    double throughput_qps = total_req > 0 ? (double)total_req / (total_test_time_us / 1000000.0) : 0;

    printf("\n=== Test Results ===\n");
    printf("Total requests: %ld\n", total_req);
    printf("Total time: %.3f seconds\n", total_test_time_us / 1000000.0);
    printf("Average latency: %.1f μs\n", avg_latency_us);
    printf("Min latency: %ld μs\n", min_lat);
    printf("Max latency: %ld μs\n", max_lat);
    printf("Throughput: %.0f QPS\n", throughput_qps);

    // Performance analysis
    printf("\n=== Performance Analysis ===\n");

    if (avg_latency_us <= TARGET_LATENCY_US) {
        printf("✅ Latency target met: %.1f μs ≤ %d μs\n", avg_latency_us, TARGET_LATENCY_US);
    } else {
        printf("❌ Latency target missed: %.1f μs > %d μs\n", avg_latency_us, TARGET_LATENCY_US);
    }

    if (throughput_qps >= TARGET_THROUGHPUT_QPS) {
        printf("✅ Throughput target met: %.0f QPS ≥ %d QPS\n", throughput_qps, TARGET_THROUGHPUT_QPS);
    } else {
        printf("❌ Throughput target missed: %.0f QPS < %d QPS\n", throughput_qps, TARGET_THROUGHPUT_QPS);
    }

    // P99 latency analysis (simplified)
    printf("P99 latency estimate: %.1f μs\n", avg_latency_us * 2.0);

    // Cleanup
    redisFree(main_ctx);
    for (int i = 0; i < num_test_keys; i++) {
        free(test_keys[i]);
    }

    return avg_latency_us <= TARGET_LATENCY_US && throughput_qps >= TARGET_THROUGHPUT_QPS ? 0 : 1;
}

// Batch processing test for UB engine
int run_ub_batch_test() {
    printf("\n=== UB Engine Batch Processing Test ===\n");

    redisContext *ctx = create_redis_connection();
    if (!ctx) return 1;

    // Test batch embedding retrieval
    long start_time = get_time_us();

    // Send multiple VEMB requests in quick succession
    for (int i = 0; i < BATCH_SIZE; i++) {
        int key_idx = rand() % num_test_keys;
        redisReply *reply = redisCommand(ctx, "VEMB test_vectors %s", test_keys[key_idx]);
        if (reply) freeReplyObject(reply);
    }

    long end_time = get_time_us();
    long batch_time_us = end_time - start_time;

    printf("UB batch processing (%d requests): %ld μs total, %.1f μs per request\n",
           BATCH_SIZE, batch_time_us, (double)batch_time_us / BATCH_SIZE);

    redisFree(ctx);
    return 0;
}

int main(int argc, char *argv[]) {
    srand(time(NULL));

    // Run comprehensive batch test
    int result = run_batch_test();

    // Run UB-specific batch test
    run_ub_batch_test();

    printf("\n=== Test Summary ===\n");
    if (result == 0) {
        printf("✅ All performance targets met!\n");
        printf("UB vector engine is ready for production use.\n");
    } else {
        printf("⚠️  Performance targets not fully met.\n");
        printf("Consider optimizing UB engine implementation.\n");
    }

    return result;
}