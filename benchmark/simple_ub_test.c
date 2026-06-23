/*
 * Simple UB Vector Engine Test
 * Basic functionality test for Redis UB integration
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <limits.h>
#include "deps/hiredis/hiredis.h"

#define VECTOR_DIM 300

// Get current time in microseconds
long get_time_us() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long)tv.tv_sec * 1000000 + (long)tv.tv_usec;
}

int main(int argc, char *argv[]) {
    printf("=== Simple Redis UB Vector Engine Test ===\n");

    // Connect to Redis
    printf("1. Connecting to Redis...\n");
    redisContext *ctx = redisConnect("127.0.0.1", 6381);
    if (ctx == NULL || ctx->err) {
        fprintf(stderr, "Failed to connect to Redis: %s\n",
                ctx ? ctx->errstr : "Unknown error");
        return 1;
    }
    printf("   Connected successfully\n");

    // Set UB engine
    printf("2. Setting vector engine to UB...\n");
    redisReply *reply = redisCommand(ctx, "VENGINE SET ub");
    if (reply) {
        printf("   Engine set to: %s\n", reply->str);
        freeReplyObject(reply);
    }

    // Add test vectors
    printf("3. Adding test vectors...\n");
    for (int i = 0; i < 10; i++) {
        char cmd[1024];
        snprintf(cmd, sizeof(cmd), "VADD test_vectors VALUES %d", VECTOR_DIM);

        char *ptr = cmd + strlen(cmd);
        for (int d = 0; d < VECTOR_DIM; d++) {
            ptr += sprintf(ptr, " %.6f", (float)(rand() % 2000 - 1000) / 1000.0f);
        }

        char elem_name[32];
        snprintf(elem_name, sizeof(elem_name), "emb:%08d", i);
        ptr += sprintf(ptr, " %s", elem_name);

        reply = redisCommand(ctx, cmd);
        if (reply) freeReplyObject(reply);
    }
    printf("   Added 10 test vectors\n");

    // Test VEMB latency
    printf("4. Testing VEMB latency...\n");
    long total_latency = 0;
    long min_latency = LONG_MAX;
    long max_latency = 0;
    int num_tests = 50;

    for (int i = 0; i < num_tests; i++) {
        int elem_idx = rand() % 10;
        char elem_name[32];
        snprintf(elem_name, sizeof(elem_name), "emb:%08d", elem_idx);

        long start = get_time_us();
        reply = redisCommand(ctx, "VEMB test_vectors %s", elem_name);
        long end = get_time_us();

        long latency = end - start;
        total_latency += latency;
        if (latency < min_latency) min_latency = latency;
        if (latency > max_latency) max_latency = latency;

        if (reply) freeReplyObject(reply);
    }

    double avg_latency = (double)total_latency / num_tests;
    printf("   Average latency: %.1f μs\n", avg_latency);
    printf("   Min latency: %ld μs\n", min_latency);
    printf("   Max latency: %ld μs\n", max_latency);

    // Check engine stats
    printf("5. Checking engine statistics...\n");
    reply = redisCommand(ctx, "VENGINE STATS");
    if (reply) {
        printf("   Engine stats: %s\n", reply->str);
        freeReplyObject(reply);
    }

    // Cleanup
    redisFree(ctx);
    printf("6. Test completed successfully!\n");

    // Performance analysis
    printf("\n=== Performance Analysis ===\n");
    if (avg_latency <= 100.0) {
        printf("✅ Target latency achieved: %.1f μs ≤ 100 μs\n", avg_latency);
        printf("UB vector engine is providing microsecond-level performance!\n");
    } else {
        printf("⚠️  Latency above target: %.1f μs > 100 μs\n", avg_latency);
        printf("Consider optimizing UB implementation for better performance.\n");
    }

    return avg_latency <= 100.0 ? 0 : 1;
}