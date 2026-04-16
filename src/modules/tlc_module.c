/*
 * TLC Module — Three-Layer Cache with UB Memory + SVE2
 *
 * Redis module exposing the three-layer cache (HOT→WARM→COLD)
 * backed by UB shared memory with consistent hashing.
 *
 * Commands:
 *   TLC.PUT <key_id> <value_bytes>     — write 1200B value
 *   TLC.GET <key_id>                   — read 1200B value
 *   TLC.MPUT <key1> <val1> [key2 val2 ...]  — batch write
 *   TLC.MGET <key1> [key2 ...]         — batch read
 *   TLC.STATS                           — print cache stats
 *   TLC.FILL <count>                    — pre-fill with random data
 *   TLC.SIM <dim> <query_floats...> <n_ids> <id1> [id2 ...]  — SVE2 similarity
 *
 * Build:
 *   gcc -O3 -march=armv8.2-a+sve -shared -fPIC -o tlc_module.so \
 *       tlc_module.c ../three_layer_cache_ub.c -lm -lpthread
 *
 * Load:
 *   loadmodule /path/to/tlc_module.so
 */

#define _GNU_SOURCE
#include "../redismodule.h"
#include "../three_layer_cache_ub.h"
#include "../sve2_gemm.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Global three-layer cache instance */
static three_layer_cache_t g_cache;
static int g_initialized = 0;

/* ============================================================
 * TLC.PUT <key_id_uint64> <value_bytes>
 * ============================================================ */
static int TLC_Put_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    if (!g_initialized) {
        RedisModule_ReplyWithError(ctx, "ERR TLC not initialized");
        return REDISMODULE_OK;
    }

    long long key_id;
    if (RedisModule_StringToLongLong(argv[1], &key_id) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "ERR invalid key_id (must be integer)");
        return REDISMODULE_OK;
    }

    size_t val_len;
    const char *val = RedisModule_StringPtrLen(argv[2], &val_len);

    /* Pad or truncate to TLC_VALUE_SIZE */
    uint8_t buf[TLC_VALUE_SIZE];
    memset(buf, 0, TLC_VALUE_SIZE);
    size_t copy_len = val_len < TLC_VALUE_SIZE ? val_len : TLC_VALUE_SIZE;
    memcpy(buf, val, copy_len);

    int rc = tlc_put(&g_cache, (uint64_t)key_id, buf);
    if (rc == 0)
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    else
        RedisModule_ReplyWithError(ctx, "ERR tlc_put failed");

    return REDISMODULE_OK;
}

/* ============================================================
 * TLC.GET <key_id_uint64>
 * ============================================================ */
static int TLC_Get_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    if (!g_initialized) {
        RedisModule_ReplyWithError(ctx, "ERR TLC not initialized");
        return REDISMODULE_OK;
    }

    long long key_id;
    if (RedisModule_StringToLongLong(argv[1], &key_id) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "ERR invalid key_id");
        return REDISMODULE_OK;
    }

    uint8_t buf[TLC_VALUE_SIZE];
    int rc = tlc_get(&g_cache, (uint64_t)key_id, buf);
    if (rc == 0)
        RedisModule_ReplyWithStringBuffer(ctx, (const char *)buf, TLC_VALUE_SIZE);
    else
        RedisModule_ReplyWithNull(ctx);

    return REDISMODULE_OK;
}

/* ============================================================
 * TLC.MPUT <key1> <val1> [key2 val2 ...]
 * ============================================================ */
static int TLC_MPut_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3 || (argc - 1) % 2 != 0) return RedisModule_WrongArity(ctx);
    if (!g_initialized) {
        RedisModule_ReplyWithError(ctx, "ERR TLC not initialized");
        return REDISMODULE_OK;
    }

    int pairs = (argc - 1) / 2;
    int ok_count = 0;
    uint8_t buf[TLC_VALUE_SIZE];

    for (int i = 0; i < pairs; i++) {
        long long key_id;
        if (RedisModule_StringToLongLong(argv[1 + i*2], &key_id) != REDISMODULE_OK)
            continue;

        size_t val_len;
        const char *val = RedisModule_StringPtrLen(argv[2 + i*2], &val_len);
        memset(buf, 0, TLC_VALUE_SIZE);
        size_t cl = val_len < TLC_VALUE_SIZE ? val_len : TLC_VALUE_SIZE;
        memcpy(buf, val, cl);

        if (tlc_put(&g_cache, (uint64_t)key_id, buf) == 0)
            ok_count++;
    }

    RedisModule_ReplyWithLongLong(ctx, ok_count);
    return REDISMODULE_OK;
}

/* ============================================================
 * TLC.MGET <key1> [key2 ...]
 * ============================================================ */
static int TLC_MGet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 2) return RedisModule_WrongArity(ctx);
    if (!g_initialized) {
        RedisModule_ReplyWithError(ctx, "ERR TLC not initialized");
        return REDISMODULE_OK;
    }

    int n = argc - 1;
    RedisModule_ReplyWithArray(ctx, n);

    uint8_t buf[TLC_VALUE_SIZE];
    for (int i = 0; i < n; i++) {
        long long key_id;
        if (RedisModule_StringToLongLong(argv[1 + i], &key_id) != REDISMODULE_OK) {
            RedisModule_ReplyWithNull(ctx);
            continue;
        }
        if (tlc_get(&g_cache, (uint64_t)key_id, buf) == 0)
            RedisModule_ReplyWithStringBuffer(ctx, (const char *)buf, TLC_VALUE_SIZE);
        else
            RedisModule_ReplyWithNull(ctx);
    }

    return REDISMODULE_OK;
}

/* ============================================================
 * TLC.FILL <count> — pre-fill with random 1200B values
 * ============================================================ */
static int TLC_Fill_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    if (!g_initialized) {
        RedisModule_ReplyWithError(ctx, "ERR TLC not initialized");
        return REDISMODULE_OK;
    }

    long long count;
    if (RedisModule_StringToLongLong(argv[1], &count) != REDISMODULE_OK || count <= 0) {
        RedisModule_ReplyWithError(ctx, "ERR invalid count");
        return REDISMODULE_OK;
    }

    uint8_t buf[TLC_VALUE_SIZE];
    unsigned int seed = 12345;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (long long i = 0; i < count; i++) {
        for (int j = 0; j < (int)(TLC_VALUE_SIZE / sizeof(uint32_t)); j++)
            ((uint32_t *)buf)[j] = rand_r(&seed);
        tlc_put(&g_cache, (uint64_t)i, buf);
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    double qps = (double)count / elapsed;

    char reply[256];
    snprintf(reply, sizeof(reply),
             "Filled %lld entries in %.3f s (%.2f M ops/s)",
             count, elapsed, qps / 1e6);
    RedisModule_ReplyWithSimpleString(ctx, reply);
    return REDISMODULE_OK;
}

/* ============================================================
 * TLC.STATS — return cache statistics
 * ============================================================ */
static int TLC_Stats_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    (void)argv;
    if (argc != 1) return RedisModule_WrongArity(ctx);
    if (!g_initialized) {
        RedisModule_ReplyWithError(ctx, "ERR TLC not initialized");
        return REDISMODULE_OK;
    }

    RedisModule_ReplyWithArray(ctx, 24);

    RedisModule_ReplyWithSimpleString(ctx, "total_reads");
    RedisModule_ReplyWithLongLong(ctx, atomic_load(&g_cache.total_reads));
    RedisModule_ReplyWithSimpleString(ctx, "total_writes");
    RedisModule_ReplyWithLongLong(ctx, atomic_load(&g_cache.total_writes));
    RedisModule_ReplyWithSimpleString(ctx, "hot_hits");
    RedisModule_ReplyWithLongLong(ctx, atomic_load(&g_cache.hot.hits));
    RedisModule_ReplyWithSimpleString(ctx, "hot_misses");
    RedisModule_ReplyWithLongLong(ctx, atomic_load(&g_cache.hot.misses));
    RedisModule_ReplyWithSimpleString(ctx, "warm_hits");
    RedisModule_ReplyWithLongLong(ctx, atomic_load(&g_cache.warm.hits));
    RedisModule_ReplyWithSimpleString(ctx, "warm_misses");
    RedisModule_ReplyWithLongLong(ctx, atomic_load(&g_cache.warm.misses));
    RedisModule_ReplyWithSimpleString(ctx, "warm_count");
    RedisModule_ReplyWithLongLong(ctx, atomic_load(&g_cache.warm.count));
    RedisModule_ReplyWithSimpleString(ctx, "cold_hits");
    RedisModule_ReplyWithLongLong(ctx, atomic_load(&g_cache.cold.hits));
    RedisModule_ReplyWithSimpleString(ctx, "cold_misses");
    RedisModule_ReplyWithLongLong(ctx, atomic_load(&g_cache.cold.misses));
    RedisModule_ReplyWithSimpleString(ctx, "read_throughs");
    RedisModule_ReplyWithLongLong(ctx, atomic_load(&g_cache.read_throughs));
    RedisModule_ReplyWithSimpleString(ctx, "ub_local_pct");
    uint64_t la = atomic_load(&g_cache.ub_mgr.local_accesses);
    uint64_t ra = atomic_load(&g_cache.ub_mgr.remote_accesses);
    uint64_t ta = la + ra;
    RedisModule_ReplyWithLongLong(ctx, ta > 0 ? (long long)(100 * la / ta) : 0);
    RedisModule_ReplyWithSimpleString(ctx, "ub_nodes");
    RedisModule_ReplyWithLongLong(ctx, g_cache.ub_mgr.num_nodes);

    return REDISMODULE_OK;
}

/* ============================================================
 * Module OnLoad
 * ============================================================ */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    (void)argv; (void)argc;

    if (RedisModule_Init(ctx, "tlc", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    /* Initialize three-layer cache with UB memory */
    RedisModule_Log(ctx, "notice", "TLC Module: Initializing three-layer cache with UB memory...");

    if (tlc_init(&g_cache, 0, 0) != 0) {
        RedisModule_Log(ctx, "warning", "TLC Module: Failed to initialize cache");
        return REDISMODULE_ERR;
    }

    /* Initialize embedding table */
    if (tlc_emb_init(&g_cache, SVE2_EMB_TABLE_SIZE, SVE2_EMB_DIM_DEFAULT) != 0) {
        RedisModule_Log(ctx, "warning", "TLC Module: Failed to initialize embedding table");
        /* Non-fatal — cache still works without embeddings */
    }

    g_initialized = 1;
    RedisModule_Log(ctx, "notice", "TLC Module: Cache initialized (HOT:%d WARM:%d UB:%d nodes)",
                    TLC_HOT_CAPACITY, TLC_WARM_CAPACITY, UB_NUM_NODES);

    /* Register commands */
    if (RedisModule_CreateCommand(ctx, "TLC.PUT", TLC_Put_RedisCommand,
                                  "write deny-oom fast", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "TLC.GET", TLC_Get_RedisCommand,
                                  "readonly fast", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "TLC.MPUT", TLC_MPut_RedisCommand,
                                  "write deny-oom fast", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "TLC.MGET", TLC_MGet_RedisCommand,
                                  "readonly fast", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "TLC.FILL", TLC_Fill_RedisCommand,
                                  "write deny-oom", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "TLC.STATS", TLC_Stats_RedisCommand,
                                  "readonly fast", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    RedisModule_Log(ctx, "notice", "TLC Module: 6 commands registered (TLC.PUT/GET/MPUT/MGET/FILL/STATS)");
    return REDISMODULE_OK;
}
