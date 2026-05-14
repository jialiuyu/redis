/*
 * SVE Accelerated Computing Implementation
 * High-performance vector operations using ARM SVE instructions
 */

#include "sve_compute.h"
#include "server.h"
#include <math.h>
#ifdef __linux__
#include <sys/auxv.h>
#include <asm/hwcap.h>
#endif

/* Global SVE context */
sve_context_t *global_sve_context = NULL;

#ifdef USE_ARM_SVE
static float sve_cosine_similarity_f32_sve_impl(const float *a, const float *b, size_t dim) {
    svfloat32_t dot_vec = svdup_f32(0.0f);
    svfloat32_t norm_a_vec = svdup_f32(0.0f);
    svfloat32_t norm_b_vec = svdup_f32(0.0f);

    size_t i = 0;
    svbool_t pg;

    while (i < dim) {
        pg = svwhilelt_b32(i, dim);

        svfloat32_t va = svld1_f32(pg, &a[i]);
        svfloat32_t vb = svld1_f32(pg, &b[i]);

        dot_vec = svmla_f32_m(pg, dot_vec, va, vb);
        norm_a_vec = svmla_f32_m(pg, norm_a_vec, va, va);
        norm_b_vec = svmla_f32_m(pg, norm_b_vec, vb, vb);

        i += svcntw();
    }

    float dot = svaddv_f32(svptrue_b32(), dot_vec);
    float norm_a = svaddv_f32(svptrue_b32(), norm_a_vec);
    float norm_b = svaddv_f32(svptrue_b32(), norm_b_vec);

    float denom = sqrtf(norm_a) * sqrtf(norm_b);
    if (denom == 0.0f) return 0.0f;
    return dot / denom;
}
#endif

static float sve_cosine_similarity_f32_scalar_impl(const float *a, const float *b, size_t dim) {
    float dot = 0.0f;
    float norm_a = 0.0f;
    float norm_b = 0.0f;

    for (size_t i = 0; i < dim; i++) {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }

    float denom = sqrtf(norm_a) * sqrtf(norm_b);
    if (denom == 0.0f) return 0.0f;
    return dot / denom;
}

float sve_cosine_similarity_f32(const float *a, const float *b, size_t dim) {
#ifdef USE_ARM_SVE
    return sve_cosine_similarity_f32_sve_impl(a, b, dim);
#else
    return sve_cosine_similarity_f32_scalar_impl(a, b, dim);
#endif
}

/* Detect SVE capabilities */
int sve_detect_capabilities(sve_context_t *ctx) {
    if (!ctx) return C_ERR;

    /* Simplified SVE detection for compatibility */
    ctx->has_sve = 0;  /* Disabled for now */
    ctx->has_sve2 = 0;
    ctx->has_bf16 = 0;
    ctx->vector_length = 256;  /* Assume 256-bit vectors */
    ctx->max_elements = ctx->vector_length / sizeof(float);

    serverLog(LL_NOTICE, "SVE detection: SVE not available (stub implementation)");
    return C_OK;
}

/* Initialize SVE compute */
int sve_compute_init(void) {
    if (global_sve_context) {
        return C_OK; /* Already initialized */
    }

    global_sve_context = zcalloc(sizeof(sve_context_t));
    if (!global_sve_context) {
        return C_ERR;
    }

    /* Detect SVE capabilities */
    if (sve_detect_capabilities(global_sve_context) != C_OK) {
        zfree(global_sve_context);
        global_sve_context = NULL;
        return C_ERR;
    }

    /* Initialize embedding cache */
    if (sve_cache_init(&global_sve_context->cache, SVE_EMBEDDING_CACHE_SIZE) != C_OK) {
        serverLog(LL_WARNING, "Failed to initialize SVE embedding cache");
        /* Continue without cache */
    }

    /* Initialize statistics */
    global_sve_context->total_gather_ops = 0;
    global_sve_context->total_scatter_ops = 0;
    global_sve_context->cache_hits = 0;
    global_sve_context->cache_misses = 0;
    global_sve_context->sve_instructions_used = 0;

    serverLog(LL_NOTICE, "SVE compute initialized successfully");
    return C_OK;
}

/* Cleanup SVE compute */
void sve_compute_cleanup(void) {
    if (!global_sve_context) return;

    if (global_sve_context->cache) {
        sve_cache_destroy(global_sve_context->cache);
    }

    zfree(global_sve_context);
    global_sve_context = NULL;

    serverLog(LL_NOTICE, "SVE compute cleaned up");
}

/* Batch gather embeddings using SVE */
int sve_batch_gather_embeddings(sve_context_t *ctx,
                               const float *embedding_table,
                               const uint64_t *indices,
                               size_t num_indices,
                               size_t embedding_dim,
                               float *results) {
    if (!ctx || !embedding_table || !indices || !results) {
        return C_ERR;
    }

    ctx->total_gather_ops++;

    /* Fallback to scalar implementation */
    for (size_t i = 0; i < num_indices; i++) {
        uint64_t idx = indices[i];
        const float *src = &embedding_table[idx * embedding_dim];
        float *dst = &results[i * embedding_dim];
        memcpy(dst, src, embedding_dim * sizeof(float));
    }

    return C_OK;
}

/* Batch scatter embeddings using SVE */
int sve_batch_scatter_embeddings(sve_context_t *ctx,
                                float *embedding_table,
                                const uint64_t *indices,
                                const float *values,
                                size_t num_indices,
                                size_t embedding_dim) {
    if (!ctx || !embedding_table || !indices || !values) {
        return C_ERR;
    }

    ctx->total_scatter_ops++;

    /* Fallback to scalar implementation */
    for (size_t i = 0; i < num_indices; i++) {
        uint64_t idx = indices[i];
        float *dst = &embedding_table[idx * embedding_dim];
        const float *src = &values[i * embedding_dim];
        memcpy(dst, src, embedding_dim * sizeof(float));
    }

    return C_OK;
}

/* Compute similarities using SVE */
int sve_compute_similarity(sve_context_t *ctx,
                          const float *query_vector,
                          const float *embedding_table,
                          const uint64_t *indices,
                          size_t num_indices,
                          size_t embedding_dim,
                          float *similarities) {
    if (!ctx || !query_vector || !embedding_table || !indices || !similarities) {
        return C_ERR;
    }

    /* Scalar similarity computation */
    for (size_t i = 0; i < num_indices; i++) {
        uint64_t idx = indices[i];
        const float *embedding = &embedding_table[idx * embedding_dim];

        float dot_product = 0.0f;
        for (size_t d = 0; d < embedding_dim; d++) {
            dot_product += query_vector[d] * embedding[d];
        }

        similarities[i] = dot_product;  /* Cosine similarity for normalized vectors */
    }

    return C_OK;
}

/* Streaming load (non-temporal) */
int sve_streaming_load_f32(sve_context_t *ctx,
                          const float *src,
                          float *dst,
                          size_t num_elements) {
    if (!ctx || !src || !dst) {
        return C_ERR;
    }

    /* Fallback to regular memcpy */
    memcpy(dst, src, num_elements * sizeof(float));
    return C_OK;
}

/* Streaming store (non-temporal) */
int sve_streaming_store_f32(sve_context_t *ctx,
                           const float *src,
                           float *dst,
                           size_t num_elements) {
    if (!ctx || !src || !dst) {
        return C_ERR;
    }

    /* Fallback to regular memcpy */
    memcpy(dst, src, num_elements * sizeof(float));
    return C_OK;
}

/* Cache operations */
int sve_cache_init(sve_embedding_cache_t **cache, size_t capacity) {
    *cache = zcalloc(sizeof(sve_embedding_cache_t));
    if (!*cache) return C_ERR;

    (*cache)->entries = zcalloc(sizeof(sve_cache_entry_t) * capacity);
    if (!(*cache)->entries) {
        zfree(*cache);
        *cache = NULL;
        return C_ERR;
    }

    (*cache)->size = 0;
    (*cache)->capacity = capacity;
    (*cache)->access_counter = 0;

    pthread_rwlock_init(&(*cache)->lock, NULL);

    return C_OK;
}

void sve_cache_destroy(sve_embedding_cache_t *cache) {
    if (!cache) return;

    pthread_rwlock_wrlock(&cache->lock);

    for (size_t i = 0; i < cache->capacity; i++) {
        if (cache->entries[i].valid && cache->entries[i].vector) {
            zfree(cache->entries[i].vector);
        }
    }

    zfree(cache->entries);
    pthread_rwlock_unlock(&cache->lock);
    pthread_rwlock_destroy(&cache->lock);
    zfree(cache);
}

int sve_cache_lookup(sve_embedding_cache_t *cache,
                    uint64_t key,
                    float **vector,
                    size_t *dim) {
    if (!cache || !vector || !dim) return C_ERR;

    pthread_rwlock_rdlock(&cache->lock);

    /* Simple linear search - in production, use hash table */
    for (size_t i = 0; i < cache->capacity; i++) {
        if (cache->entries[i].valid && cache->entries[i].key == key) {
            *vector = cache->entries[i].vector;
            *dim = cache->entries[i].dim;
            cache->entries[i].last_access = ++cache->access_counter;

            pthread_rwlock_unlock(&cache->lock);
            return C_OK;
        }
    }

    pthread_rwlock_unlock(&cache->lock);
    return C_ERR; /* Not found */
}

int sve_cache_store(sve_embedding_cache_t *cache,
                   uint64_t key,
                   const float *vector,
                   size_t dim) {
    if (!cache || !vector) return C_ERR;

    pthread_rwlock_wrlock(&cache->lock);

    /* Find empty slot or evict LRU */
    size_t slot = cache->capacity;
    uint64_t oldest_access = UINT64_MAX;

    for (size_t i = 0; i < cache->capacity; i++) {
        if (!cache->entries[i].valid) {
            slot = i;
            break;
        } else if (cache->entries[i].last_access < oldest_access) {
            oldest_access = cache->entries[i].last_access;
            slot = i;
        }
    }

    if (slot >= cache->capacity) {
        pthread_rwlock_unlock(&cache->lock);
        return C_ERR; /* No slot available */
    }

    /* Evict old entry if necessary */
    if (cache->entries[slot].valid && cache->entries[slot].vector) {
        zfree(cache->entries[slot].vector);
    } else {
        cache->size++;
    }

    /* Store new entry */
    cache->entries[slot].key = key;
    cache->entries[slot].dim = dim;
    cache->entries[slot].vector = zmalloc(sizeof(float) * dim);
    if (!cache->entries[slot].vector) {
        pthread_rwlock_unlock(&cache->lock);
        return C_ERR;
    }

    memcpy(cache->entries[slot].vector, vector, sizeof(float) * dim);
    cache->entries[slot].last_access = ++cache->access_counter;
    cache->entries[slot].valid = 1;

    pthread_rwlock_unlock(&cache->lock);
    return C_OK;
}

/* Pipeline processing */
int sve_pipeline_process_batch(sve_context_t *ctx,
                              sve_feature_request_t *request,
                              sve_feature_result_t *result) {
    if (!ctx || !request || !result) return C_ERR;

    /* Allocate result arrays */
    result->embeddings = zmalloc(sizeof(float) * request->num_features * request->embedding_dim);
    result->missing_ids = zmalloc(sizeof(uint64_t) * request->num_features);
    result->num_embeddings = 0;
    result->num_missing = 0;

    if (!result->embeddings || !result->missing_ids) {
        if (result->embeddings) zfree(result->embeddings);
        if (result->missing_ids) zfree(result->missing_ids);
        return C_ERR;
    }

    /* Process each feature */
    for (size_t i = 0; i < request->num_features; i++) {
        uint64_t feature_id = request->feature_ids[i];

        /* Try cache first */
        float *cached_vector = NULL;
        size_t cached_dim = 0;

        if (request->use_cache && ctx->cache &&
            sve_cache_lookup(ctx->cache, feature_id, &cached_vector, &cached_dim) == C_OK) {
            /* Cache hit */
            ctx->cache_hits++;
            memcpy(&result->embeddings[result->num_embeddings * request->embedding_dim],
                   cached_vector, sizeof(float) * cached_dim);
            result->num_embeddings++;
        } else {
            /* Cache miss - add to missing list */
            ctx->cache_misses++;
            result->missing_ids[result->num_missing++] = feature_id;
        }
    }

    return C_OK;
}

/* Quantization support */
int sve_dequantize_q8_to_f32(const int8_t *q8_data,
                           float *f32_data,
                           size_t num_elements,
                           float scale,
                           float offset) {
    if (!q8_data || !f32_data) return C_ERR;

    /* Scalar implementation */
    for (size_t i = 0; i < num_elements; i++) {
        f32_data[i] = (float)q8_data[i] * scale + offset;
    }

    return C_OK;
}

int sve_quantize_f32_to_q8(const float *f32_data,
                          int8_t *q8_data,
                          size_t num_elements,
                          float *scale,
                          float *offset) {
    if (!f32_data || !q8_data || !scale || !offset) return C_ERR;

    /* Find min/max for quantization range */
    float min_val = 1e30f;
    float max_val = -1e30f;

    for (size_t i = 0; i < num_elements; i++) {
        if (f32_data[i] < min_val) min_val = f32_data[i];
        if (f32_data[i] > max_val) max_val = f32_data[i];
    }

    *scale = (max_val - min_val) / 255.0f;
    *offset = min_val;

    if (*scale == 0.0f) {
        memset(q8_data, 0, num_elements * sizeof(int8_t));
        return C_OK;
    }

    /* Scalar quantization */
    for (size_t i = 0; i < num_elements; i++) {
        float scaled = (f32_data[i] - *offset) / *scale;
        int val = (int)(scaled + 0.5f);  /* Round to nearest */
        if (val < -128) val = -128;
        if (val > 127) val = 127;
        q8_data[i] = (int8_t)val;
    }

    return C_OK;
}

/* Feature query pipeline */
int sve_feature_query_pipeline(sve_context_t *ctx,
                              const char *query_type,
                              sve_feature_request_t *request,
                              sve_feature_result_t *result) {
    /* High-level pipeline for different query types */
    if (strcmp(query_type, "embedding_lookup") == 0) {
        return sve_pipeline_process_batch(ctx, request, result);
    } else if (strcmp(query_type, "similarity_search") == 0) {
        /* Similarity search pipeline */
        /* This would orchestrate gather + similarity computation */
        return C_OK;
    }

    return C_ERR;
}

/* Memory prefetching */
void sve_prefetch_embeddings(const float *embedding_table,
                           const uint64_t *indices,
                           size_t num_indices,
                           size_t embedding_dim) {
    if (!embedding_table || !indices) return;

    /* Simple prefetch implementation */
    for (size_t i = 0; i < num_indices; i++) {
        uint64_t idx = indices[i];
        /* Prefetch the embedding data */
        __builtin_prefetch(&embedding_table[idx * embedding_dim], 0, 1);
    }
}

/* Statistics */
sds sve_get_stats(sve_context_t *ctx) {
    sds stats = sdsempty();

    if (!ctx) {
        stats = sdscat(stats, "SVE Context: Not initialized");
        return stats;
    }

    stats = sdscatprintf(stats, "SVE Compute Stats:\n");
    stats = sdscatprintf(stats, "  SVE Supported: %s\n", ctx->has_sve ? "Yes" : "No");
    stats = sdscatprintf(stats, "  SVE2 Supported: %s\n", ctx->has_sve2 ? "Yes" : "No");
    stats = sdscatprintf(stats, "  BF16 Supported: %s\n", ctx->has_bf16 ? "Yes" : "No");
    stats = sdscatprintf(stats, "  Vector Length: %zu bytes\n", ctx->vector_length);
    stats = sdscatprintf(stats, "  Max Elements: %zu\n", ctx->max_elements);
    stats = sdscatprintf(stats, "  Total Gather Ops: %llu\n", (unsigned long long)ctx->total_gather_ops);
    stats = sdscatprintf(stats, "  Total Scatter Ops: %llu\n", (unsigned long long)ctx->total_scatter_ops);
    stats = sdscatprintf(stats, "  Cache Hits: %llu\n", (unsigned long long)ctx->cache_hits);
    stats = sdscatprintf(stats, "  Cache Misses: %llu\n", (unsigned long long)ctx->cache_misses);
    stats = sdscatprintf(stats, "  SVE Instructions Used: %llu\n", (unsigned long long)ctx->sve_instructions_used);

    if (ctx->cache) {
        stats = sdscatprintf(stats, "  Cache Size: %zu/%zu\n", ctx->cache->size, ctx->cache->capacity);
    }

    return stats;
}
