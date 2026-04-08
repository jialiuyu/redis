/*
 * SVE (Scalable Vector Extension) Accelerated Computing
 * High-performance vector operations for feature queries
 */

#ifndef __SVE_COMPUTE_H
#define __SVE_COMPUTE_H

#include "server.h"
#include <stdint.h>
#include <arm_sve.h>  /* ARM SVE intrinsics */

/* SVE Configuration */
#define SVE_MAX_VECTOR_LENGTH 256  /* Maximum SVE vector length in bits */
#define SVE_EMBEDDING_CACHE_SIZE (1024 * 1024)  /* 1M embeddings cache */
#define SVE_BATCH_SIZE 1024       /* Batch processing size */

/* SVE Vector Types - Simplified for compatibility */
typedef void* sve_f32_t;
typedef void* sve_u64_t;
typedef void* sve_pred_t;

/* Embedding Cache Entry */
typedef struct {
    uint64_t key;              /* Embedding ID */
    float *vector;             /* Cached vector data */
    size_t dim;               /* Vector dimension */
    uint64_t last_access;      /* Last access timestamp */
    int valid;                /* Cache entry valid */
} sve_cache_entry_t;

/* Embedding Cache */
typedef struct {
    sve_cache_entry_t *entries;
    size_t size;
    size_t capacity;
    uint64_t access_counter;
    pthread_rwlock_t lock;
} sve_embedding_cache_t;

/* SVE Context */
typedef struct {
    size_t vector_length;      /* SVE vector length in bytes */
    size_t max_elements;       /* Max elements per vector operation */
    sve_embedding_cache_t *cache; /* Embedding cache */

    /* SVE capabilities */
    int has_sve;              /* SVE support flag */
    int has_sve2;             /* SVE2 support flag */
    int has_bf16;             /* BF16 support flag */

    /* Statistics */
    uint64_t total_gather_ops;
    uint64_t total_scatter_ops;
    uint64_t cache_hits;
    uint64_t cache_misses;
    uint64_t sve_instructions_used;
} sve_context_t;

/* Feature Query Request */
typedef struct {
    uint64_t *feature_ids;     /* Array of feature IDs */
    size_t num_features;       /* Number of features */
    size_t embedding_dim;      /* Embedding dimension */
    float *query_vector;       /* Query vector (optional) */
    int use_cache;            /* Whether to use cache */
} sve_feature_request_t;

/* Feature Query Result */
typedef struct {
    float *embeddings;         /* Retrieved embeddings */
    size_t num_embeddings;     /* Number of embeddings retrieved */
    uint64_t *missing_ids;     /* IDs not found in cache/memory */
    size_t num_missing;        /* Number of missing embeddings */
} sve_feature_result_t;

/* Global SVE context */
extern sve_context_t *global_sve_context;

/* SVE API */
int sve_compute_init(void);
void sve_compute_cleanup(void);

int sve_detect_capabilities(sve_context_t *ctx);

/* Vector Operations */
int sve_batch_gather_embeddings(sve_context_t *ctx,
                               const float *embedding_table,
                               const uint64_t *indices,
                               size_t num_indices,
                               size_t embedding_dim,
                               float *results);

int sve_batch_scatter_embeddings(sve_context_t *ctx,
                                float *embedding_table,
                                const uint64_t *indices,
                                const float *values,
                                size_t num_indices,
                                size_t embedding_dim);

int sve_compute_similarity(sve_context_t *ctx,
                          const float *query_vector,
                          const float *embedding_table,
                          const uint64_t *indices,
                          size_t num_indices,
                          size_t embedding_dim,
                          float *similarities);

/* Non-temporal Memory Access (Streaming) */
int sve_streaming_load_f32(sve_context_t *ctx,
                          const float *src,
                          float *dst,
                          size_t num_elements);

int sve_streaming_store_f32(sve_context_t *ctx,
                           const float *src,
                           float *dst,
                           size_t num_elements);

/* Cache Operations */
int sve_cache_init(sve_embedding_cache_t **cache, size_t capacity);
void sve_cache_destroy(sve_embedding_cache_t *cache);

int sve_cache_lookup(sve_embedding_cache_t *cache,
                    uint64_t key,
                    float **vector,
                    size_t *dim);

int sve_cache_store(sve_embedding_cache_t *cache,
                   uint64_t key,
                   const float *vector,
                   size_t dim);

void sve_cache_evict_lru(sve_embedding_cache_t *cache);

/* Pipeline Operations */
int sve_pipeline_process_batch(sve_context_t *ctx,
                              sve_feature_request_t *request,
                              sve_feature_result_t *result);

/* Quantization Support */
int sve_dequantize_q8_to_f32(const int8_t *q8_data,
                           float *f32_data,
                           size_t num_elements,
                           float scale,
                           float offset);

int sve_quantize_f32_to_q8(const float *f32_data,
                          int8_t *q8_data,
                          size_t num_elements,
                          float *scale,
                          float *offset);

/* Feature Query Pipeline */
int sve_feature_query_pipeline(sve_context_t *ctx,
                              const char *query_type,
                              sve_feature_request_t *request,
                              sve_feature_result_t *result);

/* Memory Prefetching */
void sve_prefetch_embeddings(const float *embedding_table,
                           const uint64_t *indices,
                           size_t num_indices,
                           size_t embedding_dim);

/* Statistics */
sds sve_get_stats(sve_context_t *ctx);

/* Low-level SVE Intrinsics Wrappers - Stub implementations */
static inline sve_f32_t sve_load_f32(sve_pred_t pred, const float *addr) {
    return NULL; /* Stub */
}

static inline void sve_store_f32(sve_pred_t pred, float *addr, sve_f32_t data) {
    /* Stub */
}

static inline sve_f32_t sve_gather_f32(sve_pred_t pred,
                                     const float *base,
                                     sve_u64_t indices) {
    return NULL; /* Stub */
}

static inline void sve_scatter_f32(sve_pred_t pred,
                                  float *base,
                                  sve_u64_t indices,
                                  sve_f32_t data) {
    /* Stub */
}

#endif /* __SVE_COMPUTE_H */