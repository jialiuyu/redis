/*
 * SVE Compute - Standalone version for benchmark
 * 不依赖 Redis server.h
 */

#ifndef __SVE_COMPUTE_STANDALONE_H
#define __SVE_COMPUTE_STANDALONE_H

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>

/* SVE Context - 简化版 */
typedef struct {
    size_t vector_length;
    size_t max_elements;
    int has_sve;
    int has_sve2;
    int has_bf16;
    uint64_t total_gather_ops;
    uint64_t total_scatter_ops;
} sve_context_t;

/* API 函数 */
int sve_detect_capabilities(sve_context_t *ctx);

int sve_batch_gather_embeddings(sve_context_t *ctx,
                               const float *embedding_table,
                               const uint64_t *indices,
                               size_t num_indices,
                               size_t embedding_dim,
                               float *results);

#endif /* __SVE_COMPUTE_STANDALONE_H */
