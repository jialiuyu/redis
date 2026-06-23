/*
 * SVE Compute - Standalone implementation for benchmark
 */

#include "sve_compute_standalone.h"
#include <stdio.h>

/* SVE support detection */
#if defined(__ARM_FEATURE_SVE) || defined(USE_SVE)
#define USE_ARM_SVE 1
#include <arm_sve.h>
#endif

/* Detect SVE capabilities */
int sve_detect_capabilities(sve_context_t *ctx) {
    if (!ctx) return -1;

#ifdef USE_ARM_SVE
    /* SVE is available at compile time */
    ctx->has_sve = 1;
    ctx->has_sve2 = 1;  /* Assume SVE2 if SVE is available */
    ctx->has_bf16 = 0;
    
    /* Get SVE vector length at runtime */
    ctx->vector_length = svcntb();  /* Count bytes in SVE vector */
    ctx->max_elements = ctx->vector_length / sizeof(float);
    
    printf("  SVE detected: vector length = %zu bytes (%zu floats)\n",
           ctx->vector_length, ctx->max_elements);
#else
    /* SVE not available - use scalar fallback */
    ctx->has_sve = 0;
    ctx->has_sve2 = 0;
    ctx->has_bf16 = 0;
    ctx->vector_length = 256;  /* Assume 256-bit for estimation */
    ctx->max_elements = ctx->vector_length / sizeof(float);
    
    printf("  SVE not available - using scalar fallback\n");
#endif

    ctx->total_gather_ops = 0;
    ctx->total_scatter_ops = 0;

    return 0;
}

/* Batch gather embeddings using SVE */
int sve_batch_gather_embeddings(sve_context_t *ctx,
                               const float *embedding_table,
                               const uint64_t *indices,
                               size_t num_indices,
                               size_t embedding_dim,
                               float *results) {
    if (!ctx || !embedding_table || !indices || !results) {
        return -1;
    }

    ctx->total_gather_ops++;

#ifdef USE_ARM_SVE
    /* SVE implementation */
    if (ctx->has_sve) {
        /* Process each embedding */
        for (size_t i = 0; i < num_indices; i++) {
            uint64_t idx = indices[i];
            const float *src = &embedding_table[idx * embedding_dim];
            float *dst = &results[i * embedding_dim];
            
            /* Use SVE to copy embedding data */
            size_t offset = 0;
            svbool_t pg = svptrue_b32();
            
            while (offset < embedding_dim) {
                size_t remaining = embedding_dim - offset;
                size_t vl = svcntw();  /* Vector length in words (float32) */
                
                if (remaining < vl) {
                    /* Handle tail with predicate */
                    pg = svwhilelt_b32(offset, embedding_dim);
                }
                
                /* Load from source using SVE */
                svfloat32_t vec = svld1_f32(pg, &src[offset]);
                
                /* Store to destination using SVE */
                svst1_f32(pg, &dst[offset], vec);
                
                offset += vl;
                if (remaining < vl) break;
            }
        }
        return 0;
    }
#endif

    /* Scalar fallback — optimized with prefetching */
    for (size_t i = 0; i < num_indices; i++) {
        uint64_t idx = indices[i];
        const float *src = &embedding_table[idx * embedding_dim];
        float *dst = &results[i * embedding_dim];
        
        /* Prefetch next embedding while copying current */
        if (i + 1 < num_indices) {
            uint64_t next_idx = indices[i + 1];
            __builtin_prefetch(&embedding_table[next_idx * embedding_dim], 0, 1);
        }
        
        memcpy(dst, src, embedding_dim * sizeof(float));
    }

    return 0;
}
