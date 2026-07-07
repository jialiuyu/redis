#include "sve_operation.h"
#include "cpu_relax.h"
#include "macro.h"
#include "monotonic.h"
#include <stdlib.h>
#include <stdio.h>

#ifdef USE_ARM_SVE
#include <arm_sve.h>
#endif

#ifdef SVE_OP_STANDALONE
static inline void *zcalloc(size_t n) { return calloc(1, n); }
static inline void  zfree(void *p)    { free(p); }
#else
#include "zmalloc.h"
#endif

/* ============================================================
 * Bitmap 操作
 * ============================================================ */

int bitmap_init(state_bitmap_t *bmp, size_t num_bits) {
    bmp->num_words = (num_bits + BITMAP_BITS_PER_WORD - 1) / BITMAP_BITS_PER_WORD;
    if (bmp->num_words < 1) bmp->num_words = 1;
    bmp->bits = (bitmap_atomic_word_t *) zcalloc(bmp->num_words * sizeof(bitmap_atomic_word_t));
    RETURN_IF(!bmp->bits, -1);
    for (size_t i = 0; i < bmp->num_words; i++)
        atomic_init(&bmp->bits[i].word, 0);
    return 0;
}

void bitmap_destroy(state_bitmap_t *bmp) {
    if (bmp && bmp->bits) { zfree(bmp->bits); bmp->bits = NULL; }
}

int bitmap_try_acquire(state_bitmap_t *bmp, uint64_t bit_index) {
    RETURN_IF(!bmp || !bmp->bits, -1);
    uint64_t wi = bit_index >> BITMAP_WORD_SHIFT;
    uint64_t bo = bit_index & BITMAP_WORD_MASK;
    RETURN_IF(wi >= bmp->num_words, -1);
    const uint64_t mask = 1ULL << bo;

    /*
     * Single atomic fetch_or: set the bit and return the previous value.
     * If the bit was already set (prev & mask), someone else owns it.
     * No CAS loop needed — one instruction, no retries.
     */
    uint64_t prev = atomic_fetch_or_explicit(&bmp->bits[wi].word,
                                             mask, memory_order_acquire);
    return (prev & mask) ? -1 : 0;
}

void bitmap_release(state_bitmap_t *bmp, uint64_t bit_index) {
    RETURN_IF(!bmp || !bmp->bits);
    uint64_t wi = bit_index >> BITMAP_WORD_SHIFT;
    uint64_t bo = bit_index & BITMAP_WORD_MASK;
    RETURN_IF(wi >= bmp->num_words);
    atomic_fetch_and_explicit(&bmp->bits[wi].word,
                              ~(1ULL << bo), memory_order_release);
}

void bitmap_lock_blocking(state_bitmap_t *bmp, uint64_t bit_index) {
    while (bitmap_try_acquire(bmp, bit_index) != 0)
        cpu_relax();
}

void bitmap_unlock(state_bitmap_t *bmp, uint64_t bit_index) {
    bitmap_release(bmp, bit_index);
}

void sve_gather_ctx_init(sve_gather_ctx_t *ctx,
                         ub_address_space_t *ubas,
                         state_bitmap_t *bitmap,
                         size_t vector_dim,
                         size_t vector_stride_bytes,
                         uint64_t table_row_capacity,
                         sve_operation_stats_t *stats) {
    RETURN_IF(!ctx);

    ctx->ubas = ubas;
    ctx->bitmap = bitmap;
    ctx->vector_dim = vector_dim;
    ctx->vector_stride_bytes = vector_stride_bytes;
    ctx->table_row_capacity = table_row_capacity;
    ctx->stats = stats;
    ctx->bitmap_lock_latency_ns_accum = NULL;
    ctx->bitmap_unlock_latency_ns_accum = NULL;
    ctx->vector_load_latency_ns_accum = NULL;
}

int sve_serial_contiguous_read(sve_gather_ctx_t *ctx,
                               uint64_t *emb_ids,
                               size_t num_ids,
                               float *results) {
    RETURN_IF(!ctx || !ctx->ubas || !ctx->bitmap || !ctx->stats ||
              !ctx->ubas->mapped_addr || !emb_ids || !results || num_ids == 0 || ctx->vector_dim == 0, -1);

    const size_t dim = ctx->vector_dim;
    const size_t row_bytes = dim * sizeof(float);
    uint64_t bitmap_lock_ns_sink = 0;
    uint64_t bitmap_unlock_ns_sink = 0;
    uint64_t vector_load_ns_sink = 0;
    uint64_t *bitmap_lock_ns = ctx->bitmap_lock_latency_ns_accum ? ctx->bitmap_lock_latency_ns_accum : &bitmap_lock_ns_sink;
    uint64_t *bitmap_unlock_ns = ctx->bitmap_unlock_latency_ns_accum ? ctx->bitmap_unlock_latency_ns_accum : &bitmap_unlock_ns_sink;
    uint64_t *vector_load_ns = ctx->vector_load_latency_ns_accum ? ctx->vector_load_latency_ns_accum : &vector_load_ns_sink;
    const int measure_bitmap_lock = ctx->bitmap_lock_latency_ns_accum != NULL;
    const int measure_bitmap_unlock = ctx->bitmap_unlock_latency_ns_accum != NULL;
    const int measure_vector_load = ctx->vector_load_latency_ns_accum != NULL;

    for (size_t i = 0; i < num_ids; i++) {
        monotime lock_start = 0;
        uint64_t lock_elapsed_ns = 0;
        if (measure_bitmap_lock) {
            elapsedStartNs(&lock_start);
        }
        if (bitmap_try_acquire(ctx->bitmap, emb_ids[i]) != 0) {
            atomic_fetch_add_explicit(&ctx->stats->lock_failure, 1, memory_order_relaxed);
            if (measure_bitmap_lock) {
                lock_elapsed_ns = elapsedNs(lock_start);
            }
            *bitmap_lock_ns += lock_elapsed_ns;
            memset(&results[i * dim], 0, row_bytes);
            continue;
        }

        atomic_fetch_add_explicit(&ctx->stats->lock_success, 1, memory_order_relaxed);
        if (measure_bitmap_lock) {
            lock_elapsed_ns = elapsedNs(lock_start);
        }
        *bitmap_lock_ns += lock_elapsed_ns;
        if (emb_ids[i] >= ctx->table_row_capacity) {
            memset(&results[i * dim], 0, row_bytes);
            monotime release_start = 0;
            uint64_t release_elapsed_ns = 0;
            if (measure_bitmap_unlock) {
                elapsedStartNs(&release_start);
            }
            bitmap_release(ctx->bitmap, emb_ids[i]);
            if (measure_bitmap_unlock) {
                release_elapsed_ns = elapsedNs(release_start);
            }
            *bitmap_unlock_ns += release_elapsed_ns;
            return -1;
        }

        const float *src = (const float *)((const char *)ctx->ubas->mapped_addr +
                                           emb_ids[i] * ctx->vector_stride_bytes);
        monotime load_start = 0;
        uint64_t load_elapsed_ns = 0;
        if (measure_vector_load) {
            elapsedStartNs(&load_start);
        }
#ifdef USE_ARM_SVE
        {
            const size_t vl = svcntw();
            size_t rem = dim;
            const float *srcp = src;
            float *dstp = &results[i * dim];

            while (rem >= vl) {
                svbool_t pg = svptrue_b32();
                svfloat32_t v = svld1_f32(pg, srcp);
                svst1_f32(pg, dstp, v);
                srcp += vl;
                dstp += vl;
                rem -= vl;
            }

            if (rem > 0) {
                svbool_t pg = svwhilelt_b32_u64(0UL, (uint64_t)rem);
                svfloat32_t v = svld1_f32(pg, srcp);
                svst1_f32(pg, dstp, v);
            }
        }
#else
        memcpy(&results[i * dim], src, row_bytes);
#endif
        if (measure_vector_load) {
            load_elapsed_ns = elapsedNs(load_start);
        }
        *vector_load_ns += load_elapsed_ns;
        monotime release_start = 0;
        uint64_t release_elapsed_ns = 0;
        if (measure_bitmap_unlock) {
            elapsedStartNs(&release_start);
        }
        bitmap_release(ctx->bitmap, emb_ids[i]);
        if (measure_bitmap_unlock) {
            release_elapsed_ns = elapsedNs(release_start);
        }
        *bitmap_unlock_ns += release_elapsed_ns;
    }

    return 0;
}

int sve_serial_contiguous_read_traced(sve_gather_ctx_t *ctx,
                                      uint64_t *emb_ids,
                                      size_t num_ids,
                                      float *results,
                                      uint64_t *bitmap_lock_latency_ns,
                                      uint64_t *bitmap_unlock_latency_ns,
                                      uint64_t *vector_load_latency_ns) {
    RETURN_IF(!ctx || !ctx->ubas || !ctx->bitmap || !ctx->stats ||
              !ctx->ubas->mapped_addr || !emb_ids || !results || num_ids == 0 ||
              ctx->vector_dim == 0 || !bitmap_lock_latency_ns ||
              !bitmap_unlock_latency_ns || !vector_load_latency_ns, -1);

    const size_t dim = ctx->vector_dim;
    const size_t row_bytes = dim * sizeof(float);

    *bitmap_lock_latency_ns = 0;
    *bitmap_unlock_latency_ns = 0;
    *vector_load_latency_ns = 0;

    for (size_t i = 0; i < num_ids; i++) {
        monotime lock_start;
        elapsedStartNs(&lock_start);
        if (bitmap_try_acquire(ctx->bitmap, emb_ids[i]) != 0) {
            atomic_fetch_add_explicit(&ctx->stats->lock_failure, 1, memory_order_relaxed);
            *bitmap_lock_latency_ns += elapsedNs(lock_start);
            memset(&results[i * dim], 0, row_bytes);
            continue;
        }

        atomic_fetch_add_explicit(&ctx->stats->lock_success, 1, memory_order_relaxed);
        *bitmap_lock_latency_ns += elapsedNs(lock_start);

        if (emb_ids[i] >= ctx->table_row_capacity) {
            memset(&results[i * dim], 0, row_bytes);
            monotime release_start;
            elapsedStartNs(&release_start);
            bitmap_release(ctx->bitmap, emb_ids[i]);
            *bitmap_unlock_latency_ns += elapsedNs(release_start);
            return -1;
        }

        const float *src = (const float *)((const char *)ctx->ubas->mapped_addr +
                                           emb_ids[i] * ctx->vector_stride_bytes);
        monotime load_start;
        elapsedStartNs(&load_start);
#ifdef USE_ARM_SVE
        {
            const size_t vl = svcntw();
            size_t rem = dim;
            const float *srcp = src;
            float *dstp = &results[i * dim];

            while (rem >= vl) {
                svbool_t pg = svptrue_b32();
                svfloat32_t v = svld1_f32(pg, srcp);
                svst1_f32(pg, dstp, v);
                srcp += vl;
                dstp += vl;
                rem -= vl;
            }

            if (rem > 0) {
                svbool_t pg = svwhilelt_b32_u64(0UL, (uint64_t)rem);
                svfloat32_t v = svld1_f32(pg, srcp);
                svst1_f32(pg, dstp, v);
            }
        }
#else
        memcpy(&results[i * dim], src, row_bytes);
#endif
        *vector_load_latency_ns += elapsedNs(load_start);
        monotime release_start;
        elapsedStartNs(&release_start);
        bitmap_release(ctx->bitmap, emb_ids[i]);
        *bitmap_unlock_latency_ns += elapsedNs(release_start);
    }

    return 0;
}

int sve_serial_contiguous_read_blocking_traced(sve_gather_ctx_t *ctx,
                                               uint64_t *emb_ids,
                                               size_t num_ids,
                                               float *results,
                                               uint64_t *bitmap_lock_latency_ns,
                                               uint64_t *bitmap_unlock_latency_ns,
                                               uint64_t *vector_load_latency_ns) {
    RETURN_IF(!ctx || !ctx->ubas || !ctx->bitmap || !ctx->stats ||
              !ctx->ubas->mapped_addr || !emb_ids || !results || num_ids == 0 ||
              ctx->vector_dim == 0 || !bitmap_lock_latency_ns ||
              !bitmap_unlock_latency_ns || !vector_load_latency_ns, -1);

    const size_t dim = ctx->vector_dim;
    const size_t row_bytes = dim * sizeof(float);

    *bitmap_lock_latency_ns = 0;
    *bitmap_unlock_latency_ns = 0;
    *vector_load_latency_ns = 0;

    for (size_t i = 0; i < num_ids; i++) {
        if (emb_ids[i] >= ctx->table_row_capacity) {
            memset(&results[i * dim], 0, row_bytes);
            return -1;
        }

        monotime lock_start;
        elapsedStartNs(&lock_start);
        while (bitmap_try_acquire(ctx->bitmap, emb_ids[i]) != 0) {
            atomic_fetch_add_explicit(&ctx->stats->lock_failure, 1,
                                      memory_order_relaxed);
            __asm__ __volatile__("yield" ::: "memory");
        }
        atomic_fetch_add_explicit(&ctx->stats->lock_success, 1,
                                  memory_order_relaxed);
        *bitmap_lock_latency_ns += elapsedNs(lock_start);

        const float *src = (const float *)((const char *)ctx->ubas->mapped_addr +
                                           emb_ids[i] * ctx->vector_stride_bytes);
        monotime load_start;
        elapsedStartNs(&load_start);
#ifdef USE_ARM_SVE
        {
            const size_t vl = svcntw();
            size_t rem = dim;
            const float *srcp = src;
            float *dstp = &results[i * dim];

            while (rem >= vl) {
                svbool_t pg = svptrue_b32();
                svfloat32_t v = svld1_f32(pg, srcp);
                svst1_f32(pg, dstp, v);
                srcp += vl;
                dstp += vl;
                rem -= vl;
            }

            if (rem > 0) {
                svbool_t pg = svwhilelt_b32_u64(0UL, (uint64_t)rem);
                svfloat32_t v = svld1_f32(pg, srcp);
                svst1_f32(pg, dstp, v);
            }
        }
#else
        memcpy(&results[i * dim], src, row_bytes);
#endif
        *vector_load_latency_ns += elapsedNs(load_start);

        monotime release_start;
        elapsedStartNs(&release_start);
        bitmap_release(ctx->bitmap, emb_ids[i]);
        *bitmap_unlock_latency_ns += elapsedNs(release_start);
    }

    return 0;
}

/* ============================================================
 * Streaming Load / Store（非临时内存访问）
 * ============================================================ */

int sve_cross_emb_gather_read(sve_gather_ctx_t *ctx,
                                uint64_t *emb_ids,
                                size_t num_ids,
                                float *results) {
    RETURN_IF(!ctx || !ctx->ubas || !ctx->bitmap || !ctx->stats ||
              !ctx->ubas->mapped_addr || !emb_ids || !results ||
              num_ids == 0 || ctx->vector_dim == 0, -1);

    const size_t dim = ctx->vector_dim;
    const char *table_base = (const char *)ctx->ubas->mapped_addr;
    const size_t stride = ctx->vector_stride_bytes;

#ifdef USE_ARM_SVE
    const size_t vl = (size_t)svcntw();

    for (size_t blk_start = 0; blk_start < num_ids; blk_start += vl) {
        size_t blk_len = num_ids - blk_start;
        if (blk_len > vl) blk_len = vl;

        int acquired[SVE_OP_VL];

        for (;;) {
            int all_ok = 1;
            for (size_t i = 0; i < blk_len; i++)
                acquired[i] = 0;

            for (size_t i = 0; i < blk_len; i++) {
                if (emb_ids[blk_start + i] >= ctx->table_row_capacity)
                    continue;
                if (bitmap_try_acquire(ctx->bitmap,
                            emb_ids[blk_start + i]) == 0) {
                    acquired[i] = 1;
                } else {
                    all_ok = 0;
                    break;
                }
            }

            if (all_ok) break;

            for (size_t i = 0; i < blk_len; i++) {
                if (acquired[i]) {
                    bitmap_release(ctx->bitmap, emb_ids[blk_start + i]);
                    acquired[i] = 0;
                }
            }
            atomic_fetch_add_explicit(&ctx->stats->lock_failure, 1,
                                      memory_order_relaxed);
            __asm__ __volatile__("yield" ::: "memory");
        }

        atomic_fetch_add_explicit(&ctx->stats->lock_success, 1,
                                  memory_order_relaxed);

        const float *src_ptrs[SVE_OP_VL];
        float *dst_ptrs[SVE_OP_VL];
        int any_valid = 0;
        for (size_t i = 0; i < blk_len; i++) {
            if (acquired[i]) {
                src_ptrs[i] = (const float *)(table_base +
                    emb_ids[blk_start + i] * stride);
                dst_ptrs[i] = &results[(blk_start + i) * dim];
                any_valid = 1;
            } else {
                src_ptrs[i] = NULL;
                dst_ptrs[i] = NULL;
            }
        }

        if (any_valid) {
            size_t col_off = 0;
            while (col_off + vl <= dim) {
                for (size_t i = 0; i < blk_len; i++) {
                    if (!acquired[i]) continue;
                    svfloat32_t v = svld1_f32(svptrue_b32(), src_ptrs[i] + col_off);
                    svst1_f32(svptrue_b32(), dst_ptrs[i] + col_off, v);
                }
                col_off += vl;
            }

            if (col_off < dim) {
                svbool_t pg = svwhilelt_b32_u64((uint64_t)col_off, (uint64_t)dim);
                for (size_t i = 0; i < blk_len; i++) {
                    if (!acquired[i]) continue;
                    svfloat32_t v = svld1_f32(pg, src_ptrs[i] + col_off);
                    svst1_f32(pg, dst_ptrs[i] + col_off, v);
                }
            }
        }

        for (size_t i = 0; i < blk_len; i++) {
            if (acquired[i]) {
                bitmap_release(ctx->bitmap, emb_ids[blk_start + i]);
            }
        }
    }
#else
    for (size_t i = 0; i < num_ids; i++) {
        while (bitmap_try_acquire(ctx->bitmap, emb_ids[i]) != 0) {
            atomic_fetch_add_explicit(&ctx->stats->lock_failure, 1,
                                      memory_order_relaxed);
            __asm__ __volatile__("yield" ::: "memory");
        }

        atomic_fetch_add_explicit(&ctx->stats->lock_success, 1,
                                  memory_order_relaxed);
        if (emb_ids[i] >= ctx->table_row_capacity) {
            bitmap_release(ctx->bitmap, emb_ids[i]);
            return -1;
        }

        const float *src = (const float *)(table_base +
                            emb_ids[i] * stride);
        memcpy(&results[i * dim], src, dim * sizeof(float));
        bitmap_release(ctx->bitmap, emb_ids[i]);
    }
#endif

    return 0;
}

void sve_streaming_load(const void *src, void *dst, size_t size) {
#ifdef USE_ARM_SVE
    const uint8_t *s = (const uint8_t *)src;
    uint8_t *d = (uint8_t *)dst;
    size_t off = 0;
    while (off < size) {
        svbool_t pg = svwhilelt_b8_u64((uint64_t)off, (uint64_t)size);
        svuint8_t v = svld1_u8(pg, &s[off]);
        svst1_u8(pg, &d[off], v);
        off += svcntb();
    }
#else
    memcpy(dst, src, size);
#endif
}

void sve_streaming_store(const void *src, void *dst, size_t size) {
    sve_streaming_load(src, dst, size);
}

void sve_streaming_load_f32(const void *src, void *dst, size_t size) {
#ifdef USE_ARM_SVE
    if ((((uintptr_t)src | (uintptr_t)dst | size) & (sizeof(float) - 1u)) != 0) {
        sve_streaming_load(src, dst, size);
        return;
    }

    const size_t vl = svcntw();
    size_t rem = size / sizeof(float);
    const float *srcp = (const float *)src;
    float *dstp = (float *)dst;

    svbool_t pg = svptrue_b32();
    while (rem >= vl * 4u) {
        __builtin_prefetch(srcp + vl * 8u, 0, 3);
        svfloat32_t v0 = svld1_f32(pg, srcp);
        svfloat32_t v1 = svld1_f32(pg, srcp + vl);
        svfloat32_t v2 = svld1_f32(pg, srcp + vl * 2u);
        svfloat32_t v3 = svld1_f32(pg, srcp + vl * 3u);
        svst1_f32(pg, dstp, v0);
        svst1_f32(pg, dstp + vl, v1);
        svst1_f32(pg, dstp + vl * 2u, v2);
        svst1_f32(pg, dstp + vl * 3u, v3);
        srcp += vl * 4u;
        dstp += vl * 4u;
        rem -= vl * 4u;
    }

    while (rem >= vl) {
        svfloat32_t v = svld1_f32(pg, srcp);
        svst1_f32(pg, dstp, v);
        srcp += vl;
        dstp += vl;
        rem -= vl;
    }

    if (rem > 0) {
        svbool_t pg = svwhilelt_b32_u64(0UL, (uint64_t)rem);
        svfloat32_t v = svld1_f32(pg, srcp);
        svst1_f32(pg, dstp, v);
    }
#else
    memcpy(dst, src, size);
#endif
}

void sve_gather_scatter_load_f32(const void *src, void *dst, size_t size) {
#ifdef USE_ARM_SVE
    if ((((uintptr_t)src | (uintptr_t)dst | size) & (sizeof(float) - 1u)) != 0) {
        sve_streaming_load(src, dst, size);
        return;
    }

    const size_t vl = svcntw();
    size_t rem = size / sizeof(float);
    const float *srcp = (const float *)src;
    float *dstp = (float *)dst;

    svbool_t pg = svptrue_b32();
    svuint32_t indices = svindex_u32(0, 1);
    while (rem >= vl * 4u) {
        __builtin_prefetch(srcp + vl * 8u, 0, 3);
        svfloat32_t v0 = svld1_gather_u32index_f32(pg, srcp, indices);
        svfloat32_t v1 = svld1_gather_u32index_f32(pg, srcp + vl, indices);
        svfloat32_t v2 = svld1_gather_u32index_f32(pg, srcp + vl * 2u, indices);
        svfloat32_t v3 = svld1_gather_u32index_f32(pg, srcp + vl * 3u, indices);
        svst1_scatter_u32index_f32(pg, dstp, indices, v0);
        svst1_scatter_u32index_f32(pg, dstp + vl, indices, v1);
        svst1_scatter_u32index_f32(pg, dstp + vl * 2u, indices, v2);
        svst1_scatter_u32index_f32(pg, dstp + vl * 3u, indices, v3);
        srcp += vl * 4u;
        dstp += vl * 4u;
        rem -= vl * 4u;
    }

    while (rem >= vl) {
        svfloat32_t v = svld1_gather_u32index_f32(pg, srcp, indices);
        svst1_scatter_u32index_f32(pg, dstp, indices, v);
        srcp += vl;
        dstp += vl;
        rem -= vl;
    }

    if (rem > 0) {
        svbool_t tail = svwhilelt_b32_u64(0UL, (uint64_t)rem);
        svfloat32_t v = svld1_gather_u32index_f32(tail, srcp, indices);
        svst1_scatter_u32index_f32(tail, dstp, indices, v);
    }
#else
    memcpy(dst, src, size);
#endif
}

void sve_gatther_load_f32(const void *src, void *dst, size_t size) {
#ifdef USE_ARM_SVE
    if ((((uintptr_t)src | (uintptr_t)dst | size) & (sizeof(float) - 1u)) != 0) {
        sve_streaming_load(src, dst, size);
        return;
    }

    const size_t vl = svcntw();
    size_t rem = size / sizeof(float);
    const float *srcp = (const float *)src;
    float *dstp = (float *)dst;

    svbool_t pg = svptrue_b32();
    svuint32_t indices = svindex_u32(0, 1);
    while (rem >= vl * 4u) {
        __builtin_prefetch(srcp + vl * 8u, 0, 3);
        svfloat32_t v0 = svld1_gather_u32index_f32(pg, srcp, indices);
        svfloat32_t v1 = svld1_gather_u32index_f32(pg, srcp + vl, indices);
        svfloat32_t v2 = svld1_gather_u32index_f32(pg, srcp + vl * 2u, indices);
        svfloat32_t v3 = svld1_gather_u32index_f32(pg, srcp + vl * 3u, indices);
        svst1_f32(pg, dstp, v0);
        svst1_f32(pg, dstp + vl, v1);
        svst1_f32(pg, dstp + vl * 2u, v2);
        svst1_f32(pg, dstp + vl * 3u, v3);
        srcp += vl * 4u;
        dstp += vl * 4u;
        rem -= vl * 4u;
    }

    while (rem >= vl) {
        svfloat32_t v = svld1_gather_u32index_f32(pg, srcp, indices);
        svst1_f32(pg, dstp, v);
        srcp += vl;
        dstp += vl;
        rem -= vl;
    }

    if (rem > 0) {
        svbool_t tail = svwhilelt_b32_u64(0UL, (uint64_t)rem);
        svfloat32_t v = svld1_gather_u32index_f32(tail, srcp, indices);
        svst1_f32(tail, dstp, v);
    }
#else
    memcpy(dst, src, size);
#endif
}

void sve_column_gather_load_f32(const float *const *src_rows,
                                float *const *dst_rows,
                                size_t num_rows,
                                size_t floats_per_row) {
#ifdef USE_ARM_SVE
    const size_t rows_per_batch = svcntw();

    size_t offset = 0;
    while (offset + 4u <= floats_per_row) {
        for (size_t batch_start = 0; batch_start < num_rows; batch_start += rows_per_batch) {
            size_t batch_rows = num_rows - batch_start;
            if (batch_rows > rows_per_batch) batch_rows = rows_per_batch;
            uint32_t src_bases[rows_per_batch];
            uint32_t dst_bases[rows_per_batch];
            for (size_t i = 0; i < batch_rows; i++) {
                uintptr_t src_addr = (uintptr_t)src_rows[batch_start + i];
                uintptr_t dst_addr = (uintptr_t)dst_rows[batch_start + i];
                if (src_addr > UINT32_MAX || dst_addr > UINT32_MAX) {
                    abort();
                }
                src_bases[i] = (uint32_t)src_addr;
                dst_bases[i] = (uint32_t)dst_addr;
            }
            svbool_t pg = svwhilelt_b32_u64(0, (uint64_t)batch_rows);
            svuint32_t src_bases_vec = svld1_u32(pg, src_bases);
            svuint32_t dst_bases_vec = svld1_u32(pg, dst_bases);
            svfloat32_t v0 = svld1_gather_u32base_index_f32(pg, src_bases_vec, (int64_t)(offset + 0u));
            svfloat32_t v1 = svld1_gather_u32base_index_f32(pg, src_bases_vec, (int64_t)(offset + 1u));
            svfloat32_t v2 = svld1_gather_u32base_index_f32(pg, src_bases_vec, (int64_t)(offset + 2u));
            svfloat32_t v3 = svld1_gather_u32base_index_f32(pg, src_bases_vec, (int64_t)(offset + 3u));
            svst1_scatter_u32base_index_f32(pg, dst_bases_vec, (int64_t)(offset + 0u), v0);
            svst1_scatter_u32base_index_f32(pg, dst_bases_vec, (int64_t)(offset + 1u), v1);
            svst1_scatter_u32base_index_f32(pg, dst_bases_vec, (int64_t)(offset + 2u), v2);
            svst1_scatter_u32base_index_f32(pg, dst_bases_vec, (int64_t)(offset + 3u), v3);
        }
        offset += 4u;
    }

    while (offset < floats_per_row) {
        for (size_t batch_start = 0; batch_start < num_rows; batch_start += rows_per_batch) {
            size_t batch_rows = num_rows - batch_start;
            if (batch_rows > rows_per_batch) batch_rows = rows_per_batch;
            uint32_t src_bases[rows_per_batch];
            uint32_t dst_bases[rows_per_batch];
            for (size_t i = 0; i < batch_rows; i++) {
                uintptr_t src_addr = (uintptr_t)src_rows[batch_start + i];
                uintptr_t dst_addr = (uintptr_t)dst_rows[batch_start + i];
                if (src_addr > UINT32_MAX || dst_addr > UINT32_MAX) {
                    abort();
                }
                src_bases[i] = (uint32_t)src_addr;
                dst_bases[i] = (uint32_t)dst_addr;
            }
            svbool_t pg = svwhilelt_b32_u64(0, (uint64_t)batch_rows);
            svuint32_t src_bases_vec = svld1_u32(pg, src_bases);
            svuint32_t dst_bases_vec = svld1_u32(pg, dst_bases);
            svfloat32_t v = svld1_gather_u32base_index_f32(pg, src_bases_vec, (int64_t)offset);
            svst1_scatter_u32base_index_f32(pg, dst_bases_vec, (int64_t)offset, v);
        }
        offset++;
    }
#else
    const size_t rows_per_batch = SVE_OP_VL;

    size_t offset = 0;
    while (offset + 4u <= floats_per_row) {
        for (size_t batch_start = 0; batch_start < num_rows; batch_start += rows_per_batch) {
            size_t batch_rows = num_rows - batch_start;
            if (batch_rows > rows_per_batch) batch_rows = rows_per_batch;

            for (size_t i = 0; i < batch_rows; i++)
                dst_rows[batch_start + i][offset + 0u] = src_rows[batch_start + i][offset + 0u];
            for (size_t i = 0; i < batch_rows; i++)
                dst_rows[batch_start + i][offset + 1u] = src_rows[batch_start + i][offset + 1u];
            for (size_t i = 0; i < batch_rows; i++)
                dst_rows[batch_start + i][offset + 2u] = src_rows[batch_start + i][offset + 2u];
            for (size_t i = 0; i < batch_rows; i++)
                dst_rows[batch_start + i][offset + 3u] = src_rows[batch_start + i][offset + 3u];
        }
        offset += 4u;
    }

    while (offset < floats_per_row) {
        for (size_t batch_start = 0; batch_start < num_rows; batch_start += rows_per_batch) {
            size_t batch_rows = num_rows - batch_start;
            if (batch_rows > rows_per_batch) batch_rows = rows_per_batch;

            for (size_t i = 0; i < batch_rows; i++)
                dst_rows[batch_start + i][offset] = src_rows[batch_start + i][offset];
        }
        offset++;
    }
#endif
}

void sve_column_gather_load_f32_v2_base(const float *const *src_rows,
                                        float *const *dst_rows,
                                        size_t num_rows,
                                        size_t floats_per_row) {
#if defined(USE_ARM_SVE) && defined(__ARM_FEATURE_SVE2p1)
    const size_t rows_per_batch = svcntd();

    size_t offset = 0;
    while (offset + 4u <= floats_per_row) {
        for (size_t batch_start = 0; batch_start < num_rows; batch_start += rows_per_batch) {
            size_t batch_rows = num_rows - batch_start;
            if (batch_rows > rows_per_batch) batch_rows = rows_per_batch;
            svbool_t pg = svwhilelt_b64_u64(0, (uint64_t)batch_rows);
            uint64_t src_bases[rows_per_batch];
            uint64_t dst_bases[rows_per_batch];
            for (size_t i = 0; i < batch_rows; i++) {
                src_bases[i] = (uint64_t)(uintptr_t)src_rows[batch_start + i];
                dst_bases[i] = (uint64_t)(uintptr_t)dst_rows[batch_start + i];
            }
            svuint64_t src_bases_vec = svld1_u64(pg, src_bases);
            svuint64_t dst_bases_vec = svld1_u64(pg, dst_bases);
            svfloat32_t v0 = svld1q_gather_u64base_index_f32(pg, src_bases_vec, (int64_t)(offset + 0u));
            svfloat32_t v1 = svld1q_gather_u64base_index_f32(pg, src_bases_vec, (int64_t)(offset + 1u));
            svfloat32_t v2 = svld1q_gather_u64base_index_f32(pg, src_bases_vec, (int64_t)(offset + 2u));
            svfloat32_t v3 = svld1q_gather_u64base_index_f32(pg, src_bases_vec, (int64_t)(offset + 3u));
            svst1q_scatter_u64base_index_f32(pg, dst_bases_vec, (int64_t)(offset + 0u), v0);
            svst1q_scatter_u64base_index_f32(pg, dst_bases_vec, (int64_t)(offset + 1u), v1);
            svst1q_scatter_u64base_index_f32(pg, dst_bases_vec, (int64_t)(offset + 2u), v2);
            svst1q_scatter_u64base_index_f32(pg, dst_bases_vec, (int64_t)(offset + 3u), v3);
        }
        offset += 4u;
    }

    while (offset < floats_per_row) {
        for (size_t batch_start = 0; batch_start < num_rows; batch_start += rows_per_batch) {
            size_t batch_rows = num_rows - batch_start;
            if (batch_rows > rows_per_batch) batch_rows = rows_per_batch;
            svbool_t pg = svwhilelt_b64_u64(0, (uint64_t)batch_rows);
            uint64_t src_bases[rows_per_batch];
            uint64_t dst_bases[rows_per_batch];
            for (size_t i = 0; i < batch_rows; i++) {
                src_bases[i] = (uint64_t)(uintptr_t)src_rows[batch_start + i];
                dst_bases[i] = (uint64_t)(uintptr_t)dst_rows[batch_start + i];
            }
            svuint64_t src_bases_vec = svld1_u64(pg, src_bases);
            svuint64_t dst_bases_vec = svld1_u64(pg, dst_bases);
            svfloat32_t v = svld1q_gather_u64base_index_f32(pg, src_bases_vec, (int64_t)offset);
            svst1q_scatter_u64base_index_f32(pg, dst_bases_vec, (int64_t)offset, v);
        }
        offset++;
    }
#else
    const size_t rows_per_batch = SVE_OP_VL;

    size_t offset = 0;
    while (offset + 4u <= floats_per_row) {
        for (size_t batch_start = 0; batch_start < num_rows; batch_start += rows_per_batch) {
            size_t batch_rows = num_rows - batch_start;
            if (batch_rows > rows_per_batch) batch_rows = rows_per_batch;

            for (size_t i = 0; i < batch_rows; i++)
                dst_rows[batch_start + i][offset + 0u] = src_rows[batch_start + i][offset + 0u];
            for (size_t i = 0; i < batch_rows; i++)
                dst_rows[batch_start + i][offset + 1u] = src_rows[batch_start + i][offset + 1u];
            for (size_t i = 0; i < batch_rows; i++)
                dst_rows[batch_start + i][offset + 2u] = src_rows[batch_start + i][offset + 2u];
            for (size_t i = 0; i < batch_rows; i++)
                dst_rows[batch_start + i][offset + 3u] = src_rows[batch_start + i][offset + 3u];
        }
        offset += 4u;
    }

    while (offset < floats_per_row) {
        for (size_t batch_start = 0; batch_start < num_rows; batch_start += rows_per_batch) {
            size_t batch_rows = num_rows - batch_start;
            if (batch_rows > rows_per_batch) batch_rows = rows_per_batch;

            for (size_t i = 0; i < batch_rows; i++)
                dst_rows[batch_start + i][offset] = src_rows[batch_start + i][offset];
        }
        offset++;
    }
#endif
}
