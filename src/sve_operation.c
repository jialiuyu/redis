#include "sve_operation.h"
#include "macro.h"
#include "zmalloc.h"

#include <stdlib.h>
#include <stdio.h>

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
}

int sve_serial_contiguous_read(sve_gather_ctx_t *ctx,
                               uint64_t *emb_ids,
                               size_t num_ids,
                               float *results) {
    const size_t dim = ctx ? ctx->vector_dim : 0;
    const size_t row_bytes = dim * sizeof(float);

    RETURN_IF(!ctx || !ctx->ubas || !ctx->bitmap || !ctx->stats ||
              !ctx->ubas->mapped_addr || !emb_ids || !results || num_ids == 0 || dim == 0, -1);

    for (size_t i = 0; i < num_ids; i++) {
        if (bitmap_try_acquire(ctx->bitmap, emb_ids[i]) != 0) {
            atomic_fetch_add_explicit(&ctx->stats->lock_failure, 1, memory_order_relaxed);
            memset(&results[i * dim], 0, row_bytes);
            continue;
        }

        atomic_fetch_add_explicit(&ctx->stats->lock_success, 1, memory_order_relaxed);
        if (emb_ids[i] >= ctx->table_row_capacity) {
            memset(&results[i * dim], 0, row_bytes);
            bitmap_release(ctx->bitmap, emb_ids[i]);
            return -1;
        }

        const float *src = (const float *)((const char *)ctx->ubas->mapped_addr +
                                           emb_ids[i] * ctx->vector_stride_bytes);
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
        bitmap_release(ctx->bitmap, emb_ids[i]);
    }

    return 0;
}

/* ============================================================
 * Streaming Load / Store（非临时内存访问）
 * ============================================================ */

void sve_streaming_load(const void *src, void *dst, size_t size) {
#ifdef USE_ARM_SVE
    const uint8_t *s = (const uint8_t *)src;
    uint8_t *d = (uint8_t *)dst;
    size_t off = 0;
    svbool_t pg = svptrue_b8();
    while (off < size) {
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
