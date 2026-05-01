#include "sve_operation.h"
#include "macro.h"

#include <stdlib.h>
#include <stdio.h>

/* ============================================================
 * Bitmap 操作
 * ============================================================ */

int bitmap_init(state_bitmap_t *bmp, size_t num_bits) {
    bmp->num_words = (num_bits + BITMAP_BITS_PER_WORD - 1) / BITMAP_BITS_PER_WORD;
    if (bmp->num_words < 1) bmp->num_words = 1;
    bmp->bits = (bitmap_atomic_word_t *)calloc(bmp->num_words, sizeof(bitmap_atomic_word_t));
    if (!bmp->bits) return -1;
    for (size_t i = 0; i < bmp->num_words; i++)
        atomic_init(&bmp->bits[i].word, 0);
    return 0;
}

void bitmap_destroy(state_bitmap_t *bmp) {
    if (bmp && bmp->bits) { free(bmp->bits); bmp->bits = NULL; }
}

int bitmap_try_acquire(state_bitmap_t *bmp, uint64_t bit_index) {
    if (!bmp || !bmp->bits) return -1;
    uint64_t wi = bit_index / BITMAP_BITS_PER_WORD;
    uint64_t bo = bit_index % BITMAP_BITS_PER_WORD;
    if (wi >= bmp->num_words) return -1;
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
    if (!bmp || !bmp->bits) return;
    uint64_t wi = bit_index / BITMAP_BITS_PER_WORD;
    uint64_t bo = bit_index % BITMAP_BITS_PER_WORD;
    if (wi >= bmp->num_words) return;
    atomic_fetch_and_explicit(&bmp->bits[wi].word,
                              ~(1ULL << bo), memory_order_release);
}

static inline void *get_embedding_addr(sve_ub_mem_t *mem, uint64_t emb_id) {
    if (!mem || !mem->base_addr) return NULL;
    size_t off = emb_id * sizeof(embedding_entry_t);
    if (off + sizeof(embedding_entry_t) > mem->size) return NULL;
    return (uint8_t *)mem->base_addr + off;
}

/* ============================================================
 * 逐 embedding 串行连续加载
 * ============================================================ */

int sve_serial_contiguous_read(sve_ub_mem_t *mem,
                          state_bitmap_t *bmp,
                          uint64_t *emb_ids,
                          size_t num_ids,
                          float *results,
                          sve_operation_stats_t *stats)
{
    RETURN_IF(!mem || !mem->base_addr || !bmp || !emb_ids || !results || num_ids == 0, -1);
    const size_t dim = SVE_EMBEDDING_DIM;

    for (size_t i = 0; i < num_ids; i++) {
        if (bitmap_try_acquire(bmp, emb_ids[i]) != 0) {
            atomic_fetch_add_explicit(&stats->lock_failure, 1, memory_order_relaxed);
            memset(&results[i * dim], 0, dim * sizeof(float));
            continue;
        }

        atomic_fetch_add_explicit(&stats->lock_success, 1, memory_order_relaxed);
        embedding_entry_t *emb = get_embedding_addr(mem, emb_ids[i]);
        if (!emb) {
            memset(&results[i * dim], 0, dim * sizeof(float));
            bitmap_release(bmp, emb_ids[i]);
            continue;
        }

#ifdef USE_ARM_SVE
        {
            size_t off = 0;
            while (off < dim) {
                svbool_t pg = svwhilelt_b32_u64(off, (uint64_t)dim);
                svfloat32_t v = svld1_f32(pg, &emb->data[off]);
                svst1_f32(pg, &results[i * dim + off], v);
                off += svcntw();
            }
        }
#else
        memcpy(&results[i * dim], emb->data, dim * sizeof(float));
#endif

        bitmap_release(bmp, emb_ids[i]);
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
