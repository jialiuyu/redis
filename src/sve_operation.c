/*
 * SVE Scatter/Gather — 独立实现
 * 不依赖 server.h，可被 benchmark 直接编译链接
 */
#include "sve_operation.h"
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

int state_bitmap_try_acquire(state_bitmap_t *bmp, uint64_t bit_index) {
    if (!bmp || !bmp->bits) return -1;
    uint64_t wi = bit_index / BITMAP_BITS_PER_WORD;
    uint64_t bo = bit_index % BITMAP_BITS_PER_WORD;
    if (wi >= bmp->num_words) return -1;
    const uint64_t mask = 1ULL << bo;
    atomic_uint_fast64_t *w = &bmp->bits[wi].word;
    uint64_t old = atomic_load_explicit(w, memory_order_relaxed);
    do {
        if ((old & mask) != 0) return -1;
        if (atomic_compare_exchange_weak_explicit(w, &old, old | mask,
                memory_order_acquire, memory_order_relaxed))
            return 0;
#if defined(__aarch64__)
        __asm__ volatile("yield" ::: "memory");
#elif defined(__x86_64__)
        __asm__ volatile("pause" ::: "memory");
#endif
    } while (1);
}

void bitmap_release(state_bitmap_t *bmp, uint64_t bit_index) {
    if (!bmp || !bmp->bits) return;
    uint64_t wi = bit_index / BITMAP_BITS_PER_WORD;
    uint64_t bo = bit_index % BITMAP_BITS_PER_WORD;
    if (wi >= bmp->num_words) return;
    atomic_fetch_and_explicit(&bmp->bits[wi].word,
                              ~(1ULL << bo), memory_order_release);
}

void sve_counters_init(sve_counters_t *c) {
    atomic_init(&c->gather_ops, 0);
    atomic_init(&c->scatter_ops, 0);
    atomic_init(&c->gather_elements, 0);
    atomic_init(&c->scatter_elements, 0);
    atomic_init(&c->locked_skips, 0);
}

/* ============================================================
 * 内部工具
 * ============================================================ */



static inline void *sve_get_embedding(sve_ub_mem_t *mem, uint64_t emb_id) {
    if (!mem || !mem->base_addr) return NULL;
    size_t off = emb_id * sizeof(embedding_entry_t);
    if (off + sizeof(embedding_entry_t) > mem->size) return NULL;
    return (uint8_t *)mem->base_addr + off;
}

/* ============================================================
 * 偏移向量计算
 * ============================================================ */

void sve_compute_offsets(sve_ub_mem_t *mem,
                        const uint64_t *emb_ids,
                        size_t num_ids,
                        size_t dim_index,
                        uint64_t *out_offsets,
                        uint8_t *out_valid)
{
    const size_t entry_size = sizeof(embedding_entry_t);
    const size_t data_off = offsetof(embedding_entry_t, data);
    const size_t dim_byte_off = data_off + dim_index * sizeof(float);
    size_t mem_size = mem ? mem->size : 0;

    if (!mem || !emb_ids || !out_offsets || !out_valid || num_ids == 0) {
        for (size_t i = 0; i < num_ids; i++) { out_offsets[i] = 0; out_valid[i] = 0; }
        return;
    }

#ifdef USE_ARM_SVE
    {
        svbool_t pg = svwhilelt_b64_u64(0UL, (uint64_t)num_ids);
        svuint64_t ids = svld1_u64(pg, emb_ids);
        svuint64_t off = svadd_u64_m(pg, svmul_u64_m(pg, ids, svdup_u64(entry_size)),
                                     svdup_u64(dim_byte_off));
        svst1_u64(pg, out_offsets, off);
        for (size_t i = 0; i < num_ids; i++)
            out_valid[i] = (out_offsets[i] + sizeof(float) <= mem_size) ? 1 : 0;
    }
#else
    for (size_t i = 0; i < num_ids; i++) {
        uint64_t o = emb_ids[i] * entry_size + dim_byte_off;
        out_offsets[i] = o;
        out_valid[i] = (o + sizeof(float) <= mem_size) ? 1 : 0;
    }
#endif
}

/* ============================================================
 * Gather 读取
 * ============================================================ */

 /* 每块操作上下文 */
typedef struct {
    uint64_t emb_ids[SVE_OP_VL];
    uint8_t  acquired[SVE_OP_VL];
    size_t   num_active;
    size_t   block_size;
} sve_block_ctx_t;

int sve_gather_read(sve_ub_mem_t *mem,
                   state_bitmap_t *bmp,
                   sve_counters_t *stats,
                   uint64_t *emb_ids,
                   size_t num_ids,
                   float *results,
                   uint8_t *valid_mask)
{
    if (!mem || !mem->base_addr || !emb_ids || !results || !valid_mask || num_ids == 0)
        return -1;

    const size_t dim = SVE_EMBEDDING_DIM;
    memset(valid_mask, 0, num_ids);

    for (size_t blk = 0; blk < num_ids; blk += SVE_OP_VL) {
        size_t bs = num_ids - blk;
        if (bs > SVE_OP_VL) bs = SVE_OP_VL;

        /* Batch bitmap acquire */
        sve_block_ctx_t bctx;
        bctx.block_size = bs;
        bctx.num_active = 0;
        for (size_t i = 0; i < bs; i++) {
            bctx.emb_ids[i] = emb_ids[blk + i];
            if (state_bitmap_try_acquire(bmp, bctx.emb_ids[i]) == 0) {
                bctx.acquired[i] = 1;
                bctx.num_active++;
            } else {
                bctx.acquired[i] = 0;
                memset(&results[(blk + i) * dim], 0, dim * sizeof(float));
                if (stats) atomic_fetch_add(&stats->locked_skips, 1);
            }
        }
        if (bctx.num_active == 0) continue;

        /* Prefetch next block */
        if (blk + SVE_OP_VL < num_ids) {
            for (size_t i = 0; i < 4 && blk + SVE_OP_VL + i < num_ids; i++) {
                void *a = sve_get_embedding(mem, emb_ids[blk + SVE_OP_VL + i]);
                if (a) __builtin_prefetch(a, 0, 1);
            }
        }

#ifdef USE_ARM_SVE
        {
            svbool_t pg_base = svwhilelt_b32_u64(0UL, (uint64_t)bs);
            const size_t entry_size = sizeof(sve_embedding_entry_t);
            const size_t data_off = offsetof(sve_embedding_entry_t, data);

            uint64_t base_off[SVE_OP_VL];
            uint8_t lane_ok[SVE_OP_VL];
            for (size_t i = 0; i < bs; i++) {
                if (bctx.acquired[i]) {
                    base_off[i] = bctx.emb_ids[i] * entry_size + data_off;
                    lane_ok[i] = (base_off[i] + dim * sizeof(float) <= mem->size) ? 1 : 0;
                } else { base_off[i] = 0; lane_ok[i] = 0; }
            }
            for (size_t i = bs; i < SVE_OP_VL; i++) { base_off[i] = 0; lane_ok[i] = 0; }

            for (size_t d = 0; d < dim; d++) {
                uint64_t off_d[SVE_OP_VL];
                for (size_t i = 0; i < SVE_OP_VL; i++)
                    off_d[i] = base_off[i] + d * sizeof(float);

                svbool_t pg64 = svwhilelt_b64_u64(0UL, (uint64_t)bs);
                svuint64_t ov = svld1_u64(pg64, off_d);
                svfloat32_t gathered = svld1_gather_u64offset_f32(
                    pg_base, (const float *)mem->base_addr, ov);

                float tmp[SVE_OP_VL];
                svst1_f32(pg_base, tmp, gathered);
                for (size_t i = 0; i < bs; i++)
                    if (lane_ok[i]) results[(blk + i) * dim + d] = tmp[i];

                if (stats) {
                    atomic_fetch_add(&stats->gather_ops, 1);
                    atomic_fetch_add(&stats->gather_elements, bctx.num_active);
                }
            }
        }
#else
        for (size_t i = 0; i < bs; i++) {
            if (!bctx.acquired[i]) continue;
            embedding_entry_t *emb = sve_get_embedding(mem, bctx.emb_ids[i]);
            if (emb) memcpy(&results[(blk + i) * dim], emb->data, dim * sizeof(float));
        }
#endif

        for (size_t i = 0; i < bs; i++) {
            if (bctx.acquired[i]) {
                valid_mask[blk + i] = 1;
                bitmap_release(bmp, bctx.emb_ids[i]);
            }
        }
    }
    return 0;
}

/* ============================================================
 * Scatter 写入
 * ============================================================ */

int sve_scatter_write(sve_ub_mem_t *mem,
                     state_bitmap_t *bmp,
                     sve_counters_t *stats,
                     uint64_t *emb_ids,
                     size_t num_ids,
                     const float *src_data,
                     uint8_t *valid_mask)
{
    if (!mem || !mem->base_addr || !emb_ids || !src_data || !valid_mask || num_ids == 0)
        return -1;

    const size_t dim = SVE_EMBEDDING_DIM;
    memset(valid_mask, 0, num_ids);

    for (size_t blk = 0; blk < num_ids; blk += SVE_OP_VL) {
        size_t bs = num_ids - blk;
        if (bs > SVE_OP_VL) bs = SVE_OP_VL;

        sve_block_ctx_t bctx;
        bctx.block_size = bs;
        bctx.num_active = 0;
        for (size_t i = 0; i < bs; i++) {
            bctx.emb_ids[i] = emb_ids[blk + i];
            if (state_bitmap_try_acquire(bmp, bctx.emb_ids[i]) == 0) {
                bctx.acquired[i] = 1;
                bctx.num_active++;
            } else {
                bctx.acquired[i] = 0;
                if (stats) atomic_fetch_add(&stats->locked_skips, 1);
            }
        }
        if (bctx.num_active == 0) continue;

#ifdef USE_ARM_SVE
        {
            svbool_t pg_base = svwhilelt_b32_u64(0UL, (uint64_t)bs);
            const size_t entry_size = sizeof(sve_embedding_entry_t);
            const size_t data_off = offsetof(sve_embedding_entry_t, data);

            uint64_t base_off[SVE_OP_VL];
            uint8_t lane_ok[SVE_OP_VL];
            for (size_t i = 0; i < bs; i++) {
                if (bctx.acquired[i]) {
                    base_off[i] = bctx.emb_ids[i] * entry_size + data_off;
                    lane_ok[i] = (base_off[i] + dim * sizeof(float) <= mem->size) ? 1 : 0;
                } else { base_off[i] = 0; lane_ok[i] = 0; }
            }
            for (size_t i = bs; i < SVE_OP_VL; i++) { base_off[i] = 0; lane_ok[i] = 0; }

            for (size_t d = 0; d < dim; d++) {
                float src_vals[SVE_OP_VL];
                for (size_t i = 0; i < bs; i++)
                    src_vals[i] = lane_ok[i] ? src_data[(blk + i) * dim + d] : 0.0f;
                for (size_t i = bs; i < SVE_OP_VL; i++) src_vals[i] = 0.0f;

                svfloat32_t dv = svld1_f32(pg_base, src_vals);
                uint64_t off_d[SVE_OP_VL];
                for (size_t i = 0; i < SVE_OP_VL; i++)
                    off_d[i] = base_off[i] + d * sizeof(float);

                svbool_t pg64 = svwhilelt_b64_u64(0UL, (uint64_t)bs);
                svuint64_t ov = svld1_u64(pg64, off_d);
                svst1_scatter_u64offset_f32(pg_base, (float *)mem->base_addr, ov, dv);

                if (stats) {
                    atomic_fetch_add(&stats->scatter_ops, 1);
                    atomic_fetch_add(&stats->scatter_elements, bctx.num_active);
                }
            }
        }
#else
        for (size_t i = 0; i < bs; i++) {
            if (!bctx.acquired[i]) continue;
            embedding_entry_t *emb = sve_get_embedding(mem, bctx.emb_ids[i]);
            if (emb) memcpy(emb->data, &src_data[(blk + i) * dim], dim * sizeof(float));
        }
#endif

        for (size_t i = 0; i < bs; i++) {
            if (bctx.acquired[i]) {
                valid_mask[blk + i] = 1;
                bitmap_release(bmp, bctx.emb_ids[i]);
            }
        }
    }
    return 0;
}

/* ============================================================
 * 融合 Gather + 余弦相似度
 * ============================================================ */

int sve_fused_cosine(sve_ub_mem_t *mem,
                    const float *query,
                    size_t dim,
                    const uint64_t *emb_ids,
                    size_t num_ids,
                    float *similarities)
{
    if (!mem || !mem->base_addr || !query || !emb_ids || !similarities || num_ids == 0)
        return -1;
    if (dim != SVE_EMBEDDING_DIM) return -1;

    float q_norm_sq = 0.0f;
    for (size_t d = 0; d < dim; d++) q_norm_sq += query[d] * query[d];
    float q_norm = sqrtf(q_norm_sq);

    const size_t entry_size = sizeof(embedding_entry_t);
    const size_t data_off = offsetof(embedding_entry_t, data);

    for (size_t blk = 0; blk < num_ids; blk += SVE_OP_VL) {
        size_t bs = num_ids - blk;
        if (bs > SVE_OP_VL) bs = SVE_OP_VL;

        uint64_t base_off[SVE_OP_VL];
        uint8_t lane_ok[SVE_OP_VL];
        for (size_t i = 0; i < bs; i++) {
            base_off[i] = emb_ids[blk + i] * entry_size + data_off;
            lane_ok[i] = (base_off[i] + dim * sizeof(float) <= mem->size) ? 1 : 0;
        }
        for (size_t i = bs; i < SVE_OP_VL; i++) { base_off[i] = 0; lane_ok[i] = 0; }

#ifdef USE_ARM_SVE
        {
            svbool_t pg = svwhilelt_b32_u64(0UL, (uint64_t)bs);
            svfloat32_t dot_acc = svdup_f32(0.0f);
            svfloat32_t norm_acc = svdup_f32(0.0f);

            for (size_t d = 0; d < dim; d++) {
                uint64_t off_d[SVE_OP_VL];
                for (size_t i = 0; i < SVE_OP_VL; i++)
                    off_d[i] = base_off[i] + d * sizeof(float);
                svbool_t pg64 = svwhilelt_b64_u64(0UL, (uint64_t)bs);
                svuint64_t ov = svld1_u64(pg64, off_d);
                svfloat32_t ev = svld1_gather_u64offset_f32(pg, (const float *)mem->base_addr, ov);
                svfloat32_t qv = svdup_f32(query[d]);
                dot_acc = svmla_f32_m(pg, dot_acc, ev, qv);
                norm_acc = svmla_f32_m(pg, norm_acc, ev, ev);
            }

            float dot_arr[SVE_OP_VL], norm_arr[SVE_OP_VL];
            svst1_f32(pg, dot_arr, dot_acc);
            svst1_f32(pg, norm_arr, norm_acc);
            for (size_t i = 0; i < bs; i++) {
                if (lane_ok[i]) {
                    float dn = q_norm * sqrtf(norm_arr[i]);
                    similarities[blk + i] = (dn > 1e-12f) ? dot_arr[i] / dn : 0.0f;
                } else {
                    similarities[blk + i] = 0.0f;
                }
            }
        }
#else
        for (size_t i = 0; i < bs; i++) {
            if (!lane_ok[i]) { similarities[blk + i] = 0.0f; continue; }
            embedding_entry_t *emb = sve_get_embedding(mem, emb_ids[blk + i]);
            if (!emb) { similarities[blk + i] = 0.0f; continue; }
            float dot = 0.0f, en = 0.0f;
            for (size_t d = 0; d < dim; d++) {
                dot += emb->data[d] * query[d];
                en += emb->data[d] * emb->data[d];
            }
            float dn = q_norm * sqrtf(en);
            similarities[blk + i] = (dn > 1e-12f) ? dot / dn : 0.0f;
        }
#endif
    }
    return 0;
}

/* ============================================================
 * 融合 Gather + GEMM
 * ============================================================ */

int sve_fused_gemm(sve_ub_mem_t *mem,
                  const uint64_t *emb_ids,
                  size_t num_rows,
                  const float *W,
                  size_t emb_dim,
                  size_t out_dim,
                  float *output)
{
    if (!mem || !mem->base_addr || !emb_ids || !W || !output ||
        num_rows == 0 || out_dim == 0) return -1;
    if (emb_dim != SVE_EMBEDDING_DIM) return -1;

    memset(output, 0, num_rows * out_dim * sizeof(float));

    const size_t entry_size = sizeof(embedding_entry_t);
    const size_t data_off = offsetof(embedding_entry_t, data);

    if (num_rows > SVE_OP_VL) {
        /* Fallback: gather to temp buffer then scalar GEMM */
        for (size_t i = 0; i < num_rows; i++) {
            embedding_entry_t *emb = sve_get_embedding(mem, emb_ids[i]);
            if (!emb) continue;
            for (size_t d = 0; d < emb_dim; d++) {
                float a = emb->data[d];
                for (size_t j = 0; j < out_dim; j++)
                    output[i * out_dim + j] += a * W[d * out_dim + j];
            }
        }
        return 0;
    }

    uint64_t base_off[SVE_OP_VL];
    for (size_t i = 0; i < num_rows; i++)
        base_off[i] = emb_ids[i] * entry_size + data_off;
    for (size_t i = num_rows; i < SVE_OP_VL; i++) base_off[i] = 0;

#ifdef USE_ARM_SVE
    {
        svbool_t pg = svwhilelt_b32_u64(0UL, (uint64_t)num_rows);
        for (size_t d = 0; d < emb_dim; d++) {
            uint64_t off_d[SVE_OP_VL];
            for (size_t i = 0; i < SVE_OP_VL; i++)
                off_d[i] = base_off[i] + d * sizeof(float);
            svbool_t pg64 = svwhilelt_b64_u64(0UL, (uint64_t)num_rows);
            svuint64_t ov = svld1_u64(pg64, off_d);
            svfloat32_t emb_d = svld1_gather_u64offset_f32(pg, (const float *)mem->base_addr, ov);

            float emb_arr[SVE_OP_VL];
            svst1_f32(pg, emb_arr, emb_d);
            for (size_t j = 0; j < out_dim; j++) {
                float w = W[d * out_dim + j];
                for (size_t i = 0; i < num_rows; i++)
                    output[i * out_dim + j] += emb_arr[i] * w;
            }
        }
    }
#else
    for (size_t i = 0; i < num_rows; i++) {
        embedding_entry_t *emb = sve_get_embedding(mem, emb_ids[i]);
        if (!emb) continue;
        for (size_t d = 0; d < emb_dim; d++) {
            float a = emb->data[d];
            for (size_t j = 0; j < out_dim; j++)
                output[i * out_dim + j] += a * W[d * out_dim + j];
        }
    }
#endif
    return 0;
}

/* ============================================================
 * 逐 embedding 串行 Gather Load（旧 baseline 实现）
 * ============================================================ */

int sve_serial_gather_read(sve_ub_mem_t *mem,
                          state_bitmap_t *bmp,
                          sve_counters_t *stats,
                          uint64_t *emb_ids,
                          size_t num_ids,
                          float *results,
                          uint8_t *valid_mask)
{
    if (!mem || !mem->base_addr || !emb_ids || !results || !valid_mask || num_ids == 0)
        return -1;

    const size_t dim = SVE_EMBEDDING_DIM;

    for (size_t i = 0; i < num_ids; i++) {
        if (state_bitmap_try_acquire(bmp, emb_ids[i]) != 0) {
            valid_mask[i] = 0;
            memset(&results[i * dim], 0, dim * sizeof(float));
            if (stats) atomic_fetch_add(&stats->locked_skips, 1);
            continue;
        }

        embedding_entry_t *emb = sve_get_embedding(mem, emb_ids[i]);
        if (!emb) {
            valid_mask[i] = 0;
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
        valid_mask[i] = 1;
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
