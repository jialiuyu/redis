/*
 * SVE Scatter/Gather — 独立的跨 embedding SIMD 并行读写模块
 *
 * 不依赖 server.h，可被 benchmark 直接编译。
 * 仅依赖: sve_config.h, stdint.h, stddef.h, stdatomic.h
 *
 * 核心思路：将处理维度从"逐 embedding"翻转为"跨 embedding"
 * 每条 gather/scatter 指令同时处理 SVE_VL(=8) 个 embedding 的同一维度
 */
#ifndef __SVE_OPERATION_H
#define __SVE_OPERATION_H

#include "sve_config.h"
#include <stdint.h>
#include <stddef.h>
#include <stdatomic.h>
#include <string.h>
#include <math.h>

/* ---- 配置常量 ---- */
#ifndef SVE_EMBEDDING_DIM
#define SVE_EMBEDDING_DIM 300
#endif

#define SVE_OP_VECTOR_BITS 256
#define SVE_OP_VL (SVE_OP_VECTOR_BITS / 32)  /* 8 floats per vector */
#define BITMAP_BITS_PER_WORD 64

/* UB 内存空间 */
typedef struct sve_ub_mem {
    void    *base_addr;                 /* 基地址（映射后的虚拟地址）*/
    uint64_t physical_base;             /* 物理基地址 */
    size_t   size;                      /* 大小 */
    uint32_t token_id;                  /* 访问令牌 */
    int      numa_node;                 /* NUMA 节点 */
} sve_ub_mem_t;

/* 64 字节对齐的原子字（防伪共享）*/
typedef struct {
    atomic_uint_fast64_t word;
} __attribute__((aligned(64))) bitmap_atomic_word_t;

/* Bitmap 并发控制 */
typedef struct {
    bitmap_atomic_word_t *bits;
    size_t num_words;
} state_bitmap_t;

/* 性能计数器 */
typedef struct {
    atomic_uint_fast64_t gather_ops;
    atomic_uint_fast64_t scatter_ops;
    atomic_uint_fast64_t gather_elements;
    atomic_uint_fast64_t scatter_elements;
    atomic_uint_fast64_t locked_skips;
} sve_counters_t;

 /* Embedding 数据结构 */
typedef struct {
    float data[SVE_EMBEDDING_DIM];
} __attribute__((aligned(64))) embedding_entry_t;

/* ---- Bitmap 操作 ---- */

int  bitmap_init(state_bitmap_t *bmp, size_t num_bits);
void bitmap_destroy(state_bitmap_t *bmp);
int  state_bitmap_try_acquire(state_bitmap_t *bmp, uint64_t bit_index);
void bitmap_release(state_bitmap_t *bmp, uint64_t bit_index);

/* ---- 核心 API ---- */

/* 偏移向量计算 */
void sve_compute_offsets(sve_ub_mem_t *mem,
                        const uint64_t *emb_ids,
                        size_t num_ids,
                        size_t dim_index,
                        uint64_t *out_offsets,
                        uint8_t *out_valid);

/* 跨 embedding gather 读取 */
int sve_gather_read(sve_ub_mem_t *mem,
                   state_bitmap_t *bmp,
                   sve_counters_t *stats,
                   uint64_t *emb_ids,
                   size_t num_ids,
                   float *results,
                   uint8_t *valid_mask);

/* 跨 embedding scatter 写入 */
int sve_scatter_write(sve_ub_mem_t *mem,
                     state_bitmap_t *bmp,
                     sve_counters_t *stats,
                     uint64_t *emb_ids,
                     size_t num_ids,
                     const float *src_data,
                     uint8_t *valid_mask);

/* 融合 gather + 余弦相似度 */
int sve_fused_cosine(sve_ub_mem_t *mem,
                    const float *query,
                    size_t dim,
                    const uint64_t *emb_ids,
                    size_t num_ids,
                    float *similarities);

/* 融合 gather + GEMM */
int sve_fused_gemm(sve_ub_mem_t *mem,
                  const uint64_t *emb_ids,
                  size_t num_rows,
                  const float *W,
                  size_t emb_dim,
                  size_t out_dim,
                  float *output);

/* 统计报告 */
void sve_counters_init(sve_counters_t *c);

/* ---- 旧接口的独立版本（逐 embedding 串行读取）---- */

/* 逐 embedding 串行连续加载（baseline 对照）*/
int sve_serial_contiguous_read(sve_ub_mem_t *mem,
                          state_bitmap_t *bmp,
                          sve_counters_t *stats,
                          uint64_t *emb_ids,
                          size_t num_ids,
                          float *results,
                          uint8_t *valid_mask);

/* 非临时内存拷贝（SVE streaming load / 标量 memcpy）*/
void sve_streaming_load(const void *src, void *dst, size_t size);
void sve_streaming_store(const void *src, void *dst, size_t size);

#endif /* __SVE_OPERATION_H */
