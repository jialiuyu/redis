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

#ifndef SVE_OP_VECTOR_BITS
#define SVE_OP_VECTOR_BITS 256
#endif
#define SVE_OP_VL (SVE_OP_VECTOR_BITS / 32)  /* 8 floats per vector */
#define BITMAP_BITS_PER_WORD 64
#define BITMAP_WORD_SHIFT 6
#define BITMAP_WORD_MASK ((1ULL << BITMAP_WORD_SHIFT) - 1)

_Static_assert(BITMAP_BITS_PER_WORD == 64,
               "bitmap bit math assumes 64-bit words");

typedef struct ub_address_space ub_address_space_t; // Forward declaration

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
    _Alignas(64) atomic_uint_fast64_t word;
} bitmap_atomic_word_t;

/* Bitmap 并发控制 */
typedef struct {
    bitmap_atomic_word_t *bits;
    size_t num_words;
} state_bitmap_t;

typedef struct {
    atomic_uint_fast64_t lock_success;
    atomic_uint_fast64_t lock_failure;
} sve_operation_stats_t;

typedef struct {
    ub_address_space_t *ubas;
    state_bitmap_t *bitmap;
    size_t vector_dim;
    size_t vector_stride_bytes;
    uint64_t table_row_capacity;
    sve_operation_stats_t *stats;
} sve_gather_ctx_t;

 /* Embedding 数据结构 */
typedef struct {
    _Alignas(64) float data[SVE_EMBEDDING_DIM];
} embedding_entry_t;

/* ---- Bitmap 操作 ---- */
int  bitmap_init(state_bitmap_t *bmp, size_t num_bits);
void bitmap_destroy(state_bitmap_t *bmp);
int  bitmap_try_acquire(state_bitmap_t *bmp, uint64_t bit_index);
void bitmap_release(state_bitmap_t *bmp, uint64_t bit_index);

void sve_gather_ctx_init(sve_gather_ctx_t *ctx,
                         ub_address_space_t *ubas,
                         state_bitmap_t *bitmap,
                         size_t vector_dim,
                         size_t vector_stride_bytes,
                         uint64_t table_row_capacity,
                         sve_operation_stats_t *stats);

/* 逐 embedding 串行连续加载（baseline 对照）*/
int sve_serial_contiguous_read(sve_ub_mem_t *mem,
                          state_bitmap_t *bmp,
                          uint64_t *emb_ids,
                          size_t num_ids,
                          float *results,
                          sve_operation_stats_t *stats);

/* 非临时内存拷贝（SVE streaming load / 标量 memcpy）*/
void sve_streaming_load(const void *src, void *dst, size_t size);
void sve_streaming_store(const void *src, void *dst, size_t size);

#endif /* __SVE_OPERATION_H */
