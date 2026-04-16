/*
 * Three-Layer Cache v6 — UB Memory + SVE2 Fused Compute
 *
 * v5 base: UB shared memory backend + consistent hashing.
 * v6 adds: SVE2 fused gather-load + compute in one pass:
 *   - Embedding cosine similarity (fused gather + dot-product)
 *   - GEMM matrix multiply (fused gather + outer-product)
 *   - Batch gather load with SVE2 streaming prefetch
 *
 * Architecture:
 *   HOT  (16B index)  → UB.mem region 0 (local)
 *   WARM (1200B value) → UB.mem region 1 (consistent-hash)
 *   COLD (append-only) → UB.mem region 2 (sequential)
 *   EMB  (float[dim])  → UB.mem region 3 (embedding table for SVE2 compute)
 */
#ifndef __THREE_LAYER_CACHE_UB_H
#define __THREE_LAYER_CACHE_UB_H

#include <stdint.h>
#include <stddef.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

/* ---- Tuning knobs (same as v4 for apples-to-apples comparison) ---- */
#define TLC_HOT_CAPACITY      (1 << 17)   /* 128K index entries */
#define TLC_WARM_CAPACITY     (1 << 20)   /* 1M value entries   */
#define TLC_COLD_SEGMENT_SIZE (1 << 20)
#define TLC_MAX_COLD_SEGMENTS 64
#define TLC_VALUE_SIZE        1200
#define TLC_RING_BUFFER_SIZE  4096
#define TLC_NUM_REPLICAS      2
#define TLC_NUM_SHARDS        3
#define TLC_TOTAL_PARTITIONS  6
#define TLC_PAXOS_QUORUM      2
#define BMP_BITS_PER_WORD     64

/* ---- UB Memory Configuration ---- */
#define UB_NUM_NODES          4           /* Number of UB memory nodes */
#define UB_VNODE_PER_PHYSICAL 32          /* Virtual nodes per physical node */
#define UB_HASH_RING_SIZE     (UB_NUM_NODES * UB_VNODE_PER_PHYSICAL * (UB_LOCAL_VNODE_WEIGHT + 1))
#define UB_NODE_MEM_SIZE      (2ULL * 1024 * 1024 * 1024)  /* 2GB per node region */
#define UB_LOCAL_VNODE_WEIGHT 4           /* Local node gets 4x more vnodes */

/* ---- SVE2 Compute Configuration ---- */
#define SVE2_EMB_TABLE_SIZE   (1 << 17)   /* 128K embeddings in UB memory */
#define SVE2_EMB_DIM_DEFAULT  300         /* Default embedding dimension */
#define SVE2_GEMM_OUT_DIM     64          /* GEMM output dimension */

/* ---- Bitmap CAS lock (identical to v4) ---- */
typedef struct {
    atomic_uint_fast64_t *words;
    size_t num_words;
} bitmap_lock_t;

static inline int bmp_lock_init(bitmap_lock_t *bl, size_t n) {
    bl->num_words = (n + BMP_BITS_PER_WORD - 1) / BMP_BITS_PER_WORD;
    if (bl->num_words < 1) bl->num_words = 1;
    bl->words = (atomic_uint_fast64_t *)calloc(bl->num_words, sizeof(atomic_uint_fast64_t));
    return bl->words ? 0 : -1;
}
static inline void bmp_lock_destroy(bitmap_lock_t *bl) { free((void*)bl->words); }

static inline void bmp_lock_acquire(bitmap_lock_t *bl, uint32_t bucket) {
    size_t wi = bucket / BMP_BITS_PER_WORD;
    if (wi >= bl->num_words) wi = bucket % bl->num_words;
    uint64_t bit = 1ULL << (bucket % BMP_BITS_PER_WORD);
    for (;;) {
        uint64_t old = atomic_load_explicit(&bl->words[wi], memory_order_relaxed);
        if (!(old & bit)) {
            if (atomic_compare_exchange_weak_explicit(&bl->words[wi], &old, old | bit,
                    memory_order_acquire, memory_order_relaxed))
                return;
        }
#if defined(__aarch64__)
        __asm__ volatile("yield" ::: "memory");
#elif defined(__x86_64__)
        __asm__ volatile("pause" ::: "memory");
#endif
    }
}
static inline void bmp_lock_release(bitmap_lock_t *bl, uint32_t bucket) {
    size_t wi = bucket / BMP_BITS_PER_WORD;
    if (wi >= bl->num_words) wi = bucket % bl->num_words;
    uint64_t bit = 1ULL << (bucket % BMP_BITS_PER_WORD);
    atomic_fetch_and_explicit(&bl->words[wi], ~bit, memory_order_release);
}

/* ---- Enums ---- */
typedef enum { ENTRY_EMPTY=0, ENTRY_VALID=1, ENTRY_DIRTY=2, ENTRY_EXPIRED=3 } entry_state_t;
typedef enum { RB_EVENT_NONE=0, RB_EVENT_WRITE=1, RB_EVENT_PROMOTE=2,
               RB_EVENT_EVICT=3, RB_EVENT_FLUSH=4, RB_EVENT_READ_THROUGH=5,
               RB_EVENT_CONFLICT=6 } rb_event_type_t;

/* ---- HOT layer: key→warm_index (16 bytes, cache-friendly) ---- */
typedef struct {
    uint64_t key;
    int32_t  warm_idx;
    uint32_t _pad;
} hot_index_t;  /* 16 bytes */

typedef struct {
    hot_index_t *table;       /* Backed by UB memory */
    size_t       capacity;
    uint32_t     mask;
    bitmap_lock_t bmp;
    atomic_uint_fast64_t hits;
    atomic_uint_fast64_t misses;
} hot_layer_t;

/* ---- WARM layer ---- */
typedef struct {
    uint64_t key;
    uint8_t  value[TLC_VALUE_SIZE];
    entry_state_t state;
    uint64_t write_ts_ns;
    uint64_t ttl_ns;
    uint32_t access_count;
} warm_entry_t;

typedef struct {
    warm_entry_t *entries;    /* Backed by UB memory */
    int32_t      *hash_table; /* Backed by UB memory */
    atomic_size_t count;
    size_t        capacity;
    uint32_t      mask;
    bitmap_lock_t bmp;
    atomic_uint_fast64_t hits;
    atomic_uint_fast64_t misses;
} warm_layer_t;

/* ---- COLD layer ---- */
typedef struct { uint64_t key; uint8_t value[TLC_VALUE_SIZE]; uint64_t offset; } cold_record_t;
typedef struct { cold_record_t *data; uint64_t base_offset; size_t count; size_t capacity; } cold_segment_t;
typedef struct {
    cold_segment_t segments[TLC_MAX_COLD_SEGMENTS];
    atomic_int num_segments;
    atomic_uint_fast64_t next_offset;
    uint64_t *offset_index; size_t offset_index_size; uint32_t oi_mask;
    bitmap_lock_t bmp;
    atomic_uint_fast64_t hits, misses;
} cold_layer_t;

/* ---- Ring Buffer ---- */
typedef struct { rb_event_type_t type; uint64_t key; uint32_t shard_id; } rb_event_t;
typedef struct {
    rb_event_t events[TLC_RING_BUFFER_SIZE];
    atomic_uint_fast64_t head, tail;
} ring_buffer_t;

/* ---- Paxos ---- */
typedef struct {
    atomic_uint_fast64_t highest_seen_id, accepted_id;
    uint8_t accepted_value[TLC_VALUE_SIZE];
    bitmap_lock_t bmp;
} paxos_acceptor_t;

/* ---- HA ---- */
typedef struct { int idc_id, shard_id, partition_id; bool is_primary, is_alive; } ha_partition_t;
typedef struct {
    ha_partition_t partitions[TLC_TOTAL_PARTITIONS];
    int my_idc, my_shard; atomic_int failed_idc;
} ha_manager_t;

/* ---- UB Memory Node ---- */
typedef struct {
    int      node_id;
    void    *base_addr;       /* mmap'd UB memory region */
    size_t   total_size;
    size_t   used;
    int      is_local;        /* 1 if this node is local (no cross-node) */
} ub_mem_node_t;

/* ---- Consistent Hash Ring for UB node mapping ---- */
typedef struct {
    uint32_t hash_val;
    int      node_id;
} ub_hash_vnode_t;

typedef struct {
    ub_hash_vnode_t vnodes[UB_HASH_RING_SIZE];
    int             num_vnodes;
    int             num_physical;
} ub_hash_ring_t;

/* ---- UB Memory Manager ---- */
typedef struct {
    ub_mem_node_t   nodes[UB_NUM_NODES];
    int             num_nodes;
    ub_hash_ring_t  ring;
    int             local_node_id;    /* This process's preferred node */

    /* Pointers into UB memory for each layer */
    void           *hot_region;       /* HOT table lives here */
    void           *warm_entries_region;
    void           *warm_ht_region;
    void           *cold_region;
    float          *emb_table;        /* Embedding table in UB memory */
    size_t          emb_table_entries; /* Number of embeddings */
    size_t          emb_dim;          /* Embedding dimension */
    float          *gemm_weights;     /* GEMM weight matrix [emb_dim × gemm_out_dim] */
    size_t          gemm_out_dim;     /* GEMM output dimension */

    /* Stats */
    atomic_uint_fast64_t local_accesses;
    atomic_uint_fast64_t remote_accesses;
} ub_mem_manager_t;

/* ---- Top-level cache ---- */
typedef struct {
    hot_layer_t hot; warm_layer_t warm; cold_layer_t cold;
    ring_buffer_t ring; paxos_acceptor_t paxos[TLC_NUM_SHARDS]; ha_manager_t ha;
    ub_mem_manager_t ub_mgr;
    atomic_uint_fast64_t total_reads, total_writes, read_throughs, write_throughs, paxos_conflicts;
    /* SVE2 compute stats */
    atomic_uint_fast64_t sve2_similarity_ops;
    atomic_uint_fast64_t sve2_gemm_ops;
    atomic_uint_fast64_t sve2_gather_ops;
    atomic_uint_fast64_t sve2_fused_ops;
} three_layer_cache_t;

/* ---- API (same signatures as v4) ---- */
int  tlc_init(three_layer_cache_t *c, int idc, int shard);
void tlc_destroy(three_layer_cache_t *c);
int  tlc_get(three_layer_cache_t *c, uint64_t key, void *out);
int  tlc_put(three_layer_cache_t *c, uint64_t key, const void *val);
int  tlc_ha_failover(three_layer_cache_t *c, int fidc);
int  tlc_ha_recover(three_layer_cache_t *c, int ridc);
int  tlc_ha_get_partition(three_layer_cache_t *c, uint64_t key);
int  tlc_paxos_propose(three_layer_cache_t *c, uint64_t key, const void *val, uint32_t sid);
void tlc_print_stats(three_layer_cache_t *c);
void tlc_flush_tls_stats(three_layer_cache_t *c);
int  cold_append(cold_layer_t *c, uint64_t key, const void *val);

/* ---- UB-specific API ---- */
int  ub_mgr_init(ub_mem_manager_t *mgr, int local_node_id, int num_nodes);
void ub_mgr_destroy(ub_mem_manager_t *mgr);
int  ub_mgr_get_node_for_key(ub_mem_manager_t *mgr, uint64_t key);

/* ---- SVE2 Fused Compute API (v6) ---- */

/* Initialize embedding table in UB memory and populate with random data */
int  tlc_emb_init(three_layer_cache_t *c, size_t n_emb, size_t dim);

/* Fused gather-load + cosine similarity: read embeddings from UB, compute sim vs query */
int  tlc_sve2_similarity(three_layer_cache_t *c,
                         const float *query, size_t dim,
                         const uint64_t *emb_ids, size_t n_ids,
                         float *similarities);

/* Fused gather-load + GEMM: read embedding rows from UB, multiply by weight matrix */
int  tlc_sve2_gemm(three_layer_cache_t *c,
                   const uint64_t *row_ids, size_t n_rows,
                   float *output, size_t out_dim);

/* Batch gather load: read N embeddings from UB memory */
int  tlc_sve2_gather(three_layer_cache_t *c,
                     const uint64_t *emb_ids, size_t n_ids,
                     float *output);

#endif /* __THREE_LAYER_CACHE_UB_H */
