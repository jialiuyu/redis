/*
 * Three-Layer Cache v3 — Optimized for throughput
 *
 * Key insight: HOT layer stores key+index (not full value).
 * HOT entry = 16 bytes → fits 4 entries per cache line.
 * Value lives in WARM; HOT is just an index accelerator.
 *
 * All sync: bitmap CAS. Zero mutex.
 */
#ifndef __THREE_LAYER_CACHE_H
#define __THREE_LAYER_CACHE_H

#include <stdint.h>
#include <stddef.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#define TLC_HOT_CAPACITY      (1 << 17)   /* 128K index entries */
#define TLC_WARM_CAPACITY     (1 << 20)   /* 1M value entries */
#define TLC_COLD_SEGMENT_SIZE (1 << 20)
#define TLC_MAX_COLD_SEGMENTS 64
#define TLC_VALUE_SIZE        1200
#define TLC_RING_BUFFER_SIZE  4096
#define TLC_NUM_REPLICAS      2
#define TLC_NUM_SHARDS        3
#define TLC_TOTAL_PARTITIONS  6
#define TLC_PAXOS_QUORUM      2
#define BMP_BITS_PER_WORD     64

/* ---- Bitmap CAS lock ---- */
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
        /* spin yield */
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

/* ---- HOT layer: key→warm_index only (16 bytes per entry, cache-friendly) ---- */
typedef struct {
    uint64_t key;
    int32_t  warm_idx;   /* index into warm.entries[] */
    uint32_t _pad;
} hot_index_t;  /* exactly 16 bytes = 4 per cache line */

typedef struct {
    hot_index_t *table;       /* open-addressing hash, power-of-2 size */
    size_t       capacity;    /* must be power of 2 */
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
    warm_entry_t *entries;
    int32_t      *hash_table;
    atomic_size_t count;
    size_t        capacity;
    uint32_t      mask;       /* capacity*2 - 1 */
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

/* ---- Top-level ---- */
typedef struct {
    hot_layer_t hot; warm_layer_t warm; cold_layer_t cold;
    ring_buffer_t ring; paxos_acceptor_t paxos[TLC_NUM_SHARDS]; ha_manager_t ha;
    atomic_uint_fast64_t total_reads, total_writes, read_throughs, write_throughs, paxos_conflicts;
} three_layer_cache_t;

/* ---- API ---- */
int  tlc_init(three_layer_cache_t *c, int idc, int shard);
void tlc_destroy(three_layer_cache_t *c);
int  tlc_get(three_layer_cache_t *c, uint64_t key, void *out);
int  tlc_put(three_layer_cache_t *c, uint64_t key, const void *val);
int  tlc_ha_failover(three_layer_cache_t *c, int fidc);
int  tlc_ha_recover(three_layer_cache_t *c, int ridc);
int  tlc_ha_get_partition(three_layer_cache_t *c, uint64_t key);
int  tlc_paxos_propose(three_layer_cache_t *c, uint64_t key, const void *val, uint32_t sid);
void tlc_print_stats(three_layer_cache_t *c);
int  cold_append(cold_layer_t *c, uint64_t key, const void *val);
#endif
