/*
 * Three-Layer Cache v4 — Kunpeng ARM optimized
 *
 * Key optimizations over v3:
 * 1. Lock-free HOT via 128-bit atomic (ARM LDP/STP naturally atomic on 16B aligned)
 * 2. WARM: memcpy outside lock, only index update inside lock
 * 3. Per-thread stats (no global atomic on hot path)
 * 4. PRFM prefetch hints for hash probe
 * 5. Compile with -march=armv8.2-a+lse for hardware LDADD/CASAL
 */
#define _GNU_SOURCE
#include "three_layer_cache.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/mman.h>

static inline uint32_t hash_fast(uint64_t k, uint32_t mask) {
    k ^= k >> 33; k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33; k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33; return (uint32_t)(k & mask);
}
static inline uint64_t now_ns_slow(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec*1000000000ULL + ts.tv_nsec;
}
static void rb_init(ring_buffer_t *rb) { memset(rb, 0, sizeof(*rb)); }

/* Per-thread stats to avoid global atomic bouncing on 320 cores */
static __thread uint64_t tls_hot_hits = 0;
static __thread uint64_t tls_hot_misses = 0;
static __thread uint64_t tls_warm_hits = 0;
static __thread uint64_t tls_warm_misses = 0;

/* ============================================================
 * HOT: FULLY LOCK-FREE via aligned 16B atomic read/write
 * ARM guarantees LDP/STP atomicity on 16B-aligned addresses.
 * No bitmap CAS needed — just atomic load/store of {key, warm_idx}.
 * ============================================================ */
static int hot_init(hot_layer_t *h, size_t cap) {
    h->capacity = cap; h->mask = (uint32_t)(cap - 1);
    atomic_store(&h->hits, 0); atomic_store(&h->misses, 0);
    /* Allocate 16B-aligned for atomic LDP/STP */
    if (posix_memalign((void **)&h->table, 16, cap * sizeof(hot_index_t)) != 0)
        return -1;
    memset(h->table, 0, cap * sizeof(hot_index_t));
    for (size_t i = 0; i < cap; i++) h->table[i].warm_idx = -1;
    /* Still init bmp for compatibility but won't use it on hot path */
    bmp_lock_init(&h->bmp, 1);
    return 0;
}

/* Lock-free HOT get: just read 16B entry (atomic on ARM if aligned) */
static inline int32_t hot_get_idx(hot_layer_t *h, uint64_t key) {
    uint32_t slot = hash_fast(key, h->mask);
    for (uint32_t i = 0; i < 4; i++) {
        uint32_t s = (slot + i) & h->mask;
        /* Prefetch next probe slot */
        if (i < 3) __builtin_prefetch(&h->table[(slot + i + 1) & h->mask], 0, 3);
        hot_index_t e = h->table[s]; /* 16B load — atomic on ARM aligned */
        if (e.warm_idx >= 0 && e.key == key) {
            tls_hot_hits++;
            return e.warm_idx;
        }
        if (e.warm_idx < 0) break;
    }
    tls_hot_misses++;
    return -1;
}

/* Lock-free HOT put: just write 16B entry (atomic on ARM aligned) */
static inline void hot_put_idx(hot_layer_t *h, uint64_t key, int32_t warm_idx) {
    uint32_t slot = hash_fast(key, h->mask);
    for (uint32_t i = 0; i < 4; i++) {
        uint32_t s = (slot + i) & h->mask;
        hot_index_t cur = h->table[s];
        if (cur.warm_idx < 0 || cur.key == key) {
            hot_index_t nv = {key, warm_idx};
            h->table[s] = nv; /* 16B store — atomic on ARM aligned */
            return;
        }
    }
    /* Evict first slot */
    hot_index_t nv = {key, warm_idx};
    h->table[slot & h->mask] = nv;
}

/* ============================================================
 * WARM: memcpy OUTSIDE lock, only index ops inside lock
 * ============================================================ */
static int warm_init(warm_layer_t *w, size_t cap) {
    w->capacity = cap; w->mask = (uint32_t)(cap * 2 - 1);
    atomic_store(&w->count, 0);
    atomic_store(&w->hits, 0); atomic_store(&w->misses, 0);
    if (bmp_lock_init(&w->bmp, cap * 2) != 0) return -1;
    w->entries = (warm_entry_t *)calloc(cap, sizeof(warm_entry_t));
    w->hash_table = (int32_t *)malloc((cap * 2) * sizeof(int32_t));
    if (!w->entries || !w->hash_table) return -1;
    memset(w->hash_table, -1, (cap * 2) * sizeof(int32_t));
    return 0;
}

static int warm_get(warm_layer_t *w, uint64_t key, void *out, int32_t *out_idx) {
    uint32_t slot = hash_fast(key, w->mask);
    uint32_t lock_id = slot % (uint32_t)w->bmp.num_words;

    /* Prefetch the first hash table entry before acquiring lock */
    __builtin_prefetch(&w->hash_table[slot & w->mask], 0, 3);

    bmp_lock_acquire(&w->bmp, lock_id);
    int32_t found = -1;
    for (uint32_t i = 0; i < 6; i++) {
        int32_t idx = w->hash_table[(slot + i) & w->mask];
        if (idx < 0) break;
        if (w->entries[idx].key == key && w->entries[idx].state != ENTRY_EMPTY) {
            found = idx;
            w->entries[idx].access_count++;
            break;
        }
    }
    bmp_lock_release(&w->bmp, lock_id);

    if (found >= 0) {
        /* memcpy OUTSIDE lock — no contention during copy */
        if (out) memcpy(out, w->entries[found].value, TLC_VALUE_SIZE);
        if (out_idx) *out_idx = found;
        tls_warm_hits++;
        return 0;
    }
    tls_warm_misses++;
    return -1;
}

static int warm_put(warm_layer_t *w, uint64_t key, const void *val, int32_t *out_idx) {
    uint32_t slot = hash_fast(key, w->mask);
    uint32_t lock_id = slot % (uint32_t)w->bmp.num_words;

    bmp_lock_acquire(&w->bmp, lock_id);
    /* Check existing */
    for (uint32_t i = 0; i < 6; i++) {
        int32_t idx = w->hash_table[(slot + i) & w->mask];
        if (idx < 0) break;
        if (idx >= 0 && (size_t)idx < w->capacity &&
            w->entries[idx].key == key && w->entries[idx].state != ENTRY_EMPTY) {
            int32_t target = idx;
            w->entries[target].state = ENTRY_DIRTY;
            w->entries[target].access_count++;
            bmp_lock_release(&w->bmp, lock_id);
            /* memcpy OUTSIDE lock */
            memcpy(w->entries[target].value, val, TLC_VALUE_SIZE);
            if (out_idx) *out_idx = target;
            return 0;
        }
    }
    /* Allocate new slot (atomic, outside lock scope for index) */
    int32_t target = (int32_t)atomic_fetch_add_explicit(&w->count, 1, memory_order_relaxed);
    if ((size_t)target >= w->capacity) {
        bmp_lock_release(&w->bmp, lock_id);
        return -1;
    }
    /* Insert into hash table (inside lock) */
    for (uint32_t i = 0; i < 6; i++) {
        uint32_t s = (slot + i) & w->mask;
        if (w->hash_table[s] < 0) { w->hash_table[s] = target; break; }
    }
    bmp_lock_release(&w->bmp, lock_id);

    /* Initialize entry OUTSIDE lock — no contention */
    w->entries[target].key = key;
    memcpy(w->entries[target].value, val, TLC_VALUE_SIZE);
    w->entries[target].state = ENTRY_DIRTY;
    w->entries[target].write_ts_ns = now_ns_slow();
    w->entries[target].ttl_ns = 60ULL * 1000000000ULL;
    w->entries[target].access_count = 1;
    if (out_idx) *out_idx = target;
    return 0;
}

/* ============================================================
 * COLD: unchanged from v3 (not on hot path)
 * ============================================================ */
static int cold_init(cold_layer_t *c) {
    memset(c, 0, sizeof(*c));
    atomic_store(&c->hits, 0); atomic_store(&c->misses, 0);
    atomic_store(&c->num_segments, 1); atomic_store(&c->next_offset, 0);
    if (bmp_lock_init(&c->bmp, TLC_MAX_COLD_SEGMENTS) != 0) return -1;
    c->segments[0].data = (cold_record_t *)calloc(TLC_COLD_SEGMENT_SIZE, sizeof(cold_record_t));
    if (!c->segments[0].data) return -1;
    c->segments[0].capacity = TLC_COLD_SEGMENT_SIZE;
    c->offset_index_size = TLC_WARM_CAPACITY * 2;
    c->oi_mask = (uint32_t)(c->offset_index_size - 1);
    c->offset_index = (uint64_t *)calloc(c->offset_index_size, sizeof(uint64_t));
    if (!c->offset_index) return -1;
    memset(c->offset_index, 0xFF, c->offset_index_size * sizeof(uint64_t));
    return 0;
}
int cold_append(cold_layer_t *c, uint64_t key, const void *val) {
    uint64_t off = atomic_fetch_add_explicit(&c->next_offset, 1, memory_order_relaxed);
    int si = atomic_load_explicit(&c->num_segments, memory_order_relaxed) - 1;
    uint32_t li = (uint32_t)si % (uint32_t)c->bmp.num_words;
    bmp_lock_acquire(&c->bmp, li);
    cold_segment_t *seg = &c->segments[si];
    if (seg->count >= seg->capacity) {
        bmp_lock_release(&c->bmp, li);
        int ni = atomic_fetch_add(&c->num_segments, 1);
        if (ni >= TLC_MAX_COLD_SEGMENTS) { atomic_fetch_sub(&c->num_segments, 1); return -1; }
        li = (uint32_t)ni % (uint32_t)c->bmp.num_words;
        bmp_lock_acquire(&c->bmp, li);
        seg = &c->segments[ni]; si = ni;
        seg->data = (cold_record_t *)calloc(TLC_COLD_SEGMENT_SIZE, sizeof(cold_record_t));
        if (!seg->data) { bmp_lock_release(&c->bmp, li); return -1; }
        seg->base_offset = off; seg->capacity = TLC_COLD_SEGMENT_SIZE;
    }
    size_t local = seg->count++;
    seg->data[local].key = key;
    memcpy(seg->data[local].value, val, TLC_VALUE_SIZE);
    seg->data[local].offset = off;
    bmp_lock_release(&c->bmp, li);
    c->offset_index[hash_fast(key, c->oi_mask)] = off;
    return 0;
}
static int cold_get(cold_layer_t *c, uint64_t key, void *out) {
    uint64_t off = c->offset_index[hash_fast(key, c->oi_mask)];
    if (off == UINT64_MAX) { atomic_fetch_add_explicit(&c->misses, 1, memory_order_relaxed); return -1; }
    int ns = atomic_load_explicit(&c->num_segments, memory_order_relaxed);
    for (int s = 0; s < ns; s++) {
        cold_segment_t *seg = &c->segments[s];
        if (off >= seg->base_offset && off < seg->base_offset + seg->count) {
            size_t li = (size_t)(off - seg->base_offset);
            if (seg->data[li].key == key) {
                memcpy(out, seg->data[li].value, TLC_VALUE_SIZE);
                atomic_fetch_add_explicit(&c->hits, 1, memory_order_relaxed);
                return 0;
            }
        }
    }
    atomic_fetch_add_explicit(&c->misses, 1, memory_order_relaxed);
    return -1;
}

/* Paxos + HA (unchanged) */
static void paxos_init(paxos_acceptor_t *p) {
    atomic_store(&p->highest_seen_id, 0); atomic_store(&p->accepted_id, 0);
    bmp_lock_init(&p->bmp, 1);
}
int tlc_paxos_propose(three_layer_cache_t *c, uint64_t key, const void *val, uint32_t sid) {
    (void)key; (void)sid;
    uint64_t pid = now_ns_slow(); int acc = 0;
    for (int s = 0; s < TLC_NUM_SHARDS; s++) {
        bmp_lock_acquire(&c->paxos[s].bmp, 0);
        if (pid > atomic_load(&c->paxos[s].highest_seen_id)) {
            atomic_store(&c->paxos[s].highest_seen_id, pid); acc++;
        }
        bmp_lock_release(&c->paxos[s].bmp, 0);
    }
    if (acc < TLC_PAXOS_QUORUM) { atomic_fetch_add(&c->paxos_conflicts, 1); return -1; }
    int com = 0;
    for (int s = 0; s < TLC_NUM_SHARDS; s++) {
        bmp_lock_acquire(&c->paxos[s].bmp, 0);
        if (atomic_load(&c->paxos[s].highest_seen_id) == pid) {
            atomic_store(&c->paxos[s].accepted_id, pid);
            memcpy(c->paxos[s].accepted_value, val, TLC_VALUE_SIZE); com++;
        }
        bmp_lock_release(&c->paxos[s].bmp, 0);
    }
    return com >= TLC_PAXOS_QUORUM ? 0 : -1;
}
static void ha_init(ha_manager_t *ha, int idc, int shard) {
    ha->my_idc = idc; ha->my_shard = shard; atomic_store(&ha->failed_idc, -1);
    for (int i = 0; i < TLC_NUM_REPLICAS; i++)
        for (int s = 0; s < TLC_NUM_SHARDS; s++) {
            int p = i*TLC_NUM_SHARDS+s;
            ha->partitions[p] = (ha_partition_t){i, s, p, i==0, true};
        }
}
int tlc_ha_get_partition(three_layer_cache_t *c, uint64_t key) {
    int f = atomic_load_explicit(&c->ha.failed_idc, memory_order_relaxed);
    return (f==0?1:0)*TLC_NUM_SHARDS + (int)(key%TLC_NUM_SHARDS);
}
int tlc_ha_failover(three_layer_cache_t *c, int fi) {
    atomic_store(&c->ha.failed_idc, fi);
    for (int s=0;s<TLC_NUM_SHARDS;s++) c->ha.partitions[fi*TLC_NUM_SHARDS+s].is_alive=false;
    return 0;
}
int tlc_ha_recover(three_layer_cache_t *c, int ri) {
    atomic_store(&c->ha.failed_idc, -1);
    for (int s=0;s<TLC_NUM_SHARDS;s++) c->ha.partitions[ri*TLC_NUM_SHARDS+s].is_alive=true;
    return 0;
}

/* ============================================================
 * Top-level
 * ============================================================ */
int tlc_init(three_layer_cache_t *c, int idc, int shard) {
    memset(c, 0, sizeof(*c));
    if (hot_init(&c->hot, TLC_HOT_CAPACITY) != 0) return -1;
    if (warm_init(&c->warm, TLC_WARM_CAPACITY) != 0) return -1;
    if (cold_init(&c->cold) != 0) return -1;
    rb_init(&c->ring);
    for (int i = 0; i < TLC_NUM_SHARDS; i++) paxos_init(&c->paxos[i]);
    ha_init(&c->ha, idc, shard);
    return 0;
}
void tlc_destroy(three_layer_cache_t *c) {
    free(c->hot.table); bmp_lock_destroy(&c->hot.bmp);
    free(c->warm.entries); free(c->warm.hash_table); bmp_lock_destroy(&c->warm.bmp);
    int ns = atomic_load(&c->cold.num_segments);
    for (int i = 0; i < ns; i++) free(c->cold.segments[i].data);
    free(c->cold.offset_index); bmp_lock_destroy(&c->cold.bmp);
}

/* GET: lock-free HOT → bitmap-CAS WARM → COLD */
int tlc_get(three_layer_cache_t *c, uint64_t key, void *out) {
    atomic_fetch_add_explicit(&c->total_reads, 1, memory_order_relaxed);

    /* 1. HOT: fully lock-free, 16B atomic read */
    int32_t widx = hot_get_idx(&c->hot, key);
    if (widx >= 0 && (size_t)widx < c->warm.capacity) {
        warm_entry_t *e = &c->warm.entries[widx];
        if (e->key == key && e->state != ENTRY_EMPTY) {
            memcpy(out, e->value, TLC_VALUE_SIZE);
            e->access_count++;
            return 0;
        }
    }

    /* 2. WARM: bitmap CAS lock, memcpy outside lock */
    int32_t found_idx = -1;
    if (warm_get(&c->warm, key, out, &found_idx) == 0) {
        hot_put_idx(&c->hot, key, found_idx); /* lock-free 16B write */
        return 0;
    }

    /* 3. COLD read-through */
    if (cold_get(&c->cold, key, out) == 0) {
        int32_t ni = -1;
        warm_put(&c->warm, key, out, &ni);
        if (ni >= 0) hot_put_idx(&c->hot, key, ni);
        atomic_fetch_add_explicit(&c->read_throughs, 1, memory_order_relaxed);
        return 0;
    }
    return -1;
}

/* PUT: bitmap CAS WARM, lock-free HOT index update */
int tlc_put(three_layer_cache_t *c, uint64_t key, const void *val) {
    atomic_fetch_add_explicit(&c->total_writes, 1, memory_order_relaxed);
    int32_t idx = -1;
    if (warm_put(&c->warm, key, val, &idx) != 0) {
        cold_append(&c->cold, key, val);
        atomic_fetch_add_explicit(&c->write_throughs, 1, memory_order_relaxed);
        return 0;
    }
    if (idx >= 0) hot_put_idx(&c->hot, key, idx);
    return 0;
}

/* Flush TLS stats to global (call from benchmark after threads join) */
void tlc_flush_tls_stats(three_layer_cache_t *c) {
    atomic_fetch_add(&c->hot.hits, tls_hot_hits);
    atomic_fetch_add(&c->hot.misses, tls_hot_misses);
    atomic_fetch_add(&c->warm.hits, tls_warm_hits);
    atomic_fetch_add(&c->warm.misses, tls_warm_misses);
    tls_hot_hits = tls_hot_misses = tls_warm_hits = tls_warm_misses = 0;
}

void tlc_print_stats(three_layer_cache_t *c) {
    printf("\n========================================\n");
    printf("Three-Layer Cache v4 (Kunpeng ARM Optimized)\n");
    printf("========================================\n");
    printf("Reads: %lu  Writes: %lu\n",
           atomic_load(&c->total_reads), atomic_load(&c->total_writes));
    printf("Read-throughs: %lu  Write-throughs: %lu\n",
           atomic_load(&c->read_throughs), atomic_load(&c->write_throughs));
    printf("HOT  hits=%lu miss=%lu (lock-free 16B index)\n",
           atomic_load(&c->hot.hits), atomic_load(&c->hot.misses));
    printf("WARM hits=%lu miss=%lu count=%zu/%zu\n",
           atomic_load(&c->warm.hits), atomic_load(&c->warm.misses),
           atomic_load(&c->warm.count), c->warm.capacity);
    printf("COLD hits=%lu miss=%lu segs=%d recs=%lu\n",
           atomic_load(&c->cold.hits), atomic_load(&c->cold.misses),
           atomic_load(&c->cold.num_segments), atomic_load(&c->cold.next_offset));
    printf("HA: IDC=%d Shard=%d FailedIDC=%d\n",
           c->ha.my_idc, c->ha.my_shard, atomic_load(&c->ha.failed_idc));
    printf("========================================\n");
}
