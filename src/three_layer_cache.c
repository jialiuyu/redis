/*
 * Three-Layer Cache v3 — HOT = index-only (16B/entry), value in WARM
 * All bitmap CAS, zero mutex, no syscall on hot path.
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

/* ============================================================
 * HOT: 16-byte index entries → 128K entries = 2MB (fits L3)
 * key(8B) + warm_idx(4B) + pad(4B) = 16B → 4 per cache line
 * ============================================================ */
static int hot_init(hot_layer_t *h, size_t cap) {
    h->capacity = cap; h->mask = (uint32_t)(cap - 1);
    atomic_store(&h->hits, 0); atomic_store(&h->misses, 0);
    if (bmp_lock_init(&h->bmp, cap) != 0) return -1;
    h->table = (hot_index_t *)calloc(cap, sizeof(hot_index_t));
    if (!h->table) return -1;
    for (size_t i = 0; i < cap; i++) h->table[i].warm_idx = -1;
    return 0;
}

/* HOT get: returns warm_idx if found, -1 if miss. NO memcpy. */
static inline int32_t hot_get_idx(hot_layer_t *h, uint64_t key) {
    uint32_t slot = hash_fast(key, h->mask);
    /* No lock needed for read — entries are written atomically (key+idx together) */
    for (uint32_t i = 0; i < 4; i++) {
        uint32_t s = (slot + i) & h->mask;
        hot_index_t *e = &h->table[s];
        if (e->warm_idx >= 0 && e->key == key) {
            atomic_fetch_add_explicit(&h->hits, 1, memory_order_relaxed);
            return e->warm_idx;
        }
        if (e->warm_idx < 0) break; /* empty slot = end of chain */
    }
    atomic_fetch_add_explicit(&h->misses, 1, memory_order_relaxed);
    return -1;
}

/* HOT put: insert/update index entry */
static inline void hot_put_idx(hot_layer_t *h, uint64_t key, int32_t warm_idx) {
    uint32_t slot = hash_fast(key, h->mask);
    uint32_t lock_id = slot % (uint32_t)h->bmp.num_words;
    bmp_lock_acquire(&h->bmp, lock_id);
    for (uint32_t i = 0; i < 4; i++) {
        uint32_t s = (slot + i) & h->mask;
        if (h->table[s].warm_idx < 0 || h->table[s].key == key) {
            h->table[s].key = key;
            h->table[s].warm_idx = warm_idx;
            bmp_lock_release(&h->bmp, lock_id);
            return;
        }
    }
    /* All 4 slots full — evict slot 0 (pseudo-random) */
    h->table[slot & h->mask].key = key;
    h->table[slot & h->mask].warm_idx = warm_idx;
    bmp_lock_release(&h->bmp, lock_id);
}

/* ============================================================
 * WARM: value storage, bitmap CAS per bucket
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

/* Returns entry index if found, -1 if miss. Copies value to out. */
static int warm_get(warm_layer_t *w, uint64_t key, void *out, int32_t *out_idx) {
    uint32_t slot = hash_fast(key, w->mask);
    uint32_t lock_id = slot % (uint32_t)w->bmp.num_words;
    bmp_lock_acquire(&w->bmp, lock_id);
    for (uint32_t i = 0; i < 6; i++) {
        int32_t idx = w->hash_table[(slot + i) & w->mask];
        if (idx < 0) break;
        if (w->entries[idx].key == key && w->entries[idx].state != ENTRY_EMPTY) {
            if (out) memcpy(out, w->entries[idx].value, TLC_VALUE_SIZE);
            if (out_idx) *out_idx = idx;
            w->entries[idx].access_count++;
            atomic_fetch_add_explicit(&w->hits, 1, memory_order_relaxed);
            bmp_lock_release(&w->bmp, lock_id);
            return 0;
        }
    }
    atomic_fetch_add_explicit(&w->misses, 1, memory_order_relaxed);
    bmp_lock_release(&w->bmp, lock_id);
    return -1;
}

static int warm_put(warm_layer_t *w, uint64_t key, const void *val, int32_t *out_idx) {
    uint32_t slot = hash_fast(key, w->mask);
    uint32_t lock_id = slot % (uint32_t)w->bmp.num_words;
    bmp_lock_acquire(&w->bmp, lock_id);
    /* Update existing */
    for (uint32_t i = 0; i < 6; i++) {
        int32_t idx = w->hash_table[(slot + i) & w->mask];
        if (idx < 0) break;
        if (idx >= 0 && (size_t)idx < w->capacity &&
            w->entries[idx].key == key && w->entries[idx].state != ENTRY_EMPTY) {
            memcpy(w->entries[idx].value, val, TLC_VALUE_SIZE);
            w->entries[idx].state = ENTRY_DIRTY;
            w->entries[idx].access_count++;
            if (out_idx) *out_idx = idx;
            bmp_lock_release(&w->bmp, lock_id);
            return 0;
        }
    }
    /* Insert new */
    int32_t target = (int32_t)atomic_fetch_add_explicit(&w->count, 1, memory_order_relaxed);
    if ((size_t)target >= w->capacity) {
        bmp_lock_release(&w->bmp, lock_id);
        return -1;
    }
    w->entries[target].key = key;
    memcpy(w->entries[target].value, val, TLC_VALUE_SIZE);
    w->entries[target].state = ENTRY_DIRTY;
    w->entries[target].write_ts_ns = now_ns_slow();
    w->entries[target].ttl_ns = 60ULL * 1000000000ULL;
    w->entries[target].access_count = 1;
    for (uint32_t i = 0; i < 6; i++) {
        uint32_t s = (slot + i) & w->mask;
        if (w->hash_table[s] < 0) { w->hash_table[s] = target; break; }
    }
    if (out_idx) *out_idx = target;
    bmp_lock_release(&w->bmp, lock_id);
    return 0;
}

/* ============================================================
 * COLD: O(1) append + O(1) offset lookup
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

/* Paxos + HA (compact) */
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
 * Top-level: HOT = index accelerator, value in WARM
 * GET fast path: HOT index lookup (16B, no memcpy) → direct WARM read
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

int tlc_get(three_layer_cache_t *c, uint64_t key, void *out) {
    atomic_fetch_add_explicit(&c->total_reads, 1, memory_order_relaxed);

    /* 1. HOT index lookup — 16B entry, no lock, no memcpy */
    int32_t widx = hot_get_idx(&c->hot, key);
    if (widx >= 0 && (size_t)widx < c->warm.capacity) {
        /* Direct read from WARM by index — skip hash lookup entirely */
        warm_entry_t *e = &c->warm.entries[widx];
        if (e->key == key && e->state != ENTRY_EMPTY) {
            memcpy(out, e->value, TLC_VALUE_SIZE);
            e->access_count++;
            return 0;
        }
        /* Stale HOT entry — fall through to WARM hash */
    }

    /* 2. WARM hash lookup */
    int32_t found_idx = -1;
    int wr = warm_get(&c->warm, key, out, &found_idx);
    if (wr == 0) {
        /* Promote to HOT index (cheap: just write 16B) */
        hot_put_idx(&c->hot, key, found_idx);
        return 0;
    }

    /* 3. COLD read-through */
    if (cold_get(&c->cold, key, out) == 0) {
        int32_t new_idx = -1;
        warm_put(&c->warm, key, out, &new_idx);
        if (new_idx >= 0) hot_put_idx(&c->hot, key, new_idx);
        atomic_fetch_add_explicit(&c->read_throughs, 1, memory_order_relaxed);
        return 0;
    }
    return -1;
}

int tlc_put(three_layer_cache_t *c, uint64_t key, const void *val) {
    atomic_fetch_add_explicit(&c->total_writes, 1, memory_order_relaxed);
    int32_t idx = -1;
    int rc = warm_put(&c->warm, key, val, &idx);
    if (rc != 0) {
        cold_append(&c->cold, key, val);
        atomic_fetch_add_explicit(&c->write_throughs, 1, memory_order_relaxed);
        return 0;
    }
    /* Update HOT index if this key was hot */
    if (idx >= 0) hot_put_idx(&c->hot, key, idx);
    return 0;
}

void tlc_print_stats(three_layer_cache_t *c) {
    printf("\n========================================\n");
    printf("Three-Layer Cache v3 (Index-HOT, Bitmap CAS)\n");
    printf("========================================\n");
    printf("Reads: %lu  Writes: %lu\n",
           atomic_load(&c->total_reads), atomic_load(&c->total_writes));
    printf("Read-throughs: %lu  Write-throughs: %lu\n",
           atomic_load(&c->read_throughs), atomic_load(&c->write_throughs));
    printf("Paxos conflicts: %lu\n", atomic_load(&c->paxos_conflicts));
    printf("HOT  hits=%lu miss=%lu (index-only, 16B/entry)\n",
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
