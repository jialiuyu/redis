/*
 * Three-Layer Cache v6 — UB Memory + SVE2 Fused Compute
 *
 * v5 base: UB shared memory + consistent hashing.
 * v6 adds: SVE2 fused gather-load + compute:
 *   - sve2_fused_gather_similarity: read emb from UB + cosine sim
 *   - sve2_fused_gather_gemm: read emb from UB + matrix multiply
 *   - sve2_batch_gather_load: SVE2 streaming read from UB
 *
 * Compile: gcc -O3 -march=armv8.2-a+sve+f32mm -pthread -std=c11
 */
#define _GNU_SOURCE
#include "three_layer_cache_ub.h"
#include "sve2_gemm.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/mman.h>
#include <errno.h>

/* ============================================================
 * Utility functions
 * ============================================================ */
static inline uint32_t hash_fast(uint64_t k, uint32_t mask) {
    k ^= k >> 33; k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33; k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33; return (uint32_t)(k & mask);
}

static inline uint32_t murmur_mix32(uint32_t h) {
    h ^= h >> 16; h *= 0x85ebca6b;
    h ^= h >> 13; h *= 0xc2b2ae35;
    h ^= h >> 16; return h;
}

static inline uint64_t now_ns_slow(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec*1000000000ULL + ts.tv_nsec;
}

static void rb_init(ring_buffer_t *rb) { memset(rb, 0, sizeof(*rb)); }

/* Per-thread stats */
static __thread uint64_t tls_hot_hits = 0;
static __thread uint64_t tls_hot_misses = 0;
static __thread uint64_t tls_warm_hits = 0;
static __thread uint64_t tls_warm_misses = 0;

/* ============================================================
 * UB Memory Manager — Consistent Hash Ring
 * ============================================================ */

/* Sort comparator for vnodes */
static int vnode_cmp(const void *a, const void *b) {
    uint32_t ha = ((const ub_hash_vnode_t *)a)->hash_val;
    uint32_t hb = ((const ub_hash_vnode_t *)b)->hash_val;
    return (ha > hb) - (ha < hb);
}

/* Allocate a UB memory region via mmap (hugetlb with fallback) */
static void *ub_alloc_region(size_t size) {
    void *p = mmap(NULL, size, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (p == MAP_FAILED) {
        /* Fallback to regular anonymous mmap */
        p = mmap(NULL, size, PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (p == MAP_FAILED) return NULL;
    }
    return p;
}

static void ub_free_region(void *p, size_t size) {
    if (p && p != MAP_FAILED) munmap(p, size);
}

int ub_mgr_init(ub_mem_manager_t *mgr, int local_node_id, int num_nodes) {
    memset(mgr, 0, sizeof(*mgr));
    if (num_nodes <= 0 || num_nodes > UB_NUM_NODES) num_nodes = UB_NUM_NODES;
    mgr->num_nodes = num_nodes;
    mgr->local_node_id = local_node_id;
    atomic_store(&mgr->local_accesses, 0);
    atomic_store(&mgr->remote_accesses, 0);

    /* Allocate UB memory nodes */
    for (int i = 0; i < num_nodes; i++) {
        mgr->nodes[i].node_id = i;
        mgr->nodes[i].total_size = UB_NODE_MEM_SIZE;
        mgr->nodes[i].used = 0;
        mgr->nodes[i].is_local = (i == local_node_id) ? 1 : 0;
        mgr->nodes[i].base_addr = ub_alloc_region(UB_NODE_MEM_SIZE);
        if (!mgr->nodes[i].base_addr) {
            fprintf(stderr, "UB: Failed to allocate node %d memory (%zu MB)\n",
                    i, UB_NODE_MEM_SIZE / (1024*1024));
            /* Cleanup already allocated */
            for (int j = 0; j < i; j++)
                ub_free_region(mgr->nodes[j].base_addr, mgr->nodes[j].total_size);
            return -1;
        }
        memset(mgr->nodes[i].base_addr, 0, UB_NODE_MEM_SIZE);
    }

    /* Build consistent hash ring — local node gets UB_LOCAL_VNODE_WEIGHT× more vnodes
     * to maximize local memory access and minimize cross-node traffic */
    mgr->ring.num_physical = num_nodes;
    mgr->ring.num_vnodes = 0;
    for (int n = 0; n < num_nodes; n++) {
        int vcount = UB_VNODE_PER_PHYSICAL;
        if (n == local_node_id) vcount *= UB_LOCAL_VNODE_WEIGHT;
        for (int v = 0; v < vcount; v++) {
            uint32_t seed = (uint32_t)(n * 10000 + v);
            uint32_t h = murmur_mix32(seed);
            int idx = mgr->ring.num_vnodes++;
            if (idx >= UB_HASH_RING_SIZE) break;
            mgr->ring.vnodes[idx].hash_val = h;
            mgr->ring.vnodes[idx].node_id = n;
        }
    }
    qsort(mgr->ring.vnodes, mgr->ring.num_vnodes,
          sizeof(ub_hash_vnode_t), vnode_cmp);

    printf("UB Memory Manager: %d nodes × %zu MB = %zu MB total\n",
           num_nodes, UB_NODE_MEM_SIZE / (1024*1024),
           (size_t)num_nodes * UB_NODE_MEM_SIZE / (1024*1024));
    printf("UB Consistent Hash Ring: %d vnodes (%d per physical)\n",
           mgr->ring.num_vnodes, UB_VNODE_PER_PHYSICAL);
    printf("UB Local node: %d\n", local_node_id);

    return 0;
}

void ub_mgr_destroy(ub_mem_manager_t *mgr) {
    for (int i = 0; i < mgr->num_nodes; i++) {
        ub_free_region(mgr->nodes[i].base_addr, mgr->nodes[i].total_size);
        mgr->nodes[i].base_addr = NULL;
    }
}

/* Binary search on sorted vnode ring */
int ub_mgr_get_node_for_key(ub_mem_manager_t *mgr, uint64_t key) {
    uint32_t h = murmur_mix32((uint32_t)(key ^ (key >> 32)));
    int lo = 0, hi = mgr->ring.num_vnodes - 1;

    /* Find first vnode with hash >= h */
    while (lo < hi) {
        int mid = (lo + hi) / 2;
        if (mgr->ring.vnodes[mid].hash_val < h) lo = mid + 1;
        else hi = mid;
    }
    /* Wrap around */
    if (lo >= mgr->ring.num_vnodes) lo = 0;

    int target = mgr->ring.vnodes[lo].node_id;

    /* Track locality stats */
    if (target == mgr->local_node_id)
        atomic_fetch_add_explicit(&mgr->local_accesses, 1, memory_order_relaxed);
    else
        atomic_fetch_add_explicit(&mgr->remote_accesses, 1, memory_order_relaxed);

    return target;
}

/* ============================================================
 * HOT Layer — Lock-free, backed by UB memory
 * ============================================================ */
static int hot_init_ub(hot_layer_t *h, size_t cap, ub_mem_manager_t *mgr) {
    h->capacity = cap; h->mask = (uint32_t)(cap - 1);
    atomic_store(&h->hits, 0); atomic_store(&h->misses, 0);

    /* Allocate HOT table from UB node 0 (local node) */
    size_t hot_size = cap * sizeof(hot_index_t);
    ub_mem_node_t *local = &mgr->nodes[mgr->local_node_id];

    if (local->used + hot_size > local->total_size) {
        fprintf(stderr, "UB: Not enough memory on local node for HOT layer\n");
        return -1;
    }

    h->table = (hot_index_t *)((uint8_t *)local->base_addr + local->used);
    local->used += hot_size;
    /* Ensure 16B alignment for ARM atomic LDP/STP */
    local->used = (local->used + 15) & ~15ULL;

    memset(h->table, 0, hot_size);
    for (size_t i = 0; i < cap; i++) h->table[i].warm_idx = -1;

    mgr->hot_region = h->table;
    bmp_lock_init(&h->bmp, 1);

    printf("  HOT: %zu entries (%zu KB) on UB node %d\n",
           cap, hot_size / 1024, mgr->local_node_id);
    return 0;
}

/* Lock-free HOT get (identical to v4 — same memory layout) */
static inline int32_t hot_get_idx(hot_layer_t *h, uint64_t key) {
    uint32_t slot = hash_fast(key, h->mask);
    for (uint32_t i = 0; i < 4; i++) {
        uint32_t s = (slot + i) & h->mask;
        if (i < 3) __builtin_prefetch(&h->table[(slot + i + 1) & h->mask], 0, 3);
        hot_index_t e = h->table[s];
        if (e.warm_idx >= 0 && e.key == key) {
            tls_hot_hits++;
            return e.warm_idx;
        }
        if (e.warm_idx < 0) break;
    }
    tls_hot_misses++;
    return -1;
}

/* Lock-free HOT put */
static inline void hot_put_idx(hot_layer_t *h, uint64_t key, int32_t warm_idx) {
    uint32_t slot = hash_fast(key, h->mask);
    for (uint32_t i = 0; i < 4; i++) {
        uint32_t s = (slot + i) & h->mask;
        hot_index_t cur = h->table[s];
        if (cur.warm_idx < 0 || cur.key == key) {
            hot_index_t nv = {key, warm_idx, 0};
            h->table[s] = nv;
            return;
        }
    }
    hot_index_t nv = {key, warm_idx, 0};
    h->table[slot & h->mask] = nv;
}

/* ============================================================
 * WARM Layer — Bitmap CAS lock, UB memory backed
 * ============================================================ */
static int warm_init_ub(warm_layer_t *w, size_t cap, ub_mem_manager_t *mgr) {
    w->capacity = cap; w->mask = (uint32_t)(cap * 2 - 1);
    atomic_store(&w->count, 0);
    atomic_store(&w->hits, 0); atomic_store(&w->misses, 0);
    if (bmp_lock_init(&w->bmp, cap * 2) != 0) return -1;

    /* Allocate WARM entries across UB nodes using round-robin partitioning.
     * Each node gets cap/num_nodes entries.
     * For simplicity in the benchmark, we allocate from local node. */
    size_t entries_size = cap * sizeof(warm_entry_t);
    size_t ht_size = (cap * 2) * sizeof(int32_t);

    /* Try to fit on local node first */
    ub_mem_node_t *local = &mgr->nodes[mgr->local_node_id];
    if (local->used + entries_size + ht_size > local->total_size) {
        /* Spread across nodes */
        fprintf(stderr, "UB: WARM layer too large for single node, using fallback mmap\n");
        w->entries = (warm_entry_t *)ub_alloc_region(entries_size);
        w->hash_table = (int32_t *)ub_alloc_region(ht_size);
        if (!w->entries || !w->hash_table) return -1;
    } else {
        w->entries = (warm_entry_t *)((uint8_t *)local->base_addr + local->used);
        local->used += entries_size;
        local->used = (local->used + 63) & ~63ULL; /* 64B align */

        w->hash_table = (int32_t *)((uint8_t *)local->base_addr + local->used);
        local->used += ht_size;
        local->used = (local->used + 63) & ~63ULL;

        mgr->warm_entries_region = w->entries;
        mgr->warm_ht_region = w->hash_table;
    }

    memset(w->entries, 0, entries_size);
    memset(w->hash_table, -1, ht_size);

    printf("  WARM: %zu entries (%zu MB) + HT (%zu KB) on UB memory\n",
           cap, entries_size / (1024*1024), ht_size / 1024);
    return 0;
}

static int warm_get(warm_layer_t *w, uint64_t key, void *out, int32_t *out_idx) {
    uint32_t slot = hash_fast(key, w->mask);
    uint32_t lock_id = slot % (uint32_t)w->bmp.num_words;

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
    for (uint32_t i = 0; i < 6; i++) {
        int32_t idx = w->hash_table[(slot + i) & w->mask];
        if (idx < 0) break;
        if (idx >= 0 && (size_t)idx < w->capacity &&
            w->entries[idx].key == key && w->entries[idx].state != ENTRY_EMPTY) {
            int32_t target = idx;
            w->entries[target].state = ENTRY_DIRTY;
            w->entries[target].access_count++;
            bmp_lock_release(&w->bmp, lock_id);
            memcpy(w->entries[target].value, val, TLC_VALUE_SIZE);
            if (out_idx) *out_idx = target;
            return 0;
        }
    }
    int32_t target = (int32_t)atomic_fetch_add_explicit(&w->count, 1, memory_order_relaxed);
    if ((size_t)target >= w->capacity) {
        bmp_lock_release(&w->bmp, lock_id);
        return -1;
    }
    for (uint32_t i = 0; i < 6; i++) {
        uint32_t s = (slot + i) & w->mask;
        if (w->hash_table[s] < 0) { w->hash_table[s] = target; break; }
    }
    bmp_lock_release(&w->bmp, lock_id);

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
 * COLD Layer — Append-only, UB memory backed
 * ============================================================ */
static int cold_init_ub(cold_layer_t *c, ub_mem_manager_t *mgr) {
    memset(c, 0, sizeof(*c));
    atomic_store(&c->hits, 0); atomic_store(&c->misses, 0);
    atomic_store(&c->num_segments, 1); atomic_store(&c->next_offset, 0);
    if (bmp_lock_init(&c->bmp, TLC_MAX_COLD_SEGMENTS) != 0) return -1;

    /* First COLD segment from UB memory (use a non-local node to spread load) */
    int cold_node = (mgr->local_node_id + 1) % mgr->num_nodes;
    ub_mem_node_t *node = &mgr->nodes[cold_node];
    size_t seg_size = TLC_COLD_SEGMENT_SIZE * sizeof(cold_record_t);

    if (node->used + seg_size <= node->total_size) {
        c->segments[0].data = (cold_record_t *)((uint8_t *)node->base_addr + node->used);
        node->used += seg_size;
        node->used = (node->used + 63) & ~63ULL;
        mgr->cold_region = c->segments[0].data;
    } else {
        c->segments[0].data = (cold_record_t *)ub_alloc_region(seg_size);
        if (!c->segments[0].data) return -1;
    }
    memset(c->segments[0].data, 0, seg_size);
    c->segments[0].capacity = TLC_COLD_SEGMENT_SIZE;

    c->offset_index_size = TLC_WARM_CAPACITY * 2;
    c->oi_mask = (uint32_t)(c->offset_index_size - 1);
    c->offset_index = (uint64_t *)calloc(c->offset_index_size, sizeof(uint64_t));
    if (!c->offset_index) return -1;
    memset(c->offset_index, 0xFF, c->offset_index_size * sizeof(uint64_t));

    printf("  COLD: segment 0 (%zu MB) on UB node %d\n",
           seg_size / (1024*1024), cold_node);
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
        /* New segments use regular mmap (could also use UB nodes) */
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

/* ============================================================
 * Paxos + HA (unchanged from v4)
 * ============================================================ */
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
 * Top-level API
 * ============================================================ */
int tlc_init(three_layer_cache_t *c, int idc, int shard) {
    memset(c, 0, sizeof(*c));

    printf("\n=== Three-Layer Cache v6 (UB + SVE2 Fused Compute) ===\n");
    printf("Initializing UB Memory Manager...\n");

    /* Initialize UB memory manager first */
    if (ub_mgr_init(&c->ub_mgr, 0, UB_NUM_NODES) != 0) {
        fprintf(stderr, "Failed to init UB memory manager\n");
        return -1;
    }

    printf("Allocating cache layers on UB memory...\n");

    if (hot_init_ub(&c->hot, TLC_HOT_CAPACITY, &c->ub_mgr) != 0) return -1;
    if (warm_init_ub(&c->warm, TLC_WARM_CAPACITY, &c->ub_mgr) != 0) return -1;
    if (cold_init_ub(&c->cold, &c->ub_mgr) != 0) return -1;

    rb_init(&c->ring);
    for (int i = 0; i < TLC_NUM_SHARDS; i++) paxos_init(&c->paxos[i]);
    ha_init(&c->ha, idc, shard);

    /* Print UB memory usage summary */
    printf("\nUB Memory Usage:\n");
    for (int i = 0; i < c->ub_mgr.num_nodes; i++) {
        printf("  Node %d: %zu / %zu MB used (%s)\n",
               i, c->ub_mgr.nodes[i].used / (1024*1024),
               c->ub_mgr.nodes[i].total_size / (1024*1024),
               c->ub_mgr.nodes[i].is_local ? "LOCAL" : "remote");
    }
    printf("=== UB Init Complete ===\n\n");

    /* Initialize SVE2 stats */
    atomic_store(&c->sve2_similarity_ops, 0);
    atomic_store(&c->sve2_gemm_ops, 0);
    atomic_store(&c->sve2_gather_ops, 0);
    atomic_store(&c->sve2_fused_ops, 0);

    return 0;
}

void tlc_destroy(three_layer_cache_t *c) {
    /* HOT and WARM tables are in UB memory — don't free() them */
    bmp_lock_destroy(&c->hot.bmp);
    bmp_lock_destroy(&c->warm.bmp);

    /* COLD segments beyond segment 0 may be calloc'd */
    int ns = atomic_load(&c->cold.num_segments);
    for (int i = 1; i < ns; i++) {
        if (c->cold.segments[i].data) free(c->cold.segments[i].data);
    }
    free(c->cold.offset_index);
    bmp_lock_destroy(&c->cold.bmp);

    /* Release UB memory */
    ub_mgr_destroy(&c->ub_mgr);
}

/* GET: lock-free HOT → bitmap-CAS WARM → COLD
 * Consistent hash locality tracking sampled at 1/256 to avoid hot-path overhead */
int tlc_get(three_layer_cache_t *c, uint64_t key, void *out) {
    uint64_t rd = atomic_fetch_add_explicit(&c->total_reads, 1, memory_order_relaxed);

    /* Sample locality tracking (every 256th op) to keep hot path lean */
    if (__builtin_expect((rd & 0xFF) == 0, 0))
        ub_mgr_get_node_for_key(&c->ub_mgr, key);

    /* 1. HOT: fully lock-free */
    int32_t widx = hot_get_idx(&c->hot, key);
    if (widx >= 0 && (size_t)widx < c->warm.capacity) {
        warm_entry_t *e = &c->warm.entries[widx];
        if (e->key == key && e->state != ENTRY_EMPTY) {
            memcpy(out, e->value, TLC_VALUE_SIZE);
            e->access_count++;
            return 0;
        }
    }

    /* 2. WARM: bitmap CAS lock */
    int32_t found_idx = -1;
    if (warm_get(&c->warm, key, out, &found_idx) == 0) {
        hot_put_idx(&c->hot, key, found_idx);
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
    uint64_t wr = atomic_fetch_add_explicit(&c->total_writes, 1, memory_order_relaxed);

    /* Sample locality tracking */
    if (__builtin_expect((wr & 0xFF) == 0, 0))
        ub_mgr_get_node_for_key(&c->ub_mgr, key);

    int32_t idx = -1;
    if (warm_put(&c->warm, key, val, &idx) != 0) {
        cold_append(&c->cold, key, val);
        atomic_fetch_add_explicit(&c->write_throughs, 1, memory_order_relaxed);
        return 0;
    }
    if (idx >= 0) hot_put_idx(&c->hot, key, idx);
    return 0;
}

/* Flush TLS stats */
void tlc_flush_tls_stats(three_layer_cache_t *c) {
    atomic_fetch_add(&c->hot.hits, tls_hot_hits);
    atomic_fetch_add(&c->hot.misses, tls_hot_misses);
    atomic_fetch_add(&c->warm.hits, tls_warm_hits);
    atomic_fetch_add(&c->warm.misses, tls_warm_misses);
    tls_hot_hits = tls_hot_misses = tls_warm_hits = tls_warm_misses = 0;
}

void tlc_print_stats(three_layer_cache_t *c) {
    printf("\n========================================\n");
    printf("Three-Layer Cache v6 (UB + SVE2 Fused Compute)\n");
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
    printf("\nSVE2 Compute Stats:\n");
    printf("  Similarity ops:  %lu\n", atomic_load(&c->sve2_similarity_ops));
    printf("  GEMM ops:        %lu\n", atomic_load(&c->sve2_gemm_ops));
    printf("  Gather ops:      %lu\n", atomic_load(&c->sve2_gather_ops));
    printf("  Fused ops total: %lu\n", atomic_load(&c->sve2_fused_ops));
    if (c->ub_mgr.emb_table) {
        printf("  Embedding table: %zu entries × %zu dim (%.1f MB in UB)\n",
               c->ub_mgr.emb_table_entries, c->ub_mgr.emb_dim,
               (double)(c->ub_mgr.emb_table_entries * c->ub_mgr.emb_dim * sizeof(float)) / (1024*1024));
    }
    printf("\nUB Memory Locality (sampled ~1/256 ops):\n");
    uint64_t local_acc = atomic_load(&c->ub_mgr.local_accesses);
    uint64_t remote_acc = atomic_load(&c->ub_mgr.remote_accesses);
    uint64_t total_acc = local_acc + remote_acc;
    printf("  Local accesses:  %lu (%.1f%%)\n", local_acc,
           total_acc > 0 ? 100.0 * local_acc / total_acc : 0);
    printf("  Remote accesses: %lu (%.1f%%)\n", remote_acc,
           total_acc > 0 ? 100.0 * remote_acc / total_acc : 0);
    printf("  UB Nodes: %d, Local node: %d\n",
           c->ub_mgr.num_nodes, c->ub_mgr.local_node_id);
    for (int i = 0; i < c->ub_mgr.num_nodes; i++) {
        printf("  Node %d: %zu / %zu MB used\n",
               i, c->ub_mgr.nodes[i].used / (1024*1024),
               c->ub_mgr.nodes[i].total_size / (1024*1024));
    }
    printf("========================================\n");
}

/* ============================================================
 * SVE2 Embedding Table — allocated in UB memory
 * ============================================================ */
int tlc_emb_init(three_layer_cache_t *c, size_t n_emb, size_t dim) {
    ub_mem_manager_t *mgr = &c->ub_mgr;
    size_t emb_bytes = n_emb * dim * sizeof(float);
    size_t w_bytes = dim * SVE2_GEMM_OUT_DIM * sizeof(float);

    printf("SVE2 Embedding Init: %zu entries × %zu dim = %.1f MB\n",
           n_emb, dim, (double)emb_bytes / (1024*1024));

    /* Allocate embedding table on local UB node */
    ub_mem_node_t *local = &mgr->nodes[mgr->local_node_id];
    if (local->used + emb_bytes + w_bytes <= local->total_size) {
        mgr->emb_table = (float *)((uint8_t *)local->base_addr + local->used);
        local->used += emb_bytes;
        local->used = (local->used + 63) & ~63ULL;

        mgr->gemm_weights = (float *)((uint8_t *)local->base_addr + local->used);
        local->used += w_bytes;
        local->used = (local->used + 63) & ~63ULL;
    } else {
        /* Fallback to separate mmap */
        mgr->emb_table = (float *)ub_alloc_region(emb_bytes);
        mgr->gemm_weights = (float *)ub_alloc_region(w_bytes);
        if (!mgr->emb_table || !mgr->gemm_weights) return -1;
    }

    mgr->emb_table_entries = n_emb;
    mgr->emb_dim = dim;
    mgr->gemm_out_dim = SVE2_GEMM_OUT_DIM;

    /* Initialize with random data (simulating loaded embeddings) */
    unsigned int seed = 54321;
    for (size_t i = 0; i < n_emb * dim; i++)
        mgr->emb_table[i] = ((float)rand_r(&seed) / RAND_MAX - 0.5f) * 0.1f;

    /* Initialize GEMM weight matrix */
    for (size_t i = 0; i < dim * SVE2_GEMM_OUT_DIM; i++)
        mgr->gemm_weights[i] = ((float)rand_r(&seed) / RAND_MAX - 0.5f) * 0.02f;

    printf("  Embedding table: %.1f MB on UB node %d\n",
           (double)emb_bytes / (1024*1024), mgr->local_node_id);
    printf("  GEMM weights: %zu × %d = %.1f KB\n",
           dim, SVE2_GEMM_OUT_DIM, (double)w_bytes / 1024);

    return 0;
}

/* ============================================================
 * SVE2 Fused Gather + Cosine Similarity
 * ============================================================ */
int tlc_sve2_similarity(three_layer_cache_t *c,
                        const float *query, size_t dim,
                        const uint64_t *emb_ids, size_t n_ids,
                        float *similarities)
{
    if (!c->ub_mgr.emb_table || dim != c->ub_mgr.emb_dim) return -1;

    sve2_fused_gather_similarity(
        c->ub_mgr.emb_table, emb_ids, n_ids, dim, query, similarities);

    atomic_fetch_add_explicit(&c->sve2_similarity_ops, n_ids, memory_order_relaxed);
    atomic_fetch_add_explicit(&c->sve2_fused_ops, n_ids, memory_order_relaxed);
    return 0;
}

/* ============================================================
 * SVE2 Fused Gather + GEMM
 * ============================================================ */
int tlc_sve2_gemm(three_layer_cache_t *c,
                  const uint64_t *row_ids, size_t n_rows,
                  float *output, size_t out_dim)
{
    if (!c->ub_mgr.emb_table || !c->ub_mgr.gemm_weights) return -1;
    if (out_dim == 0) out_dim = c->ub_mgr.gemm_out_dim;

    sve2_fused_gather_gemm(
        c->ub_mgr.emb_table, row_ids, n_rows,
        c->ub_mgr.emb_dim, c->ub_mgr.gemm_weights,
        out_dim, output);

    atomic_fetch_add_explicit(&c->sve2_gemm_ops, n_rows, memory_order_relaxed);
    atomic_fetch_add_explicit(&c->sve2_fused_ops, n_rows, memory_order_relaxed);
    return 0;
}

/* ============================================================
 * SVE2 Batch Gather Load
 * ============================================================ */
int tlc_sve2_gather(three_layer_cache_t *c,
                    const uint64_t *emb_ids, size_t n_ids,
                    float *output)
{
    if (!c->ub_mgr.emb_table) return -1;

    sve2_batch_gather_load(
        c->ub_mgr.emb_table, emb_ids, n_ids,
        c->ub_mgr.emb_dim, output);

    atomic_fetch_add_explicit(&c->sve2_gather_ops, n_ids, memory_order_relaxed);
    return 0;
}
