/*
 * tlc_hash_fc_cache.c — Standalone cache for FC hash bench server
 *
 * Mirrors three_layer_cache_ub.c structure but:
 *   - No SVE2/HA dependencies
 *   - Uses hash_strategy.h for HOT layer
 *   - Uses eviction_strategy.h for HOT PUT
 *   - PUT returns warm_idx (server handles HOT PUT via FC)
 *   - GET handled by server's direct_lookup() (FC path)
 *
 * Compile with hash_strategy.h + eviction_strategy.h + three_layer_cache_ub.h (types only)
 */
#include "three_layer_cache_ub.h"
#include "hash_strategy.h"
#include "eviction_strategy.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <stdatomic.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#ifdef USE_UB_MEM
#include <errno.h>
/* Standalone stubs for ub_client dependencies (same as ub_client_ut.c) */
/* UB_CLIENT_STANDALONE is defined via Makefile CFLAGS */
#include "ub_client.h"
static void ut_vlog(const char *fmt, ...) {
    va_list ap; va_start(ap, fmt); vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n"); va_end(ap);
}
void serverLog(int level, const char *fmt, ...) {
    (void)level; va_list ap; va_start(ap, fmt); vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n"); va_end(ap);
}
void *zcalloc(size_t size) { return calloc(1, size); }
void zfree(void *ptr) { free(ptr); }
sds sdsempty(void) { char *b = malloc(1); if (b) b[0] = '\0'; return b; }
sds sdsnew(const char *init) {
    if (!init) init = "";
    size_t len = strlen(init); char *b = malloc(len + 1);
    if (b) memcpy(b, init, len + 1); return b;
}
sds sdscat(sds s, const char *t) {
    size_t sl = s ? strlen(s) : 0, tl = t ? strlen(t) : 0;
    char *b = realloc(s, sl + tl + 1);
    if (!b) { free(s); return NULL; }
    if (tl) memcpy(b + sl, t, tl); b[sl + tl] = '\0'; return b;
}
sds sdscatprintf(sds s, const char *fmt, ...) {
    va_list ap, ap2; int needed; size_t sl = s ? strlen(s) : 0;
    char *b; va_start(ap, fmt); va_copy(ap2, ap);
    needed = vsnprintf(NULL, 0, fmt, ap2); va_end(ap2);
    if (needed < 0) { va_end(ap); free(s); return NULL; }
    b = realloc(s, sl + (size_t)needed + 1);
    if (!b) { va_end(ap); free(s); return NULL; }
    vsnprintf(b + sl, (size_t)needed + 1, fmt, ap); va_end(ap); return b;
}
#endif

#ifndef HOT_ON_UB
#define HOT_ON_UB 1
#endif

/* ---- hash_fast (for WARM/COLD, not under test) ---- */
static inline uint32_t hash_fast(uint64_t k, uint32_t mask) {
    k ^= k >> 33; k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33; k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33; return (uint32_t)(k & mask);
}

static inline uint64_t now_ns_slow(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec*1000000000ULL + ts.tv_nsec;
}

static inline uint32_t murmur_mix32(uint32_t h) {
    h ^= h >> 16; h *= 0x85ebca6b;
    h ^= h >> 13; h *= 0xc2b2ae35;
    h ^= h >> 16; return h;
}

/* ---- UB Memory Manager ---- */
static void *ub_alloc_region(size_t size) {
    void *p = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_HUGETLB, -1, 0);
    if (p == MAP_FAILED)
        p = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    return p == MAP_FAILED ? NULL : p;
}
static void ub_free_region(void *p, size_t size) {
    if (p && p != MAP_FAILED) munmap(p, size);
}

/* No raw device mapping here — USE_UB_MEM uses ub_client API below */

static int vnode_cmp(const void *a, const void *b) {
    uint32_t ha = ((const ub_hash_vnode_t*)a)->hash_val;
    uint32_t hb = ((const ub_hash_vnode_t*)b)->hash_val;
    return (ha > hb) - (ha < hb);
}

static void ub_node_reset(ub_mem_node_t *node, int node_id) {
    memset(node, 0, sizeof(*node));
    node->node_id = node_id;
    node->shm_fd = -1;
    node->device_path[0] = '\0';
}

#ifdef USE_UB_MEM
static int map_ub_rw_region(ub_mem_node_t *node,
                            const char *device_path,
                            size_t map_offset,
                            size_t map_size,
                            size_t visible_size,
                            int cacheable,
                            int is_local) {
    int open_flags = O_RDWR;
    int fd;
    void *mapping;

    if (!cacheable)
        open_flags |= O_SYNC;

    fd = open(device_path, open_flags);
    if (fd < 0) {
        fprintf(stderr, "UB: open %s failed: %s\n", device_path, strerror(errno));
        return -1;
    }

    mapping = mmap(NULL, map_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, (off_t)map_offset);
    if (mapping == MAP_FAILED) {
        fprintf(stderr, "UB: mmap %s offset=%zu size=%zu failed: %s\n",
                device_path, map_offset, map_size, strerror(errno));
        close(fd);
        return -1;
    }

    node->mapping_addr = mapping;
    node->mapping_size = map_size;
    node->base_addr = (char *)mapping + map_offset;
    node->total_size = visible_size;
    node->used = 0;
    node->is_local = is_local;
    node->shm_fd = fd;
    node->map_offset = map_offset;
    node->cacheable = cacheable;
    snprintf(node->device_path, sizeof(node->device_path), "%s", device_path);
    return 0;
}
#endif

static int fc_ub_mgr_init(ub_mem_manager_t *mgr, int local_node_id, int num_nodes) {
    memset(mgr, 0, sizeof(*mgr));
    if (num_nodes <= 0 || num_nodes > UB_NUM_NODES) num_nodes = UB_NUM_NODES;
    mgr->num_nodes = num_nodes;
    mgr->local_node_id = local_node_id;
    atomic_store(&mgr->local_accesses, 0);
    atomic_store(&mgr->remote_accesses, 0);
    ub_node_reset(&mgr->hot_node, -1);
    for (int i = 0; i < num_nodes; i++) {
        ub_node_reset(&mgr->nodes[i], i);
#ifdef USE_UB_MEM
        if (i == local_node_id) {
            /* Local node: map real UB device via ub_client for init/config,
             * then mmap R/W ourselves (ub_client only maps PROT_READ for
             * gather-only use; cache needs read-write for HOT/WARM/COLD). */
            ub_mem_config_t cfg;
            memset(&cfg, 0, sizeof(cfg));
            cfg.shm_memid = UB_MEMID_BASE;
            cfg.shm_size = UB_SHM_SIZE;
            cfg.table_offset = 0;
            cfg.table_size = 0;
            cfg.vector_dimension = 300;
            cfg.cacheable = UB_CACHEABLE;
            cfg.use_ownership = 0;
            cfg.element_index_mode = UB_ELEMENT_INDEX_NUMERIC;
            cfg.table_name = "tlc_cache";
            cfg.shm_path = NULL;

            if (ub_client_init(&cfg) != 0) {
                fprintf(stderr, "UB: ub_client_init failed (memid=%llu size=%zu)\n",
                        (unsigned long long)UB_MEMID_BASE, (size_t)UB_SHM_SIZE);
                return -1;
            }
            /* Use ub_client to resolve device path and validate config */
            char device_path[UB_DEVICE_PATH_MAX];
            size_t table_size;
            /* Abuse load_embedding_table briefly just to get path + size validation */
            ub_address_space_t *as = NULL;
            if (ub_client_load_embedding_table("tlc_cache", &as) != 0 || !as) {
                fprintf(stderr, "UB: ub_client_load_embedding_table failed\n");
                ub_client_cleanup();
                return -1;
            }
            snprintf(device_path, sizeof(device_path), "%s", as->device_path);
            table_size = as->size;
            /* Destroy the PROT_READ mapping from ub_client */
            munmap(as->mapping_addr, as->mapping_size);
            /* Don't let ub_client_cleanup close our fd or unmap again. */
            as->mapping_addr = NULL;
            as->shm_fd = -1;
            ub_client_cleanup();

#if HOT_ON_UB
            char hot_device_path[UB_DEVICE_PATH_MAX];
            snprintf(hot_device_path, sizeof(hot_device_path), "/dev/obmm_shmdev%llu",
                     (unsigned long long)UB_HOT_MEMID);
            if (map_ub_rw_region(&mgr->hot_node, hot_device_path, 0, UB_HOT_SLICE_SIZE, UB_HOT_SLICE_SIZE,
                                 UB_HOT_CACHEABLE, 1) != 0) {
                return -1;
            }
#endif

            if (map_ub_rw_region(&mgr->nodes[i], device_path, 0, table_size, table_size,
                                 UB_CACHEABLE, 1) != 0) {
#if HOT_ON_UB
                if (mgr->hot_node.shm_fd >= 0) {
                    munmap(mgr->hot_node.mapping_addr, mgr->hot_node.mapping_size);
                    close(mgr->hot_node.shm_fd);
                    ub_node_reset(&mgr->hot_node, -1);
                }
#endif
                return -1;
            }

#if HOT_ON_UB
            printf("  UB HOT: mapped %s (offset=%zu MB size=%zu MB cacheable=%d)\n",
                   mgr->hot_node.device_path,
                   mgr->hot_node.map_offset / (1024*1024),
                   mgr->hot_node.total_size / (1024*1024),
                   mgr->hot_node.cacheable);
#endif
            printf("  UB WARM: mapped %s (offset=%zu MB size=%zu MB cacheable=%d)\n",
                   mgr->nodes[i].device_path,
                   mgr->nodes[i].map_offset / (1024*1024),
                   mgr->nodes[i].total_size / (1024*1024),
                   mgr->nodes[i].cacheable);
        } else {
            /* Remote nodes: anonymous mmap (would be UB interconnect in production) */
            mgr->nodes[i].total_size = UB_NODE_MEM_SIZE;
            mgr->nodes[i].used = 0;
            mgr->nodes[i].is_local = 0;
            mgr->nodes[i].base_addr = ub_alloc_region(UB_NODE_MEM_SIZE);
            if (!mgr->nodes[i].base_addr) {
                fprintf(stderr, "UB: Failed to allocate remote node %d\n", i);
                ub_client_cleanup();
                for (int j = 0; j < i; j++)
                    if (mgr->nodes[j].shm_fd < 0)
                        ub_free_region(mgr->nodes[j].base_addr, mgr->nodes[j].total_size);
                return -1;
            }
            memset(mgr->nodes[i].base_addr, 0, UB_NODE_MEM_SIZE);
        }
#else
        mgr->nodes[i].total_size = UB_NODE_MEM_SIZE;
        mgr->nodes[i].used = 0;
        mgr->nodes[i].is_local = (i == local_node_id) ? 1 : 0;
        mgr->nodes[i].base_addr = ub_alloc_region(UB_NODE_MEM_SIZE);
        if (!mgr->nodes[i].base_addr) {
            fprintf(stderr, "UB: Failed to allocate node %d\n", i);
            for (int j = 0; j < i; j++) ub_free_region(mgr->nodes[j].base_addr, mgr->nodes[j].total_size);
            return -1;
        }
        memset(mgr->nodes[i].base_addr, 0, UB_NODE_MEM_SIZE);
#endif
    }
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
    qsort(mgr->ring.vnodes, mgr->ring.num_vnodes, sizeof(ub_hash_vnode_t), vnode_cmp);
    printf("  UB Memory Manager: %d nodes", num_nodes);
#ifdef USE_UB_MEM
    printf(" (local=REAL UB device memid=%d, remote=anon mmap", UB_MEMID_BASE);
#if HOT_ON_UB
    printf(", hot-memid=%d hot-slice=%zuMB cacheable=%d", UB_HOT_MEMID,
           (size_t)(UB_HOT_SLICE_SIZE / (1024*1024)), UB_HOT_CACHEABLE);
#endif
    printf(")");
#else
    printf(" x %zu MB", UB_NODE_MEM_SIZE/(1024*1024));
#endif
    printf("\n");
    return 0;
}

static void fc_ub_mgr_destroy(ub_mem_manager_t *mgr) {
#ifdef USE_UB_MEM
    if (mgr->hot_node.shm_fd >= 0) {
        munmap(mgr->hot_node.mapping_addr, mgr->hot_node.mapping_size);
        close(mgr->hot_node.shm_fd);
    }
    for (int i = 0; i < mgr->num_nodes; i++) {
        if (mgr->nodes[i].shm_fd >= 0) {
            /* Local UB node: we own the R/W mapping and fd */
            munmap(mgr->nodes[i].mapping_addr, mgr->nodes[i].mapping_size);
            close(mgr->nodes[i].shm_fd);
        } else if (mgr->nodes[i].base_addr) {
            /* Remote node: anonymous mmap */
            ub_free_region(mgr->nodes[i].base_addr, mgr->nodes[i].total_size);
        }
    }
#else
    for (int i = 0; i < mgr->num_nodes; i++)
        ub_free_region(mgr->nodes[i].base_addr, mgr->nodes[i].total_size);
#endif
}

/* ============================================================
 * HOT Layer — hash_strategy.h + eviction_strategy.h
 * ============================================================ */
static int hot_init(hot_layer_t *h, size_t cap, ub_mem_manager_t *mgr) {
    h->capacity = cap; h->mask = (uint32_t)(cap - 1);
    atomic_store(&h->hits, 0); atomic_store(&h->misses, 0);
    size_t hot_size = cap * sizeof(hot_index_t);
    ub_mem_node_t *local = &mgr->nodes[mgr->local_node_id];
#if defined(USE_UB_MEM) && !HOT_ON_UB
    h->table = (hot_index_t*)ub_alloc_region(hot_size);
    if (!h->table) return -1;
    mgr->hot_region = h->table;
    memset(h->table, 0xFF, hot_size); /* warm_idx = -1 for all */
    printf("  HOT: %zu entries (%zu KB) strategy=%s backing=mmap\n",
           cap, hot_size/1024, hash_strategy_name());
    return 0;
#else
    if (mgr->hot_node.base_addr) {
        if (hot_size > mgr->hot_node.total_size) return -1;
        h->table = (hot_index_t*)mgr->hot_node.base_addr;
    } else if (local->used + hot_size > local->total_size) {
        h->table = (hot_index_t*)ub_alloc_region(hot_size);
        if (!h->table) return -1;
    } else {
        h->table = (hot_index_t*)((uint8_t*)local->base_addr + local->used);
        local->used += hot_size;
        local->used = (local->used + 63) & ~63ULL;
    }
    mgr->hot_region = h->table;
    memset(h->table, 0xFF, hot_size); /* warm_idx = -1 for all */
    printf("  HOT: %zu entries (%zu KB) strategy=%s backing=%s",
           cap, hot_size/1024, hash_strategy_name(),
#ifdef USE_UB_MEM
           "UB"
#else
           "mmap"
#endif
           );
#ifdef USE_UB_MEM
    if (mgr->hot_node.base_addr) {
        printf(" dev=%s offset=%zuMB cacheable=%d",
               mgr->hot_node.device_path,
               mgr->hot_node.map_offset / (1024*1024),
               mgr->hot_node.cacheable);
    }
#endif
    printf("\n");
    return 0;
#endif
}

/* ============================================================
 * WARM Layer — same as three_layer_cache_ub.c
 * ============================================================ */
static int warm_init(warm_layer_t *w, size_t cap, ub_mem_manager_t *mgr) {
    w->capacity = cap; w->mask = (uint32_t)(cap * 2 - 1);
    atomic_store(&w->count, 0);
    atomic_store(&w->hits, 0); atomic_store(&w->misses, 0);
    if (bmp_lock_init(&w->bmp, cap * 2) != 0) return -1;
    size_t entries_size = cap * sizeof(warm_entry_t);
    size_t ht_size = (cap * 2) * sizeof(int32_t);
    ub_mem_node_t *local = &mgr->nodes[mgr->local_node_id];
    if (local->used + entries_size + ht_size > local->total_size) {
        w->entries = (warm_entry_t*)ub_alloc_region(entries_size);
        w->hash_table = (int32_t*)ub_alloc_region(ht_size);
        if (!w->entries || !w->hash_table) return -1;
    } else {
        w->entries = (warm_entry_t*)((uint8_t*)local->base_addr + local->used);
        local->used += entries_size; local->used = (local->used + 63) & ~63ULL;
        w->hash_table = (int32_t*)((uint8_t*)local->base_addr + local->used);
        local->used += ht_size; local->used = (local->used + 63) & ~63ULL;
        mgr->warm_entries_region = w->entries;
        mgr->warm_ht_region = w->hash_table;
    }
    memset(w->entries, 0, entries_size);
    memset(w->hash_table, -1, ht_size);
    printf("  WARM: %zu entries (%zu MB)\n", cap, entries_size/(1024*1024));
    return 0;
}

/* WARM PUT — returns 0 on success, fills *out_idx with warm index */
int fc_cache_warm_put(three_layer_cache_t *c, uint64_t key, const void *val, int32_t *out_idx) {
    warm_layer_t *w = &c->warm;
    uint32_t slot = hash_fast(key, w->mask);
    uint32_t lock_id = slot % (uint32_t)w->bmp.num_words;
    bmp_lock_acquire(&w->bmp, lock_id);
    for (uint32_t i = 0; i < 6; i++) {
        int32_t idx = w->hash_table[(slot + i) & w->mask];
        if (idx < 0) break;
        if (idx >= 0 && (size_t)idx < w->capacity &&
            w->entries[idx].key == key && w->entries[idx].state != ENTRY_EMPTY) {
            w->entries[idx].state = ENTRY_DIRTY;
            w->entries[idx].access_count++;
            bmp_lock_release(&w->bmp, lock_id);
            memcpy(w->entries[idx].value, val, TLC_VALUE_SIZE);
            if (out_idx) *out_idx = idx;
            return 0;
        }
    }
    int32_t target = (int32_t)atomic_fetch_add_explicit(&w->count, 1, memory_order_relaxed);
    if ((size_t)target >= w->capacity) {
        bmp_lock_release(&w->bmp, lock_id);
        return -1; /* WARM full */
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
 * COLD Layer — same as three_layer_cache_ub.c
 * ============================================================ */
static int cold_init(cold_layer_t *c, ub_mem_manager_t *mgr) {
    memset(c, 0, sizeof(*c));
    atomic_store(&c->hits, 0); atomic_store(&c->misses, 0);
    atomic_store(&c->num_segments, 1); atomic_store(&c->next_offset, 0);
    if (bmp_lock_init(&c->bmp, TLC_MAX_COLD_SEGMENTS) != 0) return -1;
    int cold_node = (mgr->local_node_id + 1) % mgr->num_nodes;
    ub_mem_node_t *node = &mgr->nodes[cold_node];
    size_t seg_size = TLC_COLD_SEGMENT_SIZE * sizeof(cold_record_t);
    if (node->used + seg_size <= node->total_size) {
        c->segments[0].data = (cold_record_t*)((uint8_t*)node->base_addr + node->used);
        node->used += seg_size; node->used = (node->used + 63) & ~63ULL;
        mgr->cold_region = c->segments[0].data;
    } else {
        c->segments[0].data = (cold_record_t*)ub_alloc_region(seg_size);
        if (!c->segments[0].data) return -1;
    }
    memset(c->segments[0].data, 0, seg_size);
    c->segments[0].capacity = TLC_COLD_SEGMENT_SIZE;
    c->offset_index_size = TLC_WARM_CAPACITY * 2;
    c->oi_mask = (uint32_t)(c->offset_index_size - 1);
    c->offset_index = (uint64_t*)calloc(c->offset_index_size, sizeof(uint64_t));
    if (!c->offset_index) return -1;
    memset(c->offset_index, 0xFF, c->offset_index_size * sizeof(uint64_t));
    printf("  COLD: segment 0 (%zu MB)\n", seg_size/(1024*1024));
    return 0;
}

int fc_cache_cold_append(cold_layer_t *c, uint64_t key, const void *val) {
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
        seg->data = (cold_record_t*)calloc(TLC_COLD_SEGMENT_SIZE, sizeof(cold_record_t));
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

/* ============================================================
 * Top-level API
 * ============================================================ */
int fc_cache_init(three_layer_cache_t *c) {
    memset(c, 0, sizeof(*c));
    printf("\n=== FC Hash Bench Cache (hash=%s, eviction=%s%s) ===\n",
        hash_strategy_name(), eviction_strategy_name(),
#ifdef USE_UB_MEM
        ", UB=REAL"
#else
        ""
#endif
        );
    if (fc_ub_mgr_init(&c->ub_mgr, 0, UB_NUM_NODES) != 0) return -1;
    if (hot_init(&c->hot, TLC_HOT_CAPACITY, &c->ub_mgr) != 0) return -1;
    if (warm_init(&c->warm, TLC_WARM_CAPACITY, &c->ub_mgr) != 0) return -1;
    if (cold_init(&c->cold, &c->ub_mgr) != 0) return -1;
    if (eviction_init(TLC_HOT_CAPACITY) != 0) return -1;
    return 0;
}

void fc_cache_destroy(three_layer_cache_t *c) {
    eviction_destroy();
    bmp_lock_destroy(&c->hot.bmp);
    bmp_lock_destroy(&c->warm.bmp);
    int ns = atomic_load(&c->cold.num_segments);
    for (int i = 1; i < ns; i++) if (c->cold.segments[i].data) free(c->cold.segments[i].data);
    free(c->cold.offset_index);
    bmp_lock_destroy(&c->cold.bmp);
#if defined(USE_UB_MEM) && !HOT_ON_UB
    if (c->ub_mgr.hot_region)
        ub_free_region(c->ub_mgr.hot_region, c->hot.capacity * sizeof(hot_index_t));
#endif
    fc_ub_mgr_destroy(&c->ub_mgr);
}
