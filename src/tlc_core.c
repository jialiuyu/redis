#include "tlc_core.h"
#include "cpu_relax.h"
#include "macro.h"
#include "sve_operation.h"
#include "vemb_v16_log.h"
#include "vemb_v16_protocol.h"
#include "zmalloc.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#define TLC_CORE_HOT_PROBES 4u
/*
 * The warm/cold hash tables stay intentionally sparse, but realistic bench
 * keysets can still create 10+ entry linear-probe runs. A slightly larger
 * probe window avoids spilling valid warm keys into the cold path and later
 * surfacing them as false NOT_FOUND responses.
 */
#define TLC_CORE_WARM_PROBES 16u
#define TLC_CORE_COLD_PROBES 16u
#define TLC_CORE_INVALID_OFFSET UINT64_MAX

typedef enum tlc_core_entry_state {
    TLC_CORE_ENTRY_EMPTY = 0,
    TLC_CORE_ENTRY_VALID = 1,
    TLC_CORE_ENTRY_DIRTY = 2,
    TLC_CORE_ENTRY_EXPIRED = 3,
} tlc_core_entry_state_t;

typedef struct tlc_core_hot_entry {
    atomic_uint_fast64_t key_hash;
    atomic_int warm_idx;
    uint32_t _pad;
} tlc_core_hot_entry_t;

typedef struct tlc_core_hold_layer {
    tlc_core_hot_entry_t *table;
    uint32_t capacity;
    uint32_t mask;
    atomic_uint_fast64_t hits;
    atomic_uint_fast64_t misses;
} tlc_core_hold_layer_t;

typedef struct tlc_core_warm_entry {
    /* TODO: evaluate using uint64_t key_hash as the canonical key, matching three_layer_cache_ub. */
    uint64_t key_hash;
    /* TODO: wire these into the warm eviction/expiration policy. */
    // uint64_t write_ts_ns;
    // uint64_t ttl_ns;
    uint32_t key_len;
    uint32_t value_size;
    tlc_warm_location_t location;
    atomic_uint_fast32_t access_count;
    char key[VEMB_V16_MAX_KEY_LEN];
    atomic_int state;
} tlc_core_warm_entry_t;

typedef struct tlc_core_warm_region_runtime {
    uint32_t region_id;
    uint32_t backend_type;
    uint32_t is_local;
    uint32_t weight;
    uint32_t value_size;
    uint32_t capacity_slots;
    vemb_v16_shared_region_allocator_t *shared_allocator;
    uint64_t region_bytes;
    uint8_t *mapped_addr;
} tlc_core_warm_region_runtime_t;

typedef struct tlc_core_warm_region_vnode {
    uint32_t hash_val;
    uint32_t region_index;
} tlc_core_warm_region_vnode_t;

typedef struct tlc_core_warm_layer {
    tlc_core_warm_entry_t *entries;
    int32_t *hash_table;
    tlc_core_warm_region_runtime_t *regions;
    tlc_core_warm_region_vnode_t *vnodes;
    atomic_uint_fast32_t count;
    uint32_t capacity;
    uint32_t hash_capacity;
    uint32_t region_count;
    uint32_t vnode_count;
    uint32_t mask;
    state_bitmap_t locks;
    atomic_uint_fast64_t hits;
    atomic_uint_fast64_t misses;
} tlc_core_warm_layer_t;

typedef struct tlc_core_cold_record {
    /* TODO: evaluate using uint64_t key_hash as the canonical key, matching three_layer_cache_ub. */
    uint64_t key_hash;
    uint64_t offset;
    uint32_t key_len;
    uint32_t value_size;
    char key[VEMB_V16_MAX_KEY_LEN];
} tlc_core_cold_record_t;

typedef struct tlc_core_cold_segment {
    tlc_core_cold_record_t *records;
    uint8_t *values;
    uint64_t base_offset;
    size_t count;
    size_t capacity;
} tlc_core_cold_segment_t;

typedef struct tlc_core_cold_layer {
    tlc_core_cold_segment_t *segments;
    atomic_int num_segments;
    atomic_uint_fast64_t next_offset;
    atomic_uint_fast64_t *offset_index;
    uint32_t offset_index_size;
    uint32_t oi_mask;
    uint32_t max_segments;
    size_t segment_capacity;
    state_bitmap_t locks;
    atomic_uint_fast64_t hits;
    atomic_uint_fast64_t misses;
} tlc_core_cold_layer_t;

struct tlc_core {
    uint32_t value_size;
    uint32_t warm_capacity;
    tlc_core_hold_layer_t hold;
    tlc_core_warm_layer_t warm;
    tlc_core_cold_layer_t cold;
    atomic_uint_fast64_t total_reads;
    atomic_uint_fast64_t total_writes;
    atomic_uint_fast64_t read_throughs;
    atomic_uint_fast64_t write_throughs;
    atomic_uint_fast64_t warm_alloc_local;
    atomic_uint_fast64_t warm_alloc_remote;
    atomic_uint_fast64_t warm_alloc_fallback;
    atomic_uint_fast64_t warm_alloc_cold_spill;
    atomic_uint_fast64_t warm_alloc_fail;
};

static const tlc_warm_location_t tlc_invalid_location = {
    .region_id = TLC_CORE_INVALID_REGION_ID,
    .region_index = UINT32_MAX,
    .local_slot = TLC_CORE_INVALID_SLOT,
    .bytes = 0,
    .offset = UINT64_MAX,
};

static uint32_t pow2_ceil_u32(uint64_t value) {
    uint32_t p = 1;
    while ((uint64_t)p < value && p < (1u << 30))
        p <<= 1;
    return p;
}

static uint32_t hash_fast(uint64_t key, uint32_t mask) {
    key ^= key >> 33;
    key *= 0xff51afd7ed558ccdULL;
    key ^= key >> 33;
    key *= 0xc4ceb9fe1a85ec53ULL;
    key ^= key >> 33;
    return (uint32_t)key & mask;
}

static uint32_t mix32(uint64_t key) {
    key ^= key >> 33;
    key *= 0xff51afd7ed558ccdULL;
    key ^= key >> 33;
    key *= 0xc4ceb9fe1a85ec53ULL;
    key ^= key >> 33;
    return (uint32_t)(key ^ (key >> 32));
}

static int vnode_compare(const void *a, const void *b) {
    const tlc_core_warm_region_vnode_t *va = a;
    const tlc_core_warm_region_vnode_t *vb = b;
    if (va->hash_val < vb->hash_val) return -1;
    if (va->hash_val > vb->hash_val) return 1;
    if (va->region_index < vb->region_index) return -1;
    if (va->region_index > vb->region_index) return 1;
    return 0;
}

static uint32_t vnode_lower_bound(const tlc_core_warm_layer_t *warm,
                                  uint32_t hash_val) {
    uint32_t lo = 0;
    uint32_t hi = warm->vnode_count;
    while (lo < hi) {
        uint32_t mid = lo + ((hi - lo) >> 1);
        if (warm->vnodes[mid].hash_val < hash_val)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo == warm->vnode_count ? 0 : lo;
}

static void bitmap_lock_blocking(state_bitmap_t *locks, uint32_t lock_id) {
    while (bitmap_try_acquire(locks, lock_id) != 0)
        cpu_relax();
}

static void bitmap_unlock(state_bitmap_t *locks, uint32_t lock_id) {
    bitmap_release(locks, lock_id);
}

static int key_valid(const char *key, uint32_t key_len) {
    return key && key_len > 0 && key_len <= VEMB_V16_MAX_KEY_LEN;
}

static int key_matches(uint64_t key_hash,
                       const char *key,
                       uint32_t key_len,
                       uint64_t entry_hash,
                       const char *entry_key,
                       uint32_t entry_key_len) {
    return entry_hash == key_hash &&
           entry_key_len == key_len &&
           memcmp(entry_key, key, key_len) == 0;
}

static int entry_readable(int state) {
    return state == TLC_CORE_ENTRY_VALID ||
           state == TLC_CORE_ENTRY_DIRTY;
}

static int hot_init(tlc_core_t *core, uint32_t requested_capacity) {
    tlc_core_hold_layer_t *hold = &core->hold;
    hold->capacity = requested_capacity ?
        pow2_ceil_u32(requested_capacity) : TLC_CORE_DEFAULT_HOT_CAPACITY;
    if (hold->capacity < 2) hold->capacity = 2;
    hold->mask = hold->capacity - 1u;
    hold->table = zcalloc(sizeof(*hold->table) * hold->capacity);
    RETURN_IF(!hold->table, -1);
    for (uint32_t i = 0; i < hold->capacity; i++) {
        atomic_init(&hold->table[i].key_hash, 0);
        atomic_init(&hold->table[i].warm_idx, -1);
    }
    atomic_init(&hold->hits, 0);
    atomic_init(&hold->misses, 0);
    return 0;
}

static int32_t hot_get(tlc_core_t *core, uint64_t key_hash) {
    tlc_core_hold_layer_t *hold = &core->hold;
    uint32_t slot = hash_fast(key_hash, hold->mask);
    for (uint32_t i = 0; i < TLC_CORE_HOT_PROBES; i++) {
        uint32_t pos = (slot + i) & hold->mask;
        if (i + 1 < TLC_CORE_HOT_PROBES) {
            __builtin_prefetch(&hold->table[(slot + i + 1) & hold->mask], 0, 3);
        }
        int32_t warm_idx = atomic_load_explicit(&hold->table[pos].warm_idx, memory_order_acquire);
        if (warm_idx < 0) break;
        uint64_t entry_hash =
            atomic_load_explicit(&hold->table[pos].key_hash, memory_order_relaxed);
        if (entry_hash == key_hash) {
            atomic_fetch_add_explicit(&hold->hits, 1, memory_order_relaxed);
            return warm_idx;
        }
    }
    atomic_fetch_add_explicit(&hold->misses, 1, memory_order_relaxed);
    return -1;
}

static void hot_put(tlc_core_t *core,
                    uint64_t key_hash,
                    int32_t warm_idx) {
    tlc_core_hold_layer_t *hold = &core->hold;
    uint32_t slot = hash_fast(key_hash, hold->mask);
    for (uint32_t i = 0; i < TLC_CORE_HOT_PROBES; i++) {
        uint32_t pos = (slot + i) & hold->mask;
        int32_t cur = atomic_load_explicit(&hold->table[pos].warm_idx, memory_order_acquire);
        uint64_t cur_hash = atomic_load_explicit(&hold->table[pos].key_hash, memory_order_relaxed);
        if (cur < 0 || cur_hash == key_hash) {
            atomic_store_explicit(&hold->table[pos].key_hash, key_hash, memory_order_relaxed);
            atomic_store_explicit(&hold->table[pos].warm_idx, warm_idx, memory_order_release);
            return;
        }
    }
    atomic_store_explicit(&hold->table[slot].key_hash, key_hash, memory_order_relaxed);
    atomic_store_explicit(&hold->table[slot].warm_idx, warm_idx, memory_order_release);
}

static int warm_regions_init(tlc_core_t *core, const tlc_core_config_t *config) {
    tlc_core_warm_layer_t *warm = &core->warm;
    uint32_t region_count = config->warm_region_count;
    const tlc_core_warm_region_config_t *regions = config->warm_regions;

    RETURN_IF(!regions || region_count == 0 ||
              region_count > TLC_CORE_MAX_WARM_REGIONS, -1);

    warm->regions = zcalloc(sizeof(*warm->regions) * region_count);
    RETURN_IF(!warm->regions, -1);
    warm->region_count = region_count;

    uint32_t total_vnodes = 0;
    uint32_t local_region_weight = config->local_region_weight ?
        config->local_region_weight : 4u;
    for (uint32_t i = 0; i < region_count; i++) {
        const tlc_core_warm_region_config_t *src = &regions[i];
        RETURN_IF(!src->mapped_addr ||
                  !src->shared_allocator ||
                  src->value_size != core->value_size ||
                  src->region_bytes < src->value_size,
                  -1);
        uint64_t capacity_slots = src->region_bytes / src->value_size;
        RETURN_IF(capacity_slots == 0 || capacity_slots > UINT32_MAX, -1);
        uint32_t weight = src->weight ? src->weight : 1u;
        RETURN_IF(src->is_local &&
                  weight > UINT32_MAX / local_region_weight,
                  -1);
        uint32_t effective_weight = src->is_local ?
            weight * local_region_weight : weight;
        RETURN_IF(effective_weight == 0 ||
                  effective_weight > UINT32_MAX / 32u,
                  -1);
        RETURN_IF(total_vnodes > UINT32_MAX - 32u * effective_weight, -1);
        total_vnodes += 32u * effective_weight;
        RETURN_IF(total_vnodes == 0, -1);

        warm->regions[i] = (tlc_core_warm_region_runtime_t){
            .region_id = src->region_id,
            .backend_type = src->backend_type,
            .is_local = src->is_local,
            .weight = weight,
            .value_size = src->value_size,
            .capacity_slots = (uint32_t)capacity_slots,
            .shared_allocator = src->shared_allocator,
            .region_bytes = src->region_bytes,
            .mapped_addr = src->mapped_addr,
        };
        serverLog(LL_NOTICE,
                  "tlc warm region init: region_index=%u region_id=%u backend_type=%u is_local=%u weight=%u effective_weight=%u value_size=%u region_bytes=%llu capacity_slots=%u mapped_addr=%p shared_allocator=%p",
                  i,
                  warm->regions[i].region_id,
                  warm->regions[i].backend_type,
                  warm->regions[i].is_local,
                  warm->regions[i].weight,
                  effective_weight,
                  warm->regions[i].value_size,
                  (unsigned long long)warm->regions[i].region_bytes,
                  warm->regions[i].capacity_slots,
                  warm->regions[i].mapped_addr,
                  (void *)warm->regions[i].shared_allocator);
    }

    warm->vnodes = zcalloc(sizeof(*warm->vnodes) * total_vnodes);
    RETURN_IF(!warm->vnodes, -1);
    warm->vnode_count = total_vnodes;
    uint32_t vnode_pos = 0;
    for (uint32_t i = 0; i < region_count; i++) {
        const tlc_core_warm_region_runtime_t *region = &warm->regions[i];
        uint32_t effective_weight = region->is_local ?
            region->weight * local_region_weight : region->weight;
        uint32_t vnodes = 32u * effective_weight;
        for (uint32_t v = 0; v < vnodes; v++) {
            uint64_t seed = ((uint64_t)region->region_id << 32) |
                            ((uint64_t)i << 24) |
                            (uint64_t)v;
            warm->vnodes[vnode_pos++] = (tlc_core_warm_region_vnode_t){
                .hash_val = mix32(seed),
                .region_index = i,
            };
        }
    }
    qsort(warm->vnodes, warm->vnode_count, sizeof(*warm->vnodes),
          vnode_compare);
    return 0;
}

static void log_warm_region_switch(const char *reason,
                                   uint64_t key_hash,
                                   uint32_t want_local,
                                   uint32_t attempted_regions,
                                   uint32_t vnode_pos,
                                   uint32_t region_index,
                                   tlc_core_warm_region_runtime_t *region,
                                   int alloc_rc) {
    vemb_v16_shared_region_allocator_t *allocator = region->shared_allocator;
    uint32_t magic = atomic_load_explicit(&allocator->magic, memory_order_acquire);
    uint32_t next_slot = atomic_load_explicit(&allocator->next_slot, memory_order_relaxed);
    uint32_t full = atomic_load_explicit(&allocator->full, memory_order_acquire);
    uint32_t used_slots = atomic_load_explicit(&allocator->used_slots, memory_order_relaxed);
    serverLog(LL_NOTICE,
              "tlc warm region switch: reason=%s key_hash=%llu want_local=%u attempted_regions=%u vnode_pos=%u region_index=%u region_id=%u allocator_region_id=%u alloc_rc=%d magic=%08x version=%u next_slot=%u used_slots=%u full=%u capacity_slots=%u generation=%llu",
              reason,
              (unsigned long long)key_hash,
              want_local,
              attempted_regions,
              vnode_pos,
              region_index,
              region->region_id,
              allocator->region_id,
              alloc_rc,
              magic,
              allocator->version,
              next_slot,
              used_slots,
              full,
              allocator->capacity_slots,
              (unsigned long long)allocator->generation);
}

static int warm_alloc_location_pass(tlc_core_t *core,
                                    uint64_t key_hash,
                                    uint32_t want_local,
                                    uint8_t *tried,
                                    uint32_t *attempted_regions,
                                    tlc_warm_location_t *location) {
    tlc_core_warm_layer_t *warm = &core->warm;
    uint32_t start = vnode_lower_bound(warm, mix32(key_hash));
    for (uint32_t i = 0; i < warm->vnode_count; i++) {
        uint32_t vnode_pos = (start + i) % warm->vnode_count;
        uint32_t region_index = warm->vnodes[vnode_pos].region_index;
        if (region_index >= warm->region_count || tried[region_index]) {
            continue;
        }
        tlc_core_warm_region_runtime_t *region = &warm->regions[region_index];
        if ((region->is_local ? 1u : 0u) != want_local) {
            continue;
        }
        tried[region_index] = 1;
        (*attempted_regions)++;

        if (vemb_v16_shared_allocator_full(region->shared_allocator)) {
            log_warm_region_switch("region_full",
                                   key_hash,
                                   want_local,
                                   *attempted_regions,
                                   vnode_pos,
                                   region_index,
                                   region,
                                   VEMB_V16_SHARED_ALLOCATOR_FULL);
            continue;
        }

        uint32_t local_slot = TLC_CORE_INVALID_SLOT;
        int alloc_rc =
            vemb_v16_shared_allocator_alloc(region->shared_allocator, &local_slot);
        if (alloc_rc == VEMB_V16_SHARED_ALLOCATOR_OK) {
            *location = (tlc_warm_location_t){
                .region_id = region->region_id,
                .region_index = region_index,
                .local_slot = local_slot,
                .bytes = core->value_size,
                .offset = (uint64_t)local_slot * core->value_size,
            };
            atomic_fetch_add_explicit(region->is_local ?
                                      &core->warm_alloc_local :
                                      &core->warm_alloc_remote,
                                      1,
                                      memory_order_relaxed);
            if (*attempted_regions > 1) {
                atomic_fetch_add_explicit(&core->warm_alloc_fallback, 1, memory_order_relaxed);
            }
            return 0;
        }
        log_warm_region_switch("alloc_retry_next_region",
                               key_hash,
                               want_local,
                               *attempted_regions,
                               vnode_pos,
                               region_index,
                               region,
                               alloc_rc);
    }
    serverLog(LL_DEBUG,
              "tlc warm alloc pass failed: key_hash=%llu want_local=%u attempted_regions=%u region_count=%u vnode_count=%u",
              (unsigned long long)key_hash,
              want_local,
              *attempted_regions,
              warm->region_count,
              warm->vnode_count);
    return -1;
}

static int warm_alloc_location(tlc_core_t *core,
                               uint64_t key_hash,
                               tlc_warm_location_t *location) {
    tlc_core_warm_layer_t *warm = &core->warm;
    uint8_t tried[TLC_CORE_MAX_WARM_REGIONS] = {0};
    uint32_t attempted_regions = 0;
    RETURN_IF(warm->region_count == 0 || warm->vnode_count == 0, -1);
    if (warm_alloc_location_pass(core, key_hash, 1, tried,
                                 &attempted_regions, location) == 0) {
        tlc_core_warm_region_runtime_t *region = &warm->regions[location->region_index];
        vemb_v16_shared_region_allocator_t *allocator = region->shared_allocator;
        serverLog(LL_DEBUG,
                  "tlc warm local alloc success: key_hash=%llu region_id=%u region_index=%u local_slot=%u offset=%llu attempted_regions=%u is_local=%u next_slot=%u used_slots=%u full=%u capacity_slots=%u",
                  (unsigned long long)key_hash,
                  location->region_id,
                  location->region_index,
                  location->local_slot,
                  (unsigned long long)location->offset,
                  attempted_regions,
                  region->is_local,
                  atomic_load_explicit(&allocator->next_slot, memory_order_relaxed),
                  atomic_load_explicit(&allocator->used_slots, memory_order_relaxed),
                  atomic_load_explicit(&allocator->full, memory_order_acquire),
                  allocator->capacity_slots);
        return 0;
    }
    memset(tried, 0, sizeof(tried));
    if (warm_alloc_location_pass(core, key_hash, 0, tried,
                                 &attempted_regions, location) == 0) {
        tlc_core_warm_region_runtime_t *region = &warm->regions[location->region_index];
        vemb_v16_shared_region_allocator_t *allocator = region->shared_allocator;
        serverLog(LL_DEBUG,
                  "tlc warm remote alloc success: key_hash=%llu region_id=%u region_index=%u local_slot=%u offset=%llu attempted_regions=%u is_local=%u next_slot=%u used_slots=%u full=%u capacity_slots=%u",
                  (unsigned long long)key_hash,
                  location->region_id,
                  location->region_index,
                  location->local_slot,
                  (unsigned long long)location->offset,
                  attempted_regions,
                  region->is_local,
                  atomic_load_explicit(&allocator->next_slot, memory_order_relaxed),
                  atomic_load_explicit(&allocator->used_slots, memory_order_relaxed),
                  atomic_load_explicit(&allocator->full, memory_order_acquire),
                  allocator->capacity_slots);
        return 0;
    }

    serverLog(LL_WARNING,
              "tlc warm alloc failed: key_hash=%llu attempted_regions=%u region_count=%u vnode_count=%u",
              (unsigned long long)key_hash,
              attempted_regions,
              warm->region_count,
              warm->vnode_count);
    *location = tlc_invalid_location;
    atomic_fetch_add_explicit(&core->warm_alloc_fail, 1, memory_order_relaxed);
    return -1;
}

static int warm_init(tlc_core_t *core, const tlc_core_config_t *config) {
    tlc_core_warm_layer_t *warm = &core->warm;
    warm->capacity = core->warm_capacity;
    warm->hash_capacity = pow2_ceil_u32((uint64_t)core->warm_capacity * 2u);
    if (warm->hash_capacity < 2) warm->hash_capacity = 2;
    warm->mask = warm->hash_capacity - 1u;
    warm->entries = zcalloc(sizeof(*warm->entries) * warm->capacity);
    warm->hash_table = zcalloc(sizeof(*warm->hash_table) * warm->hash_capacity);
    RETURN_IF(!warm->entries || !warm->hash_table, -1);
    for (uint32_t i = 0; i < warm->hash_capacity; i++)
        warm->hash_table[i] = -1;
    for (uint32_t i = 0; i < warm->capacity; i++) {
        atomic_init(&warm->entries[i].state, TLC_CORE_ENTRY_EMPTY);
        atomic_init(&warm->entries[i].access_count, 0);
    }
    atomic_init(&warm->count, 0);
    atomic_init(&warm->hits, 0);
    atomic_init(&warm->misses, 0);
    if (warm_regions_init(core, config) != 0) {
        return -1;
    }
    return bitmap_init(&warm->locks, warm->hash_capacity);
}

static int warm_entry_matches(tlc_core_warm_entry_t *entry,
                              const char *key,
                              uint32_t key_len,
                              uint64_t key_hash) {
    int state = atomic_load_explicit(&entry->state, memory_order_acquire);
    return entry_readable(state) &&
           key_matches(key_hash, key, key_len,
                       entry->key_hash, entry->key, entry->key_len);
}

static int warm_validate_idx(tlc_core_t *core,
                             int32_t warm_idx,
                             const char *key,
                             uint32_t key_len,
                             uint64_t key_hash,
                             tlc_warm_location_t *location) {
    RETURN_IF(warm_idx < 0 || (uint32_t)warm_idx >= core->warm.capacity, -1);
    tlc_core_warm_entry_t *entry = &core->warm.entries[warm_idx];
    int matched = warm_entry_matches(entry, key, key_len, key_hash);
    RETURN_IF(!matched, -1);
    atomic_fetch_add_explicit(&entry->access_count, 1, memory_order_relaxed);
    *location = entry->location;
    return 0;
}

static int warm_find_locked(tlc_core_t *core,
                            const char *key,
                            uint32_t key_len,
                            uint64_t key_hash,
                            int32_t *found_idx,
                            uint32_t *empty_pos) {
    tlc_core_warm_layer_t *warm = &core->warm;
    uint32_t slot = hash_fast(key_hash, warm->mask);
    *found_idx = -1;
    *empty_pos = UINT32_MAX;
    for (uint32_t i = 0; i < TLC_CORE_WARM_PROBES; i++) {
        uint32_t pos = (slot + i) & warm->mask;
        int32_t idx = warm->hash_table[pos];
        if (idx < 0) {
            *empty_pos = pos;
            return -1;
        }
        if ((uint32_t)idx >= warm->capacity)
            continue;
        if (warm_entry_matches(&warm->entries[idx], key, key_len, key_hash)) {
            *found_idx = idx;
            return 0;
        }
    }
    return -1;
}

static uint32_t make_lock_id(const tlc_core_warm_layer_t *warm,
                             uint64_t key_hash,
                             uint32_t *slot_out) {
    uint32_t slot = hash_fast(key_hash, warm->mask);
    if (slot_out)
        *slot_out = slot;
    return slot & warm->mask;
}

static int warm_reserve_entry(tlc_core_warm_layer_t *warm,
                              uint32_t *target_slot) {
    uint_fast32_t cur =
        atomic_load_explicit(&warm->count, memory_order_relaxed);
    while (cur < warm->capacity) {
        uint_fast32_t next = cur + 1;
        if (atomic_compare_exchange_weak_explicit(&warm->count,
                                                  &cur,
                                                  next,
                                                  memory_order_relaxed,
                                                  memory_order_relaxed)) {
            *target_slot = (uint32_t)cur;
            return 0;
        }
    }
    return -1;
}

static void warm_release_location(tlc_core_t *core,
                                  const tlc_warm_location_t *location) {
    /*
     * Warm region slots are currently allocated from a shared append-only
     * allocator. Once a slot is reserved, neither the local process nor a
     * peer can safely roll allocator->next_slot / used_slots back here.
     *
     * A later write failure therefore leaks the reserved slot by design
     * instead of attempting an unsafe cross-process rollback.
     */
    (void)core;
    (void)location;
}

static int warm_lookup(tlc_core_t *core,
                       const char *key,
                       uint32_t key_len,
                       uint64_t key_hash,
                       tlc_warm_location_t *location) {
    tlc_core_warm_layer_t *warm = &core->warm;
    uint32_t slot = 0;
    uint32_t lock_id = make_lock_id(warm, key_hash, &slot);
    int32_t found_idx = -1;
    uint32_t empty_pos = UINT32_MAX;

    __builtin_prefetch(&warm->hash_table[slot], 0, 3);
    bitmap_lock_blocking(&warm->locks, lock_id);
    int found = warm_find_locked(core, key, key_len, key_hash,
                                 &found_idx, &empty_pos) == 0;
    if (found) {
        tlc_core_warm_entry_t *entry = &warm->entries[found_idx];
        atomic_fetch_add_explicit(&entry->access_count, 1, memory_order_relaxed);
        *location = entry->location;
    }
    bitmap_unlock(&warm->locks, lock_id);

    if (found) {
        atomic_fetch_add_explicit(&warm->hits, 1, memory_order_relaxed);
        hot_put(core, key_hash, found_idx);
        return 0;
    }
    atomic_fetch_add_explicit(&warm->misses, 1, memory_order_relaxed);
    return -1;
}

static int warm_put(tlc_core_t *core,
                    const char *key,
                    uint32_t key_len,
                    uint64_t key_hash,
                    const void *value,
                    uint32_t value_size,
                    tlc_warm_location_t *location) {
    tlc_core_warm_layer_t *warm = &core->warm;
    uint32_t lock_id = make_lock_id(warm, key_hash, NULL);
    int32_t target = -1;
    uint32_t empty_pos = UINT32_MAX;
    tlc_warm_location_t loc = tlc_invalid_location;

    bitmap_lock_blocking(&warm->locks, lock_id);
    if (warm_find_locked(core, key, key_len, key_hash,
                         &target, &empty_pos) == 0) {
        loc = warm->entries[target].location;
    } else {
        GOTO_IF(empty_pos == UINT32_MAX, error);
        GOTO_IF(warm_alloc_location(core, key_hash, &loc) != 0, error);
        uint32_t target_slot = 0;
        if (warm_reserve_entry(warm, &target_slot) != 0) {
            warm_release_location(core, &loc);
            goto error;
        }
        target = (int32_t)target_slot;
        tlc_core_warm_entry_t *entry = &warm->entries[target];
        entry->key_hash = key_hash;
        entry->key_len = key_len;
        entry->value_size = value_size;
        entry->location = loc;
        memcpy(entry->key, key, key_len);
        atomic_store_explicit(&entry->access_count, 0, memory_order_relaxed);
        warm->hash_table[empty_pos] = target;
    }

    tlc_core_warm_entry_t *entry = &warm->entries[target];
    GOTO_IF(loc.region_index >= warm->region_count, error);
    tlc_core_warm_region_runtime_t *region = &warm->regions[loc.region_index];
    GOTO_IF(!region->mapped_addr ||
            loc.offset > region->region_bytes ||
            value_size > region->region_bytes - loc.offset,
            error);
    sve_streaming_store(value,
                        region->mapped_addr + loc.offset,
                        value_size);
    entry->value_size = value_size;
    loc.bytes = value_size;
    entry->location = loc;
    atomic_fetch_add_explicit(&entry->access_count, 1, memory_order_relaxed);
    atomic_store_explicit(&entry->state, TLC_CORE_ENTRY_DIRTY, memory_order_release);
    *location = loc;
    bitmap_unlock(&warm->locks, lock_id);

    hot_put(core, key_hash, target);
    return 0;

error:
    serverLog(LL_WARNING,
              "tlc warm put failed: key_hash=%llu key_len=%u value_size=%u target=%d empty_pos=%u region_id=%u region_index=%u local_slot=%u offset=%llu warm_count=%u warm_capacity=%u",
              (unsigned long long)key_hash,
              key_len,
              value_size,
              target,
              empty_pos,
              loc.region_id,
              loc.region_index,
              loc.local_slot,
              (unsigned long long)loc.offset,
              (unsigned)atomic_load_explicit(&warm->count, memory_order_relaxed),
              warm->capacity);
    bitmap_unlock(&warm->locks, lock_id);
    return -1;
}

static int cold_alloc_segment(tlc_core_t *core, uint32_t seg_id) {
    tlc_core_cold_layer_t *cold = &core->cold;
    RETURN_IF(seg_id >= cold->max_segments, -1);
    tlc_core_cold_segment_t *seg = &cold->segments[seg_id];
    RETURN_IF(seg->records && seg->values, 0);
    seg->capacity = cold->segment_capacity;
    seg->base_offset = (uint64_t)seg_id * cold->segment_capacity;
    seg->records = zcalloc(sizeof(*seg->records) * seg->capacity);
    seg->values = zcalloc((size_t)core->value_size * seg->capacity);
    RETURN_IF(!seg->records || !seg->values, -1);
    return 0;
}

static int cold_init(tlc_core_t *core,
                     uint32_t max_segments,
                     uint32_t segment_records) {
    tlc_core_cold_layer_t *cold = &core->cold;
    cold->max_segments = max_segments ?
        max_segments : TLC_CORE_DEFAULT_COLD_MAX_SEGMENTS;
    cold->segment_capacity = segment_records;
    if (cold->segment_capacity == 0 ||
        cold->segment_capacity > TLC_CORE_DEFAULT_COLD_SEGMENT_RECORDS) {
        cold->segment_capacity = TLC_CORE_DEFAULT_COLD_SEGMENT_RECORDS;
    }
    cold->segments =
        zcalloc(sizeof(*cold->segments) * cold->max_segments);
    RETURN_IF(!cold->segments, -1);
    cold->offset_index_size = pow2_ceil_u32((uint64_t)core->warm_capacity * 2u);
    if (cold->offset_index_size < 1024)
        cold->offset_index_size = 1024;
    cold->oi_mask = cold->offset_index_size - 1u;
    cold->offset_index =
        zcalloc(sizeof(*cold->offset_index) * cold->offset_index_size);
    RETURN_IF(!cold->offset_index, -1);
    for (uint32_t i = 0; i < cold->offset_index_size; i++)
        atomic_init(&cold->offset_index[i], TLC_CORE_INVALID_OFFSET);
    atomic_init(&cold->num_segments, 1);
    atomic_init(&cold->next_offset, 0);
    atomic_init(&cold->hits, 0);
    atomic_init(&cold->misses, 0);
    int locks_rc = bitmap_init(&cold->locks, cold->max_segments);
    RETURN_IF(locks_rc != 0, -1);
    return cold_alloc_segment(core, 0);
}

static tlc_core_cold_record_t *cold_record_by_offset(tlc_core_t *core,
                                                     uint64_t offset,
                                                     const uint8_t **value) {
    tlc_core_cold_layer_t *cold = &core->cold;
    uint32_t seg_id = (uint32_t)(offset / cold->segment_capacity);
    size_t local = (size_t)(offset % cold->segment_capacity);
    RETURN_IF(seg_id >= cold->max_segments, NULL);
    tlc_core_cold_segment_t *seg = &cold->segments[seg_id];
    RETURN_IF(!seg->records || !seg->values || local >= seg->count, NULL);
    if (value)
        *value = seg->values + local * core->value_size;
    return &seg->records[local];
}

static int cold_offset_matches(tlc_core_t *core,
                               uint64_t offset,
                               const char *key,
                               uint32_t key_len,
                               uint64_t key_hash) {
    const uint8_t *value = NULL;
    tlc_core_cold_record_t *record =
        cold_record_by_offset(core, offset, &value);
    (void)value;
    return record &&
           key_matches(key_hash, key, key_len,
                       record->key_hash, record->key, record->key_len);
}

static void cold_index_store(tlc_core_t *core,
                             const char *key,
                             uint32_t key_len,
                             uint64_t key_hash,
                             uint64_t offset) {
    tlc_core_cold_layer_t *cold = &core->cold;
    uint32_t slot = hash_fast(key_hash, cold->oi_mask);
    for (uint32_t i = 0; i < TLC_CORE_COLD_PROBES; i++) {
        uint32_t pos = (slot + i) & cold->oi_mask;
        uint64_t cur =
            atomic_load_explicit(&cold->offset_index[pos],
                                 memory_order_acquire);
        if (cur == TLC_CORE_INVALID_OFFSET ||
            cold_offset_matches(core, cur, key, key_len, key_hash)) {
            atomic_store_explicit(&cold->offset_index[pos],
                                  offset,
                                  memory_order_release);
            return;
        }
    }
    atomic_store_explicit(&cold->offset_index[slot],
                          offset,
                          memory_order_release);
}

static int cold_append(tlc_core_t *core,
                       const char *key,
                       uint32_t key_len,
                       uint64_t key_hash,
                       const void *value,
                       uint32_t value_size) {
    tlc_core_cold_layer_t *cold = &core->cold;
    bitmap_lock_blocking(&cold->locks, 0);
    uint64_t offset =
        atomic_load_explicit(&cold->next_offset, memory_order_relaxed);
    uint32_t seg_id = (uint32_t)(offset / cold->segment_capacity);
    size_t local = (size_t)(offset % cold->segment_capacity);
    if (seg_id >= cold->max_segments ||
        cold_alloc_segment(core, seg_id) != 0) {
        bitmap_unlock(&cold->locks, 0);
        return -1;
    }
    int current_segments =
        atomic_load_explicit(&cold->num_segments, memory_order_relaxed);
    if ((int)seg_id >= current_segments) {
        atomic_store_explicit(&cold->num_segments, (int)seg_id + 1,
                              memory_order_relaxed);
    }
    tlc_core_cold_segment_t *seg = &cold->segments[seg_id];
    tlc_core_cold_record_t *record = &seg->records[local];
    record->key_hash = key_hash;
    record->offset = offset;
    record->key_len = key_len;
    record->value_size = value_size;
    memcpy(record->key, key, key_len);
    memcpy(seg->values + local * core->value_size, value, value_size);
    if (local >= seg->count)
        seg->count = local + 1;
    cold_index_store(core, key, key_len, key_hash, offset);
    atomic_store_explicit(&cold->next_offset, offset + 1,
                          memory_order_release);
    bitmap_unlock(&cold->locks, 0);
    return 0;
}

static int cold_lookup(tlc_core_t *core,
                       const char *key,
                       uint32_t key_len,
                       uint64_t key_hash,
                       const uint8_t **value,
                       uint32_t *value_size) {
    tlc_core_cold_layer_t *cold = &core->cold;
    uint32_t slot = hash_fast(key_hash, cold->oi_mask);
    for (uint32_t i = 0; i < TLC_CORE_COLD_PROBES; i++) {
        uint32_t pos = (slot + i) & cold->oi_mask;
        uint64_t offset =
            atomic_load_explicit(&cold->offset_index[pos],
                                 memory_order_acquire);
        if (offset == TLC_CORE_INVALID_OFFSET)
            break;
        const uint8_t *cold_value = NULL;
        tlc_core_cold_record_t *record =
            cold_record_by_offset(core, offset, &cold_value);
        if (record &&
            key_matches(key_hash, key, key_len,
                        record->key_hash, record->key, record->key_len)) {
            *value = cold_value;
            *value_size = record->value_size;
            atomic_fetch_add_explicit(&cold->hits, 1, memory_order_relaxed);
            return 0;
        }
    }
    atomic_fetch_add_explicit(&cold->misses, 1, memory_order_relaxed);
    return -1;
}

int tlc_core_create(tlc_core_t **out, const tlc_core_config_t *config) {
    RETURN_IF(!out || !config || config->value_size == 0 ||
              config->warm_capacity == 0 ||
              config->warm_capacity > INT32_MAX ||
              !config->warm_regions ||
              config->warm_region_count == 0,
              -1);

    tlc_core_t *core = zcalloc(sizeof(*core));
    RETURN_IF(!core, -1);
    core->value_size = config->value_size;
    core->warm_capacity = config->warm_capacity;
    atomic_init(&core->total_reads, 0);
    atomic_init(&core->total_writes, 0);
    atomic_init(&core->read_throughs, 0);
    atomic_init(&core->write_throughs, 0);
    atomic_init(&core->warm_alloc_local, 0);
    atomic_init(&core->warm_alloc_remote, 0);
    atomic_init(&core->warm_alloc_fallback, 0);
    atomic_init(&core->warm_alloc_cold_spill, 0);
    atomic_init(&core->warm_alloc_fail, 0);

    if (hot_init(core, config->hot_capacity) != 0 ||
        warm_init(core, config) != 0 ||
        cold_init(core,
                  config->cold_max_segments,
                  config->cold_segment_records) != 0) {
        tlc_core_destroy(core);
        return -1;
    }

    *out = core;
    return 0;
}

void tlc_core_destroy(tlc_core_t *core) {
    bitmap_destroy(&core->warm.locks);
    bitmap_destroy(&core->cold.locks);
    if (core->hold.table) zfree(core->hold.table);
    if (core->warm.entries) zfree(core->warm.entries);
    if (core->warm.hash_table) zfree(core->warm.hash_table);
    if (core->warm.regions) zfree(core->warm.regions);
    if (core->warm.vnodes) zfree(core->warm.vnodes);
    if (core->cold.segments) {
        for (uint32_t i = 0; i < core->cold.max_segments; i++) {
            if (core->cold.segments[i].records)
                zfree(core->cold.segments[i].records);
            if (core->cold.segments[i].values)
                zfree(core->cold.segments[i].values);
        }
    }
    if (core->cold.segments) zfree(core->cold.segments);
    if (core->cold.offset_index) zfree(core->cold.offset_index);
    zfree(core);
}

int tlc_core_get_warm_slot(tlc_core_t *core,
                           const char *key,
                           uint32_t key_len,
                           uint64_t key_hash,
                           uint32_t *warm_slot) {
    tlc_warm_location_t location = tlc_invalid_location;
    int rc = tlc_core_get_warm_location(core, key, key_len, key_hash,
                                        &location);
    if (rc == 0 && warm_slot)
        *warm_slot = location.local_slot;
    return rc;
}

int tlc_core_get_warm_location(tlc_core_t *core,
                               const char *key,
                               uint32_t key_len,
                               uint64_t key_hash,
                               tlc_warm_location_t *location) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!valid_key, -1);

    atomic_fetch_add_explicit(&core->total_reads, 1, memory_order_relaxed);
    int32_t hot_idx = hot_get(core, key_hash);
    if (warm_validate_idx(core, hot_idx, key, key_len, key_hash, location) == 0) {
        return 0;
    }
    if (warm_lookup(core, key, key_len, key_hash, location) == 0) {
        return 0;
    }

    const uint8_t *cold_value = NULL;
    uint32_t cold_value_size = 0;
    int cold_rc = cold_lookup(core, key, key_len, key_hash,
                              &cold_value, &cold_value_size);
    RETURN_IF(cold_rc != 0, -1);
    RETURN_IF(cold_value_size != core->value_size, -1);
    int warm_rc = warm_put(core, key, key_len, key_hash,
                           cold_value, cold_value_size, location);
    RETURN_IF(warm_rc != 0, -1);
    atomic_fetch_add_explicit(&core->read_throughs, 1, memory_order_relaxed);
    return 0;
}

int tlc_core_put(tlc_core_t *core,
                 const char *key,
                 uint32_t key_len,
                 uint64_t key_hash,
                 const void *value,
                 uint32_t value_size,
                 uint32_t *warm_slot) {
    tlc_warm_location_t location = tlc_invalid_location;
    int rc = tlc_core_put_location(core, key, key_len, key_hash, value,
                                   value_size, &location);
    if (rc == 0 && warm_slot)
        *warm_slot = location.local_slot;
    return rc;
}

int tlc_core_put_location(tlc_core_t *core,
                          const char *key,
                          uint32_t key_len,
                          uint64_t key_hash,
                          const void *value,
                          uint32_t value_size,
                          tlc_warm_location_t *location) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!valid_key || value_size != core->value_size, -1);

    atomic_fetch_add_explicit(&core->total_writes, 1, memory_order_relaxed);
    if (warm_put(core, key, key_len, key_hash,
                 value, value_size, location) == 0) {
        return 0;
    }

    int cold_rc = cold_append(core, key, key_len, key_hash, value, value_size);
    RETURN_IF(cold_rc != 0, -1);
    atomic_fetch_add_explicit(&core->write_throughs, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&core->warm_alloc_cold_spill, 1,
                              memory_order_relaxed);
    *location = tlc_invalid_location;
    return 0;
}

int tlc_core_cold_append(tlc_core_t *core,
                         const char *key,
                         uint32_t key_len,
                         uint64_t key_hash,
                         const void *value,
                         uint32_t value_size) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!valid_key || value_size != core->value_size, -1);
    return cold_append(core, key, key_len, key_hash, value, value_size);
}

void tlc_core_get_stats(tlc_core_t *core, tlc_core_stats_t *stats) {
    memset(stats, 0, sizeof(*stats));
    tlc_core_warm_layer_t *warm = &core->warm;
    stats->warm_region_count = warm->region_count;
    for (uint32_t i = 0; i < warm->region_count; i++) {
        if (vemb_v16_shared_allocator_full(warm->regions[i].shared_allocator)) {
            stats->warm_region_full_count++;
        }
    }
    stats->warm_alloc_local =
        atomic_load_explicit(&core->warm_alloc_local, memory_order_relaxed);
    stats->warm_alloc_remote =
        atomic_load_explicit(&core->warm_alloc_remote, memory_order_relaxed);
    stats->warm_alloc_fallback =
        atomic_load_explicit(&core->warm_alloc_fallback, memory_order_relaxed);
    stats->warm_alloc_cold_spill =
        atomic_load_explicit(&core->warm_alloc_cold_spill, memory_order_relaxed);
    stats->warm_alloc_fail =
        atomic_load_explicit(&core->warm_alloc_fail, memory_order_relaxed);
    uint64_t total = stats->warm_alloc_local + stats->warm_alloc_remote;
    if (total)
        stats->warm_region_hash_local_pct =
            (stats->warm_alloc_local * 100u) / total;
}

uint32_t tlc_core_get_region_stats(tlc_core_t *core,
                                   tlc_core_region_stats_t *regions,
                                   uint32_t max_regions) {
    tlc_core_warm_layer_t *warm = &core->warm;
    uint32_t n = warm->region_count;
    RETURN_IF (!regions || max_regions == 0, n);
    if (n > max_regions)
        n = max_regions;
    for (uint32_t i = 0; i < n; i++) {
        tlc_core_warm_region_runtime_t *region = &warm->regions[i];
        regions[i] = (tlc_core_region_stats_t){
            .region_id = region->region_id,
            .is_local = region->is_local,
            .full =
                vemb_v16_shared_allocator_full(region->shared_allocator),
            .capacity_slots = region->capacity_slots,
            .used_slots =
                vemb_v16_shared_allocator_used_slots(region->shared_allocator),
        };
    }
    return n;
}
