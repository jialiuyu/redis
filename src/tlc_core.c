#include "tlc_core.h"
#include "cpu_relax.h"
#include "macro.h"
#include "sve_operation.h"
#include "vemb_v16_hash.h"
#include "vemb_v16_log.h"
#include "vemb_v16_protocol.h"
#include "zmalloc.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define TLC_CORE_HOT_PROBES 4u
/*
 * The warm/cold hash tables stay intentionally sparse, but realistic bench
 * keysets can still create 10+ entry linear-probe runs. A slightly larger
 * probe window avoids spilling valid warm keys into the cold path and later
 * surfacing them as false NOT_FOUND responses.
 */
#define TLC_CORE_COLD_PROBES 16u
#define TLC_CORE_INVALID_OFFSET UINT64_MAX
#define TLC_CORE_WARM_WAYS 8u
#define TLC_CORE_WARM_PLACE_RETRIES 3u
#define TLC_CORE_DIAG_STALE_LOG_LIMIT 64u

static atomic_uint_fast32_t tlc_core_stale_diag_logs;

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

typedef struct tlc_core_key_meta_entry {
    int occupied;
    uint32_t key_len;
    uint64_t key_hash;
    uint64_t key_version;
    uint64_t topology_epoch;
    uint64_t owner_epoch;
    uint32_t migration_state;
    uint32_t source_owner;
    uint32_t target_owner;
    uint32_t tombstone;
    uint32_t shard_id;
    tlc_warm_location_t location;
    char key[VEMB_V16_MAX_KEY_LEN];
} tlc_core_key_meta_entry_t;

typedef struct tlc_core_warm_region_runtime {
    uint32_t region_id;
    uint32_t backend_type;
    uint32_t is_local;
    uint32_t weight;
    uint32_t value_size;
    uint32_t capacity_slots;
    vemb_v16_shared_region_allocator_t *shared_allocator;
    vemb_v16_warm_slot_meta_t *slot_meta;
    int owns_slot_meta;
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
    atomic_uint_fast64_t warm_eviction_success;
    atomic_uint_fast64_t warm_eviction_fail;
    atomic_uint_fast64_t warm_same_key_overwrite;
    atomic_uint_fast64_t warm_stale_handle_reject;
    atomic_uint_fast64_t remote_meta_stale;
    atomic_uint_fast64_t source_fence_active_count;
    atomic_uint_fast64_t tombstone_active_count;
    pthread_mutex_t key_meta_lock;
    tlc_core_key_meta_entry_t *key_meta;
    uint32_t key_meta_capacity;
    uint32_t key_meta_mask;
    uint32_t key_meta_count;
};

static const tlc_warm_location_t tlc_invalid_location = {
    .region_id = TLC_CORE_INVALID_REGION_ID,
    .region_index = UINT32_MAX,
    .local_slot = TLC_CORE_INVALID_SLOT,
    .bytes = 0,
    .offset = UINT64_MAX,
    .owner_generation = 0,
};

typedef struct tlc_core_slot_meta_registry_entry {
    vemb_v16_shared_region_allocator_t *allocator;
    vemb_v16_warm_slot_meta_t *slot_meta;
    uint32_t capacity_slots;
    uint32_t refcount;
} tlc_core_slot_meta_registry_entry_t;

#define TLC_CORE_SLOT_META_REGISTRY_MAX 128u

static pthread_mutex_t slot_meta_registry_lock = PTHREAD_MUTEX_INITIALIZER;
static tlc_core_slot_meta_registry_entry_t
    slot_meta_registry[TLC_CORE_SLOT_META_REGISTRY_MAX];

static uint64_t monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void init_runtime_slot_meta(vemb_v16_warm_slot_meta_t *slots,
                                   uint32_t region_id,
                                   uint32_t capacity_slots) {
    memset(slots, 0, (size_t)capacity_slots * sizeof(*slots));
    for (uint32_t i = 0; i < capacity_slots; i++) {
        slots[i].region_id = region_id;
        slots[i].local_slot = i;
        atomic_init(&slots[i].state, VEMB_V16_WARM_SLOT_FREE);
        atomic_init(&slots[i].owner_generation, 0);
        atomic_init(&slots[i].write_seq, 0);
        atomic_init(&slots[i].last_access_ns, 0);
        atomic_init(&slots[i].clock_bit, 0);
        atomic_init(&slots[i].cold_state, VEMB_V16_WARM_SLOT_COLD_NONE);
    }
}

static vemb_v16_warm_slot_meta_t *fallback_slot_meta_acquire(
    vemb_v16_shared_region_allocator_t *allocator,
    uint32_t region_id,
    uint32_t capacity_slots) {
    pthread_mutex_lock(&slot_meta_registry_lock);
    for (uint32_t i = 0; i < TLC_CORE_SLOT_META_REGISTRY_MAX; i++) {
        tlc_core_slot_meta_registry_entry_t *entry =
            &slot_meta_registry[i];
        if (entry->allocator == allocator) {
            if (entry->capacity_slots != capacity_slots) {
                pthread_mutex_unlock(&slot_meta_registry_lock);
                return NULL;
            }
            entry->refcount++;
            vemb_v16_warm_slot_meta_t *slots = entry->slot_meta;
            pthread_mutex_unlock(&slot_meta_registry_lock);
            return slots;
        }
    }

    for (uint32_t i = 0; i < TLC_CORE_SLOT_META_REGISTRY_MAX; i++) {
        tlc_core_slot_meta_registry_entry_t *entry =
            &slot_meta_registry[i];
        if (!entry->allocator) {
            vemb_v16_warm_slot_meta_t *slots =
                zcalloc((size_t)capacity_slots * sizeof(*slots));
            if (!slots) {
                pthread_mutex_unlock(&slot_meta_registry_lock);
                return NULL;
            }
            init_runtime_slot_meta(slots, region_id, capacity_slots);
            *entry = (tlc_core_slot_meta_registry_entry_t){
                .allocator = allocator,
                .slot_meta = slots,
                .capacity_slots = capacity_slots,
                .refcount = 1,
            };
            pthread_mutex_unlock(&slot_meta_registry_lock);
            return slots;
        }
    }

    pthread_mutex_unlock(&slot_meta_registry_lock);
    return NULL;
}

static void fallback_slot_meta_release(vemb_v16_shared_region_allocator_t *allocator,
                                       vemb_v16_warm_slot_meta_t *slot_meta) {
    if (!allocator || !slot_meta)
        return;
    pthread_mutex_lock(&slot_meta_registry_lock);
    for (uint32_t i = 0; i < TLC_CORE_SLOT_META_REGISTRY_MAX; i++) {
        tlc_core_slot_meta_registry_entry_t *entry =
            &slot_meta_registry[i];
        if (entry->allocator == allocator && entry->slot_meta == slot_meta) {
            if (entry->refcount > 0)
                entry->refcount--;
            if (entry->refcount == 0) {
                zfree(entry->slot_meta);
                memset(entry, 0, sizeof(*entry));
            }
            break;
        }
    }
    pthread_mutex_unlock(&slot_meta_registry_lock);
}

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

static int key_meta_init(tlc_core_t *core) {
    uint32_t capacity =
        pow2_ceil_u32((uint64_t)core->warm_capacity * 2u);
    if (capacity < 1024)
        capacity = 1024;
    core->key_meta =
        zcalloc(sizeof(*core->key_meta) * capacity);
    RETURN_IF(!core->key_meta, -1);
    core->key_meta_capacity = capacity;
    core->key_meta_mask = capacity - 1u;
    core->key_meta_count = 0;
    return 0;
}

static tlc_core_key_meta_entry_t *key_meta_find_locked(
        tlc_core_t *core,
        const char *key,
        uint32_t key_len,
        uint64_t key_hash) {
    if (!core->key_meta)
        return NULL;
    uint32_t slot = hash_fast(key_hash, core->key_meta_mask);
    for (uint32_t i = 0; i < core->key_meta_capacity; i++) {
        tlc_core_key_meta_entry_t *entry =
            &core->key_meta[(slot + i) & core->key_meta_mask];
        if (!entry->occupied)
            return NULL;
        if (key_matches(key_hash, key, key_len,
                        entry->key_hash, entry->key, entry->key_len)) {
            return entry;
        }
    }
    return NULL;
}

static tlc_core_key_meta_entry_t *key_meta_find_or_create_locked(
        tlc_core_t *core,
        const char *key,
        uint32_t key_len,
        uint64_t key_hash) {
    if (!core->key_meta)
        return NULL;
    uint32_t slot = hash_fast(key_hash, core->key_meta_mask);
    for (uint32_t i = 0; i < core->key_meta_capacity; i++) {
        tlc_core_key_meta_entry_t *entry =
            &core->key_meta[(slot + i) & core->key_meta_mask];
        if (entry->occupied) {
            if (key_matches(key_hash, key, key_len,
                            entry->key_hash, entry->key, entry->key_len)) {
                return entry;
            }
            continue;
        }
        if (core->key_meta_count >= core->key_meta_capacity)
            return NULL;
        entry->occupied = 1;
        entry->key_hash = key_hash;
        entry->key_len = key_len;
        memcpy(entry->key, key, key_len);
        entry->key_version = 0;
        entry->topology_epoch = 0;
        entry->owner_epoch = 0;
        entry->migration_state = TLC_CORE_KEY_SOURCE_ACTIVE;
        entry->source_owner = UINT32_MAX;
        entry->target_owner = UINT32_MAX;
        entry->tombstone = 0;
        entry->shard_id = 0;
        entry->location = tlc_invalid_location;
        core->key_meta_count++;
        return entry;
    }
    return NULL;
}

static void key_meta_fill_info(const tlc_core_key_meta_entry_t *entry,
                               tlc_core_key_migration_info_t *info) {
    if (!info)
        return;
    *info = (tlc_core_key_migration_info_t){
        .key_hash = entry->key_hash,
        .key_version = entry->key_version,
        .topology_epoch = entry->topology_epoch,
        .owner_epoch = entry->owner_epoch,
        .migration_state = entry->migration_state,
        .source_owner = entry->source_owner,
        .target_owner = entry->target_owner,
        .tombstone = entry->tombstone,
        .shard_id = entry->shard_id,
        .location = entry->location,
    };
}

static int incoming_snapshot_is_stale(
        const tlc_core_key_meta_entry_t *entry,
        const tlc_core_migration_snapshot_t *snapshot) {
    if (!entry || !snapshot)
        return 0;
    if (snapshot->owner_epoch < entry->owner_epoch)
        return 1;
    if (snapshot->topology_epoch < entry->topology_epoch)
        return 1;
    if (snapshot->topology_epoch == entry->topology_epoch &&
        snapshot->key_version < entry->key_version) {
        return 1;
    }
    return 0;
}

static int incoming_snapshot_is_duplicate(
        const tlc_core_key_meta_entry_t *entry,
        const tlc_core_migration_snapshot_t *snapshot) {
    return entry &&
           snapshot &&
           snapshot->owner_epoch == entry->owner_epoch &&
           snapshot->topology_epoch == entry->topology_epoch &&
           snapshot->key_version == entry->key_version;
}

static int key_meta_state_blocks_source_access(uint32_t migration_state) {
    return migration_state == TLC_CORE_KEY_CUTOVER ||
           migration_state == TLC_CORE_KEY_SOURCE_GC;
}

int tlc_core_source_fence_active(const tlc_core_t *core) {
    if (!core)
        return 0;
    return atomic_load_explicit(&core->source_fence_active_count,
                                memory_order_acquire) != 0;
}

static int tombstone_filter_active(const tlc_core_t *core) {
    if (!core)
        return 0;
    return atomic_load_explicit(&core->tombstone_active_count,
                                memory_order_acquire) != 0;
}

static void key_meta_note_source_fence_transition(tlc_core_t *core,
                                                  uint32_t old_state,
                                                  uint32_t new_state) {
    if (!key_meta_state_blocks_source_access(old_state) &&
        key_meta_state_blocks_source_access(new_state)) {
        atomic_fetch_add_explicit(&core->source_fence_active_count,
                                  1,
                                  memory_order_release);
    }
}

static void key_meta_set_tombstone_locked(tlc_core_t *core,
                                          tlc_core_key_meta_entry_t *meta,
                                          uint32_t tombstone) {
    uint32_t new_value = tombstone ? 1u : 0u;
    if (meta->tombstone == new_value)
        return;
    if (new_value) {
        atomic_fetch_add_explicit(&core->tombstone_active_count,
                                  1,
                                  memory_order_release);
        meta->tombstone = 1;
    } else {
        meta->tombstone = 0;
        atomic_fetch_sub_explicit(&core->tombstone_active_count,
                                  1,
                                  memory_order_release);
    }
}

static int key_meta_blocks_source_access(tlc_core_t *core,
                                         const char *key,
                                         uint32_t key_len,
                                         uint64_t key_hash) {
    int source_fence_active = tlc_core_source_fence_active(core);
    if (!source_fence_active && !tombstone_filter_active(core))
        return 0;

    int blocked = 0;
    pthread_mutex_lock(&core->key_meta_lock);
    tlc_core_key_meta_entry_t *meta =
        key_meta_find_locked(core, key, key_len, key_hash);
    blocked = meta &&
        (meta->tombstone ||
         (source_fence_active &&
          key_meta_state_blocks_source_access(meta->migration_state)));
    pthread_mutex_unlock(&core->key_meta_lock);
    return blocked;
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
        warm->regions[i].slot_meta =
            vemb_v16_shared_allocator_slot_meta(src->shared_allocator);
        if (!warm->regions[i].slot_meta) {
            warm->regions[i].slot_meta =
                fallback_slot_meta_acquire(src->shared_allocator,
                                           src->region_id,
                                           (uint32_t)capacity_slots);
            warm->regions[i].owns_slot_meta = 1;
        }
        RETURN_IF(!warm->regions[i].slot_meta, -1);
        serverLog(LL_NOTICE,
                  "tlc warm region init: region_index=%u region_id=%u backend_type=%u is_local=%u weight=%u effective_weight=%u value_size=%u region_bytes=%llu capacity_slots=%u mapped_addr=%p shared_allocator=%p slot_meta=%p slot_meta_shared=%u",
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
                  (void *)warm->regions[i].shared_allocator,
                  (void *)warm->regions[i].slot_meta,
                  warm->regions[i].owns_slot_meta ? 0u : 1u);
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

static uint64_t key_fingerprint(const char *key, uint32_t key_len) {
    return vemb_v16_fnv1a64_bytes(key, key_len);
}

static uint32_t region_ways(const tlc_core_warm_region_runtime_t *region) {
    return region->capacity_slots < TLC_CORE_WARM_WAYS ?
        region->capacity_slots : TLC_CORE_WARM_WAYS;
}

static void region_set_bounds(const tlc_core_warm_region_runtime_t *region,
                              uint64_t key_hash,
                              uint32_t *start,
                              uint32_t *end) {
    uint32_t ways = region_ways(region);
    uint32_t set_count =
        (region->capacity_slots + ways - 1u) / ways;
    uint32_t set = set_count ? mix32(key_hash) % set_count : 0;
    *start = set * ways;
    *end = *start + ways;
    if (*end > region->capacity_slots)
        *end = region->capacity_slots;
}

static int slot_seq_try_begin(vemb_v16_warm_slot_meta_t *meta,
                              uint64_t *old_seq) {
    uint64_t seq = atomic_load_explicit(&meta->write_seq,
                                        memory_order_acquire);
    if (seq & 1u)
        return -1;
    uint64_t expected = seq;
    if (!atomic_compare_exchange_strong_explicit(&meta->write_seq,
                                                 &expected,
                                                 seq + 1u,
                                                 memory_order_acq_rel,
                                                 memory_order_acquire)) {
        return -1;
    }
    *old_seq = seq;
    return 0;
}

static void slot_write_payload(tlc_core_warm_region_runtime_t *region,
                               uint32_t local_slot,
                               const void *value,
                               uint32_t value_size) {
    sve_streaming_store(value,
                        region->mapped_addr +
                            (uint64_t)local_slot * region->value_size,
                        value_size);
}

static void slot_publish_ready(vemb_v16_warm_slot_meta_t *meta,
                               uint64_t old_seq) {
    atomic_store_explicit(&meta->write_seq, old_seq + 2u,
                          memory_order_release);
    atomic_store_explicit(&meta->state, VEMB_V16_WARM_SLOT_READY,
                          memory_order_release);
}

static void fill_location_from_slot(const tlc_core_warm_region_runtime_t *region,
                                    uint32_t region_index,
                                    const vemb_v16_warm_slot_meta_t *meta,
                                    uint32_t local_slot,
                                    tlc_warm_location_t *location) {
    *location = (tlc_warm_location_t){
        .region_id = region->region_id,
        .region_index = region_index,
        .local_slot = local_slot,
        .bytes = meta->bytes,
        .offset = (uint64_t)local_slot * region->value_size,
        .owner_generation =
            atomic_load_explicit(&meta->owner_generation,
                                 memory_order_acquire),
    };
}

static int warm_slot_verify_ready(tlc_core_warm_region_runtime_t *region,
                                  uint32_t region_index,
                                  uint32_t local_slot,
                                  uint64_t key_hash,
                                  uint64_t fp,
                                  uint32_t expected_bytes,
                                  uint64_t expected_generation,
                                  tlc_warm_location_t *location) {
    if (local_slot >= region->capacity_slots)
        return -1;
    vemb_v16_warm_slot_meta_t *meta = &region->slot_meta[local_slot];
    uint32_t state = atomic_load_explicit(&meta->state,
                                          memory_order_acquire);
    if (state != VEMB_V16_WARM_SLOT_READY)
        return -1;
    uint64_t seq1 = atomic_load_explicit(&meta->write_seq,
                                         memory_order_acquire);
    if (seq1 & 1u)
        return -1;
    uint64_t owner_generation =
        atomic_load_explicit(&meta->owner_generation,
                             memory_order_acquire);
    uint32_t bytes = meta->bytes;
    uint64_t slot_key_hash = meta->key_hash;
    uint64_t slot_fp = meta->key_fingerprint;
    atomic_thread_fence(memory_order_acquire);
    uint64_t seq2 = atomic_load_explicit(&meta->write_seq,
                                         memory_order_acquire);
    if (seq1 != seq2 || (seq2 & 1u))
        return -1;
    if (slot_key_hash != key_hash || (fp != 0 && slot_fp != fp) ||
        bytes != expected_bytes ||
        (expected_generation != 0 &&
         owner_generation != expected_generation)) {
        return -1;
    }
    atomic_store_explicit(&meta->last_access_ns, monotonic_ns(),
                          memory_order_relaxed);
    atomic_store_explicit(&meta->clock_bit, 1, memory_order_relaxed);
    if (location) {
        *location = (tlc_warm_location_t){
            .region_id = region->region_id,
            .region_index = region_index,
            .local_slot = local_slot,
            .bytes = bytes,
            .offset = (uint64_t)local_slot * region->value_size,
            .owner_generation = owner_generation,
        };
    }
    return 0;
}

static int warm_lookup_region(tlc_core_warm_region_runtime_t *region,
                              uint32_t region_index,
                              uint64_t key_hash,
                              uint64_t fp,
                              uint32_t expected_bytes,
                              tlc_warm_location_t *location) {
    uint32_t start = 0, end = 0;
    region_set_bounds(region, key_hash, &start, &end);
    for (uint32_t slot = start; slot < end; slot++) {
        if (warm_slot_verify_ready(region, region_index, slot,
                                   key_hash, fp, expected_bytes, 0,
                                   location) == 0) {
            return 0;
        }
    }
    return -1;
}

static int warm_try_overwrite_same(tlc_core_warm_region_runtime_t *region,
                                   tlc_core_t *core,
                                   uint32_t region_index,
                                   uint32_t slot,
                                   uint64_t key_hash,
                                   uint64_t fp,
                                   const void *value,
                                   uint32_t value_size,
                                   tlc_warm_location_t *location) {
    vemb_v16_warm_slot_meta_t *meta = &region->slot_meta[slot];
    if (atomic_load_explicit(&meta->state, memory_order_acquire) !=
        VEMB_V16_WARM_SLOT_READY) {
        return -1;
    }
    if (meta->key_hash != key_hash || meta->key_fingerprint != fp)
        return -1;
    uint64_t old_seq = 0;
    if (slot_seq_try_begin(meta, &old_seq) != 0)
        return -1;
    atomic_fetch_add_explicit(&core->warm_same_key_overwrite, 1,
                              memory_order_relaxed);
    slot_write_payload(region, slot, value, value_size);
    meta->bytes = value_size;
    atomic_store_explicit(&meta->cold_state,
                          VEMB_V16_WARM_SLOT_COLD_COMMITTED,
                          memory_order_release);
    atomic_store_explicit(&meta->last_access_ns, monotonic_ns(),
                          memory_order_relaxed);
    atomic_store_explicit(&meta->clock_bit, 1, memory_order_relaxed);
    atomic_store_explicit(&meta->write_seq, old_seq + 2u,
                          memory_order_release);
    fill_location_from_slot(region, region_index, meta, slot, location);
    return 0;
}

static int warm_try_fill_free(tlc_core_warm_region_runtime_t *region,
                              uint32_t region_index,
                              uint32_t slot,
                              uint64_t key_hash,
                              uint64_t fp,
                              const void *value,
                              uint32_t value_size,
                              tlc_warm_location_t *location) {
    vemb_v16_warm_slot_meta_t *meta = &region->slot_meta[slot];
    uint32_t expected = VEMB_V16_WARM_SLOT_FREE;
    if (!atomic_compare_exchange_strong_explicit(&meta->state,
                                                 &expected,
                                                 VEMB_V16_WARM_SLOT_FILLING,
                                                 memory_order_acq_rel,
                                                 memory_order_acquire)) {
        return -1;
    }
    uint64_t old_seq = atomic_load_explicit(&meta->write_seq,
                                            memory_order_relaxed);
    if ((old_seq & 1u) == 0)
        old_seq++;
    atomic_store_explicit(&meta->write_seq, old_seq, memory_order_release);
    uint64_t owner_generation =
        atomic_fetch_add_explicit(&meta->owner_generation, 1,
                                  memory_order_acq_rel) + 1u;
    meta->region_id = region->region_id;
    meta->local_slot = slot;
    meta->bytes = value_size;
    meta->key_hash = key_hash;
    meta->key_fingerprint = fp;
    atomic_store_explicit(&meta->cold_state,
                          VEMB_V16_WARM_SLOT_COLD_COMMITTED,
                          memory_order_release);
    atomic_store_explicit(&meta->last_access_ns, monotonic_ns(),
                          memory_order_relaxed);
    atomic_store_explicit(&meta->clock_bit, 1, memory_order_relaxed);
    slot_write_payload(region, slot, value, value_size);
    atomic_store_explicit(&meta->write_seq, old_seq + 1u,
                          memory_order_release);
    atomic_store_explicit(&meta->state, VEMB_V16_WARM_SLOT_READY,
                          memory_order_release);
    uint32_t used =
        atomic_fetch_add_explicit(&region->shared_allocator->used_slots, 1,
                                  memory_order_relaxed) + 1u;
    if (used >= region->capacity_slots)
        atomic_store_explicit(&region->shared_allocator->full, 1,
                              memory_order_release);
    *location = (tlc_warm_location_t){
        .region_id = region->region_id,
        .region_index = region_index,
        .local_slot = slot,
        .bytes = value_size,
        .offset = (uint64_t)slot * region->value_size,
        .owner_generation = owner_generation,
    };
    return 0;
}

static int warm_slot_can_evict(vemb_v16_warm_slot_meta_t *meta) {
    if (atomic_load_explicit(&meta->state, memory_order_acquire) !=
        VEMB_V16_WARM_SLOT_READY) {
        return 0;
    }
    if (atomic_load_explicit(&meta->cold_state, memory_order_acquire) !=
        VEMB_V16_WARM_SLOT_COLD_COMMITTED) {
        return 0;
    }
    uint64_t seq = atomic_load_explicit(&meta->write_seq,
                                        memory_order_acquire);
    return (seq & 1u) == 0;
}

static int choose_victim_slot(tlc_core_warm_region_runtime_t *region,
                              uint32_t start,
                              uint32_t end,
                              uint32_t *victim) {
    uint32_t oldest = UINT32_MAX;
    uint64_t oldest_ns = UINT64_MAX;
    for (uint32_t slot = start; slot < end; slot++) {
        vemb_v16_warm_slot_meta_t *meta = &region->slot_meta[slot];
        if (!warm_slot_can_evict(meta))
            continue;
        uint32_t clock_bit =
            atomic_load_explicit(&meta->clock_bit, memory_order_relaxed);
        if (clock_bit == 0) {
            *victim = slot;
            return 0;
        }
        atomic_store_explicit(&meta->clock_bit, 0, memory_order_relaxed);
        uint64_t last_access =
            atomic_load_explicit(&meta->last_access_ns,
                                 memory_order_relaxed);
        if (last_access < oldest_ns) {
            oldest_ns = last_access;
            oldest = slot;
        }
    }
    if (oldest != UINT32_MAX) {
        *victim = oldest;
        return 0;
    }
    return -1;
}

static int warm_try_evict_and_fill(tlc_core_warm_region_runtime_t *region,
                                   tlc_core_t *core,
                                   uint32_t region_index,
                                   uint32_t slot,
                                   uint64_t key_hash,
                                   uint64_t fp,
                                   const void *value,
                                   uint32_t value_size,
                                   tlc_warm_location_t *location) {
    vemb_v16_warm_slot_meta_t *meta = &region->slot_meta[slot];
    uint32_t expected = VEMB_V16_WARM_SLOT_READY;
    if (!atomic_compare_exchange_strong_explicit(&meta->state,
                                                 &expected,
                                                 VEMB_V16_WARM_SLOT_EVICTING,
                                                 memory_order_acq_rel,
                                                 memory_order_acquire)) {
        atomic_fetch_add_explicit(&core->warm_eviction_fail, 1,
                                  memory_order_relaxed);
        return -1;
    }
    if (atomic_load_explicit(&meta->cold_state, memory_order_acquire) !=
        VEMB_V16_WARM_SLOT_COLD_COMMITTED) {
        atomic_store_explicit(&meta->state, VEMB_V16_WARM_SLOT_READY,
                              memory_order_release);
        atomic_fetch_add_explicit(&core->warm_eviction_fail, 1,
                                  memory_order_relaxed);
        return -1;
    }
    uint64_t old_seq = 0;
    if (slot_seq_try_begin(meta, &old_seq) != 0) {
        atomic_store_explicit(&meta->state, VEMB_V16_WARM_SLOT_READY,
                              memory_order_release);
        atomic_fetch_add_explicit(&core->warm_eviction_fail, 1,
                                  memory_order_relaxed);
        return -1;
    }
    atomic_store_explicit(&meta->state, VEMB_V16_WARM_SLOT_FILLING,
                          memory_order_release);
    uint64_t owner_generation =
        atomic_fetch_add_explicit(&meta->owner_generation, 1,
                                  memory_order_acq_rel) + 1u;
    meta->region_id = region->region_id;
    meta->local_slot = slot;
    meta->bytes = value_size;
    meta->key_hash = key_hash;
    meta->key_fingerprint = fp;
    atomic_store_explicit(&meta->cold_state,
                          VEMB_V16_WARM_SLOT_COLD_COMMITTED,
                          memory_order_release);
    atomic_store_explicit(&meta->last_access_ns, monotonic_ns(),
                          memory_order_relaxed);
    atomic_store_explicit(&meta->clock_bit, 1, memory_order_relaxed);
    slot_write_payload(region, slot, value, value_size);
    slot_publish_ready(meta, old_seq);
    atomic_fetch_add_explicit(&core->warm_eviction_success, 1,
                              memory_order_relaxed);
    *location = (tlc_warm_location_t){
        .region_id = region->region_id,
        .region_index = region_index,
        .local_slot = slot,
        .bytes = value_size,
        .offset = (uint64_t)slot * region->value_size,
        .owner_generation = owner_generation,
    };
    return 0;
}

static int warm_place_in_region(tlc_core_warm_region_runtime_t *region,
                                tlc_core_t *core,
                                uint32_t region_index,
                                uint64_t key_hash,
                                uint64_t fp,
                                const void *value,
                                uint32_t value_size,
                                tlc_warm_location_t *location) {
    uint32_t start = 0, end = 0;
    region_set_bounds(region, key_hash, &start, &end);
    for (uint32_t round = 0; round < TLC_CORE_WARM_PLACE_RETRIES; round++) {
        for (uint32_t slot = start; slot < end; slot++) {
            if (warm_try_overwrite_same(region, core, region_index, slot,
                                        key_hash, fp, value, value_size,
                                        location) == 0) {
                return 0;
            }
        }
        for (uint32_t slot = start; slot < end; slot++) {
            if (warm_try_fill_free(region, region_index, slot,
                                   key_hash, fp, value, value_size,
                                   location) == 0) {
                return 0;
            }
        }
        uint32_t victim = UINT32_MAX;
        if (choose_victim_slot(region, start, end, &victim) == 0 &&
            warm_try_evict_and_fill(region, core, region_index, victim,
                                    key_hash, fp, value, value_size,
                                    location) == 0) {
            return 0;
        }
        if (victim == UINT32_MAX) {
            atomic_fetch_add_explicit(&core->warm_eviction_fail, 1,
                                      memory_order_relaxed);
        }
        cpu_relax();
    }
    return -1;
}

static int warm_lookup(tlc_core_t *core,
                       const char *key,
                       uint32_t key_len,
                       uint64_t key_hash,
                       tlc_warm_location_t *location) {
    (void)key_len;
    tlc_core_warm_layer_t *warm = &core->warm;
    uint64_t fp = key_fingerprint(key, key_len);
    uint8_t tried[TLC_CORE_MAX_WARM_REGIONS] = {0};
    uint32_t start = vnode_lower_bound(warm, mix32(key_hash));
    for (uint32_t pass = 0; pass < 2; pass++) {
        uint32_t want_local = pass == 0 ? 1u : 0u;
        for (uint32_t i = 0; i < warm->vnode_count; i++) {
            uint32_t vnode_pos = (start + i) % warm->vnode_count;
            uint32_t region_index = warm->vnodes[vnode_pos].region_index;
            if (region_index >= warm->region_count || tried[region_index])
                continue;
            tlc_core_warm_region_runtime_t *region =
                &warm->regions[region_index];
            if ((region->is_local ? 1u : 0u) != want_local)
                continue;
            tried[region_index] = 1;
            if (warm_lookup_region(region, region_index, key_hash, fp,
                                   core->value_size, location) == 0) {
                atomic_fetch_add_explicit(&warm->hits, 1,
                                          memory_order_relaxed);
                return 0;
            }
        }
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
    uint64_t fp = key_fingerprint(key, key_len);
    uint8_t tried[TLC_CORE_MAX_WARM_REGIONS] = {0};
    uint32_t attempted_regions = 0;
    uint32_t start = vnode_lower_bound(warm, mix32(key_hash));
    for (uint32_t pass = 0; pass < 2; pass++) {
        uint32_t want_local = pass == 0 ? 1u : 0u;
        for (uint32_t i = 0; i < warm->vnode_count; i++) {
            uint32_t vnode_pos = (start + i) % warm->vnode_count;
            uint32_t region_index = warm->vnodes[vnode_pos].region_index;
            if (region_index >= warm->region_count || tried[region_index])
                continue;
            tlc_core_warm_region_runtime_t *region =
                &warm->regions[region_index];
            if ((region->is_local ? 1u : 0u) != want_local)
                continue;
            tried[region_index] = 1;
            attempted_regions++;
            if (!region->mapped_addr || value_size != region->value_size)
                continue;
            if (warm_place_in_region(region, core, region_index, key_hash, fp,
                                     value, value_size, location) == 0) {
                atomic_fetch_add_explicit(region->is_local ?
                                          &core->warm_alloc_local :
                                          &core->warm_alloc_remote,
                                          1,
                                          memory_order_relaxed);
                if (attempted_regions > 1) {
                    atomic_fetch_add_explicit(&core->warm_alloc_fallback, 1,
                                              memory_order_relaxed);
                }
                return 0;
            }
            log_warm_region_switch("set_no_place",
                                   key_hash,
                                   want_local,
                                   attempted_regions,
                                   vnode_pos,
                                   region_index,
                                   region,
                                   VEMB_V16_SHARED_ALLOCATOR_FULL);
        }
    }
    serverLog(LL_WARNING,
              "tlc warm put failed: key_hash=%llu key_len=%u value_size=%u attempted_regions=%u warm_capacity=%u",
              (unsigned long long)key_hash,
              key_len,
              value_size,
              attempted_regions,
              warm->capacity);
    atomic_fetch_add_explicit(&core->warm_alloc_fail, 1, memory_order_relaxed);
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

static int warm_copy_location_value(tlc_core_t *core,
                                    uint64_t key_hash,
                                    const tlc_warm_location_t *location,
                                    void *value_out,
                                    uint32_t value_out_size) {
    RETURN_IF(!location || !value_out ||
              location->region_id == TLC_CORE_INVALID_REGION_ID ||
              location->local_slot == TLC_CORE_INVALID_SLOT ||
              value_out_size < core->value_size,
              -1);
    tlc_core_warm_layer_t *warm = &core->warm;
    tlc_core_warm_region_runtime_t *region = NULL;
    uint32_t region_index = location->region_index;
    if (region_index < warm->region_count &&
        warm->regions[region_index].region_id == location->region_id) {
        region = &warm->regions[region_index];
    } else {
        for (uint32_t i = 0; i < warm->region_count; i++) {
            if (warm->regions[i].region_id == location->region_id) {
                region = &warm->regions[i];
                region_index = i;
                break;
            }
        }
    }
    RETURN_IF(!region ||
              location->offset !=
                  (uint64_t)location->local_slot * region->value_size ||
              location->bytes != region->value_size,
              -1);
    if (warm_slot_verify_ready(region,
                               region_index,
                               location->local_slot,
                               key_hash,
                               0,
                               location->bytes,
                               location->owner_generation,
                               NULL) != 0) {
        return -1;
    }
    memcpy(value_out,
           region->mapped_addr + location->offset,
           location->bytes);
    return 0;
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
    atomic_init(&core->warm_eviction_success, 0);
    atomic_init(&core->warm_eviction_fail, 0);
    atomic_init(&core->warm_same_key_overwrite, 0);
    atomic_init(&core->warm_stale_handle_reject, 0);
    atomic_init(&core->remote_meta_stale, 0);
    atomic_init(&core->source_fence_active_count, 0);
    atomic_init(&core->tombstone_active_count, 0);
    pthread_mutex_init(&core->key_meta_lock, NULL);

    if (hot_init(core, config->hot_capacity) != 0 ||
        warm_init(core, config) != 0 ||
        cold_init(core,
                  config->cold_max_segments,
                  config->cold_segment_records) != 0 ||
        key_meta_init(core) != 0) {
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
    if (core->warm.regions) {
        for (uint32_t i = 0; i < core->warm.region_count; i++) {
            if (core->warm.regions[i].owns_slot_meta)
                fallback_slot_meta_release(
                    core->warm.regions[i].shared_allocator,
                    core->warm.regions[i].slot_meta);
        }
    }
    if (core->warm.regions) zfree(core->warm.regions);
    if (core->warm.vnodes) zfree(core->warm.vnodes);
    if (core->key_meta) zfree(core->key_meta);
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
    pthread_mutex_destroy(&core->key_meta_lock);
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

static int tlc_core_get_warm_location_raw(tlc_core_t *core,
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

int tlc_core_get_warm_location(tlc_core_t *core,
                               const char *key,
                               uint32_t key_len,
                               uint64_t key_hash,
                               tlc_warm_location_t *location) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!valid_key, -1);
    RETURN_IF(key_meta_blocks_source_access(core, key, key_len, key_hash),
              -1);

    int rc = tlc_core_get_warm_location_raw(core,
                                            key,
                                            key_len,
                                            key_hash,
                                            location);
    if (rc == 0 &&
        key_meta_blocks_source_access(core, key, key_len, key_hash)) {
        return -1;
    }
    return rc;
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

static int tlc_core_put_location_epoch(tlc_core_t *core,
                                       const char *key,
                                       uint32_t key_len,
                                       uint64_t key_hash,
                                       const void *value,
                                       uint32_t value_size,
                                       uint64_t topology_epoch,
                                       int enforce_epoch,
                                       tlc_warm_location_t *location) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!valid_key || value_size != core->value_size, -1);

    pthread_mutex_lock(&core->key_meta_lock);
    tlc_core_key_meta_entry_t *meta =
        key_meta_find_locked(core, key, key_len, key_hash);
    if (tlc_core_source_fence_active(core) &&
        meta &&
        key_meta_state_blocks_source_access(meta->migration_state)) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }
    if (enforce_epoch && meta && topology_epoch < meta->topology_epoch) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }
    if (!meta && core->key_meta_count >= core->key_meta_capacity) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }
    atomic_fetch_add_explicit(&core->total_writes, 1, memory_order_relaxed);
    int cold_rc = cold_append(core, key, key_len, key_hash, value, value_size);
    if (cold_rc != 0) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }
    atomic_fetch_add_explicit(&core->write_throughs, 1, memory_order_relaxed);

    if (warm_put(core, key, key_len, key_hash,
                 value, value_size, location) == 0) {
        meta = key_meta_find_or_create_locked(core, key, key_len, key_hash);
        if (!meta) {
            pthread_mutex_unlock(&core->key_meta_lock);
            return -1;
        }
        meta->key_version++;
        if (enforce_epoch && topology_epoch > meta->topology_epoch)
            meta->topology_epoch = topology_epoch;
        key_meta_set_tombstone_locked(core, meta, 0);
        meta->location = *location;
        pthread_mutex_unlock(&core->key_meta_lock);
        return 0;
    }

    atomic_fetch_add_explicit(&core->warm_alloc_cold_spill, 1,
                              memory_order_relaxed);
    *location = tlc_invalid_location;
    meta = key_meta_find_or_create_locked(core, key, key_len, key_hash);
    if (!meta) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }
    meta->key_version++;
    if (enforce_epoch && topology_epoch > meta->topology_epoch)
        meta->topology_epoch = topology_epoch;
    key_meta_set_tombstone_locked(core, meta, 0);
    meta->location = *location;
    pthread_mutex_unlock(&core->key_meta_lock);
    return 0;
}

int tlc_core_put_location(tlc_core_t *core,
                          const char *key,
                          uint32_t key_len,
                          uint64_t key_hash,
                          const void *value,
                          uint32_t value_size,
                          tlc_warm_location_t *location) {
    return tlc_core_put_location_epoch(core,
                                       key,
                                       key_len,
                                       key_hash,
                                       value,
                                       value_size,
                                       0,
                                       0,
                                       location);
}

int tlc_core_put_location_with_epoch(tlc_core_t *core,
                                     const char *key,
                                     uint32_t key_len,
                                     uint64_t key_hash,
                                     const void *value,
                                     uint32_t value_size,
                                     uint64_t topology_epoch,
                                     tlc_warm_location_t *location) {
    return tlc_core_put_location_epoch(core,
                                       key,
                                       key_len,
                                       key_hash,
                                       value,
                                       value_size,
                                       topology_epoch,
                                       1,
                                       location);
}

int tlc_core_delete_with_epoch(tlc_core_t *core,
                               const char *key,
                               uint32_t key_len,
                               uint64_t key_hash,
                               uint64_t topology_epoch,
                               tlc_core_key_migration_info_t *info) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!core || !valid_key, -1);
    if (info)
        memset(info, 0, sizeof(*info));

    pthread_mutex_lock(&core->key_meta_lock);
    tlc_core_key_meta_entry_t *meta =
        key_meta_find_locked(core, key, key_len, key_hash);
    if (!meta ||
        meta->tombstone ||
        (tlc_core_source_fence_active(core) &&
         key_meta_state_blocks_source_access(meta->migration_state)) ||
        topology_epoch < meta->topology_epoch) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }

    meta->key_version++;
    if (topology_epoch > meta->topology_epoch)
        meta->topology_epoch = topology_epoch;
    key_meta_set_tombstone_locked(core, meta, 1);
    meta->location = tlc_invalid_location;
    key_meta_fill_info(meta, info);
    pthread_mutex_unlock(&core->key_meta_lock);
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

int tlc_core_get_migration_info(tlc_core_t *core,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                tlc_core_key_migration_info_t *info) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!core || !info || !valid_key, -1);

    pthread_mutex_lock(&core->key_meta_lock);
    tlc_core_key_meta_entry_t *meta =
        key_meta_find_locked(core, key, key_len, key_hash);
    if (!meta) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }
    key_meta_fill_info(meta, info);
    pthread_mutex_unlock(&core->key_meta_lock);
    return 0;
}

int tlc_core_mark_migrating(tlc_core_t *core,
                            const char *key,
                            uint32_t key_len,
                            uint64_t key_hash,
                            uint64_t topology_epoch,
                            uint32_t target_owner,
                            tlc_core_key_migration_info_t *info) {
    return tlc_core_mark_migrating_in_shard(core,
                                            key,
                                            key_len,
                                            key_hash,
                                            topology_epoch,
                                            target_owner,
                                            0,
                                            info);
}

int tlc_core_mark_migrating_in_shard(tlc_core_t *core,
                                     const char *key,
                                     uint32_t key_len,
                                     uint64_t key_hash,
                                     uint64_t topology_epoch,
                                     uint32_t target_owner,
                                     uint32_t shard_id,
                                     tlc_core_key_migration_info_t *info) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!core || !valid_key || target_owner == UINT32_MAX, -1);

    pthread_mutex_lock(&core->key_meta_lock);
    tlc_core_key_meta_entry_t *meta =
        key_meta_find_locked(core, key, key_len, key_hash);
    if (!meta || meta->tombstone) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }
    if (meta->migration_state != TLC_CORE_KEY_SOURCE_ACTIVE &&
        meta->migration_state != TLC_CORE_KEY_MIGRATING) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }
    if (meta->migration_state == TLC_CORE_KEY_MIGRATING &&
        (meta->target_owner != target_owner ||
         meta->shard_id != shard_id)) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }
    if (topology_epoch < meta->topology_epoch) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }
    meta->migration_state = TLC_CORE_KEY_MIGRATING;
    meta->target_owner = target_owner;
    meta->shard_id = shard_id;
    if (topology_epoch > meta->topology_epoch)
        meta->topology_epoch = topology_epoch;
    key_meta_fill_info(meta, info);
    pthread_mutex_unlock(&core->key_meta_lock);
    return 0;
}

int tlc_core_mark_cutover(tlc_core_t *core,
                          const char *key,
                          uint32_t key_len,
                          uint64_t key_hash,
                          uint64_t topology_epoch,
                          uint32_t target_owner,
                          tlc_core_key_migration_info_t *info) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!core || !valid_key || target_owner == UINT32_MAX, -1);

    pthread_mutex_lock(&core->key_meta_lock);
    tlc_core_key_meta_entry_t *meta =
        key_meta_find_locked(core, key, key_len, key_hash);
    if (!meta ||
        (meta->migration_state != TLC_CORE_KEY_MIGRATING &&
         meta->migration_state != TLC_CORE_KEY_CUTOVER) ||
        meta->target_owner != target_owner ||
        topology_epoch < meta->topology_epoch) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }
    key_meta_note_source_fence_transition(core,
                                          meta->migration_state,
                                          TLC_CORE_KEY_CUTOVER);
    meta->migration_state = TLC_CORE_KEY_CUTOVER;
    if (topology_epoch > meta->topology_epoch)
        meta->topology_epoch = topology_epoch;
    if (topology_epoch > meta->owner_epoch)
        meta->owner_epoch = topology_epoch;
    key_meta_fill_info(meta, info);
    pthread_mutex_unlock(&core->key_meta_lock);
    return 0;
}

int tlc_core_mark_source_gc(tlc_core_t *core,
                            const char *key,
                            uint32_t key_len,
                            uint64_t key_hash,
                            uint64_t topology_epoch,
                            uint32_t target_owner,
                            tlc_core_key_migration_info_t *info) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!core || !valid_key || target_owner == UINT32_MAX, -1);

    pthread_mutex_lock(&core->key_meta_lock);
    tlc_core_key_meta_entry_t *meta =
        key_meta_find_locked(core, key, key_len, key_hash);
    if (!meta ||
        (meta->migration_state != TLC_CORE_KEY_CUTOVER &&
         meta->migration_state != TLC_CORE_KEY_SOURCE_GC) ||
        meta->target_owner != target_owner ||
        topology_epoch < meta->topology_epoch) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }
    key_meta_note_source_fence_transition(core,
                                          meta->migration_state,
                                          TLC_CORE_KEY_SOURCE_GC);
    meta->migration_state = TLC_CORE_KEY_SOURCE_GC;
    if (topology_epoch > meta->topology_epoch)
        meta->topology_epoch = topology_epoch;
    if (topology_epoch > meta->owner_epoch)
        meta->owner_epoch = topology_epoch;
    key_meta_fill_info(meta, info);
    pthread_mutex_unlock(&core->key_meta_lock);
    return 0;
}

int tlc_core_accept_owner_lease(tlc_core_t *core,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                uint64_t topology_epoch,
                                uint64_t owner_epoch,
                                uint32_t target_owner,
                                tlc_core_key_migration_info_t *info) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!core || !valid_key || target_owner == UINT32_MAX, -1);

    pthread_mutex_lock(&core->key_meta_lock);
    tlc_core_key_meta_entry_t *meta =
        key_meta_find_locked(core, key, key_len, key_hash);
    if (!meta ||
        meta->migration_state != TLC_CORE_KEY_DEST_COMMITTED ||
        meta->target_owner != target_owner ||
        topology_epoch < meta->topology_epoch ||
        owner_epoch < meta->owner_epoch) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }
    if (topology_epoch > meta->topology_epoch)
        meta->topology_epoch = topology_epoch;
    if (owner_epoch > meta->owner_epoch)
        meta->owner_epoch = owner_epoch;
    key_meta_fill_info(meta, info);
    pthread_mutex_unlock(&core->key_meta_lock);
    return 0;
}

int tlc_core_key_is_source_cutover(tlc_core_t *core,
                                   const char *key,
                                   uint32_t key_len,
                                   uint64_t key_hash,
                                   tlc_core_key_migration_info_t *info) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!core || !valid_key, -1);
    if (!tlc_core_source_fence_active(core))
        return 0;

    int cutover = 0;
    pthread_mutex_lock(&core->key_meta_lock);
    tlc_core_key_meta_entry_t *meta =
        key_meta_find_locked(core, key, key_len, key_hash);
    if (meta) {
        cutover = key_meta_state_blocks_source_access(meta->migration_state);
        if (cutover)
            key_meta_fill_info(meta, info);
    }
    pthread_mutex_unlock(&core->key_meta_lock);
    return cutover;
}

int tlc_core_has_uncommitted_source_migrations(tlc_core_t *core) {
    RETURN_IF(!core, 1);
    int has_uncommitted = 0;
    pthread_mutex_lock(&core->key_meta_lock);
    for (uint32_t i = 0; i < core->key_meta_capacity; i++) {
        const tlc_core_key_meta_entry_t *meta = &core->key_meta[i];
        if (meta->occupied &&
            meta->migration_state == TLC_CORE_KEY_MIGRATING &&
            meta->target_owner != UINT32_MAX) {
            has_uncommitted = 1;
            break;
        }
    }
    pthread_mutex_unlock(&core->key_meta_lock);
    return has_uncommitted;
}

int tlc_core_collect_migration_keys(tlc_core_t *core,
                                    uint64_t topology_epoch,
                                    uint32_t target_owner,
                                    uint32_t shard_id,
                                    uint32_t migration_state,
                                    tlc_core_migration_key_ref_t *keys,
                                    uint32_t max_keys,
                                    uint32_t *key_count) {
    RETURN_IF(!core || !keys || !key_count ||
              target_owner == UINT32_MAX,
              -1);
    *key_count = 0;
    pthread_mutex_lock(&core->key_meta_lock);
    for (uint32_t i = 0; i < core->key_meta_capacity; i++) {
        const tlc_core_key_meta_entry_t *meta = &core->key_meta[i];
        if (!meta->occupied ||
            meta->topology_epoch != topology_epoch ||
            meta->target_owner != target_owner ||
            meta->shard_id != shard_id ||
            (migration_state != UINT32_MAX &&
             meta->migration_state != migration_state)) {
            continue;
        }
        if (*key_count >= max_keys) {
            pthread_mutex_unlock(&core->key_meta_lock);
            return -1;
        }
        tlc_core_migration_key_ref_t *entry = &keys[*key_count];
        memset(entry, 0, sizeof(*entry));
        entry->key_hash = meta->key_hash;
        entry->key_len = meta->key_len;
        memcpy(entry->key, meta->key, meta->key_len);
        key_meta_fill_info(meta, &entry->info);
        (*key_count)++;
    }
    pthread_mutex_unlock(&core->key_meta_lock);
    return 0;
}

int tlc_core_collect_migration_keys_page(tlc_core_t *core,
                                         uint64_t topology_epoch,
                                         uint32_t target_owner,
                                         uint32_t shard_id,
                                         uint32_t migration_state,
                                         tlc_core_migration_key_ref_t *keys,
                                         uint32_t max_keys,
                                         uint32_t *key_count,
                                         uint32_t *remaining_count) {
    RETURN_IF(!core || !keys || !key_count || !remaining_count ||
              target_owner == UINT32_MAX || max_keys == 0,
              -1);
    *key_count = 0;
    *remaining_count = 0;

    pthread_mutex_lock(&core->key_meta_lock);
    for (uint32_t i = 0; i < core->key_meta_capacity; i++) {
        const tlc_core_key_meta_entry_t *meta = &core->key_meta[i];
        if (!meta->occupied ||
            meta->topology_epoch != topology_epoch ||
            meta->target_owner != target_owner ||
            meta->shard_id != shard_id ||
            (migration_state != UINT32_MAX &&
             meta->migration_state != migration_state)) {
            continue;
        }
        if (*key_count >= max_keys) {
            (*remaining_count)++;
            continue;
        }
        tlc_core_migration_key_ref_t *entry = &keys[*key_count];
        memset(entry, 0, sizeof(*entry));
        entry->key_hash = meta->key_hash;
        entry->key_len = meta->key_len;
        memcpy(entry->key, meta->key, meta->key_len);
        key_meta_fill_info(meta, &entry->info);
        (*key_count)++;
    }
    pthread_mutex_unlock(&core->key_meta_lock);
    return 0;
}

int tlc_core_count_migration_keys(tlc_core_t *core,
                                  uint64_t topology_epoch,
                                  uint32_t target_owner,
                                  uint32_t shard_id,
                                  uint32_t migration_state,
                                  uint32_t *key_count) {
    RETURN_IF(!core || !key_count || target_owner == UINT32_MAX, -1);
    *key_count = 0;

    pthread_mutex_lock(&core->key_meta_lock);
    for (uint32_t i = 0; i < core->key_meta_capacity; i++) {
        const tlc_core_key_meta_entry_t *meta = &core->key_meta[i];
        if (!meta->occupied ||
            meta->topology_epoch != topology_epoch ||
            meta->target_owner != target_owner ||
            meta->shard_id != shard_id ||
            (migration_state != UINT32_MAX &&
             meta->migration_state != migration_state)) {
            continue;
        }
        (*key_count)++;
    }
    pthread_mutex_unlock(&core->key_meta_lock);
    return 0;
}

int tlc_core_collect_migration_ranges(tlc_core_t *core,
                                      uint64_t topology_epoch,
                                      uint32_t migration_state,
                                      tlc_core_migration_range_ref_t *ranges,
                                      uint32_t max_ranges,
                                      uint32_t *range_count) {
    RETURN_IF(!core || !ranges || !range_count || max_ranges == 0, -1);
    *range_count = 0;
    pthread_mutex_lock(&core->key_meta_lock);
    for (uint32_t i = 0; i < core->key_meta_capacity; i++) {
        const tlc_core_key_meta_entry_t *meta = &core->key_meta[i];
        if (!meta->occupied ||
            meta->topology_epoch != topology_epoch ||
            meta->target_owner == UINT32_MAX ||
            (migration_state != UINT32_MAX &&
             meta->migration_state != migration_state)) {
            continue;
        }

        tlc_core_migration_range_ref_t *range = NULL;
        for (uint32_t j = 0; j < *range_count; j++) {
            if (ranges[j].topology_epoch == meta->topology_epoch &&
                ranges[j].target_owner == meta->target_owner &&
                ranges[j].shard_id == meta->shard_id) {
                range = &ranges[j];
                break;
            }
        }
        if (!range) {
            if (*range_count >= max_ranges) {
                pthread_mutex_unlock(&core->key_meta_lock);
                return -1;
            }
            range = &ranges[*range_count];
            memset(range, 0, sizeof(*range));
            range->topology_epoch = meta->topology_epoch;
            range->target_owner = meta->target_owner;
            range->shard_id = meta->shard_id;
            (*range_count)++;
        }
        range->key_count++;
    }
    pthread_mutex_unlock(&core->key_meta_lock);
    return 0;
}

int tlc_core_collect_source_active_keys(tlc_core_t *core,
                                        uint32_t *cursor,
                                        tlc_core_migration_key_ref_t *keys,
                                        uint32_t max_keys,
                                        uint32_t *key_count,
                                        int *done) {
    RETURN_IF(!core || !cursor || !keys || !key_count || !done ||
              max_keys == 0,
              -1);
    *key_count = 0;
    *done = 0;

    pthread_mutex_lock(&core->key_meta_lock);
    uint32_t i = *cursor;
    for (; i < core->key_meta_capacity; i++) {
        const tlc_core_key_meta_entry_t *meta = &core->key_meta[i];
        if (!meta->occupied ||
            meta->tombstone ||
            meta->migration_state != TLC_CORE_KEY_SOURCE_ACTIVE) {
            continue;
        }
        if (*key_count >= max_keys)
            break;
        tlc_core_migration_key_ref_t *entry = &keys[*key_count];
        memset(entry, 0, sizeof(*entry));
        entry->key_hash = meta->key_hash;
        entry->key_len = meta->key_len;
        memcpy(entry->key, meta->key, meta->key_len);
        key_meta_fill_info(meta, &entry->info);
        (*key_count)++;
    }
    *cursor = i;
    if (i >= core->key_meta_capacity)
        *done = 1;
    pthread_mutex_unlock(&core->key_meta_lock);
    return 0;
}

int tlc_core_snapshot(tlc_core_t *core,
                      const char *key,
                      uint32_t key_len,
                      uint64_t key_hash,
                      uint32_t source_owner,
                      uint32_t target_owner,
                      tlc_core_migration_snapshot_t *snapshot,
                      void *value_out,
                      uint32_t value_out_size) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!core || !snapshot || !valid_key, -1);

    pthread_mutex_lock(&core->key_meta_lock);
    tlc_core_key_meta_entry_t *meta =
        key_meta_find_locked(core, key, key_len, key_hash);
    if (!meta) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }
    if (key_meta_state_blocks_source_access(meta->migration_state)) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }

    memset(snapshot, 0, sizeof(*snapshot));
    snapshot->key_hash = key_hash;
    snapshot->key_len = key_len;
    snapshot->key_version = meta->key_version;
    snapshot->topology_epoch = meta->topology_epoch;
    snapshot->owner_epoch = meta->owner_epoch;
    snapshot->migration_state = meta->migration_state;
    snapshot->source_owner = source_owner;
    snapshot->target_owner = target_owner;
    snapshot->tombstone = meta->tombstone;
    snapshot->value_size = meta->tombstone ? 0 : core->value_size;
    snapshot->shard_id = meta->shard_id;
    snapshot->location = meta->location;
    memcpy(snapshot->key, key, key_len);

    if (snapshot->tombstone) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return 0;
    }

    tlc_warm_location_t location = tlc_invalid_location;
    if (tlc_core_get_warm_location_raw(core, key, key_len,
                                       key_hash, &location) != 0) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }
    if (warm_copy_location_value(core,
                                 key_hash,
                                 &location,
                                 value_out,
                                 value_out_size) != 0) {
        pthread_mutex_unlock(&core->key_meta_lock);
        return -1;
    }
    snapshot->location = location;
    meta->location = location;
    pthread_mutex_unlock(&core->key_meta_lock);
    return 0;
}

int tlc_core_apply_migration(tlc_core_t *core,
                             const tlc_core_migration_snapshot_t *snapshot,
                             const void *value,
                             uint32_t value_size,
                             tlc_core_migration_apply_status_t *status,
                             tlc_warm_location_t *location) {
    RETURN_IF(!core || !snapshot || !status ||
              snapshot->key_len == 0 ||
              snapshot->key_len > VEMB_V16_MAX_KEY_LEN,
              -1);
    if (!snapshot->tombstone &&
        (!value || value_size != core->value_size ||
         snapshot->value_size != core->value_size)) {
        *status = TLC_CORE_MIGRATION_ERROR;
        return -1;
    }

    pthread_mutex_lock(&core->key_meta_lock);
    tlc_core_key_meta_entry_t *meta =
        key_meta_find_or_create_locked(core,
                                       snapshot->key,
                                       snapshot->key_len,
                                       snapshot->key_hash);
    if (!meta) {
        pthread_mutex_unlock(&core->key_meta_lock);
        *status = TLC_CORE_MIGRATION_ERROR;
        return -1;
    }
    if (incoming_snapshot_is_stale(meta, snapshot)) {
        pthread_mutex_unlock(&core->key_meta_lock);
        *status = TLC_CORE_MIGRATION_STALE_REJECTED;
        return 0;
    }
    if (incoming_snapshot_is_duplicate(meta, snapshot)) {
        if (location)
            *location = meta->location;
        pthread_mutex_unlock(&core->key_meta_lock);
        *status = TLC_CORE_MIGRATION_DUPLICATE;
        return 0;
    }

    tlc_warm_location_t new_location = tlc_invalid_location;
    if (!snapshot->tombstone) {
        if (cold_append(core,
                        snapshot->key,
                        snapshot->key_len,
                        snapshot->key_hash,
                        value,
                        value_size) != 0 ||
            warm_put(core,
                     snapshot->key,
                     snapshot->key_len,
                     snapshot->key_hash,
                     value,
                     value_size,
                     &new_location) != 0) {
            pthread_mutex_unlock(&core->key_meta_lock);
            *status = TLC_CORE_MIGRATION_RETRY;
            return 0;
        }
    }

    meta->key_version = snapshot->key_version;
    meta->topology_epoch = snapshot->topology_epoch;
    meta->owner_epoch = snapshot->owner_epoch;
    meta->migration_state = TLC_CORE_KEY_DEST_COMMITTED;
    meta->source_owner = snapshot->source_owner;
    meta->target_owner = snapshot->target_owner;
    key_meta_set_tombstone_locked(core, meta, snapshot->tombstone ? 1u : 0u);
    meta->shard_id = snapshot->shard_id;
    meta->location = snapshot->tombstone ? tlc_invalid_location : new_location;
    if (location)
        *location = meta->location;
    pthread_mutex_unlock(&core->key_meta_lock);
    *status = TLC_CORE_MIGRATION_APPLIED;
    return 0;
}

int tlc_core_validate_warm_location(tlc_core_t *core,
                                    uint64_t key_hash,
                                    const tlc_warm_location_t *location) {
    RETURN_IF(!location ||
              location->region_id == TLC_CORE_INVALID_REGION_ID ||
              location->local_slot == TLC_CORE_INVALID_SLOT,
              -1);
    tlc_core_warm_layer_t *warm = &core->warm;
    tlc_core_warm_region_runtime_t *region = NULL;
    uint32_t region_index = location->region_index;
    if (region_index < warm->region_count &&
        warm->regions[region_index].region_id == location->region_id) {
        region = &warm->regions[region_index];
    } else {
        for (uint32_t i = 0; i < warm->region_count; i++) {
            if (warm->regions[i].region_id == location->region_id) {
                region = &warm->regions[i];
                region_index = i;
                break;
            }
        }
    }
    RETURN_IF(!region, -1);
    uint64_t expected_offset =
        (uint64_t)location->local_slot * region->value_size;
    RETURN_IF(location->offset != expected_offset ||
              location->bytes != region->value_size,
              -1);
    int rc = warm_slot_verify_ready(region,
                                    region_index,
                                    location->local_slot,
                                    key_hash,
                                    0,
                                    location->bytes,
                                    location->owner_generation,
                                    NULL);
    if (rc != 0) {
        uint32_t log_index =
            atomic_fetch_add_explicit(&tlc_core_stale_diag_logs, 1,
                                      memory_order_relaxed);
        if (log_index < TLC_CORE_DIAG_STALE_LOG_LIMIT &&
            location->local_slot < region->capacity_slots) {
            vemb_v16_warm_slot_meta_t *meta =
                &region->slot_meta[location->local_slot];
            uint32_t state =
                atomic_load_explicit(&meta->state, memory_order_acquire);
            uint64_t owner_generation =
                atomic_load_explicit(&meta->owner_generation,
                                     memory_order_acquire);
            uint64_t write_seq =
                atomic_load_explicit(&meta->write_seq,
                                     memory_order_acquire);
            uint32_t cold_state =
                atomic_load_explicit(&meta->cold_state,
                                     memory_order_acquire);
            serverLog(LL_NOTICE,
                      "tlc diag stale warm location: key_hash=%llu location_region_id=%u location_region_index=%u location_slot=%u location_offset=%llu location_bytes=%u location_generation=%llu current_state=%u current_region_id=%u current_slot=%u current_key_hash=%llu current_fp=%llu current_bytes=%u current_generation=%llu current_write_seq=%llu current_cold_state=%u",
                      (unsigned long long)key_hash,
                      location->region_id,
                      region_index,
                      location->local_slot,
                      (unsigned long long)location->offset,
                      location->bytes,
                      (unsigned long long)location->owner_generation,
                      state,
                      meta->region_id,
                      meta->local_slot,
                      (unsigned long long)meta->key_hash,
                      (unsigned long long)meta->key_fingerprint,
                      meta->bytes,
                      (unsigned long long)owner_generation,
                      (unsigned long long)write_seq,
                      cold_state);
        }
        atomic_fetch_add_explicit(&core->warm_stale_handle_reject, 1,
                                  memory_order_relaxed);
        return -1;
    }
    return 0;
}

void tlc_core_note_remote_meta_stale(tlc_core_t *core) {
    atomic_fetch_add_explicit(&core->remote_meta_stale, 1,
                              memory_order_relaxed);
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
    stats->warm_eviction_success =
        atomic_load_explicit(&core->warm_eviction_success, memory_order_relaxed);
    stats->warm_eviction_fail =
        atomic_load_explicit(&core->warm_eviction_fail, memory_order_relaxed);
    stats->warm_same_key_overwrite =
        atomic_load_explicit(&core->warm_same_key_overwrite, memory_order_relaxed);
    stats->warm_stale_handle_reject =
        atomic_load_explicit(&core->warm_stale_handle_reject, memory_order_relaxed);
    stats->remote_meta_stale =
        atomic_load_explicit(&core->remote_meta_stale, memory_order_relaxed);
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
