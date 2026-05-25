#include "tlc_core.h"
#include "cpu_relax.h"
#include "macro.h"
#include "sve_operation.h"
#include "vemb_v16_protocol.h"
#include "zmalloc.h"

#include <stdatomic.h>
#include <string.h>

#define TLC_CORE_HOT_PROBES 4u
#define TLC_CORE_WARM_PROBES 6u
#define TLC_CORE_COLD_PROBES 6u
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
    uint32_t warm_slot;
    atomic_uint_fast32_t access_count;
    char key[VEMB_V16_MAX_KEY_LEN];
    atomic_int state;
} tlc_core_warm_entry_t;

typedef struct tlc_core_warm_layer {
    tlc_core_warm_entry_t *entries;
    int32_t *hash_table;
    atomic_uint_fast32_t count;
    uint32_t capacity;
    uint32_t hash_capacity;
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
    uint8_t *warm_data;
    size_t warm_data_bytes;
    tlc_core_hold_layer_t hold;
    tlc_core_warm_layer_t warm;
    tlc_core_cold_layer_t cold;
    atomic_uint_fast64_t total_reads;
    atomic_uint_fast64_t total_writes;
    atomic_uint_fast64_t read_throughs;
    atomic_uint_fast64_t write_throughs;
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

static int warm_init(tlc_core_t *core) {
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
                             uint32_t *warm_slot) {
    RETURN_IF(warm_idx < 0 || (uint32_t)warm_idx >= core->warm.capacity, -1);
    tlc_core_warm_entry_t *entry = &core->warm.entries[warm_idx];
    int matched = warm_entry_matches(entry, key, key_len, key_hash);
    RETURN_IF(!matched, -1);
    atomic_fetch_add_explicit(&entry->access_count, 1, memory_order_relaxed);
    *warm_slot = entry->warm_slot;
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

static int warm_lookup(tlc_core_t *core,
                       const char *key,
                       uint32_t key_len,
                       uint64_t key_hash,
                       uint32_t *warm_slot) {
    tlc_core_warm_layer_t *warm = &core->warm;
    uint32_t slot = hash_fast(key_hash, warm->mask);
    uint32_t lock_id = slot & warm->mask;
    int32_t found_idx = -1;
    uint32_t empty_pos = UINT32_MAX;

    __builtin_prefetch(&warm->hash_table[slot], 0, 3);
    bitmap_lock_blocking(&warm->locks, lock_id);
    int found = warm_find_locked(core, key, key_len, key_hash,
                                 &found_idx, &empty_pos) == 0;
    if (found) {
        tlc_core_warm_entry_t *entry = &warm->entries[found_idx];
        atomic_fetch_add_explicit(&entry->access_count, 1, memory_order_relaxed);
        *warm_slot = entry->warm_slot;
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
                    uint32_t *warm_slot) {
    tlc_core_warm_layer_t *warm = &core->warm;
    uint32_t slot = hash_fast(key_hash, warm->mask);
    uint32_t lock_id = slot & warm->mask;
    int32_t found_idx = -1;
    uint32_t empty_pos = UINT32_MAX;
    int32_t target = -1;

    bitmap_lock_blocking(&warm->locks, lock_id);
    if (warm_find_locked(core, key, key_len, key_hash,
                         &found_idx, &empty_pos) == 0) {
        target = found_idx;
    } else {
        if (empty_pos == UINT32_MAX) {
            bitmap_unlock(&warm->locks, lock_id);
            return -1;
        }
        target = (int32_t)atomic_fetch_add_explicit(&warm->count, 1,
                                                    memory_order_relaxed);
        if (target < 0 || (uint32_t)target >= warm->capacity) {
            bitmap_unlock(&warm->locks, lock_id);
            return -1;
        }
        tlc_core_warm_entry_t *entry = &warm->entries[target];
        entry->key_hash = key_hash;
        entry->key_len = key_len;
        entry->value_size = value_size;
        entry->warm_slot = (uint32_t)target;
        memcpy(entry->key, key, key_len);
        atomic_store_explicit(&entry->access_count, 0, memory_order_relaxed);
        warm->hash_table[empty_pos] = target;
    }

    tlc_core_warm_entry_t *entry = &warm->entries[target];
    sve_streaming_store(value,
                        core->warm_data +
                            (size_t)entry->warm_slot * core->value_size,
                        value_size);
    entry->value_size = value_size;
    atomic_fetch_add_explicit(&entry->access_count, 1, memory_order_relaxed);
    atomic_store_explicit(&entry->state, TLC_CORE_ENTRY_DIRTY,
                          memory_order_release);
    *warm_slot = entry->warm_slot;
    bitmap_unlock(&warm->locks, lock_id);

    hot_put(core, key_hash, target);
    return 0;
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
              config->warm_capacity == 0 || !config->warm_data ||
              config->warm_data_bytes <
                  (uint64_t)config->value_size * config->warm_capacity,
              -1);

    tlc_core_t *core = zcalloc(sizeof(*core));
    RETURN_IF(!core, -1);
    core->value_size = config->value_size;
    core->warm_capacity = config->warm_capacity;
    core->warm_data = config->warm_data;
    core->warm_data_bytes = config->warm_data_bytes;
    atomic_init(&core->total_reads, 0);
    atomic_init(&core->total_writes, 0);
    atomic_init(&core->read_throughs, 0);
    atomic_init(&core->write_throughs, 0);

    if (hot_init(core, config->hot_capacity) != 0 ||
        warm_init(core) != 0 ||
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
    RETURN_IF(!core);
    bitmap_destroy(&core->warm.locks);
    bitmap_destroy(&core->cold.locks);
    if (core->hold.table) zfree(core->hold.table);
    if (core->warm.entries) zfree(core->warm.entries);
    if (core->warm.hash_table) zfree(core->warm.hash_table);
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
    RETURN_IF(!core || !warm_slot, -1);
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!valid_key, -1);

    atomic_fetch_add_explicit(&core->total_reads, 1, memory_order_relaxed);
    int32_t hot_idx = hot_get(core, key_hash);
    if (warm_validate_idx(core, hot_idx, key, key_len, key_hash, warm_slot) == 0)
        return 0;
    if (warm_lookup(core, key, key_len, key_hash, warm_slot) == 0)
        return 0;

    const uint8_t *cold_value = NULL;
    uint32_t cold_value_size = 0;
    int cold_rc = cold_lookup(core, key, key_len, key_hash,
                              &cold_value, &cold_value_size);
    RETURN_IF(cold_rc != 0, -1);
    RETURN_IF(cold_value_size != core->value_size, -1);
    int warm_rc = warm_put(core, key, key_len, key_hash,
                           cold_value, cold_value_size, warm_slot);
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
    RETURN_IF(!core || !warm_slot || !value, -1);
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!valid_key || value_size != core->value_size, -1);

    atomic_fetch_add_explicit(&core->total_writes, 1, memory_order_relaxed);
    if (warm_put(core, key, key_len, key_hash,
                 value, value_size, warm_slot) == 0) {
        return 0;
    }

    int cold_rc = cold_append(core, key, key_len, key_hash, value, value_size);
    RETURN_IF(cold_rc != 0, -1);
    atomic_fetch_add_explicit(&core->write_throughs, 1, memory_order_relaxed);
    *warm_slot = TLC_CORE_INVALID_SLOT;
    return 0;
}

int tlc_core_cold_append(tlc_core_t *core,
                         const char *key,
                         uint32_t key_len,
                         uint64_t key_hash,
                         const void *value,
                         uint32_t value_size) {
    RETURN_IF(!core || !value, -1);
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!valid_key || value_size != core->value_size, -1);
    return cold_append(core, key, key_len, key_hash, value, value_size);
}
