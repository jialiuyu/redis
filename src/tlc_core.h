#ifndef __TLC_CORE_H
#define __TLC_CORE_H

#include "vemb_v16_shared_allocator.h"

#include <stddef.h>
#include <stdint.h>

#define TLC_CORE_DEFAULT_HOT_CAPACITY 65536u
#define TLC_CORE_DEFAULT_COLD_MAX_SEGMENTS 64u
#define TLC_CORE_DEFAULT_COLD_SEGMENT_RECORDS 4096u
#define TLC_CORE_INVALID_SLOT UINT32_MAX
#define TLC_CORE_INVALID_REGION_ID UINT32_MAX
#define TLC_CORE_MAX_WARM_REGIONS 128u

typedef struct tlc_warm_location {
    uint32_t region_id;
    uint32_t region_index;
    uint32_t local_slot;
    uint32_t bytes;
    uint64_t offset;
} tlc_warm_location_t;

typedef struct tlc_core_warm_region_config {
    uint32_t region_id;
    uint32_t backend_type;
    uint32_t is_local;
    uint32_t weight;
    uint32_t value_size;
    uint64_t region_bytes;
    uint8_t *mapped_addr;
    vemb_v16_shared_region_allocator_t *shared_allocator;
} tlc_core_warm_region_config_t;

typedef struct tlc_core_region_stats {
    uint32_t region_id;
    uint32_t is_local;
    uint32_t full;
    uint32_t capacity_slots;
    uint32_t used_slots;
} tlc_core_region_stats_t;

typedef struct tlc_core_stats {
    uint64_t warm_region_count;
    uint64_t warm_region_full_count;
    uint64_t warm_alloc_local;
    uint64_t warm_alloc_remote;
    uint64_t warm_alloc_fallback;
    uint64_t warm_alloc_cold_spill;
    uint64_t warm_alloc_fail;
    uint64_t warm_region_hash_local_pct;
} tlc_core_stats_t;

typedef struct tlc_core_config {
    uint32_t value_size;
    uint32_t warm_capacity;
    uint32_t hot_capacity;
    uint32_t cold_max_segments;
    uint32_t cold_segment_records;
    const tlc_core_warm_region_config_t *warm_regions;
    uint32_t warm_region_count;
    uint32_t local_region_weight;
} tlc_core_config_t;

typedef struct tlc_core tlc_core_t;

int tlc_core_create(tlc_core_t **out, const tlc_core_config_t *config);
void tlc_core_destroy(tlc_core_t *core);

int tlc_core_get_warm_slot(tlc_core_t *core,
                           const char *key,
                           uint32_t key_len,
                           uint64_t key_hash,
                           uint32_t *warm_slot);
int tlc_core_get_warm_location(tlc_core_t *core,
                               const char *key,
                               uint32_t key_len,
                               uint64_t key_hash,
                               tlc_warm_location_t *location);
int tlc_core_put(tlc_core_t *core,
                 const char *key,
                 uint32_t key_len,
                 uint64_t key_hash,
                 const void *value,
                 uint32_t value_size,
                 uint32_t *warm_slot);
int tlc_core_put_location(tlc_core_t *core,
                          const char *key,
                          uint32_t key_len,
                          uint64_t key_hash,
                          const void *value,
                          uint32_t value_size,
                          tlc_warm_location_t *location);
int tlc_core_cold_append(tlc_core_t *core,
                         const char *key,
                         uint32_t key_len,
                         uint64_t key_hash,
                         const void *value,
                         uint32_t value_size);
void tlc_core_get_stats(tlc_core_t *core, tlc_core_stats_t *stats);
uint32_t tlc_core_get_region_stats(tlc_core_t *core,
                                   tlc_core_region_stats_t *regions,
                                   uint32_t max_regions);

#endif
