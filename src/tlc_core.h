#ifndef __TLC_CORE_H
#define __TLC_CORE_H

#include "sve_operation.h"

#include <stddef.h>
#include <stdint.h>

#define TLC_CORE_DEFAULT_HOT_CAPACITY 65536u
#define TLC_CORE_DEFAULT_COLD_MAX_SEGMENTS 64u
#define TLC_CORE_DEFAULT_COLD_SEGMENT_RECORDS 4096u
#define TLC_CORE_INVALID_SLOT UINT32_MAX

typedef struct tlc_core_config {
    uint32_t value_size;
    uint32_t warm_capacity;
    uint32_t hot_capacity;
    uint32_t cold_max_segments;
    uint32_t cold_segment_records;
    uint8_t *warm_data;
    size_t warm_data_bytes;
} tlc_core_config_t;

typedef struct tlc_core tlc_core_t;

int tlc_core_create(tlc_core_t **out, const tlc_core_config_t *config);
void tlc_core_destroy(tlc_core_t *core);

int tlc_core_get_warm_slot(tlc_core_t *core,
                           const char *key,
                           uint32_t key_len,
                           uint64_t key_hash,
                           uint32_t *warm_slot);
int tlc_core_put(tlc_core_t *core,
                 const char *key,
                 uint32_t key_len,
                 uint64_t key_hash,
                 const void *value,
                 uint32_t value_size,
                 uint32_t *warm_slot);
int tlc_core_cold_append(tlc_core_t *core,
                         const char *key,
                         uint32_t key_len,
                         uint64_t key_hash,
                         const void *value,
                         uint32_t value_size);

#endif
