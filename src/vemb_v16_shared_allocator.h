#ifndef __VEMB_V16_SHARED_ALLOCATOR_H
#define __VEMB_V16_SHARED_ALLOCATOR_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>

#include "vemb_v16_mapped_region.h"

#define VEMB_V16_SHARED_ALLOCATOR_MAGIC 0x5631414cu /* V1AL */
#define VEMB_V16_SHARED_ALLOCATOR_INITIALIZING 0x56314149u /* V1AI */
#define VEMB_V16_SHARED_ALLOCATOR_VERSION 1u
#define VEMB_V16_SHARED_ALLOCATOR_NAME_MAX 256u
#define VEMB_V16_SHARED_ALLOCATOR_ALIGNMENT 64u

#define VEMB_V16_SHARED_ALLOCATOR_OK 0
#define VEMB_V16_SHARED_ALLOCATOR_FULL 1
#define VEMB_V16_SHARED_ALLOCATOR_F_SLOT_META 0x01u

typedef enum vemb_v16_warm_slot_state {
    VEMB_V16_WARM_SLOT_FREE = 0,
    VEMB_V16_WARM_SLOT_FILLING = 1,
    VEMB_V16_WARM_SLOT_READY = 2,
    VEMB_V16_WARM_SLOT_EVICTING = 3,
} vemb_v16_warm_slot_state_t;

typedef enum vemb_v16_warm_slot_cold_state {
    VEMB_V16_WARM_SLOT_COLD_NONE = 0,
    VEMB_V16_WARM_SLOT_COLD_PENDING = 1,
    VEMB_V16_WARM_SLOT_COLD_COMMITTED = 2,
} vemb_v16_warm_slot_cold_state_t;

typedef struct vemb_v16_warm_slot_meta {
    _Atomic uint32_t state;
    uint32_t region_id;
    uint32_t local_slot;
    uint32_t bytes;
    _Atomic uint64_t owner_generation;
    _Atomic uint64_t write_seq;
    uint64_t key_hash;
    uint64_t key_fingerprint;
    _Atomic uint64_t last_access_ns;
    _Atomic uint32_t clock_bit;
    _Atomic uint32_t cold_state;
} vemb_v16_warm_slot_meta_t;

_Static_assert(sizeof(vemb_v16_warm_slot_meta_t) ==
                   VEMB_V16_SHARED_ALLOCATOR_ALIGNMENT,
               "vemb_v16_warm_slot_meta_t must be one aligned UB cacheline");

typedef struct vemb_v16_shared_region_allocator {
    _Atomic uint32_t magic;
    uint32_t version;
    uint32_t region_id;
    uint32_t capacity_slots;
    _Atomic uint32_t next_slot;
    _Atomic uint32_t full;
    _Atomic uint32_t used_slots;
    uint32_t flags;
    uint64_t generation;
    uint8_t reserved[24];
} vemb_v16_shared_region_allocator_t;

_Static_assert(sizeof(vemb_v16_shared_region_allocator_t) ==
                   VEMB_V16_SHARED_ALLOCATOR_ALIGNMENT,
               "vemb_v16_shared_region_allocator_t must be one aligned UB cacheline");

typedef struct vemb_v16_shared_allocator_mapping {
    const vemb_v16_mapped_region_t *mapping;
    vemb_v16_mapped_region_t owned_mapping;
    int owns_mapping;
    int fd;
    uint32_t backend_type;
    size_t mapping_size;
    uint64_t mmap_offset;
    uint64_t mmap_aligned_offset;
    void *mapping_addr;
    char name[VEMB_V16_SHARED_ALLOCATOR_NAME_MAX];
    vemb_v16_shared_region_allocator_t *allocator;
} vemb_v16_shared_allocator_mapping_t;

int vemb_v16_shared_allocator_name_from_region_path(const char *region_path,
                                                    uint32_t region_id,
                                                    char *out,
                                                    size_t out_len);
int vemb_v16_shared_allocator_open(vemb_v16_shared_allocator_mapping_t *mapping,
                                   uint32_t backend_type,
                                   const char *path,
                                   uint64_t mmap_offset,
                                   uint32_t region_id,
                                   uint32_t capacity_slots);
int vemb_v16_shared_allocator_attach(vemb_v16_shared_allocator_mapping_t *mapping,
                                     const vemb_v16_mapped_region_t *region,
                                     uint64_t view_offset,
                                     uint32_t backend_type,
                                     const char *path,
                                     uint64_t mmap_offset,
                                     uint32_t region_id,
                                     uint32_t capacity_slots);
int vemb_v16_shared_allocator_reset(uint32_t backend_type,
                                    const char *path,
                                    uint64_t mmap_offset,
                                    uint32_t region_id,
                                    uint32_t capacity_slots);
void vemb_v16_shared_allocator_close(vemb_v16_shared_allocator_mapping_t *mapping);
int vemb_v16_shared_allocator_unlink(const char *name);
int vemb_v16_shared_allocator_alloc(vemb_v16_shared_region_allocator_t *allocator,
                                    uint32_t *local_slot);
size_t vemb_v16_shared_allocator_layout_bytes(uint32_t capacity_slots);
vemb_v16_warm_slot_meta_t *vemb_v16_shared_allocator_slot_meta(
    vemb_v16_shared_region_allocator_t *allocator);
const vemb_v16_warm_slot_meta_t *vemb_v16_shared_allocator_const_slot_meta(
    const vemb_v16_shared_region_allocator_t *allocator);
uint32_t vemb_v16_shared_allocator_full(const vemb_v16_shared_region_allocator_t *allocator);
uint32_t vemb_v16_shared_allocator_used_slots(const vemb_v16_shared_region_allocator_t *allocator);

#endif
