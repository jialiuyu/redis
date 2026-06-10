#include "tlc_core.h"
#include "vemb_v16_tlc.h"
#include "zmalloc.h"
#include "macro.h"

#include <stdatomic.h>
#include <string.h>

static void make_handle(vemb_v16_tlc_t *tlc,
                        uint64_t key_hash,
                        const tlc_warm_location_t *location,
                        vemb_v16_vector_handle_t *handle) {
    (void)tlc;
    handle->region_id = location->region_id;
    handle->bytes = location->bytes;
    handle->offset = location->offset;
    handle->key_hash = key_hash;
}

int vemb_v16_tlc_create(vemb_v16_tlc_t **out,
                        uint32_t vector_dim,
                        uint32_t max_vectors,
                        const vemb_v16_tlc_warm_region_t *warm_regions,
                        uint32_t warm_region_count,
                        uint32_t local_region_weight) {
    RETURN_IF(!out || warm_region_count == 0 ||
        warm_region_count > TLC_CORE_MAX_WARM_REGIONS ||
        vector_dim == 0 || max_vectors == 0, -1);
    /* Region ids must stay unique because vector handles address data by region_id. */
    for (uint32_t i = 0; i < warm_region_count; i++) {
        for (uint32_t j = 0; j < i; j++) {
            if (warm_regions[j].region_id == warm_regions[i].region_id)
                return -1;
        }
    }

    vemb_v16_tlc_t *tlc = zcalloc(sizeof(vemb_v16_tlc_t));
    RETURN_IF(!tlc, -1);
    tlc->vector_dim = vector_dim;
    tlc->value_size = vector_dim * sizeof(float);
    tlc->max_vectors = max_vectors;
    tlc->region_id = warm_regions[0].region_id;
    tlc->backend_type = warm_regions[0].backend_type;
    tlc->warm_region_count = warm_region_count;
    tlc->warm_regions = zcalloc(sizeof(vemb_v16_tlc_warm_region_t) * warm_region_count);
    if (!tlc->warm_regions) {
        vemb_v16_tlc_destroy(tlc);
        return -1;
    }
    memcpy(tlc->warm_regions, warm_regions,
           sizeof(vemb_v16_tlc_warm_region_t) * warm_region_count);
    atomic_init(&tlc->sve_stats.lock_success, 0);
    atomic_init(&tlc->sve_stats.lock_failure, 0);

    tlc_core_warm_region_config_t core_regions[TLC_CORE_MAX_WARM_REGIONS];
    memset(core_regions, 0, sizeof(core_regions));
    for (uint32_t i = 0; i < warm_region_count; i++) {
        core_regions[i] = (tlc_core_warm_region_config_t){
            .region_id = warm_regions[i].region_id,
            .backend_type = warm_regions[i].backend_type,
            .is_local = warm_regions[i].is_local,
            .weight = warm_regions[i].weight,
            .value_size = warm_regions[i].value_size,
            .region_bytes = warm_regions[i].region_bytes,
            .mapped_addr = warm_regions[i].mapped_addr,
            .shared_allocator = warm_regions[i].shared_allocator,
        };
    }
    tlc_core_config_t core_config = {
        .value_size = tlc->value_size,
        .warm_capacity = max_vectors,
        .hot_capacity = TLC_CORE_DEFAULT_HOT_CAPACITY,
        .cold_max_segments = TLC_CORE_DEFAULT_COLD_MAX_SEGMENTS,
        .cold_segment_records = TLC_CORE_DEFAULT_COLD_SEGMENT_RECORDS,
        .warm_regions = core_regions,
        .warm_region_count = warm_region_count,
        .local_region_weight = local_region_weight,
    };
    if (bitmap_init(&tlc->bitmap, max_vectors) != 0 ||
        tlc_core_create(&tlc->core, &core_config) != 0) {
        vemb_v16_tlc_destroy(tlc);
        return -1;
    }

    *out = tlc;
    return 0;
}

void vemb_v16_tlc_destroy(vemb_v16_tlc_t *tlc) {
    RETURN_IF(!tlc);
    bitmap_destroy(&tlc->bitmap);
    tlc_core_destroy(tlc->core);
    if (tlc->warm_regions) zfree(tlc->warm_regions);
    zfree(tlc);
}

int vemb_v16_tlc_get_handle(vemb_v16_tlc_t *tlc,
                            const char *key,
                            uint32_t key_len,
                            uint64_t key_hash,
                            vemb_v16_vector_handle_t *handle,
                            uint32_t *warm_slot) {
    tlc_warm_location_t location = {0};
    if (tlc_core_get_warm_location(tlc->core, key, key_len,
                                   key_hash, &location) != 0) {
        return -1;
    }
    if (warm_slot) *warm_slot = location.local_slot;
    make_handle(tlc, key_hash, &location, handle);
    return 0;
}

int vemb_v16_tlc_put(vemb_v16_tlc_t *tlc,
                     const char *key,
                     uint32_t key_len,
                     uint64_t key_hash,
                     const float *vector,
                     uint32_t vector_bytes,
                     vemb_v16_vector_handle_t *handle,
                     uint32_t *warm_slot) {
    tlc_warm_location_t location = {0};
    if (tlc_core_put_location(tlc->core, key, key_len, key_hash,
                              vector, vector_bytes, &location) != 0) {
        return -1;
    }
    if (warm_slot) *warm_slot = location.local_slot;
    if (location.local_slot == TLC_CORE_INVALID_SLOT ||
        location.region_id == TLC_CORE_INVALID_REGION_ID) {
        if (handle) memset(handle, 0, sizeof(vemb_v16_vector_handle_t));
        return 0;
    }
    make_handle(tlc, key_hash, &location, handle);
    return 0;
}

int vemb_v16_tlc_cold_append(vemb_v16_tlc_t *tlc,
                             const char *key,
                             uint32_t key_len,
                             uint64_t key_hash,
                             const float *vector,
                             uint32_t vector_bytes) {
    return tlc_core_cold_append(tlc->core, key, key_len, key_hash,
                                vector, vector_bytes);
}

const vemb_v16_tlc_warm_region_t *vemb_v16_tlc_find_region(
    const vemb_v16_tlc_t *tlc, uint32_t region_id) {
    for (uint32_t i = 0; i < tlc->warm_region_count; i++) {
        if (tlc->warm_regions[i].region_id == region_id)
            return &tlc->warm_regions[i];
    }
    return NULL;
}

int vemb_v16_tlc_vector_slice(const vemb_v16_tlc_t *tlc,
                              const vemb_v16_vector_handle_t *handle,
                              const uint8_t **vector,
                              uint32_t *vector_bytes) {
    *vector = NULL;
    *vector_bytes = 0;
    const vemb_v16_tlc_warm_region_t *region =
        vemb_v16_tlc_find_region(tlc, handle->region_id);
    if (!region || !region->mapped_addr ||
        handle->offset > region->region_bytes ||
        handle->bytes > region->region_bytes - handle->offset) {
        return -1;
    }
    *vector = (const uint8_t *)region->mapped_addr + handle->offset;
    *vector_bytes = handle->bytes;
    return 0;
}

void vemb_v16_tlc_get_core_stats(vemb_v16_tlc_t *tlc,
                                 tlc_core_stats_t *stats) {
    tlc_core_get_stats(tlc->core, stats);
}

uint32_t vemb_v16_tlc_get_region_stats(vemb_v16_tlc_t *tlc,
                                       tlc_core_region_stats_t *regions,
                                       uint32_t max_regions) {
    return tlc_core_get_region_stats(tlc->core, regions, max_regions);
}
