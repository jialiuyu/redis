#include "tlc_core.h"
#include "vemb_v16_tlc.h"
#include "zmalloc.h"

#include <stdatomic.h>
#include <string.h>

static void make_handle(vemb_v16_tlc_t *tlc,
                        uint64_t key_hash,
                        uint32_t warm_slot,
                        vemb_v16_vector_handle_t *handle) {
    if (!handle) return;
    handle->region_id = tlc->region_id;
    handle->bytes = tlc->value_size;
    handle->offset = (uint64_t)warm_slot * tlc->value_size;
    handle->key_hash = key_hash;
}

int vemb_v16_tlc_create(vemb_v16_tlc_t **out,
                        uint32_t vector_dim,
                        uint32_t max_vectors,
                        const vemb_v16_tlc_warm_region_t *warm_region) {
    if (!out || !warm_region || !warm_region->mapped_addr ||
        vector_dim == 0 || max_vectors == 0 ||
        warm_region->value_size != vector_dim * sizeof(float) ||
        warm_region->region_bytes <
            (uint64_t)warm_region->value_size * max_vectors) {
        return -1;
    }

    vemb_v16_tlc_t *tlc = zcalloc(sizeof(*tlc));
    if (!tlc) return -1;
    tlc->vector_dim = vector_dim;
    tlc->value_size = warm_region->value_size;
    tlc->max_vectors = max_vectors;
    tlc->region_id = warm_region->region_id;
    tlc->backend_type = warm_region->backend_type;
    tlc->ubas = (ub_address_space_t){
        .size = (size_t)warm_region->region_bytes,
        .mapped_addr = warm_region->mapped_addr,
        .mapping_addr = warm_region->mapped_addr,
        .mapping_size = (size_t)warm_region->region_bytes,
        .vector_stride_bytes = warm_region->value_size,
        .shm_fd = -1,
    };
    atomic_init(&tlc->sve_stats.lock_success, 0);
    atomic_init(&tlc->sve_stats.lock_failure, 0);

    tlc_core_config_t core_config = {
        .value_size = warm_region->value_size,
        .warm_capacity = max_vectors,
        .hot_capacity = TLC_CORE_DEFAULT_HOT_CAPACITY,
        .cold_max_segments = TLC_CORE_DEFAULT_COLD_MAX_SEGMENTS,
        .cold_segment_records = TLC_CORE_DEFAULT_COLD_SEGMENT_RECORDS,
        .warm_data = warm_region->mapped_addr,
        .warm_data_bytes = (size_t)warm_region->region_bytes,
    };
    if (bitmap_init(&tlc->bitmap, max_vectors) != 0 ||
        tlc_core_create(&tlc->core, &core_config) != 0) {
        vemb_v16_tlc_destroy(tlc);
        return -1;
    }
    sve_gather_ctx_init(&tlc->gather_ctx,
                        &tlc->ubas,
                        &tlc->bitmap,
                        tlc->vector_dim,
                        tlc->value_size,
                        tlc->max_vectors,
                        &tlc->sve_stats);

    *out = tlc;
    return 0;
}

void vemb_v16_tlc_destroy(vemb_v16_tlc_t *tlc) {
    if (!tlc) return;
    bitmap_destroy(&tlc->bitmap);
    tlc_core_destroy(tlc->core);
    zfree(tlc);
}

int vemb_v16_tlc_get_handle(vemb_v16_tlc_t *tlc,
                            const char *key,
                            uint32_t key_len,
                            uint64_t key_hash,
                            vemb_v16_vector_handle_t *handle,
                            uint32_t *warm_slot) {
    if (tlc_core_get_warm_slot(tlc->core, key, key_len,
                               key_hash, warm_slot) != 0) {
        return -1;
    }
    make_handle(tlc, key_hash, *warm_slot, handle);
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
    if (tlc_core_put(tlc->core, key, key_len, key_hash,
                     vector, vector_bytes, warm_slot) != 0) {
        return -1;
    }
    if (*warm_slot == TLC_CORE_INVALID_SLOT) {
        memset(handle, 0, sizeof(*handle));
        return 0;
    }
    make_handle(tlc, key_hash, *warm_slot, handle);
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
