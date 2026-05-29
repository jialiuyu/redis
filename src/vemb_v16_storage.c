#define _GNU_SOURCE

#include "macro.h"
#include "vemb_v16_storage.h"
#include "vemb_v16_log.h"
#include "zmalloc.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

int vemb_v16_storage_ctx_create(vemb_v16_storage_ctx_t **out,
                                uint32_t vector_dim,
                                uint32_t vector_stride,
                                uint32_t max_vectors,
                                const char *vector_region_name,
                                uint32_t warm_region_id,
                                uint32_t warm_backend_type,
                                uint64_t warm_mmap_offset) {
    vemb_v16_storage_ctx_t *storage = zcalloc(sizeof(*storage));
    RETURN_IF(!storage, -1);

    storage->vector_dim = vector_dim;
    storage->vector_stride = vector_stride;
    storage->max_vectors = max_vectors;
    storage->warm_region_id = warm_region_id;
    storage->warm_backend_type = warm_backend_type ?
        warm_backend_type : VEMB_V16_REGION_LOCAL_SHM;
    storage->warm_mmap_offset = warm_mmap_offset;
    storage->vector_region_size = (size_t)vector_stride * max_vectors;

    if (!vector_region_name || !vector_region_name[0])
        vector_region_name = VEMB_V16_DEFAULT_VECTOR_REGION;
    if (vector_region_name[0] != '/' ||
        strlen(vector_region_name) >= sizeof(storage->vector_region_name)) {
        serverLog(LL_WARNING, "invalid vemb_v16 vector region name: %s",
                  vector_region_name);
        zfree(storage);
        return -1;
    }
    strncpy(storage->vector_region_name, vector_region_name, sizeof(storage->vector_region_name) - 1);
    if (vemb_v16_warm_provider_open(&storage->warm_provider,
                                    storage->warm_region_id,
                                    storage->warm_backend_type,
                                    storage->vector_region_name,
                                    storage->warm_mmap_offset,
                                    storage->vector_stride,
                                    storage->max_vectors) != 0) {
        zfree(storage);
        return -1;
    }
    storage->vector_region = storage->warm_provider.region.mapped_addr;
    storage->vector_region_size = storage->warm_provider.region.region_bytes;

    if (vemb_v16_tlc_create(&storage->tlc,
                            storage->vector_dim,
                            storage->max_vectors,
                            &storage->warm_provider.region) != 0) {
        serverLog(LL_WARNING, "vemb_v16 tlc create failed: dim=%u max_vectors=%u",
                  storage->vector_dim, storage->max_vectors);
        vemb_v16_warm_provider_close(&storage->warm_provider);
        zfree(storage);
        return -1;
    }

    *out = storage;
    return 0;
}

void vemb_v16_storage_ctx_destroy(vemb_v16_storage_ctx_t *storage) {
    assert(storage != NULL);
    vemb_v16_tlc_destroy(storage->tlc);
    vemb_v16_warm_provider_close(&storage->warm_provider);
    zfree(storage);
}

const char *vemb_v16_storage_vector_region_name(vemb_v16_storage_ctx_t *storage) {
    assert(storage != NULL);
    return storage->vector_region_name;
}

size_t vemb_v16_storage_vector_region_size(vemb_v16_storage_ctx_t *storage) {
    assert(storage != NULL);
    return storage->vector_region_size;
}

sve_operation_stats_t *vemb_v16_storage_sve_stats(vemb_v16_storage_ctx_t *storage) {
    assert(storage != NULL);
    assert(storage->tlc != NULL);
    return &storage->tlc->sve_stats;
}

void vemb_v16_storage_fill_channel_desc(vemb_v16_storage_ctx_t *storage,
                                        vemb_v16_channel_desc_t *desc) {
    assert(storage != NULL);
    assert(desc != NULL);

    desc->warm_region_id = storage->warm_region_id;
    desc->warm_backend_type = storage->warm_backend_type;
    desc->warm_region_bytes = storage->warm_provider.region.region_bytes;
    desc->warm_mmap_offset = storage->warm_provider.region.mmap_offset;
    strncpy(desc->vector_region_name, storage->vector_region_name,
            sizeof(desc->vector_region_name) - 1);
}

int vemb_v16_storage_vector_slice(vemb_v16_storage_ctx_t *storage,
                                  vemb_v16_resp_t *resp,
                                  const uint8_t **vector,
                                  uint32_t *vector_bytes) {
    assert(storage != NULL);
    assert(resp != NULL);
    assert(vector != NULL);
    assert(vector_bytes != NULL);

    *vector = NULL;
    *vector_bytes = 0;
    if (!(resp->flags & VEMB_V16_REQ_F_INLINE_VECTOR) ||
        resp->status != VEMB_V16_STATUS_OK ||
        resp->vector_bytes == 0) {
        return 0;
    }
    if (!storage->vector_region ||
        resp->vector_offset > storage->vector_region_size ||
        resp->vector_bytes > storage->vector_region_size - resp->vector_offset) {
        resp->status = VEMB_V16_STATUS_ERR;
        resp->vector_bytes = 0;
        resp->vector_offset = 0;
        return 0;
    }
    *vector = storage->vector_region + resp->vector_offset;
    *vector_bytes = resp->vector_bytes;
    return 0;
}
