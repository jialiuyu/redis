#define _GNU_SOURCE

#include "macro.h"
#include "vemb_v16_storage.h"
#include "vemb_v16_log.h"
#include "zmalloc.h"

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

static char *trim_ws(char *s) {
    while (*s && isspace((unsigned char)*s)) s++;
    char *end = s + strlen(s);
    while (end > s && isspace((unsigned char)end[-1])) end--;
    *end = '\0';
    return s;
}

static void strip_comment(char *s) {
    for (; *s; s++) {
        if (*s == '#') {
            *s = '\0';
            return;
        }
    }
}

static int parse_u32_value(const char *s, uint32_t *out) {
    char *end = NULL;
    unsigned long v = strtoul(s, &end, 10);
    RETURN_IF(end == s || *trim_ws(end) != '\0' || v > UINT32_MAX, -1);
    *out = (uint32_t)v;
    return 0;
}

static int parse_u64_value(const char *s, uint64_t *out) {
    char *end = NULL;
    unsigned long long v = strtoull(s, &end, 10);
    RETURN_IF(end == s || *trim_ws(end) != '\0', -1);
    *out = (uint64_t)v;
    return 0;
}

static int parse_bool_value(const char *s, uint32_t *out) {
    if (!strcmp(s, "true") || !strcmp(s, "yes") || !strcmp(s, "1")) {
        *out = 1;
        return 0;
    }
    if (!strcmp(s, "false") || !strcmp(s, "no") || !strcmp(s, "0")) {
        *out = 0;
        return 0;
    }
    return -1;
}

static int parse_backend_value(const char *s, uint32_t *out) {
    if (!strcmp(s, "shm") || !strcmp(s, "local_shm") ||
        !strcmp(s, "mock_ub") || !strcmp(s, "shm_mock_ub")) {
        *out = VEMB_V16_REGION_LOCAL_SHM;
        return 0;
    }
    if (!strcmp(s, "ub")) {
        *out = VEMB_V16_REGION_UB;
        return 0;
    }
    return -1;
}

static void manifest_region_defaults(vemb_v16_manifest_region_t *region,
                                     uint32_t value_size) {
    memset(region, 0, sizeof(*region));
    region->backend_type = VEMB_V16_REGION_LOCAL_SHM;
    region->weight = 1;
    region->value_size = value_size;
}

static int manifest_push_region(vemb_v16_warm_regions_manifest_t *manifest,
                                const vemb_v16_manifest_region_t *region) {
    RETURN_IF(manifest->region_count >= VEMB_V16_MAX_MANIFEST_REGIONS, -1);
    RETURN_IF(!region->path[0] || region->value_size == 0 ||
              region->region_bytes < region->value_size, -1);
    for (uint32_t i = 0; i < manifest->region_count; i++) {
        RETURN_IF(manifest->regions[i].region_id == region->region_id, -1);
    }
    manifest->regions[manifest->region_count++] = *region;
    return 0;
}

static int parse_manifest_field(vemb_v16_warm_regions_manifest_t *manifest,
                                vemb_v16_manifest_region_t *current,
                                int in_region,
                                const char *key,
                                const char *value) {
    if (!in_region) {
        if (!strcmp(key, "local_ub_node_id")) {
            if (parse_u32_value(value, &manifest->local_ub_node_id) != 0)
                return -1;
            manifest->has_local_ub_node_id = 1;
            return 0;
        }
        if (!strcmp(key, "local_region_weight")) {
            return parse_u32_value(value, &manifest->local_region_weight);
        }
        return 0;
    }

    if (!strcmp(key, "region_id"))
        return parse_u32_value(value, &current->region_id);
    if (!strcmp(key, "provider") || !strcmp(key, "backend"))
        return parse_backend_value(value, &current->backend_type);
    if (!strcmp(key, "path")) {
        RETURN_IF(strlen(value) >= sizeof(current->path), -1);
        strcpy(current->path, value);
        return 0;
    }
    if (!strcmp(key, "mmap_offset"))
        return parse_u64_value(value, &current->mmap_offset);
    if (!strcmp(key, "bytes") || !strcmp(key, "region_bytes"))
        return parse_u64_value(value, &current->region_bytes);
    if (!strcmp(key, "value_size"))
        return parse_u32_value(value, &current->value_size);
    if (!strcmp(key, "home_ub_node_id"))
        return parse_u32_value(value, &current->home_ub_node_id);
    if (!strcmp(key, "weight"))
        return parse_u32_value(value, &current->weight);
    if (!strcmp(key, "is_local")) {
        if (parse_bool_value(value, &current->is_local) != 0)
            return -1;
        current->has_is_local = 1;
        return 0;
    }
    return 0;
}

int vemb_v16_parse_warm_regions_manifest(
        const char *path,
        uint32_t value_size,
        vemb_v16_warm_regions_manifest_t *manifest) {
    FILE *fp = fopen(path, "r");
    RETURN_IF(!fp, -1);
    memset(manifest, 0, sizeof(*manifest));
    manifest->local_region_weight = 4;

    char line[512];
    vemb_v16_manifest_region_t current;
    int in_region = 0;
    manifest_region_defaults(&current, value_size);

    while (fgets(line, sizeof(line), fp)) {
        strip_comment(line);
        char *p = trim_ws(line);
        if (!p[0])
            continue;
        if (!strncmp(p, "- ", 2)) {
            if (in_region &&
                manifest_push_region(manifest, &current) != 0) {
                fclose(fp);
                return -1;
            }
            manifest_region_defaults(&current, value_size);
            in_region = 1;
            p = trim_ws(p + 2);
            if (!p[0])
                continue;
        }
        char *colon = strchr(p, ':');
        if (!colon)
            continue;
        *colon = '\0';
        char *key = trim_ws(p);
        char *value = trim_ws(colon + 1);
        if (!value[0] || !strcmp(key, "warm_regions"))
            continue;
        if (parse_manifest_field(manifest, &current, in_region,
                                 key, value) != 0) {
            fclose(fp);
            return -1;
        }
    }
    if (in_region &&
        manifest_push_region(manifest, &current) != 0) {
        fclose(fp);
        return -1;
    }
    fclose(fp);
    RETURN_IF(manifest->region_count == 0, -1);

    for (uint32_t i = 0; i < manifest->region_count; i++) {
        vemb_v16_manifest_region_t *region = &manifest->regions[i];
        if (!region->has_is_local && manifest->has_local_ub_node_id)
            region->is_local = region->home_ub_node_id == manifest->local_ub_node_id;
    }
    return 0;
}

static int unlink_shm_if_exists(const char *name) {
    if (!name || name[0] != '/')
        return -1;
    if (shm_unlink(name) == 0)
        return 0;
    return errno == ENOENT ? 0 : -1;
}

int vemb_v16_storage_reset_manifest_regions(const vemb_v16_warm_regions_manifest_t *manifest) {
    RETURN_IF(!manifest, -1);
    for (uint32_t i = 0; i < manifest->region_count; i++) {
        const vemb_v16_manifest_region_t *region = &manifest->regions[i];
        uint32_t capacity_slots = (uint32_t)(region->region_bytes / region->value_size);
        if (region->backend_type == VEMB_V16_REGION_LOCAL_SHM) {
            char allocator_name[VEMB_V16_SHARED_ALLOCATOR_NAME_MAX];
            int rc = vemb_v16_shared_allocator_name_from_region_path(
                region->path,
                region->region_id,
                allocator_name,
                sizeof(allocator_name));
            serverLog(LL_NOTICE,
                      "reset warm allocator shm name: region_id=%u path=%s rc=%d status=%s",
                      region->region_id,
                      region->path,
                      rc,
                      strerror(errno));
            RETURN_IF(rc != 0, -1);

            rc = unlink_shm_if_exists(allocator_name);
            int unlink_allocator_errno = errno;
            serverLog(LL_NOTICE,
                      "reset warm allocator shm: region_id=%u allocator=%s rc=%d status=%s",
                      region->region_id,
                      allocator_name,
                      rc,
                      strerror(unlink_allocator_errno));
            RETURN_IF(rc != 0, -1);

            rc = unlink_shm_if_exists(region->path);
            int unlink_payload_errno = errno;
            serverLog(LL_NOTICE,
                      "reset warm payload shm: region_id=%u path=%s rc=%d status=%s",
                      region->region_id,
                      region->path,
                      rc,
                      strerror(unlink_payload_errno));
            RETURN_IF(rc != 0, -1);
        } else if (region->backend_type == VEMB_V16_REGION_UB) {
            int rc = vemb_v16_shared_allocator_reset(region->backend_type,
                                                     region->path,
                                                     region->mmap_offset,
                                                     region->region_id,
                                                     capacity_slots);
            int reset_allocator_errno = errno;
            serverLog(LL_NOTICE,
                      "reset warm allocator ub: region_id=%u path=%s offset=%llu rc=%d status=%s",
                      region->region_id,
                      region->path,
                      (unsigned long long)region->mmap_offset,
                      rc,
                      strerror(reset_allocator_errno));
            RETURN_IF(rc != 0, -1);
        } else {
            serverLog(LL_WARNING,
                      "unsupported warm region backend type during reset: region_id=%u backend_type=%u",
                      region->region_id,
                      region->backend_type);
            return -1;
        }
    }
    return 0;
}

int vemb_v16_storage_ctx_create_from_manifest(vemb_v16_storage_ctx_t **out,
                                              uint32_t vector_dim,
                                              uint32_t vector_stride,
                                              uint32_t max_vectors,
                                              const vemb_v16_warm_regions_manifest_t *manifest) {
    RETURN_IF(!out || !manifest ||
              vector_dim == 0 || vector_stride == 0 ||
              max_vectors == 0 || manifest->region_count == 0 ||
              manifest->region_count > VEMB_V16_MAX_MANIFEST_REGIONS, -1);

    vemb_v16_storage_ctx_t *storage = zcalloc(sizeof(*storage));
    RETURN_IF(!storage, -1);
    storage->vector_dim = vector_dim;
    storage->vector_stride = vector_stride;
    storage->max_vectors = max_vectors;
    storage->warm_region_count = manifest->region_count;
    storage->local_region_weight = manifest->local_region_weight ?
        manifest->local_region_weight : 4;
    storage->warm_providers =
        zcalloc(sizeof(*storage->warm_providers) * storage->warm_region_count);
    storage->warm_data_mappings =
        zcalloc(sizeof(*storage->warm_data_mappings) * storage->warm_region_count);
    storage->warm_allocator_mappings =
        zcalloc(sizeof(*storage->warm_allocator_mappings) * storage->warm_region_count);
    storage->warm_allocators =
        zcalloc(sizeof(*storage->warm_allocators) * storage->warm_region_count);
    if (!storage->warm_providers || !storage->warm_data_mappings ||
        !storage->warm_allocator_mappings || !storage->warm_allocators) {
        if (storage->warm_providers)
            zfree(storage->warm_providers);
        if (storage->warm_data_mappings)
            zfree(storage->warm_data_mappings);
        if (storage->warm_allocator_mappings)
            zfree(storage->warm_allocator_mappings);
        if (storage->warm_allocators)
            zfree(storage->warm_allocators);
        zfree(storage);
        return -1;
    }
    for (uint32_t i = 0; i < storage->warm_region_count; i++) {
        storage->warm_providers[i].fd = -1;
        storage->warm_allocators[i].fd = -1;
        storage->warm_data_mappings[i].fd = -1;
        storage->warm_allocator_mappings[i].fd = -1;
    }

    vemb_v16_tlc_warm_region_t warm_regions[VEMB_V16_MAX_MANIFEST_REGIONS];
    memset(warm_regions, 0, sizeof(warm_regions));
    for (uint32_t i = 0; i < storage->warm_region_count; i++) {
        const vemb_v16_manifest_region_t *src = &manifest->regions[i];
        uint32_t capacity_slots =
            (uint32_t)(src->region_bytes / src->value_size);
        if (src->backend_type == VEMB_V16_REGION_UB) {
            /* One UB backing region: [64B allocator header][payload bytes...] */
            if (vemb_v16_mapped_region_open(&storage->warm_data_mappings[i],
                                            src->backend_type,
                                            src->path,
                                            src->mmap_offset,
                                            sizeof(vemb_v16_shared_region_allocator_t) +
                                                (size_t)src->region_bytes) != 0) {
                serverLog(LL_WARNING,
                          "failed to open warm ub backing region: region_id=%u path=%s",
                          src->region_id, src->path);
                GOTO_IF(1, err);
            }
            if (vemb_v16_warm_provider_attach(&storage->warm_providers[i],
                                              &storage->warm_data_mappings[i],
                                              sizeof(vemb_v16_shared_region_allocator_t),
                                              src->region_id,
                                              src->backend_type,
                                              src->path,
                                              src->mmap_offset +
                                                  sizeof(vemb_v16_shared_region_allocator_t),
                                              src->value_size,
                                              src->region_bytes,
                                              src->home_ub_node_id,
                                              src->is_local,
                                              src->weight) != 0) {
                serverLog(LL_WARNING,
                          "failed to attach warm ub provider: region_id=%u path=%s",
                          src->region_id, src->path);
                GOTO_IF(1, err);
            }
            if (vemb_v16_shared_allocator_attach(&storage->warm_allocators[i],
                                                 &storage->warm_data_mappings[i],
                                                 0,
                                                 src->backend_type,
                                                 src->path,
                                                 src->mmap_offset,
                                                 src->region_id,
                                                 capacity_slots) != 0) {
                serverLog(LL_WARNING,
                          "failed to attach warm ub shared allocator: region_id=%u path=%s",
                          src->region_id, src->path);
                GOTO_IF(1, err);
            }
        } else if (src->backend_type == VEMB_V16_REGION_LOCAL_SHM) {
            if (vemb_v16_mapped_region_open(&storage->warm_data_mappings[i],
                                            src->backend_type,
                                            src->path,
                                            src->mmap_offset,
                                            (size_t)src->region_bytes) != 0) {
                serverLog(LL_WARNING,
                          "failed to open warm shm payload region: region_id=%u path=%s",
                          src->region_id, src->path);
                GOTO_IF(1, err);
            }
            if (vemb_v16_warm_provider_attach(&storage->warm_providers[i],
                                              &storage->warm_data_mappings[i],
                                              0,
                                              src->region_id,
                                              src->backend_type,
                                              src->path,
                                              src->mmap_offset,
                                              src->value_size,
                                              src->region_bytes,
                                              src->home_ub_node_id,
                                              src->is_local,
                                              src->weight) != 0) {
                serverLog(LL_WARNING,
                          "failed to attach warm shm provider: region_id=%u path=%s",
                          src->region_id, src->path);
                GOTO_IF(1, err);
            }

            char allocator_name[VEMB_V16_SHARED_ALLOCATOR_NAME_MAX];
            if (vemb_v16_shared_allocator_name_from_region_path(
                    src->path,
                    src->region_id,
                    allocator_name,
                    sizeof(allocator_name)) != 0) {
                serverLog(LL_WARNING,
                          "failed to derive warm shared allocator name: region_id=%u path=%s",
                          src->region_id, src->path);
                GOTO_IF(1, err);
            }
            if (vemb_v16_mapped_region_open(&storage->warm_allocator_mappings[i],
                                            src->backend_type,
                                            allocator_name,
                                            0,
                                            sizeof(vemb_v16_shared_region_allocator_t)) != 0) {
                serverLog(LL_WARNING,
                          "failed to open warm shm allocator backing: region_id=%u path=%s allocator=%s",
                          src->region_id, src->path, allocator_name);
                GOTO_IF(1, err);
            }
            if (vemb_v16_shared_allocator_attach(&storage->warm_allocators[i],
                                                 &storage->warm_allocator_mappings[i],
                                                 0,
                                                 src->backend_type,
                                                 allocator_name,
                                                 0,
                                                 src->region_id,
                                                 capacity_slots) != 0) {
                serverLog(LL_WARNING,
                          "failed to attach warm shm shared allocator: region_id=%u path=%s allocator=%s",
                          src->region_id, src->path, allocator_name);
                GOTO_IF(1, err);
            }
        } else {
            // Should not happen due to manifest validation, but just in case.
            serverLog(LL_WARNING,
                      "unsupported warm region backend type: region_id=%u backend_type=%u",
                        src->region_id, src->backend_type);
        }
        warm_regions[i] = storage->warm_providers[i].region;
        warm_regions[i].shared_allocator =
            storage->warm_allocators[i].allocator;
    }

    vemb_v16_warm_provider_t *first = &storage->warm_providers[0];
    storage->warm_region_id = first->region.region_id;
    storage->warm_backend_type = first->region.backend_type;
    storage->warm_mmap_offset = first->region.mmap_offset;
    storage->vector_region = first->region.mapped_addr;
    storage->vector_region_size = first->region.region_bytes;
    strncpy(storage->vector_region_name, first->path, sizeof(storage->vector_region_name) - 1);

    if (vemb_v16_tlc_create(&storage->tlc,
                            storage->vector_dim,
                            storage->max_vectors,
                            warm_regions,
                            storage->warm_region_count,
                            storage->local_region_weight) != 0) {
        serverLog(LL_WARNING,
                  "vemb_v16 tlc create failed: dim=%u max_vectors=%u regions=%u",
                  storage->vector_dim,
                  storage->max_vectors,
                  storage->warm_region_count);
        GOTO_IF(1, err);
    }

    *out = storage;
    return 0;

err:
    vemb_v16_storage_ctx_destroy(storage);
    return -1;
}

void vemb_v16_storage_ctx_destroy(vemb_v16_storage_ctx_t *storage) {
    assert(storage != NULL);
    vemb_v16_tlc_destroy(storage->tlc);
    if (storage->warm_providers) {
        for (uint32_t i = 0; i < storage->warm_region_count; i++)
            vemb_v16_warm_provider_close(&storage->warm_providers[i]);
        if (storage->warm_providers != &storage->warm_provider)
            zfree(storage->warm_providers);
    } else {
        vemb_v16_warm_provider_close(&storage->warm_provider);
    }
    if (storage->warm_allocators) {
        for (uint32_t i = 0; i < storage->warm_region_count; i++)
            vemb_v16_shared_allocator_close(&storage->warm_allocators[i]);
        zfree(storage->warm_allocators);
    }
    if (storage->warm_data_mappings) {
        for (uint32_t i = 0; i < storage->warm_region_count; i++)
            vemb_v16_mapped_region_close(&storage->warm_data_mappings[i]);
        zfree(storage->warm_data_mappings);
    }
    if (storage->warm_allocator_mappings) {
        for (uint32_t i = 0; i < storage->warm_region_count; i++)
            vemb_v16_mapped_region_close(&storage->warm_allocator_mappings[i]);
        zfree(storage->warm_allocator_mappings);
    }
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

    vemb_v16_warm_provider_t *provider = storage->warm_providers ?
        &storage->warm_providers[0] : &storage->warm_provider;
    desc->warm_region_id = provider->region.region_id;
    desc->warm_backend_type = provider->region.backend_type;
    desc->warm_region_bytes = provider->region.region_bytes;
    desc->warm_mmap_offset = provider->region.mmap_offset;
    strncpy(desc->vector_region_name, storage->vector_region_name,
            sizeof(desc->vector_region_name) - 1);
    uint32_t count = storage->warm_region_count;
    if (count > VEMB_V16_MAX_DESC_WARM_REGIONS)
        count = VEMB_V16_MAX_DESC_WARM_REGIONS;
    desc->warm_region_count = count;
    for (uint32_t i = 0; i < count; i++) {
        vemb_v16_warm_provider_t *p = &storage->warm_providers[i];
        desc->warm_regions[i].region_id = p->region.region_id;
        desc->warm_regions[i].backend_type = p->region.backend_type;
        desc->warm_regions[i].region_bytes = p->region.region_bytes;
        desc->warm_regions[i].mmap_offset = p->region.mmap_offset;
        strncpy(desc->warm_regions[i].path, p->path,
                sizeof(desc->warm_regions[i].path) - 1);
    }
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
    vemb_v16_vector_handle_t handle = {
        .region_id = resp->region_id,
        .bytes = resp->vector_bytes,
        .offset = resp->vector_offset,
        .key_hash = resp->key_hash,
    };
    if (vemb_v16_tlc_vector_slice(storage->tlc, &handle, vector,
                                  vector_bytes) != 0) {
        resp->status = VEMB_V16_STATUS_ERR;
        resp->vector_bytes = 0;
        resp->vector_offset = 0;
        return 0;
    }
    return 0;
}
