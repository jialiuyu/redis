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

static uint32_t pow2_ceil_u32(uint64_t value) {
    uint32_t p = 1;
    while ((uint64_t)p < value && p < (1u << 30))
        p <<= 1;
    return p;
}

static int cmp_u32(const void *a, const void *b) {
    uint32_t va = *(const uint32_t *)a;
    uint32_t vb = *(const uint32_t *)b;
    if (va < vb) return -1;
    if (va > vb) return 1;
    return 0;
}

static int owner_hash_node_cmp(const void *a, const void *b) {
    const vemb_v16_storage_owner_hash_node_t *ha = a;
    const vemb_v16_storage_owner_hash_node_t *hb = b;
    if (ha->hash_value < hb->hash_value) return -1;
    if (ha->hash_value > hb->hash_value) return 1;
    if (ha->owner_id < hb->owner_id) return -1;
    if (ha->owner_id > hb->owner_id) return 1;
    return 0;
}

static uint32_t storage_owner_resolver(uint64_t key_hash,
                                       const char *key,
                                       uint32_t key_len,
                                       void *arg) {
    (void)key;
    (void)key_len;
    const vemb_v16_storage_ctx_t *storage = arg;
    if (!storage || storage->owner_hash_node_count == 0)
        return 0;

    uint32_t hash = (uint32_t)key_hash;
    uint32_t left = 0;
    uint32_t right = storage->owner_hash_node_count;
    while (left < right) {
        uint32_t mid = left + (right - left) / 2;
        if (storage->owner_hash_nodes[mid].hash_value < hash)
            left = mid + 1;
        else
            right = mid;
    }
    if (left >= storage->owner_hash_node_count)
        left = 0;
    return storage->owner_hash_nodes[left].owner_id;
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

static void manifest_remote_meta_view_defaults(
        vemb_v16_manifest_remote_meta_view_t *view) {
    memset(view, 0, sizeof(*view));
    view->backend_type = VEMB_V16_REGION_LOCAL_SHM;
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

static int manifest_push_remote_meta_view(
        vemb_v16_warm_regions_manifest_t *manifest,
        const vemb_v16_manifest_remote_meta_view_t *view) {
    RETURN_IF(manifest->remote_meta_view_count >=
              VEMB_V16_MAX_MANIFEST_REMOTE_META_VIEWS, -1);
    RETURN_IF(!view->has_owner_id || !view->path[0] ||
              view->entry_count == 0, -1);
    for (uint32_t i = 0; i < manifest->remote_meta_view_count; i++) {
        RETURN_IF(manifest->remote_meta_views[i].owner_id == view->owner_id,
                  -1);
    }
    manifest->remote_meta_views[manifest->remote_meta_view_count++] = *view;
    return 0;
}

static int storage_remote_meta_open_view(vemb_v16_storage_ctx_t *storage,
                                         uint32_t owner_supernode_id,
                                         uint32_t backend_type,
                                         const char *path,
                                         uint64_t mmap_offset,
                                         uint32_t requested_entry_count,
                                         uint32_t requested_bucket_count,
                                         int allow_malloc,
                                         vemb_v16_mapped_region_t *mapping,
                                         uint32_t *is_mapped,
                                         void **base,
                                         size_t *bytes,
                                         uint32_t *entry_count,
                                         uint32_t *bucket_count,
                                         vemb_v16_remote_meta_view_t *view) {
    RETURN_IF(!storage || !mapping || !is_mapped || !base || !bytes ||
              !entry_count || !bucket_count || !view, -1);
    *is_mapped = 0;
    *base = NULL;
    *entry_count = requested_entry_count ?
        requested_entry_count : storage->max_vectors;
    *bucket_count = requested_bucket_count ?
        requested_bucket_count :
        pow2_ceil_u32((uint64_t)(*entry_count) * 2u);
    *bytes = vemb_v16_remote_meta_layout_bytes(*entry_count, *bucket_count);

    if (path && path[0]) {
        if ((mmap_offset & 63u) != 0) {
            serverLog(LL_WARNING,
                      "vemb_v16 remote meta offset misaligned: owner=%u path=%s offset=%llu alignment=64",
                      owner_supernode_id,
                      path,
                      (unsigned long long)mmap_offset);
            return -1;
        }
        *is_mapped = 1;
        if (vemb_v16_mapped_region_open(mapping,
                                        backend_type,
                                        path,
                                        mmap_offset,
                                        *bytes) != 0) {
            serverLog(LL_WARNING,
                      "failed to open vemb_v16 remote meta backing: owner=%u backend=%u path=%s offset=%llu entries=%u buckets=%u bytes=%zu",
                      owner_supernode_id,
                      backend_type,
                      path,
                      (unsigned long long)mmap_offset,
                      *entry_count,
                      *bucket_count,
                      *bytes);
            return -1;
        }
        *base = mapping->mapped_addr;
    } else if (allow_malloc) {
        if (posix_memalign(base, 64, *bytes) != 0) {
            *base = NULL;
            serverLog(LL_WARNING,
                      "failed to allocate vemb_v16 remote meta: owner=%u entries=%u buckets=%u bytes=%zu",
                      owner_supernode_id,
                      *entry_count,
                      *bucket_count,
                      *bytes);
            return -1;
        }
    } else {
        serverLog(LL_WARNING,
                  "remote meta owner view requires mapped backing: owner=%u",
                  owner_supernode_id);
        return -1;
    }

    vemb_v16_remote_meta_header_t *header = *base;
    int rc;
    if (*is_mapped &&
        header->magic == VEMB_V16_REMOTE_META_MAGIC &&
        header->version == VEMB_V16_REMOTE_META_VERSION) {
        rc = vemb_v16_remote_meta_attach(view, *base, *bytes);
    } else {
        rc = vemb_v16_remote_meta_init(view,
                                       *base,
                                       *bytes,
                                       owner_supernode_id,
                                       storage->vector_stride,
                                       *entry_count,
                                       *bucket_count);
    }
    if (rc != VEMB_V16_REMOTE_META_OK) {
        serverLog(LL_WARNING,
                  "failed to initialize vemb_v16 remote meta: owner=%u backend=%u path=%s entries=%u buckets=%u bytes=%zu",
                  owner_supernode_id,
                  backend_type,
                  path && path[0] ? path : "(malloc)",
                  *entry_count,
                  *bucket_count,
                  *bytes);
        if (*is_mapped) {
            vemb_v16_mapped_region_close(mapping);
        } else if (*base) {
            free(*base);
        }
        *is_mapped = 0;
        *base = NULL;
        return -1;
    }
    serverLog(LL_NOTICE,
              "vemb_v16 remote meta ready: owner=%u backend=%u path=%s offset=%llu entries=%u buckets=%u bytes=%zu base=%p mapped=%u",
              owner_supernode_id,
              backend_type,
              path && path[0] ? path : "(malloc)",
              (unsigned long long)mmap_offset,
              *entry_count,
              *bucket_count,
              *bytes,
              *base,
              *is_mapped);
    return 0;
}

static int storage_remote_meta_init(vemb_v16_storage_ctx_t *storage,
                                    const vemb_v16_warm_regions_manifest_t *manifest) {
    uint32_t owner_supernode_id = manifest->has_local_ub_node_id ?
        manifest->local_ub_node_id : 0;
    storage->remote_meta_backend_type = manifest->has_remote_meta_backend_type ?
        manifest->remote_meta_backend_type : VEMB_V16_REGION_LOCAL_SHM;
    storage->remote_meta_mmap_offset = manifest->remote_meta_mmap_offset;
    if (manifest->remote_meta_path[0]) {
        strncpy(storage->remote_meta_path,
                manifest->remote_meta_path,
                sizeof(storage->remote_meta_path) - 1);
    }

    return storage_remote_meta_open_view(storage,
                                         owner_supernode_id,
                                         storage->remote_meta_backend_type,
                                         storage->remote_meta_path,
                                         storage->remote_meta_mmap_offset,
                                         manifest->remote_meta_entry_count,
                                         manifest->remote_meta_bucket_count,
                                         1,
                                         &storage->remote_meta_mapping,
                                         &storage->remote_meta_is_mapped,
                                         &storage->remote_meta_base,
                                         &storage->remote_meta_bytes,
                                         &storage->remote_meta_entry_count,
                                         &storage->remote_meta_bucket_count,
                                         &storage->remote_meta_view);
}

static int storage_remote_meta_owner_views_init(
        vemb_v16_storage_ctx_t *storage,
        const vemb_v16_warm_regions_manifest_t *manifest) {
    RETURN_IF(!storage || !manifest || !storage->tlc, -1);
    for (uint32_t i = 0; i < manifest->remote_meta_view_count; i++) {
        const vemb_v16_manifest_remote_meta_view_t *src =
            &manifest->remote_meta_views[i];
        vemb_v16_storage_remote_meta_view_t *dst =
            &storage->remote_meta_owner_views[storage->remote_meta_owner_view_count];
        uint32_t backend_type = src->has_backend_type ?
            src->backend_type : VEMB_V16_REGION_LOCAL_SHM;

        memset(dst, 0, sizeof(*dst));
        dst->mapping.fd = -1;
        dst->owner_id = src->owner_id;
        dst->backend_type = backend_type;
        dst->mmap_offset = src->mmap_offset;
        strncpy(dst->path, src->path, sizeof(dst->path) - 1);

        if (storage_remote_meta_open_view(storage,
                                          dst->owner_id,
                                          dst->backend_type,
                                          dst->path,
                                          dst->mmap_offset,
                                          src->entry_count,
                                          src->bucket_count,
                                          0,
                                          &dst->mapping,
                                          &dst->is_mapped,
                                          &dst->base,
                                          &dst->bytes,
                                          &dst->entry_count,
                                          &dst->bucket_count,
                                          &dst->view) != 0) {
            return -1;
        }
        storage->remote_meta_owner_view_count++;
        if (vemb_v16_tlc_set_remote_meta_owner_view(storage->tlc,
                                                    dst->owner_id,
                                                    &dst->view) != 0) {
            serverLog(LL_WARNING,
                      "failed to register vemb_v16 remote meta owner view: owner=%u path=%s",
                      dst->owner_id,
                      dst->path);
            return -1;
        }
        serverLog(LL_NOTICE,
                  "registered vemb_v16 remote meta owner view: owner=%u backend=%u path=%s offset=%llu entries=%u buckets=%u",
                  dst->owner_id,
                  dst->backend_type,
                  dst->path,
                  (unsigned long long)dst->mmap_offset,
                  dst->entry_count,
                  dst->bucket_count);
    }
    return 0;
}

static int storage_owner_resolver_init(
        vemb_v16_storage_ctx_t *storage,
        const vemb_v16_warm_regions_manifest_t *manifest) {
    RETURN_IF(!storage || !manifest || !storage->tlc, -1);
    uint32_t owners[VEMB_V16_MAX_MANIFEST_REMOTE_META_VIEWS + 1u];
    uint32_t owner_count = 0;
    owners[owner_count++] = manifest->has_local_ub_node_id ?
        manifest->local_ub_node_id : 0;
    for (uint32_t i = 0; i < manifest->remote_meta_view_count; i++) {
        uint32_t owner_id = manifest->remote_meta_views[i].owner_id;
        int exists = 0;
        for (uint32_t j = 0; j < owner_count; j++) {
            if (owners[j] == owner_id) {
                exists = 1;
                break;
            }
        }
        if (!exists) {
            RETURN_IF(owner_count >=
                      VEMB_V16_MAX_MANIFEST_REMOTE_META_VIEWS + 1u, -1);
            owners[owner_count++] = owner_id;
        }
    }
    if (owner_count <= 1)
        return 0;

    qsort(owners, owner_count, sizeof(owners[0]), cmp_u32);
    storage->owner_hash_node_count = 0;
    for (uint32_t i = 0; i < owner_count; i++) {
        for (uint32_t vnode = 0;
             vnode < VEMB_V16_STORAGE_OWNER_HASH_VNODES;
             vnode++) {
            uint32_t vnode_id = storage->owner_hash_node_count;
            char vnode_key[64];
            snprintf(vnode_key,
                     sizeof(vnode_key),
                     "supernode_%u_vnode_%u",
                     owners[i],
                     vnode_id);
            RETURN_IF(storage->owner_hash_node_count >=
                      VEMB_V16_STORAGE_MAX_OWNER_HASH_NODES, -1);
            storage->owner_hash_nodes[storage->owner_hash_node_count++] =
                (vemb_v16_storage_owner_hash_node_t){
                    .hash_value = vemb_v16_murmur3(vnode_key,
                                                   strlen(vnode_key)),
                    .owner_id = owners[i],
                };
        }
    }
    qsort(storage->owner_hash_nodes,
          storage->owner_hash_node_count,
          sizeof(storage->owner_hash_nodes[0]),
          owner_hash_node_cmp);
    vemb_v16_tlc_set_owner_resolver(storage->tlc,
                                    storage_owner_resolver,
                                    storage);
    serverLog(LL_NOTICE,
              "vemb_v16 storage owner resolver ready: owners=%u hash_nodes=%u local_owner=%u",
              owner_count,
              storage->owner_hash_node_count,
              manifest->has_local_ub_node_id ? manifest->local_ub_node_id : 0);
    return 0;
}

static int parse_manifest_field(vemb_v16_warm_regions_manifest_t *manifest,
                                vemb_v16_manifest_region_t *current,
                                vemb_v16_manifest_remote_meta_view_t *current_meta,
                                int in_region,
                                int in_remote_meta_view,
                                const char *key,
                                const char *value) {
    if (!in_region && !in_remote_meta_view) {
        if (!strcmp(key, "local_ub_node_id")) {
            if (parse_u32_value(value, &manifest->local_ub_node_id) != 0)
                return -1;
            manifest->has_local_ub_node_id = 1;
            return 0;
        }
        if (!strcmp(key, "local_region_weight")) {
            return parse_u32_value(value, &manifest->local_region_weight);
        }
        if (!strcmp(key, "remote_meta_provider") ||
            !strcmp(key, "remote_meta_backend")) {
            if (parse_backend_value(value, &manifest->remote_meta_backend_type) != 0)
                return -1;
            manifest->has_remote_meta_backend_type = 1;
            return 0;
        }
        if (!strcmp(key, "remote_meta_path")) {
            RETURN_IF(strlen(value) >= sizeof(manifest->remote_meta_path), -1);
            strcpy(manifest->remote_meta_path, value);
            return 0;
        }
        if (!strcmp(key, "remote_meta_mmap_offset"))
            return parse_u64_value(value, &manifest->remote_meta_mmap_offset);
        if (!strcmp(key, "remote_meta_entries") ||
            !strcmp(key, "remote_meta_entry_count"))
            return parse_u32_value(value, &manifest->remote_meta_entry_count);
        if (!strcmp(key, "remote_meta_buckets") ||
            !strcmp(key, "remote_meta_bucket_count"))
            return parse_u32_value(value, &manifest->remote_meta_bucket_count);
        return 0;
    }

    if (in_region) {
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

    if (!strcmp(key, "owner_id")) {
        if (parse_u32_value(value, &current_meta->owner_id) != 0)
            return -1;
        current_meta->has_owner_id = 1;
        return 0;
    }
    if (!strcmp(key, "provider") || !strcmp(key, "backend")) {
        if (parse_backend_value(value, &current_meta->backend_type) != 0)
            return -1;
        current_meta->has_backend_type = 1;
        return 0;
    }
    if (!strcmp(key, "path")) {
        RETURN_IF(strlen(value) >= sizeof(current_meta->path), -1);
        strcpy(current_meta->path, value);
        return 0;
    }
    if (!strcmp(key, "mmap_offset"))
        return parse_u64_value(value, &current_meta->mmap_offset);
    if (!strcmp(key, "entries") || !strcmp(key, "entry_count"))
        return parse_u32_value(value, &current_meta->entry_count);
    if (!strcmp(key, "buckets") || !strcmp(key, "bucket_count"))
        return parse_u32_value(value, &current_meta->bucket_count);
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
    vemb_v16_manifest_remote_meta_view_t current_meta;
    int in_region = 0;
    int in_remote_meta_view = 0;
    enum {
        MANIFEST_SECTION_TOP = 0,
        MANIFEST_SECTION_WARM_REGIONS,
        MANIFEST_SECTION_REMOTE_META_VIEWS,
    } section = MANIFEST_SECTION_TOP;
    manifest_region_defaults(&current, value_size);
    manifest_remote_meta_view_defaults(&current_meta);

    while (fgets(line, sizeof(line), fp)) {
        strip_comment(line);
        char *p = trim_ws(line);
        if (!p[0])
            continue;
        if (!strncmp(p, "- ", 2)) {
            if (section == MANIFEST_SECTION_WARM_REGIONS) {
                if (in_region &&
                    manifest_push_region(manifest, &current) != 0) {
                    fclose(fp);
                    return -1;
                }
                manifest_region_defaults(&current, value_size);
                in_region = 1;
            } else if (section == MANIFEST_SECTION_REMOTE_META_VIEWS) {
                if (in_remote_meta_view &&
                    manifest_push_remote_meta_view(manifest,
                                                   &current_meta) != 0) {
                    fclose(fp);
                    return -1;
                }
                manifest_remote_meta_view_defaults(&current_meta);
                in_remote_meta_view = 1;
            } else {
                fclose(fp);
                return -1;
            }
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
        if (!value[0]) {
            if (!strcmp(key, "warm_regions")) {
                if (in_remote_meta_view &&
                    manifest_push_remote_meta_view(manifest,
                                                   &current_meta) != 0) {
                    fclose(fp);
                    return -1;
                }
                in_remote_meta_view = 0;
                section = MANIFEST_SECTION_WARM_REGIONS;
            } else if (!strcmp(key, "remote_meta_views")) {
                if (in_region &&
                    manifest_push_region(manifest, &current) != 0) {
                    fclose(fp);
                    return -1;
                }
                in_region = 0;
                section = MANIFEST_SECTION_REMOTE_META_VIEWS;
            }
            continue;
        }
        if (parse_manifest_field(manifest, &current, &current_meta,
                                 in_region, in_remote_meta_view,
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
    if (in_remote_meta_view &&
        manifest_push_remote_meta_view(manifest, &current_meta) != 0) {
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

static int reset_remote_meta_backing(uint32_t owner_id,
                                     uint32_t backend_type,
                                     const char *path,
                                     uint64_t mmap_offset,
                                     uint32_t value_size,
                                     uint32_t entry_count,
                                     uint32_t bucket_count) {
    RETURN_IF(!path || !path[0], 0);
    if (backend_type == VEMB_V16_REGION_LOCAL_SHM) {
        int rc = unlink_shm_if_exists(path);
        int unlink_remote_meta_errno = errno;
        serverLog(LL_NOTICE,
                  "reset remote meta shm: owner=%u path=%s rc=%d status=%s",
                  owner_id,
                  path,
                  rc,
                  strerror(unlink_remote_meta_errno));
        RETURN_IF(rc != 0, -1);
        return 0;
    }
    if (backend_type == VEMB_V16_REGION_UB) {
        if (entry_count == 0) {
            serverLog(LL_NOTICE,
                      "skip reset remote meta ub without entry_count: owner=%u path=%s offset=%llu",
                      owner_id,
                      path,
                      (unsigned long long)mmap_offset);
            return 0;
        }
        uint32_t effective_bucket_count = bucket_count ?
            bucket_count : pow2_ceil_u32((uint64_t)entry_count * 2u);
        size_t bytes =
            vemb_v16_remote_meta_layout_bytes(entry_count,
                                              effective_bucket_count);
        vemb_v16_mapped_region_t mapping;
        memset(&mapping, 0, sizeof(mapping));
        mapping.fd = -1;
        if (vemb_v16_mapped_region_open(&mapping,
                                        backend_type,
                                        path,
                                        mmap_offset,
                                        bytes) != 0) {
            serverLog(LL_WARNING,
                      "failed to open remote meta ub during reset: owner=%u path=%s offset=%llu entries=%u buckets=%u bytes=%zu",
                      owner_id,
                      path,
                      (unsigned long long)mmap_offset,
                      entry_count,
                      effective_bucket_count,
                      bytes);
            RETURN_IF(1, -1);
        }
        vemb_v16_remote_meta_view_t view;
        int rc = vemb_v16_remote_meta_init(&view,
                                           mapping.mapped_addr,
                                           bytes,
                                           owner_id,
                                           value_size,
                                           entry_count,
                                           effective_bucket_count);
        serverLog(LL_NOTICE,
                  "reset remote meta ub: owner=%u path=%s offset=%llu entries=%u buckets=%u bytes=%zu rc=%d",
                  owner_id,
                  path,
                  (unsigned long long)mmap_offset,
                  entry_count,
                  effective_bucket_count,
                  bytes,
                  rc);
        vemb_v16_mapped_region_close(&mapping);
        RETURN_IF(rc != VEMB_V16_REMOTE_META_OK, -1);
        return 0;
    }

    serverLog(LL_WARNING,
              "unsupported remote meta backend type during reset: owner=%u backend_type=%u path=%s",
              owner_id,
              backend_type,
              path);
    return -1;
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
    uint32_t owner_id = manifest->has_local_ub_node_id ?
        manifest->local_ub_node_id : 0;
    uint32_t backend_type = manifest->has_remote_meta_backend_type ?
        manifest->remote_meta_backend_type : VEMB_V16_REGION_LOCAL_SHM;
    if (reset_remote_meta_backing(owner_id,
                                  backend_type,
                                  manifest->remote_meta_path,
                                  manifest->remote_meta_mmap_offset,
                                  manifest->regions[0].value_size,
                                  manifest->remote_meta_entry_count,
                                  manifest->remote_meta_bucket_count) != 0) {
        RETURN_IF(1, -1);
    }
    for (uint32_t i = 0; i < manifest->remote_meta_view_count; i++) {
        const vemb_v16_manifest_remote_meta_view_t *view =
            &manifest->remote_meta_views[i];
        backend_type = view->has_backend_type ?
            view->backend_type : VEMB_V16_REGION_LOCAL_SHM;
        if (reset_remote_meta_backing(view->owner_id,
                                      backend_type,
                                      view->path,
                                      view->mmap_offset,
                                      manifest->regions[0].value_size,
                                      view->entry_count,
                                      view->bucket_count) != 0) {
            RETURN_IF(1, -1);
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

    if (storage_remote_meta_init(storage, manifest) != 0) {
        GOTO_IF(1, err);
    }

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
    vemb_v16_tlc_set_remote_meta_view(storage->tlc,
                                      &storage->remote_meta_view,
                                      VEMB_V16_REMOTE_META_DEFAULT_RETRIES);
    if (storage_remote_meta_owner_views_init(storage, manifest) != 0) {
        GOTO_IF(1, err);
    }
    if (storage_owner_resolver_init(storage, manifest) != 0) {
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
    for (uint32_t i = 0; i < storage->remote_meta_owner_view_count; i++) {
        vemb_v16_storage_remote_meta_view_t *view =
            &storage->remote_meta_owner_views[i];
        if (view->is_mapped) {
            vemb_v16_mapped_region_close(&view->mapping);
            view->base = NULL;
        } else if (view->base) {
            free(view->base);
            view->base = NULL;
        }
    }
    if (storage->remote_meta_is_mapped) {
        vemb_v16_mapped_region_close(&storage->remote_meta_mapping);
        storage->remote_meta_base = NULL;
    } else if (storage->remote_meta_base) {
        free(storage->remote_meta_base);
        storage->remote_meta_base = NULL;
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
