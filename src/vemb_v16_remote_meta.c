#include "vemb_v16_remote_meta.h"

#include "cpu_relax.h"
#include "macro.h"
#include "vemb_v16_hash.h"

#include <stdatomic.h>
#include <string.h>

typedef char vemb_v16_remote_meta_header_size_must_be_64[
    sizeof(vemb_v16_remote_meta_header_t) == 64 ? 1 : -1];
typedef char vemb_v16_remote_meta_bucket_size_must_be_64[
    sizeof(vemb_v16_remote_meta_bucket_t) == 64 ? 1 : -1];
typedef char vemb_v16_remote_meta_entry_size_must_be_64[
    sizeof(vemb_v16_remote_meta_entry_t) == 64 ? 1 : -1];

static size_t align64(size_t value) {
    return (value + 63u) & ~(size_t)63u;
}

static int is_power_of_two(uint32_t value) {
    return value && ((value & (value - 1u)) == 0);
}

static uint64_t fingerprint_key(const char *key, uint32_t key_len) {
    return vemb_v16_fnv1a64_bytes(key, key_len);
}

static int layout(vemb_v16_remote_meta_view_t *view,
                  void *base,
                  size_t bytes,
                  uint32_t entry_count,
                  uint32_t bucket_count) {
    size_t header_off = 0;
    size_t buckets_off = align64(sizeof(vemb_v16_remote_meta_header_t));
    size_t entries_off =
        align64(buckets_off +
                (size_t)bucket_count *
                sizeof(vemb_v16_remote_meta_bucket_t));
    size_t need =
        entries_off +
        (size_t)entry_count * sizeof(vemb_v16_remote_meta_entry_t);

    RETURN_IF(bytes < need, -1);
    view->base = base;
    view->bytes = bytes;
    view->header = (void *)((uint8_t *)base + header_off);
    view->buckets = (void *)((uint8_t *)base + buckets_off);
    view->entries = (void *)((uint8_t *)base + entries_off);
    return 0;
}

size_t vemb_v16_remote_meta_layout_bytes(uint32_t entry_count,
                                         uint32_t bucket_count) {
    size_t buckets_off = align64(sizeof(vemb_v16_remote_meta_header_t));
    size_t entries_off =
        align64(buckets_off +
                (size_t)bucket_count *
                sizeof(vemb_v16_remote_meta_bucket_t));
    return entries_off +
           (size_t)entry_count * sizeof(vemb_v16_remote_meta_entry_t);
}

int vemb_v16_remote_meta_init(vemb_v16_remote_meta_view_t *view,
                              void *base,
                              size_t bytes,
                              uint32_t owner_supernode_id,
                              uint32_t value_size,
                              uint32_t entry_count,
                              uint32_t bucket_count) {
    RETURN_IF(!is_power_of_two(bucket_count) ||
              entry_count == 0 ||
              bucket_count < entry_count, VEMB_V16_REMOTE_META_INVALID);
    RETURN_IF(layout(view, base, bytes, entry_count, bucket_count) != 0,
              VEMB_V16_REMOTE_META_INVALID);

    memset(base, 0, bytes);
    view->header->magic = VEMB_V16_REMOTE_META_MAGIC;
    view->header->version = VEMB_V16_REMOTE_META_VERSION;
    view->header->owner_supernode_id = owner_supernode_id;
    view->header->entry_count = entry_count;
    view->header->bucket_count = bucket_count;
    view->header->bucket_mask = bucket_count - 1u;
    view->header->value_size = value_size;
    view->header->flags = 0;
    view->header->generation = 0;
    atomic_init(&view->header->next_entry, 0);
    for (uint32_t i = 0; i < bucket_count; i++)
        atomic_init(&view->buckets[i].entry_index, VEMB_V16_REMOTE_META_EMPTY);
    for (uint32_t i = 0; i < entry_count; i++)
        atomic_init(&view->entries[i].version, 0);
    return VEMB_V16_REMOTE_META_OK;
}

int vemb_v16_remote_meta_attach(vemb_v16_remote_meta_view_t *view,
                                void *base,
                                size_t bytes) {
    RETURN_IF(bytes < sizeof(vemb_v16_remote_meta_header_t),
              VEMB_V16_REMOTE_META_INVALID);
    vemb_v16_remote_meta_header_t *header = base;
    RETURN_IF(header->magic != VEMB_V16_REMOTE_META_MAGIC ||
              header->version != VEMB_V16_REMOTE_META_VERSION ||
              !is_power_of_two(header->bucket_count) ||
              header->bucket_mask != header->bucket_count - 1u ||
              header->entry_count == 0,
              VEMB_V16_REMOTE_META_INVALID);
    RETURN_IF(layout(view,
                     base,
                     bytes,
                     header->entry_count,
                     header->bucket_count) != 0,
              VEMB_V16_REMOTE_META_INVALID);
    return VEMB_V16_REMOTE_META_OK;
}

static void publish_entry(vemb_v16_remote_meta_entry_t *entry,
                          uint64_t key_hash,
                          uint64_t key_fingerprint,
                          const vemb_v16_remote_meta_handle_t *handle) {
    uint32_t version =
        atomic_load_explicit(&entry->version, memory_order_relaxed);
    if ((version & 1u) == 0)
        version++;
    atomic_store_explicit(&entry->version, version, memory_order_release);

    entry->region_id = handle->region_id;
    entry->offset = handle->offset;
    entry->bytes = handle->bytes;
    entry->flags = 0;
    entry->key_hash = key_hash;
    entry->key_fingerprint = key_fingerprint;

    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&entry->version, version + 1u,
                          memory_order_release);
}

int vemb_v16_remote_meta_publish(vemb_v16_remote_meta_view_t *view,
                                 const char *key,
                                 uint32_t key_len,
                                 uint64_t key_hash,
                                 const vemb_v16_remote_meta_handle_t *handle) {
    RETURN_IF(key_len == 0, VEMB_V16_REMOTE_META_INVALID);

    uint64_t fp = fingerprint_key(key, key_len);
    uint32_t pos = (uint32_t)key_hash & view->header->bucket_mask;
    for (uint32_t i = 0; i < view->header->bucket_count; i++) {
        vemb_v16_remote_meta_bucket_t *bucket =
            &view->buckets[(pos + i) & view->header->bucket_mask];
        uint32_t idx =
            atomic_load_explicit(&bucket->entry_index, memory_order_acquire);
        if (idx == VEMB_V16_REMOTE_META_WRITING) {
            cpu_relax();
            continue;
        }
        if (idx != VEMB_V16_REMOTE_META_EMPTY) {
            if (idx >= view->header->entry_count)
                return VEMB_V16_REMOTE_META_INVALID;
            if (bucket->key_hash == key_hash &&
                bucket->key_fingerprint == fp) {
                publish_entry(&view->entries[idx], key_hash, fp, handle);
                return VEMB_V16_REMOTE_META_OK;
            }
            continue;
        }

        uint32_t expected = VEMB_V16_REMOTE_META_EMPTY;
        if (!atomic_compare_exchange_strong_explicit(
                &bucket->entry_index,
                &expected,
                VEMB_V16_REMOTE_META_WRITING,
                memory_order_acq_rel,
                memory_order_acquire)) {
            cpu_relax();
            continue;
        }

        uint32_t new_idx =
            atomic_fetch_add_explicit(&view->header->next_entry,
                                      1,
                                      memory_order_relaxed);
        if (new_idx >= view->header->entry_count) {
            atomic_store_explicit(&bucket->entry_index,
                                  VEMB_V16_REMOTE_META_EMPTY,
                                  memory_order_release);
            return VEMB_V16_REMOTE_META_FULL;
        }
        bucket->probe_len = i;
        bucket->key_hash = key_hash;
        bucket->key_fingerprint = fp;
        publish_entry(&view->entries[new_idx], key_hash, fp, handle);
        atomic_store_explicit(&bucket->entry_index,
                              new_idx,
                              memory_order_release);
        return VEMB_V16_REMOTE_META_OK;
    }
    return VEMB_V16_REMOTE_META_COLLISION;
}

static int read_entry(vemb_v16_remote_meta_entry_t *entry,
                      uint64_t key_hash,
                      uint64_t key_fingerprint,
                      uint32_t retry_budget,
                      vemb_v16_remote_meta_handle_t *handle) {
    if (retry_budget == 0)
        retry_budget = VEMB_V16_REMOTE_META_DEFAULT_RETRIES;
    for (uint32_t i = 0; i < retry_budget; i++) {
        uint32_t v1 =
            atomic_load_explicit(&entry->version, memory_order_acquire);
        if (v1 & 1u) {
            cpu_relax();
            continue;
        }

        uint32_t region_id = entry->region_id;
        uint64_t offset = entry->offset;
        uint32_t bytes = entry->bytes;
        uint64_t entry_hash = entry->key_hash;
        uint64_t entry_fp = entry->key_fingerprint;

        atomic_thread_fence(memory_order_acquire);

        uint32_t v2 =
            atomic_load_explicit(&entry->version, memory_order_acquire);
        if (v1 == v2 && !(v2 & 1u)) {
            if (entry_hash != key_hash || entry_fp != key_fingerprint)
                return VEMB_V16_REMOTE_META_NOT_FOUND;
            *handle = (vemb_v16_remote_meta_handle_t){
                .region_id = region_id,
                .bytes = bytes,
                .offset = offset,
                .key_hash = key_hash,
            };
            return VEMB_V16_REMOTE_META_OK;
        }
        cpu_relax();
    }
    return VEMB_V16_REMOTE_META_BUSY;
}

int vemb_v16_remote_meta_lookup(vemb_v16_remote_meta_view_t *view,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                uint32_t retry_budget,
                                vemb_v16_remote_meta_handle_t *handle) {
    RETURN_IF(key_len == 0, VEMB_V16_REMOTE_META_INVALID);

    uint64_t fp = fingerprint_key(key, key_len);
    uint32_t pos = (uint32_t)key_hash & view->header->bucket_mask;
    for (uint32_t i = 0; i < view->header->bucket_count; i++) {
        vemb_v16_remote_meta_bucket_t *bucket =
            &view->buckets[(pos + i) & view->header->bucket_mask];
        uint32_t idx =
            atomic_load_explicit(&bucket->entry_index, memory_order_acquire);
        if (idx == VEMB_V16_REMOTE_META_EMPTY)
            return VEMB_V16_REMOTE_META_NOT_FOUND;
        if (idx == VEMB_V16_REMOTE_META_WRITING) {
            cpu_relax();
            continue;
        }
        if (idx >= view->header->entry_count)
            return VEMB_V16_REMOTE_META_INVALID;
        if (bucket->key_hash != key_hash || bucket->key_fingerprint != fp)
            continue;
        return read_entry(&view->entries[idx],
                          key_hash,
                          fp,
                          retry_budget,
                          handle);
    }
    return VEMB_V16_REMOTE_META_NOT_FOUND;
}
