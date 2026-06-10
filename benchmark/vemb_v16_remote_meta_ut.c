#include "../src/vemb_v16_remote_meta.h"

#include "../src/vemb_v16_hash.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void *alloc_meta_region(uint32_t entries,
                               uint32_t buckets,
                               size_t *bytes) {
    void *base = NULL;
    *bytes = vemb_v16_remote_meta_layout_bytes(entries, buckets);
    assert(posix_memalign(&base, 64, *bytes) == 0);
    assert(base != NULL);
    return base;
}

static uint64_t hash_key(const char *key) {
    return vemb_v16_murmur3(key, strlen(key));
}

static void test_layout_and_attach(void) {
    enum { entries = 4, buckets = 8, value_size = 16 };
    size_t bytes = 0;
    void *base = alloc_meta_region(entries, buckets, &bytes);
    vemb_v16_remote_meta_view_t view;
    vemb_v16_remote_meta_view_t attached;

    assert(sizeof(vemb_v16_remote_meta_header_t) == 64);
    assert(sizeof(vemb_v16_remote_meta_bucket_t) == 64);
    assert(sizeof(vemb_v16_remote_meta_entry_t) == 64);
    assert(vemb_v16_remote_meta_init(&view,
                                     base,
                                     bytes,
                                     3,
                                     value_size,
                                     entries,
                                     buckets) == VEMB_V16_REMOTE_META_OK);
    assert(view.header->owner_supernode_id == 3);
    assert(view.header->entry_count == entries);
    assert(view.header->bucket_count == buckets);
    assert(view.header->bucket_mask == buckets - 1);
    assert(view.header->value_size == value_size);
    assert(((uintptr_t)view.buckets & 63u) == 0);
    assert(((uintptr_t)view.entries & 63u) == 0);
    assert(vemb_v16_remote_meta_attach(&attached, base, bytes) ==
           VEMB_V16_REMOTE_META_OK);
    assert(attached.header == view.header);
    assert(attached.buckets == view.buckets);
    assert(attached.entries == view.entries);

    free(base);
}

static void test_publish_lookup_and_overwrite(void) {
    enum { entries = 4, buckets = 8 };
    const char *key = "remote-meta:key";
    uint64_t key_hash = hash_key(key);
    size_t bytes = 0;
    void *base = alloc_meta_region(entries, buckets, &bytes);
    vemb_v16_remote_meta_view_t view;
    vemb_v16_remote_meta_handle_t handle = {
        .region_id = 9,
        .bytes = 32,
        .offset = 128,
        .key_hash = key_hash,
    };
    vemb_v16_remote_meta_handle_t out = {0};

    assert(vemb_v16_remote_meta_init(&view, base, bytes, 1, 32,
                                     entries, buckets) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_publish(&view,
                                        key,
                                        (uint32_t)strlen(key),
                                        key_hash,
                                        &handle) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_lookup(&view,
                                       key,
                                       (uint32_t)strlen(key),
                                       key_hash,
                                       8,
                                       &out) ==
           VEMB_V16_REMOTE_META_OK);
    assert(out.region_id == handle.region_id);
    assert(out.bytes == handle.bytes);
    assert(out.offset == handle.offset);
    assert(out.key_hash == key_hash);
    assert(atomic_load_explicit(&view.header->next_entry,
                                memory_order_relaxed) == 1);

    handle.offset = 256;
    handle.region_id = 10;
    assert(vemb_v16_remote_meta_publish(&view,
                                        key,
                                        (uint32_t)strlen(key),
                                        key_hash,
                                        &handle) ==
           VEMB_V16_REMOTE_META_OK);
    memset(&out, 0, sizeof(out));
    assert(vemb_v16_remote_meta_lookup(&view,
                                       key,
                                       (uint32_t)strlen(key),
                                       key_hash,
                                       8,
                                       &out) ==
           VEMB_V16_REMOTE_META_OK);
    assert(out.region_id == 10);
    assert(out.offset == 256);
    assert(atomic_load_explicit(&view.header->next_entry,
                                memory_order_relaxed) == 1);

    free(base);
}

static void test_lookup_miss_and_full(void) {
    enum { entries = 1, buckets = 2 };
    const char *key1 = "remote-meta:key1";
    const char *key2 = "remote-meta:key2";
    size_t bytes = 0;
    void *base = alloc_meta_region(entries, buckets, &bytes);
    vemb_v16_remote_meta_view_t view;
    vemb_v16_remote_meta_handle_t handle = {
        .region_id = 1,
        .bytes = 8,
        .offset = 0,
    };
    vemb_v16_remote_meta_handle_t out = {0};

    assert(vemb_v16_remote_meta_init(&view, base, bytes, 1, 8,
                                     entries, buckets) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_lookup(&view,
                                       key1,
                                       (uint32_t)strlen(key1),
                                       hash_key(key1),
                                       8,
                                       &out) ==
           VEMB_V16_REMOTE_META_NOT_FOUND);
    assert(vemb_v16_remote_meta_publish(&view,
                                        key1,
                                        (uint32_t)strlen(key1),
                                        hash_key(key1),
                                        &handle) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_publish(&view,
                                        key2,
                                        (uint32_t)strlen(key2),
                                        hash_key(key2),
                                        &handle) ==
           VEMB_V16_REMOTE_META_FULL);

    free(base);
}

static void test_busy_entry_retry_budget(void) {
    enum { entries = 2, buckets = 4 };
    const char *key = "remote-meta:busy";
    uint64_t key_hash = hash_key(key);
    size_t bytes = 0;
    void *base = alloc_meta_region(entries, buckets, &bytes);
    vemb_v16_remote_meta_view_t view;
    vemb_v16_remote_meta_handle_t handle = {
        .region_id = 7,
        .bytes = 16,
        .offset = 64,
        .key_hash = key_hash,
    };
    vemb_v16_remote_meta_handle_t out = {0};

    assert(vemb_v16_remote_meta_init(&view, base, bytes, 1, 16,
                                     entries, buckets) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_publish(&view,
                                        key,
                                        (uint32_t)strlen(key),
                                        key_hash,
                                        &handle) ==
           VEMB_V16_REMOTE_META_OK);
    atomic_store_explicit(&view.entries[0].version, 3, memory_order_release);
    assert(vemb_v16_remote_meta_lookup(&view,
                                       key,
                                       (uint32_t)strlen(key),
                                       key_hash,
                                       2,
                                       &out) ==
           VEMB_V16_REMOTE_META_BUSY);

    free(base);
}

int main(void) {
    test_layout_and_attach();
    test_publish_lookup_and_overwrite();
    test_lookup_miss_and_full();
    test_busy_entry_retry_budget();
    printf("vemb_v16_remote_meta_ut: all tests passed\n");
    return 0;
}
