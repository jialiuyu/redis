#ifndef __VEMB_V16_REMOTE_META_H
#define __VEMB_V16_REMOTE_META_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#define VEMB_V16_REMOTE_META_MAGIC 0x56314d52u
#define VEMB_V16_REMOTE_META_VERSION 1u
#define VEMB_V16_REMOTE_META_EMPTY UINT32_MAX
#define VEMB_V16_REMOTE_META_WRITING (UINT32_MAX - 1u)
#define VEMB_V16_REMOTE_META_DEFAULT_RETRIES 16u

typedef enum vemb_v16_remote_meta_rc {
    VEMB_V16_REMOTE_META_OK = 0,
    VEMB_V16_REMOTE_META_NOT_FOUND = 1,
    VEMB_V16_REMOTE_META_BUSY = 2,
    VEMB_V16_REMOTE_META_FULL = 3,
    VEMB_V16_REMOTE_META_COLLISION = 4,
    VEMB_V16_REMOTE_META_INVALID = 5,
} vemb_v16_remote_meta_rc_t;

typedef struct vemb_v16_remote_meta_handle {
    uint32_t region_id;
    uint32_t bytes;
    uint64_t offset;
    uint64_t key_hash;
} vemb_v16_remote_meta_handle_t;

typedef struct vemb_v16_remote_meta_header {
    uint32_t magic;
    uint32_t version;
    uint32_t owner_supernode_id;
    uint32_t entry_count;
    uint32_t bucket_count;
    uint32_t bucket_mask;
    uint32_t value_size;
    uint32_t flags;
    uint64_t generation;
    _Atomic uint32_t next_entry;
    uint32_t reserved0;
    uint8_t reserved1[16];
} vemb_v16_remote_meta_header_t;

typedef struct vemb_v16_remote_meta_bucket {
    _Atomic uint32_t entry_index;
    uint32_t probe_len;
    uint64_t key_hash;
    uint64_t key_fingerprint;
    uint8_t reserved[40];
} vemb_v16_remote_meta_bucket_t;

typedef struct vemb_v16_remote_meta_entry {
    _Atomic uint32_t version;
    uint32_t region_id;
    uint64_t offset;
    uint32_t bytes;
    uint32_t flags;
    uint64_t key_hash;
    uint64_t key_fingerprint;
    uint8_t reserved0[24];
} vemb_v16_remote_meta_entry_t;

typedef struct vemb_v16_remote_meta_view {
    uint8_t *base;
    size_t bytes;
    vemb_v16_remote_meta_header_t *header;
    vemb_v16_remote_meta_bucket_t *buckets;
    vemb_v16_remote_meta_entry_t *entries;
} vemb_v16_remote_meta_view_t;

size_t vemb_v16_remote_meta_layout_bytes(uint32_t entry_count,
                                         uint32_t bucket_count);
int vemb_v16_remote_meta_init(vemb_v16_remote_meta_view_t *view,
                              void *base,
                              size_t bytes,
                              uint32_t owner_supernode_id,
                              uint32_t value_size,
                              uint32_t entry_count,
                              uint32_t bucket_count);
int vemb_v16_remote_meta_attach(vemb_v16_remote_meta_view_t *view,
                                void *base,
                                size_t bytes);
int vemb_v16_remote_meta_publish(vemb_v16_remote_meta_view_t *view,
                                 const char *key,
                                 uint32_t key_len,
                                 uint64_t key_hash,
                                 const vemb_v16_remote_meta_handle_t *handle);
int vemb_v16_remote_meta_lookup(vemb_v16_remote_meta_view_t *view,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                uint32_t retry_budget,
                                vemb_v16_remote_meta_handle_t *handle);

#endif
