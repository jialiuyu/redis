#ifndef __PROXY_ACTIVE_BUCKET_HEAP_H
#define __PROXY_ACTIVE_BUCKET_HEAP_H

#include "proxy_batch_bucket.h"

#include <stddef.h>
#include <stdint.h>

typedef struct proxy_active_bucket_heap {
    proxy_batch_bucket_t *buckets;
    size_t *indices;
    size_t *slots;
    uint8_t *registered;
    size_t count;
    size_t capacity;
    uint64_t time_limit_us;
} proxy_active_bucket_heap_t;

int proxy_active_bucket_heap_init(proxy_active_bucket_heap_t *heap,
                                  proxy_batch_bucket_t *buckets,
                                  size_t num_buckets,
                                  uint64_t time_limit_us);
void proxy_active_bucket_heap_cleanup(proxy_active_bucket_heap_t *heap);

void proxy_active_bucket_heap_add(proxy_active_bucket_heap_t *heap, size_t bucket_index);
void proxy_active_bucket_heap_remove(proxy_active_bucket_heap_t *heap, size_t bucket_index);

int proxy_active_bucket_heap_peek(const proxy_active_bucket_heap_t *heap,
                                  size_t *bucket_index,
                                  uint64_t *deadline_us);

#endif /* __PROXY_ACTIVE_BUCKET_HEAP_H */
