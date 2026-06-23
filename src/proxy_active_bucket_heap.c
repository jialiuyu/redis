#include "proxy_active_bucket_heap.h"

#include "macro.h"
#include "server.h"

#include <string.h>

static inline uint64_t proxy_active_bucket_deadline_us(const proxy_active_bucket_heap_t *heap,
                                                       size_t bucket_index) {
    return heap->buckets[bucket_index].last_flush_time_us + heap->time_limit_us;
}

static inline void proxy_active_bucket_heap_swap(proxy_active_bucket_heap_t *heap,
                                                 size_t a, size_t b) {
    size_t bucket_a = heap->indices[a];
    size_t bucket_b = heap->indices[b];

    heap->indices[a] = bucket_b;
    heap->indices[b] = bucket_a;
    heap->slots[bucket_a] = b;
    heap->slots[bucket_b] = a;
}

static void proxy_active_bucket_heap_sift_up(proxy_active_bucket_heap_t *heap, size_t pos) {
    while (pos > 0) {
        size_t parent = (pos - 1) / 2;
        if (proxy_active_bucket_deadline_us(heap, heap->indices[parent]) <=
            proxy_active_bucket_deadline_us(heap, heap->indices[pos])) {
            break;
        }
        proxy_active_bucket_heap_swap(heap, parent, pos);
        pos = parent;
    }
}

static void proxy_active_bucket_heap_sift_down(proxy_active_bucket_heap_t *heap, size_t pos) {
    while (1) {
        size_t left = pos * 2 + 1;
        size_t right = left + 1;
        size_t smallest = pos;

        if (left < heap->count &&
            proxy_active_bucket_deadline_us(heap, heap->indices[left]) <
                proxy_active_bucket_deadline_us(heap, heap->indices[smallest])) {
            smallest = left;
        }
        if (right < heap->count &&
            proxy_active_bucket_deadline_us(heap, heap->indices[right]) <
                proxy_active_bucket_deadline_us(heap, heap->indices[smallest])) {
            smallest = right;
        }
        if (smallest == pos) {
            break;
        }

        proxy_active_bucket_heap_swap(heap, pos, smallest);
        pos = smallest;
    }
}

int proxy_active_bucket_heap_init(proxy_active_bucket_heap_t *heap,
                                  proxy_batch_bucket_t *buckets,
                                  size_t num_buckets,
                                  uint64_t time_limit_us) {
    RETURN_IF(!heap || !buckets || num_buckets == 0, C_ERR);

    memset(heap, 0, sizeof(*heap));
    heap->buckets = buckets;
    heap->capacity = num_buckets;
    heap->time_limit_us = time_limit_us;
    heap->indices = zcalloc(sizeof(size_t) * num_buckets);
    heap->slots = zcalloc(sizeof(size_t) * num_buckets);
    heap->registered = zcalloc(sizeof(uint8_t) * num_buckets);
    if (!heap->indices || !heap->slots || !heap->registered) {
        proxy_active_bucket_heap_cleanup(heap);
        return C_ERR;
    }

    return C_OK;
}

void proxy_active_bucket_heap_cleanup(proxy_active_bucket_heap_t *heap) {
    RETURN_IF(!heap);

    zfree(heap->indices);
    zfree(heap->slots);
    zfree(heap->registered);
    memset(heap, 0, sizeof(*heap));
}

void proxy_active_bucket_heap_add(proxy_active_bucket_heap_t *heap, size_t bucket_index) {
    RETURN_IF(!heap || bucket_index >= heap->capacity);

    if (!heap->registered[bucket_index]) {
        size_t slot = heap->count++;
        heap->indices[slot] = bucket_index;
        heap->slots[bucket_index] = slot;
        heap->registered[bucket_index] = 1;
        proxy_active_bucket_heap_sift_up(heap, slot);
    }
}

void proxy_active_bucket_heap_remove(proxy_active_bucket_heap_t *heap, size_t bucket_index) {
    RETURN_IF(!heap || bucket_index >= heap->capacity);

    if (heap->registered[bucket_index]) {
        size_t slot = heap->slots[bucket_index];
        size_t last_slot = heap->count - 1;
        if (slot != last_slot) {
            size_t moved_bucket = heap->indices[last_slot];
            heap->indices[slot] = moved_bucket;
            heap->slots[moved_bucket] = slot;
            heap->count = last_slot;
            heap->registered[bucket_index] = 0;
            heap->slots[bucket_index] = 0;

            if (slot > 0 &&
                proxy_active_bucket_deadline_us(heap, heap->indices[slot]) <
                    proxy_active_bucket_deadline_us(heap, heap->indices[(slot - 1) / 2])) {
                proxy_active_bucket_heap_sift_up(heap, slot);
            } else {
                proxy_active_bucket_heap_sift_down(heap, slot);
            }
        } else {
            heap->count = last_slot;
            heap->registered[bucket_index] = 0;
            heap->slots[bucket_index] = 0;
        }
    }
}

int proxy_active_bucket_heap_peek(const proxy_active_bucket_heap_t *heap,
                                  size_t *bucket_index,
                                  uint64_t *deadline_us) {
    RETURN_IF(!heap || !bucket_index || !deadline_us || heap->count == 0, C_ERR);

    *bucket_index = heap->indices[0];
    *deadline_us = proxy_active_bucket_deadline_us(heap, *bucket_index);
    return C_OK;
}
