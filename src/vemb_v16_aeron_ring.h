#ifndef __VEMB_V16_AERON_RING_H
#define __VEMB_V16_AERON_RING_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define VEMB_V16_AERON_RING_BITS 12u
#define VEMB_V16_AERON_RING_SIZE (1u << VEMB_V16_AERON_RING_BITS)
#define VEMB_V16_AERON_RING_MASK (VEMB_V16_AERON_RING_SIZE - 1u)

typedef struct vemb_v16_aeron_ring {
    _Alignas(64) atomic_uint_fast64_t head;
    _Alignas(64) atomic_uint_fast64_t tail;
    uint32_t slot_size;
    uint32_t slot_count;
    uint32_t slot_mask;
    uint32_t reserved;
    uint8_t *slots;
} vemb_v16_aeron_ring_t;

static inline int vemb_v16_aeron_ring_init(vemb_v16_aeron_ring_t *ring,
                                           void *slots,
                                           uint32_t slot_size,
                                           uint32_t slot_count) {
    if (!ring || !slots || slot_size == 0 || slot_count == 0)
        return -1;
    if ((slot_count & (slot_count - 1u)) != 0)
        return -1;
    atomic_init(&ring->head, 0);
    atomic_init(&ring->tail, 0);
    ring->slot_size = slot_size;
    ring->slot_count = slot_count;
    ring->slot_mask = slot_count - 1u;
    ring->reserved = 0;
    ring->slots = slots;
    return 0;
}

// vemb_v16_aeron_publish
static inline int vemb_v16_aeron_publish(vemb_v16_aeron_ring_t *ring,
                                         const void *slot) {
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
    if (tail - head >= ring->slot_count)
        return -1;
    memcpy(ring->slots + (tail & ring->slot_mask) * ring->slot_size,
           slot,
           ring->slot_size);
    atomic_store_explicit(&ring->tail, tail + 1, memory_order_release);
    return 0;
}

static inline int vemb_v16_aeron_publish_batch(vemb_v16_aeron_ring_t *ring,
                                               const void *slots,
                                               uint32_t count) {
    if (count == 0)
        return 0;

    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
    if (tail - head + count > ring->slot_count)
        return -1;

    const uint8_t *src = slots;
    for (uint32_t i = 0; i < count; i++) {
        memcpy(ring->slots + ((tail + i) & ring->slot_mask) * ring->slot_size,
               src + (size_t)i * ring->slot_size,
               ring->slot_size);
    }
    atomic_store_explicit(&ring->tail, tail + count, memory_order_release);
    return 0;
}

static inline int vemb_v16_aeron_poll(vemb_v16_aeron_ring_t *ring,
                                      void *slot) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    if (head >= tail)
        return 0;
    memcpy(slot,
           ring->slots + (head & ring->slot_mask) * ring->slot_size,
           ring->slot_size);
    atomic_store_explicit(&ring->head, head + 1, memory_order_release);
    return 1;
}

static inline uint32_t vemb_v16_aeron_poll_batch(vemb_v16_aeron_ring_t *ring,
                                                 void *slots,
                                                 uint32_t max_count) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    uint64_t available = tail - head;
    if (available == 0 || max_count == 0)
        return 0;
    if (available > max_count)
        available = max_count;
    uint8_t *dst = slots;
    for (uint32_t i = 0; i < (uint32_t)available; i++) {
        memcpy(dst + (size_t)i * ring->slot_size,
               ring->slots + ((head + i) & ring->slot_mask) * ring->slot_size,
               ring->slot_size);
    }
    atomic_store_explicit(&ring->head, head + available, memory_order_release);
    return (uint32_t)available;
}

static inline uint64_t vemb_v16_aeron_available(vemb_v16_aeron_ring_t *ring) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    return tail - head;
}

#endif
