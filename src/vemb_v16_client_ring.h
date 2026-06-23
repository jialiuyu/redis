#ifndef __VEMB_V16_CLIENT_RING_H
#define __VEMB_V16_CLIENT_RING_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define VEMB_V16_CLIENT_RING_BITS 12u
#define VEMB_V16_CLIENT_RING_SIZE (1u << VEMB_V16_CLIENT_RING_BITS)
#define VEMB_V16_CLIENT_RING_MASK (VEMB_V16_CLIENT_RING_SIZE - 1u)

typedef struct vemb_v16_client_ring {
    _Alignas(64) atomic_uint_fast64_t head;
    _Alignas(64) atomic_uint_fast64_t tail;
    uint32_t slot_size;
    uint32_t slot_count;
    uint32_t slot_mask;
    uint32_t reserved;
    uint8_t slots[];
} vemb_v16_client_ring_t;

static inline size_t vemb_v16_client_ring_bytes(uint32_t slot_size) {
    return sizeof(vemb_v16_client_ring_t) +
           (size_t)slot_size * VEMB_V16_CLIENT_RING_SIZE;
}

static inline void vemb_v16_client_ring_init(vemb_v16_client_ring_t *ring,
                                             uint32_t slot_size) {
    atomic_init(&ring->head, 0);
    atomic_init(&ring->tail, 0);
    ring->slot_size = slot_size;
    ring->slot_count = VEMB_V16_CLIENT_RING_SIZE;
    ring->slot_mask = VEMB_V16_CLIENT_RING_MASK;
    ring->reserved = 0;
}

static inline int vemb_v16_client_publish(vemb_v16_client_ring_t *ring,
                                          const void *data,
                                          uint32_t len) {
    if (len > ring->slot_size) return -2;
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
    if (tail - head >= ring->slot_count) return -1;
    memcpy(ring->slots + (tail & ring->slot_mask) * ring->slot_size,
           data,
           len);
    atomic_store_explicit(&ring->tail, tail + 1, memory_order_release);
    return 0;
}

static inline int vemb_v16_client_poll(vemb_v16_client_ring_t *ring,
                                       void *data,
                                       uint32_t max_len) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    if (head >= tail) return 0;
    uint32_t len = ring->slot_size;
    if (len > max_len) len = max_len;
    memcpy(data,
           ring->slots + (head & ring->slot_mask) * ring->slot_size,
           len);
    atomic_store_explicit(&ring->head, head + 1, memory_order_release);
    return (int)len;
}

static inline uint32_t vemb_v16_client_poll_batch(vemb_v16_client_ring_t *ring,
                                                  void *slots,
                                                  uint32_t max_len,
                                                  uint32_t max_count) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    uint64_t available = tail - head;
    if (available == 0 || max_count == 0) return 0;
    if (available > max_count) available = max_count;
    uint32_t len = ring->slot_size;
    if (len > max_len) len = max_len;
    uint8_t *dst = slots;
    for (uint32_t i = 0; i < (uint32_t)available; i++) {
        memcpy(dst + (size_t)i * max_len,
               ring->slots + ((head + i) & ring->slot_mask) * ring->slot_size,
               len);
    }
    atomic_store_explicit(&ring->head, head + available, memory_order_release);
    return (uint32_t)available;
}

static inline uint64_t vemb_v16_client_available(vemb_v16_client_ring_t *ring) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    return tail - head;
}

#endif
