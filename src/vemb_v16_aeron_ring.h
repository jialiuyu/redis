#ifndef __VEMB_V16_AERON_RING_H
#define __VEMB_V16_AERON_RING_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

/* Adaptive backoff — 三阶段（参考 aeron_ipc.h::aeron_poll_adaptive）：
 *   spins <  64: 纯 spin，compiler barrier only（最低延迟）
 *   spins < 256: spin + ARM yield / x86 pause（让 SMT 兄弟核）
 *   spins ≥ 256: nanosleep(1μs)（真正 idle 时省 CPU）
 * 注意：跨 UB PCIe 场景下 Phase 1 阈值需要按 RTT 重新调参。 */
static inline void vemb_v16_aeron_backoff(uint32_t spins) {
    if (spins < 64u) {
        __asm__ volatile("" ::: "memory");
    } else if (spins < 256u) {
#if defined(__aarch64__)
        __asm__ volatile("yield" ::: "memory");
#elif defined(__x86_64__)
        __asm__ volatile("pause" ::: "memory");
#else
        __asm__ volatile("" ::: "memory");
#endif
    } else {
        struct timespec ts = {0, 1000};  /* 1μs */
        nanosleep(&ts, NULL);
    }
}

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
    uint64_t slots_off;   /* byte offset from ring start to slots area.
                           * Cross-process safe: two PEs mmap the same
                           * physical memory at different virtual addrs,
                           * but the offset is identical. */
} vemb_v16_aeron_ring_t;

static inline int vemb_v16_aeron_ring_init(vemb_v16_aeron_ring_t *ring,
                                           void *slots,
                                           uint32_t slot_size,
                                           uint32_t slot_count) {
    if (!ring || !slots || slot_size == 0 || slot_count == 0)
        return -1;
    if ((slot_count & (slot_count - 1u)) != 0)
        return -1;
    /* Store the byte offset from ring to slots. Roundtrip arithmetic
     * (uint8_t*)ring + slots_off == slots works for any relative position
     * (positive or negative-wrapped-to-uint64) as long as both pointers
     * remain stable. Two usage patterns:
     *   (1) Cross-process shmdev mmap: slots embedded right after header,
     *       offset is small positive (sizeof(*ring)). Same physical memory
     *       mapped at different VAs in two PEs — offset is identical.
     *   (2) Heap-only shard queues (init_shard_queue_array): slots and ring
     *       are separate allocations; offset is whatever malloc gave us.
     * The previous "slots must be after ring" sanity check broke case (2). */
    uint64_t off = (uint64_t)((uint8_t *)slots - (uint8_t *)ring);
    atomic_init(&ring->head, 0);
    atomic_init(&ring->tail, 0);
    ring->slot_size = slot_size;
    ring->slot_count = slot_count;
    ring->slot_mask = slot_count - 1u;
    ring->reserved = 0;
    ring->slots_off = off;
    return 0;
}

// vemb_v16_aeron_publish
static inline int vemb_v16_aeron_publish(vemb_v16_aeron_ring_t *ring,
                                         const void *slot) {
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
    if (tail - head >= ring->slot_count)
        return -1;
    uint8_t *slots_base = (uint8_t *)ring + ring->slots_off;
    memcpy(slots_base + (tail & ring->slot_mask) * ring->slot_size,
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

    uint8_t *slots_base = (uint8_t *)ring + ring->slots_off;
    const uint8_t *src = slots;
    for (uint32_t i = 0; i < count; i++) {
        memcpy(slots_base + ((tail + i) & ring->slot_mask) * ring->slot_size,
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
    uint8_t *slots_base = (uint8_t *)ring + ring->slots_off;
    memcpy(slot,
           slots_base + (head & ring->slot_mask) * ring->slot_size,
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
    uint8_t *slots_base = (uint8_t *)ring + ring->slots_off;
    uint8_t *dst = slots;
    for (uint32_t i = 0; i < (uint32_t)available; i++) {
        memcpy(dst + (size_t)i * ring->slot_size,
               slots_base + ((head + i) & ring->slot_mask) * ring->slot_size,
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

/* Adaptive publish — 阻塞直到 publish 成功（三阶段退避）。
 * 跟 aeron_ipc.h::aeron_publish_adaptive 对称。
 * 注意：不带 running/active 检查，调用方有终止语义时应该自己包 while
 * 并调用 vemb_v16_aeron_backoff(spins++) 而不是用这个完整封装。 */
static inline void vemb_v16_aeron_publish_adaptive(vemb_v16_aeron_ring_t *ring,
                                                    const void *slot) {
    uint32_t spins = 0;
    while (vemb_v16_aeron_publish(ring, slot) != 0) {
        vemb_v16_aeron_backoff(spins++);
    }
}

/* Adaptive poll — 阻塞直到拿到数据（三阶段退避）。
 * 跟 aeron_ipc.h::aeron_poll_adaptive 对称。返回值总是 1（必然拿到）。 */
static inline int vemb_v16_aeron_poll_adaptive(vemb_v16_aeron_ring_t *ring,
                                                void *slot) {
    uint32_t spins = 0;
    int got;
    while ((got = vemb_v16_aeron_poll(ring, slot)) == 0) {
        vemb_v16_aeron_backoff(spins++);
    }
    return got;
}

#endif
