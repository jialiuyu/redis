/*
 * Aeron-Style IPC — Lock-Free Shared Memory Message Bus
 *
 * Designed for sub-microsecond latency (< 0.1μs target).
 * Uses SPSC (Single-Producer Single-Consumer) ring buffers
 * in shared memory. Each client gets a dedicated pair of rings:
 *   - Request ring: client → server (SPSC)
 *   - Response ring: server → client (SPSC)
 *
 * No locks, no CAS, no syscalls in the data path.
 * Only atomic load/store with acquire/release semantics.
 *
 * Memory layout per channel (mmap'd shared memory):
 *   [8B head][56B pad][8B tail][56B pad][N × slot]
 *
 * Inspired by Aeron's publication/subscription model and
 * LMAX Disruptor's single-writer principle.
 */
#ifndef __AERON_IPC_H
#define __AERON_IPC_H

#include <stdint.h>
#include <stddef.h>
#include <string.h>

#define AERON_RING_BITS   12          /* 4096 slots */
#define AERON_RING_SIZE   (1 << AERON_RING_BITS)
#define AERON_RING_MASK   (AERON_RING_SIZE - 1)
#define AERON_MSG_SIZE    (24000 + 16)  /* Fits MGET: 5B hdr + 3000×8B keys = 24005B */
#define AERON_MAX_CHANNELS 64
#define AERON_SHM_PREFIX  "/aeron_tlc_"

/* For large MGET responses: separate large-message ring */
#define AERON_LARGE_MSG_SIZE (4 + 3000 * (1 + 1200) + 64)  /* ~3.6MB max */
#define AERON_LARGE_RING_BITS 4   /* 16 slots for large messages */
#define AERON_LARGE_RING_SIZE (1 << AERON_LARGE_RING_BITS)
#define AERON_LARGE_RING_MASK (AERON_LARGE_RING_SIZE - 1)

/* Message header in each slot */
typedef struct {
    uint32_t len;          /* 0 = empty, >0 = message length */
    uint8_t  data[AERON_MSG_SIZE];
} __attribute__((aligned(64))) aeron_msg_t;

/* SPSC Ring Buffer — single producer, single consumer, zero-lock */
typedef struct {
    volatile uint64_t head __attribute__((aligned(64)));  /* Consumer */
    volatile uint64_t tail __attribute__((aligned(64)));  /* Producer */
    aeron_msg_t msgs[AERON_RING_SIZE];
} aeron_ring_t;

/* Channel: bidirectional pair of SPSC rings */
typedef struct {
    aeron_ring_t *req_ring;   /* Client → Server (GET/PUT/MGET requests) */
    aeron_ring_t *resp_ring;  /* Server → Client (GET/PUT responses) */
} aeron_channel_t;

/* Large message slot for MGET responses (up to ~3.6MB) */
typedef struct {
    volatile uint32_t len;     /* 0 = empty */
    uint32_t          capacity;
    uint8_t           data[];  /* Flexible array */
} __attribute__((aligned(64))) aeron_large_slot_t;

/* Large ring for MGET responses */
typedef struct {
    volatile uint64_t head __attribute__((aligned(64)));
    volatile uint64_t tail __attribute__((aligned(64)));
    uint32_t slot_size;  /* Size of each slot including header */
    uint32_t num_slots;
    /* Slots follow in memory */
} aeron_large_ring_t;

static inline aeron_large_slot_t *aeron_large_slot(aeron_large_ring_t *ring, uint64_t idx) {
    uint8_t *base = (uint8_t *)(ring + 1);
    return (aeron_large_slot_t *)(base + (idx % ring->num_slots) * ring->slot_size);
}

static inline int aeron_large_publish(aeron_large_ring_t *ring, const void *data, uint32_t len) {
    uint64_t tail = ring->tail;
    uint64_t head = __atomic_load_n(&ring->head, __ATOMIC_ACQUIRE);
    if (tail - head >= ring->num_slots) return -1;
    aeron_large_slot_t *slot = aeron_large_slot(ring, tail);
    if (len > slot->capacity) return -2;
    memcpy(slot->data, data, len);
    slot->len = len;
    __atomic_store_n(&ring->tail, tail + 1, __ATOMIC_RELEASE);
    return 0;
}

static inline int aeron_large_poll(aeron_large_ring_t *ring, void *data, uint32_t max_len) {
    uint64_t head = ring->head;
    uint64_t tail = __atomic_load_n(&ring->tail, __ATOMIC_ACQUIRE);
    if (head >= tail) return 0;
    aeron_large_slot_t *slot = aeron_large_slot(ring, head);
    uint32_t len = slot->len;
    if (len == 0) return 0;
    if (len > max_len) len = max_len;
    memcpy(data, slot->data, len);
    slot->len = 0;
    __atomic_store_n(&ring->head, head + 1, __ATOMIC_RELEASE);
    return (int)len;
}

/* ---- SPSC Operations (zero-lock, zero-syscall) ---- */

/* Publish: write message to ring (producer side) */
static inline int aeron_publish(aeron_ring_t *ring, const void *data, uint32_t len) {
    uint64_t tail = ring->tail;
    uint64_t head = __atomic_load_n(&ring->head, __ATOMIC_ACQUIRE);

    /* Check if ring is full */
    if (tail - head >= AERON_RING_SIZE) return -1;

    aeron_msg_t *slot = &ring->msgs[tail & AERON_RING_MASK];
    memcpy(slot->data, data, len);
    slot->len = len;

    /* Release: make data visible before advancing tail */
    __atomic_store_n(&ring->tail, tail + 1, __ATOMIC_RELEASE);
    return 0;
}

/* Subscribe: read message from ring (consumer side) */
static inline int aeron_poll(aeron_ring_t *ring, void *data, uint32_t max_len) {
    uint64_t head = ring->head;
    uint64_t tail = __atomic_load_n(&ring->tail, __ATOMIC_ACQUIRE);

    if (head >= tail) return 0;  /* Empty */

    aeron_msg_t *slot = &ring->msgs[head & AERON_RING_MASK];
    uint32_t len = slot->len;
    if (len == 0) return 0;
    if (len > max_len) len = max_len;

    memcpy(data, slot->data, len);
    slot->len = 0;

    /* Release: mark slot as consumed */
    __atomic_store_n(&ring->head, head + 1, __ATOMIC_RELEASE);
    return (int)len;
}

/* Check if ring has data (non-consuming peek) */
static inline int aeron_available(aeron_ring_t *ring) {
    uint64_t head = ring->head;
    uint64_t tail = __atomic_load_n(&ring->tail, __ATOMIC_ACQUIRE);
    return (int)(tail - head);
}

#endif /* __AERON_IPC_H */
