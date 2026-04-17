/*
 * TLC Transport Layer — Userspace Protocol Alternatives
 *
 * Three transport modes to bypass kernel TCP stack:
 *
 * A. Unix Domain Socket (UDS)
 *    - Bypasses TCP/IP stack entirely (no routing, no netfilter)
 *    - Still uses kernel socket buffer copies
 *    - Expected: 1.5-2x vs TCP loopback
 *
 * B. Shared Memory IPC (SHM)
 *    - Zero-copy: client writes request to shared ring buffer
 *    - Server polls ring, processes, writes response to shared ring
 *    - Only syscall: futex for wake notification (optional)
 *    - Expected: 5-10x vs TCP loopback
 *    - This is the closest to real URMA single-sided READ
 *
 * C. io_uring (planned, requires kernel 5.10+)
 *    - Async I/O with submission/completion queues in shared memory
 *    - Kernel-side polling mode (SQPOLL)
 *    - Expected: 2-3x vs TCP loopback
 */
#ifndef __TLC_TRANSPORT_H
#define __TLC_TRANSPORT_H

#include <stdint.h>
#include <stddef.h>
#include <stdatomic.h>

/* ---- Shared Memory Ring Buffer for zero-syscall IPC ---- */

#define SHM_RING_SIZE     (1 << 16)  /* 64K slots */
#define SHM_RING_MASK     (SHM_RING_SIZE - 1)
#define SHM_SLOT_SIZE     1280       /* Max request/response size */
#define SHM_PATH          "/tlc_shm_ring"

/* Slot states */
#define SLOT_EMPTY    0
#define SLOT_REQUEST  1   /* Client wrote request, server should process */
#define SLOT_RESPONSE 2   /* Server wrote response, client should read */

typedef struct {
    volatile uint32_t state;   /* SLOT_EMPTY / SLOT_REQUEST / SLOT_RESPONSE */
    uint32_t          len;     /* Payload length */
    uint8_t           data[SHM_SLOT_SIZE];
} __attribute__((aligned(64))) shm_slot_t;  /* Cache-line aligned */

typedef struct {
    /* Request ring: client → server */
    atomic_uint_fast64_t req_head;   /* Server reads from here */
    uint8_t _pad1[56];
    atomic_uint_fast64_t req_tail;   /* Client writes here */
    uint8_t _pad2[56];

    /* Response ring: server → client (per-slot, no separate ring needed) */
    /* Responses are written back to the same slot */

    shm_slot_t slots[SHM_RING_SIZE];
} shm_ring_t;

/* Size of the shared memory region */
#define SHM_TOTAL_SIZE (sizeof(shm_ring_t))

#endif /* __TLC_TRANSPORT_H */
