/*
 * eviction_strategy.h — Compile-time Eviction Strategy Dispatch for HOT Layer
 *
 * Select via -DEVICTION_STRATEGY=N:
 *   0 = BLIND   Blind eviction of primary slot (baseline)
 *   1 = CLOCK   Clock Second-Chance (1-bit ref per slot)
 *   2 = RR      Round-Robin across probe positions (no per-slot state)
 *
 * Orthogonal to hash_strategy.h — combine freely:
 *   -DHASH_STRATEGY=1 -DEVICTION_STRATEGY=2
 */
#ifndef __EVICTION_STRATEGY_H
#define __EVICTION_STRATEGY_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "hash_strategy.h"

#define EVICTION_BLIND  0
#define EVICTION_CLOCK  1
#define EVICTION_RR     2

#ifndef EVICTION_STRATEGY
#define EVICTION_STRATEGY EVICTION_BLIND
#endif

/* ============================================================
 * Strategy: BLIND — overwrite primary slot (current behavior)
 * ============================================================ */
#if EVICTION_STRATEGY == EVICTION_BLIND

static inline const char *eviction_strategy_name(void) { return "BLIND"; }

static inline int eviction_init(size_t capacity) {
    (void)capacity; return 0;
}

static inline void eviction_on_get(uint32_t slot) { (void)slot; }

static inline void eviction_on_put_hit(uint32_t slot) { (void)slot; }

static inline uint32_t eviction_select_victim_slots(const uint32_t *slots, uint32_t n) {
    (void)n;
    return slots[0];
}

static inline uint32_t eviction_select_victim(uint64_t key, uint32_t mask) {
    uint32_t slots[HASH_MAX_PROBES];
    for (uint32_t i = 0; i < HASH_MAX_PROBES; i++)
        slots[i] = hash_probe(key, mask, (int)i);
    return eviction_select_victim_slots(slots, HASH_MAX_PROBES);
}

static inline void eviction_destroy(void) {}

/* ============================================================
 * Strategy: CLOCK — Second-Chance (1-bit reference per slot)
 * ============================================================ */
#elif EVICTION_STRATEGY == EVICTION_CLOCK

static uint8_t *_clock_ref_bits;
static size_t   _clock_capacity;
static uint32_t _clock_evict_rr;  /* round-robin counter for fallback eviction */

static inline const char *eviction_strategy_name(void) { return "CLOCK"; }

static inline int eviction_init(size_t capacity) {
    _clock_capacity = capacity;
    _clock_ref_bits = (uint8_t *)calloc(capacity, 1);
    return _clock_ref_bits ? 0 : -1;
}

static inline void eviction_on_get(uint32_t slot) {
    if (slot < _clock_capacity) _clock_ref_bits[slot] = 1;
}

static inline void eviction_on_put_hit(uint32_t slot) {
    if (slot < _clock_capacity) _clock_ref_bits[slot] = 1;
}

static inline uint32_t eviction_select_victim_slots(const uint32_t *slots, uint32_t n) {
    /* Scan candidate slots, evict first with ref_bit == 0 */
    for (uint32_t i = 0; i < n; i++) {
        uint32_t s = slots[i];
        if (s < _clock_capacity && _clock_ref_bits[s] == 0)
            return s;
    }
    /* All have ref=1 — clear all, round-robin eviction */
    for (uint32_t i = 0; i < n; i++) {
        uint32_t s = slots[i];
        if (s < _clock_capacity) _clock_ref_bits[s] = 0;
    }
    uint32_t probe_idx = (_clock_evict_rr++) % n;
    return slots[probe_idx];
}

static inline uint32_t eviction_select_victim(uint64_t key, uint32_t mask) {
    uint32_t slots[HASH_MAX_PROBES];
    for (uint32_t i = 0; i < HASH_MAX_PROBES; i++)
        slots[i] = hash_probe(key, mask, (int)i);
    return eviction_select_victim_slots(slots, HASH_MAX_PROBES);
}

static inline void eviction_destroy(void) {
    free(_clock_ref_bits);
    _clock_ref_bits = NULL;
}

/* ============================================================
 * Strategy: RR — Round-Robin across probe positions
 * No per-slot state, just cycles 0→1→2→3→0...
 * ============================================================ */
#elif EVICTION_STRATEGY == EVICTION_RR

static uint32_t _rr_counter;

static inline const char *eviction_strategy_name(void) { return "RR"; }

static inline int eviction_init(size_t capacity) {
    (void)capacity; _rr_counter = 0; return 0;
}

static inline void eviction_on_get(uint32_t slot) { (void)slot; }

static inline void eviction_on_put_hit(uint32_t slot) { (void)slot; }

static inline uint32_t eviction_select_victim_slots(const uint32_t *slots, uint32_t n) {
    uint32_t probe_idx = (_rr_counter++) % n;
    return slots[probe_idx];
}

static inline uint32_t eviction_select_victim(uint64_t key, uint32_t mask) {
    uint32_t slots[HASH_MAX_PROBES];
    for (uint32_t i = 0; i < HASH_MAX_PROBES; i++)
        slots[i] = hash_probe(key, mask, (int)i);
    return eviction_select_victim_slots(slots, HASH_MAX_PROBES);
}

static inline void eviction_destroy(void) {}

#else
#error "Unknown EVICTION_STRATEGY. Use 0 (BLIND), 1 (CLOCK), or 2 (RR)"
#endif

#endif /* __EVICTION_STRATEGY_H */
