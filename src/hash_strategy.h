/*
 * hash_strategy.h — Compile-time Hash Strategy Dispatch for HOT Layer
 *
 * Select strategy via -DHASH_STRATEGY=N:
 *   1 = V1_LINEAR     Murmur3 full-64bit, linear stride +1
 *   2 = V2_ORIGINAL   Murmur3 on key>>32 (reproduces small-key bug), fib stride
 *   3 = V2_FIXED       Murmur3 on key&0xFFFFFFFF (bugfix), fib stride
 *   4 = V2_FULL64     Murmur3 full-64bit primary, fib stride
 *   5 = V3_ODDEVEN    Odd-bit positions for primary, even-bit for stride
 */
#ifndef __HASH_STRATEGY_H
#define __HASH_STRATEGY_H

#include <stdint.h>

#define HASH_STRATEGY_V1_LINEAR    1
#define HASH_STRATEGY_V2_ORIGINAL  2
#define HASH_STRATEGY_V2_FIXED     3
#define HASH_STRATEGY_V2_FULL64    4
#define HASH_STRATEGY_V3_ODDEVEN   5

#ifndef HASH_STRATEGY
#define HASH_STRATEGY HASH_STRATEGY_V1_LINEAR
#endif

#define HASH_MAX_PROBES 4

/* --- Murmur3 finalizer (32-bit) --- */
static inline uint32_t _hs_murmur3_mix32(uint32_t h) {
    h ^= h >> 16; h *= 0x85ebca6b;
    h ^= h >> 13; h *= 0xc2b2ae35;
    h ^= h >> 16; return h;
}

/* --- Murmur3 finalizer (64-bit) --- */
static inline uint32_t _hs_murmur3_mix64(uint64_t k) {
    k ^= k >> 33; k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33; k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33; return (uint32_t)(k & 0xFFFFFFFF);
}

/* --- Fibonacci hash for stride --- */
static inline uint32_t _hs_fib_stride(uint32_t h, uint32_t mask) {
    return (uint32_t)((uint64_t)h * 2654435769ULL >> 32) & mask | 1;
}

/* ============================================================
 * Strategy: V1_LINEAR — Murmur3 full-64bit, stride=1
 * ============================================================ */
#if HASH_STRATEGY == HASH_STRATEGY_V1_LINEAR

static inline const char *hash_strategy_name(void) { return "V1_LINEAR"; }

static inline uint32_t hash_primary(uint64_t key, uint32_t mask) {
    return _hs_murmur3_mix64(key) & mask;
}

static inline uint32_t hash_probe(uint64_t key, uint32_t mask, int probe_idx) {
    return (hash_primary(key, mask) + (uint32_t)probe_idx) & mask;
}

/* ============================================================
 * Strategy: V2_ORIGINAL — Murmur3 on high-32 (BUGGY for small keys)
 * ============================================================ */
#elif HASH_STRATEGY == HASH_STRATEGY_V2_ORIGINAL

static inline const char *hash_strategy_name(void) { return "V2_ORIGINAL"; }

static inline uint32_t hash_primary(uint64_t key, uint32_t mask) {
    uint32_t hi = (uint32_t)(key >> 32);
    return _hs_murmur3_mix32(hi) & mask;
}

static inline uint32_t hash_probe(uint64_t key, uint32_t mask, int probe_idx) {
    uint32_t primary = hash_primary(key, mask);
    uint32_t lo = (uint32_t)(key & 0xFFFFFFFF);
    uint32_t stride = _hs_fib_stride(lo, mask);
    return (primary + (uint32_t)probe_idx * stride) & mask;
}

/* ============================================================
 * Strategy: V2_FIXED — Murmur3 on low-32 (bugfix candidate)
 * ============================================================ */
#elif HASH_STRATEGY == HASH_STRATEGY_V2_FIXED

static inline const char *hash_strategy_name(void) { return "V2_FIXED"; }

static inline uint32_t hash_primary(uint64_t key, uint32_t mask) {
    uint32_t lo = (uint32_t)(key & 0xFFFFFFFF);
    return _hs_murmur3_mix32(lo) & mask;
}

static inline uint32_t hash_probe(uint64_t key, uint32_t mask, int probe_idx) {
    uint32_t primary = hash_primary(key, mask);
    uint32_t hi = (uint32_t)(key >> 32);
    uint32_t stride = _hs_fib_stride(hi, mask);
    return (primary + (uint32_t)probe_idx * stride) & mask;
}

/* ============================================================
 * Strategy: V2_FULL64 — Murmur3 full-64bit primary, fib stride
 * ============================================================ */
#elif HASH_STRATEGY == HASH_STRATEGY_V2_FULL64

static inline const char *hash_strategy_name(void) { return "V2_FULL64"; }

static inline uint32_t hash_primary(uint64_t key, uint32_t mask) {
    return _hs_murmur3_mix64(key) & mask;
}

static inline uint32_t hash_probe(uint64_t key, uint32_t mask, int probe_idx) {
    uint32_t primary = hash_primary(key, mask);
    uint32_t lo = (uint32_t)(key & 0xFFFFFFFF);
    uint32_t stride = _hs_fib_stride(lo, mask);
    return (primary + (uint32_t)probe_idx * stride) & mask;
}

/* ============================================================
 * Strategy: V3_ODDEVEN — odd-bit primary, even-bit stride
 *
 * key = 64 bits, split by bit position:
 *   primary ← Murmur3(key & 0xAAAAAAAAAAAAAAAA)  (bits 1,3,5,...)
 *   stride  ← FibHash(key & 0x5555555555555555)  (bits 0,2,4,...)
 *
 * For small keys (< 2^32), both halves carry information from the
 * active bits, unlike V2_FIXED where hi32 = 0 kills the stride.
 * ============================================================ */
#elif HASH_STRATEGY == HASH_STRATEGY_V3_ODDEVEN

static inline const char *hash_strategy_name(void) { return "V3_ODDEVEN"; }

static inline uint32_t hash_primary(uint64_t key, uint32_t mask) {
    return _hs_murmur3_mix64(key & 0xAAAAAAAAAAAAAAAAULL) & mask;
}

static inline uint32_t hash_probe(uint64_t key, uint32_t mask, int probe_idx) {
    uint32_t primary = hash_primary(key, mask);
    uint32_t even = (uint32_t)(key & 0x5555555555555555ULL);
    uint32_t stride = _hs_fib_stride(even, mask);
    return (primary + (uint32_t)probe_idx * stride) & mask;
}

#else
#error "Unknown HASH_STRATEGY. Use 1..5"
#endif

#endif /* __HASH_STRATEGY_H */
