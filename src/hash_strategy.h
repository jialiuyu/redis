/*
 * hash_strategy.h — Compile-time Hash Strategy Dispatch for HOT Layer
 *
 * Select strategy via -DHASH_STRATEGY=N:
 *   1 = V1_LINEAR     Murmur3 full-64bit, linear stride +1
 *   2 = V2_ORIGINAL   Murmur3 on key>>32 (reproduces small-key bug), fib stride
 *   3 = V2_FIXED       Murmur3 on key&0xFFFFFFFF (bugfix), fib stride
 *   4 = V2_FULL64     Murmur3 full-64bit primary, fib stride
 *   5 = V3_ODDEVEN    Odd-bit positions for primary, even-bit for stride
 *   6 = V4_CRC32      Hardware CRC32, linear stride +1
 */
#ifndef __HASH_STRATEGY_H
#define __HASH_STRATEGY_H

#include <stdint.h>

#define HASH_STRATEGY_V1_LINEAR    1
#define HASH_STRATEGY_V2_FIXED     2
#define HASH_STRATEGY_V2_FULL64    3
#define HASH_STRATEGY_V4_CRC32     4

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

/* --- Salted variants for independent alternate placement --- */
static inline uint32_t _hs_murmur3_mix32_alt(uint32_t h) {
    return _hs_murmur3_mix32(h ^ 0x9e3779b9U);
}

static inline uint32_t _hs_murmur3_mix64_alt(uint64_t k) {
    return _hs_murmur3_mix64(k ^ 0x9e3779b97f4a7c15ULL);
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

static inline uint32_t hash_primary_alt(uint64_t key, uint32_t mask) {
    return _hs_murmur3_mix64_alt(key) & mask;
}

static inline uint32_t hash_probe_alt(uint64_t key, uint32_t mask, int probe_idx) {
    return (hash_primary_alt(key, mask) + (uint32_t)probe_idx) & mask;
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

static inline uint32_t hash_primary_alt(uint64_t key, uint32_t mask) {
    uint32_t lo = (uint32_t)(key & 0xFFFFFFFF);
    return _hs_murmur3_mix32_alt(lo) & mask;
}

static inline uint32_t hash_probe_alt(uint64_t key, uint32_t mask, int probe_idx) {
    uint32_t primary = hash_primary_alt(key, mask);
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

static inline uint32_t hash_primary_alt(uint64_t key, uint32_t mask) {
    return _hs_murmur3_mix64_alt(key) & mask;
}

static inline uint32_t hash_probe_alt(uint64_t key, uint32_t mask, int probe_idx) {
    uint32_t primary = hash_primary_alt(key, mask);
    uint32_t lo = (uint32_t)(key & 0xFFFFFFFF);
    uint32_t stride = _hs_fib_stride(lo ^ 0x9e3779b9U, mask);
    return (primary + (uint32_t)probe_idx * stride) & mask;
}

/* ============================================================
 * Strategy: V4_CRC32 — Hardware CRC32, stride=1
 *
 * Uses ARM CRC32 instruction (__crc32d) for single-cycle hashing.
 * Falls back to software CRC32 on non-ARM.
 * Linear probing with stride +1 (same as V1_LINEAR).
 * ============================================================ */
#elif HASH_STRATEGY == HASH_STRATEGY_V4_CRC32

#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
static inline uint32_t _hs_crc32_hash(uint64_t key) {
    return __crc32d(0xFFFFFFFF, key);
}
#else
static inline uint32_t _hs_crc32_hash(uint64_t key) {
    uint64_t crc = 0xFFFFFFFF;
    for (int i = 0; i < 8; i++) {
        crc ^= (uint64_t)((key >> (i * 8)) & 0xFF) << 56;
        for (int j = 0; j < 8; j++) {
            crc = (crc << 1) ^ ((crc >> 63) ? 0x42F0E1EBA9EA3693ULL : 0);
        }
    }
    return (uint32_t)(crc ^ 0xFFFFFFFF);
}
#endif

static inline const char *hash_strategy_name(void) { return "V4_CRC32"; }

static inline uint32_t hash_primary(uint64_t key, uint32_t mask) {
    return _hs_crc32_hash(key) & mask;
}

static inline uint32_t hash_probe(uint64_t key, uint32_t mask, int probe_idx) {
    return (hash_primary(key, mask) + (uint32_t)probe_idx) & mask;
}

static inline uint32_t hash_primary_alt(uint64_t key, uint32_t mask) {
    return _hs_crc32_hash(key ^ 0x9e3779b97f4a7c15ULL) & mask;
}

static inline uint32_t hash_probe_alt(uint64_t key, uint32_t mask, int probe_idx) {
    return (hash_primary_alt(key, mask) + (uint32_t)probe_idx) & mask;
}

#else
#error "Unknown HASH_STRATEGY. Use 1..4"
#endif

#endif /* __HASH_STRATEGY_H */
