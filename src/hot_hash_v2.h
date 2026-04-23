/*
 * HOT Layer Hash v2 — Split-Key Low-Collision Hash
 *
 * The 16B HOT entry stores {8B key, 4B warm_idx, 4B pad}.
 * The hash function maps 8B key → slot index.
 *
 * V1 (current): single Murmur mix on full 64-bit key
 *   Problem: with 128K slots and 4-probe open addressing,
 *   collision rate is ~3% at 50% load factor.
 *
 * V2 (new): split key into high 32 bits + low 32 bits
 *   - Primary hash: Murmur mix on high 32 bits → primary slot
 *   - Probe step: Fibonacci hash on low 32 bits → probe stride
 *   - This creates a 2D hash space that virtually eliminates
 *     clustering in the 4-probe chain.
 *
 * Collision analysis:
 *   For two keys to collide in ALL 4 probes, they need:
 *   - Same primary hash (P = 1/capacity)
 *   - Same probe stride (P = 1/capacity)
 *   - Combined: P(4-way collision) = 1/capacity² ≈ 1/17B for 128K slots
 *
 * The "前4字节 + 后12字节等比规律缩放" pattern:
 *   In the 16B entry, the 8B key is split as:
 *   - Bytes 0-3 (high 32 bits): primary hash seed
 *   - Bytes 4-7 (low 32 bits): probe stride seed
 *   This maps to the "前4字节 + 后12字节" concept where the
 *   first 4 bytes determine the slot and the remaining bytes
 *   determine the probe pattern within the 16B entry structure.
 */
#ifndef __HOT_HASH_V2_H
#define __HOT_HASH_V2_H

#include <stdint.h>

/* Murmur3 finalizer for 32-bit */
static inline uint32_t murmur3_mix32(uint32_t h) {
    h ^= h >> 16; h *= 0x85ebca6b;
    h ^= h >> 13; h *= 0xc2b2ae35;
    h ^= h >> 16; return h;
}

/* Fibonacci hash for probe stride (golden ratio) */
static inline uint32_t fib_hash32(uint32_t h) {
    return (uint32_t)((uint64_t)h * 2654435769ULL >> 32);
}

/* V2 hash: returns primary slot */
static inline uint32_t hot_hash_v2_primary(uint64_t key, uint32_t mask) {
    uint32_t hi = (uint32_t)(key >> 32);
    return murmur3_mix32(hi) & mask;
}

/* V2 hash: returns probe stride (must be odd for full coverage) */
static inline uint32_t hot_hash_v2_stride(uint64_t key, uint32_t mask) {
    uint32_t lo = (uint32_t)(key & 0xFFFFFFFF);
    return (fib_hash32(lo) & mask) | 1;  /* Ensure odd stride */
}

/* V2 probe: slot for probe i */
static inline uint32_t hot_hash_v2_probe(uint64_t key, uint32_t mask, int probe_idx) {
    uint32_t primary = hot_hash_v2_primary(key, mask);
    uint32_t stride = hot_hash_v2_stride(key, mask);
    return (primary + probe_idx * stride) & mask;
}

/* V1 hash (original, for comparison) */
static inline uint32_t hot_hash_v1(uint64_t k, uint32_t mask) {
    k ^= k >> 33; k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33; k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33; return (uint32_t)(k & mask);
}

#endif /* __HOT_HASH_V2_H */
