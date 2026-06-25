#ifndef __VEMB_V16_HASH_H
#define __VEMB_V16_HASH_H

#include <stddef.h>
#include <stdint.h>

static inline uint64_t vemb_v16_fnv1a64(const char *text) {
    const unsigned char *p = (const unsigned char *)text;
    uint64_t hash = UINT64_C(1469598103934665603);

    while (p && *p) {
        hash ^= (uint64_t)*p++;
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}

static inline uint64_t vemb_v16_fnv1a64_bytes(const void *data, size_t len) {
    const unsigned char *p = (const unsigned char *)data;
    uint64_t hash = UINT64_C(1469598103934665603);

    for (size_t i = 0; p && i < len; i++) {
        hash ^= (uint64_t)p[i];
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}

static inline uint64_t vemb_v16_avalanche64(uint64_t key) {
    key ^= key >> 33;
    key *= UINT64_C(0xff51afd7ed558ccd);
    key ^= key >> 33;
    key *= UINT64_C(0xc4ceb9fe1a85ec53);
    key ^= key >> 33;
    return key;
}

static inline uint32_t vemb_v16_hash_mask_u64(uint64_t key, uint32_t mask) {
    return (uint32_t)vemb_v16_avalanche64(key) & mask;
}

static inline uint32_t vemb_v16_mix32_u64(uint64_t key) {
    key = vemb_v16_avalanche64(key);
    return (uint32_t)(key ^ (key >> 32));
}

static inline uint32_t vemb_v16_murmur3_32(const char *key, size_t len) {
    const uint32_t c1 = 0xcc9e2d51u;
    const uint32_t c2 = 0x1b873593u;
    const uint32_t seed = 0x5bd1e995u;

    uint32_t h = seed;
    const uint8_t *data = (const uint8_t *)key;
    const int nblocks = (int)(len / 4);
    const uint32_t *blocks =
        (const uint32_t *)(const void *)(data + nblocks * 4);

    for (int i = -nblocks; i; i++) {
        uint32_t k = blocks[i];
        k *= c1;
        k = (k << 15) | (k >> 17);
        k *= c2;

        h ^= k;
        h = (h << 13) | (h >> 19);
        h = h * 5 + 0xe6546b64u;
    }

    const uint8_t *tail = data + nblocks * 4;
    uint32_t k = 0;
    switch (len & 3u) {
    case 3: k ^= (uint32_t)tail[2] << 16; /* fall through */
    case 2: k ^= (uint32_t)tail[1] << 8;  /* fall through */
    case 1:
        k ^= (uint32_t)tail[0];
        k *= c1;
        k = (k << 15) | (k >> 17);
        k *= c2;
        h ^= k;
        break;
    default:
        break;
    }

    h ^= (uint32_t)len;
    h ^= h >> 16;
    h *= 0x85ebca6bu;
    h ^= h >> 13;
    h *= 0xc2b2ae35u;
    h ^= h >> 16;
    return h;
}

static inline uint32_t vemb_v16_murmur3(const char *key, size_t len) {
    return vemb_v16_murmur3_32(key, len);
}

static inline uint32_t murmur3_hash(const char *key, size_t len) {
    return vemb_v16_murmur3_32(key, len);
}

#endif
