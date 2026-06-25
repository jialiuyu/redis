#ifndef __VEMB_V16_UTIL_H
#define __VEMB_V16_UTIL_H

#include <stddef.h>
#include <stdint.h>
#include <time.h>

static inline uint64_t vemb_v16_monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) +
        (uint64_t)ts.tv_nsec;
}

static inline uint32_t vemb_v16_pow2_ceil_u32(uint64_t value) {
    uint32_t p = 1;
    while ((uint64_t)p < value && p < (UINT32_C(1) << 30))
        p <<= 1;
    return p;
}

static inline size_t vemb_v16_align64_size(size_t value) {
    return (value + 63u) & ~(size_t)63u;
}

#endif
