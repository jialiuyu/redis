/*
 * Compare the current SVE row-copy loop in sve_serial_contiguous_read()
 * with the older always-full-predicate implementation.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#if defined(__ARM_FEATURE_SVE) || defined(USE_SVE)
#define USE_ARM_SVE 1
#include <arm_sve.h>
#endif

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static size_t round_up(size_t value, size_t align) {
    return ((value + align - 1) / align) * align;
}

#ifdef USE_ARM_SVE
static void copy_current(const float *src, float *dst, size_t dim) {
    const size_t vl = svcntw();
    size_t rem = dim;
    const float *srcp = src;
    float *dstp = dst;

    while (rem >= vl) {
        svbool_t pg = svptrue_b32();
        svfloat32_t v = svld1_f32(pg, srcp);
        svst1_f32(pg, dstp, v);
        srcp += vl;
        dstp += vl;
        rem -= vl;
    }

    if (rem > 0) {
        svbool_t pg = svwhilelt_b32_u64(0UL, (uint64_t)rem);
        svfloat32_t v = svld1_f32(pg, srcp);
        svst1_f32(pg, dstp, v);
    }
}

static void copy_legacy(const float *src, float *dst, size_t dim) {
    const size_t vl = svcntw();
    svbool_t pg = svptrue_b32();
    size_t offset = 0;

    while (offset < dim) {
        size_t remaining = dim - offset;
        size_t count = remaining < vl ? remaining : vl;
        svfloat32_t vec = svld1_f32(pg, &src[offset]);
        svst1_f32(pg, &dst[offset], vec);
        offset += count;
    }
}
#endif

static int verify_results(const float *src, const float *dst, size_t dim) {
    for (size_t i = 0; i < dim; i++) {
        if (src[i] != dst[i]) return 0;
    }
    return 1;
}

static int verify_tail_untouched(const float *dst, size_t dim, size_t padded_dim, float sentinel) {
    for (size_t i = dim; i < padded_dim; i++) {
        if (dst[i] != sentinel) return 0;
    }
    return 1;
}

static void run_case(size_t dim, int iters) {
#ifdef USE_ARM_SVE
    const size_t vl = svcntw();
    const size_t padded_dim = round_up(dim, vl) + vl;
    const float sentinel = -12345.0f;
    float *src = aligned_alloc(64, padded_dim * sizeof(float));
    float *dst_current = aligned_alloc(64, padded_dim * sizeof(float));
    float *dst_legacy = aligned_alloc(64, padded_dim * sizeof(float));
    if (!src || !dst_current || !dst_legacy) {
        fprintf(stderr, "allocation failed\n");
        free(src);
        free(dst_current);
        free(dst_legacy);
        return;
    }

    for (size_t i = 0; i < padded_dim; i++) {
        src[i] = (float)(i + 1);
        dst_current[i] = sentinel;
        dst_legacy[i] = sentinel;
    }

    copy_current(src, dst_current, dim);
    copy_legacy(src, dst_legacy, dim);

    int current_ok = verify_results(src, dst_current, dim);
    int legacy_ok = verify_results(src, dst_legacy, dim);
    int current_tail_ok = verify_tail_untouched(dst_current, dim, padded_dim, sentinel);
    int legacy_tail_ok = verify_tail_untouched(dst_legacy, dim, padded_dim, sentinel);

    uint64_t start = now_ns();
    for (int i = 0; i < iters; i++) copy_current(src, dst_current, dim);
    uint64_t current_ns = now_ns() - start;

    start = now_ns();
    for (int i = 0; i < iters; i++) copy_legacy(src, dst_legacy, dim);
    uint64_t legacy_ns = now_ns() - start;

    double current_ns_per_iter = (double)current_ns / iters;
    double legacy_ns_per_iter = (double)legacy_ns / iters;
    double current_gbps = ((double)dim * sizeof(float)) / current_ns_per_iter;
    double legacy_gbps = ((double)dim * sizeof(float)) / legacy_ns_per_iter;

    printf("dim=%zu, vl=%zu\n", dim, vl);
    printf("  current: %.2f ns/iter, %.2f bytes/ns, result=%s, tail=%s\n",
           current_ns_per_iter, current_gbps,
           current_ok ? "ok" : "bad",
           current_tail_ok ? "clean" : "dirty");
    printf("  legacy : %.2f ns/iter, %.2f bytes/ns, result=%s, tail=%s\n",
           legacy_ns_per_iter, legacy_gbps,
           legacy_ok ? "ok" : "bad",
           legacy_tail_ok ? "clean" : "dirty");
    printf("  speedup(current/legacy): %.2fx\n\n", legacy_ns_per_iter / current_ns_per_iter);

    free(src);
    free(dst_current);
    free(dst_legacy);
#else
    (void)dim;
    (void)iters;
    printf("SVE row-copy compare requires USE_SVE or an SVE-capable compiler target.\n");
#endif
}

int main(void) {
#ifdef USE_ARM_SVE
    printf("SVE row-copy compare benchmark\n\n");
    run_case(256, 200000);
    run_case(300, 200000);
    run_case(512, 200000);
    run_case(1024, 100000);
#else
    printf("SVE row-copy compare benchmark\n\n");
    run_case(0, 0);
#endif
    return 0;
}
