#include "sve_similarity.h"
#include "sve_config.h"

#include <math.h>

#ifdef USE_ARM_SVE
static float sve_cosine_similarity_f32_sve_impl(const float *a,
                                                const float *b,
                                                size_t dim) {
    svfloat32_t dot_vec = svdup_f32(0.0f);
    svfloat32_t norm_a_vec = svdup_f32(0.0f);
    svfloat32_t norm_b_vec = svdup_f32(0.0f);

    size_t i = 0;
    while (i < dim) {
        svbool_t pg = svwhilelt_b32((uint64_t)i, (uint64_t)dim);
        svfloat32_t va = svld1_f32(pg, &a[i]);
        svfloat32_t vb = svld1_f32(pg, &b[i]);

        dot_vec = svmla_f32_m(pg, dot_vec, va, vb);
        norm_a_vec = svmla_f32_m(pg, norm_a_vec, va, va);
        norm_b_vec = svmla_f32_m(pg, norm_b_vec, vb, vb);

        i += svcntw();
    }

    float dot = svaddv_f32(svptrue_b32(), dot_vec);
    float norm_a = svaddv_f32(svptrue_b32(), norm_a_vec);
    float norm_b = svaddv_f32(svptrue_b32(), norm_b_vec);

    float denom = sqrtf(norm_a) * sqrtf(norm_b);
    if (denom == 0.0f) return 0.0f;
    return dot / denom;
}
#endif

static float sve_cosine_similarity_f32_scalar_impl(const float *a,
                                                   const float *b,
                                                   size_t dim) {
    float dot = 0.0f;
    float norm_a = 0.0f;
    float norm_b = 0.0f;

    for (size_t i = 0; i < dim; i++) {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }

    float denom = sqrtf(norm_a) * sqrtf(norm_b);
    if (denom == 0.0f) return 0.0f;
    return dot / denom;
}

float sve_cosine_similarity_f32(const float *a, const float *b, size_t dim) {
#ifdef USE_ARM_SVE
    return sve_cosine_similarity_f32_sve_impl(a, b, dim);
#else
    return sve_cosine_similarity_f32_scalar_impl(a, b, dim);
#endif
}
