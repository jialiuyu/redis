/*
 * SVE2 Adam Optimizer — CPU-side optimizer state offload
 *
 * Implements Adam/AdamW update on CPU using SVE2 FMA:
 *   m_t = β1 * m_{t-1} + (1-β1) * g
 *   v_t = β2 * v_{t-1} + (1-β2) * g²
 *   θ_t = θ_{t-1} - lr * m_t / (√v_t + ε)
 *
 * All state (m, v, θ, g) resides in UB memory (L3-resident).
 * SVE2 processes 8 floats/cycle (256-bit), 4-way unrolled.
 *
 * This offloads ~24 bytes/param of optimizer state from NPU HBM
 * (FP32 m + FP32 v + FP32 master_weight = 12B, plus gradients).
 */
#ifndef __SVE2_ADAM_H
#define __SVE2_ADAM_H

#include <stddef.h>
#include <math.h>
#include "sve_config.h"

typedef struct {
    float lr;       /* Learning rate */
    float beta1;    /* First moment decay (0.9) */
    float beta2;    /* Second moment decay (0.999) */
    float eps;      /* Epsilon (1e-8) */
    float wd;       /* Weight decay (AdamW) */
    int   step;     /* Current step (for bias correction) */
} adam_config_t;

static inline adam_config_t adam_default(void) {
    return (adam_config_t){.lr=1e-4f, .beta1=0.9f, .beta2=0.999f,
                           .eps=1e-8f, .wd=0.01f, .step=0};
}

/*
 * SVE2 Adam update kernel — processes N parameters in one call.
 * All arrays must be 64-byte aligned (UB memory guarantees this).
 *
 * FLOPs per parameter: ~15 (2 FMA for m, 3 for v, 1 sqrt, 1 div, etc.)
 * Bytes per parameter: 16 (read m,v,θ,g; write m,v,θ) = memory-bound
 */
static inline void sve2_adam_update(
    float *param,       /* θ: model parameters [N] */
    const float *grad,  /* g: gradients [N] */
    float *exp_avg,     /* m: first moment [N] */
    float *exp_avg_sq,  /* v: second moment [N] */
    size_t N,
    const adam_config_t *cfg)
{
    float b1 = cfg->beta1, b2 = cfg->beta2;
    float ob1 = 1.0f - b1, ob2 = 1.0f - b2;
    int step = cfg->step + 1;
    float bc1 = 1.0f / (1.0f - powf(b1, (float)step));
    float bc2 = 1.0f / (1.0f - powf(b2, (float)step));
    float lr = cfg->lr;
    float eps = cfg->eps;
    float wd = cfg->wd;

#ifdef USE_ARM_SVE
    svfloat32_t vb1  = svdup_f32(b1);
    svfloat32_t vob1 = svdup_f32(ob1);
    svfloat32_t vb2  = svdup_f32(b2);
    svfloat32_t vob2 = svdup_f32(ob2);
    svfloat32_t vbc1 = svdup_f32(bc1);
    svfloat32_t vbc2 = svdup_f32(bc2);
    svfloat32_t vlr  = svdup_f32(-lr);
    svfloat32_t veps = svdup_f32(eps);
    svfloat32_t vwd  = svdup_f32(1.0f - lr * wd);

    size_t i = 0;
    while (i < N) {
        svbool_t pg = svwhilelt_b32_u64(i, N);

        /* Load */
        svfloat32_t vg = svld1_f32(pg, &grad[i]);
        svfloat32_t vm = svld1_f32(pg, &exp_avg[i]);
        svfloat32_t vv = svld1_f32(pg, &exp_avg_sq[i]);
        svfloat32_t vp = svld1_f32(pg, &param[i]);

        /* m = β1*m + (1-β1)*g */
        vm = svmla_f32_m(pg, svmul_f32_m(pg, vm, vb1), vob1, vg);

        /* v = β2*v + (1-β2)*g² */
        svfloat32_t g2 = svmul_f32_m(pg, vg, vg);
        vv = svmla_f32_m(pg, svmul_f32_m(pg, vv, vb2), vob2, g2);

        /* Bias-corrected: m_hat = m*bc1, v_hat = v*bc2 */
        svfloat32_t mh = svmul_f32_m(pg, vm, vbc1);
        svfloat32_t vh = svmul_f32_m(pg, vv, vbc2);

        /* θ = θ*(1-lr*wd) - lr * m_hat / (√v_hat + ε) */
        svfloat32_t denom = svadd_f32_m(pg, svsqrt_f32_x(pg, vh), veps);
        svfloat32_t update = svdiv_f32_m(pg, mh, denom);
        vp = svmla_f32_m(pg, svmul_f32_m(pg, vp, vwd), vlr, update);

        /* Store */
        svst1_f32(pg, &exp_avg[i], vm);
        svst1_f32(pg, &exp_avg_sq[i], vv);
        svst1_f32(pg, &param[i], vp);

        i += svcntw();
    }
#else
    for (size_t i = 0; i < N; i++) {
        float g = grad[i];
        exp_avg[i] = b1 * exp_avg[i] + ob1 * g;
        exp_avg_sq[i] = b2 * exp_avg_sq[i] + ob2 * g * g;
        float mh = exp_avg[i] * bc1;
        float vh = exp_avg_sq[i] * bc2;
        param[i] = param[i] * (1.0f - lr*wd) - lr * mh / (sqrtf(vh) + eps);
    }
#endif
}

#endif /* __SVE2_ADAM_H */
