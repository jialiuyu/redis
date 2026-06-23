/*
 * SVE Configuration Header
 * Unified SVE support detection for hpc-redis
 *
 * Usage:
 *   #include "sve_config.h"
 *   #ifdef USE_ARM_SVE
 *       // SVE code path
 *   #endif
 *
 * To enable SVE manually, compile with -DUSE_SVE
 */

#ifndef __SVE_CONFIG_H
#define __SVE_CONFIG_H

/*
 * Unified SVE detection macro: USE_ARM_SVE
 *
 * Enabled when:
 *   1. Compiler defines __ARM_FEATURE_SVE (automatic detection)
 *   2. User defines USE_SVE via -DUSE_SVE (manual override)
 */
#if defined(__ARM_FEATURE_SVE) || defined(USE_SVE)
#define USE_ARM_SVE 1
#endif

#ifdef USE_ARM_SVE
#include <arm_sve.h>
#endif

#endif /* __SVE_CONFIG_H */
