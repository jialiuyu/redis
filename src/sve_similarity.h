#ifndef __SVE_SIMILARITY_H
#define __SVE_SIMILARITY_H

#include <stddef.h>

float sve_cosine_similarity_f32(const float *a, const float *b, size_t dim);

#endif
