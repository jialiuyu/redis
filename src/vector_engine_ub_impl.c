/*
 * Vector Engine - UB Implementation
 * High-performance UB bus-based vector operations with SVE acceleration
 *
 * Key differences from native Redis vector-set behaviour:
 *
 * 1. Single-table addressing model
 *    Redis uses per-key HNSW graphs where elements are user-defined names
 *    (e.g. "doc:123"). UB uses a flat shared-memory table where vectors are
 *    addressed by row index. The `key` parameter is therefore unused in all
 *    UB engine callbacks (UNUSED(key)).
 *
 * 2. Element identity
 *    In VSIM results, `element` is the stringified row index ("0", "1", …)
 *    rather than a user-supplied name. Callers must maintain their own
 *    index-to-object mapping externally.
 *
 * 3. Attributes not supported
 *    Redis HNSW nodes can carry JSON attributes (set via VADD … SETATTR)
 *    used for hybrid FILTER queries. UB stores raw vectors only, so
 *    `attributes` is always NULL in query results. VSIM … WITHATTRIBS will
 *    return nil for every element; VSIM … FILTER is not available.
 *
 * 4. Similarity search strategy
 *    Redis VSIM walks an HNSW graph (approximate, O(log N)).
 *    UB VSIM performs a brute-force full-table scan (exact, O(N·dim)).
 *    UB is faster for small tables; Redis HNSW wins on large datasets.
 */

#include "macro.h"
#include "vector_engine.h"
#include "ub_client.h"
#include "sve_config.h"
#include "zmalloc.h"
#include "server.h"

#include <inttypes.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#ifndef UNUSED
#define UNUSED(V) ((void)(V))
#endif

/* ============================================================
 * VSIM helper types and functions
 * ============================================================ */

typedef struct {
    uint64_t index;   /* Row index in the embedding table */
    float score;      /* Cosine similarity with the query vector */
} vsim_candidate_t;

#ifdef USE_ARM_SVE
/* SVE-optimized cosine similarity for ARM processors */
static float cosine_similarity_f32_sve(const float *a, const float *b, size_t dim)
{
    svfloat32_t dot_vec = svdup_f32(0.0f);
    svfloat32_t norm_a_vec = svdup_f32(0.0f);
    svfloat32_t norm_b_vec = svdup_f32(0.0f);

    size_t i = 0;
    svbool_t pg;

    /* Process vectors using SVE with predication for tail handling */
    while (i < dim) {
        pg = svwhilelt_b32(i, dim);

        svfloat32_t va = svld1_f32(pg, &a[i]);
        svfloat32_t vb = svld1_f32(pg, &b[i]);

        /* Accumulate dot product: dot += a[i] * b[i] */
        dot_vec = svmla_f32_m(pg, dot_vec, va, vb);

        /* Accumulate squared norms: norm_a += a[i]^2, norm_b += b[i]^2 */
        norm_a_vec = svmla_f32_m(pg, norm_a_vec, va, va);
        norm_b_vec = svmla_f32_m(pg, norm_b_vec, vb, vb);

        i += svcntw();
    }

    /* Horizontal reduction to get scalar results */
    float dot = svaddv_f32(svptrue_b32(), dot_vec);
    float norm_a = svaddv_f32(svptrue_b32(), norm_a_vec);
    float norm_b = svaddv_f32(svptrue_b32(), norm_b_vec);

    float denom = sqrtf(norm_a) * sqrtf(norm_b);
    if (denom == 0.0f) return 0.0f;
    return dot / denom;
}
#endif /* USE_ARM_SVE */

/* Scalar fallback implementation */
static float cosine_similarity_f32_scalar(const float *a, const float *b, size_t dim)
{
    float dot = 0.0f;
    float norm_a = 0.0f;
    float norm_b = 0.0f;

    for (size_t i = 0; i < dim; i++) {
        dot    += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }

    float denom = sqrtf(norm_a) * sqrtf(norm_b);
    if (denom == 0.0f) return 0.0f;
    return dot / denom;
}

/* Unified cosine similarity function - dispatches to SVE or scalar */
static float cosine_similarity_f32(const float *a, const float *b, size_t dim)
{
#ifdef USE_ARM_SVE
    return cosine_similarity_f32_sve(a, b, dim);
#else
    return cosine_similarity_f32_scalar(a, b, dim);
#endif
}

static int vsim_score_cmp(const void *x, const void *y)
{
    const vsim_candidate_t *ca = (const vsim_candidate_t *)x;
    const vsim_candidate_t *cb = (const vsim_candidate_t *)y;

    if (cb->score > ca->score) return 1;
    if (cb->score < ca->score) return -1;
    return 0;
}

/* Helper function to extract C string from Redis object */
static const char *ub_engine_object_to_cstring(void *arg, sds *tmp)
{
    robj *obj = (robj *)arg;

    if (arg == NULL) {
        return NULL;
    }
    if (sdsEncodedObject(obj)) {
        return obj->ptr;
    }

    obj = getDecodedObject(obj);
    if (obj == NULL || !sdsEncodedObject(obj)) {
        return NULL;
    }

    *tmp = sdsdup(obj->ptr);
    decrRefCount(obj);
    return *tmp;
}

/* ============================================================
 * UB Engine Common Helpers
 * ============================================================ */

/* Cached address space, loaded once during init */
static ub_address_space_t *ub_cached_addr_space = NULL;

/*
 * Get the cached address space. Returns NULL if engine not initialized.
 */
static inline ub_address_space_t *ub_engine_get_addr_space(void) {
    return ub_cached_addr_space;
}

/*
 * Resolve element name and index from Redis object or raw C string.
 * Caller must sdsfree both resource_tmp and element_tmp after use.
 */
static int ub_engine_resolve_element(void *ctx, void *element,
                                     uint64_t *index,
                                     sds *element_tmp) {
    ub_mem_config_t *cfg = &server.ub;
    const char *element_name = ctx ? ub_engine_object_to_cstring(element, element_tmp)
                                   : (const char *)element;
    if (!element_name) return C_ERR;
    return ub_client_resolve_element_index(element_name, index, cfg->vector_dimension);
}

/* ============================================================
 * UB Engine Implementation
 * ============================================================ */

static int ub_engine_init(void) {
    serverLog(LL_NOTICE, "Initializing UB Vector Engine");
    if (ub_client_init(&server.ub) != C_OK) return C_ERR;

    /* Pre-load the embedding table once */
    if (ub_client_load_embedding_table(server.ub.table_name, &ub_cached_addr_space) != C_OK
        || !ub_cached_addr_space) {
        serverLog(LL_WARNING, "UB Vector Engine: failed to load embedding table");
        return C_ERR;
    }

    return C_OK;
}

static void ub_engine_cleanup(void) {
    ub_cached_addr_space = NULL;  /* owned by ub_client, freed in ub_client_cleanup */
    ub_client_cleanup();
    serverLog(LL_NOTICE, "Cleaning up UB Vector Engine");
}

static int ub_engine_vadd(void *ctx, void *key, vector_data_t *vector,
                          void *element, void *attributes) {
    ub_mem_config_t *cfg = &server.ub;
    ub_address_space_t *addr_space = ub_engine_get_addr_space();
    uint64_t index = 0;
    sds element_tmp = NULL;
    int rc = C_ERR;

    UNUSED(attributes); UNUSED(key);

    if (!addr_space) return C_ERR;
    if (!vector || cfg->vector_dimension <= 0) return C_ERR;
    if (vector->dim != (size_t)cfg->vector_dimension) return C_ERR;

    if (ub_engine_resolve_element(ctx, element, &index, &element_tmp) != C_OK) goto cleanup;
    if (ub_client_store_single(addr_space, index, vector->data, vector->dim) != C_OK) goto cleanup;

    rc = C_OK;

cleanup:
    sdsfree(element_tmp);
    return rc;
}

static int ub_engine_vrem(void *ctx, void *key, void *element) {
    ub_mem_config_t *cfg = &server.ub;
    ub_address_space_t *addr_space = ub_engine_get_addr_space();
    uint64_t index = 0;
    sds element_tmp = NULL;
    float *zero_buf = NULL;
    int rc = C_ERR;

    UNUSED(key);

    if (!addr_space || cfg->vector_dimension <= 0) return C_ERR;

    if (ub_engine_resolve_element(ctx, element, &index, &element_tmp) != C_OK) goto cleanup;

    /* Allocate zero-filled buffer for soft delete */
    zero_buf = zcalloc((size_t)cfg->vector_dimension * sizeof(float));
    if (!zero_buf) goto cleanup;

    if (ub_client_store_single(addr_space, index,
                              zero_buf, (size_t)cfg->vector_dimension) != C_OK) goto cleanup;

    rc = C_OK;

cleanup:
    zfree(zero_buf);
    sdsfree(element_tmp);
    return rc;
}

// TODO: brute-force full-table scan for now, needs optimization
static int ub_engine_vsim(void *ctx, void *key, vector_data_t *query_vector,
                          size_t count, vector_query_result_t **results,
                          size_t *num_results) {
    ub_mem_config_t *cfg = &server.ub;
    ub_address_space_t *addr_space = ub_engine_get_addr_space();
    float *candidates_buf = NULL;
    vsim_candidate_t *candidates = NULL;
    size_t dim, capacity, result_count;
    int rc = C_ERR;

    UNUSED(ctx); UNUSED(key);

    RETURN_IF(!query_vector || !results || !num_results || !addr_space, C_ERR);

    dim = (size_t)cfg->vector_dimension;
    if (dim == 0 || query_vector->dim != dim) return C_ERR;

    if (addr_space->vector_stride_bytes == 0) return C_ERR;

    capacity = addr_space->size / addr_space->vector_stride_bytes;
    if (capacity == 0) {
        *num_results = 0;
        *results = NULL;
        return C_OK;
    }

    /* Allocate temporary buffers — no indices array needed */
    candidates_buf = zmalloc(capacity * dim * sizeof(float));
    candidates = zmalloc(capacity * sizeof(vsim_candidate_t));
    if (!candidates_buf || !candidates) goto cleanup;

    /* Contiguous load: all rows starting from index 0 */
    if (ub_client_perform_contiguous_load(addr_space, 0, capacity,
                                          candidates_buf, dim) != C_OK) {
        goto cleanup;
    }

    /* Compute cosine similarity for each candidate */
    for (size_t i = 0; i < capacity; i++) {
        candidates[i].index = (uint64_t)i;
        candidates[i].score = cosine_similarity_f32(query_vector->data,
                                                    candidates_buf + i * dim,
                                                    dim);
    }

    /* Sort by score descending */
    qsort(candidates, capacity, sizeof(vsim_candidate_t), vsim_score_cmp);

    /* Build top-k results */
    result_count = count < capacity ? count : capacity;
    *results = vector_query_result_create(result_count);
    if (!*results) goto cleanup;

    for (size_t i = 0; i < result_count; i++) {
        (*results)[i].element = sdscatprintf(sdsempty(), "%" PRIu64, candidates[i].index);
        (*results)[i].score = (double)candidates[i].score;
        (*results)[i].attributes = NULL;
    }
    *num_results = result_count;
    rc = C_OK;

cleanup:
    zfree(candidates_buf);
    zfree(candidates);
    return rc;
}

/* Exported for use by vset module */
int ub_engine_vemb(void *ctx, void *key, void *element, vector_data_t *result) {
    ub_mem_config_t *cfg = &server.ub;
    ub_address_space_t *addr_space = ub_engine_get_addr_space();
    uint64_t index = 0;
    sds element_tmp = NULL;
    int rc = C_ERR;

    UNUSED(key);

    if (!result || !addr_space || cfg->vector_dimension <= 0) return C_ERR;

    if (ub_engine_resolve_element(ctx, element, &index, &element_tmp) != C_OK) goto cleanup;

    result->dim = (size_t)cfg->vector_dimension;
    result->is_fp32 = 1;
    result->data = zmalloc(sizeof(float) * result->dim);
    if (!result->data) goto cleanup;

    if (ub_client_load_single(addr_space, index,
                             result->data, result->dim) != C_OK) {
        zfree(result->data);
        result->data = NULL;
        result->dim = 0;
        goto cleanup;
    }

    rc = C_OK;

cleanup:
    sdsfree(element_tmp);
    return rc;
}

static int ub_engine_vcard(void *ctx, void *key) {
    ub_address_space_t *addr_space = ub_engine_get_addr_space();

    UNUSED(ctx); UNUSED(key);

    if (!addr_space || addr_space->vector_stride_bytes == 0) return 0;
    return (int)(addr_space->size / addr_space->vector_stride_bytes);
}

static int ub_engine_vdim(void *ctx, void *key) {
    UNUSED(ctx); UNUSED(key);
    return server.ub.vector_dimension > 0 ? server.ub.vector_dimension : 0;
}

static int ub_engine_set_config(const char *key, const char *value) {
    return ub_client_set_config(key, value);
}

static sds ub_engine_get_config(const char *key) {
    return ub_client_get_config(key);
}

static sds ub_engine_get_stats(void) {
    return ub_client_get_stats();
}

/* UB Engine Structure */
static vector_engine_t ub_vector_engine = {
    .type = VECTOR_ENGINE_UB,
    .init = ub_engine_init,
    .cleanup = ub_engine_cleanup,
    .vadd = ub_engine_vadd,
    .vrem = ub_engine_vrem,
    .vsim = ub_engine_vsim,
    .vemb = ub_engine_vemb,
    .vcard = ub_engine_vcard,
    .vdim = ub_engine_vdim,
    .set_config = ub_engine_set_config,
    .get_config = ub_engine_get_config,
    .get_stats = ub_engine_get_stats
};

/* Auto-register on load via GCC/Clang constructor */
__attribute__((constructor))
static void ub_engine_register(void) {
    vector_engine_register(VECTOR_ENGINE_UB, &ub_vector_engine);
}
