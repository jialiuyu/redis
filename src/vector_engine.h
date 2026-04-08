/*
 * Vector Engine Abstraction Layer
 * Provides unified interface for traditional Redis vector operations
 * and UB bus-based high-performance feature queries
 */

#ifndef __VECTOR_ENGINE_H
#define __VECTOR_ENGINE_H

#include "server.h"
#ifdef VECTOR_ENGINE_MODULE
#include "redismodule.h"
#endif

/* Vector Engine Types */
#define VECTOR_ENGINE_REDIS 0    /* Traditional Redis HNSW implementation */
#define VECTOR_ENGINE_UB 1       /* UB bus + SVE high-performance implementation */

/* Vector Query Result */
typedef struct {
    sds element;           /* Element name */
    double score;          /* Similarity score */
    sds attributes;        /* JSON attributes (optional) */
} vector_query_result_t;

/* Vector Data */
typedef struct {
    float *data;           /* Vector data */
    size_t dim;            /* Vector dimension */
    int is_fp32;           /* Whether data is FP32 format */
} vector_data_t;

/* Vector Engine Interface */
typedef struct vector_engine {
    vector_engine_type_t type;
    int (*init)(void);
    void (*cleanup)(void);
    int (*vadd)(void *ctx, void *key, vector_data_t *vector, void *element, void *attributes);
    int (*vrem)(void *ctx, void *key, void *element);
    int (*vsim)(void *ctx, void *key, vector_data_t *query_vector, size_t count,
                vector_query_result_t **results, size_t *num_results);
    int (*vemb)(void *ctx, void *key, void *element, vector_data_t *result);
    int (*vcard)(void *ctx, void *key);
    int (*vdim)(void *ctx, void *key);
    int (*set_config)(const char *key, const char *value);
    sds (*get_config)(const char *key);
    sds (*get_stats)(void);
} vector_engine_t;

/* Global vector engine instance */
extern vector_engine_t *current_vector_engine;

/* Engine Management Functions */
vector_engine_t *vector_engine_create(vector_engine_type_t type);
void vector_engine_destroy(vector_engine_t *engine);
int vector_engine_switch(vector_engine_type_t type);

/* UB Engine Functions */
int ub_engine_vemb(void *ctx, void *key, void *element, vector_data_t *result);

/* Utility Functions */
vector_data_t *vector_data_create(float *data, size_t dim, int is_fp32);
void vector_data_destroy(vector_data_t *vd);
vector_query_result_t *vector_query_result_create(size_t count);
void vector_query_result_destroy(vector_query_result_t *results, size_t count);

/* Configuration */
int vector_engine_init_from_config(void);
vector_engine_type_t vector_engine_get_default_type(void);

#endif /* __VECTOR_ENGINE_H */