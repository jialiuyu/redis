/*
 * Vector Engine Abstraction Layer
 * Provides unified interface for traditional Redis vector operations
 * and UB bus-based high-performance feature queries
 */

#ifndef __VECTOR_ENGINE_H
#define __VECTOR_ENGINE_H

#include "vector_engine_types.h"
#include "sds.h"
#include <stddef.h>  /* size_t */


/* Vector Engine Configuration */
typedef struct vector_engine_config {
    vector_engine_type_t engine_type;    /* Engine type to use */
    int vector_dimension;                 /* Vector dimension */
} vector_engine_config_t;

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
typedef struct {
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

/* Get the active engine instance (NULL if not initialized) */
vector_engine_t *vector_engine_get(void);

/* Engine Registration - called by each impl to register itself */
void vector_engine_register(vector_engine_type_t type, vector_engine_t *engine);

/* Initialization - called once at startup, engine type fixed for lifetime */
int vector_engine_init_from_config(vector_engine_config_t *config);
void vector_engine_cleanup(void);
vector_engine_type_t vector_engine_get_default_type(void);
vector_engine_type_t vector_engine_get_current_type(void);

/* Engine lifecycle */
vector_engine_t *vector_engine_create(vector_engine_type_t type);
void vector_engine_destroy(vector_engine_t *engine);

/* Runtime engine switching */
int vector_engine_switch(vector_engine_type_t type);

/* UB engine operations (defined in vector_engine_ub_impl.c) */
int ub_engine_vemb(void *ctx, void *key, void *element, vector_data_t *result);

/* Utility Functions */
vector_data_t *vector_data_create(float *data, size_t dim, int is_fp32);
void vector_data_destroy(vector_data_t *vd);
vector_query_result_t *vector_query_result_create(size_t count);
void vector_query_result_destroy(vector_query_result_t *results, size_t count);

#endif /* __VECTOR_ENGINE_H */
