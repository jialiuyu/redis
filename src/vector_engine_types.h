/*
 * Vector Engine Type Definitions
 * Shared between server.h and vector_engine.h to avoid circular dependencies
 */

#ifndef __VECTOR_ENGINE_TYPES_H
#define __VECTOR_ENGINE_TYPES_H

typedef enum {
    VECTOR_ENGINE_REDIS = 0,    /* Traditional Redis HNSW implementation */
    VECTOR_ENGINE_UB    = 1,    /* UB bus + SVE high-performance implementation */
    VECTOR_ENGINE_VEMB_V16 = 2, /* VEMB V16 proxy + TLC + TCP/SHM dataplane */
    VECTOR_ENGINE_MAX           /* Must be last - used as registry size */
} vector_engine_type_t;

#endif /* __VECTOR_ENGINE_TYPES_H */
