/*
 * Vector Engine Manager
 *
 * The engine type is determined at startup via vector_engine_init_from_config()
 * and remains fixed for the lifetime of the process. Runtime switching is not
 * supported — the data lives in one place (Redis HNSW or UB shared memory),
 * and silently falling back to a different data source would break consistency.
 */

#include "macro.h"
#include "server.h"
#include "vector_engine.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Active engine instance — internal, use vector_engine_get() */
static vector_engine_t *current_engine = NULL;

/* Engine registry — populated by each impl via __attribute__((constructor)) */
static vector_engine_t *engine_registry[VECTOR_ENGINE_MAX] = {0};

/* ============================================================
 * Engine Registration
 * ============================================================ */

void vector_engine_register(vector_engine_type_t type, vector_engine_t *engine) {
    if (type >= 0 && type < VECTOR_ENGINE_MAX) {
        engine_registry[type] = engine;
    }
}

/* ============================================================
 * Engine Lifecycle
 * ============================================================ */

vector_engine_t *vector_engine_create(vector_engine_type_t type) {
    if (type < 0 || type >= VECTOR_ENGINE_MAX) return NULL;

    vector_engine_t *engine = engine_registry[type];
    if (!engine) return NULL;

    if (engine->init() != C_OK) {
        return NULL;
    }

    return engine;
}

void vector_engine_destroy(vector_engine_t *engine) {
    if (engine) {
        engine->cleanup();
    }
}

/* ============================================================
 * Initialization (once at startup)
 * ============================================================ */

int vector_engine_init_from_config(vector_engine_config_t *config) {
    vector_engine_type_t type;
    vector_engine_t *engine;

    if (config == NULL) {
        type = server.vector_engine_type;
    } else {
        type = config->engine_type;

        if (type == VECTOR_ENGINE_UB && config->vector_dimension > 0) {
            server.ub.vector_dimension = config->vector_dimension;
        }
    }

    if (type < 0 || type >= VECTOR_ENGINE_MAX) {
        serverLog(LL_WARNING, "Invalid vector engine type %d", type);
        return C_ERR;
    }

    engine = engine_registry[type];
    if (!engine) {
        serverLog(LL_WARNING, "Vector engine type %d not registered", type);
        return C_ERR;
    }

    if (engine->init() != C_OK) {
        serverLog(LL_WARNING, "Vector engine type %d init failed", type);
        return C_ERR;
    }

    current_engine = engine;
    serverLog(LL_NOTICE, "Vector engine initialized: %s",
              type == VECTOR_ENGINE_UB ? "UB" : "Redis");
    return C_OK;
}

void vector_engine_cleanup(void) {
    if (current_engine) {
        current_engine->cleanup();
        current_engine = NULL;
    }
}

vector_engine_type_t vector_engine_get_default_type(void) {
    return VECTOR_ENGINE_REDIS;
}

vector_engine_type_t vector_engine_get_current_type(void) {
    return current_engine ? current_engine->type : VECTOR_ENGINE_REDIS;
}

vector_engine_t *vector_engine_get(void) {
    return current_engine;
}

int vector_engine_switch(vector_engine_type_t type) {
    if (type < 0 || type >= VECTOR_ENGINE_MAX) {
        serverLog(LL_WARNING, "Invalid vector engine type %d", type);
        return C_ERR;
    }

    vector_engine_t *engine = engine_registry[type];
    if (!engine) {
        serverLog(LL_WARNING, "Vector engine type %d not registered", type);
        return C_ERR;
    }

    /* If switching to the same engine, nothing to do */
    if (current_engine == engine) {
        return C_OK;
    }

    /* Cleanup old engine */
    if (current_engine) {
        current_engine->cleanup();
    }

    /* Init and activate new engine */
    if (engine->init() != C_OK) {
        serverLog(LL_WARNING, "Vector engine type %d init failed during switch", type);
        return C_ERR;
    }

    current_engine = engine;
    serverLog(LL_NOTICE, "Vector engine switched to: %s",
              type == VECTOR_ENGINE_UB ? "UB" : "Redis");
    return C_OK;
}

/* ============================================================
 * Utility Functions
 * ============================================================ */

vector_data_t *vector_data_create(float *data, size_t dim, int is_fp32) {
    vector_data_t *vd = zmalloc(sizeof(vector_data_t));
    RETURN_IF(!vd, NULL);

    vd->data = data;
    vd->dim = dim;
    vd->is_fp32 = is_fp32;

    return vd;
}

void vector_data_destroy(vector_data_t *vd) {
    if (vd) {
        if (vd->data) zfree(vd->data);
        zfree(vd);
    }
}

vector_query_result_t *vector_query_result_create(size_t count) {
    return zmalloc(sizeof(vector_query_result_t) * count);
}

void vector_query_result_destroy(vector_query_result_t *results, size_t count) {
    if (results) {
        for (size_t i = 0; i < count; i++) {
            if (results[i].element) sdsfree(results[i].element);
            if (results[i].attributes) sdsfree(results[i].attributes);
        }
        zfree(results);
    }
}
