#include "vector_proxy_completion.h"

#include "dict.h"
#include "macro.h"
#include "monotonic.h"
#include "sds.h"
#include "zmalloc.h"

#include <inttypes.h>
#include <pthread.h>
#include <string.h>

#ifndef C_OK
#define C_OK 0
#endif
#ifndef C_ERR
#define C_ERR 1
#endif

typedef struct vector_proxy_completion_registry {
    dict *requests;
    pthread_mutex_t lock;
    int initialized;
} vector_proxy_completion_registry_t;

static vector_proxy_completion_registry_t g_completion = {0};

static uint64_t completion_dict_hash(const void *key) {
    return dictGenHashFunction(key, sdslen((const sds)key));
}

static int completion_dict_compare(dictCmpCache *cache, const void *key1, const void *key2) {
    (void)cache;
    return sdscmp((const sds)key1, (const sds)key2) == 0;
}

static void completion_dict_key_destructor(dict *d, void *key) {
    (void)d;
    sdsfree(key);
}

static dictType completion_dict_type = {
    .hashFunction = completion_dict_hash,
    .keyDup = NULL,
    .valDup = NULL,
    .keyCompare = completion_dict_compare,
    .keyDestructor = completion_dict_key_destructor,
    .valDestructor = NULL,
    .resizeAllowed = NULL,
    .rehashingStarted = NULL,
    .rehashingCompleted = NULL,
    .bucketChanged = NULL,
    .dictMetadataBytes = NULL,
    .userdata = NULL,
};

static sds completion_request_key(uint64_t request_id) {
    return sdscatprintf(sdsempty(), "%" PRIu64, request_id);
}

int vector_proxy_completion_init(void) {
    if (g_completion.initialized) return C_OK;

    g_completion.requests = dictCreate(&completion_dict_type);
    if (!g_completion.requests) return C_ERR;
    if (pthread_mutex_init(&g_completion.lock, NULL) != 0) {
        dictRelease(g_completion.requests);
        g_completion.requests = NULL;
        return C_ERR;
    }
    g_completion.initialized = 1;
    return C_OK;
}

void vector_proxy_completion_cleanup(void) {
    if (!g_completion.initialized) return;

    dictRelease(g_completion.requests);
    pthread_mutex_destroy(&g_completion.lock);
    memset(&g_completion, 0, sizeof(g_completion));
}

int vector_proxy_completion_register(proxy_vector_request_t *req) {
    RETURN_IF(!req, C_ERR);
    if (!g_completion.initialized && vector_proxy_completion_init() != C_OK) return C_ERR;

    sds key = completion_request_key(req->request_id);
    RETURN_IF(!key, C_ERR);

    pthread_mutex_lock(&g_completion.lock);
    int rc = dictAdd(g_completion.requests, key, req) == DICT_OK ? C_OK : C_ERR;
    pthread_mutex_unlock(&g_completion.lock);
    if (rc != C_OK) sdsfree(key);
    return rc;
}

proxy_vector_request_t *vector_proxy_completion_lookup(uint64_t request_id) {
    proxy_vector_request_t *req = NULL;
    RETURN_IF(!g_completion.initialized, NULL);

    sds key = completion_request_key(request_id);
    RETURN_IF(!key, NULL);

    pthread_mutex_lock(&g_completion.lock);
    dictEntry *de = dictFind(g_completion.requests, key);
    if (de) req = dictGetVal(de);
    pthread_mutex_unlock(&g_completion.lock);
    sdsfree(key);
    return req;
}

int vector_proxy_completion_complete_vemb(uint64_t request_id,
                                          const float *vector,
                                          size_t dim,
                                          int error_code) {
    RETURN_IF(!g_completion.initialized, C_ERR);

    sds key = completion_request_key(request_id);
    RETURN_IF(!key, C_ERR);

    pthread_mutex_lock(&g_completion.lock);
    dictEntry *de = dictFind(g_completion.requests, key);
    if (!de) {
        pthread_mutex_unlock(&g_completion.lock);
        sdsfree(key);
        return C_ERR;
    }

    proxy_vector_request_t *req = dictGetVal(de);
    req->error_code = error_code;
    req->completion_time_us = getMonotonicUs();

    if (error_code == C_OK && vector && dim > 0) {
        req->result_vector = zmalloc(sizeof(float) * dim);
        if (!req->result_vector) {
            req->error_code = C_ERR;
        } else {
            memcpy(req->result_vector, vector, sizeof(float) * dim);
            req->result_dim = dim;
        }
    }

    pthread_mutex_unlock(&g_completion.lock);
    sdsfree(key);
    return req->error_code == C_OK ? C_OK : C_ERR;
}

int vector_proxy_completion_complete_vsim(uint64_t request_id,
                                          const uint64_t *row_ids,
                                          const float *scores,
                                          size_t num_results,
                                          int error_code) {
    RETURN_IF(!g_completion.initialized, C_ERR);

    sds key = completion_request_key(request_id);
    RETURN_IF(!key, C_ERR);

    pthread_mutex_lock(&g_completion.lock);
    dictEntry *de = dictFind(g_completion.requests, key);
    if (!de) {
        pthread_mutex_unlock(&g_completion.lock);
        sdsfree(key);
        return C_ERR;
    }

    proxy_vector_request_t *req = dictGetVal(de);
    req->error_code = error_code;
    req->completion_time_us = getMonotonicUs();

    if (error_code == C_OK && num_results > 0 && row_ids && scores) {
        req->candidate_rows = zmalloc(sizeof(uint64_t) * num_results);
        req->result_scores = zmalloc(sizeof(float) * num_results);
        if (!req->candidate_rows || !req->result_scores) {
            zfree(req->candidate_rows);
            zfree(req->result_scores);
            req->candidate_rows = NULL;
            req->result_scores = NULL;
            req->error_code = C_ERR;
        } else {
            memcpy(req->candidate_rows, row_ids, sizeof(uint64_t) * num_results);
            memcpy(req->result_scores, scores, sizeof(float) * num_results);
            req->result_count = num_results;
        }
    } else {
        req->result_count = 0;
    }

    pthread_mutex_unlock(&g_completion.lock);
    sdsfree(key);
    return req->error_code == C_OK ? C_OK : C_ERR;
}

int vector_proxy_completion_take(uint64_t request_id, proxy_vector_request_t **req) {
    RETURN_IF(!g_completion.initialized || !req, C_ERR);
    *req = NULL;

    sds key = completion_request_key(request_id);
    RETURN_IF(!key, C_ERR);

    pthread_mutex_lock(&g_completion.lock);
    dictEntry *de = dictFind(g_completion.requests, key);
    if (!de) {
        pthread_mutex_unlock(&g_completion.lock);
        sdsfree(key);
        return C_ERR;
    }

    *req = dictGetVal(de);
    dictDelete(g_completion.requests, key);
    pthread_mutex_unlock(&g_completion.lock);
    sdsfree(key);
    return C_OK;
}
