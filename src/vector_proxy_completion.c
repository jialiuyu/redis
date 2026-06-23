#include "vector_proxy_completion.h"

#include "config.h"
#include "dict.h"
#include "macro.h"
#include "monotonic.h"
#include "zmalloc.h"

#include <pthread.h>
#include <string.h>

#ifndef C_OK
#define C_OK 0
#endif
#ifndef C_ERR
#define C_ERR 1
#endif

#define VECTOR_PROXY_COMPLETION_SHARDS 64
#define VECTOR_PROXY_COMPLETION_SHARD_MASK (VECTOR_PROXY_COMPLETION_SHARDS - 1)

typedef struct __attribute__((aligned(CACHE_LINE_SIZE))) vector_proxy_completion_shard {
    dict *requests;
    pthread_mutex_t lock;
    int lock_initialized;
} vector_proxy_completion_shard_t;

typedef struct vector_proxy_completion_registry {
    vector_proxy_completion_shard_t shards[VECTOR_PROXY_COMPLETION_SHARDS];
    int initialized;
} vector_proxy_completion_registry_t;

static vector_proxy_completion_registry_t g_completion = {0};
static pthread_mutex_t g_completion_init_lock = PTHREAD_MUTEX_INITIALIZER;

static inline uint64_t completion_mix_u64(uint64_t x) {
    x ^= x >> 33;
    x *= UINT64_C(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x *= UINT64_C(0xc4ceb9fe1a85ec53);
    x ^= x >> 33;
    return x;
}

static uint64_t completion_dict_hash(const void *key) {
    return completion_mix_u64(*(const uint64_t *)key);
}

static int completion_dict_compare(dictCmpCache *cache, const void *key1, const void *key2) {
    (void)cache;
    return *(const uint64_t *)key1 == *(const uint64_t *)key2;
}

static void completion_dict_key_destructor(dict *d, void *key) {
    (void)d;
    zfree(key);
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

static inline vector_proxy_completion_shard_t *completion_shard_for(uint64_t request_id) {
    return &g_completion.shards[completion_mix_u64(request_id) & VECTOR_PROXY_COMPLETION_SHARD_MASK];
}

static uint64_t *completion_request_key_create(uint64_t request_id) {
    uint64_t *key = zmalloc(sizeof(*key));
    if (key) *key = request_id;
    return key;
}

static void completion_release_shards(size_t count) {
    for (size_t i = 0; i < count; i++) {
        vector_proxy_completion_shard_t *shard = &g_completion.shards[i];
        if (shard->requests) {
            dictRelease(shard->requests);
            shard->requests = NULL;
        }
        if (shard->lock_initialized) {
            pthread_mutex_destroy(&shard->lock);
            shard->lock_initialized = 0;
        }
    }
}

int vector_proxy_completion_init(void) {
    if (g_completion.initialized) return C_OK;

    pthread_mutex_lock(&g_completion_init_lock);
    if (g_completion.initialized) {
        pthread_mutex_unlock(&g_completion_init_lock);
        return C_OK;
    }

    memset(&g_completion, 0, sizeof(g_completion));
    for (size_t i = 0; i < VECTOR_PROXY_COMPLETION_SHARDS; i++) {
        vector_proxy_completion_shard_t *shard = &g_completion.shards[i];

        shard->requests = dictCreate(&completion_dict_type);
        if (!shard->requests) {
            completion_release_shards(i);
            memset(&g_completion, 0, sizeof(g_completion));
            pthread_mutex_unlock(&g_completion_init_lock);
            return C_ERR;
        }
        if (pthread_mutex_init(&shard->lock, NULL) != 0) {
            dictRelease(shard->requests);
            shard->requests = NULL;
            completion_release_shards(i);
            memset(&g_completion, 0, sizeof(g_completion));
            pthread_mutex_unlock(&g_completion_init_lock);
            return C_ERR;
        }
        shard->lock_initialized = 1;
    }

    g_completion.initialized = 1;
    pthread_mutex_unlock(&g_completion_init_lock);
    return C_OK;
}

void vector_proxy_completion_cleanup(void) {
    pthread_mutex_lock(&g_completion_init_lock);
    if (!g_completion.initialized) {
        pthread_mutex_unlock(&g_completion_init_lock);
        return;
    }

    g_completion.initialized = 0;
    completion_release_shards(VECTOR_PROXY_COMPLETION_SHARDS);
    memset(&g_completion, 0, sizeof(g_completion));
    pthread_mutex_unlock(&g_completion_init_lock);
}

int vector_proxy_completion_register(proxy_vector_request_t *req) {
    RETURN_IF(!req, C_ERR);
    if (!g_completion.initialized && vector_proxy_completion_init() != C_OK) return C_ERR;

    uint64_t *key = completion_request_key_create(req->request_id);
    RETURN_IF(!key, C_ERR);

    vector_proxy_completion_shard_t *shard = completion_shard_for(req->request_id);
    pthread_mutex_lock(&shard->lock);
    int rc = dictAdd(shard->requests, key, req) == DICT_OK ? C_OK : C_ERR;
    pthread_mutex_unlock(&shard->lock);
    if (rc != C_OK) zfree(key);
    return rc;
}

proxy_vector_request_t *vector_proxy_completion_lookup(uint64_t request_id) {
    proxy_vector_request_t *req = NULL;
    RETURN_IF(!g_completion.initialized, NULL);

    uint64_t key = request_id;
    vector_proxy_completion_shard_t *shard = completion_shard_for(request_id);
    pthread_mutex_lock(&shard->lock);
    dictEntry *de = dictFind(shard->requests, &key);
    if (de) req = dictGetVal(de);
    pthread_mutex_unlock(&shard->lock);
    return req;
}

int vector_proxy_completion_complete_vemb(uint64_t request_id,
                                          const float *vector,
                                          size_t dim,
                                          int error_code) {
    RETURN_IF(!g_completion.initialized, C_ERR);

    float *result_vector = NULL;
    if (error_code == C_OK && vector && dim > 0) {
        result_vector = zmalloc(sizeof(float) * dim);
        if (!result_vector) {
            error_code = C_ERR;
        } else {
            memcpy(result_vector, vector, sizeof(float) * dim);
        }
    }

    uint64_t key = request_id;
    monotime completion_time_us = getMonotonicUs();
    vector_proxy_completion_shard_t *shard = completion_shard_for(request_id);

    pthread_mutex_lock(&shard->lock);
    dictEntry *de = dictFind(shard->requests, &key);
    if (!de) {
        pthread_mutex_unlock(&shard->lock);
        zfree(result_vector);
        return C_ERR;
    }

    proxy_vector_request_t *req = dictGetVal(de);
    zfree(req->result_vector);
    req->result_vector = NULL;
    req->result_dim = 0;

    req->error_code = error_code;
    req->completion_time_us = completion_time_us;

    if (error_code == C_OK && result_vector) {
        req->result_vector = result_vector;
        req->result_dim = dim;
        result_vector = NULL;
    }

    pthread_mutex_unlock(&shard->lock);
    zfree(result_vector);
    return req->error_code == C_OK ? C_OK : C_ERR;
}

int vector_proxy_completion_complete_vsim(uint64_t request_id,
                                          const uint64_t *row_ids,
                                          const float *scores,
                                          size_t num_results,
                                          int error_code) {
    RETURN_IF(!g_completion.initialized, C_ERR);

    uint64_t *result_rows = NULL;
    float *result_scores = NULL;
    size_t result_count = 0;
    if (error_code == C_OK && num_results > 0 && row_ids && scores) {
        result_rows = zmalloc(sizeof(uint64_t) * num_results);
        result_scores = zmalloc(sizeof(float) * num_results);
        if (!result_rows || !result_scores) {
            zfree(result_rows);
            zfree(result_scores);
            result_rows = NULL;
            result_scores = NULL;
            error_code = C_ERR;
        } else {
            memcpy(result_rows, row_ids, sizeof(uint64_t) * num_results);
            memcpy(result_scores, scores, sizeof(float) * num_results);
            result_count = num_results;
        }
    }

    uint64_t key = request_id;
    monotime completion_time_us = getMonotonicUs();
    vector_proxy_completion_shard_t *shard = completion_shard_for(request_id);

    pthread_mutex_lock(&shard->lock);
    dictEntry *de = dictFind(shard->requests, &key);
    if (!de) {
        pthread_mutex_unlock(&shard->lock);
        zfree(result_rows);
        zfree(result_scores);
        return C_ERR;
    }

    proxy_vector_request_t *req = dictGetVal(de);
    zfree(req->result_rows);
    zfree(req->result_scores);
    req->result_rows = NULL;
    req->result_scores = NULL;
    req->result_count = 0;

    req->error_code = error_code;
    req->completion_time_us = completion_time_us;

    if (error_code == C_OK && result_rows && result_scores) {
        req->result_rows = result_rows;
        req->result_scores = result_scores;
        req->result_count = result_count;
        result_rows = NULL;
        result_scores = NULL;
    }

    pthread_mutex_unlock(&shard->lock);
    zfree(result_rows);
    zfree(result_scores);
    return req->error_code == C_OK ? C_OK : C_ERR;
}

int vector_proxy_completion_take(uint64_t request_id, proxy_vector_request_t **req) {
    RETURN_IF(!g_completion.initialized || !req, C_ERR);
    *req = NULL;

    uint64_t key = request_id;
    vector_proxy_completion_shard_t *shard = completion_shard_for(request_id);

    pthread_mutex_lock(&shard->lock);
    dictEntry *de = dictUnlink(shard->requests, &key);
    if (!de) {
        pthread_mutex_unlock(&shard->lock);
        return C_ERR;
    }

    *req = dictGetVal(de);
    dictFreeUnlinkedEntry(shard->requests, de);
    pthread_mutex_unlock(&shard->lock);
    return C_OK;
}
