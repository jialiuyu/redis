#include "../src/vemb_v16_tlc.h"

#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void fill_vector(float *vector, uint32_t dim, uint32_t seed) {
    for (uint32_t i = 0; i < dim; i++)
        vector[i] = (float)(seed + i);
}

static void test_put_get_handle(void) {
    enum { dim = 4, max_vectors = 8 };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 7,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
    };
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = 0;
    const char *key = "item:1";
    uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));

    memset(region, 0, sizeof(region));
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm) == 0);
    fill_vector(vector, dim, 10);
    assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                            vector, sizeof(vector), &handle, &warm_slot) == 0);
    assert(handle.region_id == 7);
    assert(handle.bytes == sizeof(vector));
    assert(handle.offset == 0);
    assert(warm_slot == 0);
    assert(memcmp(region, vector, sizeof(vector)) == 0);

    memset(&handle, 0, sizeof(handle));
    warm_slot = 99;
    assert(vemb_v16_tlc_get_handle(tlc, key, (uint32_t)strlen(key), key_hash,
                                   &handle, &warm_slot) == 0);
    assert(handle.region_id == 7);
    assert(handle.offset == 0);
    assert(warm_slot == 0);
    vemb_v16_tlc_destroy(tlc);
}

static void test_overwrite_and_capacity(void) {
    enum { dim = 2, max_vectors = 1 };
    float region[dim * max_vectors];
    float first[dim], second[dim];
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 0,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
    };
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = 0;
    const char *key = "only";
    const char *missing = "extra";
    uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));
    uint64_t missing_hash = vemb_v16_murmur3(missing, strlen(missing));

    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm) == 0);
    fill_vector(first, dim, 1);
    fill_vector(second, dim, 100);
    assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                            first, sizeof(first), &handle, &warm_slot) == 0);
    assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                            second, sizeof(second), &handle, &warm_slot) == 0);
    assert(warm_slot == 0);
    assert(memcmp(region, second, sizeof(second)) == 0);
    memset(&handle, 0xff, sizeof(handle));
    warm_slot = 0;
    assert(vemb_v16_tlc_put(tlc, missing, (uint32_t)strlen(missing), missing_hash,
                            first, sizeof(first), &handle, &warm_slot) == 0);
    assert(warm_slot == UINT32_MAX);
    assert(handle.bytes == 0);
    assert(vemb_v16_tlc_get_handle(tlc, missing, (uint32_t)strlen(missing),
                                   missing_hash, &handle, &warm_slot) != 0);
    vemb_v16_tlc_destroy(tlc);
}

static void test_cold_read_through_promotes_warm_handle(void) {
    enum { dim = 3, max_vectors = 4 };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 42,
        .backend_type = VEMB_V16_REGION_UB,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
    };
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = 99;
    const char *key = "cold-key";
    uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));

    memset(region, 0, sizeof(region));
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm) == 0);
    fill_vector(vector, dim, 200);
    assert(vemb_v16_tlc_cold_append(tlc, key, (uint32_t)strlen(key), key_hash,
                                    vector, sizeof(vector)) == 0);
    assert(vemb_v16_tlc_get_handle(tlc, key, (uint32_t)strlen(key), key_hash,
                                   &handle, &warm_slot) == 0);
    assert(handle.region_id == 42);
    assert(handle.bytes == sizeof(vector));
    assert(handle.offset == 0);
    assert(warm_slot == 0);
    assert(memcmp(region, vector, sizeof(vector)) == 0);
    vemb_v16_tlc_destroy(tlc);
}

static void test_hot_is_cache_only(void) {
    enum { dim = 2, max_vectors = 16 };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 5,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
    };
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = 0;
    char key[32];

    memset(region, 0, sizeof(region));
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm) == 0);
    for (uint32_t i = 0; i < max_vectors; i++) {
        snprintf(key, sizeof(key), "hot-cache:%u", i);
        fill_vector(vector, dim, i + 1);
        uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));
        assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                                vector, sizeof(vector),
                                &handle, &warm_slot) == 0);
    }
    for (uint32_t i = 0; i < max_vectors; i++) {
        snprintf(key, sizeof(key), "hot-cache:%u", i);
        uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));
        memset(&handle, 0, sizeof(handle));
        warm_slot = UINT32_MAX;
        assert(vemb_v16_tlc_get_handle(tlc, key, (uint32_t)strlen(key),
                                       key_hash, &handle, &warm_slot) == 0);
        assert(handle.region_id == 5);
        assert(handle.offset == (uint64_t)warm_slot * sizeof(vector));
        assert(warm_slot < max_vectors);
    }
    vemb_v16_tlc_destroy(tlc);
}

typedef struct concurrent_arg {
    vemb_v16_tlc_t *tlc;
    int tid;
    int iterations;
    uint32_t dim;
} concurrent_arg_t;

static void *concurrent_worker(void *arg) {
    concurrent_arg_t *a = arg;
    float vector[4];
    char key[32];
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = 0;

    assert(a->dim == 4);
    for (int i = 0; i < a->iterations; i++) {
        uint32_t id = (uint32_t)(a->tid * a->iterations + i);
        snprintf(key, sizeof(key), "k:%u", id);
        fill_vector(vector, a->dim, id);
        uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));
        assert(vemb_v16_tlc_put(a->tlc, key, (uint32_t)strlen(key), key_hash,
                                vector, sizeof(vector),
                                &handle, &warm_slot) == 0);
        assert(handle.region_id == 11);
        assert(handle.bytes == sizeof(vector));
        assert(handle.offset + handle.bytes <=
               (uint64_t)4 * (uint64_t)128 * sizeof(float));
        memset(&handle, 0xff, sizeof(handle));
        warm_slot = UINT32_MAX;
        assert(vemb_v16_tlc_get_handle(a->tlc, key, (uint32_t)strlen(key),
                                       key_hash, &handle, &warm_slot) == 0);
        assert(handle.region_id == 11);
        assert(warm_slot < 128);
    }
    return NULL;
}

static void test_concurrent_distinct_keys(void) {
    enum { dim = 4, max_vectors = 128, threads = 4, iterations = 24 };
    float region[dim * max_vectors];
    vemb_v16_tlc_t *tlc = NULL;
    pthread_t tids[threads];
    concurrent_arg_t args[threads];
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 11,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
    };

    memset(region, 0, sizeof(region));
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm) == 0);
    for (int i = 0; i < threads; i++) {
        args[i] = (concurrent_arg_t){
            .tlc = tlc,
            .tid = i,
            .iterations = iterations,
            .dim = dim,
        };
        assert(pthread_create(&tids[i], NULL, concurrent_worker, &args[i]) == 0);
    }
    for (int i = 0; i < threads; i++)
        assert(pthread_join(tids[i], NULL) == 0);
    vemb_v16_tlc_destroy(tlc);
}

int main(void) {
    test_put_get_handle();
    test_overwrite_and_capacity();
    test_cold_read_through_promotes_warm_handle();
    test_hot_is_cache_only();
    test_concurrent_distinct_keys();
    printf("vemb_v16_tlc_ut: all tests passed\n");
    return 0;
}
