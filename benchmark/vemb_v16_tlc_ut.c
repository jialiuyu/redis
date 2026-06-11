#include "../src/vemb_v16_tlc.h"
#include "../src/vemb_v16_remote_meta.h"

#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void fill_vector(float *vector, uint32_t dim, uint32_t seed) {
    for (uint32_t i = 0; i < dim; i++)
        vector[i] = (float)(seed + i);
}

static void make_key(char *buf, size_t len, uint32_t id) {
    snprintf(buf, len, "item:%u", id);
}

static void init_test_allocator(vemb_v16_shared_region_allocator_t *allocator,
                                uint32_t region_id,
                                uint32_t capacity_slots) {
    memset(allocator, 0, sizeof(*allocator));
    atomic_init(&allocator->magic, VEMB_V16_SHARED_ALLOCATOR_MAGIC);
    allocator->version = VEMB_V16_SHARED_ALLOCATOR_VERSION;
    allocator->region_id = region_id;
    allocator->capacity_slots = capacity_slots;
    atomic_init(&allocator->next_slot, 0);
    atomic_init(&allocator->full, 0);
    atomic_init(&allocator->used_slots, 0);
}

static void test_put_get_handle(void) {
    enum { dim = 4, max_vectors = 8 };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_shared_region_allocator_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 7,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .shared_allocator = &allocator,
    };
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = 0;
    const char *key = "item:1";
    uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 7, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);
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
    vemb_v16_shared_region_allocator_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 0,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .shared_allocator = &allocator,
    };
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = 0;
    const char *key = "only";
    const char *missing = "extra";
    uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));
    uint64_t missing_hash = vemb_v16_murmur3(missing, strlen(missing));

    init_test_allocator(&allocator, 0, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);
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
    vemb_v16_shared_region_allocator_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 42,
        .backend_type = VEMB_V16_REGION_UB,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .shared_allocator = &allocator,
    };
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = 99;
    const char *key = "cold-key";
    uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 42, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);
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
    vemb_v16_shared_region_allocator_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 5,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .shared_allocator = &allocator,
    };
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = 0;
    char key[32];

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 5, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);
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

static void test_prefill_distribution_stays_warm(void) {
    enum { dim = 1, max_vectors = 131072, prefill = 65536 };
    float *region = calloc((size_t)dim * max_vectors, sizeof(*region));
    float vector[dim];
    vemb_v16_shared_region_allocator_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 19,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .mapped_addr = region,
        .region_bytes = (uint64_t)sizeof(*region) * dim * max_vectors,
        .value_size = dim * sizeof(float),
        .shared_allocator = &allocator,
    };
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    uint32_t cold_only_writes = 0;
    char key[32];

    assert(region);
    init_test_allocator(&allocator, 19, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);
    for (uint32_t i = 0; i < prefill; i++) {
        make_key(key, sizeof(key), i);
        fill_vector(vector, dim, i);
        uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));
        memset(&handle, 0, sizeof(handle));
        warm_slot = UINT32_MAX;
        assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                                vector, sizeof(vector),
                                &handle, &warm_slot) == 0);
        if (warm_slot == UINT32_MAX)
            cold_only_writes++;
    }
    assert(cold_only_writes == 0);

    for (uint32_t i = 0; i < prefill; i++) {
        make_key(key, sizeof(key), i);
        uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));
        memset(&handle, 0, sizeof(handle));
        warm_slot = UINT32_MAX;
        assert(vemb_v16_tlc_get_handle(tlc, key, (uint32_t)strlen(key),
                                       key_hash, &handle, &warm_slot) == 0);
        assert(handle.region_id == 19);
        assert(handle.bytes == sizeof(vector));
        assert(warm_slot < max_vectors);
    }
    vemb_v16_tlc_destroy(tlc);
    free(region);
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
    vemb_v16_shared_region_allocator_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    pthread_t tids[threads];
    concurrent_arg_t args[threads];
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 11,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .shared_allocator = &allocator,
    };

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 11, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);
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

static uint32_t collect_keys_for_region(uint32_t wanted_region_id,
                                        char keys[][32],
                                        uint32_t needed) {
    enum { dim = 2, max_vectors = 64 };
    float region1[dim * max_vectors];
    float region2[dim * max_vectors];
    float vector[dim];
    vemb_v16_shared_region_allocator_t alloc1, alloc2;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t regions[] = {
        {
            .region_id = 1,
            .backend_type = VEMB_V16_REGION_UB,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = region1,
            .region_bytes = sizeof(region1),
            .value_size = dim * sizeof(float),
            .shared_allocator = &alloc1,
        },
        {
            .region_id = 2,
            .backend_type = VEMB_V16_REGION_UB,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = region2,
            .region_bytes = sizeof(region2),
            .value_size = dim * sizeof(float),
            .shared_allocator = &alloc2,
        },
    };
    uint32_t found = 0;

    memset(region1, 0, sizeof(region1));
    memset(region2, 0, sizeof(region2));
    init_test_allocator(&alloc1, 1, max_vectors);
    init_test_allocator(&alloc2, 2, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors,
                                    regions, 2, 16) == 0);
    fill_vector(vector, dim, 500);
    for (uint32_t i = 0; i < 10000 && found < needed; i++) {
        char key[32];
        vemb_v16_vector_handle_t handle = {0};
        uint32_t warm_slot = UINT32_MAX;
        snprintf(key, sizeof(key), "region-probe:%u", i);
        uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));
        assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                                vector, sizeof(vector),
                                &handle, &warm_slot) == 0);
        if (handle.region_id == wanted_region_id) {
            snprintf(keys[found], 32, "%s", key);
            found++;
        }
    }
    vemb_v16_tlc_destroy(tlc);
    return found;
}

static void test_multi_region_local_full_fallback_and_overwrite(void) {
    enum { dim = 2 };
    float local_region[dim * 1];
    float remote_region[dim * 4];
    float first[dim], second[dim], overwrite[dim];
    char local_keys[2][32];
    vemb_v16_shared_region_allocator_t local_alloc, remote_alloc;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t regions[] = {
        {
            .region_id = 1,
            .backend_type = VEMB_V16_REGION_UB,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = local_region,
            .region_bytes = sizeof(local_region),
            .value_size = dim * sizeof(float),
            .shared_allocator = &local_alloc,
        },
        {
            .region_id = 2,
            .backend_type = VEMB_V16_REGION_UB,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = remote_region,
            .region_bytes = sizeof(remote_region),
            .value_size = dim * sizeof(float),
            .shared_allocator = &remote_alloc,
        },
    };
    vemb_v16_vector_handle_t h1 = {0}, h2 = {0}, h3 = {0};
    uint32_t warm_slot = UINT32_MAX;

    assert(collect_keys_for_region(1, local_keys, 2) == 2);
    memset(local_region, 0, sizeof(local_region));
    memset(remote_region, 0, sizeof(remote_region));
    init_test_allocator(&local_alloc, 1, 1);
    init_test_allocator(&remote_alloc, 2, 4);
    assert(vemb_v16_tlc_create(&tlc, dim, 5, regions, 2, 16) == 0);

    fill_vector(first, dim, 10);
    fill_vector(second, dim, 20);
    fill_vector(overwrite, dim, 30);
    uint64_t h1_hash = vemb_v16_murmur3(local_keys[0], strlen(local_keys[0]));
    uint64_t h2_hash = vemb_v16_murmur3(local_keys[1], strlen(local_keys[1]));

    assert(vemb_v16_tlc_put(tlc, local_keys[0], (uint32_t)strlen(local_keys[0]),
                            h1_hash, first, sizeof(first),
                            &h1, &warm_slot) == 0);
    assert(h1.region_id == 1);
    assert(h1.offset == 0);
    assert(memcmp(local_region, first, sizeof(first)) == 0);

    assert(vemb_v16_tlc_put(tlc, local_keys[1], (uint32_t)strlen(local_keys[1]),
                            h2_hash, second, sizeof(second),
                            &h2, &warm_slot) == 0);
    assert(h2.region_id == 2);
    assert(h2.offset == 0);
    assert(memcmp(remote_region, second, sizeof(second)) == 0);

    assert(vemb_v16_tlc_put(tlc, local_keys[0], (uint32_t)strlen(local_keys[0]),
                            h1_hash, overwrite, sizeof(overwrite),
                            &h3, &warm_slot) == 0);
    assert(h3.region_id == h1.region_id);
    assert(h3.offset == h1.offset);
    assert(memcmp(local_region, overwrite, sizeof(overwrite)) == 0);
    tlc_core_stats_t stats;
    vemb_v16_tlc_get_core_stats(tlc, &stats);
    assert(stats.warm_region_count == 2);
    assert(stats.warm_alloc_local >= 1);
    assert(stats.warm_alloc_remote >= 1);
    assert(stats.warm_alloc_fallback >= 1);
    assert(stats.warm_region_full_count >= 1);
    vemb_v16_tlc_destroy(tlc);
}

static void test_multi_region_all_full_spills_cold(void) {
    enum { dim = 2, max_vectors = 2 };
    float region1[dim];
    float region2[dim];
    float vector[dim];
    vemb_v16_shared_region_allocator_t alloc1, alloc2;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t regions[] = {
        {
            .region_id = 10,
            .backend_type = VEMB_V16_REGION_UB,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = region1,
            .region_bytes = sizeof(region1),
            .value_size = dim * sizeof(float),
            .shared_allocator = &alloc1,
        },
        {
            .region_id = 20,
            .backend_type = VEMB_V16_REGION_UB,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = region2,
            .region_bytes = sizeof(region2),
            .value_size = dim * sizeof(float),
            .shared_allocator = &alloc2,
        },
    };
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = 0;
    char key[32];

    init_test_allocator(&alloc1, 10, 1);
    init_test_allocator(&alloc2, 20, 1);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors,
                                    regions, 2, 8) == 0);
    fill_vector(vector, dim, 700);
    for (uint32_t i = 0; i < max_vectors; i++) {
        snprintf(key, sizeof(key), "full:%u", i);
        uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));
        assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                                vector, sizeof(vector),
                                &handle, &warm_slot) == 0);
        assert(handle.bytes == sizeof(vector));
    }

    memset(&handle, 0xff, sizeof(handle));
    snprintf(key, sizeof(key), "full:%u", max_vectors);
    uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));
    assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                            vector, sizeof(vector),
                            &handle, &warm_slot) == 0);
    assert(warm_slot == UINT32_MAX);
    assert(handle.bytes == 0);
    tlc_core_stats_t stats;
    vemb_v16_tlc_get_core_stats(tlc, &stats);
    assert(stats.warm_region_count == 2);
    assert(stats.warm_region_full_count == 2);
    assert(stats.warm_alloc_cold_spill == 1);
    assert(stats.warm_alloc_fail >= 1);
    vemb_v16_tlc_destroy(tlc);
}

static void test_shared_allocator_two_tlcs_unique_slots(void) {
    enum { dim = 2, max_vectors = 4 };
    float region[dim * max_vectors];
    float v1[dim], v2[dim], overwrite[dim];
    vemb_v16_shared_region_allocator_t allocator;
    vemb_v16_tlc_t *tlc1 = NULL, *tlc2 = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 77,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .shared_allocator = &allocator,
    };
    vemb_v16_vector_handle_t h1 = {0}, h2 = {0}, h3 = {0};
    uint32_t warm_slot = UINT32_MAX;
    const char *key1 = "shared:one";
    const char *key2 = "shared:two";
    uint64_t key1_hash = vemb_v16_murmur3(key1, strlen(key1));
    uint64_t key2_hash = vemb_v16_murmur3(key2, strlen(key2));

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 77, max_vectors);
    assert(vemb_v16_tlc_create(&tlc1, dim, max_vectors, &warm, 1, 4) == 0);
    assert(vemb_v16_tlc_create(&tlc2, dim, max_vectors, &warm, 1, 4) == 0);

    fill_vector(v1, dim, 1000);
    fill_vector(v2, dim, 2000);
    fill_vector(overwrite, dim, 3000);
    assert(vemb_v16_tlc_put(tlc1, key1, (uint32_t)strlen(key1), key1_hash,
                            v1, sizeof(v1), &h1, &warm_slot) == 0);
    assert(h1.region_id == 77);
    assert(h1.offset == 0);
    assert(warm_slot == 0);
    assert(vemb_v16_tlc_put(tlc2, key2, (uint32_t)strlen(key2), key2_hash,
                            v2, sizeof(v2), &h2, &warm_slot) == 0);
    assert(h2.region_id == 77);
    assert(h2.offset == sizeof(v2));
    assert(warm_slot == 1);
    assert(vemb_v16_shared_allocator_used_slots(&allocator) == 2);

    assert(vemb_v16_tlc_put(tlc1, key1, (uint32_t)strlen(key1), key1_hash,
                            overwrite, sizeof(overwrite), &h3,
                            &warm_slot) == 0);
    assert(h3.region_id == h1.region_id);
    assert(h3.offset == h1.offset);
    assert(warm_slot == 0);
    assert(vemb_v16_shared_allocator_used_slots(&allocator) == 2);
    assert(memcmp(region, overwrite, sizeof(overwrite)) == 0);
    assert(memcmp(region + dim, v2, sizeof(v2)) == 0);

    vemb_v16_tlc_destroy(tlc2);
    vemb_v16_tlc_destroy(tlc1);
}

static void test_vsim_key2_lookup_local_source(void) {
    enum { dim = 2, max_vectors = 4 };
    float region[dim * max_vectors];
    float v1[dim], v2[dim];
    vemb_v16_shared_region_allocator_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 88,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .shared_allocator = &allocator,
    };
    vemb_v16_vector_handle_t handle = {0};
    vemb_v16_vector_handle_t key2_handle = {0};
    vemb_v16_tlc_lookup_source_t source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    vemb_v16_tlc_lookup_timing_t timing = {0};
    uint32_t warm_slot = UINT32_MAX;
    const char *key1 = "vsim:one";
    const char *key2 = "vsim:two";
    const char *missing = "vsim:missing";
    uint64_t key1_hash = vemb_v16_murmur3(key1, strlen(key1));
    uint64_t key2_hash = vemb_v16_murmur3(key2, strlen(key2));
    uint64_t missing_hash = vemb_v16_murmur3(missing, strlen(missing));

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 88, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);

    fill_vector(v1, dim, 100);
    fill_vector(v2, dim, 200);
    assert(vemb_v16_tlc_put(tlc, key1, (uint32_t)strlen(key1), key1_hash,
                            v1, sizeof(v1), &handle, &warm_slot) == 0);
    assert(vemb_v16_tlc_put(tlc, key2, (uint32_t)strlen(key2), key2_hash,
                            v2, sizeof(v2), &handle, &warm_slot) == 0);

    assert(vemb_v16_tlc_lookup_vsim_key2(tlc,
                                         key2,
                                         (uint32_t)strlen(key2),
                                         key2_hash,
                                         &key2_handle,
                                         &source,
                                         &timing) == 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_LOCAL);
    assert(timing.local_lookup_count == 1);
    assert(timing.remote_meta_lookup_count == 0);
    assert(key2_handle.region_id == 88);
    assert(key2_handle.offset == sizeof(v2));
    assert(key2_handle.bytes == sizeof(v2));

    source = VEMB_V16_TLC_LOOKUP_SOURCE_LOCAL;
    memset(&key2_handle, 0xff, sizeof(key2_handle));
    assert(vemb_v16_tlc_lookup_vsim_key2(tlc,
                                         missing,
                                         (uint32_t)strlen(missing),
                                         missing_hash,
                                         &key2_handle,
                                         &source,
                                         &timing) != 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_NONE);
    assert(timing.local_lookup_count == 1);
    assert(timing.remote_meta_lookup_count == 0);

    vemb_v16_tlc_destroy(tlc);
}

static void test_vsim_key2_lookup_remote_source(void) {
    enum { dim = 2, max_vectors = 4, remote_entries = 4, remote_buckets = 8 };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_shared_region_allocator_t allocator;
    vemb_v16_tlc_t *owner = NULL;
    vemb_v16_tlc_t *reader = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 99,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .shared_allocator = &allocator,
    };
    vemb_v16_remote_meta_view_t remote_meta;
    size_t remote_meta_bytes =
        vemb_v16_remote_meta_layout_bytes(remote_entries, remote_buckets);
    void *remote_meta_base = NULL;
    const char *key2 = "vsim:remote-key2";
    uint64_t key2_hash = vemb_v16_murmur3(key2, strlen(key2));
    vemb_v16_vector_handle_t handle = {0};
    vemb_v16_vector_handle_t remote_handle = {0};
    vemb_v16_tlc_lookup_source_t source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    vemb_v16_tlc_lookup_timing_t timing = {0};
    uint32_t warm_slot = UINT32_MAX;
    const uint8_t *bytes = NULL;
    uint32_t len = 0;

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 99, max_vectors);
    assert(posix_memalign(&remote_meta_base, 64, remote_meta_bytes) == 0);
    assert(vemb_v16_remote_meta_init(&remote_meta,
                                     remote_meta_base,
                                     remote_meta_bytes,
                                     1,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);

    assert(vemb_v16_tlc_create(&owner, dim, max_vectors, &warm, 1, 4) == 0);
    assert(vemb_v16_tlc_create(&reader, dim, max_vectors, &warm, 1, 4) == 0);
    vemb_v16_tlc_set_remote_meta_view(owner, &remote_meta, 8);
    vemb_v16_tlc_set_remote_meta_view(reader, &remote_meta, 8);

    fill_vector(vector, dim, 300);
    assert(vemb_v16_tlc_put(owner, key2, (uint32_t)strlen(key2), key2_hash,
                            vector, sizeof(vector), &handle, &warm_slot) == 0);
    assert(vemb_v16_tlc_publish_remote_meta(owner,
                                            key2,
                                            (uint32_t)strlen(key2),
                                            key2_hash,
                                            &handle) == 0);

    assert(vemb_v16_tlc_lookup_vsim_key2(reader,
                                         key2,
                                         (uint32_t)strlen(key2),
                                         key2_hash,
                                         &remote_handle,
                                         &source,
                                         &timing) == 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_REMOTE);
    assert(timing.local_lookup_count == 1);
    assert(timing.remote_meta_lookup_count == 1);
    assert(remote_handle.region_id == handle.region_id);
    assert(remote_handle.offset == handle.offset);
    assert(remote_handle.bytes == handle.bytes);
    assert(vemb_v16_tlc_vector_slice(reader, &remote_handle, &bytes, &len) == 0);
    assert(len == sizeof(vector));
    assert(memcmp(bytes, vector, sizeof(vector)) == 0);

    vemb_v16_tlc_destroy(reader);
    vemb_v16_tlc_destroy(owner);
    free(remote_meta_base);
}

static uint32_t fixed_owner_resolver(uint64_t key_hash,
                                     const char *key,
                                     uint32_t key_len,
                                     void *arg) {
    (void)key_hash;
    (void)key;
    (void)key_len;
    return *(uint32_t *)arg;
}

static void test_vsim_key2_lookup_remote_owner_routing(void) {
    enum { dim = 2, max_vectors = 4, remote_entries = 4, remote_buckets = 8 };
    float region1[dim * max_vectors];
    float region2[dim * max_vectors];
    float vector[dim];
    vemb_v16_shared_region_allocator_t alloc1, alloc2;
    vemb_v16_tlc_t *owner2 = NULL;
    vemb_v16_tlc_t *reader = NULL;
    vemb_v16_tlc_warm_region_t owner2_region = {
        .region_id = 992,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region2,
        .region_bytes = sizeof(region2),
        .value_size = dim * sizeof(float),
        .shared_allocator = &alloc2,
    };
    vemb_v16_tlc_warm_region_t reader_regions[] = {
        {
            .region_id = 991,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = region1,
            .region_bytes = sizeof(region1),
            .value_size = dim * sizeof(float),
            .shared_allocator = &alloc1,
        },
        {
            .region_id = 992,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = region2,
            .region_bytes = sizeof(region2),
            .value_size = dim * sizeof(float),
            .shared_allocator = &alloc2,
        },
    };
    vemb_v16_remote_meta_view_t owner1_meta, owner2_meta;
    size_t meta_bytes =
        vemb_v16_remote_meta_layout_bytes(remote_entries, remote_buckets);
    void *owner1_base = NULL;
    void *owner2_base = NULL;
    uint32_t wanted_owner = 2;
    const char *key2 = "vsim:owner2-key2";
    uint64_t key2_hash = vemb_v16_murmur3(key2, strlen(key2));
    vemb_v16_vector_handle_t handle = {0};
    vemb_v16_vector_handle_t remote_handle = {0};
    vemb_v16_tlc_lookup_source_t source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    vemb_v16_tlc_lookup_timing_t timing = {0};
    uint32_t warm_slot = UINT32_MAX;
    const uint8_t *bytes = NULL;
    uint32_t len = 0;

    memset(region1, 0, sizeof(region1));
    memset(region2, 0, sizeof(region2));
    init_test_allocator(&alloc1, 991, max_vectors);
    init_test_allocator(&alloc2, 992, max_vectors);
    assert(posix_memalign(&owner1_base, 64, meta_bytes) == 0);
    assert(posix_memalign(&owner2_base, 64, meta_bytes) == 0);
    assert(vemb_v16_remote_meta_init(&owner1_meta,
                                     owner1_base,
                                     meta_bytes,
                                     1,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_init(&owner2_meta,
                                     owner2_base,
                                     meta_bytes,
                                     2,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);

    assert(vemb_v16_tlc_create(&owner2, dim, max_vectors,
                               &owner2_region, 1, 4) == 0);
    assert(vemb_v16_tlc_create(&reader, dim, max_vectors,
                               reader_regions, 2, 4) == 0);
    vemb_v16_tlc_set_remote_meta_view(owner2, &owner2_meta, 8);
    vemb_v16_tlc_set_remote_meta_view(reader, &owner1_meta, 8);
    assert(vemb_v16_tlc_set_remote_meta_owner_view(reader, 2, &owner2_meta) == 0);
    vemb_v16_tlc_set_owner_resolver(reader,
                                    fixed_owner_resolver,
                                    &wanted_owner);

    fill_vector(vector, dim, 400);
    assert(vemb_v16_tlc_put(owner2,
                            key2,
                            (uint32_t)strlen(key2),
                            key2_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);
    assert(vemb_v16_tlc_publish_remote_meta(owner2,
                                            key2,
                                            (uint32_t)strlen(key2),
                                            key2_hash,
                                            &handle) == 0);

    assert(vemb_v16_tlc_lookup_vsim_key2(reader,
                                         key2,
                                         (uint32_t)strlen(key2),
                                         key2_hash,
                                         &remote_handle,
                                         &source,
                                         &timing) == 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_REMOTE);
    assert(timing.local_lookup_count == 1);
    assert(timing.remote_meta_lookup_count == 1);
    assert(remote_handle.region_id == handle.region_id);
    assert(remote_handle.offset == handle.offset);
    assert(remote_handle.bytes == handle.bytes);
    assert(vemb_v16_tlc_vector_slice(reader, &remote_handle, &bytes, &len) == 0);
    assert(len == sizeof(vector));
    assert(memcmp(bytes, vector, sizeof(vector)) == 0);

    vemb_v16_tlc_destroy(reader);
    vemb_v16_tlc_destroy(owner2);
    free(owner2_base);
    free(owner1_base);
}

static void test_shared_allocator_local_set_before_remote(void) {
    enum { dim = 2, max_vectors = 6 };
    float local0[dim], local1[dim], remote[dim * 4];
    vemb_v16_shared_region_allocator_t alloc0, alloc1, alloc_remote;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t regions[] = {
        {
            .region_id = 100,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = local0,
            .region_bytes = sizeof(local0),
            .value_size = dim * sizeof(float),
            .shared_allocator = &alloc0,
        },
        {
            .region_id = 101,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = local1,
            .region_bytes = sizeof(local1),
            .value_size = dim * sizeof(float),
            .shared_allocator = &alloc1,
        },
        {
            .region_id = 200,
            .backend_type = VEMB_V16_REGION_UB,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = remote,
            .region_bytes = sizeof(remote),
            .value_size = dim * sizeof(float),
            .shared_allocator = &alloc_remote,
        },
    };
    float vector[dim];
    uint32_t local_writes = 0;
    uint32_t remote_writes = 0;

    memset(local0, 0, sizeof(local0));
    memset(local1, 0, sizeof(local1));
    memset(remote, 0, sizeof(remote));
    init_test_allocator(&alloc0, 100, 1);
    init_test_allocator(&alloc1, 101, 1);
    init_test_allocator(&alloc_remote, 200, 4);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, regions, 3, 4) == 0);
    fill_vector(vector, dim, 4000);

    for (uint32_t i = 0; i < 3; i++) {
        char key[32];
        vemb_v16_vector_handle_t handle = {0};
        uint32_t warm_slot = UINT32_MAX;
        snprintf(key, sizeof(key), "local-first:%u", i);
        uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));
        assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                                vector, sizeof(vector), &handle,
                                &warm_slot) == 0);
        if (handle.region_id == 100 || handle.region_id == 101)
            local_writes++;
        if (handle.region_id == 200)
            remote_writes++;
    }
    assert(local_writes == 2);
    assert(remote_writes == 1);
    assert(vemb_v16_shared_allocator_full(&alloc0) == 1);
    assert(vemb_v16_shared_allocator_full(&alloc1) == 1);
    assert(vemb_v16_shared_allocator_used_slots(&alloc_remote) == 1);

    vemb_v16_tlc_destroy(tlc);
}

int main(void) {
    test_put_get_handle();
    test_overwrite_and_capacity();
    test_cold_read_through_promotes_warm_handle();
    test_hot_is_cache_only();
    test_prefill_distribution_stays_warm();
    test_concurrent_distinct_keys();
    test_multi_region_local_full_fallback_and_overwrite();
    test_multi_region_all_full_spills_cold();
    test_shared_allocator_two_tlcs_unique_slots();
    test_vsim_key2_lookup_local_source();
    test_vsim_key2_lookup_remote_source();
    test_vsim_key2_lookup_remote_owner_routing();
    test_shared_allocator_local_set_before_remote();
    printf("vemb_v16_tlc_ut: all tests passed\n");
    return 0;
}
