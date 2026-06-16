#include "../src/vemb_v16_tlc.h"
#include "../src/vemb_v16_remote_meta.h"
#include "../src/vemb_v16_ub_rpc.h"

#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

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

static void set_rpc_ring(vemb_v16_ub_rpc_ring_config_t *ring,
                         const char *path) {
    memset(ring, 0, sizeof(*ring));
    ring->backend_type = VEMB_V16_REGION_LOCAL_SHM;
    snprintf(ring->path, sizeof(ring->path), "%s", path);
}

static void cleanup_rpc_rings(const char *req_a_b,
                              const char *req_b_a,
                              const char *resp_a_b,
                              const char *resp_b_a) {
    shm_unlink(req_a_b);
    shm_unlink(req_b_a);
    shm_unlink(resp_a_b);
    shm_unlink(resp_b_a);
}

static void make_rpc_peers(vemb_v16_ub_rpc_peer_t *peer_a_to_b,
                           uint32_t owner_b,
                           vemb_v16_ub_rpc_peer_t *peer_b_to_a,
                           uint32_t owner_a,
                           const char *req_a_b,
                           const char *req_b_a,
                           const char *resp_a_b,
                           const char *resp_b_a) {
    memset(peer_a_to_b, 0, sizeof(*peer_a_to_b));
    memset(peer_b_to_a, 0, sizeof(*peer_b_to_a));
    peer_a_to_b->owner_id = owner_b;
    set_rpc_ring(&peer_a_to_b->request, req_a_b);
    set_rpc_ring(&peer_a_to_b->response, resp_b_a);
    set_rpc_ring(&peer_a_to_b->inbound_request, req_b_a);
    set_rpc_ring(&peer_a_to_b->outbound_response, resp_a_b);

    peer_b_to_a->owner_id = owner_a;
    set_rpc_ring(&peer_b_to_a->request, req_b_a);
    set_rpc_ring(&peer_b_to_a->response, resp_a_b);
    set_rpc_ring(&peer_b_to_a->inbound_request, req_a_b);
    set_rpc_ring(&peer_b_to_a->outbound_response, resp_b_a);
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
    assert(handle.local_slot == 0);
    assert(handle.owner_generation == 1);
    assert(handle.offset == 0);
    assert(warm_slot == 0);
    assert(memcmp(region, vector, sizeof(vector)) == 0);

    memset(&handle, 0, sizeof(handle));
    warm_slot = 99;
    assert(vemb_v16_tlc_get_handle(tlc, key, (uint32_t)strlen(key), key_hash,
                                   &handle, &warm_slot) == 0);
    assert(handle.region_id == 7);
    assert(handle.offset == 0);
    assert(handle.local_slot == 0);
    assert(handle.owner_generation == 1);
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
    const char *evicting = "extra";
    uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));
    uint64_t evicting_hash = vemb_v16_murmur3(evicting, strlen(evicting));

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
    assert(vemb_v16_tlc_put(tlc, evicting, (uint32_t)strlen(evicting), evicting_hash,
                            first, sizeof(first), &handle, &warm_slot) == 0);
    assert(warm_slot == 0);
    assert(handle.bytes == sizeof(first));
    assert(handle.owner_generation == 2);
    assert(vemb_v16_tlc_get_handle(tlc, evicting, (uint32_t)strlen(evicting),
                                   evicting_hash, &handle, &warm_slot) == 0);
    tlc_core_stats_t stats;
    vemb_v16_tlc_get_core_stats(tlc, &stats);
    assert(stats.warm_same_key_overwrite >= 1);
    assert(stats.warm_eviction_success >= 1);
    vemb_v16_tlc_destroy(tlc);
}

static void test_eviction_rejects_stale_handle(void) {
    enum { dim = 2, max_vectors = 1 };
    float region[dim * max_vectors];
    float first[dim], second[dim];
    vemb_v16_shared_region_allocator_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 6,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .shared_allocator = &allocator,
    };
    vemb_v16_vector_handle_t stale = {0};
    vemb_v16_vector_handle_t fresh = {0};
    uint32_t warm_slot = UINT32_MAX;
    const uint8_t *bytes = NULL;
    uint32_t len = 0;
    const char *key1 = "stale:one";
    const char *key2 = "stale:two";
    uint64_t key1_hash = vemb_v16_murmur3(key1, strlen(key1));
    uint64_t key2_hash = vemb_v16_murmur3(key2, strlen(key2));

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 6, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);
    fill_vector(first, dim, 11);
    fill_vector(second, dim, 22);
    assert(vemb_v16_tlc_put(tlc, key1, (uint32_t)strlen(key1), key1_hash,
                            first, sizeof(first), &stale, &warm_slot) == 0);
    assert(vemb_v16_tlc_vector_slice(tlc, &stale, &bytes, &len) == 0);
    assert(len == sizeof(first));
    assert(memcmp(bytes, first, sizeof(first)) == 0);

    assert(vemb_v16_tlc_put(tlc, key2, (uint32_t)strlen(key2), key2_hash,
                            second, sizeof(second), &fresh, &warm_slot) == 0);
    assert(fresh.local_slot == stale.local_slot);
    assert(fresh.owner_generation == stale.owner_generation + 1);
    assert(vemb_v16_tlc_vector_slice(tlc, &stale, &bytes, &len) != 0);
    assert(vemb_v16_tlc_vector_slice(tlc, &fresh, &bytes, &len) == 0);
    assert(len == sizeof(second));
    assert(memcmp(bytes, second, sizeof(second)) == 0);
    tlc_core_stats_t stats;
    vemb_v16_tlc_get_core_stats(tlc, &stats);
    assert(stats.warm_stale_handle_reject >= 1);
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
    assert(h2.region_id == 1);
    assert(h2.offset == 0);
    assert(memcmp(local_region, second, sizeof(second)) == 0);

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
    assert(stats.warm_region_full_count >= 1);
    vemb_v16_tlc_destroy(tlc);
}

static void test_multi_region_all_full_evicts_committed_warm(void) {
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
    assert(warm_slot != UINT32_MAX);
    assert(handle.bytes == sizeof(vector));
    assert(handle.owner_generation >= 2);
    tlc_core_stats_t stats;
    vemb_v16_tlc_get_core_stats(tlc, &stats);
    assert(stats.warm_region_count == 2);
    assert(stats.warm_region_full_count >= 1);
    assert(stats.warm_alloc_cold_spill == 0);
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

static uint32_t fixed_owner_resolver(uint64_t key_hash,
                                     const char *key,
                                     uint32_t key_len,
                                     void *arg);

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
    vemb_v16_remote_meta_view_t owner_meta;
    vemb_v16_remote_meta_view_t reader_meta;
    size_t remote_meta_bytes =
        vemb_v16_remote_meta_layout_bytes(remote_entries, remote_buckets);
    void *owner_meta_base = NULL;
    void *reader_meta_base = NULL;
    uint32_t wanted_owner = 1;
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
    assert(posix_memalign(&owner_meta_base, 64, remote_meta_bytes) == 0);
    assert(posix_memalign(&reader_meta_base, 64, remote_meta_bytes) == 0);
    assert(vemb_v16_remote_meta_init(&owner_meta,
                                     owner_meta_base,
                                     remote_meta_bytes,
                                     1,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_init(&reader_meta,
                                     reader_meta_base,
                                     remote_meta_bytes,
                                     2,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);

    assert(vemb_v16_tlc_create(&owner, dim, max_vectors, &warm, 1, 4) == 0);
    assert(vemb_v16_tlc_create(&reader, dim, max_vectors, &warm, 1, 4) == 0);
    vemb_v16_tlc_set_remote_meta_view(owner, &owner_meta, 8);
    vemb_v16_tlc_set_remote_meta_view(reader, &reader_meta, 8);
    assert(vemb_v16_tlc_set_remote_meta_owner_view(reader, 1, &owner_meta) == 0);
    vemb_v16_tlc_set_owner_resolver(reader,
                                    fixed_owner_resolver,
                                    &wanted_owner);

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
    assert(remote_handle.local_slot == handle.local_slot);
    assert(remote_handle.owner_generation == handle.owner_generation);
    assert(vemb_v16_tlc_vector_slice(reader, &remote_handle, &bytes, &len) == 0);
    assert(len == sizeof(vector));
    assert(memcmp(bytes, vector, sizeof(vector)) == 0);

    vemb_v16_tlc_destroy(reader);
    vemb_v16_tlc_destroy(owner);
    free(reader_meta_base);
    free(owner_meta_base);
}

static void test_remote_meta_async_publish_flush(void) {
    enum { dim = 2, max_vectors = 4, remote_entries = 4, remote_buckets = 8 };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_shared_region_allocator_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 299,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .shared_allocator = &allocator,
    };
    vemb_v16_remote_meta_view_t meta;
    size_t meta_bytes =
        vemb_v16_remote_meta_layout_bytes(remote_entries, remote_buckets);
    void *meta_base = NULL;
    const char *key = "remote-meta:async";
    uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));
    vemb_v16_vector_handle_t handle = {0};
    vemb_v16_remote_meta_handle_t remote_handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    vemb_v16_stats_t stats;

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 299, max_vectors);
    assert(posix_memalign(&meta_base, 64, meta_bytes) == 0);
    assert(vemb_v16_remote_meta_init(&meta,
                                     meta_base,
                                     meta_bytes,
                                     9,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);
    vemb_v16_tlc_set_remote_meta_view(tlc, &meta, 8);

    fill_vector(vector, dim, 700);
    assert(vemb_v16_tlc_put(tlc,
                            key,
                            (uint32_t)strlen(key),
                            key_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);
    assert(vemb_v16_tlc_publish_remote_meta_async(tlc,
                                                  key,
                                                  (uint32_t)strlen(key),
                                                  key_hash,
                                                  &handle) == 0);
    (void)vemb_v16_tlc_flush_remote_meta_publishes(tlc, 0);
    assert(vemb_v16_remote_meta_lookup(&meta,
                                       key,
                                       (uint32_t)strlen(key),
                                       key_hash,
                                       8,
                                       &remote_handle) ==
           VEMB_V16_REMOTE_META_OK);
    assert(remote_handle.local_slot == handle.local_slot);
    memset(&stats, 0, sizeof(stats));
    vemb_v16_tlc_get_runtime_stats(tlc, &stats);
    assert(stats.remote_meta_publish_async_enqueue >= 1);
    assert(stats.remote_meta_publish_ok >= 1);

    vemb_v16_tlc_destroy(tlc);
    free(meta_base);
}

static void test_vsim_key2_lookup_rpc_fallback_and_repair(void) {
    enum { dim = 2, max_vectors = 4, remote_entries = 4, remote_buckets = 8 };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_shared_region_allocator_t allocator;
    vemb_v16_tlc_t *owner = NULL;
    vemb_v16_tlc_t *reader = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 399,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .shared_allocator = &allocator,
    };
    vemb_v16_remote_meta_view_t owner_meta;
    vemb_v16_remote_meta_view_t reader_meta;
    size_t remote_meta_bytes =
        vemb_v16_remote_meta_layout_bytes(remote_entries, remote_buckets);
    void *owner_meta_base = NULL;
    void *reader_meta_base = NULL;
    uint32_t wanted_owner = 1;
    const char *key2 = "vsim:rpc-key2";
    uint64_t key2_hash = vemb_v16_murmur3(key2, strlen(key2));
    vemb_v16_vector_handle_t handle = {0};
    vemb_v16_vector_handle_t remote_handle = {0};
    vemb_v16_tlc_lookup_source_t source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    vemb_v16_tlc_lookup_timing_t timing = {0};
    uint32_t warm_slot = UINT32_MAX;
    vemb_v16_remote_meta_handle_t repaired = {0};
    vemb_v16_stats_t stats;

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 399, max_vectors);
    assert(posix_memalign(&owner_meta_base, 64, remote_meta_bytes) == 0);
    assert(posix_memalign(&reader_meta_base, 64, remote_meta_bytes) == 0);
    assert(vemb_v16_remote_meta_init(&owner_meta,
                                     owner_meta_base,
                                     remote_meta_bytes,
                                     1,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_init(&reader_meta,
                                     reader_meta_base,
                                     remote_meta_bytes,
                                     2,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);

    assert(vemb_v16_tlc_create(&owner, dim, max_vectors, &warm, 1, 4) == 0);
    assert(vemb_v16_tlc_create(&reader, dim, max_vectors, &warm, 1, 4) == 0);
    vemb_v16_tlc_set_remote_meta_view(owner, &owner_meta, 8);
    vemb_v16_tlc_set_remote_meta_view(reader, &reader_meta, 8);
    assert(vemb_v16_tlc_set_remote_meta_owner_view(reader, 1, &owner_meta) == 0);
    vemb_v16_tlc_set_owner_resolver(reader,
                                    fixed_owner_resolver,
                                    &wanted_owner);
    vemb_v16_tlc_set_lookup_rpc(reader,
                                vemb_v16_tlc_lookup_rpc_local_handler,
                                owner);

    fill_vector(vector, dim, 800);
    assert(vemb_v16_tlc_put(owner,
                            key2,
                            (uint32_t)strlen(key2),
                            key2_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);

    assert(vemb_v16_tlc_lookup_vsim_key2(reader,
                                         key2,
                                         (uint32_t)strlen(key2),
                                         key2_hash,
                                         &remote_handle,
                                         &source,
                                         &timing) == 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_UB_RPC);
    assert(timing.remote_meta_lookup_count == 1);
    assert(remote_handle.local_slot == handle.local_slot);
    assert(remote_handle.owner_generation == handle.owner_generation);

    (void)vemb_v16_tlc_flush_remote_meta_publishes(reader, 0);
    assert(vemb_v16_remote_meta_lookup(&owner_meta,
                                       key2,
                                       (uint32_t)strlen(key2),
                                       key2_hash,
                                       8,
                                       &repaired) ==
           VEMB_V16_REMOTE_META_OK);
    assert(repaired.local_slot == handle.local_slot);
    memset(&stats, 0, sizeof(stats));
    vemb_v16_tlc_get_runtime_stats(reader, &stats);
    assert(stats.remote_meta_lookup_miss >= 1);
    assert(stats.ub_lookup_rpc_ok >= 1);
    assert(stats.ub_lookup_rpc_handle >= 1);
    assert(stats.remote_meta_repair_enqueue >= 1);

    vemb_v16_tlc_destroy(reader);
    vemb_v16_tlc_destroy(owner);
    free(reader_meta_base);
    free(owner_meta_base);
}

static void test_vsim_key2_lookup_ub_ring_rpc_fallback_and_repair(void) {
    enum { dim = 2, max_vectors = 8, remote_entries = 8, remote_buckets = 16 };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_shared_region_allocator_t allocator;
    vemb_v16_tlc_t *owner = NULL;
    vemb_v16_tlc_t *reader = NULL;
    vemb_v16_ub_rpc_t *owner_rpc = NULL;
    vemb_v16_ub_rpc_t *reader_rpc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 499,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .shared_allocator = &allocator,
    };
    vemb_v16_remote_meta_view_t owner_meta;
    vemb_v16_remote_meta_view_t reader_meta;
    size_t remote_meta_bytes =
        vemb_v16_remote_meta_layout_bytes(remote_entries, remote_buckets);
    void *owner_meta_base = NULL;
    void *reader_meta_base = NULL;
    uint32_t wanted_owner = 1;
    const char *key2 = "vsim:ub-ring-rpc-key2";
    uint64_t key2_hash = vemb_v16_murmur3(key2, strlen(key2));
    vemb_v16_vector_handle_t handle = {0};
    vemb_v16_vector_handle_t remote_handle = {0};
    vemb_v16_tlc_lookup_source_t source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    vemb_v16_tlc_lookup_timing_t timing = {0};
    uint32_t warm_slot = UINT32_MAX;
    vemb_v16_remote_meta_handle_t repaired = {0};
    vemb_v16_stats_t stats;
    char req_reader_owner[64];
    char req_owner_reader[64];
    char resp_reader_owner[64];
    char resp_owner_reader[64];
    vemb_v16_ub_rpc_peer_t reader_peer;
    vemb_v16_ub_rpc_peer_t owner_peer;

    snprintf(req_reader_owner, sizeof(req_reader_owner),
             "/v16rpc_%ld_req_2_1", (long)getpid());
    snprintf(req_owner_reader, sizeof(req_owner_reader),
             "/v16rpc_%ld_req_1_2", (long)getpid());
    snprintf(resp_reader_owner, sizeof(resp_reader_owner),
             "/v16rpc_%ld_resp_2_1", (long)getpid());
    snprintf(resp_owner_reader, sizeof(resp_owner_reader),
             "/v16rpc_%ld_resp_1_2", (long)getpid());
    cleanup_rpc_rings(req_reader_owner,
                      req_owner_reader,
                      resp_reader_owner,
                      resp_owner_reader);
    make_rpc_peers(&reader_peer,
                   1,
                   &owner_peer,
                   2,
                   req_reader_owner,
                   req_owner_reader,
                   resp_reader_owner,
                   resp_owner_reader);

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 499, max_vectors);
    assert(posix_memalign(&owner_meta_base, 64, remote_meta_bytes) == 0);
    assert(posix_memalign(&reader_meta_base, 64, remote_meta_bytes) == 0);
    assert(vemb_v16_remote_meta_init(&owner_meta,
                                     owner_meta_base,
                                     remote_meta_bytes,
                                     1,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_init(&reader_meta,
                                     reader_meta_base,
                                     remote_meta_bytes,
                                     2,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);

    assert(vemb_v16_tlc_create(&owner, dim, max_vectors, &warm, 1, 4) == 0);
    assert(vemb_v16_tlc_create(&reader, dim, max_vectors, &warm, 1, 4) == 0);
    vemb_v16_tlc_set_remote_meta_view(owner, &owner_meta, 8);
    vemb_v16_tlc_set_remote_meta_view(reader, &reader_meta, 8);
    assert(vemb_v16_tlc_set_remote_meta_owner_view(reader, 1, &owner_meta) == 0);
    vemb_v16_tlc_set_owner_resolver(reader,
                                    fixed_owner_resolver,
                                    &wanted_owner);
    assert(vemb_v16_ub_rpc_create(&owner_rpc,
                                  owner,
                                  1,
                                  100,
                                  &owner_peer,
                                  1) == 0);
    assert(vemb_v16_ub_rpc_create(&reader_rpc,
                                  reader,
                                  2,
                                  100,
                                  &reader_peer,
                                  1) == 0);

    fill_vector(vector, dim, 1800);
    assert(vemb_v16_tlc_put(owner,
                            key2,
                            (uint32_t)strlen(key2),
                            key2_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);

    assert(vemb_v16_tlc_lookup_vsim_key2(reader,
                                         key2,
                                         (uint32_t)strlen(key2),
                                         key2_hash,
                                         &remote_handle,
                                         &source,
                                         &timing) == 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_UB_RPC);
    assert(timing.remote_meta_lookup_count == 1);
    assert(remote_handle.local_slot == handle.local_slot);
    assert(remote_handle.owner_generation == handle.owner_generation);

    (void)vemb_v16_tlc_flush_remote_meta_publishes(reader, 0);
    assert(vemb_v16_remote_meta_lookup(&owner_meta,
                                       key2,
                                       (uint32_t)strlen(key2),
                                       key2_hash,
                                       8,
                                       &repaired) ==
           VEMB_V16_REMOTE_META_OK);
    assert(repaired.local_slot == handle.local_slot);
    memset(&stats, 0, sizeof(stats));
    vemb_v16_tlc_get_runtime_stats(reader, &stats);
    assert(stats.ub_lookup_rpc_ok >= 1);
    assert(stats.ub_lookup_rpc_handle >= 1);
    assert(stats.remote_meta_repair_enqueue >= 1);

    vemb_v16_ub_rpc_destroy(reader_rpc);
    vemb_v16_ub_rpc_destroy(owner_rpc);
    vemb_v16_tlc_destroy(reader);
    vemb_v16_tlc_destroy(owner);
    free(reader_meta_base);
    free(owner_meta_base);
    cleanup_rpc_rings(req_reader_owner,
                      req_owner_reader,
                      resp_reader_owner,
                      resp_owner_reader);
}

typedef struct ub_ring_rpc_concurrent_arg {
    vemb_v16_tlc_t *reader;
    char (*keys)[VEMB_V16_MAX_KEY_LEN];
    uint64_t *hashes;
    uint32_t key_count;
    uint32_t loops;
    uint32_t tid;
    atomic_uint_fast32_t *ok_count;
} ub_ring_rpc_concurrent_arg_t;

static void *ub_ring_rpc_concurrent_worker(void *arg) {
    ub_ring_rpc_concurrent_arg_t *ctx = arg;
    for (uint32_t i = 0; i < ctx->loops; i++) {
        uint32_t idx = (i + ctx->tid) % ctx->key_count;
        vemb_v16_vector_handle_t handle = {0};
        vemb_v16_tlc_lookup_source_t source =
            VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
        vemb_v16_tlc_lookup_timing_t timing = {0};
        assert(vemb_v16_tlc_lookup_vsim_key2(
                   ctx->reader,
                   ctx->keys[idx],
                   (uint32_t)strlen(ctx->keys[idx]),
                   ctx->hashes[idx],
                   &handle,
                   &source,
                   &timing) == 0);
        assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_UB_RPC);
        assert(handle.key_hash == ctx->hashes[idx]);
        assert(handle.bytes != 0);
        atomic_fetch_add_explicit(ctx->ok_count, 1,
                                  memory_order_relaxed);
    }
    return NULL;
}

static void test_vsim_key2_lookup_ub_ring_rpc_concurrent(void) {
    enum {
        dim = 2,
        max_vectors = 32,
        remote_entries = 8,
        remote_buckets = 16,
        key_count = 8,
        thread_count = 4,
        loops = 32,
    };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_shared_region_allocator_t allocator;
    vemb_v16_tlc_t *owner = NULL;
    vemb_v16_tlc_t *reader = NULL;
    vemb_v16_ub_rpc_t *owner_rpc = NULL;
    vemb_v16_ub_rpc_t *reader_rpc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 599,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .shared_allocator = &allocator,
    };
    vemb_v16_remote_meta_view_t owner_meta;
    vemb_v16_remote_meta_view_t reader_meta;
    size_t remote_meta_bytes =
        vemb_v16_remote_meta_layout_bytes(remote_entries, remote_buckets);
    void *owner_meta_base = NULL;
    void *reader_meta_base = NULL;
    uint32_t wanted_owner = 1;
    char keys[key_count][VEMB_V16_MAX_KEY_LEN];
    uint64_t hashes[key_count];
    pthread_t threads[thread_count];
    ub_ring_rpc_concurrent_arg_t args[thread_count];
    atomic_uint_fast32_t ok_count;
    char req_reader_owner[64];
    char req_owner_reader[64];
    char resp_reader_owner[64];
    char resp_owner_reader[64];
    vemb_v16_ub_rpc_peer_t reader_peer;
    vemb_v16_ub_rpc_peer_t owner_peer;

    snprintf(req_reader_owner, sizeof(req_reader_owner),
             "/v16rpc_%ld_c_req_2_1", (long)getpid());
    snprintf(req_owner_reader, sizeof(req_owner_reader),
             "/v16rpc_%ld_c_req_1_2", (long)getpid());
    snprintf(resp_reader_owner, sizeof(resp_reader_owner),
             "/v16rpc_%ld_c_resp_2_1", (long)getpid());
    snprintf(resp_owner_reader, sizeof(resp_owner_reader),
             "/v16rpc_%ld_c_resp_1_2", (long)getpid());
    cleanup_rpc_rings(req_reader_owner,
                      req_owner_reader,
                      resp_reader_owner,
                      resp_owner_reader);
    make_rpc_peers(&reader_peer,
                   1,
                   &owner_peer,
                   2,
                   req_reader_owner,
                   req_owner_reader,
                   resp_reader_owner,
                   resp_owner_reader);

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 599, max_vectors);
    atomic_init(&ok_count, 0);
    assert(posix_memalign(&owner_meta_base, 64, remote_meta_bytes) == 0);
    assert(posix_memalign(&reader_meta_base, 64, remote_meta_bytes) == 0);
    assert(vemb_v16_remote_meta_init(&owner_meta,
                                     owner_meta_base,
                                     remote_meta_bytes,
                                     1,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_init(&reader_meta,
                                     reader_meta_base,
                                     remote_meta_bytes,
                                     2,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);

    assert(vemb_v16_tlc_create(&owner, dim, max_vectors, &warm, 1, 4) == 0);
    assert(vemb_v16_tlc_create(&reader, dim, max_vectors, &warm, 1, 4) == 0);
    vemb_v16_tlc_set_remote_meta_view(owner, &owner_meta, 8);
    vemb_v16_tlc_set_remote_meta_view(reader, &reader_meta, 8);
    vemb_v16_tlc_set_owner_resolver(reader,
                                    fixed_owner_resolver,
                                    &wanted_owner);
    assert(vemb_v16_ub_rpc_create(&owner_rpc,
                                  owner,
                                  1,
                                  100,
                                  &owner_peer,
                                  1) == 0);
    assert(vemb_v16_ub_rpc_create(&reader_rpc,
                                  reader,
                                  2,
                                  100,
                                  &reader_peer,
                                  1) == 0);

    for (uint32_t i = 0; i < key_count; i++) {
        vemb_v16_vector_handle_t handle = {0};
        uint32_t warm_slot = UINT32_MAX;
        snprintf(keys[i], sizeof(keys[i]), "vsim:ub-ring-rpc-conc:%u", i);
        hashes[i] = vemb_v16_murmur3(keys[i], strlen(keys[i]));
        fill_vector(vector, dim, 2000 + i);
        assert(vemb_v16_tlc_put(owner,
                                keys[i],
                                (uint32_t)strlen(keys[i]),
                                hashes[i],
                                vector,
                                sizeof(vector),
                                &handle,
                                &warm_slot) == 0);
    }

    for (uint32_t t = 0; t < thread_count; t++) {
        args[t] = (ub_ring_rpc_concurrent_arg_t){
            .reader = reader,
            .keys = keys,
            .hashes = hashes,
            .key_count = key_count,
            .loops = loops,
            .tid = t,
            .ok_count = &ok_count,
        };
        assert(pthread_create(&threads[t],
                              NULL,
                              ub_ring_rpc_concurrent_worker,
                              &args[t]) == 0);
    }
    for (uint32_t t = 0; t < thread_count; t++)
        assert(pthread_join(threads[t], NULL) == 0);
    assert(atomic_load_explicit(&ok_count, memory_order_relaxed) ==
           thread_count * loops);

    vemb_v16_stats_t stats;
    memset(&stats, 0, sizeof(stats));
    vemb_v16_tlc_get_runtime_stats(reader, &stats);
    assert(stats.ub_lookup_rpc_ok >= thread_count * loops);
    assert(stats.ub_lookup_rpc_handle >= thread_count * loops);

    vemb_v16_ub_rpc_destroy(reader_rpc);
    vemb_v16_ub_rpc_destroy(owner_rpc);
    vemb_v16_tlc_destroy(reader);
    vemb_v16_tlc_destroy(owner);
    free(reader_meta_base);
    free(owner_meta_base);
    cleanup_rpc_rings(req_reader_owner,
                      req_owner_reader,
                      resp_reader_owner,
                      resp_owner_reader);
}

static void test_vsim_key2_lookup_ub_ring_rpc_stale_and_conflict(void) {
    enum { dim = 2, max_vectors = 8 };
    float region[dim * max_vectors];
    float first[dim], second[dim], other[dim];
    vemb_v16_shared_region_allocator_t allocator;
    vemb_v16_tlc_t *owner = NULL;
    vemb_v16_tlc_t *reader = NULL;
    vemb_v16_ub_rpc_t *owner_rpc = NULL;
    vemb_v16_ub_rpc_t *reader_rpc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 699,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .shared_allocator = &allocator,
    };
    vemb_v16_remote_meta_view_t owner_meta;
    vemb_v16_remote_meta_view_t reader_meta;
    size_t remote_meta_bytes =
        vemb_v16_remote_meta_layout_bytes_for_sets(1, 1);
    void *owner_meta_base = NULL;
    void *reader_meta_base = NULL;
    uint32_t wanted_owner = 1;
    const char *stale_key = "vsim:ub-ring-rpc-stale";
    const char *evict_a = "vsim:ub-ring-rpc-evict-a";
    const char *evict_b = "vsim:ub-ring-rpc-evict-b";
    uint64_t stale_hash = vemb_v16_murmur3(stale_key, strlen(stale_key));
    uint64_t evict_a_hash = vemb_v16_murmur3(evict_a, strlen(evict_a));
    uint64_t evict_b_hash = vemb_v16_murmur3(evict_b, strlen(evict_b));
    vemb_v16_vector_handle_t stale_old = {0};
    vemb_v16_vector_handle_t stale_new = {0};
    vemb_v16_vector_handle_t evict_a_handle = {0};
    vemb_v16_vector_handle_t evict_b_handle = {0};
    vemb_v16_vector_handle_t remote_handle = {0};
    vemb_v16_tlc_lookup_source_t source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    vemb_v16_tlc_lookup_timing_t timing = {0};
    uint32_t warm_slot = UINT32_MAX;
    vemb_v16_stats_t stats;
    char req_reader_owner[64];
    char req_owner_reader[64];
    char resp_reader_owner[64];
    char resp_owner_reader[64];
    vemb_v16_ub_rpc_peer_t reader_peer;
    vemb_v16_ub_rpc_peer_t owner_peer;

    snprintf(req_reader_owner, sizeof(req_reader_owner),
             "/v16rpc_%ld_sc_req_2_1", (long)getpid());
    snprintf(req_owner_reader, sizeof(req_owner_reader),
             "/v16rpc_%ld_sc_req_1_2", (long)getpid());
    snprintf(resp_reader_owner, sizeof(resp_reader_owner),
             "/v16rpc_%ld_sc_resp_2_1", (long)getpid());
    snprintf(resp_owner_reader, sizeof(resp_owner_reader),
             "/v16rpc_%ld_sc_resp_1_2", (long)getpid());
    cleanup_rpc_rings(req_reader_owner,
                      req_owner_reader,
                      resp_reader_owner,
                      resp_owner_reader);
    make_rpc_peers(&reader_peer,
                   1,
                   &owner_peer,
                   2,
                   req_reader_owner,
                   req_owner_reader,
                   resp_reader_owner,
                   resp_owner_reader);

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 699, max_vectors);
    assert(posix_memalign(&owner_meta_base, 64, remote_meta_bytes) == 0);
    assert(posix_memalign(&reader_meta_base, 64, remote_meta_bytes) == 0);
    assert(vemb_v16_remote_meta_init_sets(&owner_meta,
                                          owner_meta_base,
                                          remote_meta_bytes,
                                          1,
                                          dim * sizeof(float),
                                          1,
                                          1) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_init_sets(&reader_meta,
                                          reader_meta_base,
                                          remote_meta_bytes,
                                          2,
                                          dim * sizeof(float),
                                          1,
                                          1) ==
           VEMB_V16_REMOTE_META_OK);

    assert(vemb_v16_tlc_create(&owner, dim, max_vectors, &warm, 1, 4) == 0);
    assert(vemb_v16_tlc_create(&reader, dim, max_vectors, &warm, 1, 4) == 0);
    vemb_v16_tlc_set_remote_meta_view(owner, &owner_meta, 8);
    vemb_v16_tlc_set_remote_meta_view(reader, &reader_meta, 8);
    assert(vemb_v16_tlc_set_remote_meta_owner_view(reader, 1, &owner_meta) == 0);
    vemb_v16_tlc_set_owner_resolver(reader,
                                    fixed_owner_resolver,
                                    &wanted_owner);
    assert(vemb_v16_ub_rpc_create(&owner_rpc,
                                  owner,
                                  1,
                                  100,
                                  &owner_peer,
                                  1) == 0);
    assert(vemb_v16_ub_rpc_create(&reader_rpc,
                                  reader,
                                  2,
                                  100,
                                  &reader_peer,
                                  1) == 0);

    fill_vector(first, dim, 3000);
    assert(vemb_v16_tlc_put(owner,
                            stale_key,
                            (uint32_t)strlen(stale_key),
                            stale_hash,
                            first,
                            sizeof(first),
                            &stale_new,
                            &warm_slot) == 0);
    fill_vector(second, dim, 3100);
    assert(vemb_v16_tlc_put(owner,
                            stale_key,
                            (uint32_t)strlen(stale_key),
                            stale_hash,
                            second,
                            sizeof(second),
                            &stale_new,
                            &warm_slot) == 0);
    stale_old = stale_new;
    stale_old.owner_generation++;
    assert(vemb_v16_tlc_publish_remote_meta(owner,
                                            stale_key,
                                            (uint32_t)strlen(stale_key),
                                            stale_hash,
                                            &stale_old) == 0);

    assert(vemb_v16_tlc_lookup_vsim_key2(reader,
                                         stale_key,
                                         (uint32_t)strlen(stale_key),
                                         stale_hash,
                                         &remote_handle,
                                         &source,
                                         &timing) == 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_UB_RPC);
    assert(remote_handle.owner_generation == stale_new.owner_generation);
    assert(remote_handle.local_slot == stale_new.local_slot);
    (void)vemb_v16_tlc_flush_remote_meta_publishes(reader, 0);

    fill_vector(first, dim, 3200);
    fill_vector(other, dim, 3300);
    assert(vemb_v16_tlc_put(owner,
                            evict_a,
                            (uint32_t)strlen(evict_a),
                            evict_a_hash,
                            first,
                            sizeof(first),
                            &evict_a_handle,
                            &warm_slot) == 0);
    assert(vemb_v16_tlc_publish_remote_meta(owner,
                                            evict_a,
                                            (uint32_t)strlen(evict_a),
                                            evict_a_hash,
                                            &evict_a_handle) == 0);
    assert(vemb_v16_tlc_put(owner,
                            evict_b,
                            (uint32_t)strlen(evict_b),
                            evict_b_hash,
                            other,
                            sizeof(other),
                            &evict_b_handle,
                            &warm_slot) == 0);
    assert(vemb_v16_tlc_publish_remote_meta(owner,
                                            evict_b,
                                            (uint32_t)strlen(evict_b),
                                            evict_b_hash,
                                            &evict_b_handle) == 0);

    memset(&remote_handle, 0, sizeof(remote_handle));
    source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    memset(&timing, 0, sizeof(timing));
    assert(vemb_v16_tlc_lookup_vsim_key2(reader,
                                         evict_a,
                                         (uint32_t)strlen(evict_a),
                                         evict_a_hash,
                                         &remote_handle,
                                         &source,
                                         &timing) == 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_UB_RPC);
    assert(remote_handle.local_slot == evict_a_handle.local_slot);
    assert(remote_handle.owner_generation == evict_a_handle.owner_generation);

    memset(&stats, 0, sizeof(stats));
    vemb_v16_tlc_get_runtime_stats(reader, &stats);
    assert(stats.ub_lookup_rpc_ok >= 2);
    assert(stats.ub_lookup_rpc_handle >= 2);
    assert(stats.remote_meta_lookup_set_conflict >= 1);

    vemb_v16_ub_rpc_destroy(reader_rpc);
    vemb_v16_ub_rpc_destroy(owner_rpc);
    vemb_v16_tlc_destroy(reader);
    vemb_v16_tlc_destroy(owner);
    free(reader_meta_base);
    free(owner_meta_base);
    cleanup_rpc_rings(req_reader_owner,
                      req_owner_reader,
                      resp_reader_owner,
                      resp_owner_reader);
}

static void test_vsim_key2_lookup_remote_meta_stale(void) {
    enum { dim = 2, max_vectors = 1, remote_entries = 4, remote_buckets = 8 };
    float region[dim * max_vectors];
    float first[dim], second[dim];
    vemb_v16_shared_region_allocator_t allocator;
    vemb_v16_tlc_t *owner = NULL;
    vemb_v16_tlc_t *reader = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 199,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .shared_allocator = &allocator,
    };
    vemb_v16_remote_meta_view_t owner_meta;
    vemb_v16_remote_meta_view_t reader_meta;
    size_t remote_meta_bytes =
        vemb_v16_remote_meta_layout_bytes(remote_entries, remote_buckets);
    void *owner_meta_base = NULL;
    void *reader_meta_base = NULL;
    uint32_t wanted_owner = 1;
    const char *key1 = "vsim:remote-stale-old";
    const char *key2 = "vsim:remote-stale-new";
    uint64_t key1_hash = vemb_v16_murmur3(key1, strlen(key1));
    uint64_t key2_hash = vemb_v16_murmur3(key2, strlen(key2));
    vemb_v16_vector_handle_t handle = {0};
    vemb_v16_vector_handle_t remote_handle = {0};
    vemb_v16_tlc_lookup_source_t source = VEMB_V16_TLC_LOOKUP_SOURCE_LOCAL;
    vemb_v16_tlc_lookup_timing_t timing = {0};
    uint32_t warm_slot = UINT32_MAX;
    tlc_core_stats_t stats;

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 199, max_vectors);
    assert(posix_memalign(&owner_meta_base, 64, remote_meta_bytes) == 0);
    assert(posix_memalign(&reader_meta_base, 64, remote_meta_bytes) == 0);
    assert(vemb_v16_remote_meta_init(&owner_meta,
                                     owner_meta_base,
                                     remote_meta_bytes,
                                     1,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_init(&reader_meta,
                                     reader_meta_base,
                                     remote_meta_bytes,
                                     2,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);

    assert(vemb_v16_tlc_create(&owner, dim, max_vectors, &warm, 1, 4) == 0);
    assert(vemb_v16_tlc_create(&reader, dim, max_vectors, &warm, 1, 4) == 0);
    vemb_v16_tlc_set_remote_meta_view(owner, &owner_meta, 8);
    vemb_v16_tlc_set_remote_meta_view(reader, &reader_meta, 8);
    assert(vemb_v16_tlc_set_remote_meta_owner_view(reader, 1, &owner_meta) == 0);
    vemb_v16_tlc_set_owner_resolver(reader,
                                    fixed_owner_resolver,
                                    &wanted_owner);

    fill_vector(first, dim, 500);
    fill_vector(second, dim, 600);
    assert(vemb_v16_tlc_put(owner, key1, (uint32_t)strlen(key1), key1_hash,
                            first, sizeof(first), &handle, &warm_slot) == 0);
    assert(vemb_v16_tlc_publish_remote_meta(owner,
                                            key1,
                                            (uint32_t)strlen(key1),
                                            key1_hash,
                                            &handle) == 0);
    assert(vemb_v16_tlc_put(owner, key2, (uint32_t)strlen(key2), key2_hash,
                            second, sizeof(second), &handle, &warm_slot) == 0);

    assert(vemb_v16_tlc_lookup_vsim_key2(reader,
                                         key1,
                                         (uint32_t)strlen(key1),
                                         key1_hash,
                                         &remote_handle,
                                         &source,
                                         &timing) != 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_NONE);
    assert(timing.remote_meta_lookup_count == 1);
    vemb_v16_tlc_get_core_stats(reader, &stats);
    assert(stats.remote_meta_stale >= 1);
    assert(stats.warm_stale_handle_reject >= 1);

    vemb_v16_tlc_destroy(reader);
    vemb_v16_tlc_destroy(owner);
    free(reader_meta_base);
    free(owner_meta_base);
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
    assert(remote_handle.local_slot == handle.local_slot);
    assert(remote_handle.owner_generation == handle.owner_generation);
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
    assert(local_writes == 3);
    assert(remote_writes == 0);
    assert(vemb_v16_shared_allocator_full(&alloc0) == 1);
    assert(vemb_v16_shared_allocator_full(&alloc1) == 1);
    assert(vemb_v16_shared_allocator_used_slots(&alloc_remote) == 0);

    vemb_v16_tlc_destroy(tlc);
}

int main(void) {
    test_put_get_handle();
    test_overwrite_and_capacity();
    test_eviction_rejects_stale_handle();
    test_cold_read_through_promotes_warm_handle();
    test_hot_is_cache_only();
    test_prefill_distribution_stays_warm();
    test_concurrent_distinct_keys();
    test_multi_region_local_full_fallback_and_overwrite();
    test_multi_region_all_full_evicts_committed_warm();
    test_shared_allocator_two_tlcs_unique_slots();
    test_vsim_key2_lookup_local_source();
    test_vsim_key2_lookup_remote_source();
    test_remote_meta_async_publish_flush();
    test_vsim_key2_lookup_rpc_fallback_and_repair();
    test_vsim_key2_lookup_ub_ring_rpc_fallback_and_repair();
    test_vsim_key2_lookup_ub_ring_rpc_concurrent();
    test_vsim_key2_lookup_ub_ring_rpc_stale_and_conflict();
    test_vsim_key2_lookup_remote_meta_stale();
    test_vsim_key2_lookup_remote_owner_routing();
    test_shared_allocator_local_set_before_remote();
    printf("vemb_v16_tlc_ut: all tests passed\n");
    return 0;
}
