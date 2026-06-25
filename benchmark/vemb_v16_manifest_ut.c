#include "../src/vemb_v16_storage.h"

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

static void fill_vector(float *vector, uint32_t dim, uint32_t seed) {
    for (uint32_t i = 0; i < dim; i++)
        vector[i] = (float)(seed + i);
}

static void cleanup_region_and_allocator(const char *path, uint32_t region_id) {
    char allocator_name[VEMB_V16_SHARED_ALLOCATOR_NAME_MAX];
    shm_unlink(path);
    if (vemb_v16_shared_allocator_name_from_region_path(
            path, region_id, allocator_name, sizeof(allocator_name)) == 0) {
        vemb_v16_shared_allocator_unlink(allocator_name);
    }
}

static void test_shm_provider_attaches_existing_payload(void) {
    enum { dim = 2, slots = 2 };
    char shm_name[64];
    vemb_v16_warm_provider_t first, second;
    float vector[dim];

    snprintf(shm_name, sizeof(shm_name),
             "/vemb_v16_provider_%ld_attach", (long)getpid());
    shm_unlink(shm_name);
    memset(&first, 0, sizeof(first));
    memset(&second, 0, sizeof(second));
    first.fd = -1;
    second.fd = -1;

    assert(vemb_v16_warm_provider_open(&first,
                                       301,
                                       VEMB_V16_REGION_LOCAL_SHM,
                                       shm_name,
                                       0,
                                       dim * sizeof(float),
                                       slots * dim * sizeof(float),
                                       0,
                                       1,
                                       1) == 0);
    fill_vector(vector, dim, 11);
    memcpy(first.region.mapped_addr, vector, sizeof(vector));

    assert(vemb_v16_warm_provider_open(&second,
                                       301,
                                       VEMB_V16_REGION_LOCAL_SHM,
                                       shm_name,
                                       0,
                                       dim * sizeof(float),
                                       slots * dim * sizeof(float),
                                       0,
                                       1,
                                       1) == 0);
    assert(memcmp(second.region.mapped_addr, vector, sizeof(vector)) == 0);

    vemb_v16_warm_provider_close(&first);
    assert(memcmp(second.region.mapped_addr, vector, sizeof(vector)) == 0);
    vemb_v16_warm_provider_close(&second);
    shm_unlink(shm_name);
}

static void test_manifest_shm_mock_ub_create_and_put(void) {
    enum { dim = 2, max_vectors = 4 };
    char manifest_path[128];
    char shm1[64];
    char ub2[128];
    vemb_v16_storage_ctx_t *storage = NULL;
    float vector[dim];
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    const char *key = "manifest-key";
    uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));

    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_manifest_%ld.yaml", (long)getpid());
    snprintf(shm1, sizeof(shm1), "/vemb_v16_manifest_%ld_r1", (long)getpid());
    snprintf(ub2, sizeof(ub2), "/tmp/vemb_v16_manifest_%ld_r2.ub", (long)getpid());
    unlink(ub2);
    int ub_fd = open(ub2, O_CREAT | O_TRUNC | O_RDWR, 0666);
    assert(ub_fd >= 0);
    assert(ftruncate(ub_fd,
                     sizeof(vemb_v16_shared_region_allocator_t) +
                     sizeof(float) * dim * 2) == 0);
    close(ub_fd);

    FILE *fp = fopen(manifest_path, "w");
    assert(fp != NULL);
    fprintf(fp,
            "supernode_id: 0\n"
            "local_ub_node_id: 0\n"
            "local_region_weight: 8\n"
            "warm_regions:\n"
            "  - region_id: 101\n"
            "    provider: shm\n"
            "    path: %s\n"
            "    mmap_offset: 0\n"
            "    bytes: %u\n"
            "    value_size: %u\n"
            "    home_ub_node_id: 0\n"
            "    weight: 1\n"
            "  - region_id: 202\n"
            "    provider: ub\n"
            "    path: %s\n"
            "    mmap_offset: 0\n"
            "    bytes: %u\n"
            "    value_size: %u\n"
            "    home_ub_node_id: 1\n"
            "    weight: 1\n",
            shm1,
            (unsigned)(sizeof(float) * dim * 2),
            (unsigned)(sizeof(float) * dim),
            ub2,
            (unsigned)(sizeof(float) * dim * 2),
            (unsigned)(sizeof(float) * dim));
    fclose(fp);

    vemb_v16_warm_regions_manifest_t manifest;
    assert(vemb_v16_parse_warm_regions_manifest(manifest_path,
                                                dim * sizeof(float),
                                                &manifest) == 0);
    assert(vemb_v16_storage_ctx_create_from_manifest(&storage,
                                                     dim,
                                                     dim * sizeof(float),
                                                     max_vectors,
                                                     &manifest) == 0);
    assert(storage->warm_region_count == 2);
    assert(storage->tlc->warm_region_count == 2);
    assert(vemb_v16_tlc_find_region(storage->tlc, 101) != NULL);
    assert(vemb_v16_tlc_find_region(storage->tlc, 202) != NULL);
    vemb_v16_channel_desc_t desc;
    memset(&desc, 0, sizeof(desc));
    vemb_v16_storage_fill_channel_desc(storage, &desc);
    assert(desc.warm_region_count == 2);
    assert(desc.warm_regions[0].region_id == 101);
    assert(desc.warm_regions[1].region_id == 202);
    assert(!strcmp(desc.warm_regions[0].path, shm1));
    assert(!strcmp(desc.warm_regions[1].path, ub2));
    assert(desc.warm_regions[1].backend_type == VEMB_V16_REGION_UB);
    assert(desc.warm_regions[1].mmap_offset ==
           sizeof(vemb_v16_shared_region_allocator_t));

    fill_vector(vector, dim, 42);
    assert(vemb_v16_tlc_put(storage->tlc,
                            key,
                            (uint32_t)strlen(key),
                            key_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);
    assert(handle.bytes == sizeof(vector));
    assert(handle.region_id == 101 || handle.region_id == 202);
    const uint8_t *slice = NULL;
    uint32_t slice_bytes = 0;
    assert(vemb_v16_tlc_vector_slice(storage->tlc,
                                     &handle,
                                     &slice,
                                     &slice_bytes) == 0);
    assert(slice_bytes == sizeof(vector));
    assert(memcmp(slice, vector, sizeof(vector)) == 0);

    vemb_v16_storage_ctx_destroy(storage);
    unlink(manifest_path);
    cleanup_region_and_allocator(shm1, 101);
    unlink(ub2);
}

static void test_storage_reset_clears_payload_and_allocator(void) {
    enum { dim = 2, slots = 2 };
    char shm_name[64];
    vemb_v16_warm_regions_manifest_t manifest;
    vemb_v16_warm_provider_t provider;
    vemb_v16_shared_allocator_mapping_t allocator;
    char allocator_name[VEMB_V16_SHARED_ALLOCATOR_NAME_MAX];
    float vector[dim];

    snprintf(shm_name, sizeof(shm_name),
             "/vemb_v16_reset_%ld_region", (long)getpid());
    cleanup_region_and_allocator(shm_name, 401);

    memset(&manifest, 0, sizeof(manifest));
    manifest.region_count = 1;
    manifest.local_region_weight = 4;
    manifest.regions[0].region_id = 401;
    manifest.regions[0].backend_type = VEMB_V16_REGION_LOCAL_SHM;
    manifest.regions[0].is_local = 1;
    manifest.regions[0].has_is_local = 1;
    manifest.regions[0].weight = 1;
    manifest.regions[0].value_size = dim * sizeof(float);
    manifest.regions[0].region_bytes = slots * dim * sizeof(float);
    snprintf(manifest.regions[0].path,
             sizeof(manifest.regions[0].path),
             "%s",
             shm_name);

    memset(&provider, 0, sizeof(provider));
    provider.fd = -1;
    assert(vemb_v16_warm_provider_open(&provider,
                                       401,
                                       VEMB_V16_REGION_LOCAL_SHM,
                                       shm_name,
                                       0,
                                       dim * sizeof(float),
                                       slots * dim * sizeof(float),
                                       0,
                                       1,
                                       1) == 0);
    fill_vector(vector, dim, 55);
    memcpy(provider.region.mapped_addr, vector, sizeof(vector));
    vemb_v16_warm_provider_close(&provider);

    assert(vemb_v16_shared_allocator_name_from_region_path(
               shm_name, 401, allocator_name, sizeof(allocator_name)) == 0);
    assert(vemb_v16_shared_allocator_open(&allocator,
                                          VEMB_V16_REGION_LOCAL_SHM,
                                          allocator_name,
                                          0,
                                          401,
                                          slots,
                                          1) == 0);
    uint32_t slot = UINT32_MAX;
    assert(vemb_v16_shared_allocator_alloc(allocator.allocator, &slot) ==
           VEMB_V16_SHARED_ALLOCATOR_OK);
    assert(slot == 0);
    vemb_v16_shared_allocator_close(&allocator);

    assert(vemb_v16_storage_reset_manifest_regions(&manifest) == 0);

    memset(&provider, 0, sizeof(provider));
    provider.fd = -1;
    assert(vemb_v16_warm_provider_open(&provider,
                                       401,
                                       VEMB_V16_REGION_LOCAL_SHM,
                                       shm_name,
                                       0,
                                       dim * sizeof(float),
                                       slots * dim * sizeof(float),
                                       0,
                                       1,
                                       1) == 0);
    float zero[dim] = {0};
    assert(memcmp(provider.region.mapped_addr, zero, sizeof(zero)) == 0);
    vemb_v16_warm_provider_close(&provider);

    assert(vemb_v16_shared_allocator_open(&allocator,
                                          VEMB_V16_REGION_LOCAL_SHM,
                                          allocator_name,
                                          0,
                                          401,
                                          slots,
                                          1) == 0);
    slot = UINT32_MAX;
    assert(vemb_v16_shared_allocator_alloc(allocator.allocator, &slot) ==
           VEMB_V16_SHARED_ALLOCATOR_OK);
    assert(slot == 0);
    vemb_v16_shared_allocator_close(&allocator);
    cleanup_region_and_allocator(shm_name, 401);
}

int main(void) {
    test_shm_provider_attaches_existing_payload();
    test_manifest_shm_mock_ub_create_and_put();
    test_storage_reset_clears_payload_and_allocator();
    printf("vemb_v16_manifest_ut: all tests passed\n");
    return 0;
}
