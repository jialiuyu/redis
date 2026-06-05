#include "../src/vemb_v16_storage.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static void fill_vector(float *vector, uint32_t dim, uint32_t seed) {
    for (uint32_t i = 0; i < dim; i++)
        vector[i] = (float)(seed + i);
}

static void test_manifest_shm_mock_ub_create_and_put(void) {
    enum { dim = 2, max_vectors = 4 };
    char manifest_path[128];
    char shm1[64];
    char shm2[64];
    vemb_v16_storage_ctx_t *storage = NULL;
    float vector[dim];
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    const char *key = "manifest-key";
    uint64_t key_hash = vemb_v16_murmur3(key, strlen(key));

    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_manifest_%ld.yaml", (long)getpid());
    snprintf(shm1, sizeof(shm1), "/vemb_v16_manifest_%ld_r1", (long)getpid());
    snprintf(shm2, sizeof(shm2), "/vemb_v16_manifest_%ld_r2", (long)getpid());

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
            "    provider: mock_ub\n"
            "    path: %s\n"
            "    mmap_offset: 0\n"
            "    bytes: %u\n"
            "    value_size: %u\n"
            "    home_ub_node_id: 1\n"
            "    weight: 1\n",
            shm1,
            (unsigned)(sizeof(float) * dim * 2),
            (unsigned)(sizeof(float) * dim),
            shm2,
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
    assert(!strcmp(desc.warm_regions[1].path, shm2));

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
}

int main(void) {
    test_manifest_shm_mock_ub_create_and_put();
    printf("vemb_v16_manifest_ut: all tests passed\n");
    return 0;
}
