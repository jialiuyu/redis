/*
 * UB client data-plane adapter.
 * The control plane (export/import) is managed outside Redis by obmmctl.
 */

#ifndef __UB_CLIENT_H
#define __UB_CLIENT_H

#include "sds.h"

#include <stddef.h>
#include <stdint.h>

#define UB_DEVICE_PATH_MAX 256

enum {
    UB_ELEMENT_INDEX_NUMERIC = 0,
    UB_ELEMENT_INDEX_SUFFIX_NUMERIC = 1,
    UB_ELEMENT_INDEX_HASH = 2
};
typedef int ub_element_index_mode_t;

typedef struct {
    int vector_dimension;              /* UB data-plane vector dimension */
    int cacheable;                     /* Use cacheable OBMM mapping */
    int use_ownership;                 /* Acquire read ownership for cacheable mappings */
    ub_element_index_mode_t element_index_mode; /* Element to row resolver */
    unsigned long long shm_memid;      /* Existing OBMM shmdev mem_id */
    size_t shm_size;                   /* Full OBMM shmdev mapping size */
    size_t table_offset;               /* Embedding table offset in mapped shmdev */
    size_t table_size;                 /* Embedding table byte size, 0 means till end */
    size_t vector_stride_bytes;        /* Row stride, 0 means dim*sizeof(float) */
    char *table_name;                  /* Redis key name bound to the configured table */
    char *shm_path;                    /* Existing OBMM shmdev path */
} ub_mem_config_t;

typedef struct ub_address_space {
    uint64_t base_addr;          /* Table base offset inside the shmdev mapping */
    size_t size;                 /* Accessible embedding table size */
    uint32_t token_id;           /* Reserved for future use */
    void *mapped_addr;           /* Table base in local VA */
    void *mapping_addr;          /* mmap base address */
    size_t mapping_size;         /* mmap length */
    size_t data_offset;          /* Table offset relative to mapping_addr */
    size_t vector_stride_bytes;  /* Per-row stride */
    unsigned long long mem_id;   /* Existing shmdev mem_id */
    int shm_fd;                  /* Open shmdev fd */
    int cacheable;               /* Cacheable vs non-cacheable mapping */
    int use_ownership;           /* Whether read ownership was acquired */
    char device_path[UB_DEVICE_PATH_MAX];
} ub_address_space_t;

typedef struct {
    int initialized;
    const ub_mem_config_t *config;
    ub_address_space_t *global_ubas;
    uint64_t total_requests;
    uint64_t total_responses;
    uint64_t cache_hits;
    uint64_t cache_misses;
} ub_client_t;

extern ub_client_t *global_ub_client;

int ub_client_init(const ub_mem_config_t *cfg);
void ub_client_cleanup(void);

int ub_client_load_embedding_table(const char *resource_name,
                                   ub_address_space_t **addr_space);
int ub_client_resolve_element_index(const char *element_name,
                                    uint64_t *index,
                                    size_t vector_dim);
int ub_client_perform_gather_load(ub_address_space_t *addr_space,
                                  uint64_t *indices,
                                  size_t num_indices,
                                  float *results,
                                  size_t vector_dim);
int ub_client_perform_scatter_store(ub_address_space_t *addr_space,
                                    uint64_t *indices,
                                    size_t num_indices,
                                    float *data,
                                    size_t vector_dim);

/* Contiguous bulk load — optimised path when indices are {0, 1, …, n-1}.
 * Falls back to gather_load when stride != dim*sizeof(float) (padding). */
int ub_client_perform_contiguous_load(ub_address_space_t *addr_space,
                                      size_t start_index,
                                      size_t num_rows,
                                      float *results,
                                      size_t vector_dim);

/* Single-row convenience wrappers */
static inline int ub_client_store_single(ub_address_space_t *addr_space,
                                         uint64_t index,
                                         float *data, size_t vector_dim) {
    return ub_client_perform_scatter_store(addr_space, &index, 1, data, vector_dim);
}

static inline int ub_client_load_single(ub_address_space_t *addr_space,
                                        uint64_t index,
                                        float *result, size_t vector_dim) {
    return ub_client_perform_gather_load(addr_space, &index, 1, result, vector_dim);
}

int ub_client_set_config(const char *key, const char *value);
sds ub_client_get_config(const char *key);
sds ub_client_get_stats(void);

#endif /* __UB_CLIENT_H */
