#ifndef __UB_METADATA_H
#define __UB_METADATA_H

#include "dict.h"
#include "sds.h"

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

typedef struct ub_row_entry {
    uint64_t row_id;
    sds element;
    int active;
} ub_row_entry_t;

typedef struct ub_vector_set_meta {
    sds key;
    size_t dim;
    size_t cardinality;
    uint64_t next_row_id;
    int suffix_numeric_dense;
    int suffix_numeric_disabled;
    sds suffix_numeric_prefix;
    dict *element_to_row;      /* element -> uint64_t* */
    dict *row_to_element;      /* row_id(string) -> ub_row_entry_t* */
    pthread_rwlock_t lock;
} ub_vector_set_meta_t;

typedef struct ub_metadata_registry {
    dict *sets;                /* key -> ub_vector_set_meta* */
    pthread_rwlock_t lock;
    int initialized;
} ub_metadata_registry_t;

int ub_metadata_init(void);
void ub_metadata_cleanup(void);

ub_vector_set_meta_t *ub_metadata_get_or_create_set(const char *key, size_t dim);
ub_vector_set_meta_t *ub_metadata_get_set(const char *key);

int ub_metadata_lookup_row(ub_vector_set_meta_t *set, const char *element, uint64_t *row_id);
int ub_metadata_lookup_dense_suffix_row(ub_vector_set_meta_t *set,
                                        const char *element,
                                        size_t element_len,
                                        uint64_t *row_id);
int ub_metadata_alloc_row(ub_vector_set_meta_t *set, const char *element, uint64_t *row_id);
int ub_metadata_remove_row(ub_vector_set_meta_t *set, const char *element, uint64_t *row_id);
size_t ub_metadata_cardinality(const ub_vector_set_meta_t *set);

size_t ub_metadata_collect_rows(ub_vector_set_meta_t *set,
                                uint64_t **rows_out,
                                sds **elements_out);

#endif /* __UB_METADATA_H */
