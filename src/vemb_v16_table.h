#ifndef __VEMB_V16_TABLE_H
#define __VEMB_V16_TABLE_H

#include <stddef.h>
#include <stdint.h>

#include "sve_operation.h"

typedef struct vemb_v16_table vemb_v16_table_t;

int vemb_v16_table_create(vemb_v16_table_t **out,
                          uint32_t vector_dim,
                          uint32_t max_vectors,
                          uint8_t *vector_region,
                          size_t vector_region_size);
void vemb_v16_table_destroy(vemb_v16_table_t *table);

uint32_t vemb_v16_table_dim(vemb_v16_table_t *table);
uint32_t vemb_v16_table_stride(vemb_v16_table_t *table);
uint32_t vemb_v16_table_max_vectors(vemb_v16_table_t *table);
sve_gather_ctx_t *vemb_v16_table_gather_ctx(vemb_v16_table_t *table);
sve_operation_stats_t *vemb_v16_table_sve_stats(vemb_v16_table_t *table);

int vemb_v16_table_lookup(vemb_v16_table_t *table,
                          const char *key,
                          uint32_t key_len,
                          uint64_t key_hash,
                          uint32_t *row_id);
int vemb_v16_table_upsert(vemb_v16_table_t *table,
                          const char *key,
                          uint32_t key_len,
                          uint64_t key_hash,
                          const float *vector,
                          uint32_t vector_bytes,
                          uint32_t *row_id);

#endif
