#ifndef __VECTOR_PROXY_REQUEST_H
#define __VECTOR_PROXY_REQUEST_H

#include "sds.h"

#include <stddef.h>
#include <stdint.h>

typedef struct RedisModuleBlockedClient RedisModuleBlockedClient;

typedef enum proxy_vector_op_type {
    PROXY_VECTOR_OP_VEMB = 1,
    PROXY_VECTOR_OP_VSIM = 2,
} proxy_vector_op_type_t;

typedef struct proxy_vector_request {
    proxy_vector_op_type_t op_type;
    uint64_t request_id;
    uint64_t row_id;
    int raw_output;
    RedisModuleBlockedClient *bc;
    float *result_vector;
    size_t result_dim;
    float *query_vector;
    size_t query_dim;
    uint64_t *candidate_rows;
    sds *candidate_elements;
    size_t candidate_count;
    size_t requested_count;
    int withscores;
    float *result_scores;
    size_t result_count;
    int error_code;
} proxy_vector_request_t;

proxy_vector_request_t *proxy_vector_request_create_vemb(uint64_t request_id,
                                                         uint64_t row_id,
                                                         int raw_output,
                                                         RedisModuleBlockedClient *bc);
proxy_vector_request_t *proxy_vector_request_create_vsim(uint64_t request_id,
                                                         float *query_vector,
                                                         size_t query_dim,
                                                         uint64_t *candidate_rows,
                                                         sds *candidate_elements,
                                                         size_t candidate_count,
                                                         size_t requested_count,
                                                         int withscores,
                                                         RedisModuleBlockedClient *bc);
void proxy_vector_request_free(proxy_vector_request_t *req);

#endif /* __VECTOR_PROXY_REQUEST_H */
