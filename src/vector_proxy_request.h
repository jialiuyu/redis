#ifndef __VECTOR_PROXY_REQUEST_H
#define __VECTOR_PROXY_REQUEST_H

#include <stddef.h>
#include <stdint.h>

typedef struct RedisModuleBlockedClient RedisModuleBlockedClient;

typedef enum proxy_vector_op_type {
    PROXY_VECTOR_OP_VEMB = 1,
} proxy_vector_op_type_t;

typedef struct proxy_vector_request {
    proxy_vector_op_type_t op_type;
    uint64_t request_id;
    uint64_t row_id;
    int raw_output;
    RedisModuleBlockedClient *bc;
    float *result_vector;
    size_t result_dim;
    int error_code;
} proxy_vector_request_t;

proxy_vector_request_t *proxy_vector_request_create_vemb(uint64_t request_id,
                                                         uint64_t row_id,
                                                         int raw_output,
                                                         RedisModuleBlockedClient *bc);
void proxy_vector_request_free(proxy_vector_request_t *req);

#endif /* __VECTOR_PROXY_REQUEST_H */
