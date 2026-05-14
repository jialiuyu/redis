#ifndef __VECTOR_PROXY_COMPLETION_H
#define __VECTOR_PROXY_COMPLETION_H

#include "vector_proxy_request.h"

#include <stdint.h>

int vector_proxy_completion_init(void);
void vector_proxy_completion_cleanup(void);
int vector_proxy_completion_register(proxy_vector_request_t *req);
proxy_vector_request_t *vector_proxy_completion_lookup(uint64_t request_id);
int vector_proxy_completion_complete_vemb(uint64_t request_id,
                                          const float *vector,
                                          size_t dim,
                                          int error_code);
int vector_proxy_completion_take(uint64_t request_id, proxy_vector_request_t **req);

#endif /* __VECTOR_PROXY_COMPLETION_H */
