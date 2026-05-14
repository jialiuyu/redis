#include "vector_proxy_request.h"

#include "macro.h"
#include "zmalloc.h"

proxy_vector_request_t *proxy_vector_request_create_vemb(uint64_t request_id,
                                                         uint64_t row_id,
                                                         int raw_output,
                                                         RedisModuleBlockedClient *bc) {
    proxy_vector_request_t *req = zcalloc(sizeof(*req));
    RETURN_IF(!req, NULL);

    req->op_type = PROXY_VECTOR_OP_VEMB;
    req->request_id = request_id;
    req->row_id = row_id;
    req->raw_output = raw_output;
    req->bc = bc;
    return req;
}

void proxy_vector_request_free(proxy_vector_request_t *req) {
    RETURN_IF(!req);
    zfree(req->result_vector);
    zfree(req);
}
