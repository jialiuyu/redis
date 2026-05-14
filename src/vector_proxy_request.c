#include "vector_proxy_request.h"

#include "macro.h"
#include "sds.h"
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

proxy_vector_request_t *proxy_vector_request_create_vsim(uint64_t request_id,
                                                         float *query_vector,
                                                         size_t query_dim,
                                                         uint64_t *candidate_rows,
                                                         sds *candidate_elements,
                                                         size_t candidate_count,
                                                         size_t requested_count,
                                                         int withscores,
                                                         RedisModuleBlockedClient *bc) {
    proxy_vector_request_t *req = zcalloc(sizeof(*req));
    RETURN_IF(!req, NULL);

    req->op_type = PROXY_VECTOR_OP_VSIM;
    req->request_id = request_id;
    req->query_vector = query_vector;
    req->query_dim = query_dim;
    req->candidate_rows = candidate_rows;
    req->candidate_elements = candidate_elements;
    req->candidate_count = candidate_count;
    req->requested_count = requested_count;
    req->withscores = withscores;
    req->bc = bc;
    return req;
}

void proxy_vector_request_free(proxy_vector_request_t *req) {
    RETURN_IF(!req);
    zfree(req->result_vector);
    zfree(req->query_vector);
    zfree(req->candidate_rows);
    zfree(req->result_scores);
    if (req->candidate_elements) {
        for (size_t i = 0; i < req->candidate_count; i++) {
            sdsfree(req->candidate_elements[i]);
        }
        zfree(req->candidate_elements);
    }
    zfree(req);
}
