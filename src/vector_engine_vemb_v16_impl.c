#define _GNU_SOURCE

#include "vector_engine.h"
#include "vemb_v16_server_tcp_client.h"
#include "server.h"

#include <string.h>

#ifndef UNUSED
#define UNUSED(V) ((void)(V))
#endif

static int vemb_v16_engine_init(void) {
    if (!server.vemb_v16_enabled) return C_ERR;
    const char *host = server.vemb_v16_tcp_host && server.vemb_v16_tcp_host[0]
        ? server.vemb_v16_tcp_host
        : VEMB_V16_TCP_HOST;
    uint16_t port = (uint16_t)(server.vemb_v16_tcp_port > 0
                               ? server.vemb_v16_tcp_port
                               : VEMB_V16_TCP_PORT);
    uint32_t dim = server.vemb_v16_dim > 0
        ? (uint32_t)server.vemb_v16_dim
        : VEMB_V16_DEFAULT_DIM;

    if (vemb_v16_stc_init(host, port, dim) != 0) {
        serverLog(LL_WARNING, "vemb_v16_stc_init failed");
        return C_ERR;
    }
    serverLog(LL_NOTICE, "VEMB V16 vector engine initialized: tcp=%s:%u dim=%u",
              host, port, dim);
    return C_OK;
}

static void vemb_v16_engine_cleanup(void) {
    vemb_v16_stc_cleanup();
}

static const char *vemb_v16_obj_to_cstring(void *obj, sds *tmp) {
    robj *o = (robj *)obj;
    if (!obj) return NULL;
    if (sdsEncodedObject(o)) return (const char *)o->ptr;
    o = getDecodedObject(o);
    if (!o || !sdsEncodedObject(o)) return NULL;
    *tmp = sdsdup((sds)o->ptr);
    decrRefCount(o);
    return *tmp;
}

static int vemb_v16_engine_vadd(void *ctx, void *key,
                                vector_data_t *vector,
                                void *element, void *attributes) {
    UNUSED(ctx);
    UNUSED(attributes);

    if (!vector || vector->dim == 0 || !vector->data) return C_ERR;

    sds key_tmp = NULL;
    const char *set_name = vemb_v16_obj_to_cstring(key, &key_tmp);
    sds element_tmp = NULL;
    const char *element_name = vemb_v16_obj_to_cstring(element, &element_tmp);
    if (!set_name || !element_name) {
        sdsfree(key_tmp);
        sdsfree(element_tmp);
        return C_ERR;
    }

    size_t set_len = strlen(set_name);
    size_t elem_len = strlen(element_name);
    size_t total = set_len + 1 + elem_len;
    if (total >= VEMB_V16_MAX_KEY_LEN) {
        sdsfree(key_tmp);
        sdsfree(element_tmp);
        return C_ERR;
    }
    char combined[VEMB_V16_MAX_KEY_LEN];
    memcpy(combined, set_name, set_len);
    combined[set_len] = '\0';
    memcpy(combined + set_len + 1, element_name, elem_len);

    vemb_v16_resp_t resp;
    int rc = vemb_v16_stc_vadd(combined,
                               (uint32_t)total,
                               vector->data,
                               (uint32_t)vector->dim,
                               &resp);
    sdsfree(key_tmp);
    sdsfree(element_tmp);

    if (rc != 0 || resp.status != VEMB_V16_STATUS_OK) return C_ERR;
    return C_OK;
}

static int vemb_v16_engine_vrem(void *ctx, void *key, void *element) {
    UNUSED(ctx);
    UNUSED(key);
    UNUSED(element);
    /* TODO: implement if needed */
    return C_ERR;
}

static int vemb_v16_engine_vsim(void *ctx, void *key,
                                vector_data_t *query_vector, size_t count,
                                vector_query_result_t **results,
                                size_t *num_results) {
    UNUSED(ctx);
    UNUSED(key);
    UNUSED(query_vector);
    UNUSED(count);
    UNUSED(results);
    UNUSED(num_results);
    /*
     * VEMB V16 VSIM_INLINE/VSIM_KEY_KEY compute exact cosine similarity
     * between two specific vectors (one-to-one). The vector_engine vsim
     * interface expects ANN-style search that returns top-K similar
     * elements (one-to-many). These semantics do not map directly.
     *
     * For direct VSIM_INLINE calls, use the client SDK:
     *   vemb_v16_client_vsim(client, set, elem, query_vector, dim, &score);
     * or the redis-cli fast path:
     *   VSIM set_name elem_name 0.1,0.2,0.3,...
     */
    return C_ERR;
}

static int vemb_v16_engine_vemb(void *ctx, void *key,
                                void *element, vector_data_t *result) {
    UNUSED(ctx);

    if (!result) return C_ERR;

    sds key_tmp = NULL;
    const char *set_name = vemb_v16_obj_to_cstring(key, &key_tmp);
    sds element_tmp = NULL;
    const char *element_name = vemb_v16_obj_to_cstring(element, &element_tmp);
    if (!set_name || !element_name) {
        sdsfree(key_tmp);
        sdsfree(element_tmp);
        return C_ERR;
    }

    size_t set_len = strlen(set_name);
    size_t elem_len = strlen(element_name);
    size_t total = set_len + 1 + elem_len;
    if (total >= VEMB_V16_MAX_KEY_LEN) {
        sdsfree(key_tmp);
        sdsfree(element_tmp);
        return C_ERR;
    }
    char combined[VEMB_V16_MAX_KEY_LEN];
    memcpy(combined, set_name, set_len);
    combined[set_len] = '\0';
    memcpy(combined + set_len + 1, element_name, elem_len);

    vemb_v16_resp_t resp;
    uint32_t dim = server.vemb_v16_dim > 0
        ? (uint32_t)server.vemb_v16_dim
        : VEMB_V16_DEFAULT_DIM;

    /* zero-copy: ask proxy not to inline vector over TCP */
    int rc = vemb_v16_stc_vemb(combined,
                               (uint32_t)total,
                               0, /* inline_vector = 0 */
                               &resp,
                               NULL, 0, NULL);
    sdsfree(key_tmp);
    sdsfree(element_tmp);

    if (rc != 0) {
        return C_ERR;
    }

    if (resp.status == VEMB_V16_STATUS_NOT_FOUND) {
        result->data = NULL;
        result->dim = 0;
        return C_OK; /* (nil) case */
    }

    if (resp.status != VEMB_V16_STATUS_OK) {
        return C_ERR;
    }

    /* read vector from SHM warm region (aligned with vemb_v16_bench.c) */
    const uint8_t *warm_mapped = vemb_v16_stc_get_warm_mapped_addr();
    uint64_t warm_bytes = vemb_v16_stc_get_warm_region_bytes();
    if (!warm_mapped || resp.vector_bytes == 0 ||
        resp.vector_offset + resp.vector_bytes > warm_bytes) {
        return C_ERR;
    }

    uint8_t *vector_buf = zmalloc(resp.vector_bytes);
    if (!vector_buf) return C_ERR;

    memcpy(vector_buf, warm_mapped + resp.vector_offset, resp.vector_bytes);

    result->dim = resp.dim > 0 ? resp.dim : dim;
    result->is_fp32 = 1;
    result->data = (float *)vector_buf;
    return C_OK;
}

static int vemb_v16_engine_vcard(void *ctx, void *key) {
    UNUSED(ctx);
    UNUSED(key);
    /* TODO: implement if needed */
    return 0;
}

static int vemb_v16_engine_vdim(void *ctx, void *key) {
    UNUSED(ctx);
    UNUSED(key);
    return server.vemb_v16_dim > 0 ? server.vemb_v16_dim : VEMB_V16_DEFAULT_DIM;
}

static int vemb_v16_engine_set_config(const char *key, const char *value) {
    UNUSED(key);
    UNUSED(value);
    return C_ERR;
}

static sds vemb_v16_engine_get_config(const char *key) {
    UNUSED(key);
    return NULL;
}

static sds vemb_v16_engine_get_stats(void) {
    return sdscatfmt(sdsempty(),
                     "VEMB V16 engine: tcp=%s:%i dim=%i max_vectors=%i",
                     server.vemb_v16_tcp_host ? server.vemb_v16_tcp_host : "127.0.0.1",
                     server.vemb_v16_tcp_port,
                     server.vemb_v16_dim,
                     server.vemb_v16_max_vectors);
}

static vector_engine_t vemb_v16_vector_engine = {
    .type = VECTOR_ENGINE_VEMB_V16,
    .init = vemb_v16_engine_init,
    .cleanup = vemb_v16_engine_cleanup,
    .vadd = vemb_v16_engine_vadd,
    .vrem = vemb_v16_engine_vrem,
    .vsim = vemb_v16_engine_vsim,
    .vemb = vemb_v16_engine_vemb,
    .vcard = vemb_v16_engine_vcard,
    .vdim = vemb_v16_engine_vdim,
    .set_config = vemb_v16_engine_set_config,
    .get_config = vemb_v16_engine_get_config,
    .get_stats = vemb_v16_engine_get_stats,
};

__attribute__((constructor))
static void vemb_v16_engine_register(void) {
    vector_engine_register(VECTOR_ENGINE_VEMB_V16, &vemb_v16_vector_engine);
}
