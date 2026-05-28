#define _GNU_SOURCE

#include "vemb_v16_client_sdk.h"
#include "../../src/vemb_v16_net.h"
#include "../../src/vemb_v16_client_ring.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>
#include <errno.h>

struct vemb_v16_client {
    int fd;
    uint64_t channel_id;
    uint32_t dim;
    uint32_t req_id;

    /* for stats re-connection */
    char     host[64];
    uint16_t port;

    /* warm region (mmap'd once at create) */
    uint64_t warm_region_bytes;
    uint64_t warm_mmap_offset;
    size_t   mapping_bytes;
    uint8_t *mapping_addr;
    uint8_t *mapped_addr;
};

/* ------------------------------------------------------------------ */
/* Shared helpers (used by both sync & async APIs)                    */
/* ------------------------------------------------------------------ */

int vemb_v16_build_combined_key(char *out, size_t out_cap,
                                const char *set_name, const char *elem_name,
                                uint32_t *out_len)
{
    size_t set_len = strlen(set_name);
    size_t elem_len = strlen(elem_name);
    size_t total = set_len + 1 + elem_len;
    if (total >= out_cap) {
        fprintf(stderr, "vemb_v16_client: combined key too long: %zu >= %zu\n",
                total, out_cap);
        return -1;
    }
    memcpy(out, set_name, set_len);
    out[set_len] = '\0';  /* separator */
    memcpy(out + set_len + 1, elem_name, elem_len);
    *out_len = (uint32_t)total;
    return 0;
}

int vemb_v16_open_warm_region(const vemb_v16_channel_desc_t *desc,
                              void **out_mapping_addr,
                              size_t *out_mapping_bytes,
                              void **out_mapped_addr,
                              uint64_t *out_region_bytes)
{
    if (!desc || !desc->vector_region_name[0])
        return -1;

    int fd = -1;
    if (desc->warm_backend_type == VEMB_V16_REGION_LOCAL_SHM) {
        fd = shm_open(desc->vector_region_name, O_RDONLY, 0666);
    } else if (desc->warm_backend_type == VEMB_V16_REGION_UB) {
        fd = open(desc->vector_region_name, O_RDONLY);
    } else {
        return -1;
    }
    if (fd < 0)
        return -1;

    size_t size = desc->warm_region_bytes
        ? (size_t)desc->warm_region_bytes
        : (size_t)desc->vector_stride * desc->max_vectors;

    long page_size = sysconf(_SC_PAGESIZE);
    uint64_t page_mask = (uint64_t)(page_size > 0 ? page_size : 4096) - 1u;
    uint64_t aligned_offset = desc->warm_mmap_offset & ~page_mask;
    size_t offset_delta = (size_t)(desc->warm_mmap_offset - aligned_offset);
    size_t map_size = size + offset_delta;

    void *ptr = mmap(NULL, map_size, PROT_READ, MAP_SHARED, fd,
                     (off_t)aligned_offset);
    close(fd);
    if (ptr == MAP_FAILED)
        return -1;

    if (out_region_bytes) *out_region_bytes = size;
    if (out_mapping_bytes) *out_mapping_bytes = map_size;
    if (out_mapping_addr) *out_mapping_addr = ptr;
    if (out_mapped_addr) *out_mapped_addr = (uint8_t *)ptr + offset_delta;
    return 0;
}

void vemb_v16_close_warm_region(void *mapping_addr, size_t mapping_bytes)
{
    if (mapping_addr) {
        munmap(mapping_addr, mapping_bytes);
    }
}

/* ------------------------------------------------------------------ */
/* Async / Buffer-based serialization API                             */
/* ------------------------------------------------------------------ */

ssize_t vemb_v16_serialize_hello(void *buf, size_t buf_cap,
                                 uint32_t vector_dim, uint32_t flags)
{
    size_t total = sizeof(vemb_v16_net_hdr_t) + sizeof(vemb_v16_alloc_req_t);
    if (buf_cap < total)
        return -1;

    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)buf;
    memset(hdr, 0, sizeof(*hdr));
    hdr->magic = VEMB_V16_MAGIC;
    hdr->version = VEMB_V16_VERSION;
    hdr->type = VEMB_V16_NET_HELLO;
    hdr->payload_len = (uint32_t)sizeof(vemb_v16_alloc_req_t);

    vemb_v16_alloc_req_t *req = (vemb_v16_alloc_req_t *)((char *)buf + sizeof(*hdr));
    memset(req, 0, sizeof(*req));
    req->vector_dim = vector_dim;
    req->flags = flags;

    return (ssize_t)total;
}

ssize_t vemb_v16_serialize_vadd(void *buf, size_t buf_cap,
                                uint64_t channel_id, uint32_t req_id,
                                const char *key, uint32_t key_len,
                                const float *vector, uint32_t dim)
{
    if (!key || key_len == 0 || key_len >= VEMB_V16_MAX_KEY_LEN ||
        !vector || dim == 0 || dim > VEMB_V16_MAX_DIM) {
        return -1;
    }

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_VADD_INLINE;
    req.flags = 0;
    req.req_id = req_id;
    req.channel_id = channel_id;
    req.key_len = key_len;
    memcpy(req.key, key, key_len);
    req.key_hash = vemb_v16_murmur3(req.key, key_len);
    req.dim = dim;
    req.vector_bytes = dim * sizeof(float);
    memcpy(req.vector, vector, dim * sizeof(float));

    uint32_t payload_len = (uint32_t)vemb_v16_req_inline_len(req.vector_bytes);
    size_t total = sizeof(vemb_v16_net_hdr_t) + payload_len;
    if (buf_cap < total)
        return -1;

    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)buf;
    memset(hdr, 0, sizeof(*hdr));
    hdr->magic = VEMB_V16_MAGIC;
    hdr->version = VEMB_V16_VERSION;
    hdr->type = VEMB_V16_NET_REQUEST;
    hdr->payload_len = payload_len;
    hdr->channel_id = channel_id;
    hdr->req_id = req_id;

    memcpy((char *)buf + sizeof(*hdr), &req, payload_len);
    return (ssize_t)total;
}

ssize_t vemb_v16_serialize_vemb(void *buf, size_t buf_cap,
                                uint64_t channel_id, uint32_t req_id,
                                const char *key, uint32_t key_len,
                                uint32_t dim)
{
    if (!key || key_len == 0 || key_len >= VEMB_V16_MAX_KEY_LEN ||
        dim == 0 || dim > VEMB_V16_MAX_DIM) {
        return -1;
    }

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_VEMB_HANDLE;
    req.flags = 0;
    req.req_id = req_id;
    req.channel_id = channel_id;
    req.key_len = key_len;
    memcpy(req.key, key, key_len);
    req.key_hash = vemb_v16_murmur3(req.key, key_len);
    req.dim = dim;
    req.vector_bytes = dim * sizeof(float);

    uint32_t payload_len = (uint32_t)vemb_v16_req_handle_len();
    size_t total = sizeof(vemb_v16_net_hdr_t) + payload_len;
    if (buf_cap < total)
        return -1;

    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)buf;
    memset(hdr, 0, sizeof(*hdr));
    hdr->magic = VEMB_V16_MAGIC;
    hdr->version = VEMB_V16_VERSION;
    hdr->type = VEMB_V16_NET_REQUEST;
    hdr->payload_len = payload_len;
    hdr->channel_id = channel_id;
    hdr->req_id = req_id;

    memcpy((char *)buf + sizeof(*hdr), &req, payload_len);
    return (ssize_t)total;
}

ssize_t vemb_v16_serialize_vsim_inline(void *buf, size_t buf_cap,
                                       uint64_t channel_id, uint32_t req_id,
                                       const char *key, uint32_t key_len,
                                       const float *query_vector, uint32_t dim)
{
    if (!key || key_len == 0 || key_len >= VEMB_V16_MAX_KEY_LEN ||
        !query_vector || dim == 0 || dim > VEMB_V16_MAX_DIM) {
        return -1;
    }

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_VSIM_INLINE;
    req.flags = 0;
    req.req_id = req_id;
    req.channel_id = channel_id;
    req.key_len = key_len;
    memcpy(req.key, key, key_len);
    req.key_hash = vemb_v16_murmur3(req.key, key_len);
    req.dim = dim;
    req.vector_bytes = dim * sizeof(float);
    memcpy(req.vector, query_vector, dim * sizeof(float));

    uint32_t payload_len = (uint32_t)vemb_v16_req_inline_len(req.vector_bytes);
    size_t total = sizeof(vemb_v16_net_hdr_t) + payload_len;
    if (buf_cap < total)
        return -1;

    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)buf;
    memset(hdr, 0, sizeof(*hdr));
    hdr->magic = VEMB_V16_MAGIC;
    hdr->version = VEMB_V16_VERSION;
    hdr->type = VEMB_V16_NET_REQUEST;
    hdr->payload_len = payload_len;
    hdr->channel_id = channel_id;
    hdr->req_id = req_id;

    memcpy((char *)buf + sizeof(*hdr), &req, payload_len);
    return (ssize_t)total;
}

/* ------------------------------------------------------------------ */
/* Internal helpers for sync API                                      */
/* ------------------------------------------------------------------ */

static int sdk_open_warm_region(vemb_v16_client_t *c,
                                const vemb_v16_channel_desc_t *desc)
{
    void *mapping_addr = NULL;
    size_t mapping_bytes = 0;
    void *mapped_addr = NULL;
    uint64_t region_bytes = 0;

    if (vemb_v16_open_warm_region(desc, &mapping_addr, &mapping_bytes,
                                  &mapped_addr, &region_bytes) != 0) {
        return -1;
    }

    c->warm_region_bytes = region_bytes;
    c->mapping_bytes     = mapping_bytes;
    c->mapping_addr      = (uint8_t *)mapping_addr;
    c->mapped_addr       = (uint8_t *)mapped_addr;
    return 0;
}

static void sdk_close_warm_region(vemb_v16_client_t *c)
{
    if (c->mapping_addr) {
        vemb_v16_close_warm_region(c->mapping_addr, c->mapping_bytes);
        c->mapping_addr = NULL;
        c->mapped_addr  = NULL;
    }
}

static int read_response(vemb_v16_client_t *c,
                         vemb_v16_resp_t *resp,
                         uint8_t *inline_vector,
                         uint32_t inline_vector_cap,
                         uint32_t *inline_vector_bytes)
{
    if (inline_vector_bytes) *inline_vector_bytes = 0;

    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(c->fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_RESPONSE ||
        hdr.channel_id != c->channel_id ||
        hdr.payload_len < sizeof(*resp)) {
        return -1;
    }

    if (vemb_v16_net_read_full(c->fd, resp, sizeof(*resp)) != 0)
        return -1;

    uint32_t extra = hdr.payload_len - (uint32_t)sizeof(*resp);
    if (extra > 0 && inline_vector && inline_vector_cap > 0) {
        uint32_t to_read = extra < inline_vector_cap ? extra : inline_vector_cap;
        if (vemb_v16_net_read_full(c->fd, inline_vector, to_read) != 0)
            return -1;
        /* drain remainder if any */
        if (to_read < extra) {
            uint8_t discard[256];
            uint32_t remain = extra - to_read;
            while (remain > 0) {
                uint32_t chunk = remain < sizeof(discard) ? remain : sizeof(discard);
                if (vemb_v16_net_read_full(c->fd, discard, chunk) != 0)
                    return -1;
                remain -= chunk;
            }
        }
        if (inline_vector_bytes) *inline_vector_bytes = to_read;
    } else if (extra > 0) {
        uint8_t discard[256];
        while (extra > 0) {
            uint32_t chunk = extra < sizeof(discard) ? extra : sizeof(discard);
            if (vemb_v16_net_read_full(c->fd, discard, chunk) != 0)
                return -1;
            extra -= chunk;
        }
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* Public Sync API                                                    */
/* ------------------------------------------------------------------ */

vemb_v16_client_t *vemb_v16_client_create(const char *host,
                                          uint16_t port,
                                          uint32_t dim)
{
    if (!host || dim == 0 || dim > VEMB_V16_MAX_DIM)
        return NULL;

    vemb_v16_client_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;

    int fd = -1;
    for (int retry = 0; retry < 50; retry++) {
        fd = vemb_v16_net_connect(host, port, 10000);
        if (fd >= 0) break;
        usleep(100000);
    }
    if (fd < 0) {
        fprintf(stderr, "vemb_v16_client: connect %s:%u failed\n", host, port);
        free(c);
        return NULL;
    }

    char hello_buf[64];
    ssize_t hello_len = vemb_v16_serialize_hello(hello_buf, sizeof(hello_buf),
                                                  dim, 0);
    if (hello_len < 0 || vemb_v16_net_write_full(fd, hello_buf, (size_t)hello_len) != 0) {
        close(fd);
        free(c);
        return NULL;
    }

    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_WELCOME ||
        hdr.payload_len != sizeof(vemb_v16_channel_desc_t)) {
        close(fd);
        free(c);
        return NULL;
    }

    vemb_v16_channel_desc_t desc;
    memset(&desc, 0, sizeof(desc));
    if (vemb_v16_net_read_full(fd, &desc, sizeof(desc)) != 0 ||
        desc.magic != VEMB_V16_MAGIC ||
        desc.version != VEMB_V16_VERSION) {
        close(fd);
        free(c);
        return NULL;
    }

    if (sdk_open_warm_region(c, &desc) != 0) {
        close(fd);
        free(c);
        return NULL;
    }

    c->fd         = fd;
    c->channel_id = desc.channel_id;
    c->dim        = dim;
    c->req_id     = 1;
    strncpy(c->host, host, sizeof(c->host) - 1);
    c->host[sizeof(c->host) - 1] = '\0';
    c->port = port;
    return c;
}

void vemb_v16_client_destroy(vemb_v16_client_t *c)
{
    if (!c) return;
    sdk_close_warm_region(c);
    if (c->fd >= 0) {
        char close_buf[32];
        ssize_t close_len = sizeof(vemb_v16_net_hdr_t);
        memset(close_buf, 0, sizeof(close_buf));
        vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)close_buf;
        hdr->magic = VEMB_V16_MAGIC;
        hdr->version = VEMB_V16_VERSION;
        hdr->type = VEMB_V16_NET_CLOSE_CHANNEL;
        hdr->channel_id = c->channel_id;
        vemb_v16_net_write_full(c->fd, close_buf, (size_t)close_len);
        close(c->fd);
    }
    free(c);
}

int vemb_v16_client_vadd(vemb_v16_client_t *c,
                         const char *set_name,
                         const char *elem_name,
                         const float *vector,
                         uint32_t dim)
{
    if (!c || c->fd < 0 || !set_name || !elem_name || !vector || dim != c->dim)
        return -1;

    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0)
        return -1;

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_VADD_INLINE;
    req.flags = 0;
    req.req_id = c->req_id++;
    req.channel_id = c->channel_id;
    req.key_len = key_len;
    memcpy(req.key, combined, key_len);
    req.key_hash = vemb_v16_murmur3(req.key, key_len);
    req.dim = dim;
    req.vector_bytes = dim * sizeof(float);
    memcpy(req.vector, vector, dim * sizeof(float));

    size_t req_len = vemb_v16_req_inline_len(req.vector_bytes);
    if (vemb_v16_net_write_frame(c->fd,
                                 VEMB_V16_NET_REQUEST,
                                 0,
                                 c->channel_id,
                                 req.req_id,
                                 &req,
                                 (uint32_t)req_len) != 0) {
        return -1;
    }

    vemb_v16_resp_t resp;
    if (read_response(c, &resp, NULL, 0, NULL) != 0)
        return -1;

    if (resp.status == VEMB_V16_STATUS_ERR)
        return -1;
    return 0;
}

int vemb_v16_client_vemb_handle(vemb_v16_client_t *c,
                                const char *set_name,
                                const char *elem_name,
                                uint64_t *out_offset,
                                uint32_t *out_bytes,
                                uint32_t *out_dim,
                                uint32_t *out_region_id)
{
    if (!c || c->fd < 0 || !set_name || !elem_name)
        return -1;

    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0)
        return -1;

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_VEMB_HANDLE;
    req.flags = 0;
    req.req_id = c->req_id++;
    req.channel_id = c->channel_id;
    req.key_len = key_len;
    memcpy(req.key, combined, key_len);
    req.key_hash = vemb_v16_murmur3(req.key, key_len);
    req.dim = c->dim;
    req.vector_bytes = c->dim * sizeof(float);

    size_t req_len = vemb_v16_req_handle_len();
    if (vemb_v16_net_write_frame(c->fd,
                                 VEMB_V16_NET_REQUEST,
                                 0,
                                 c->channel_id,
                                 req.req_id,
                                 &req,
                                 (uint32_t)req_len) != 0) {
        return -1;
    }

    vemb_v16_resp_t resp;
    if (read_response(c, &resp, NULL, 0, NULL) != 0)
        return -1;

    if (resp.status == VEMB_V16_STATUS_NOT_FOUND)
        return 1;
    if (resp.status != VEMB_V16_STATUS_OK)
        return -1;

    if (out_offset)    *out_offset    = resp.vector_offset;
    if (out_bytes)     *out_bytes     = resp.vector_bytes;
    if (out_dim)       *out_dim       = resp.dim > 0 ? resp.dim : c->dim;
    if (out_region_id) *out_region_id = resp.region_id;
    return 0;
}

int vemb_v16_client_vemb_vector(vemb_v16_client_t *c,
                                const char *set_name,
                                const char *elem_name,
                                float *out_vector,
                                uint32_t out_cap,
                                uint32_t *out_dim)
{
    uint64_t offset;
    uint32_t bytes, dim, region_id;
    int rc = vemb_v16_client_vemb_handle(c, set_name, elem_name,
                                         &offset, &bytes, &dim, &region_id);
    if (rc != 0)
        return rc;

    rc = vemb_v16_client_read_vector(c, offset, bytes,
                                     out_vector, out_cap);
    if (rc == 0 && out_dim)
        *out_dim = dim;
    return rc;
}

int vemb_v16_client_vsim(vemb_v16_client_t *c,
                         const char *set_name,
                         const char *elem_name,
                         const float *query_vector,
                         uint32_t dim,
                         float *out_score)
{
    if (!c || c->fd < 0 || !set_name || !elem_name ||
        !query_vector || dim != c->dim || !out_score)
        return -1;

    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0)
        return -1;

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_VSIM_INLINE;
    req.flags = 0;
    req.req_id = c->req_id++;
    req.channel_id = c->channel_id;
    req.key_len = key_len;
    memcpy(req.key, combined, key_len);
    req.key_hash = vemb_v16_murmur3(req.key, key_len);
    req.dim = dim;
    req.vector_bytes = dim * sizeof(float);
    memcpy(req.vector, query_vector, dim * sizeof(float));

    size_t req_len = vemb_v16_req_inline_len(req.vector_bytes);
    if (vemb_v16_net_write_frame(c->fd,
                                 VEMB_V16_NET_REQUEST,
                                 0,
                                 c->channel_id,
                                 req.req_id,
                                 &req,
                                 (uint32_t)req_len) != 0)
        return -1;

    vemb_v16_resp_t resp;
    if (read_response(c, &resp, NULL, 0, NULL) != 0)
        return -1;

    if (resp.status == VEMB_V16_STATUS_NOT_FOUND)
        return 1;
    if (resp.status != VEMB_V16_STATUS_OK)
        return -1;

    *out_score = resp.score;
    return 0;
}

/* ------------------------------------------------------------------ */
/* Pipeline API                                                       */
/* ------------------------------------------------------------------ */

int vemb_v16_client_vadd_pipeline(vemb_v16_client_t *c,
                                  const char **set_names,
                                  const char **elem_names,
                                  const float **vectors,
                                  uint32_t count,
                                  uint32_t max_inflight)
{
    if (!c || c->fd < 0 || count == 0 || !set_names || !elem_names || !vectors)
        return -1;
    if (max_inflight == 0) max_inflight = count; /* unlimited */

    uint32_t vector_bytes = c->dim * sizeof(float);
    size_t req_copy_len = vemb_v16_req_inline_len(vector_bytes);
    uint32_t sent = 0;
    uint32_t received = 0;

    /* Pre-build the invariant part of the request */
    vemb_v16_req_t req_base;
    memset(&req_base, 0, sizeof(req_base));
    req_base.op = VEMB_V16_OP_VADD_INLINE;
    req_base.flags = 0;
    req_base.channel_id = c->channel_id;
    req_base.dim = c->dim;
    req_base.vector_bytes = vector_bytes;

    while (received < count) {
        /* Send burst up to max_inflight */
        while (sent < count && (sent - received) < max_inflight) {
            char combined[VEMB_V16_MAX_KEY_LEN];
            uint32_t key_len;
            if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                            set_names[sent], elem_names[sent],
                                            &key_len) != 0)
                return -1;

            vemb_v16_req_t req;
            memcpy(&req, &req_base, req_copy_len);
            req.req_id = c->req_id++;
            req.key_len = key_len;
            memcpy(req.key, combined, key_len);
            req.key_hash = vemb_v16_murmur3(req.key, key_len);
            memcpy(req.vector, vectors[sent], vector_bytes);

            if (vemb_v16_net_write_frame(c->fd,
                                         VEMB_V16_NET_REQUEST,
                                         0,
                                         c->channel_id,
                                         req.req_id,
                                         &req,
                                         (uint32_t)req_copy_len) != 0) {
                return -1;
            }
            sent++;
        }

        /* Receive one response */
        vemb_v16_resp_t resp;
        if (read_response(c, &resp, NULL, 0, NULL) != 0)
            return -1;
        if (resp.status == VEMB_V16_STATUS_ERR)
            return -1;
        received++;
    }
    return 0;
}

int vemb_v16_client_vemb_pipeline(vemb_v16_client_t *c,
                                  const char **set_names,
                                  const char **elem_names,
                                  uint32_t count,
                                  vemb_v16_pipeline_resp_t *out_resps,
                                  uint32_t max_inflight)
{
    if (!c || c->fd < 0 || count == 0 || !set_names || !elem_names || !out_resps)
        return -1;
    if (max_inflight == 0) max_inflight = count; /* unlimited */

    size_t req_copy_len = vemb_v16_req_handle_len();
    uint32_t sent = 0;
    uint32_t received = 0;

    /* Pre-build the invariant part of the request */
    vemb_v16_req_t req_base;
    memset(&req_base, 0, sizeof(req_base));
    req_base.op = VEMB_V16_OP_VEMB_HANDLE;
    req_base.flags = 0;
    req_base.channel_id = c->channel_id;
    req_base.dim = c->dim;
    req_base.vector_bytes = c->dim * sizeof(float);

    while (received < count) {
        /* Send burst up to max_inflight */
        while (sent < count && (sent - received) < max_inflight) {
            char combined[VEMB_V16_MAX_KEY_LEN];
            uint32_t key_len;
            if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                            set_names[sent], elem_names[sent],
                                            &key_len) != 0)
                return -1;

            vemb_v16_req_t req;
            memcpy(&req, &req_base, req_copy_len);
            req.req_id = c->req_id++;
            req.key_len = key_len;
            memcpy(req.key, combined, key_len);
            req.key_hash = vemb_v16_murmur3(req.key, key_len);

            if (vemb_v16_net_write_frame(c->fd,
                                         VEMB_V16_NET_REQUEST,
                                         0,
                                         c->channel_id,
                                         req.req_id,
                                         &req,
                                         (uint32_t)req_copy_len) != 0) {
                return -1;
            }
            sent++;
        }

        /* Receive one response */
        vemb_v16_resp_t resp;
        vemb_v16_pipeline_resp_t *out = &out_resps[received];
        memset(out, 0, sizeof(*out));

        if (read_response(c, &resp, NULL, 0, NULL) != 0) {
            out->status = -1;
            return -1;
        }

        if (resp.status == VEMB_V16_STATUS_NOT_FOUND) {
            out->status = 1;
        } else if (resp.status != VEMB_V16_STATUS_OK) {
            out->status = -1;
            return -1;
        } else {
            out->status    = 0;
            out->offset    = resp.vector_offset;
            out->bytes     = resp.vector_bytes;
            out->dim       = resp.dim > 0 ? resp.dim : c->dim;
            out->region_id = resp.region_id;
        }
        received++;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* Diagnostics / Control                                              */
/* ------------------------------------------------------------------ */

int vemb_v16_client_ping(vemb_v16_client_t *c)
{
    if (!c || c->fd < 0) return -1;

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_PING;
    req.flags = 0;
    req.req_id = c->req_id++;
    req.channel_id = c->channel_id;

    size_t payload_len = vemb_v16_req_handle_len();
    char buf[2048];
    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)buf;
    memset(hdr, 0, sizeof(*hdr));
    hdr->magic = VEMB_V16_MAGIC;
    hdr->version = VEMB_V16_VERSION;
    hdr->type = VEMB_V16_NET_REQUEST;
    hdr->payload_len = (uint32_t)payload_len;
    hdr->channel_id = c->channel_id;
    hdr->req_id = req.req_id;
    memcpy(buf + sizeof(*hdr), &req, payload_len);

    size_t total = sizeof(*hdr) + payload_len;
    if (vemb_v16_net_write_full(c->fd, buf, total) != 0)
        return -1;

    vemb_v16_resp_t resp;
    if (read_response(c, &resp, NULL, 0, NULL) != 0)
        return -1;

    return (resp.status == VEMB_V16_STATUS_OK) ? 0 : -1;
}

int vemb_v16_client_stats(vemb_v16_client_t *c, vemb_v16_stats_t *out_stats)
{
    if (!c || !out_stats) return -1;

    /* STATS must be sent on a fresh connection (proxy rejects
     * non-REQUEST frames on established channels). */
    int fd = vemb_v16_net_connect(c->host, c->port, 10000);
    if (fd < 0) return -1;

    vemb_v16_net_hdr_t hdr = {0};
    hdr.magic = VEMB_V16_MAGIC;
    hdr.version = VEMB_V16_VERSION;
    hdr.type = VEMB_V16_NET_STATS;
    hdr.payload_len = 0;

    int rc = -1;
    if (vemb_v16_net_write_full(fd, &hdr, sizeof(hdr)) != 0)
        goto out;

    vemb_v16_net_hdr_t resp_hdr;
    if (vemb_v16_net_read_header(fd, &resp_hdr) != 0 ||
        resp_hdr.type != VEMB_V16_NET_STATS ||
        resp_hdr.payload_len != sizeof(vemb_v16_stats_t)) {
        goto out;
    }

    if (vemb_v16_net_read_full(fd, out_stats, sizeof(*out_stats)) != 0)
        goto out;

    rc = 0;
out:
    close(fd);
    return rc;
}

/* ------------------------------------------------------------------ */
/* Internal accessors                                                 */
/* ------------------------------------------------------------------ */

int vemb_v16_client_fd(const vemb_v16_client_t *c)
{
    return c ? c->fd : -1;
}

uint64_t vemb_v16_client_channel_id(const vemb_v16_client_t *c)
{
    return c ? c->channel_id : 0;
}

int vemb_v16_client_read_vector(vemb_v16_client_t *c,
                                uint64_t offset,
                                uint32_t bytes,
                                float *out_vector,
                                uint32_t out_cap)
{
    if (!c || !c->mapped_addr || !out_vector || bytes == 0)
        return -1;

    uint32_t nfloats = bytes / sizeof(float);
    if (nfloats > out_cap) {
        fprintf(stderr, "vemb_v16_client: output buffer too small: %u < %u\n",
                out_cap, nfloats);
        return -1;
    }
    if (offset + bytes > c->warm_region_bytes) {
        fprintf(stderr, "vemb_v16_client: invalid warm region range\n");
        return -1;
    }

    memcpy(out_vector, c->mapped_addr + offset, bytes);
    return 0;
}

/* ------------------------------------------------------------------ */
/* VSIM_INLINE Pipeline                                               */
/* ------------------------------------------------------------------ */

int vemb_v16_client_vsim_pipeline(vemb_v16_client_t *c,
                                  const char **set_names,
                                  const char **elem_names,
                                  const float *query_vector,
                                  uint32_t count,
                                  float *out_scores,
                                  uint32_t max_inflight)
{
    if (!c || c->fd < 0 || count == 0 || !set_names || !elem_names ||
        !query_vector || !out_scores)
        return -1;
    if (max_inflight == 0) max_inflight = count;

    uint32_t vector_bytes = c->dim * sizeof(float);
    size_t req_copy_len = vemb_v16_req_inline_len(vector_bytes);
    uint32_t sent = 0;
    uint32_t received = 0;

    /* Pre-build the invariant part of the request */
    vemb_v16_req_t req_base;
    memset(&req_base, 0, sizeof(req_base));
    req_base.op = VEMB_V16_OP_VSIM_INLINE;
    req_base.channel_id = c->channel_id;
    req_base.dim = c->dim;
    req_base.vector_bytes = vector_bytes;
    memcpy(req_base.vector, query_vector, vector_bytes);

    while (received < count) {
        /* Send burst */
        while (sent < count && (sent - received) < max_inflight) {
            char combined[VEMB_V16_MAX_KEY_LEN];
            uint32_t key_len;
            if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                            set_names[sent], elem_names[sent],
                                            &key_len) != 0)
                return -1;

            vemb_v16_req_t req;
            memcpy(&req, &req_base, req_copy_len);
            req.req_id = c->req_id++;
            req.key_len = key_len;
            memcpy(req.key, combined, key_len);
            req.key_hash = vemb_v16_murmur3(req.key, key_len);

            if (vemb_v16_net_write_frame(c->fd,
                                         VEMB_V16_NET_REQUEST,
                                         0,
                                         c->channel_id,
                                         req.req_id,
                                         &req,
                                         (uint32_t)req_copy_len) != 0)
                return -1;
            sent++;
        }

        /* Receive one response */
        vemb_v16_resp_t resp;
        if (read_response(c, &resp, NULL, 0, NULL) != 0)
            return -1;

        if (resp.status == VEMB_V16_STATUS_NOT_FOUND) {
            out_scores[received] = 0.0f;  /* sentinel for not-found */
        } else if (resp.status != VEMB_V16_STATUS_OK) {
            return -1;
        } else {
            out_scores[received] = resp.score;
        }
        received++;
    }
    return 0;
}

/* ===================================================================== */
/* Convenience helpers                                                   */
/* ===================================================================== */

float *vemb_v16_parse_vector_csv(const char *str, uint32_t expected_dim)
{
    float *vec = malloc(expected_dim * sizeof(float));
    if (!vec) return NULL;

    char *copy = strdup(str);
    if (!copy) { free(vec); return NULL; }

    char *p = copy;
    uint32_t parsed = 0;
    while (parsed < expected_dim) {
        char *end = NULL;
        float v = strtof(p, &end);
        if (end == p) break;
        vec[parsed++] = v;
        if (*end == '\0') break;
        p = end + 1;
    }
    free(copy);

    if (parsed != expected_dim) {
        free(vec);
        return NULL;
    }
    return vec;
}

float *vemb_v16_parse_vector_argv(char **argv, int argc, int start_idx,
                                   uint32_t expected_dim, int *out_consumed)
{
    if (start_idx >= argc) return NULL;

    /* Case 1: single token with comma-separated values */
    if (strchr(argv[start_idx], ',')) {
        float *vec = vemb_v16_parse_vector_csv(argv[start_idx], expected_dim);
        if (vec && out_consumed) *out_consumed = 1;
        return vec;
    }

    /* Case 2: individual float tokens */
    if (start_idx + (int)expected_dim > argc) return NULL;
    float *vec = malloc(expected_dim * sizeof(float));
    if (!vec) return NULL;
    for (uint32_t i = 0; i < expected_dim; i++) {
        char *end = NULL;
        float v = strtof(argv[start_idx + i], &end);
        if (end == argv[start_idx + i] || *end != '\0') {
            free(vec);
            return NULL;
        }
        vec[i] = v;
    }
    if (out_consumed) *out_consumed = (int)expected_dim;
    return vec;
}

int vemb_v16_client_vsim_repeat(vemb_v16_client_t *c,
                                 const char *set_name, const char *elem_name,
                                 const float *query_vector, uint32_t repeat,
                                 float *out_score, int *out_found,
                                 uint32_t max_inflight)
{
    if (!c || !set_name || !elem_name || !query_vector || repeat == 0)
        return -1;

#define REPEAT_BATCH 4096
    const char *batch_sets[REPEAT_BATCH];
    const char *batch_elems[REPEAT_BATCH];
    float batch_scores[REPEAT_BATCH];
    for (int i = 0; i < REPEAT_BATCH; i++) {
        batch_sets[i] = set_name;
        batch_elems[i] = elem_name;
    }

    float last_score = 0.0f;
    int last_found = 0;

    for (uint32_t offset = 0; offset < repeat; offset += REPEAT_BATCH) {
        uint32_t n = (repeat - offset < REPEAT_BATCH)
                     ? (repeat - offset) : REPEAT_BATCH;
        if (vemb_v16_client_vsim_pipeline(c, batch_sets, batch_elems,
                                          query_vector, n, batch_scores,
                                          max_inflight) != 0) {
            return -1;
        }
        last_score = batch_scores[n - 1];
        last_found = (last_score != 0.0f);
    }

    if (out_score) *out_score = last_score;
    if (out_found) *out_found = last_found;
    return 0;
#undef REPEAT_BATCH
}

/* ===================================================================== */
/* SHM Client                                                            */
/* ===================================================================== */

struct vemb_v16_client_shm {
    vemb_v16_client_ring_t *req_ring;
    vemb_v16_client_ring_t *resp_ring;
    vemb_v16_channel_desc_t desc;
    char socket_path[256];
    uint32_t req_id;
    /* warm region (optional) */
    size_t   warm_mapping_bytes;
    uint8_t *warm_mapping_addr;
};

static int shm_connect_uds(const char *path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct timeval tv;
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int shm_write_full(int fd, const void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = write(fd, (const char *)buf + done, n - done);
        if (r <= 0) return -1;
        done += (size_t)r;
    }
    return 0;
}

static int shm_read_full(int fd, void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = read(fd, (char *)buf + done, n - done);
        if (r <= 0) return -1;
        done += (size_t)r;
    }
    return 0;
}

static int shm_open_ring(const char *name, uint32_t slot_size,
                         vemb_v16_client_ring_t **ring) {
    int fd = shm_open(name, O_RDWR, 0666);
    if (fd < 0) return -1;
    size_t bytes = vemb_v16_client_ring_bytes(slot_size);
    void *ptr = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) return -1;
    *ring = ptr;
    return 0;
}

static int shm_send_req(vemb_v16_client_shm_t *c,
                        const vemb_v16_req_t *req, size_t len)
{
    if (!c || !c->req_ring) return -1;
    uint64_t spins = 0;
    while (vemb_v16_client_publish(c->req_ring, req, (uint32_t)len) != 0) {
        spins++;
        if ((spins & 0xffffu) == 0) usleep(1);
        if (spins > 10000000ULL) return -1;
        __asm__ volatile("" ::: "memory");
    }
    return 0;
}

static int shm_recv_resp(vemb_v16_client_shm_t *c, vemb_v16_resp_t *resp)
{
    if (!c || !c->resp_ring) return -1;
    uint64_t polls = 0;
    int got;
    while ((got = vemb_v16_client_poll(c->resp_ring, resp, sizeof(*resp))) <= 0) {
        polls++;
        if ((polls & 0xffffu) == 0) usleep(1);
        if (polls > 10000000ULL) return -1;
        __asm__ volatile("" ::: "memory");
    }
    return got == (int)sizeof(*resp) ? 0 : -1;
}

vemb_v16_client_shm_t *vemb_v16_client_shm_create(const char *socket_path,
                                                   uint32_t vector_dim)
{
    if (!socket_path || !socket_path[0]) return NULL;

    vemb_v16_client_shm_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;

    strncpy(c->socket_path, socket_path, sizeof(c->socket_path) - 1);
    c->socket_path[sizeof(c->socket_path) - 1] = '\0';

    int fd = shm_connect_uds(socket_path);
    if (fd < 0) {
        free(c);
        return NULL;
    }

    uint8_t op = VEMB_V16_CTRL_ALLOC_CHANNEL;
    vemb_v16_alloc_req_t req = {.vector_dim = vector_dim, .flags = 0};
    uint8_t status = VEMB_V16_STATUS_ERR;

    if (shm_write_full(fd, &op, sizeof(op)) != 0 ||
        shm_write_full(fd, &req, sizeof(req)) != 0 ||
        shm_read_full(fd, &status, sizeof(status)) != 0 ||
        status != VEMB_V16_STATUS_OK ||
        shm_read_full(fd, &c->desc, sizeof(c->desc)) != 0) {
        close(fd);
        free(c);
        return NULL;
    }
    close(fd);

    if (c->desc.magic != VEMB_V16_MAGIC || c->desc.version != VEMB_V16_VERSION) {
        free(c);
        return NULL;
    }

    if (shm_open_ring(c->desc.request_ring_name,
                      c->desc.request_ring_slot_size,
                      &c->req_ring) != 0) {
        free(c);
        return NULL;
    }
    if (shm_open_ring(c->desc.response_ring_name,
                      c->desc.response_ring_slot_size,
                      &c->resp_ring) != 0) {
        munmap(c->req_ring, vemb_v16_client_ring_bytes(c->desc.request_ring_slot_size));
        free(c);
        return NULL;
    }

    /* Map warm region if available */
    if (c->desc.vector_region_name[0]) {
        int wfd = shm_open(c->desc.vector_region_name, O_RDONLY, 0666);
        if (wfd >= 0) {
            size_t wbytes = (size_t)c->desc.warm_region_bytes;
            if (wbytes == 0) wbytes = 1;
            void *wptr = mmap(NULL, wbytes, PROT_READ, MAP_SHARED, wfd, 0);
            close(wfd);
            if (wptr != MAP_FAILED) {
                c->warm_mapping_addr = wptr;
                c->warm_mapping_bytes = wbytes;
            }
        }
    }

    return c;
}

void vemb_v16_client_shm_destroy(vemb_v16_client_shm_t *c)
{
    if (!c) return;
    if (c->desc.channel_id && c->socket_path[0]) {
        int fd = shm_connect_uds(c->socket_path);
        if (fd >= 0) {
            uint8_t op = VEMB_V16_CTRL_CLOSE_CHANNEL;
            shm_write_full(fd, &op, sizeof(op));
            shm_write_full(fd, &c->desc.channel_id, sizeof(c->desc.channel_id));
            close(fd);
        }
    }
    if (c->req_ring) {
        munmap(c->req_ring, vemb_v16_client_ring_bytes(c->desc.request_ring_slot_size));
    }
    if (c->warm_mapping_addr) {
        munmap(c->warm_mapping_addr, c->warm_mapping_bytes);
    }
    if (c->resp_ring) {
        munmap(c->resp_ring, vemb_v16_client_ring_bytes(c->desc.response_ring_slot_size));
    }
    free(c);
}

static int vemb_v16_client_shm_do_vadd(vemb_v16_client_shm_t *c,
                                        const char *set_name,
                                        const char *elem_name,
                                        const float *vector, uint32_t dim)
{
    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0) {
        return -1;
    }

    vemb_v16_req_t req;
    memset(&req, 0, sizeof(req));
    req.op = VEMB_V16_OP_VADD_INLINE;
    req.req_id = c->req_id++;
    req.channel_id = c->desc.channel_id;
    req.key_len = key_len;
    req.key_hash = vemb_v16_murmur3(req.key, key_len);
    req.dim = dim;
    req.vector_bytes = dim * sizeof(float);
    memcpy(req.key, combined, key_len);
    memcpy(req.vector, vector, req.vector_bytes);

    size_t req_len = vemb_v16_req_inline_len(req.vector_bytes);
    if (shm_send_req(c, &req, req_len) != 0) return -1;

    vemb_v16_resp_t resp;
    if (shm_recv_resp(c, &resp) != 0) return -1;
    return (resp.status == VEMB_V16_STATUS_OK) ? 0 : -1;
}

int vemb_v16_client_shm_vadd(vemb_v16_client_shm_t *c,
                              const char *set_name, const char *elem_name,
                              const float *vector, uint32_t dim)
{
    if (!c || !set_name || !elem_name || !vector) return -1;
    return vemb_v16_client_shm_do_vadd(c, set_name, elem_name, vector, dim);
}

int vemb_v16_client_shm_vemb(vemb_v16_client_shm_t *c,
                              const char *set_name, const char *elem_name,
                              float *out_vector, uint32_t out_cap,
                              uint32_t *out_dim)
{
    if (!c || !set_name || !elem_name || !out_vector) return -1;

    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0) {
        return -1;
    }

    vemb_v16_req_t req;
    memset(&req, 0, sizeof(req));
    req.op = VEMB_V16_OP_VEMB_HANDLE;
    req.req_id = c->req_id++;
    req.channel_id = c->desc.channel_id;
    req.key_len = key_len;
    req.key_hash = vemb_v16_murmur3(req.key, key_len);
    req.dim = c->desc.vector_dim;
    req.vector_bytes = c->desc.vector_dim * sizeof(float);
    memcpy(req.key, combined, key_len);

    size_t req_len = vemb_v16_req_handle_len();
    if (shm_send_req(c, &req, req_len) != 0) return -1;

    vemb_v16_resp_t resp;
    if (shm_recv_resp(c, &resp) != 0) return -1;

    if (resp.status == VEMB_V16_STATUS_NOT_FOUND) return 1;
    if (resp.status != VEMB_V16_STATUS_OK) return -1;

    uint32_t dim = resp.dim > 0 ? resp.dim : c->desc.vector_dim;
    uint32_t bytes = resp.vector_bytes;
    if (bytes > out_cap * sizeof(float)) bytes = out_cap * sizeof(float);
    if (c->warm_mapping_addr && bytes > 0) {
        uint64_t off = resp.vector_offset;
        uint64_t reg_off = off - c->desc.warm_mmap_offset;
        if (reg_off + bytes <= c->warm_mapping_bytes) {
            memcpy(out_vector, c->warm_mapping_addr + reg_off, bytes);
        }
    }
    if (out_dim) *out_dim = dim;
    return 0;
}

int vemb_v16_client_shm_vsim(vemb_v16_client_shm_t *c,
                              const char *set_name, const char *elem_name,
                              const float *query_vector, uint32_t dim,
                              float *out_score)
{
    if (!c || !set_name || !elem_name || !query_vector || !out_score) return -1;

    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0) {
        return -1;
    }

    vemb_v16_req_t req;
    memset(&req, 0, sizeof(req));
    req.op = VEMB_V16_OP_VSIM_INLINE;
    req.req_id = c->req_id++;
    req.channel_id = c->desc.channel_id;
    req.key_len = key_len;
    req.key_hash = vemb_v16_murmur3(req.key, key_len);
    req.dim = dim;
    req.vector_bytes = dim * sizeof(float);
    memcpy(req.key, combined, key_len);
    memcpy(req.vector, query_vector, req.vector_bytes);

    size_t req_len = vemb_v16_req_inline_len(req.vector_bytes);
    if (shm_send_req(c, &req, req_len) != 0) return -1;

    vemb_v16_resp_t resp;
    if (shm_recv_resp(c, &resp) != 0) return -1;

    if (resp.status == VEMB_V16_STATUS_NOT_FOUND) return 1;
    if (resp.status != VEMB_V16_STATUS_OK) return -1;

    *out_score = resp.score;
    return 0;
}

uint32_t vemb_v16_client_shm_dim(const vemb_v16_client_shm_t *c)
{
    return c ? c->desc.vector_dim : 0;
}

int vemb_v16_client_vemb_repeat(vemb_v16_client_t *c,
                                 const char *set_name, const char *elem_name,
                                 uint32_t repeat, uint32_t max_inflight)
{
    if (!c || !set_name || !elem_name || repeat == 0)
        return -1;

#define REPEAT_BATCH 4096
    const char *batch_sets[REPEAT_BATCH];
    const char *batch_elems[REPEAT_BATCH];
    vemb_v16_pipeline_resp_t batch_resps[REPEAT_BATCH];
    for (int i = 0; i < REPEAT_BATCH; i++) {
        batch_sets[i] = set_name;
        batch_elems[i] = elem_name;
    }

    for (uint32_t offset = 0; offset < repeat; offset += REPEAT_BATCH) {
        uint32_t n = (repeat - offset < REPEAT_BATCH)
                     ? (repeat - offset) : REPEAT_BATCH;
        memset(batch_resps, 0, sizeof(batch_resps[0]) * n);
        if (vemb_v16_client_vemb_pipeline(c, batch_sets, batch_elems,
                                          n, batch_resps, max_inflight) != 0) {
            return -1;
        }
    }
    return 0;
#undef REPEAT_BATCH
}

int vemb_v16_client_vadd_repeat(vemb_v16_client_t *c,
                                 const char *set_name, const char *elem_name,
                                 const float *vector, uint32_t repeat,
                                 uint32_t max_inflight)
{
    if (!c || !set_name || !elem_name || !vector || repeat == 0)
        return -1;

#define REPEAT_BATCH 4096
    const char *batch_sets[REPEAT_BATCH];
    const char *batch_elems[REPEAT_BATCH];
    const float *batch_vectors[REPEAT_BATCH];
    for (int i = 0; i < REPEAT_BATCH; i++) {
        batch_sets[i] = set_name;
        batch_elems[i] = elem_name;
        batch_vectors[i] = vector;
    }

    for (uint32_t offset = 0; offset < repeat; offset += REPEAT_BATCH) {
        uint32_t n = (repeat - offset < REPEAT_BATCH)
                     ? (repeat - offset) : REPEAT_BATCH;
        if (vemb_v16_client_vadd_pipeline(c, batch_sets, batch_elems,
                                          batch_vectors, n, max_inflight) != 0) {
            return -1;
        }
    }
    return 0;
#undef REPEAT_BATCH
}
