#define _GNU_SOURCE

#include "vemb_v16_client_sdk.h"
#include "../../src/vemb_v16_net.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <errno.h>

#define VEMB_V16_SDK_MAX_ENDPOINTS 16
/* Must match benchmark/vemb_v16_bench.c VEMB_V16_BENCH_HASH_VNODES so that
 * multi-endpoint routing stays interoperable across tools (a set filled by
 * redis-cli is visible to vemb_v16_bench / memtier). */
#define VEMB_V16_SDK_HASH_VNODES  10

typedef struct {
    int      fd;
    uint64_t channel_id;
    char     host[64];
    uint16_t port;
    /* warm region (mmap'd once per backend at connect) */
    uint64_t warm_region_bytes;
    uint64_t warm_mmap_offset;
    size_t   mapping_bytes;
    uint8_t *mapping_addr;
    uint8_t *mapped_addr;
} sdk_backend_t;

typedef struct {
    uint32_t hash_value;
    uint32_t backend_idx;
} sdk_hash_node_t;

struct vemb_v16_client {
    /* Routed view — fields below mirror backends[cur_idx] and are kept in
     * sync by sdk_route(). All v*_free functions read these instead of
     * touching backends[] directly, so they need no changes once routing
     * has selected the right backend. */
    int      fd;
    uint64_t channel_id;
    char     host[64];
    uint16_t port;
    uint64_t warm_region_bytes;
    uint64_t warm_mmap_offset;
    size_t   mapping_bytes;
    uint8_t *mapping_addr;
    uint8_t *mapped_addr;

    uint32_t dim;
    uint32_t req_id;

    /* Multi-endpoint support. backend_count==1 means single-endpoint mode
     * (backwards compatible); the hash ring is only built when >1. */
    int             backend_count;
    int             cur_idx;
    sdk_backend_t   backends[VEMB_V16_SDK_MAX_ENDPOINTS];
    sdk_hash_node_t hash_ring[VEMB_V16_SDK_MAX_ENDPOINTS * VEMB_V16_SDK_HASH_VNODES];
    uint32_t        hash_node_count;
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

    if (desc->warm_backend_type != VEMB_V16_REGION_UB)
        return -1;
    int fd = open(desc->vector_region_name, O_RDWR | O_SYNC);
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

    void *ptr = mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
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

static int sdk_open_warm_region(sdk_backend_t *b,
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

    b->warm_region_bytes = region_bytes;
    b->mapping_bytes     = mapping_bytes;
    b->mapping_addr      = (uint8_t *)mapping_addr;
    b->mapped_addr       = (uint8_t *)mapped_addr;
    return 0;
}

static void sdk_close_warm_region(sdk_backend_t *b)
{
    if (b->mapping_addr) {
        vemb_v16_close_warm_region(b->mapping_addr, b->mapping_bytes);
        b->mapping_addr = NULL;
        b->mapped_addr  = NULL;
    }
}

/* Connect one backend: TCP connect, HELLO/WELCOME, mmap warm region. */
static int sdk_connect_backend(sdk_backend_t *b,
                               const char *host, uint16_t port,
                               uint32_t dim)
{
    int fd = -1;
    for (int retry = 0; retry < 50; retry++) {
        fd = vemb_v16_net_connect(host, port, 10000);
        if (fd >= 0) break;
        usleep(100000);
    }
    if (fd < 0) {
        fprintf(stderr, "vemb_v16_client: connect %s:%u failed\n", host, port);
        return -1;
    }

    char hello_buf[64];
    ssize_t hello_len = vemb_v16_serialize_hello(hello_buf, sizeof(hello_buf),
                                                  dim, 0);
    if (hello_len < 0 ||
        vemb_v16_net_write_full(fd, hello_buf, (size_t)hello_len) != 0) {
        close(fd); return -1;
    }

    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_WELCOME ||
        hdr.payload_len != sizeof(vemb_v16_channel_desc_t)) {
        close(fd); return -1;
    }

    vemb_v16_channel_desc_t desc;
    memset(&desc, 0, sizeof(desc));
    if (vemb_v16_net_read_full(fd, &desc, sizeof(desc)) != 0 ||
        desc.magic != VEMB_V16_MAGIC ||
        desc.version != VEMB_V16_VERSION) {
        close(fd); return -1;
    }

    if (sdk_open_warm_region(b, &desc) != 0) {
        close(fd); return -1;
    }

    b->fd         = fd;
    b->channel_id = desc.channel_id;
    strncpy(b->host, host, sizeof(b->host) - 1);
    b->host[sizeof(b->host) - 1] = '\0';
    b->port = port;
    return 0;
}

static void sdk_close_backend(sdk_backend_t *b)
{
    sdk_close_warm_region(b);
    if (b->fd >= 0) {
        char close_buf[32];
        memset(close_buf, 0, sizeof(close_buf));
        vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)close_buf;
        hdr->magic = VEMB_V16_MAGIC;
        hdr->version = VEMB_V16_VERSION;
        hdr->type = VEMB_V16_NET_CLOSE_CHANNEL;
        hdr->channel_id = b->channel_id;
        vemb_v16_net_write_full(b->fd, close_buf, sizeof(vemb_v16_net_hdr_t));
        close(b->fd);
        b->fd = -1;
    }
}

/* ---- Consistent-hash ring (private reimplementation, aligned with
 *      benchmark/vemb_v16_bench.c so routing decisions interoperate) ---- */

static int sdk_hash_node_cmp(const void *a, const void *b)
{
    const sdk_hash_node_t *ha = a;
    const sdk_hash_node_t *hb = b;
    if (ha->hash_value < hb->hash_value) return -1;
    if (ha->hash_value > hb->hash_value) return 1;
    if (ha->backend_idx < hb->backend_idx) return -1;
    if (ha->backend_idx > hb->backend_idx) return 1;
    return 0;
}

static void sdk_build_hash_ring(vemb_v16_client_t *c)
{
    if (!c || c->backend_count <= 1) { c->hash_node_count = 0; return; }
    c->hash_node_count = 0;
    for (uint32_t node = 0; node < (uint32_t)c->backend_count; node++) {
        for (uint32_t vnode = 0; vnode < VEMB_V16_SDK_HASH_VNODES; vnode++) {
            char vnode_key[64];
            uint32_t vnode_id = c->hash_node_count;
            snprintf(vnode_key, sizeof(vnode_key),
                     "supernode_%u_vnode_%u", node, vnode_id);
            c->hash_ring[c->hash_node_count++] = (sdk_hash_node_t){
                .hash_value  = vemb_v16_murmur3(vnode_key, strlen(vnode_key)),
                .backend_idx = node,
            };
        }
    }
    qsort(c->hash_ring, c->hash_node_count, sizeof(c->hash_ring[0]),
          sdk_hash_node_cmp);
}

/* Pick backend for key and sync the routed view. Pass NULL/"" for the
 * canonical "first backend" (used by ping/stats/etc). */
static int sdk_route(vemb_v16_client_t *c, const char *key)
{
    if (!c || c->backend_count <= 0) return -1;
    int idx = 0;
    if (c->backend_count > 1 && key && key[0]) {
        uint32_t hash = vemb_v16_murmur3(key, strlen(key));
        uint32_t left = 0, right = c->hash_node_count;
        while (left < right) {
            uint32_t mid = left + (right - left) / 2;
            if (c->hash_ring[mid].hash_value < hash) left = mid + 1;
            else right = mid;
        }
        if (left >= c->hash_node_count) left = 0;
        idx = (int)c->hash_ring[left].backend_idx;
    }
    sdk_backend_t *b = &c->backends[idx];
    c->fd                = b->fd;
    c->channel_id        = b->channel_id;
    memcpy(c->host, b->host, sizeof(c->host));
    c->port              = b->port;
    c->warm_region_bytes = b->warm_region_bytes;
    c->warm_mmap_offset  = b->warm_mmap_offset;
    c->mapping_bytes     = b->mapping_bytes;
    c->mapping_addr      = b->mapping_addr;
    c->mapped_addr       = b->mapped_addr;
    c->cur_idx           = idx;
    return 0;
}

/* Parse "host:port" — last ':' wins (IPv6 friendly enough for our use). */
static int sdk_parse_endpoint(const char *s, char *host, size_t host_cap,
                              uint16_t *port)
{
    const char *colon = strrchr(s, ':');
    if (!colon || colon == s) return -1;
    size_t host_len = (size_t)(colon - s);
    if (host_len >= host_cap) return -1;
    memcpy(host, s, host_len);
    host[host_len] = '\0';
    char *end = NULL;
    long p = strtol(colon + 1, &end, 10);
    if (end == colon + 1 || *end != '\0' || p <= 0 || p > 65535) return -1;
    *port = (uint16_t)p;
    return 0;
}

static int read_response(vemb_v16_client_t *c,
                         vemb_v16_resp_t *resp,
                         uint8_t *inline_vector,
                         uint32_t inline_vector_cap,
                         uint32_t *inline_vector_bytes)
{
    if (inline_vector_bytes) *inline_vector_bytes = 0;

    /* Read header + response body in one syscall. */
    char hdr_resp_buf[sizeof(vemb_v16_net_hdr_t) + sizeof(vemb_v16_resp_t)];
    if (vemb_v16_net_read_full(c->fd, hdr_resp_buf, sizeof(hdr_resp_buf)) != 0)
        return -1;

    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)hdr_resp_buf;
    if (hdr->magic != VEMB_V16_MAGIC || hdr->version != VEMB_V16_VERSION ||
        hdr->type != VEMB_V16_NET_RESPONSE ||
        hdr->channel_id != c->channel_id ||
        hdr->payload_len < sizeof(*resp)) {
        return -1;
    }

    memcpy(resp, hdr_resp_buf + sizeof(*hdr), sizeof(*resp));

    uint32_t extra = hdr->payload_len - (uint32_t)sizeof(*resp);
    if (extra > 0 && inline_vector && inline_vector_cap > 0) {
        uint32_t to_read = extra < inline_vector_cap ? extra : inline_vector_cap;
        if (vemb_v16_net_read_full(c->fd, inline_vector, to_read) != 0)
            return -1;
        /* drain remainder if any */
        if (to_read < extra) {
            uint8_t discard[8192];
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
        uint8_t discard[8192];
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

vemb_v16_client_t *vemb_v16_client_create_multi(const char *endpoints[],
                                                 int endpoint_count,
                                                 uint32_t dim)
{
    if (!endpoints || endpoint_count <= 0 ||
        endpoint_count > VEMB_V16_SDK_MAX_ENDPOINTS ||
        dim == 0 || dim > VEMB_V16_MAX_DIM)
        return NULL;

    vemb_v16_client_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;
    c->dim           = dim;
    c->req_id        = 1;
    c->backend_count = endpoint_count;

    for (int i = 0; i < endpoint_count; i++) {
        char host[64]; uint16_t port;
        if (sdk_parse_endpoint(endpoints[i], host, sizeof(host), &port) != 0) {
            fprintf(stderr, "vemb_v16_client: bad endpoint '%s'\n", endpoints[i]);
            for (int j = 0; j < i; j++) sdk_close_backend(&c->backends[j]);
            free(c); return NULL;
        }
        if (sdk_connect_backend(&c->backends[i], host, port, dim) != 0) {
            for (int j = 0; j < i; j++) sdk_close_backend(&c->backends[j]);
            free(c); return NULL;
        }
    }

    sdk_build_hash_ring(c);
    sdk_route(c, NULL);  /* sync view to backend[0] */
    return c;
}

vemb_v16_client_t *vemb_v16_client_create(const char *host,
                                          uint16_t port,
                                          uint32_t dim)
{
    if (!host || dim == 0 || dim > VEMB_V16_MAX_DIM) return NULL;
    char ep[80];
    snprintf(ep, sizeof(ep), "%s:%u", host, port);
    const char *endpoints[1] = { ep };
    return vemb_v16_client_create_multi(endpoints, 1, dim);
}

void vemb_v16_client_destroy(vemb_v16_client_t *c)
{
    if (!c) return;
    for (int i = 0; i < c->backend_count; i++)
        sdk_close_backend(&c->backends[i]);
    free(c);
}

int vemb_v16_client_vadd(vemb_v16_client_t *c,
                         const char *set_name,
                         const char *elem_name,
                         const float *vector,
                         uint32_t dim)
{
    if (!c || !set_name || !elem_name || !vector || dim != c->dim)
        return -1;
    if (sdk_route(c, set_name) != 0 || c->fd < 0)
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
    if (!c || !set_name || !elem_name)
        return -1;
    if (sdk_route(c, set_name) != 0 || c->fd < 0)
        return -1;

    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0)
        return -1;

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_VEMB_HANDLE;
    req.flags = 0;  /* handle-only: caller reads vector via mmap */
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
    if (!c || !set_name || !elem_name || !out_vector || out_cap == 0)
        return -1;
    if (sdk_route(c, set_name) != 0 || c->fd < 0)
        return -1;

    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0)
        return -1;

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_VEMB_HANDLE;
    req.flags = VEMB_V16_REQ_F_INLINE_VECTOR;
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
    uint32_t inline_bytes = 0;
    uint32_t inline_cap_bytes = out_cap * sizeof(float);
    if (read_response(c, &resp,
                      (uint8_t *)out_vector,
                      inline_cap_bytes,
                      &inline_bytes) != 0) {
        return -1;
    }

    if (resp.status == VEMB_V16_STATUS_NOT_FOUND)
        return 1;
    if (resp.status != VEMB_V16_STATUS_OK)
        return -1;

    uint32_t expected_bytes = resp.vector_bytes;
    if (inline_bytes != expected_bytes ||
        (expected_bytes / sizeof(float)) > out_cap) {
        return -1;
    }
    if (out_dim)
        *out_dim = resp.dim > 0 ? resp.dim : c->dim;
    return 0;
}

int vemb_v16_client_vsim(vemb_v16_client_t *c,
                         const char *set_name,
                         const char *elem_name,
                         const float *query_vector,
                         uint32_t dim,
                         float *out_score)
{
    if (!c || !set_name || !elem_name ||
        !query_vector || dim != c->dim || !out_score)
        return -1;
    if (sdk_route(c, set_name) != 0 || c->fd < 0)
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
    if (!c || count == 0 || !set_names || !elem_names || !vectors)
        return -1;
    /* Multi-endpoint: pipeline targets the backend picked by set_names[0].
     * Caller must group by backend if keys span endpoints. */
    if (sdk_route(c, set_names[0]) != 0 || c->fd < 0)
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
    if (!c || count == 0 || !set_names || !elem_names || !out_resps)
        return -1;
    /* Multi-endpoint: pipeline targets the backend picked by set_names[0]. */
    if (sdk_route(c, set_names[0]) != 0 || c->fd < 0)
        return -1;
    if (max_inflight == 0) max_inflight = count; /* unlimited */

    size_t req_copy_len = vemb_v16_req_handle_len();
    uint32_t sent = 0;
    uint32_t received = 0;

    /* Pre-build the invariant part of the request */
    vemb_v16_req_t req_base;
    memset(&req_base, 0, sizeof(req_base));
    req_base.op = VEMB_V16_OP_VEMB_HANDLE;
    req_base.flags = VEMB_V16_REQ_F_INLINE_VECTOR;
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
    if (!c) return -1;
    if (sdk_route(c, NULL) != 0 || c->fd < 0) return -1;

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
    /* STATS opens a fresh control connection. In multi-endpoint mode it
     * queries the first backend (NULL key routes to backend[0]). */
    if (sdk_route(c, NULL) != 0) return -1;

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
    if (!c || count == 0 || !set_names || !elem_names ||
        !query_vector || !out_scores)
        return -1;
    /* Multi-endpoint: pipeline targets the backend picked by set_names[0]. */
    if (sdk_route(c, set_names[0]) != 0 || c->fd < 0)
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
