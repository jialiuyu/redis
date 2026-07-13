#define _GNU_SOURCE

#include "../src/vemb_v16_net.h"
#include "../src/vemb_v16_protocol.h"
#include "../src/vemb_v16_topology.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct verify_cfg {
    const char *host;
    uint16_t port;
    uint32_t dim;
    uint32_t prefill;
    uint32_t sample_count;
    uint32_t target_owner;
    uint32_t vnode_count;
    uint32_t timeout_ms;
    uint32_t owner_count;
    uint32_t owners[VEMB_V16_TOPOLOGY_MAX_OWNERS];
} verify_cfg_t;

static void fill_vector(float *vector, uint32_t dim, uint32_t seed) {
    for (uint32_t i = 0; i < dim; i++)
        vector[i] = (float)((seed + i) & 1023u) / 1024.0f;
}

static void make_key(char *buf, size_t len, uint32_t id) {
    snprintf(buf, len, "item:%u", id);
}

static int alloc_tcp_channel(const verify_cfg_t *cfg,
                             vemb_v16_channel_desc_t *desc,
                             int *net_fd) {
    int fd = vemb_v16_net_connect(cfg->host, cfg->port, cfg->timeout_ms);
    if (fd < 0)
        return -1;

    vemb_v16_alloc_req_t req = {.vector_dim = cfg->dim};
    uint8_t req_buf[8];
    size_t encoded_len = 0;
    if (vemb_v16_alloc_req_encode(req_buf,
                                  sizeof(req_buf),
                                  &req,
                                  &encoded_len) != 0) {
        close(fd);
        return -1;
    }

    if (vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_HELLO,
                                 0,
                                 0,
                                 0,
                                 req_buf,
                                 (uint32_t)encoded_len) != 0) {
        close(fd);
        return -1;
    }

    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_WELCOME ||
        hdr.flags != 0) {
        close(fd);
        return -1;
    }

    uint8_t *desc_buf = malloc(hdr.payload_len);
    if (!desc_buf) {
        close(fd);
        return -1;
    }
    int rc = 0;
    if (vemb_v16_net_read_full(fd, desc_buf, hdr.payload_len) != 0 ||
        vemb_v16_channel_desc_decode(desc, desc_buf, hdr.payload_len) != 0 ||
        desc->magic != VEMB_V16_MAGIC ||
        desc->version != VEMB_V16_VERSION) {
        rc = -1;
    }
    free(desc_buf);
    if (rc != 0) {
        close(fd);
        return -1;
    }

    *net_fd = fd;
    return 0;
}

static int read_inline_vector(int net_fd,
                              const vemb_v16_channel_desc_t *desc,
                              const char *key,
                              uint32_t dim,
                              uint32_t timeout_ms,
                              uint32_t req_id,
                              float *vector_out) {
    vemb_v16_req_t req;
    memset(&req, 0, sizeof(req));
    req.op = VEMB_V16_OP_VEMB_INLINE;
    req.req_id = req_id;
    req.channel_id = desc->channel_id;
    req.key_len = (uint32_t)strlen(key);
    req.key_hash = vemb_v16_xxh3_64_str(key, req.key_len);
    req.dim = dim;
    req.vector_bytes = dim * sizeof(float);
    memcpy(req.key, key, req.key_len);

    size_t req_len = vemb_v16_req_encoded_len(&req);
    uint8_t req_buf[512];
    if (req_len > sizeof(req_buf) ||
        vemb_v16_req_encode(req_buf, sizeof(req_buf), &req, &req_len) != 0) {
        return -1;
    }

    if (vemb_v16_net_write_frame(net_fd,
                                 VEMB_V16_NET_REQUEST,
                                 0,
                                 desc->channel_id,
                                 req.req_id,
                                 req_buf,
                                 (uint32_t)req_len) != 0) {
        return -1;
    }

    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(net_fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_RESPONSE ||
        hdr.channel_id != desc->channel_id ||
        hdr.req_id != req.req_id ||
        hdr.flags != 0) {
        return -1;
    }

    uint8_t resp_buf[64];
    uint32_t base_bytes = (uint32_t)vemb_v16_resp_encoded_base_len();
    if (hdr.payload_len < base_bytes ||
        vemb_v16_net_read_full(net_fd, resp_buf, base_bytes) != 0) {
        return -1;
    }

    size_t resp_bytes = vemb_v16_resp_encoded_len_for_fields(
        resp_buf[0],
        (uint8_t)(resp_buf[1] & VEMB_V16_TCP_RESP_OP_MASK));
    if (resp_bytes > sizeof(resp_buf) ||
        hdr.payload_len < resp_bytes ||
        vemb_v16_net_read_full(net_fd,
                               resp_buf + base_bytes,
                               resp_bytes - base_bytes) != 0) {
        return -1;
    }

    vemb_v16_resp_t resp;
    if (vemb_v16_resp_decode(&resp, resp_buf, resp_bytes) != 0)
        return -1;
    if (resp.status != VEMB_V16_STATUS_OK || resp.op != VEMB_V16_OP_VEMB_INLINE)
        return -1;

    uint32_t inline_bytes = hdr.payload_len - (uint32_t)resp_bytes;
    uint32_t expected_bytes = dim * sizeof(float);
    if (inline_bytes != expected_bytes || resp.vector_bytes != expected_bytes)
        return -1;
    if (vemb_v16_net_read_full(net_fd, vector_out, inline_bytes) != 0)
        return -1;

    (void)timeout_ms;
    return 0;
}

static int parse_u32_arg(const char *arg, uint32_t *out) {
    char *end = NULL;
    unsigned long value = strtoul(arg, &end, 10);
    if (!arg || !arg[0] || !end || *end != '\0' || value > UINT32_MAX)
        return -1;
    *out = (uint32_t)value;
    return 0;
}

static int parse_u16_arg(const char *arg, uint16_t *out) {
    uint32_t value = 0;
    if (parse_u32_arg(arg, &value) != 0 || value == 0 || value > UINT16_MAX)
        return -1;
    *out = (uint16_t)value;
    return 0;
}

static int parse_owner_list(const char *arg,
                            uint32_t *owners,
                            uint32_t *owner_count) {
    const char *p = arg;
    uint32_t count = 0;
    if (!arg || !arg[0] || !owners || !owner_count)
        return -1;
    while (*p) {
        if (count >= VEMB_V16_TOPOLOGY_MAX_OWNERS)
            return -1;
        char *end = NULL;
        unsigned long value = strtoul(p, &end, 10);
        if (!end || end == p || value > UINT32_MAX)
            return -1;
        owners[count++] = (uint32_t)value;
        if (*end == '\0')
            break;
        if (*end != ',')
            return -1;
        p = end + 1;
        if (!*p)
            return -1;
    }
    *owner_count = count;
    return count > 0 ? 0 : -1;
}

static int parse_args(int argc, char **argv, verify_cfg_t *cfg) {
    *cfg = (verify_cfg_t){
        .host = VEMB_V16_TCP_HOST,
        .port = VEMB_V16_TCP_PORT,
        .dim = 16,
        .prefill = 1024,
        .sample_count = 16,
        .target_owner = 1,
        .vnode_count = VEMB_V16_TOPOLOGY_DEFAULT_VNODES,
        .timeout_ms = 10000,
        .owner_count = 0,
    };

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--host") && i + 1 < argc) {
            cfg->host = argv[++i];
        } else if (!strcmp(argv[i], "--port") && i + 1 < argc) {
            if (parse_u16_arg(argv[++i], &cfg->port) != 0)
                return -1;
        } else if (!strcmp(argv[i], "--dim") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg->dim) != 0)
                return -1;
        } else if (!strcmp(argv[i], "--prefill") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg->prefill) != 0)
                return -1;
        } else if (!strcmp(argv[i], "--sample-count") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg->sample_count) != 0)
                return -1;
        } else if (!strcmp(argv[i], "--target-owner") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg->target_owner) != 0)
                return -1;
        } else if (!strcmp(argv[i], "--owners") && i + 1 < argc) {
            if (parse_owner_list(argv[++i], cfg->owners, &cfg->owner_count) != 0)
                return -1;
        } else if (!strcmp(argv[i], "--vnode-count") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg->vnode_count) != 0)
                return -1;
        } else if (!strcmp(argv[i], "--timeout-ms") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg->timeout_ms) != 0)
                return -1;
        } else if (!strcmp(argv[i], "--help")) {
            printf("usage: %s [--host HOST] [--port PORT] [--dim N] [--prefill N] [--sample-count N] [--target-owner N] [--owners 0,1,2] [--vnode-count N] [--timeout-ms N]\n",
                   argv[0]);
            return 2;
        } else {
            return -1;
        }
    }

    if (cfg->owner_count == 0) {
        cfg->owners[0] = 0;
        cfg->owners[1] = cfg->target_owner;
        cfg->owner_count = cfg->target_owner == 0 ? 1 : 2;
    }

    return cfg->dim > 0 && cfg->prefill > 0 && cfg->sample_count > 0 ? 0 : -1;
}

int main(int argc, char **argv) {
    verify_cfg_t cfg;
    int parse_rc = parse_args(argc, argv, &cfg);
    if (parse_rc == 2)
        return 0;
    if (parse_rc != 0) {
        fprintf(stderr, "invalid arguments, use --help\n");
        return 1;
    }

    vemb_v16_topology_ring_t ring;
    if (vemb_v16_topology_ring_build(&ring,
                                     1,
                                     cfg.owners,
                                     cfg.owner_count,
                                     cfg.vnode_count) != 0) {
        fprintf(stderr, "failed to build topology ring\n");
        return 1;
    }

    vemb_v16_channel_desc_t desc;
    int net_fd = -1;
    if (alloc_tcp_channel(&cfg, &desc, &net_fd) != 0) {
        fprintf(stderr, "failed to allocate tcp channel host=%s port=%u\n",
                cfg.host,
                cfg.port);
        return 1;
    }

    float *actual = malloc(cfg.dim * sizeof(float));
    float *expected = malloc(cfg.dim * sizeof(float));
    if (!actual || !expected) {
        fprintf(stderr, "allocation failed\n");
        close(net_fd);
        free(actual);
        free(expected);
        return 1;
    }

    uint32_t candidate_count = 0;
    for (uint32_t key_id = 0; key_id < cfg.prefill; key_id++) {
        char key[VEMB_V16_MAX_KEY_LEN];
        make_key(key, sizeof(key), key_id);
        uint32_t owner =
            vemb_v16_topology_ring_owner(&ring,
                                         vemb_v16_xxh3_64_str(key, strlen(key)));
        if (owner == cfg.target_owner)
            candidate_count++;
    }
    if (candidate_count == 0) {
        fprintf(stderr,
                "no migrated keys found for target_owner=%u within prefill=%u\n",
                cfg.target_owner,
                cfg.prefill);
        close(net_fd);
        free(actual);
        free(expected);
        return 1;
    }

    uint32_t verify_goal = cfg.sample_count < candidate_count ?
        cfg.sample_count :
        candidate_count;
    uint32_t verified = 0;
    for (uint32_t key_id = 0; key_id < cfg.prefill; key_id++) {
        char key[VEMB_V16_MAX_KEY_LEN];
        make_key(key, sizeof(key), key_id);
        uint32_t owner =
            vemb_v16_topology_ring_owner(&ring,
                                         vemb_v16_xxh3_64_str(key, strlen(key)));
        if (owner != cfg.target_owner)
            continue;

        if (read_inline_vector(net_fd,
                               &desc,
                               key,
                               cfg.dim,
                               cfg.timeout_ms,
                               verified + 1,
                               actual) != 0) {
            fprintf(stderr, "read verify failed for key=%s target_owner=%u\n",
                    key,
                    cfg.target_owner);
            close(net_fd);
            free(actual);
            free(expected);
            return 1;
        }

        fill_vector(expected, cfg.dim, key_id);
        if (memcmp(actual, expected, cfg.dim * sizeof(float)) != 0) {
            fprintf(stderr,
                    "value mismatch for key=%s target_owner=%u dim=%u\n",
                    key,
                    cfg.target_owner,
                    cfg.dim);
            close(net_fd);
            free(actual);
            free(expected);
            return 1;
        }

        printf("[verify] key=%s owner=%u ok\n", key, owner);
        verified++;
        if (verified >= verify_goal)
            break;
    }

    close(net_fd);
    free(actual);
    free(expected);

    if (verified < verify_goal) {
        fprintf(stderr,
                "not enough migrated keys found: verified=%u verify_goal=%u prefill=%u\n",
                verified,
                verify_goal,
                cfg.prefill);
        return 1;
    }

    printf("[ok] verified=%u/%u migrated keys on owner=%u host=%s port=%u\n",
           verified,
           candidate_count,
           cfg.target_owner,
           cfg.host,
           cfg.port);
    return 0;
}
