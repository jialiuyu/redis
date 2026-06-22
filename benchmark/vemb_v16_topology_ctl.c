#define _GNU_SOURCE

#include "../src/vemb_v16_net.h"
#include "../src/vemb_v16_migration_outbox.h"
#include "../src/vemb_v16_protocol.h"
#include "../src/vemb_v16_topology.h"

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

typedef enum topology_ctl_action {
    TOPOLOGY_CTL_GET = 0,
    TOPOLOGY_CTL_SET = 1,
    TOPOLOGY_CTL_RANGE_BARRIER = 2,
    TOPOLOGY_CTL_RANGE_CUTOVER = 3,
    TOPOLOGY_CTL_RANGE_WAIT_READY = 4,
    TOPOLOGY_CTL_RANGE_WAIT_CUTOVER = 5,
    TOPOLOGY_CTL_RANGE_SOURCE_GC = 6,
    TOPOLOGY_CTL_COORDINATOR_LISTEN = 7,
} topology_ctl_action_t;

typedef struct topology_ctl_cfg {
    uint32_t transport_type;
    const char *socket_path;
    const char *host;
    uint16_t port;
    uint32_t timeout_ms;
    topology_ctl_action_t action;
    int has_epoch;
    int has_min_write_epoch;
    uint64_t current_topology_epoch;
    uint64_t min_write_epoch;
    uint32_t flags;
    uint32_t vnode_count;
    uint32_t active_owners[VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS];
    uint32_t standby_owners[VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS];
    uint32_t active_owner_count;
    uint32_t standby_owner_count;
    int has_migration_epoch;
    int has_cutover_epoch;
    uint64_t migration_topology_epoch;
    uint64_t cutover_topology_epoch;
    uint32_t target_owner;
    uint32_t shard_id;
    uint32_t page_limit;
    uint32_t wait_ms;
    uint32_t poll_ms;
    uint32_t endpoint_count;
    vemb_v16_topology_endpoint_t
        endpoints[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
    uint32_t coordinator_endpoint_valid;
    vemb_v16_topology_endpoint_t coordinator_endpoint;
    uint32_t expected_source_count;
    uint32_t expected_sources[VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS];
} topology_ctl_cfg_t;

static void usage(const char *prog) {
    fprintf(stderr,
            "usage: %s --get|--set [--transport tcp|aeron] "
            "[--host HOST --port PORT | --socket PATH] "
            "[--epoch N] [--min-write-epoch N] "
            "[--active 0,1] [--standby 0,1,2] [--dual-write] "
            "[--auto-scaleout] [--coordinated-scaleout] "
            "[--owner-endpoints 0=HOST:PORT,1=HOST:PORT] "
            "[--owner-sockets 0=PATH,1=PATH] "
            "[--coordinator-endpoint HOST:PORT | --coordinator-socket PATH] "
            "[--vnode-count N] [--timeout-ms N]\n"
            "       %s --range-barrier|--range-cutover|--range-source-gc|--range-wait-ready|--range-wait-cutover "
            "[--transport tcp|aeron] [--host HOST --port PORT | --socket PATH] "
            "--migration-epoch N [--cutover-epoch N] --target-owner N "
            "[--shard-id N] [--page-limit N] [--wait-ms N] [--poll-ms N] [--timeout-ms N]\n"
            "       %s --coordinator-listen [--transport tcp|aeron] "
            "[--host HOST --port PORT | --socket PATH] "
            "--expected-sources 0,1 [--migration-epoch N] [--cutover-epoch N] "
            "[--active 0,1,2 | --standby 0,1,2] "
            "[--owner-endpoints 0=HOST:PORT,1=HOST:PORT | --owner-sockets 0=PATH,1=PATH] "
            "[--wait-ms N] [--timeout-ms N]\n",
            prog,
            prog,
            prog);
}

static int parse_u64_arg(const char *arg, uint64_t *out) {
    char *end = NULL;
    errno = 0;
    unsigned long long value = strtoull(arg, &end, 10);
    if (errno != 0 || !end || *end != '\0')
        return -1;
    *out = (uint64_t)value;
    return 0;
}

static int parse_u32_arg(const char *arg, uint32_t *out) {
    uint64_t value = 0;
    if (parse_u64_arg(arg, &value) != 0 || value > UINT32_MAX)
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
        if (count >= VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS)
            return -1;
        char *end = NULL;
        errno = 0;
        unsigned long value = strtoul(p, &end, 10);
        if (errno != 0 || !end || end == p || value > UINT32_MAX)
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

static int append_endpoint(topology_ctl_cfg_t *cfg,
                           const vemb_v16_topology_endpoint_t *endpoint) {
    if (!cfg || !endpoint ||
        cfg->endpoint_count >= VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS) {
        return -1;
    }
    cfg->endpoints[cfg->endpoint_count++] = *endpoint;
    return 0;
}

static int parse_owner_endpoint_list(topology_ctl_cfg_t *cfg,
                                     const char *arg,
                                     uint32_t transport_type) {
    if (!cfg || !arg || !arg[0])
        return -1;
    const char *p = arg;
    while (*p) {
        const char *comma = strchr(p, ',');
        size_t len = comma ? (size_t)(comma - p) : strlen(p);
        if (len == 0)
            return -1;
        const char *eq = memchr(p, '=', len);
        if (!eq || eq == p || eq + 1 >= p + len)
            return -1;
        char owner_buf[16];
        size_t owner_len = (size_t)(eq - p);
        if (owner_len >= sizeof(owner_buf))
            return -1;
        memcpy(owner_buf, p, owner_len);
        owner_buf[owner_len] = '\0';
        uint32_t owner = 0;
        if (parse_u32_arg(owner_buf, &owner) != 0)
            return -1;

        vemb_v16_topology_endpoint_t endpoint = {
            .owner_id = owner,
            .transport_type = transport_type,
        };
        const char *value = eq + 1;
        size_t value_len = len - owner_len - 1;
        if (transport_type == VEMB_V16_TRANSPORT_TCP) {
            const char *colon = NULL;
            for (const char *q = value; q < value + value_len; q++) {
                if (*q == ':') colon = q;
            }
            if (!colon || colon == value || colon + 1 >= value + value_len)
                return -1;
            size_t host_len = (size_t)(colon - value);
            size_t port_len = value_len - host_len - 1;
            if (host_len >= sizeof(endpoint.host))
                return -1;
            char port_buf[16];
            if (port_len >= sizeof(port_buf))
                return -1;
            memcpy(endpoint.host, value, host_len);
            endpoint.host[host_len] = '\0';
            memcpy(port_buf, colon + 1, port_len);
            port_buf[port_len] = '\0';
            if (parse_u16_arg(port_buf, &endpoint.tcp_port) != 0)
                return -1;
        } else {
            if (value_len >= sizeof(endpoint.uds_path))
                return -1;
            memcpy(endpoint.uds_path, value, value_len);
            endpoint.uds_path[value_len] = '\0';
        }
        if (append_endpoint(cfg, &endpoint) != 0)
            return -1;
        if (!comma)
            break;
        p = comma + 1;
    }
    return 0;
}

static int parse_coordinator_endpoint(topology_ctl_cfg_t *cfg,
                                      const char *arg,
                                      uint32_t transport_type) {
    if (!cfg || !arg || !arg[0])
        return -1;
    memset(&cfg->coordinator_endpoint, 0,
           sizeof(cfg->coordinator_endpoint));
    cfg->coordinator_endpoint.owner_id = UINT32_MAX;
    cfg->coordinator_endpoint.transport_type = transport_type;
    if (transport_type == VEMB_V16_TRANSPORT_TCP) {
        const char *colon = strrchr(arg, ':');
        if (!colon || colon == arg || colon[1] == '\0')
            return -1;
        size_t host_len = (size_t)(colon - arg);
        if (host_len >= sizeof(cfg->coordinator_endpoint.host))
            return -1;
        char port_buf[16];
        size_t port_len = strlen(colon + 1);
        if (port_len >= sizeof(port_buf))
            return -1;
        memcpy(cfg->coordinator_endpoint.host, arg, host_len);
        cfg->coordinator_endpoint.host[host_len] = '\0';
        memcpy(port_buf, colon + 1, port_len);
        port_buf[port_len] = '\0';
        if (parse_u16_arg(port_buf,
                          &cfg->coordinator_endpoint.tcp_port) != 0) {
            return -1;
        }
    } else {
        size_t path_len = strlen(arg);
        if (path_len >= sizeof(cfg->coordinator_endpoint.uds_path))
            return -1;
        memcpy(cfg->coordinator_endpoint.uds_path, arg, path_len + 1);
    }
    cfg->coordinator_endpoint_valid = 1;
    return 0;
}

static int connect_uds(const char *path, uint32_t timeout_ms) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    vemb_v16_net_set_timeouts(fd, timeout_ms);
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    if (strlen(path) >= sizeof(addr.sun_path)) {
        close(fd);
        return -1;
    }
    strcpy(addr.sun_path, path);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int topology_control_tcp(const topology_ctl_cfg_t *cfg,
                                const vemb_v16_topology_control_req_t *req,
                                vemb_v16_topology_control_resp_t *resp) {
    int fd = vemb_v16_net_connect(cfg->host, cfg->port, cfg->timeout_ms);
    if (fd < 0)
        return -1;
    uint16_t type = req ? VEMB_V16_NET_TOPOLOGY_SET :
                          VEMB_V16_NET_TOPOLOGY_GET;
    uint32_t payload_len = req ? (uint32_t)sizeof(*req) : 0;
    if (vemb_v16_net_write_frame(fd,
                                 type,
                                 0,
                                 0,
                                 0,
                                 req,
                                 payload_len) != 0) {
        close(fd);
        return -1;
    }

    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_TOPOLOGY_RESPONSE ||
        hdr.payload_len != sizeof(*resp) ||
        vemb_v16_net_read_full(fd, resp, sizeof(*resp)) != 0) {
        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}

static int topology_control_uds(const topology_ctl_cfg_t *cfg,
                                const vemb_v16_topology_control_req_t *req,
                                vemb_v16_topology_control_resp_t *resp) {
    int fd = connect_uds(cfg->socket_path, cfg->timeout_ms);
    if (fd < 0)
        return -1;
    uint8_t op = req ? VEMB_V16_CTRL_TOPOLOGY_SET :
                       VEMB_V16_CTRL_TOPOLOGY_GET;
    if (vemb_v16_net_write_full(fd, &op, sizeof(op)) != 0 ||
        (req && vemb_v16_net_write_full(fd, req, sizeof(*req)) != 0) ||
        vemb_v16_net_read_full(fd, resp, sizeof(*resp)) != 0) {
        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}

static int topology_control_endpoint(
        const topology_ctl_cfg_t *cfg,
        const vemb_v16_topology_endpoint_t *endpoint,
        const vemb_v16_topology_control_req_t *req,
        vemb_v16_topology_control_resp_t *resp) {
    if (!cfg || !endpoint || !req || !resp)
        return -1;
    topology_ctl_cfg_t endpoint_cfg = *cfg;
    endpoint_cfg.transport_type = endpoint->transport_type;
    if (endpoint->transport_type == VEMB_V16_TRANSPORT_TCP) {
        endpoint_cfg.host = endpoint->host;
        endpoint_cfg.port = endpoint->tcp_port;
        return topology_control_tcp(&endpoint_cfg, req, resp);
    }
    if (endpoint->transport_type == VEMB_V16_TRANSPORT_AERON) {
        endpoint_cfg.socket_path = endpoint->uds_path;
        return topology_control_uds(&endpoint_cfg, req, resp);
    }
    return -1;
}

static uint64_t monotonic_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static void sleep_ms(uint32_t ms) {
    struct timespec ts = {
        .tv_sec = ms / 1000u,
        .tv_nsec = (long)(ms % 1000u) * 1000000L,
    };
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
    }
}

static int migration_range_control_tcp(
        const topology_ctl_cfg_t *cfg,
        uint16_t type,
        const vemb_v16_migration_range_control_req_t *req,
        vemb_v16_migration_range_control_resp_t *resp) {
    int fd = vemb_v16_net_connect(cfg->host, cfg->port, cfg->timeout_ms);
    if (fd < 0)
        return -1;
    if (vemb_v16_net_write_frame(fd,
                                 type,
                                 0,
                                 0,
                                 0,
                                 req,
                                 (uint32_t)sizeof(*req)) != 0) {
        close(fd);
        return -1;
    }
    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_MIGRATION_RANGE_CONTROL_RESPONSE ||
        hdr.payload_len != sizeof(*resp) ||
        vemb_v16_net_read_full(fd, resp, sizeof(*resp)) != 0) {
        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}

static int migration_range_control_uds(
        const topology_ctl_cfg_t *cfg,
        uint8_t op,
        const vemb_v16_migration_range_control_req_t *req,
        vemb_v16_migration_range_control_resp_t *resp) {
    int fd = connect_uds(cfg->socket_path, cfg->timeout_ms);
    if (fd < 0)
        return -1;
    if (vemb_v16_net_write_full(fd, &op, sizeof(op)) != 0 ||
        vemb_v16_net_write_full(fd, req, sizeof(*req)) != 0 ||
        vemb_v16_net_read_full(fd, resp, sizeof(*resp)) != 0) {
        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}

static void print_owner_list(const char *name,
                             const uint32_t *owners,
                             uint32_t owner_count) {
    printf("%s=", name);
    for (uint32_t i = 0; i < owner_count; i++) {
        printf("%s%u", i ? "," : "", owners[i]);
    }
    printf("\n");
}

static void print_response(const vemb_v16_topology_control_resp_t *resp) {
    printf("status=%u\n", resp->status);
    printf("current_topology_epoch=%llu\n",
           (unsigned long long)resp->current_topology_epoch);
    printf("min_write_epoch=%llu\n",
           (unsigned long long)resp->min_write_epoch);
    printf("vnode_count=%u\n", resp->vnode_count);
    printf("flags=0x%x\n", resp->flags);
    print_owner_list("active_owners",
                     resp->active_owners,
                     resp->active_owner_count);
    print_owner_list("standby_owners",
                     resp->standby_owners,
                     resp->standby_owner_count);
    printf("coordinator_endpoint_valid=%u\n",
           resp->coordinator_endpoint_valid);
    if (resp->coordinator_endpoint_valid) {
        const vemb_v16_topology_endpoint_t *endpoint =
            &resp->coordinator_endpoint;
        if (endpoint->transport_type == VEMB_V16_TRANSPORT_TCP) {
            printf("coordinator_endpoint=transport:tcp host:%s port:%u\n",
                   endpoint->host,
                   endpoint->tcp_port);
        } else if (endpoint->transport_type == VEMB_V16_TRANSPORT_AERON) {
            printf("coordinator_endpoint=transport:uds socket:%s\n",
                   endpoint->uds_path);
        }
    }
    printf("endpoint_count=%u\n", resp->endpoint_count);
    for (uint32_t i = 0; i < resp->endpoint_count; i++) {
        const vemb_v16_topology_endpoint_t *endpoint = &resp->endpoints[i];
        if (endpoint->transport_type == VEMB_V16_TRANSPORT_TCP) {
            printf("endpoint[%u]=owner:%u transport:tcp host:%s port:%u\n",
                   i,
                   endpoint->owner_id,
                   endpoint->host,
                   endpoint->tcp_port);
        } else if (endpoint->transport_type == VEMB_V16_TRANSPORT_AERON) {
            printf("endpoint[%u]=owner:%u transport:uds socket:%s\n",
                   i,
                   endpoint->owner_id,
                   endpoint->uds_path);
        }
    }
}

static void print_range_response(
        const vemb_v16_migration_range_control_resp_t *resp) {
    printf("status=%u\n", resp->status);
    printf("migration_topology_epoch=%llu\n",
           (unsigned long long)resp->migration_topology_epoch);
    printf("cutover_topology_epoch=%llu\n",
           (unsigned long long)resp->cutover_topology_epoch);
    printf("owner_epoch=%llu\n",
           (unsigned long long)resp->owner_epoch);
    printf("target_owner=%u\n", resp->target_owner);
    printf("shard_id=%u\n", resp->shard_id);
    printf("key_count=%u\n", resp->key_count);
    printf("success_count=%u\n", resp->success_count);
    printf("error_count=%u\n", resp->error_count);
    printf("applied_seq=%llu\n", (unsigned long long)resp->applied_seq);
    printf("barrier_seq=%llu\n", (unsigned long long)resp->barrier_seq);
    printf("source_seq=%llu\n", (unsigned long long)resp->source_seq);
    printf("retry_delta=%llu\n", (unsigned long long)resp->retry_delta);
    printf("pending_delta=%u\n", resp->pending_delta);
    printf("outbox_state=%u\n", resp->outbox_state);
    printf("remaining_keys=%u\n", resp->remaining_keys);
    printf("page_key_count=%u\n", resp->page_key_count);
    printf("range_done=%u\n", resp->range_done);
    printf("range_ready=%u\n", resp->range_ready);
    printf("page_limit=%u\n", resp->page_limit);
}

static int range_resp_cutover_ready(
        const vemb_v16_migration_range_control_resp_t *resp) {
    return resp &&
           resp->status == VEMB_V16_STATUS_OK &&
           resp->range_ready != 0;
}

static int build_request(const topology_ctl_cfg_t *cfg,
                         vemb_v16_topology_control_req_t *req) {
    if (!cfg->has_epoch ||
        cfg->active_owner_count == 0 ||
        cfg->standby_owner_count == 0) {
        return -1;
    }
    memset(req, 0, sizeof(*req));
    req->current_topology_epoch = cfg->current_topology_epoch;
    req->min_write_epoch = cfg->has_min_write_epoch ?
        cfg->min_write_epoch : cfg->current_topology_epoch;
    req->active_owner_count = cfg->active_owner_count;
    req->standby_owner_count = cfg->standby_owner_count;
    req->vnode_count = cfg->vnode_count ?
        cfg->vnode_count : VEMB_V16_TOPOLOGY_DEFAULT_VNODES;
    req->flags = cfg->flags;
    memcpy(req->active_owners,
           cfg->active_owners,
           sizeof(uint32_t) * cfg->active_owner_count);
    memcpy(req->standby_owners,
           cfg->standby_owners,
           sizeof(uint32_t) * cfg->standby_owner_count);
    req->endpoint_count = cfg->endpoint_count;
    req->coordinator_endpoint_valid = cfg->coordinator_endpoint_valid;
    if (cfg->coordinator_endpoint_valid) {
        req->coordinator_endpoint = cfg->coordinator_endpoint;
    }
    if (cfg->endpoint_count > 0) {
        memcpy(req->endpoints,
               cfg->endpoints,
               sizeof(req->endpoints[0]) * cfg->endpoint_count);
    }
    return 0;
}

static int build_range_request(
        const topology_ctl_cfg_t *cfg,
        vemb_v16_migration_range_control_req_t *req,
        int need_cutover_epoch) {
    if (!cfg->has_migration_epoch ||
        cfg->target_owner == UINT32_MAX ||
        (need_cutover_epoch && !cfg->has_cutover_epoch)) {
        return -1;
    }
    memset(req, 0, sizeof(*req));
    req->migration_topology_epoch = cfg->migration_topology_epoch;
    req->cutover_topology_epoch = cfg->has_cutover_epoch ?
        cfg->cutover_topology_epoch : 0;
    req->target_owner = cfg->target_owner;
    req->shard_id = cfg->shard_id;
    req->page_limit = cfg->page_limit;
    return 0;
}

static int send_range_control(
        const topology_ctl_cfg_t *cfg,
        topology_ctl_action_t action,
        const vemb_v16_migration_range_control_req_t *req,
        vemb_v16_migration_range_control_resp_t *resp) {
    if (cfg->transport_type == VEMB_V16_TRANSPORT_TCP) {
        uint16_t type = VEMB_V16_NET_MIGRATION_RANGE_BARRIER;
        if (action == TOPOLOGY_CTL_RANGE_CUTOVER ||
            action == TOPOLOGY_CTL_RANGE_WAIT_CUTOVER) {
            type = VEMB_V16_NET_MIGRATION_RANGE_MARK_CUTOVER;
        } else if (action == TOPOLOGY_CTL_RANGE_SOURCE_GC) {
            type = VEMB_V16_NET_MIGRATION_RANGE_SOURCE_GC;
        }
        return migration_range_control_tcp(cfg, type, req, resp);
    }
    uint8_t op = VEMB_V16_CTRL_MIGRATION_RANGE_BARRIER;
    if (action == TOPOLOGY_CTL_RANGE_CUTOVER ||
        action == TOPOLOGY_CTL_RANGE_WAIT_CUTOVER) {
        op = VEMB_V16_CTRL_MIGRATION_RANGE_MARK_CUTOVER;
    } else if (action == TOPOLOGY_CTL_RANGE_SOURCE_GC) {
        op = VEMB_V16_CTRL_MIGRATION_RANGE_SOURCE_GC;
    }
    return migration_range_control_uds(cfg, op, req, resp);
}

static int run_range_action(const topology_ctl_cfg_t *cfg) {
    int direct_cutover = cfg->action == TOPOLOGY_CTL_RANGE_CUTOVER;
    int direct_source_gc = cfg->action == TOPOLOGY_CTL_RANGE_SOURCE_GC;
    int wait_ready = cfg->action == TOPOLOGY_CTL_RANGE_WAIT_READY ||
                     cfg->action == TOPOLOGY_CTL_RANGE_WAIT_CUTOVER;
    int wait_cutover = cfg->action == TOPOLOGY_CTL_RANGE_WAIT_CUTOVER;
    vemb_v16_migration_range_control_req_t req;
    if (build_range_request(cfg,
                            &req,
                            direct_cutover ||
                            direct_source_gc ||
                            wait_cutover) != 0) {
        usage("vemb_v16_topology_ctl");
        return 1;
    }

    vemb_v16_migration_range_control_resp_t resp;
    memset(&resp, 0, sizeof(resp));
    if (!wait_ready) {
        if (direct_cutover || direct_source_gc) {
            uint64_t start_ms = monotonic_ms();
            uint64_t deadline_ms = start_ms + cfg->wait_ms;
            for (;;) {
                memset(&resp, 0, sizeof(resp));
                if (send_range_control(cfg, cfg->action, &req, &resp) != 0) {
                    fprintf(stderr, "migration range control request failed\n");
                    return 1;
                }
                if (resp.status != VEMB_V16_STATUS_OK ||
                    resp.range_done != 0) {
                    print_range_response(&resp);
                    return resp.status == VEMB_V16_STATUS_OK &&
                        resp.range_done != 0 ? 0 : 1;
                }
                uint64_t now = monotonic_ms();
                if (cfg->wait_ms == 0 || now >= deadline_ms) {
                    print_range_response(&resp);
                    return 1;
                }
                uint32_t remaining = (uint32_t)(deadline_ms - now);
                sleep_ms(cfg->poll_ms < remaining ?
                         cfg->poll_ms : remaining);
            }
        }
        if (send_range_control(cfg, cfg->action, &req, &resp) != 0) {
            fprintf(stderr, "migration range control request failed\n");
            return 1;
        }
        print_range_response(&resp);
        return resp.status == VEMB_V16_STATUS_OK ? 0 : 1;
    }

    uint64_t start_ms = monotonic_ms();
    uint64_t deadline_ms = start_ms + cfg->wait_ms;
    int ready = 0;
    for (;;) {
        memset(&resp, 0, sizeof(resp));
        if (send_range_control(cfg,
                               TOPOLOGY_CTL_RANGE_BARRIER,
                               &req,
                               &resp) != 0) {
            fprintf(stderr, "migration range barrier request failed\n");
            return 1;
        }
        if (range_resp_cutover_ready(&resp)) {
            ready = 1;
            break;
        }
        uint64_t now = monotonic_ms();
        if (cfg->wait_ms == 0 || now >= deadline_ms)
            break;
        uint32_t remaining = (uint32_t)(deadline_ms - now);
        sleep_ms(cfg->poll_ms < remaining ? cfg->poll_ms : remaining);
    }

    if (!ready) {
        print_range_response(&resp);
        return 1;
    }
    if (!wait_cutover) {
        print_range_response(&resp);
        return 0;
    }

    for (;;) {
        memset(&resp, 0, sizeof(resp));
        if (send_range_control(cfg,
                               TOPOLOGY_CTL_RANGE_CUTOVER,
                               &req,
                               &resp) != 0) {
            fprintf(stderr, "migration range cutover request failed\n");
            return 1;
        }
        if (resp.status != VEMB_V16_STATUS_OK ||
            resp.range_done != 0) {
            print_range_response(&resp);
            return resp.status == VEMB_V16_STATUS_OK &&
                resp.range_done != 0 ? 0 : 1;
        }
        uint64_t now = monotonic_ms();
        if (cfg->wait_ms == 0 || now >= deadline_ms) {
            print_range_response(&resp);
            return 1;
        }
        uint32_t remaining = (uint32_t)(deadline_ms - now);
        sleep_ms(cfg->poll_ms < remaining ? cfg->poll_ms : remaining);
    }
}

static int listen_uds(const char *path, int backlog) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    if (!path || strlen(path) >= sizeof(addr.sun_path)) {
        close(fd);
        return -1;
    }
    strcpy(addr.sun_path, path);
    unlink(path);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 ||
        listen(fd, backlog > 0 ? backlog : 128) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int expected_source_index(const topology_ctl_cfg_t *cfg,
                                 uint32_t source_owner) {
    for (uint32_t i = 0; i < cfg->expected_source_count; i++) {
        if (cfg->expected_sources[i] == source_owner)
            return (int)i;
    }
    return -1;
}

static uint8_t coordinator_record_done(
        const topology_ctl_cfg_t *cfg,
        const vemb_v16_scaleout_local_done_req_t *req,
        uint8_t *done,
        uint32_t *done_count) {
    if (!cfg || !req || !done || !done_count)
        return VEMB_V16_STATUS_ERR;
    if (cfg->has_migration_epoch &&
        req->migration_topology_epoch != cfg->migration_topology_epoch) {
        return VEMB_V16_STATUS_ERR;
    }
    if (cfg->has_cutover_epoch &&
        req->cutover_topology_epoch != cfg->cutover_topology_epoch) {
        return VEMB_V16_STATUS_ERR;
    }
    int idx = expected_source_index(cfg, req->source_owner);
    if (idx < 0)
        return VEMB_V16_STATUS_ERR;
    if (!done[idx]) {
        done[idx] = 1;
        (*done_count)++;
        printf("scaleout_local_done source_owner=%u migration_epoch=%llu cutover_epoch=%llu notify_seq=%llu done=%u/%u\n",
               req->source_owner,
               (unsigned long long)req->migration_topology_epoch,
               (unsigned long long)req->cutover_topology_epoch,
               (unsigned long long)req->notify_seq,
               *done_count,
               cfg->expected_source_count);
        fflush(stdout);
    }
    return VEMB_V16_STATUS_OK;
}

static void coordinator_fill_resp(
        const vemb_v16_scaleout_local_done_req_t *req,
        uint8_t status,
        vemb_v16_scaleout_local_done_resp_t *resp) {
    memset(resp, 0, sizeof(*resp));
    resp->status = status;
    if (!req)
        return;
    resp->migration_topology_epoch = req->migration_topology_epoch;
    resp->cutover_topology_epoch = req->cutover_topology_epoch;
    resp->notify_seq = req->notify_seq;
    resp->source_owner = req->source_owner;
}

static int coordinator_handle_tcp_fd(const topology_ctl_cfg_t *cfg,
                                     int fd,
                                     uint8_t *done,
                                     uint32_t *done_count) {
    vemb_v16_net_set_timeouts(fd, cfg->timeout_ms);
    vemb_v16_net_hdr_t hdr;
    vemb_v16_scaleout_local_done_req_t req;
    vemb_v16_scaleout_local_done_resp_t resp;
    memset(&req, 0, sizeof(req));
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_SCALEOUT_LOCAL_DONE ||
        hdr.payload_len != sizeof(req) ||
        vemb_v16_net_read_full(fd, &req, sizeof(req)) != 0) {
        coordinator_fill_resp(NULL, VEMB_V16_STATUS_ERR, &resp);
    } else {
        uint8_t status = coordinator_record_done(cfg,
                                                 &req,
                                                 done,
                                                 done_count);
        coordinator_fill_resp(&req, status, &resp);
    }
    (void)vemb_v16_net_write_frame(
        fd,
        VEMB_V16_NET_SCALEOUT_LOCAL_DONE_RESPONSE,
        0,
        0,
        0,
        &resp,
        (uint32_t)sizeof(resp));
    return resp.status == VEMB_V16_STATUS_OK ? 0 : -1;
}

static int coordinator_handle_uds_fd(const topology_ctl_cfg_t *cfg,
                                     int fd,
                                     uint8_t *done,
                                     uint32_t *done_count) {
    vemb_v16_net_set_timeouts(fd, cfg->timeout_ms);
    uint8_t op = 0;
    vemb_v16_scaleout_local_done_req_t req;
    vemb_v16_scaleout_local_done_resp_t resp;
    memset(&req, 0, sizeof(req));
    if (vemb_v16_net_read_full(fd, &op, sizeof(op)) != 0 ||
        op != VEMB_V16_CTRL_SCALEOUT_LOCAL_DONE ||
        vemb_v16_net_read_full(fd, &req, sizeof(req)) != 0) {
        coordinator_fill_resp(NULL, VEMB_V16_STATUS_ERR, &resp);
    } else {
        uint8_t status = coordinator_record_done(cfg,
                                                 &req,
                                                 done,
                                                 done_count);
        coordinator_fill_resp(&req, status, &resp);
    }
    (void)vemb_v16_net_write_full(fd, &resp, sizeof(resp));
    return resp.status == VEMB_V16_STATUS_OK ? 0 : -1;
}

static const uint32_t *coordinator_full_active_owners(
        const topology_ctl_cfg_t *cfg,
        uint32_t *owner_count) {
    if (!cfg || !owner_count)
        return NULL;
    if (cfg->standby_owner_count > 0) {
        *owner_count = cfg->standby_owner_count;
        return cfg->standby_owners;
    }
    if (cfg->active_owner_count > 0) {
        *owner_count = cfg->active_owner_count;
        return cfg->active_owners;
    }
    *owner_count = 0;
    return NULL;
}

static int coordinator_build_full_active_req(
        const topology_ctl_cfg_t *cfg,
        vemb_v16_topology_control_req_t *req) {
    if (!cfg || !req || !cfg->has_cutover_epoch)
        return -1;
    uint32_t owner_count = 0;
    const uint32_t *owners =
        coordinator_full_active_owners(cfg, &owner_count);
    if (!owners || owner_count == 0 ||
        owner_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS ||
        cfg->endpoint_count == 0) {
        return -1;
    }

    memset(req, 0, sizeof(*req));
    req->current_topology_epoch = cfg->cutover_topology_epoch;
    req->min_write_epoch = cfg->has_min_write_epoch ?
        cfg->min_write_epoch : cfg->cutover_topology_epoch;
    req->active_owner_count = owner_count;
    req->standby_owner_count = owner_count;
    req->vnode_count = cfg->vnode_count ?
        cfg->vnode_count : VEMB_V16_TOPOLOGY_DEFAULT_VNODES;
    req->flags =
        cfg->flags &
        ~(VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED |
          VEMB_V16_TOPOLOGY_CONTROL_F_AUTO_SCALEOUT |
          VEMB_V16_TOPOLOGY_CONTROL_F_COORDINATED_SCALEOUT);
    memcpy(req->active_owners, owners, sizeof(uint32_t) * owner_count);
    memcpy(req->standby_owners, owners, sizeof(uint32_t) * owner_count);
    req->endpoint_count = cfg->endpoint_count;
    memcpy(req->endpoints,
           cfg->endpoints,
           sizeof(req->endpoints[0]) * cfg->endpoint_count);
    return 0;
}

static int coordinator_publish_full_active(const topology_ctl_cfg_t *cfg) {
    vemb_v16_topology_control_req_t req;
    if (coordinator_build_full_active_req(cfg, &req) != 0) {
        printf("scaleout_full_active_publish=skipped\n");
        return 0;
    }

    uint32_t success_count = 0;
    uint32_t error_count = 0;
    for (uint32_t i = 0; i < cfg->endpoint_count; i++) {
        const vemb_v16_topology_endpoint_t *endpoint = &cfg->endpoints[i];
        vemb_v16_topology_control_resp_t resp;
        memset(&resp, 0, sizeof(resp));
        if (topology_control_endpoint(cfg, endpoint, &req, &resp) == 0 &&
            resp.status == VEMB_V16_STATUS_OK) {
            success_count++;
            printf("scaleout_full_active_publish owner=%u status=ok epoch=%llu\n",
                   endpoint->owner_id,
                   (unsigned long long)req.current_topology_epoch);
        } else {
            error_count++;
            printf("scaleout_full_active_publish owner=%u status=error epoch=%llu\n",
                   endpoint->owner_id,
                   (unsigned long long)req.current_topology_epoch);
        }
        fflush(stdout);
    }
    printf("scaleout_full_active_published=%u errors=%u targets=%u\n",
           success_count,
           error_count,
           cfg->endpoint_count);
    return error_count == 0 ? 0 : -1;
}

static int run_coordinator_listen(const topology_ctl_cfg_t *cfg) {
    if (!cfg || cfg->expected_source_count == 0)
        return 1;
    int listen_fd = cfg->transport_type == VEMB_V16_TRANSPORT_TCP ?
        vemb_v16_net_listen(cfg->host, cfg->port, 128) :
        listen_uds(cfg->socket_path, 128);
    if (listen_fd < 0) {
        fprintf(stderr, "coordinator listen failed\n");
        return 1;
    }
    uint8_t done[VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS];
    memset(done, 0, sizeof(done));
    uint32_t done_count = 0;
    uint64_t start_ms = monotonic_ms();
    uint64_t deadline_ms = cfg->wait_ms ? start_ms + cfg->wait_ms : 0;
    printf("coordinator_listen transport=%s expected_sources=%u\n",
           vemb_v16_transport_name(cfg->transport_type),
           cfg->expected_source_count);
    fflush(stdout);

    while (done_count < cfg->expected_source_count) {
        struct timeval tv;
        struct timeval *tvp = NULL;
        if (deadline_ms != 0) {
            uint64_t now = monotonic_ms();
            if (now >= deadline_ms)
                break;
            uint64_t remain_ms = deadline_ms - now;
            tv.tv_sec = (time_t)(remain_ms / 1000u);
            tv.tv_usec = (suseconds_t)((remain_ms % 1000u) * 1000u);
            tvp = &tv;
        }

        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(listen_fd, &rfds);
        int ready = select(listen_fd + 1, &rfds, NULL, NULL, tvp);
        if (ready < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        if (ready == 0)
            break;

        int fd = accept(listen_fd, NULL, NULL);
        if (fd < 0) {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            break;
        }
        if (cfg->transport_type == VEMB_V16_TRANSPORT_TCP) {
            (void)coordinator_handle_tcp_fd(cfg, fd, done, &done_count);
        } else {
            (void)coordinator_handle_uds_fd(cfg, fd, done, &done_count);
        }
        close(fd);
    }

    close(listen_fd);
    if (cfg->transport_type == VEMB_V16_TRANSPORT_AERON)
        unlink(cfg->socket_path);
    if (done_count == cfg->expected_source_count) {
        printf("scaleout_all_sources_done=%u\n", done_count);
        return coordinator_publish_full_active(cfg) == 0 ? 0 : 1;
    }
    printf("scaleout_all_sources_done=%u expected=%u\n",
           done_count,
           cfg->expected_source_count);
    return 1;
}

int main(int argc, char **argv) {
    topology_ctl_cfg_t cfg = {
        .transport_type = VEMB_V16_TRANSPORT_TCP,
        .socket_path = VEMB_V16_UDS_PATH,
        .host = VEMB_V16_TCP_HOST,
        .port = VEMB_V16_TCP_PORT,
        .timeout_ms = 5000,
        .action = TOPOLOGY_CTL_GET,
        .target_owner = UINT32_MAX,
        .wait_ms = 5000,
        .poll_ms = 100,
        .vnode_count = VEMB_V16_TOPOLOGY_DEFAULT_VNODES,
    };

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--get")) {
            cfg.action = TOPOLOGY_CTL_GET;
        } else if (!strcmp(argv[i], "--set")) {
            cfg.action = TOPOLOGY_CTL_SET;
        } else if (!strcmp(argv[i], "--range-barrier")) {
            cfg.action = TOPOLOGY_CTL_RANGE_BARRIER;
        } else if (!strcmp(argv[i], "--range-cutover")) {
            cfg.action = TOPOLOGY_CTL_RANGE_CUTOVER;
        } else if (!strcmp(argv[i], "--range-source-gc")) {
            cfg.action = TOPOLOGY_CTL_RANGE_SOURCE_GC;
        } else if (!strcmp(argv[i], "--range-wait-ready")) {
            cfg.action = TOPOLOGY_CTL_RANGE_WAIT_READY;
        } else if (!strcmp(argv[i], "--range-wait-cutover")) {
            cfg.action = TOPOLOGY_CTL_RANGE_WAIT_CUTOVER;
        } else if (!strcmp(argv[i], "--coordinator-listen")) {
            cfg.action = TOPOLOGY_CTL_COORDINATOR_LISTEN;
        } else if (!strcmp(argv[i], "--transport") && i + 1 < argc) {
            const char *transport = argv[++i];
            if (!strcmp(transport, "tcp")) {
                cfg.transport_type = VEMB_V16_TRANSPORT_TCP;
            } else if (!strcmp(transport, "aeron") ||
                       !strcmp(transport, "uds")) {
                cfg.transport_type = VEMB_V16_TRANSPORT_AERON;
            } else {
                usage(argv[0]);
                return 1;
            }
        } else if ((!strcmp(argv[i], "--host") ||
                    !strcmp(argv[i], "--tcp-host")) && i + 1 < argc) {
            cfg.host = argv[++i];
        } else if ((!strcmp(argv[i], "--port") ||
                    !strcmp(argv[i], "--tcp-port")) && i + 1 < argc) {
            if (parse_u16_arg(argv[++i], &cfg.port) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--socket") && i + 1 < argc) {
            cfg.socket_path = argv[++i];
        } else if (!strcmp(argv[i], "--epoch") && i + 1 < argc) {
            if (parse_u64_arg(argv[++i], &cfg.current_topology_epoch) != 0) {
                usage(argv[0]);
                return 1;
            }
            cfg.has_epoch = 1;
        } else if (!strcmp(argv[i], "--min-write-epoch") && i + 1 < argc) {
            if (parse_u64_arg(argv[++i], &cfg.min_write_epoch) != 0) {
                usage(argv[0]);
                return 1;
            }
            cfg.has_min_write_epoch = 1;
        } else if (!strcmp(argv[i], "--active") && i + 1 < argc) {
            if (parse_owner_list(argv[++i],
                                 cfg.active_owners,
                                 &cfg.active_owner_count) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--standby") && i + 1 < argc) {
            if (parse_owner_list(argv[++i],
                                 cfg.standby_owners,
                                 &cfg.standby_owner_count) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--dual-write")) {
            cfg.flags |= VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED;
        } else if (!strcmp(argv[i], "--no-dual-write")) {
            cfg.flags &= ~VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED;
        } else if (!strcmp(argv[i], "--auto-scaleout")) {
            cfg.flags |= VEMB_V16_TOPOLOGY_CONTROL_F_AUTO_SCALEOUT;
        } else if (!strcmp(argv[i], "--no-auto-scaleout")) {
            cfg.flags &= ~VEMB_V16_TOPOLOGY_CONTROL_F_AUTO_SCALEOUT;
        } else if (!strcmp(argv[i], "--coordinated-scaleout")) {
            cfg.flags |= VEMB_V16_TOPOLOGY_CONTROL_F_COORDINATED_SCALEOUT;
        } else if (!strcmp(argv[i], "--no-coordinated-scaleout")) {
            cfg.flags &= ~VEMB_V16_TOPOLOGY_CONTROL_F_COORDINATED_SCALEOUT;
        } else if (!strcmp(argv[i], "--owner-endpoints") && i + 1 < argc) {
            if (parse_owner_endpoint_list(&cfg,
                                          argv[++i],
                                          VEMB_V16_TRANSPORT_TCP) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--owner-sockets") && i + 1 < argc) {
            if (parse_owner_endpoint_list(&cfg,
                                          argv[++i],
                                          VEMB_V16_TRANSPORT_AERON) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--coordinator-endpoint") &&
                   i + 1 < argc) {
            if (parse_coordinator_endpoint(&cfg,
                                           argv[++i],
                                           VEMB_V16_TRANSPORT_TCP) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--coordinator-socket") &&
                   i + 1 < argc) {
            if (parse_coordinator_endpoint(&cfg,
                                           argv[++i],
                                           VEMB_V16_TRANSPORT_AERON) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--vnode-count") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg.vnode_count) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--migration-epoch") && i + 1 < argc) {
            if (parse_u64_arg(argv[++i],
                              &cfg.migration_topology_epoch) != 0) {
                usage(argv[0]);
                return 1;
            }
            cfg.has_migration_epoch = 1;
        } else if (!strcmp(argv[i], "--cutover-epoch") && i + 1 < argc) {
            if (parse_u64_arg(argv[++i],
                              &cfg.cutover_topology_epoch) != 0) {
                usage(argv[0]);
                return 1;
            }
            cfg.has_cutover_epoch = 1;
        } else if ((!strcmp(argv[i], "--target-owner") ||
                    !strcmp(argv[i], "--target")) && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg.target_owner) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--shard-id") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg.shard_id) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--page-limit") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg.page_limit) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--expected-sources") &&
                   i + 1 < argc) {
            if (parse_owner_list(argv[++i],
                                 cfg.expected_sources,
                                 &cfg.expected_source_count) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--wait-ms") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg.wait_ms) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--poll-ms") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg.poll_ms) != 0 ||
                cfg.poll_ms == 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--timeout-ms") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg.timeout_ms) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--help")) {
            usage(argv[0]);
            return 0;
        } else {
            usage(argv[0]);
            return 1;
        }
    }

    if (cfg.action == TOPOLOGY_CTL_RANGE_BARRIER ||
        cfg.action == TOPOLOGY_CTL_RANGE_CUTOVER ||
        cfg.action == TOPOLOGY_CTL_RANGE_SOURCE_GC ||
        cfg.action == TOPOLOGY_CTL_RANGE_WAIT_READY ||
        cfg.action == TOPOLOGY_CTL_RANGE_WAIT_CUTOVER) {
        return run_range_action(&cfg);
    }
    if (cfg.action == TOPOLOGY_CTL_COORDINATOR_LISTEN) {
        return run_coordinator_listen(&cfg);
    }

    vemb_v16_topology_control_req_t req;
    vemb_v16_topology_control_req_t *req_ptr = NULL;
    if (cfg.action == TOPOLOGY_CTL_SET) {
        if (build_request(&cfg, &req) != 0) {
            usage(argv[0]);
            return 1;
        }
        req_ptr = &req;
    }

    vemb_v16_topology_control_resp_t resp;
    memset(&resp, 0, sizeof(resp));
    int rc;
    if (cfg.transport_type == VEMB_V16_TRANSPORT_TCP) {
        rc = topology_control_tcp(&cfg, req_ptr, &resp);
    } else {
        rc = topology_control_uds(&cfg, req_ptr, &resp);
    }
    if (rc != 0) {
        fprintf(stderr, "topology control request failed\n");
        return 1;
    }

    print_response(&resp);
    return resp.status == VEMB_V16_STATUS_OK ? 0 : 1;
}
