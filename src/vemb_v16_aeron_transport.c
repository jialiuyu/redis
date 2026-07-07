#define _GNU_SOURCE

#include "cpu_relax.h"
#include "vemb_v16_aeron_transport.h"
#include "vemb_v16_client_ring.h"
#include "vemb_v16_log.h"
#include "zmalloc.h"
#include "macro.h"

#include <stdatomic.h>
#include <fcntl.h>
#include <stdint.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

/// UDS + SHM/Aeron transport implementation.

int vemb_v16_aeron_listen(vemb_v16_proxy_t *proxy,
                          int backlog,
                          vemb_v16_transport_listener_t *listener) {
    const char *uds_path = vemb_v16_proxy_uds_path(proxy);
    unlink(uds_path);

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, uds_path, sizeof(addr.sun_path) - 1);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 ||
        listen(fd, backlog) != 0) {
        close(fd);
        unlink(uds_path);
        return -1;
    }
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);

    *listener = (vemb_v16_transport_listener_t){
        .name = "uds",
        .fd = fd,
        .handle_fd = vemb_v16_aeron_handle_control_fd,
    };
    return 0;
}

/// UB/SHM control plane: create the per-channel client request/response rings.
int vemb_v16_aeron_create_shared_ring(const char *name,
                                      uint32_t slot_size,
                                      vemb_v16_client_ring_t **ring,
                                      size_t *ring_bytes) {
    shm_unlink(name);
    size_t bytes = vemb_v16_client_ring_bytes(slot_size);
    int fd = shm_open(name, O_CREAT | O_RDWR, 0666);
    if (fd < 0) return -1;
    if (ftruncate(fd, (off_t)bytes) != 0) {
        close(fd);
        shm_unlink(name);
        return -1;
    }
    void *ptr = mmap(NULL, bytes, PROT_READ | PROT_WRITE,
                     MAP_SHARED, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) {
        shm_unlink(name);
        return -1;
    }
    memset(ptr, 0, bytes);
    vemb_v16_client_ring_init(ptr, slot_size);
    *ring = ptr;
    if (ring_bytes) *ring_bytes = bytes;
    return 0;
}

void vemb_v16_aeron_destroy_shared_ring(const char *name,
                                        vemb_v16_client_ring_t *ring,
                                        size_t ring_bytes) {
    if (ring) munmap(ring, ring_bytes);
    if (name && name[0]) shm_unlink(name);
}

static int read_full(int fd, void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = read(fd, (char *)buf + done, n - done);
        if (r <= 0) return -1;
        done += (size_t)r;
    }
    return 0;
}

static int write_full(int fd, const void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = write(fd, (const char *)buf + done, n - done);
        if (r <= 0) return -1;
        done += (size_t)r;
    }
    return 0;
}

/// UB/SHM transport: poll client request ring and hand jobs to the scheduler.
int vemb_v16_aeron_poll_shm_requests(vemb_v16_channel_t *ch,
                                     uint32_t proxy_io_worker_id) {
    vemb_v16_client_ring_t *request_ring = vemb_v16_channel_request_ring(ch);

    vemb_v16_req_t *req_buf =
        zmalloc(sizeof(*req_buf) * VEMB_V16_PROXY_BATCH);
    if (!req_buf)
        return -1;

    uint32_t req_count = vemb_v16_client_poll_batch(request_ring,
                                                    req_buf,
                                                    sizeof(req_buf[0]),
                                                    VEMB_V16_PROXY_BATCH);
    if (req_count == 0) {
        zfree(req_buf);
        return 0;
    }

    for (uint32_t i = 0; i < req_count; i++) {
        vemb_v16_proxy_handle_request(ch, &req_buf[i],
                       (int)vemb_v16_channel_request_slot_size(ch),
                       proxy_io_worker_id);
    }
    zfree(req_buf);
    return (int)req_count;
}

/// UB/SHM transport: publish one response to the client response ring.
int vemb_v16_aeron_publish_response(vemb_v16_channel_t *ch,
                                    const vemb_v16_resp_t *resp) {
    while (vemb_v16_client_publish(vemb_v16_channel_response_ring(ch),
                                   resp,
                                   sizeof(*resp)) != 0 &&
           vemb_v16_channel_proxy_running(ch) &&
           vemb_v16_channel_active(ch)) {
        vemb_v16_channel_add_proxy_response_ring_full(ch, 1);
        cpu_relax();
    }
    return vemb_v16_channel_active(ch) ? 0 : -1;
}

/// UDS control plane: allocate UB/SHM channels and serve stats/close commands.
void vemb_v16_aeron_handle_control_fd(vemb_v16_proxy_t *proxy, int fd) {
    uint8_t op = 0;
    if (read(fd, &op, 1) != 1) goto close_fd;

    if (op == VEMB_V16_CTRL_PING) {
        uint8_t ok = 0;
        write_full(fd, &ok, sizeof(ok));
    } else if (op == VEMB_V16_CTRL_ALLOC_CHANNEL) {
        vemb_v16_alloc_req_t req;
        GOTO_IF(read_full(fd, &req, sizeof(req)) != 0, close_fd);
        vemb_v16_channel_desc_t desc;
        uint8_t status = vemb_v16_proxy_alloc_shm_channel(proxy, &desc) == 0 ? VEMB_V16_STATUS_OK : VEMB_V16_STATUS_ERR;
        if (status != VEMB_V16_STATUS_OK)
            serverLog(LL_WARNING, "vemb_v16 alloc channel failed");
        write_full(fd, &status, sizeof(status));
        if (status == VEMB_V16_STATUS_OK)
            write_full(fd, &desc, sizeof(desc));
    } else if (op == VEMB_V16_CTRL_STATS) {
        uint8_t status = VEMB_V16_STATUS_OK;
        vemb_v16_stats_t stats;
        vemb_v16_proxy_get_stats(proxy, &stats);
        write_full(fd, &status, sizeof(status));
        write_full(fd, &stats, sizeof(stats));
    } else if (op == VEMB_V16_CTRL_CLOSE_CHANNEL) {
        uint64_t channel_id = 0;
        if (read_full(fd, &channel_id, sizeof(channel_id)) != 0) goto close_fd;
        uint8_t status = vemb_v16_proxy_close_channel_by_id(proxy, channel_id) == 0 ?
            VEMB_V16_STATUS_OK : VEMB_V16_STATUS_ERR;
        write_full(fd, &status, sizeof(status));
    } else if (op == VEMB_V16_CTRL_CLOSE_ALL_CHANNELS) {
        uint64_t closed = vemb_v16_proxy_close_all_channels(proxy);
        uint8_t status = VEMB_V16_STATUS_OK;
        write_full(fd, &status, sizeof(status));
        write_full(fd, &closed, sizeof(closed));
    } else if (op == VEMB_V16_CTRL_MIGRATION_MARK_MIGRATING ||
               op == VEMB_V16_CTRL_MIGRATION_MARK_CUTOVER ||
               op == VEMB_V16_CTRL_MIGRATION_MARK_SOURCE_GC ||
               op == VEMB_V16_CTRL_MIGRATION_BARRIER) {
        vemb_v16_migration_control_req_t req;
        vemb_v16_migration_control_resp_t resp;
        memset(&req, 0, sizeof(req));
        memset(&resp, 0, sizeof(resp));
        GOTO_IF(read_full(fd, &req, sizeof(req)) != 0, close_fd);
        if (op == VEMB_V16_CTRL_MIGRATION_MARK_MIGRATING) {
            vemb_v16_proxy_migration_mark_migrating(proxy, &req, &resp);
        } else if (op == VEMB_V16_CTRL_MIGRATION_MARK_CUTOVER) {
            vemb_v16_proxy_migration_mark_cutover(proxy, &req, &resp);
        } else if (op == VEMB_V16_CTRL_MIGRATION_MARK_SOURCE_GC) {
            vemb_v16_proxy_migration_mark_source_gc(proxy, &req, &resp);
        } else {
            vemb_v16_proxy_migration_barrier(proxy, &req, &resp);
        }
        write_full(fd, &resp, sizeof(resp));
    } else if (op == VEMB_V16_CTRL_MIGRATION_MARK_MIGRATING_BATCH) {
        vemb_v16_migration_control_batch_req_t req;
        vemb_v16_migration_control_batch_resp_t resp;
        memset(&req, 0, sizeof(req));
        memset(&resp, 0, sizeof(resp));
        GOTO_IF(read_full(fd, &req, sizeof(req)) != 0, close_fd);
       vemb_v16_proxy_migration_mark_migrating_batch(proxy, &req, &resp);
        write_full(fd, &resp, sizeof(resp));
    } else if (op == VEMB_V16_CTRL_MIGRATION_RANGE_BARRIER ||
               op == VEMB_V16_CTRL_MIGRATION_RANGE_MARK_CUTOVER ||
               op == VEMB_V16_CTRL_MIGRATION_RANGE_SOURCE_GC) {
        vemb_v16_migration_range_control_req_t req;
        vemb_v16_migration_range_control_resp_t resp;
        memset(&req, 0, sizeof(req));
        memset(&resp, 0, sizeof(resp));
        GOTO_IF(read_full(fd, &req, sizeof(req)) != 0, close_fd);
        if (op == VEMB_V16_CTRL_MIGRATION_RANGE_BARRIER) {
           vemb_v16_proxy_migration_range_barrier(proxy, &req, &resp);
        } else if (op == VEMB_V16_CTRL_MIGRATION_RANGE_MARK_CUTOVER) {
           vemb_v16_proxy_migration_range_mark_cutover(proxy, &req, &resp);
        } else {
           vemb_v16_proxy_migration_range_mark_source_gc(proxy, &req, &resp);
        }
        write_full(fd, &resp, sizeof(resp));
    } else if (op == VEMB_V16_CTRL_EPOCH_SET) {
        vemb_v16_epoch_control_req_t req;
        vemb_v16_epoch_control_resp_t resp;
        memset(&req, 0, sizeof(req));
        memset(&resp, 0, sizeof(resp));
        GOTO_IF(read_full(fd, &req, sizeof(req)) != 0, close_fd);
       vemb_v16_proxy_epoch_set(proxy, &req, &resp);
        write_full(fd, &resp, sizeof(resp));
    } else if (op == VEMB_V16_CTRL_EPOCH_GET) {
        vemb_v16_epoch_control_resp_t resp;
        memset(&resp, 0, sizeof(resp));
       vemb_v16_proxy_epoch_get(proxy, &resp);
        write_full(fd, &resp, sizeof(resp));
    } else if (op == VEMB_V16_CTRL_TOPOLOGY_SET) {
        vemb_v16_topology_control_req_t req;
        vemb_v16_topology_control_resp_t resp;
        memset(&req, 0, sizeof(req));
        memset(&resp, 0, sizeof(resp));
        GOTO_IF(read_full(fd, &req, sizeof(req)) != 0, close_fd);
       vemb_v16_proxy_topology_set(proxy, &req, &resp);
        write_full(fd, &resp, sizeof(resp));
    } else if (op == VEMB_V16_CTRL_TOPOLOGY_GET) {
        vemb_v16_topology_control_resp_t resp;
        memset(&resp, 0, sizeof(resp));
       vemb_v16_proxy_topology_get(proxy, &resp);
        write_full(fd, &resp, sizeof(resp));
    }

close_fd:
    close(fd);
}
