#define _GNU_SOURCE

#include "vemb_v16_tcp_transport.h"
#include "vemb_v16_log.h"
#include "vemb_v16_net.h"
#include "redisassert.h"
#include "zmalloc.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

/// TCP transport implementation.

#ifdef __linux__
/// TCP transport: queue partial response writes when clients apply backpressure.
static int tcp_response_backlog_pending(vemb_v16_channel_t *ch) {
    return vemb_v16_tcp_backlog_pending(ch);
}

static int ensure_tcp_response_backlog_capacity(vemb_v16_channel_t *ch,
                                                size_t append_bytes) {
    if (!ch)
        return -1;
    size_t pending = vemb_v16_tcp_backlog_pending_bytes(ch);
    if (append_bytes > VEMB_V16_TCP_RESPONSE_BACKLOG_LIMIT ||
        pending > VEMB_V16_TCP_RESPONSE_BACKLOG_LIMIT - append_bytes) {
        return -1;
    }
    vemb_v16_tcp_backlog_compact(ch, pending);
    if (vemb_v16_tcp_backlog_capacity(ch) >= pending + append_bytes)
        return 0;

    size_t next_cap = vemb_v16_tcp_backlog_capacity(ch) ?
        vemb_v16_tcp_backlog_capacity(ch) : 4096u;
    while (next_cap < pending + append_bytes) {
        next_cap <<= 1;
        if (next_cap > VEMB_V16_TCP_RESPONSE_BACKLOG_LIMIT) {
            next_cap = VEMB_V16_TCP_RESPONSE_BACKLOG_LIMIT;
            break;
        }
    }
    if (next_cap < pending + append_bytes)
        return -1;
    uint8_t *next = zrealloc(vemb_v16_tcp_backlog_buffer(ch), next_cap);
    if (!next)
        return -1;
    vemb_v16_tcp_backlog_set_buffer(ch, next, next_cap);
    return 0;
}

static int append_tcp_response_backlog(vemb_v16_channel_t *ch,
                                       const void *buf,
                                       size_t len) {
    if (!ch || (!buf && len != 0))
        return -1;
    if (ensure_tcp_response_backlog_capacity(ch, len) != 0)
        return -1;
    memcpy(vemb_v16_tcp_backlog_buffer(ch) +
               vemb_v16_tcp_backlog_pending_bytes(ch),
           buf,
           len);
    vemb_v16_tcp_backlog_append_done(ch, len);
    return 0;
}

/// Copy the portion of an iovec starting at byte offset 'skip' into the backlog.
static int append_iovec_remainder_to_backlog(vemb_v16_channel_t *ch,
                                             const struct iovec *iov,
                                             int iovcnt,
                                             size_t skip) {
    size_t total = 0;
    for (int i = 0; i < iovcnt; i++)
        total += iov[i].iov_len;
    if (skip >= total)
        return 0;

    size_t remain = total - skip;
    uint8_t *buf = zmalloc(remain);
    if (!buf)
        return -1;

    size_t off = 0;
    size_t consumed = skip;
    for (int i = 0; i < iovcnt && off < remain; i++) {
        if (consumed >= iov[i].iov_len) {
            consumed -= iov[i].iov_len;
            continue;
        }
        size_t src_off = consumed;
        consumed = 0;
        size_t to_copy = iov[i].iov_len - src_off;
        if (to_copy > remain - off)
            to_copy = remain - off;
        memcpy(buf + off, (const char *)iov[i].iov_base + src_off, to_copy);
        off += to_copy;
    }

    int rc = append_tcp_response_backlog(ch, buf, off);
    zfree(buf);
    return rc;
}

static ssize_t tcp_send_nonblocking(int fd, const void *buf, size_t len) {
    if (len == 0)
        return 0;
    ssize_t n = send(fd, buf, len, MSG_DONTWAIT);
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        return 0;
    return n;
}

int vemb_v16_tcp_flush_response_backlog(vemb_v16_channel_t *ch) {
    if (!ch || vemb_v16_channel_net_fd(ch) < 0 || !tcp_response_backlog_pending(ch))
        return 1;

    size_t pending = vemb_v16_tcp_backlog_pending_bytes(ch);
    ssize_t n = tcp_send_nonblocking(vemb_v16_channel_net_fd(ch),
                                     vemb_v16_tcp_backlog_pending_ptr(ch),
                                     pending);
    if (n < 0)
        return -1;
    vemb_v16_tcp_backlog_consume(ch, (size_t)n);
    if (!tcp_response_backlog_pending(ch)) {
        vemb_v16_tcp_backlog_reset(ch);
        return 1;
    }
    return 0;
}

/// TCP transport: write one completion response to the socket.
int vemb_v16_tcp_publish_response(vemb_v16_channel_t *ch, vemb_v16_resp_t *resp) {
    const uint8_t *vector = NULL;
    uint32_t vector_bytes = 0;
    vemb_v16_proxy_tcp_response_vector_slice(ch, resp, &vector, &vector_bytes);

    vemb_v16_net_hdr_t hdr = {
        .magic = VEMB_V16_MAGIC,
        .version = VEMB_V16_VERSION,
        .type = VEMB_V16_NET_RESPONSE,
        .flags = vector_bytes ? VEMB_V16_NET_F_INLINE_VECTOR : 0,
        .payload_len = (uint32_t)sizeof(*resp) + vector_bytes,
        .channel_id = vemb_v16_channel_id(ch),
        .req_id = resp->req_id,
    };

    struct iovec iov[3];
    int iovcnt = 0;
    iov[iovcnt++] = (struct iovec){ .iov_base = &hdr, .iov_len = sizeof(hdr) };
    iov[iovcnt++] = (struct iovec){ .iov_base = resp, .iov_len = sizeof(*resp) };
    if (vector_bytes) {
        iov[iovcnt++] = (struct iovec){ .iov_base = (void *)vector,
                                        .iov_len = vector_bytes };
    }

    if (!vemb_v16_channel_tcp_backpressure_enabled(ch)) {
        return vemb_v16_net_writev_full(vemb_v16_channel_net_fd(ch), iov, iovcnt);
    }

#ifdef __linux__
    size_t total = sizeof(hdr) + sizeof(*resp) + vector_bytes;
    if (tcp_response_backlog_pending(ch)) {
        /* Preserve ordering: append to existing backlog. */
        return append_iovec_remainder_to_backlog(ch, iov, iovcnt, 0);
    }

    ssize_t sent = vemb_v16_net_writev_nonblocking(vemb_v16_channel_net_fd(ch),
                                                   iov, iovcnt);
    if (sent < 0)
        return -1;
    if ((size_t)sent == total)
        return 0;

    /* Partial send or EAGAIN: queue the unsent remainder. */
    return append_iovec_remainder_to_backlog(ch, iov, iovcnt, (size_t)sent);
#else
    return -1;
#endif
}

/// TCP transport: batch completion responses into writev/backlog output.
int vemb_v16_tcp_publish_response_batch(vemb_v16_channel_t *ch,
                               const vemb_v16_completion_t *completions,
                               uint32_t n,
                               uint32_t *published) {
    vemb_v16_resp_t responses[VEMB_V16_PROXY_BATCH];
    vemb_v16_net_hdr_t headers[VEMB_V16_PROXY_BATCH];
    struct iovec iov[VEMB_V16_PROXY_BATCH * 3u];
    int iovcnt = 0;
    uint32_t out = 0;
    size_t total_bytes = 0;

    for (uint32_t i = 0; i < n; i++) {
        if (completions[i].channel_id != vemb_v16_channel_id(ch) ||
            !vemb_v16_channel_active(ch)) {
            continue;
        }

        vemb_v16_make_response_from(&responses[out], &completions[i]);
        const uint8_t *vector = NULL;
        uint32_t vector_bytes = 0;
        vemb_v16_proxy_tcp_response_vector_slice(ch, &responses[out],
                                                 &vector, &vector_bytes);

        headers[out] = (vemb_v16_net_hdr_t){
            .magic = VEMB_V16_MAGIC,
            .version = VEMB_V16_VERSION,
            .type = VEMB_V16_NET_RESPONSE,
            .flags = vector_bytes ? VEMB_V16_NET_F_INLINE_VECTOR : 0,
            .payload_len = (uint32_t)sizeof(responses[out]) + vector_bytes,
            .channel_id = vemb_v16_channel_id(ch),
            .req_id = responses[out].req_id,
        };
        iov[iovcnt++] = (struct iovec){ .iov_base = &headers[out],
                                        .iov_len = sizeof(headers[out]) };
        iov[iovcnt++] = (struct iovec){ .iov_base = &responses[out],
                                        .iov_len = sizeof(responses[out]) };
        if (vector_bytes) {
            iov[iovcnt++] = (struct iovec){ .iov_base = (void *)vector,
                                            .iov_len = vector_bytes };
        }
        total_bytes += sizeof(headers[out]) + sizeof(responses[out]) + vector_bytes;
        out++;
    }

    if (published) *published = out;
    if (out == 0)
        return 0;

    if (!vemb_v16_channel_tcp_backpressure_enabled(ch)) {
        return vemb_v16_net_writev_full(vemb_v16_channel_net_fd(ch), iov, iovcnt);
    }

#ifdef __linux__
    if (tcp_response_backlog_pending(ch)) {
        /* Preserve ordering: append to existing backlog. */
        return append_iovec_remainder_to_backlog(ch, iov, iovcnt, 0);
    }

    ssize_t sent = vemb_v16_net_writev_nonblocking(vemb_v16_channel_net_fd(ch),
                                                   iov, iovcnt);
    if (sent < 0)
        return -1;
    if ((size_t)sent == total_bytes)
        return 0;

    /* Partial send or EAGAIN: queue the unsent remainder. */
    return append_iovec_remainder_to_backlog(ch, iov, iovcnt, (size_t)sent);
#else
    return -1;
#endif
}
#endif

static int tcp_poll_input(int fd) {
    if (fd < 0) return -1;
    struct pollfd pfd = {
        .fd = fd,
        .events = POLLIN,
    };
    int pr = poll(&pfd, 1, 0);
    if (pr < 0) return errno == EINTR ? 0 : -1;
    if (pr == 0) return 0;
    if (pfd.revents & (POLLERR | POLLNVAL))
        return -1;
    if (pfd.revents & POLLIN)
        return 1;
    if (pfd.revents & POLLHUP)
        return -1;
    return 0;
}

/// TCP transport: read one request frame and hand it to the scheduler.
static int channel_read_tcp_request(vemb_v16_channel_t *ch,
                                    uint32_t proxy_io_worker_id) {
    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(vemb_v16_channel_net_fd(ch), &hdr) != 0)
        return -1;
    if (hdr.type == VEMB_V16_NET_CLOSE)
        return -1;
    if (hdr.type != VEMB_V16_NET_REQUEST ||
        hdr.channel_id != vemb_v16_channel_id(ch) ||
        hdr.payload_len == 0 ||
        hdr.payload_len > sizeof(vemb_v16_req_t)) {
        return -1;
    }

    vemb_v16_req_t *req = zmalloc(sizeof(*req));
    if (!req)
        return -1;
    memset(req, 0, sizeof(*req));
    if (vemb_v16_net_read_full(vemb_v16_channel_net_fd(ch), req, hdr.payload_len) != 0) {
        zfree(req);
        return -1;
    }
    vemb_v16_proxy_handle_request(ch, req, (int)hdr.payload_len, proxy_io_worker_id);
    zfree(req);
    if (vemb_v16_channel_net_fd(ch) < 0)
        return -1;
    return 1;
}

/// TCP transport: read a bounded batch of already-ready request frames.
int vemb_v16_tcp_read_ready_requests(vemb_v16_channel_t *ch,
                                    uint32_t proxy_io_worker_id) {
    uint32_t count = 0;
    int ready = 0;
    while (count < VEMB_V16_PROXY_BATCH) {
        if (channel_read_tcp_request(ch, proxy_io_worker_id) < 0)
            return -1;
        count++;
        if (count >= VEMB_V16_PROXY_BATCH)
            break;
        ready = tcp_poll_input(vemb_v16_channel_net_fd(ch));
        if (ready < 0)
            return -1;
        if (ready == 0)
            break;
    }

    vemb_v16_channel_add_proxy_request_poll(ch, count);
    vemb_v16_channel_add_channel_ops(ch, count);
    return (int)count;
}

static void tcp_write_status(int fd, uint8_t status, uint64_t value) {
    vemb_v16_net_status_t st = {
        .status = status,
        .value = value,
    };
    vemb_v16_net_write_frame(fd,
                             VEMB_V16_NET_CONTROL_STATUS,
                             0,
                             0,
                             0,
                             &st,
                             sizeof(st));
}

/// TCP control plane: process one accepted TCP control or channel setup socket.
void vemb_v16_tcp_handle_fd(vemb_v16_proxy_t *proxy, int fd) {
    assert(proxy != NULL);
    if (fd < 0) return;
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0)
        fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
    vemb_v16_net_set_tcp_nodelay(fd);
    vemb_v16_net_set_timeouts(fd, 10000);

    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0) {
        close(fd);
        return;
    }

    if (hdr.type == VEMB_V16_NET_STATS) {
        if (hdr.payload_len != 0) {
            close(fd);
            return;
        }
        vemb_v16_stats_t stats;
        vemb_v16_proxy_get_stats(proxy, &stats);
        vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_STATS,
                                 0,
                                 0,
                                 0,
                                 &stats,
                                 sizeof(stats));
        close(fd);
        return;
    }

    if (hdr.type == VEMB_V16_NET_CLOSE_CHANNEL) {
        if (hdr.payload_len != 0) {
            close(fd);
            return;
        }
        uint8_t status = vemb_v16_proxy_close_channel_by_id(proxy, hdr.channel_id) == 0 ?
            VEMB_V16_STATUS_OK : VEMB_V16_STATUS_ERR;
        tcp_write_status(fd, status, 0);
        close(fd);
        return;
    }

    if (hdr.type == VEMB_V16_NET_CLOSE_ALL_CHANNELS) {
        if (hdr.payload_len != 0) {
            close(fd);
            return;
        }
        uint64_t closed = vemb_v16_proxy_close_all_channels(proxy);
        tcp_write_status(fd, VEMB_V16_STATUS_OK, closed);
        close(fd);
        return;
    }

    vemb_v16_alloc_req_t req;
    if (hdr.type != VEMB_V16_NET_HELLO ||
        hdr.payload_len != sizeof(req) ||
        vemb_v16_net_read_full(fd, &req, sizeof(req)) != 0) {
        close(fd);
        return;
    }
    (void)req;

    vemb_v16_channel_desc_t desc;
    if (vemb_v16_proxy_alloc_tcp_channel(proxy, fd, &desc) != 0) {
        return;
    }
    if (vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_WELCOME,
                                 0,
                                 desc.channel_id,
                                 0,
                                 &desc,
                                 sizeof(desc)) != 0) {
        vemb_v16_proxy_close_channel_by_id(proxy, desc.channel_id);
    }
}
