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

int vemb_v16_tcp_listen(vemb_v16_proxy_t *proxy,
                        int backlog,
                        vemb_v16_transport_listener_t *listener) {
    const char *host = vemb_v16_proxy_tcp_host(proxy);
    uint16_t port = vemb_v16_proxy_tcp_port(proxy);
    int fd = vemb_v16_net_listen(host, port, backlog);
    if (fd < 0) {
        serverLog(LL_WARNING, "vemb_v16 tcp listen failed: %s:%u errno=%d error=%s",
                  host, port, errno, strerror(errno));
        return -1;
    }
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);

    *listener = (vemb_v16_transport_listener_t){
        .name = "tcp",
        .fd = fd,
        .handle_fd = vemb_v16_tcp_handle_fd,
    };
    return 0;
}

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

/// TCP transport: compute one response frame size.
static size_t tcp_response_wire_size(vemb_v16_resp_t *resp,
                                     uint32_t vector_bytes) {
    (void)resp;
    return sizeof(vemb_v16_net_hdr_t) + sizeof(vemb_v16_resp_t) + vector_bytes;
}

/// TCP transport: encode one response frame, optionally including inline vector bytes.
static uint8_t *encode_tcp_response_bytes(vemb_v16_channel_t *ch,
                                          vemb_v16_resp_t *resp,
                                          size_t *out_len) {
    const uint8_t *vector = NULL;
    uint32_t vector_bytes = 0;
    vemb_v16_proxy_tcp_response_vector_slice(ch, resp, &vector, &vector_bytes);

    size_t bytes = tcp_response_wire_size(resp, vector_bytes);
    uint8_t *buf = zmalloc(bytes);
    if (!buf)
        return NULL;

    vemb_v16_net_hdr_t hdr = {
        .magic = VEMB_V16_MAGIC,
        .version = VEMB_V16_VERSION,
        .type = VEMB_V16_NET_RESPONSE,
        .flags = vector_bytes ? VEMB_V16_NET_F_INLINE_VECTOR : 0,
        .payload_len = (uint32_t)sizeof(*resp) + vector_bytes,
        .channel_id = vemb_v16_channel_id(ch),
        .req_id = resp->req_id,
    };
    size_t off = 0;
    memcpy(buf + off, &hdr, sizeof(hdr));
    off += sizeof(hdr);
    memcpy(buf + off, resp, sizeof(*resp));
    off += sizeof(*resp);
    if (vector_bytes) {
        memcpy(buf + off, vector, vector_bytes);
        off += vector_bytes;
    }
    if (out_len) *out_len = off;
    return buf;
}

/// TCP transport: encode a batch of response frames for nonblocking writes.
static uint8_t *encode_tcp_response_batch(vemb_v16_channel_t *ch,
                                          const vemb_v16_completion_t *completions,
                                          uint32_t n,
                                          uint32_t *published,
                                          size_t *out_len) {
    vemb_v16_resp_t responses[VEMB_V16_PROXY_BATCH];
    const uint8_t *vectors[VEMB_V16_PROXY_BATCH];
    uint32_t vector_bytes[VEMB_V16_PROXY_BATCH];
    vemb_v16_net_hdr_t headers[VEMB_V16_PROXY_BATCH];
    uint32_t out = 0;
    size_t total_bytes = 0;

    for (uint32_t i = 0; i < n; i++) {
        if (completions[i].channel_id != vemb_v16_channel_id(ch) ||
            !vemb_v16_channel_active(ch)) {
            continue;
        }
        vemb_v16_make_response_from(&responses[out], &completions[i]);
        vectors[out] = NULL;
        vector_bytes[out] = 0;
        vemb_v16_proxy_tcp_response_vector_slice(ch, &responses[out], &vectors[out], &vector_bytes[out]);
        headers[out] = (vemb_v16_net_hdr_t){
            .magic = VEMB_V16_MAGIC,
            .version = VEMB_V16_VERSION,
            .type = VEMB_V16_NET_RESPONSE,
            .flags = vector_bytes[out] ? VEMB_V16_NET_F_INLINE_VECTOR : 0,
            .payload_len = (uint32_t)sizeof(responses[out]) + vector_bytes[out],
            .channel_id = vemb_v16_channel_id(ch),
            .req_id = responses[out].req_id,
        };
        total_bytes += sizeof(headers[out]) + sizeof(responses[out]) + vector_bytes[out];
        out++;
    }

    if (published) *published = out;
    if (out_len) *out_len = total_bytes;
    if (out == 0)
        return NULL;

    uint8_t *buf = zmalloc(total_bytes);
    if (!buf)
        return NULL;
    size_t off = 0;
    for (uint32_t i = 0; i < out; i++) {
        memcpy(buf + off, &headers[i], sizeof(headers[i]));
        off += sizeof(headers[i]);
        memcpy(buf + off, &responses[i], sizeof(responses[i]));
        off += sizeof(responses[i]);
        if (vector_bytes[i]) {
            memcpy(buf + off, vectors[i], vector_bytes[i]);
            off += vector_bytes[i];
        }
    }
    return buf;
}
#endif

/// TCP transport: write one completion response to the socket.
int vemb_v16_tcp_publish_response(vemb_v16_channel_t *ch, vemb_v16_resp_t *resp) {
    if (!vemb_v16_channel_tcp_backpressure_enabled(ch)) {
        const uint8_t *vector = NULL;
        uint32_t vector_bytes = 0;
        vemb_v16_proxy_tcp_response_vector_slice(ch, resp, &vector, &vector_bytes);
        return vemb_v16_net_write_frame2(vemb_v16_channel_net_fd(ch),
                                         VEMB_V16_NET_RESPONSE,
                                         vector_bytes ? VEMB_V16_NET_F_INLINE_VECTOR : 0,
                                         vemb_v16_channel_id(ch),
                                         resp->req_id,
                                         resp,
                                         (uint32_t)sizeof(*resp),
                                         vector,
                                         vector_bytes);
    }

#ifdef __linux__
    size_t bytes = 0;
    uint8_t *buf = encode_tcp_response_bytes(ch, resp, &bytes);
    if (!buf)
        return -1;
    int rc = 0;
    if (tcp_response_backlog_pending(ch)) {
        rc = append_tcp_response_backlog(ch, buf, bytes);
    } else {
        ssize_t n = tcp_send_nonblocking(vemb_v16_channel_net_fd(ch), buf, bytes);
        if (n < 0) {
            rc = -1;
        } else if ((size_t)n < bytes) {
            rc = append_tcp_response_backlog(ch, buf + n, bytes - (size_t)n);
        }
    }
    zfree(buf);
    return rc;
#else
    return -1;
#endif
}

/// TCP transport: batch completion responses into writev/backlog output.
int vemb_v16_tcp_publish_response_batch(vemb_v16_channel_t *ch,
                               const vemb_v16_completion_t *completions,
                               uint32_t n,
                               uint32_t *published) {
    if (!vemb_v16_channel_tcp_backpressure_enabled(ch)) {
        vemb_v16_resp_t *responses =
            zmalloc(sizeof(*responses) * VEMB_V16_PROXY_BATCH);
        vemb_v16_net_hdr_t *headers =
            zmalloc(sizeof(*headers) * VEMB_V16_PROXY_BATCH);
        struct iovec *iov =
            zmalloc(sizeof(*iov) * VEMB_V16_PROXY_BATCH * 3u);
        if (!responses || !headers || !iov) {
            zfree(responses);
            zfree(headers);
            zfree(iov);
            if (published) *published = 0;
            return -1;
        }
        int iovcnt = 0;
        uint32_t out = 0;

        for (uint32_t i = 0; i < n; i++) {
            if (completions[i].channel_id != vemb_v16_channel_id(ch) ||
                !vemb_v16_channel_active(ch)) {
                continue;
            }

            vemb_v16_make_response_from(&responses[out], &completions[i]);
            const uint8_t *vector = NULL;
            uint32_t vector_bytes = 0;
            vemb_v16_proxy_tcp_response_vector_slice(ch, &responses[out], &vector, &vector_bytes);

            headers[out] = (vemb_v16_net_hdr_t){
                .magic = VEMB_V16_MAGIC,
                .version = VEMB_V16_VERSION,
                .type = VEMB_V16_NET_RESPONSE,
                .flags = vector_bytes ? VEMB_V16_NET_F_INLINE_VECTOR : 0,
                .payload_len = (uint32_t)sizeof(responses[out]) + vector_bytes,
                .channel_id = vemb_v16_channel_id(ch),
                .req_id = responses[out].req_id,
            };
            iov[iovcnt++] = (struct iovec){ .iov_base = &headers[out], .iov_len = sizeof(headers[out]) };
            iov[iovcnt++] = (struct iovec){ .iov_base = &responses[out], .iov_len = sizeof(responses[out]) };
            if (vector_bytes) {
                iov[iovcnt++] = (struct iovec){ .iov_base = (void *)vector, .iov_len = vector_bytes };
            }
            out++;
        }

        if (published) *published = out;
        if (out == 0) {
            zfree(responses);
            zfree(headers);
            zfree(iov);
            return 0;
        }
        int rc = vemb_v16_net_writev_full(vemb_v16_channel_net_fd(ch), iov, iovcnt);
        zfree(responses);
        zfree(headers);
        zfree(iov);
        return rc;
    }

#ifdef __linux__
    size_t bytes = 0;
    uint32_t out = 0;
    uint8_t *buf = encode_tcp_response_batch(ch, completions, n, &out, &bytes);
    if (published) *published = out;
    if (out == 0)
        return 0;
    if (!buf)
        return -1;

    int rc = 0;
    if (tcp_response_backlog_pending(ch)) {
        rc = append_tcp_response_backlog(ch, buf, bytes);
    } else {
        ssize_t sent = tcp_send_nonblocking(vemb_v16_channel_net_fd(ch), buf, bytes);
        if (sent < 0) {
            rc = -1;
        } else if ((size_t)sent < bytes) {
            rc = append_tcp_response_backlog(ch, buf + sent, bytes - (size_t)sent);
        }
    }
    zfree(buf);
    return rc;
#else
    if (published) *published = 0;
    return -1;
#endif
}

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
