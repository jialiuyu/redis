#define _GNU_SOURCE

#include "vemb_v16_net.h"
#include "macro.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <unistd.h>

#define VEMB_V16_NET_MAX_IOV 128

int vemb_v16_net_set_timeouts(int fd, uint32_t timeout_ms) {
    if (fd < 0 || timeout_ms == 0) return 0;
    struct timeval tv = {
        .tv_sec = (time_t)(timeout_ms / 1000),
        .tv_usec = (suseconds_t)((timeout_ms % 1000) * 1000),
    };
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) != 0)
        return -1;
    if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) != 0)
        return -1;
    return 0;
}

int vemb_v16_net_set_tcp_nodelay(int fd) {
    if (fd < 0) return -1;
    int one = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

static int vemb_v16_addr4(const char *host,
                          uint16_t port,
                          struct sockaddr_in *addr) {
    memset(addr, 0, sizeof(*addr));
    addr->sin_family = AF_INET;
    addr->sin_port = htons(port);
    const char *h = host && host[0] ? host : VEMB_V16_TCP_HOST;
    if (inet_pton(AF_INET, h, &addr->sin_addr) != 1)
        return -1;
    return 0;
}

int vemb_v16_net_connect(const char *host, uint16_t port, uint32_t timeout_ms) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    vemb_v16_net_set_timeouts(fd, timeout_ms);
    vemb_v16_net_set_tcp_nodelay(fd);

    struct sockaddr_in addr;
    if (vemb_v16_addr4(host, port, &addr) != 0 ||
        connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

int vemb_v16_net_listen(const char *host, uint16_t port, int backlog) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));

    struct sockaddr_in addr;
    if (vemb_v16_addr4(host, port, &addr) != 0 ||
        bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 ||
        listen(fd, backlog > 0 ? backlog : 4096) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

int vemb_v16_net_read_full(int fd, void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = read(fd, (char *)buf + done, n - done);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return -1;
        done += (size_t)r;
    }
    return 0;
}

int vemb_v16_net_write_full(int fd, const void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = write(fd, (const char *)buf + done, n - done);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return -1;
        done += (size_t)r;
    }
    return 0;
}

static int make_iov(struct iovec *dst, const struct iovec *src, int iovcnt) {
    if (iovcnt <= 0 || iovcnt > VEMB_V16_NET_MAX_IOV)
        return -1;
    int out = 0;
    for (int i = 0; i < iovcnt; i++) {
        if (src[i].iov_len == 0) continue;
        if (!src[i].iov_base) return -1;
        dst[out++] = src[i];
    }
    return out > 0 ? out : -1;
}

static void advance_iov(struct iovec **iov, int *iovcnt, size_t bytes) {
    while (*iovcnt > 0 && bytes >= (*iov)->iov_len) {
        bytes -= (*iov)->iov_len;
        (*iov)++;
        (*iovcnt)--;
    }
    if (*iovcnt > 0 && bytes > 0) {
        (*iov)->iov_base = (char *)(*iov)->iov_base + bytes;
        (*iov)->iov_len -= bytes;
    }
}

int vemb_v16_net_readv_full(int fd, const struct iovec *iov, int iovcnt) {
    struct iovec local[VEMB_V16_NET_MAX_IOV];
    int nlocal = make_iov(local, iov, iovcnt);
    RETURN_IF(nlocal < 0, -1);

    struct iovec *cur = local;
    int curcnt = nlocal;
    while (curcnt > 0) {
        ssize_t r = readv(fd, cur, curcnt);
        if (r < 0 && errno == EINTR) continue;
        RETURN_IF(r <= 0, -1);
        advance_iov(&cur, &curcnt, (size_t)r);
    }
    return 0;
}

int vemb_v16_net_writev_full(int fd, const struct iovec *iov, int iovcnt) {
    struct iovec local[VEMB_V16_NET_MAX_IOV];
    int nlocal = make_iov(local, iov, iovcnt);
    RETURN_IF(nlocal < 0, -1);

    struct iovec *cur = local;
    int curcnt = nlocal;
    while (curcnt > 0) {
        ssize_t r = writev(fd, cur, curcnt);
        if (r < 0 && errno == EINTR) continue;
        RETURN_IF(r <= 0, -1);
        advance_iov(&cur, &curcnt, (size_t)r);
    }
    return 0;
}

ssize_t vemb_v16_net_writev_nonblocking(int fd, const struct iovec *iov, int iovcnt) {
    struct iovec local[VEMB_V16_NET_MAX_IOV];
    int nlocal = make_iov(local, iov, iovcnt);
    if (nlocal < 0) {
        return -1;
    }

    struct msghdr msg = {
        .msg_iov = local,
        .msg_iovlen = nlocal,
    };
    ssize_t r = sendmsg(fd, &msg, MSG_DONTWAIT);
    if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return 0;
    }
    return r;
}

int vemb_v16_net_read_header(int fd, vemb_v16_net_hdr_t *hdr) {
    RETURN_IF(vemb_v16_net_read_full(fd, hdr, sizeof(*hdr)) != 0, -1);
    RETURN_IF(hdr->magic != VEMB_V16_MAGIC || hdr->version != VEMB_V16_VERSION, -1);
    return 0;
}

int vemb_v16_net_write_frame(int fd,
                             uint16_t type,
                             uint32_t flags,
                             uint64_t channel_id,
                             uint32_t req_id,
                             const void *payload,
                             uint32_t payload_len) {
    vemb_v16_net_hdr_t hdr = {
        .magic = VEMB_V16_MAGIC,
        .version = VEMB_V16_VERSION,
        .type = type,
        .flags = flags,
        .payload_len = payload_len,
        .channel_id = channel_id,
        .req_id = req_id,
    };
    struct iovec iov[2];
    int iovcnt = 0;
    iov[iovcnt++] = (struct iovec){.iov_base = &hdr, .iov_len = sizeof(hdr)};
    if (payload_len)
        iov[iovcnt++] = (struct iovec){
            .iov_base = (void *)payload,
            .iov_len = payload_len,
        };
    return vemb_v16_net_writev_full(fd, iov, iovcnt);
}

const char *vemb_v16_transport_name(uint32_t transport) {
    switch (transport) {
    case VEMB_V16_TRANSPORT_AERON: return "aeron";
    case VEMB_V16_TRANSPORT_TCP: return "tcp";
    default: return "unknown";
    }
}
