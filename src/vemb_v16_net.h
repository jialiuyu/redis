#ifndef __VEMB_V16_NET_H
#define __VEMB_V16_NET_H

#include "vemb_v16_protocol.h"

#include <stddef.h>
#include <stdint.h>
#include <sys/uio.h>

int vemb_v16_net_set_timeouts(int fd, uint32_t timeout_ms);
int vemb_v16_net_set_tcp_nodelay(int fd);
int vemb_v16_net_connect(const char *host, uint16_t port, uint32_t timeout_ms);
int vemb_v16_net_listen(const char *host, uint16_t port, int backlog);
int vemb_v16_net_read_full(int fd, void *buf, size_t n);
int vemb_v16_net_write_full(int fd, const void *buf, size_t n);
int vemb_v16_net_readv_full(int fd, const struct iovec *iov, int iovcnt);
int vemb_v16_net_writev_full(int fd, const struct iovec *iov, int iovcnt);
ssize_t vemb_v16_net_writev_nonblocking(int fd, const struct iovec *iov, int iovcnt);
int vemb_v16_net_read_header(int fd, vemb_v16_net_hdr_t *hdr);
int vemb_v16_net_write_frame(int fd,
                             uint16_t type,
                             uint32_t flags,
                             uint64_t channel_id,
                             uint32_t req_id,
                             const void *payload,
                             uint32_t payload_len);
const char *vemb_v16_transport_name(uint32_t transport);

#endif
