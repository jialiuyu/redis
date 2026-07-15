#ifndef __VEMB_V16_TCP_TRANSPORT_H
#define __VEMB_V16_TCP_TRANSPORT_H

#include "vemb_v16_proxy_internal.h"

int vemb_v16_tcp_flush_response_backlog(vemb_v16_channel_t *ch);
int vemb_v16_tcp_listen(vemb_v16_proxy_t *proxy,
                        int backlog,
                        vemb_v16_transport_listener_t *listener);
int vemb_v16_tcp_publish_response(vemb_v16_channel_t *ch,
                                  vemb_v16_resp_t *resp);
int vemb_v16_tcp_publish_response_batch(vemb_v16_channel_t *ch,
                                        const vemb_v16_completion_t *completions,
                                        const uint16_t *completion_indices,
                                        uint32_t ready_count,
                                        uint32_t *published);
int vemb_v16_tcp_has_buffered_requests(vemb_v16_channel_t *ch);
int vemb_v16_tcp_read_ready_requests(vemb_v16_channel_t *ch,
                                     uint32_t proxy_io_worker_id);
void vemb_v16_tcp_handle_fd(vemb_v16_proxy_t *proxy, int fd);

#endif
