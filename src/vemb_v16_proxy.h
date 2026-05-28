#ifndef __VEMB_V16_PROXY_H
#define __VEMB_V16_PROXY_H

#include "vemb_v16_protocol.h"
#include "vemb_v16_storage.h"

typedef struct vemb_v16_proxy vemb_v16_proxy_t;

int vemb_v16_proxy_create(vemb_v16_proxy_t **out,
                          uint32_t vector_dim,
                          uint32_t max_vectors,
                          vemb_v16_storage_ctx_t *storage);
int vemb_v16_proxy_set_proxy_io_threads(vemb_v16_proxy_t *proxy,
                                        uint32_t threads);
int vemb_v16_proxy_set_supernode_workers(vemb_v16_proxy_t *proxy,
                                         uint32_t workers);
void vemb_v16_proxy_destroy(vemb_v16_proxy_t *proxy);
int vemb_v16_proxy_run(vemb_v16_proxy_t *proxy);
void vemb_v16_proxy_stop(vemb_v16_proxy_t *proxy);
void vemb_v16_proxy_get_stats(vemb_v16_proxy_t *proxy, vemb_v16_stats_t *stats);
int vemb_v16_proxy_enable_tcp(vemb_v16_proxy_t *proxy,
                              const char *host,
                              uint16_t port);
int vemb_v16_proxy_enable_inject(vemb_v16_proxy_t *proxy);
int vemb_v16_proxy_inject_fd(vemb_v16_proxy_t *proxy, int fd);

#endif
