#ifndef __VEMB_V16_PROXY_H
#define __VEMB_V16_PROXY_H

#include "vemb_v16_protocol.h"

typedef struct vemb_v16_proxy vemb_v16_proxy_t;

int vemb_v16_proxy_create(vemb_v16_proxy_t **out,
                          const char *uds_path,
                          uint32_t vector_dim,
                          uint32_t max_vectors,
                          const char *vector_region_name,
                          uint32_t warm_region_id,
                          uint32_t warm_backend_type,
                          uint64_t warm_mmap_offset);
void vemb_v16_proxy_destroy(vemb_v16_proxy_t *proxy);
int vemb_v16_proxy_run(vemb_v16_proxy_t *proxy);
void vemb_v16_proxy_stop(vemb_v16_proxy_t *proxy);
void vemb_v16_proxy_get_stats(vemb_v16_proxy_t *proxy, vemb_v16_stats_t *stats);

#endif
