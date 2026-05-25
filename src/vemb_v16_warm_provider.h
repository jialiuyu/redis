#ifndef __VEMB_V16_WARM_PROVIDER_H
#define __VEMB_V16_WARM_PROVIDER_H

#include "vemb_v16_tlc.h"

#include <stddef.h>
#include <stdint.h>

typedef struct vemb_v16_warm_provider {
    uint32_t region_id;
    uint32_t backend_type;
    char path[256];
    uint64_t mmap_offset;
    uint64_t mmap_aligned_offset;
    size_t mapping_size;
    size_t mapping_bytes;
    void *mapping_addr;
    int fd;
    int unlink_on_destroy;
    vemb_v16_tlc_warm_region_t region;
} vemb_v16_warm_provider_t;

int vemb_v16_warm_provider_open(vemb_v16_warm_provider_t *provider,
                                uint32_t region_id,
                                uint32_t backend_type,
                                const char *path,
                                uint64_t mmap_offset,
                                uint32_t value_size,
                                uint32_t max_vectors);
void vemb_v16_warm_provider_close(vemb_v16_warm_provider_t *provider);

#endif
