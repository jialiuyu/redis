#define _GNU_SOURCE

#include "vemb_v16_warm_provider.h"
#include "macro.h"
#include "vemb_v16_log.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static uint64_t page_align_down(uint64_t value) {
    long page_size = sysconf(_SC_PAGESIZE);
    uint64_t page_mask = (uint64_t)(page_size > 0 ? page_size : 4096) - 1u;
    return value & ~page_mask;
}

static const char *warm_backend_name(uint32_t backend_type) {
    switch (backend_type) {
    case VEMB_V16_REGION_LOCAL_SHM:
        return "shm";
    case VEMB_V16_REGION_UB:
        return "ub";
    default:
        return "unknown";
    }
}

int vemb_v16_warm_provider_open(vemb_v16_warm_provider_t *provider,
                                uint32_t region_id,
                                uint32_t backend_type,
                                const char *path,
                                uint64_t mmap_offset,
                                uint32_t value_size,
                                uint64_t region_bytes,
                                uint32_t home_ub_node_id,
                                uint32_t is_local,
                                uint32_t weight) {
    RETURN_IF(!provider || !path || !path[0] || value_size == 0 ||
              region_bytes == 0 || region_bytes > SIZE_MAX, -1);
    size_t path_len = strlen(path);
    RETURN_IF(path_len >= sizeof(provider->path), -1);
    memset(provider, 0, sizeof(*provider));
    provider->fd = -1;
    provider->region_id = region_id;
    provider->backend_type = backend_type ? backend_type : VEMB_V16_REGION_LOCAL_SHM;
    RETURN_IF(provider->backend_type != VEMB_V16_REGION_LOCAL_SHM &&
              provider->backend_type != VEMB_V16_REGION_UB,
              -1);
    provider->mmap_offset = mmap_offset;
    provider->mmap_aligned_offset = page_align_down(mmap_offset);
    provider->mapping_size = (size_t)region_bytes;
    provider->mapping_bytes =
        provider->mapping_size + (size_t)(mmap_offset - provider->mmap_aligned_offset);
    memcpy(provider->path, path, path_len + 1);

    if (provider->backend_type == VEMB_V16_REGION_LOCAL_SHM) {
        RETURN_IF(path[0] != '/', -1);
        shm_unlink(path);
        provider->fd = shm_open(path, O_CREAT | O_RDWR, 0666);
        if (provider->fd < 0 ||
            ftruncate(provider->fd,
                      (off_t)(provider->mapping_size + (size_t)mmap_offset)) != 0) {
            serverLog(LL_WARNING, "vemb_v16 shm warm provider create failed: path=%s size=%zu error=%s",
                      path, provider->mapping_size, strerror(errno));
            vemb_v16_warm_provider_close(provider);
            return -1;
        }
        provider->unlink_on_destroy = 1;
    } else {
        provider->fd = open(path, O_RDWR);
        if (provider->fd < 0) {
            serverLog(LL_WARNING, "vemb_v16 UB warm provider open failed: path=%s error=%s",
                      path, strerror(errno));
            vemb_v16_warm_provider_close(provider);
            return -1;
        }
    }
    serverLog(LL_NOTICE,
              "vemb_v16 warm provider open ok: region_id=%u backend=%s path=%s fd=%d bytes=%zu mmap_offset=%llu aligned_offset=%llu",
              region_id,
              warm_backend_name(provider->backend_type),
              path,
              provider->fd,
              provider->mapping_size,
              (unsigned long long)mmap_offset,
              (unsigned long long)provider->mmap_aligned_offset);

    int mmap_flags = MAP_SHARED;
    /*
     * POSIX shm_open() objects live on tmpfs and are not hugetlbfs files.
     * Mapping them with MAP_HUGETLB fails with EINVAL on normal Linux
     * deployments, so shm stays on regular MAP_SHARED. UB backend still tries
     * HugeTLB first because its path may be backed by hugepage-capable memory.
     */
#if defined(__linux__) && defined(MAP_HUGETLB)
    if (provider->backend_type == VEMB_V16_REGION_UB) {
        provider->mapping_addr = mmap(NULL,
                                      provider->mapping_bytes,
                                      PROT_READ | PROT_WRITE,
                                      mmap_flags | MAP_HUGETLB,
                                      provider->fd,
                                      (off_t)provider->mmap_aligned_offset);
        if (provider->mapping_addr == MAP_FAILED) {
            serverLog(LL_WARNING, "vemb_v16 warm provider huge tlb mmap failed, fallback to regular mmap: path=%s size=%zu offset=%llu error=%s",
                      path,
                      provider->mapping_bytes,
                      (unsigned long long)mmap_offset,
                      strerror(errno));
        }
    } else {
        provider->mapping_addr = MAP_FAILED;
    }
#else
    provider->mapping_addr = MAP_FAILED;
#endif

    if (provider->mapping_addr == MAP_FAILED) {
        provider->mapping_addr = mmap(NULL,
                                      provider->mapping_bytes,
                                      PROT_READ | PROT_WRITE,
                                      mmap_flags,
                                      provider->fd,
                                      (off_t)provider->mmap_aligned_offset);
    }
    if (provider->mapping_addr == MAP_FAILED) {
        serverLog(LL_WARNING, "vemb_v16 warm provider mmap failed: path=%s size=%zu offset=%llu error=%s",
                  path,
                  provider->mapping_bytes,
                  (unsigned long long)mmap_offset,
                  strerror(errno));
        provider->mapping_addr = NULL;
        vemb_v16_warm_provider_close(provider);
        return -1;
    }
    close(provider->fd);
    provider->fd = -1;

    provider->region = (vemb_v16_tlc_warm_region_t){
        .region_id = region_id,
        .backend_type = provider->backend_type,
        .home_ub_node_id = home_ub_node_id,
        .is_local = is_local,
        .weight = weight ? weight : 1,
        .mapped_addr = (uint8_t *)provider->mapping_addr +
            (mmap_offset - provider->mmap_aligned_offset),
        .region_bytes = provider->mapping_size,
        .mmap_offset = mmap_offset,
        .value_size = value_size,
    };
    serverLog(LL_NOTICE,
              "vemb_v16 warm provider mmap ok: region_id=%u backend=%s path=%s mapping_addr=%p mapped_addr=%p mapping_bytes=%zu region_bytes=%llu value_size=%u home_ub_node_id=%u is_local=%u weight=%u",
              region_id,
              warm_backend_name(provider->backend_type),
              path,
              provider->mapping_addr,
              provider->region.mapped_addr,
              provider->mapping_bytes,
              (unsigned long long)provider->region.region_bytes,
              provider->region.value_size,
              provider->region.home_ub_node_id,
              provider->region.is_local,
              provider->region.weight);
    return 0;
}

void vemb_v16_warm_provider_close(vemb_v16_warm_provider_t *provider) {
    RETURN_IF(!provider);
    if (provider->mapping_addr) {
        munmap(provider->mapping_addr, provider->mapping_bytes);
    }
    if (provider->fd >= 0)
        close(provider->fd);
    if (provider->unlink_on_destroy && provider->path[0])
        shm_unlink(provider->path);
    memset(provider, 0, sizeof(*provider));
    provider->fd = -1;
}
