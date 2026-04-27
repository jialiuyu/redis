/*
 * Minimal OBMM ownership implementation for static linking into Redis.
 * Extracted from libobmm — only the obmm_set_ownership ioctl wrapper.
 */

#include "obmm_ownership.h"

#include <errno.h>
#include <stdint.h>
#include <sys/ioctl.h>
#include <sys/mman.h>

/*
 * Kernel UAPI definitions from <ub/obmm.h>.
 * Duplicated here so the build does not require the kernel header package.
 */
#ifndef OBMM_SHMDEV_UPDATE_RANGE
#define OBMM_IOCTL_MAGIC 'O'
#define OBMM_SHMDEV_UPDATE_RANGE _IOW(OBMM_IOCTL_MAGIC, 0x30, struct obmm_cmd_update_range)
#endif

#ifndef OBMM_SHM_MEM_NORMAL
#define OBMM_SHM_MEM_NORMAL      (1ULL << 0)
#define OBMM_SHM_MEM_NORMAL_NC   (1ULL << 1)
#define OBMM_SHM_MEM_NO_ACCESS   (1ULL << 4)
#define OBMM_SHM_MEM_READONLY    (1ULL << 5)
#define OBMM_SHM_MEM_READWRITE   (1ULL << 6)
#define OBMM_SHM_CACHE_INFER     (0ULL)
#endif

struct obmm_cmd_update_range {
    uint64_t start;
    uint64_t end;
    uint64_t mem_state;
    uint64_t cache_ops;
};

int obmm_set_ownership(int fd, void *start, void *end, int prot)
{
    uint64_t mem_attr;
    struct obmm_cmd_update_range update_info;

    if (prot == PROT_NONE) {
        mem_attr = OBMM_SHM_MEM_NORMAL_NC | OBMM_SHM_MEM_NO_ACCESS;
    } else if (prot == PROT_READ) {
        mem_attr = OBMM_SHM_MEM_NORMAL | OBMM_SHM_MEM_READONLY;
    } else if (prot == PROT_WRITE || prot == (PROT_READ | PROT_WRITE)) {
        mem_attr = OBMM_SHM_MEM_NORMAL | OBMM_SHM_MEM_READWRITE;
    } else {
        errno = EINVAL;
        return -1;
    }

    update_info.start = (uintptr_t)start;
    update_info.end = (uintptr_t)end;
    update_info.mem_state = mem_attr;
    update_info.cache_ops = OBMM_SHM_CACHE_INFER;

    return ioctl(fd, OBMM_SHMDEV_UPDATE_RANGE, &update_info);
}
