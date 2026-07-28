/*
 * Micro benchmark for sve_streaming_load_f32() only.
 *
 * It measures one copy primitive across:
 *   - USE_SVE enabled/disabled at build time
 *   - local / remote UB path selected from a manifest
 *
 * The benchmark is intentionally narrow: one source region, one destination
 * buffer, one load primitive, no server, no memtier.
 */

#include "../src/sve_operation.h"
#include "../src/vemb_v16_protocol.h"

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

typedef struct warm_region_desc {
    uint32_t region_id;
    uint32_t backend_type;
    uint32_t home_ub_node_id;
    uint32_t is_local;
    uint32_t weight;
    uint32_t value_size;
    uint64_t mmap_offset;
    uint64_t region_bytes;
    char path[256];
} warm_region_desc_t;

typedef struct manifest_desc {
    uint32_t local_ub_node_id;
    uint32_t has_local_ub_node_id;
    warm_region_desc_t regions[16];
    size_t region_count;
} manifest_desc_t;

typedef struct mapped_region {
    int fd;
    uint8_t *mapping_addr;
    uint8_t *mapped_addr;
    size_t mapping_bytes;
    uint64_t mmap_offset;
    uint64_t mmap_aligned_offset;
} mapped_region_t;

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static uint64_t page_align_down(uint64_t value) {
    long page_size = sysconf(_SC_PAGESIZE);
    uint64_t page_mask = (uint64_t)(page_size > 0 ? page_size : 4096) - 1u;
    return value & ~page_mask;
}

static void trim(char *s) {
    char *p = s;
    while (*p == ' ' || *p == '\t')
        p++;
    if (p != s)
        memmove(s, p, strlen(p) + 1u);
    size_t n = strlen(s);
    while (n > 0 && (s[n - 1] == '\n' || s[n - 1] == '\r' || s[n - 1] == ' ' || s[n - 1] == '\t'))
        s[--n] = '\0';
}

static int parse_u32(const char *text, uint32_t *out) {
    char *end = NULL;
    errno = 0;
    unsigned long v = strtoul(text, &end, 0);
    if (errno || end == text)
        return -1;
    *out = (uint32_t)v;
    return 0;
}

static int parse_u64(const char *text, uint64_t *out) {
    char *end = NULL;
    errno = 0;
    unsigned long long v = strtoull(text, &end, 0);
    if (errno || end == text)
        return -1;
    *out = (uint64_t)v;
    return 0;
}

static int parse_backend(const char *text, uint32_t *out) {
    if (!strcmp(text, "ub")) {
        *out = VEMB_V16_REGION_UB;
        return 0;
    }
    if (!strcmp(text, "shm")) {
        *out = VEMB_V16_REGION_LOCAL_SHM;
        return 0;
    }
    return -1;
}

static int parse_manifest(const char *path, manifest_desc_t *manifest) {
    FILE *fp = fopen(path, "r");
    if (!fp)
        return -1;

    memset(manifest, 0, sizeof(*manifest));
    char line[512];
    warm_region_desc_t *cur = NULL;

    while (fgets(line, sizeof(line), fp)) {
        trim(line);
        if (!line[0] || line[0] == '#')
            continue;

        if (!strncmp(line, "local_ub_node_id:", 17)) {
            if (parse_u32(line + 17, &manifest->local_ub_node_id) != 0) {
                fclose(fp);
                return -1;
            }
            manifest->has_local_ub_node_id = 1;
            continue;
        }
        if (!strcmp(line, "warm_regions:"))
            continue;
        if (!strncmp(line, "- ", 2)) {
            if (manifest->region_count >= sizeof(manifest->regions) / sizeof(manifest->regions[0])) {
                fclose(fp);
                return -1;
            }
            cur = &manifest->regions[manifest->region_count++];
            memset(cur, 0, sizeof(*cur));
            continue;
        }
        if (!cur)
            continue;

        char *value = strchr(line, ':');
        if (!value)
            continue;
        *value++ = '\0';
        trim(line);
        trim(value);

        if (!strcmp(line, "region_id")) {
            if (parse_u32(value, &cur->region_id) != 0) goto fail;
        } else if (!strcmp(line, "provider") || !strcmp(line, "backend")) {
            if (parse_backend(value, &cur->backend_type) != 0) goto fail;
        } else if (!strcmp(line, "path")) {
            if (strlen(value) >= sizeof(cur->path)) goto fail;
            memcpy(cur->path, value, strlen(value) + 1u);
        } else if (!strcmp(line, "mmap_offset")) {
            if (parse_u64(value, &cur->mmap_offset) != 0) goto fail;
        } else if (!strcmp(line, "bytes") || !strcmp(line, "region_bytes")) {
            if (parse_u64(value, &cur->region_bytes) != 0) goto fail;
        } else if (!strcmp(line, "value_size")) {
            if (parse_u32(value, &cur->value_size) != 0) goto fail;
        } else if (!strcmp(line, "home_ub_node_id")) {
            if (parse_u32(value, &cur->home_ub_node_id) != 0) goto fail;
        } else if (!strcmp(line, "weight")) {
            if (parse_u32(value, &cur->weight) != 0) goto fail;
        } else if (!strcmp(line, "is_local")) {
            cur->is_local = (!strcmp(value, "yes") || !strcmp(value, "true") || !strcmp(value, "1"));
        }
    }

    fclose(fp);
    for (size_t i = 0; i < manifest->region_count; i++) {
        warm_region_desc_t *r = &manifest->regions[i];
        if (!r->is_local && manifest->has_local_ub_node_id)
            r->is_local = (r->home_ub_node_id == manifest->local_ub_node_id);
    }
    return manifest->region_count > 0 ? 0 : -1;

fail:
    fclose(fp);
    return -1;
}

static int open_mapped_region(mapped_region_t *region,
                              uint32_t backend_type,
                              const char *path,
                              uint64_t mmap_offset,
                              size_t requested_size) {
    memset(region, 0, sizeof(*region));
    region->fd = -1;
    region->mmap_offset = mmap_offset;
    region->mmap_aligned_offset = page_align_down(mmap_offset);

    if (backend_type == VEMB_V16_REGION_LOCAL_SHM) {
        region->fd = shm_open(path, O_RDWR, 0666);
    } else {
        region->fd = open(path, O_RDWR);
        if (region->fd < 0 && (errno == EACCES || errno == EPERM))
            region->fd = open(path, O_RDWR | O_SYNC);
    }
    if (region->fd < 0)
        return -1;

    size_t offset_delta = (size_t)(mmap_offset - region->mmap_aligned_offset);
    region->mapping_bytes = requested_size + offset_delta;
    region->mapping_addr = mmap(NULL,
                                region->mapping_bytes,
                                PROT_READ | PROT_WRITE,
                                MAP_SHARED,
                                region->fd,
                                (off_t)region->mmap_aligned_offset);
    if (region->mapping_addr == MAP_FAILED) {
        close(region->fd);
        region->fd = -1;
        region->mapping_addr = NULL;
        return -1;
    }
    region->mapped_addr = region->mapping_addr + offset_delta;
    close(region->fd);
    region->fd = -1;
    return 0;
}

static void close_mapped_region(mapped_region_t *region) {
    if (!region)
        return;
    if (region->mapping_addr)
        munmap(region->mapping_addr, region->mapping_bytes);
    if (region->fd >= 0)
        close(region->fd);
    memset(region, 0, sizeof(*region));
    region->fd = -1;
}

static const warm_region_desc_t *pick_region(const manifest_desc_t *manifest,
                                             int want_local) {
    for (size_t i = 0; i < manifest->region_count; i++) {
        const warm_region_desc_t *r = &manifest->regions[i];
        if ((want_local && r->is_local) || (!want_local && !r->is_local))
            return r;
    }
    return NULL;
}

static float checksum_stride(const float *buf, size_t floats) {
    float sum = 0.0f;
    for (size_t i = 0; i < floats; i += 17u)
        sum += buf[i];
    return sum;
}

static void fill_dst(float *dst, size_t floats) {
    for (size_t i = 0; i < floats; i++)
        dst[i] = -12345.0f;
}

static void fill_src(float *src, size_t floats) {
    for (size_t i = 0; i < floats; i++)
        src[i] = (float)((i + 1u) * 13u);
}

static int run_one(const char *manifest_path, int want_local, int warmup, int iters) {
    manifest_desc_t manifest;
    if (parse_manifest(manifest_path, &manifest) != 0) {
        fprintf(stderr, "parse manifest failed: %s\n", manifest_path);
        return -1;
    }

    const warm_region_desc_t *region = pick_region(&manifest, want_local);
    if (!region) {
        fprintf(stderr, "no %s region found in %s\n",
                want_local ? "local" : "remote", manifest_path);
        return -1;
    }

    if (region->value_size == 0 || (region->value_size % sizeof(float)) != 0) {
        fprintf(stderr, "invalid value_size=%u in %s\n", region->value_size, manifest_path);
        return -1;
    }

    mapped_region_t mapped;
    if (open_mapped_region(&mapped,
                           region->backend_type,
                           region->path,
                           region->mmap_offset,
                           (size_t)region->region_bytes) != 0) {
        fprintf(stderr, "open mapped region failed: path=%s\n", region->path);
        return -1;
    }

    const size_t bytes = region->value_size;
    const size_t floats = bytes / sizeof(float);
    float *dst = aligned_alloc(64, bytes);
    if (!dst) {
        close_mapped_region(&mapped);
        return -1;
    }
    fill_dst(dst, floats);
    fill_src((float *)mapped.mapped_addr, floats);

    for (int i = 0; i < warmup; i++)
        sve_streaming_load_f32(mapped.mapped_addr, dst, bytes);

    uint64_t start = now_ns();
    for (int i = 0; i < iters; i++)
        sve_streaming_load_f32(mapped.mapped_addr, dst, bytes);
    uint64_t elapsed = now_ns() - start;

    float checksum = checksum_stride(dst, floats);
    double avg_ns = (double)elapsed / (double)iters;
    double mb_per_s = ((double)bytes / 1000000.0) / (avg_ns / 1000000000.0);
    const char *mode =
#ifdef USE_ARM_SVE
        "sve";
#else
        "nosve";
#endif

    printf("mode=%s region=%s path=%s bytes=%zu warmup=%d iters=%d avg_ns=%.2f MB/s=%.2f checksum=%.1f\n",
           mode,
           want_local ? "local" : "remote",
           region->path,
           bytes,
           warmup,
           iters,
           avg_ns,
           mb_per_s,
           checksum);

    free(dst);
    close_mapped_region(&mapped);
    return 0;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s --manifest PATH [--local|--remote] [--warmup N] [--iters N]\n"
            "Example: %s --manifest ../examples/vemb_v16_warm_regions_111.yaml --local\n",
            prog, prog);
}

int main(int argc, char **argv) {
    const char *manifest_path = NULL;
    int want_local = -1;
    int warmup = 20000;
    int iters = 500000;

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--manifest") && i + 1 < argc) {
            manifest_path = argv[++i];
        } else if (!strcmp(argv[i], "--local")) {
            want_local = 1;
        } else if (!strcmp(argv[i], "--remote")) {
            want_local = 0;
        } else if (!strcmp(argv[i], "--warmup") && i + 1 < argc) {
            warmup = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--iters") && i + 1 < argc) {
            iters = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--help")) {
            usage(argv[0]);
            return 0;
        } else {
            usage(argv[0]);
            return 1;
        }
    }

    if (!manifest_path || want_local < 0 || warmup < 0 || iters <= 0) {
        usage(argv[0]);
        return 1;
    }

#ifdef USE_ARM_SVE
    fprintf(stderr, "sve_streaming_load_f32 benchmark: USE_SVE=on f32_lanes=%zu\n", (size_t)svcntw());
#else
    fprintf(stderr, "sve_streaming_load_f32 benchmark: USE_SVE=off scalar_fallback\n");
#endif

    if (run_one(manifest_path, want_local, warmup, iters) != 0)
        return 1;
    return 0;
}
