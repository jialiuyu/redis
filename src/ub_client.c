/*
 * UB client data-plane implementation.
 * Export/import are managed out-of-process by obmmctl; Redis only consumes
 * an existing shmdev through configuration.
 */

#include "ub_client.h"
#include "macro.h"
#include "server.h"
#include "sve_config.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>


#ifdef USE_CC_MODE
#include "obmm_ownership.h"
#endif

ub_client_t *global_ub_client = NULL;

static size_t configured_vector_stride(const ub_mem_config_t *cfg, size_t vector_dim)
{
    if (cfg->vector_stride_bytes != 0) {
        return cfg->vector_stride_bytes;
    }
    if (vector_dim == 0 || vector_dim > SIZE_MAX / sizeof(float)) {
        return 0;
    }
    return vector_dim * sizeof(float);
}

static int configured_table_size(const ub_mem_config_t *cfg, size_t *table_size)
{
    size_t size = cfg->table_size;

    if (cfg->shm_size == 0) {
        errno = EINVAL;
        return C_ERR;
    }
    if (cfg->table_offset >= cfg->shm_size) {
        errno = EINVAL;
        return C_ERR;
    }
    if (size == 0) {
        size = cfg->shm_size - cfg->table_offset;
    }
    if (size == 0 || cfg->table_offset + size > cfg->shm_size) {
        errno = EINVAL;
        return C_ERR;
    }

    *table_size = size;
    return C_OK;
}

static int configured_device_path(const ub_mem_config_t *cfg, char path[UB_DEVICE_PATH_MAX])
{
    int written;

    if (cfg->shm_path && cfg->shm_path[0] != '\0') {
        if (strlen(cfg->shm_path) >= UB_DEVICE_PATH_MAX) {
            errno = ENAMETOOLONG;
            return C_ERR;
        }
        memcpy(path, cfg->shm_path, strlen(cfg->shm_path) + 1);
        return C_OK;
    }

    if (cfg->shm_memid == 0) {
        errno = ENOENT;
        return C_ERR;
    }

    written = snprintf(path, UB_DEVICE_PATH_MAX, "/dev/obmm_shmdev%llu",
                       cfg->shm_memid);
    if (written < 0 || written >= UB_DEVICE_PATH_MAX) {
        errno = ENAMETOOLONG;
        return C_ERR;
    }
    return C_OK;
}

static int resource_name_matches(const ub_mem_config_t *cfg, const char *resource_name)
{
    if (cfg->table_name == NULL || cfg->table_name[0] == '\0') {
        return 1;
    }
    if (resource_name == NULL) {
        return 0;
    }
    return strcmp(cfg->table_name, resource_name) == 0;
}

static int set_read_ownership(ub_address_space_t *addr_space)
{
    void *start = addr_space->mapping_addr;
    void *end = (char *)addr_space->mapping_addr + addr_space->mapping_size;

    if (!addr_space->cacheable || !addr_space->use_ownership) {
        return C_OK;
    }
#ifdef USE_CC_MODE
    if (obmm_set_ownership(addr_space->shm_fd, start, end, PROT_READ) != 0) {
        serverLog(LL_WARNING,
                  "Failed to acquire OBMM read ownership for %s: %s",
                  addr_space->device_path, strerror(errno));
        return C_ERR;
    }

    addr_space->use_ownership = 1;
    return C_OK;
#else
    (void)start;
    (void)end;
    serverLog(LL_WARNING, "OBMM ownership requires USE_CC_MODE=yes at build time");
    return C_ERR;
#endif
}

static int maybe_set_write_ownership(ub_address_space_t *addr_space)
{
    void *start = addr_space->mapping_addr;
    void *end = (char *)addr_space->mapping_addr + addr_space->mapping_size;

    if (!addr_space->cacheable || !addr_space->use_ownership) {
        return C_OK;
    }
#ifdef USE_CC_MODE
    if (obmm_set_ownership(addr_space->shm_fd, start, end, PROT_WRITE) != 0) {
        serverLog(LL_WARNING,
                  "Failed to acquire OBMM write ownership for %s: %s",
                  addr_space->device_path, strerror(errno));
        return C_ERR;
    }
#else
    UNUSED(start);
    UNUSED(end);
#endif

    return C_OK;
}

static void maybe_release_ownership(ub_address_space_t *addr_space)
{
    if (!addr_space || !addr_space->use_ownership) {
        return;
    }
#ifdef USE_CC_MODE
    {
        void *start = addr_space->mapping_addr;
        void *end = (char *)addr_space->mapping_addr + addr_space->mapping_size;

        if (obmm_set_ownership(addr_space->shm_fd, start, end, PROT_NONE) != 0) {
            serverLog(LL_WARNING,
                      "Failed to release OBMM ownership for %s: %s",
                      addr_space->device_path, strerror(errno));
        }
    }
#endif
}

static void destroy_addr_space(ub_address_space_t *addr_space)
{
    if (!addr_space) {
        return;
    }

    maybe_release_ownership(addr_space);

    if (addr_space->mapping_addr && addr_space->mapping_addr != MAP_FAILED &&
        addr_space->mapping_size > 0) {
        munmap(addr_space->mapping_addr, addr_space->mapping_size);
    }
    if (addr_space->shm_fd >= 0) {
        close(addr_space->shm_fd);
    }

    zfree(addr_space);
}

static int create_addr_space(const ub_mem_config_t *cfg, ub_address_space_t **addr_space)
{
    ub_address_space_t *new_addr_space = NULL;
    char device_path[UB_DEVICE_PATH_MAX];
    size_t table_size;
    size_t vector_stride;
    int open_flags;
    int mmap_prot;
    int fd = -1;
    void *mapping = MAP_FAILED;

    if (configured_table_size(cfg, &table_size) != C_OK) {
        serverLog(LL_WARNING, "Invalid UB table size/offset configuration");
        return C_ERR;
    }

    if (configured_device_path(cfg, device_path) != C_OK) {
        serverLog(LL_WARNING, "Missing UB shmdev configuration");
        return C_ERR;
    }

    vector_stride = configured_vector_stride(cfg, cfg->vector_dimension);
    if (vector_stride == 0 || vector_stride < cfg->vector_dimension * sizeof(float)) {
        serverLog(LL_WARNING, "Invalid UB vector stride configuration");
        return C_ERR;
    }
    if (table_size < vector_stride) {
        serverLog(LL_WARNING, "UB table size is smaller than one vector row");
        return C_ERR;
    }

    open_flags = O_RDWR;
    if (!cfg->cacheable) {
        open_flags |= O_SYNC;
    }

    fd = open(device_path, open_flags);
    if (fd < 0) {
        serverLog(LL_WARNING, "Failed to open %s: %s", device_path, strerror(errno));
        return C_ERR;
    }

    mmap_prot = (cfg->cacheable && cfg->use_ownership) ? PROT_NONE : (PROT_READ | PROT_WRITE);
    mapping = mmap(NULL, cfg->shm_size, mmap_prot, MAP_SHARED, fd, 0);
    if (mapping == MAP_FAILED) {
        serverLog(LL_WARNING, "Failed to mmap %s: %s", device_path, strerror(errno));
        close(fd);
        return C_ERR;
    }

    new_addr_space = zcalloc(sizeof(*new_addr_space));
    if (!new_addr_space) {
        munmap(mapping, cfg->shm_size);
        close(fd);
        return C_ERR;
    }

    new_addr_space->base_addr = cfg->table_offset;
    new_addr_space->size = table_size;
    new_addr_space->mapped_addr = (char *)mapping + cfg->table_offset;
    new_addr_space->mapping_addr = mapping;
    new_addr_space->mapping_size = cfg->shm_size;
    new_addr_space->data_offset = cfg->table_offset;
    new_addr_space->vector_stride_bytes = vector_stride;
    new_addr_space->mem_id = cfg->shm_memid;
    new_addr_space->shm_fd = fd;
    new_addr_space->cacheable = cfg->cacheable;
    new_addr_space->use_ownership = cfg->use_ownership ? 1 : 0;
    memcpy(new_addr_space->device_path, device_path, strlen(device_path) + 1);

    if (set_read_ownership(new_addr_space) != C_OK) {
        destroy_addr_space(new_addr_space);
        return C_ERR;
    }

    *addr_space = new_addr_space;
    return C_OK;
}

static uint64_t fnv1a64(const char *text)
{
    const unsigned char *p = (const unsigned char *)text;
    uint64_t hash = UINT64_C(1469598103934665603);

    while (*p) {
        hash ^= (uint64_t)*p++;
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}

static int parse_u64_strict(const char *text, uint64_t *value)
{
    char *end = NULL;
    unsigned long long parsed;

    if (text == NULL || *text == '\0') {
        return C_ERR;
    }

    errno = 0;
    parsed = strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        return C_ERR;
    }

    *value = parsed;
    return C_OK;
}

static int parse_numeric_suffix(const char *text, uint64_t *value)
{
    size_t len;
    size_t start;

    if (text == NULL || *text == '\0') {
        return C_ERR;
    }

    len = strlen(text);
    start = len;
    while (start > 0 && isdigit((unsigned char)text[start - 1])) {
        start--;
    }
    if (start == len) {
        return C_ERR;
    }

    return parse_u64_strict(text + start, value);
}

int ub_client_init(const ub_mem_config_t *cfg)
{
    if (global_ub_client) {
        return C_OK;
    }
    if (cfg == NULL) {
        errno = EINVAL;
        return C_ERR;
    }
    global_ub_client = zcalloc(sizeof(*global_ub_client));
    if (!global_ub_client) {
        return C_ERR;
    }

    global_ub_client->config = cfg;
    global_ub_client->initialized = 1;
    serverLog(LL_NOTICE, "UB client initialized in data-plane mode");
    return C_OK;
}

void ub_client_cleanup(void)
{
    if (!global_ub_client) {
        return;
    }

    destroy_addr_space(global_ub_client->global_ubas);
    global_ub_client->global_ubas = NULL;

    zfree(global_ub_client);
    global_ub_client = NULL;

    serverLog(LL_NOTICE, "UB client cleaned up");
}

int ub_client_load_embedding_table(const char *resource_name,
                                   ub_address_space_t **addr_space)
{
    const ub_mem_config_t *cfg;

    if (!addr_space || !global_ub_client || !global_ub_client->initialized) {
        return C_ERR;
    }
    cfg = global_ub_client->config;
    if (!resource_name_matches(cfg, resource_name)) {
        serverLog(LL_WARNING, "UB table resource mismatch for key %s",
                  resource_name ? resource_name : "(null)");
        return C_ERR;
    }
    if (global_ub_client->global_ubas) {
        global_ub_client->cache_hits++;
        *addr_space = global_ub_client->global_ubas;
        return C_OK;
    }

    global_ub_client->cache_misses++;
    if (create_addr_space(cfg, &global_ub_client->global_ubas) != C_OK) {
        return C_ERR;
    }

    *addr_space = global_ub_client->global_ubas;
    return C_OK;
}

int ub_client_resolve_element_index(const char *element_name,
                                    uint64_t *index,
                                    size_t vector_dim)
{
    RETURN_IF(!element_name || !index, C_ERR);
    const ub_mem_config_t *cfg = global_ub_client ? global_ub_client->config : NULL;
    ub_address_space_t *addr_space = global_ub_client ? global_ub_client->global_ubas : NULL;
    RETURN_IF(!addr_space || !cfg, C_ERR);
    size_t vector_stride = configured_vector_stride(cfg, vector_dim);
    RETURN_IF(vector_stride == 0, C_ERR);
    uint64_t capacity = addr_space->size / vector_stride;
    RETURN_IF(capacity == 0, C_ERR);

    uint64_t resolved = 0;
    switch (cfg->element_index_mode) {
    case UB_ELEMENT_INDEX_NUMERIC:
        if (parse_u64_strict(element_name, &resolved) != C_OK) {
            return C_ERR;
        }
        break;
    case UB_ELEMENT_INDEX_SUFFIX_NUMERIC:
        if (parse_numeric_suffix(element_name, &resolved) != C_OK) {
            return C_ERR;
        }
        break;
    case UB_ELEMENT_INDEX_HASH:
        resolved = fnv1a64(element_name) % capacity;
        break;
    default:
        return C_ERR;
    }

    if (resolved >= capacity) {
        serverLog(LL_WARNING, "Resolved UB index %" PRIu64 " out of range (capacity=%" PRIu64 ")",
                  resolved, capacity);
        return C_ERR;
    }

    *index = resolved;
    return C_OK;
}

int ub_client_perform_gather_load(ub_address_space_t *addr_space,
                                  uint64_t *indices,
                                  size_t num_indices,
                                  float *results,
                                  size_t vector_dim)
{
    const char *base;
    size_t vector_bytes;
    size_t capacity;

    if (!global_ub_client || !addr_space || !indices || !results || vector_dim == 0) {
        return C_ERR;
    }

    vector_bytes = vector_dim * sizeof(float);
    if (addr_space->vector_stride_bytes < vector_bytes) {
        return C_ERR;
    }

    capacity = addr_space->size / addr_space->vector_stride_bytes;
    base = (const char *)addr_space->mapped_addr;

#ifdef USE_ARM_SVE
    /*
     * SVE gather-load path.
     * Each row is vector_dim floats; we copy one row per index using SVE
     * contiguous loads (LD1W) with a predicate covering the row width.
     * The stride between rows may be larger than vector_dim (padding), so
     * we use a plain contiguous load per row rather than a true scatter-gather
     * across rows — this matches the memory layout and avoids strided-gather
     * complexity while still benefiting from SVE's wide vector registers.
     */
    {
        const size_t vl_f32 = svcntw();   /* SVE vector length in float lanes */

        for (size_t i = 0; i < num_indices; i++) {
            uint64_t idx = indices[i];
            if (idx >= capacity) {
                serverLog(LL_WARNING, "UB index %" PRIu64 " out of bounds", idx);
                return C_ERR;
            }

            const float *src = (const float *)(base + idx * addr_space->vector_stride_bytes);
            float       *dst = results + i * vector_dim;
            size_t       rem = vector_dim;

            /* Copy full SVE-width chunks */
            while (rem >= vl_f32) {
                svbool_t pg = svptrue_b32();
                svfloat32_t v = svld1_f32(pg, src);
                svst1_f32(pg, dst, v);
                src += vl_f32;
                dst += vl_f32;
                rem -= vl_f32;
            }

            /* Tail: predicated store for remaining elements */
            if (rem > 0) {
                svbool_t pg = svwhilelt_b32_u64(0UL, (uint64_t)rem);
                svfloat32_t v = svld1_f32(pg, src);
                svst1_f32(pg, dst, v);
            }
        }
    }
#else
    /* Scalar fallback */
    for (size_t i = 0; i < num_indices; i++) {
        uint64_t idx = indices[i];
        const char *src;

        if (idx >= capacity) {
            serverLog(LL_WARNING, "UB index %" PRIu64 " out of bounds", idx);
            return C_ERR;
        }

        src = base + (idx * addr_space->vector_stride_bytes);
        memcpy(&results[i * vector_dim], src, vector_bytes);
    }
#endif

    global_ub_client->total_requests += num_indices;
    return C_OK;
}

int ub_client_perform_contiguous_load(ub_address_space_t *addr_space,
                                      size_t start_index,
                                      size_t num_rows,
                                      float *results,
                                      size_t vector_dim)
{
    size_t vector_bytes;
    size_t capacity;

    if (!global_ub_client || !addr_space || !results || vector_dim == 0 || num_rows == 0) {
        return C_ERR;
    }

    vector_bytes = vector_dim * sizeof(float);
    if (addr_space->vector_stride_bytes < vector_bytes) {
        return C_ERR;
    }

    capacity = addr_space->size / addr_space->vector_stride_bytes;
    if (start_index + num_rows > capacity) {
        serverLog(LL_WARNING,
                  "UB contiguous load: range [%zu, %zu) exceeds capacity %zu",
                  start_index, start_index + num_rows, capacity);
        return C_ERR;
    }

    const char *base = (const char *)addr_space->mapped_addr;

    if (addr_space->vector_stride_bytes == vector_bytes) {
        /* No padding — single memcpy for the entire block */
        const char *src = base + start_index * vector_bytes;
        memcpy(results, src, num_rows * vector_bytes);
    } else {
        /* Stride > vector_bytes (padding between rows) — copy row by row */
#ifdef USE_ARM_SVE
        const size_t vl_f32 = svcntw();

        for (size_t i = 0; i < num_rows; i++) {
            const float *src = (const float *)(base + (start_index + i) * addr_space->vector_stride_bytes);
            float       *dst = results + i * vector_dim;
            size_t       rem = vector_dim;

            while (rem >= vl_f32) {
                svbool_t pg = svptrue_b32();
                svst1_f32(pg, dst, svld1_f32(pg, src));
                src += vl_f32;
                dst += vl_f32;
                rem -= vl_f32;
            }
            if (rem > 0) {
                svbool_t pg = svwhilelt_b32_u64(0UL, (uint64_t)rem);
                svst1_f32(pg, dst, svld1_f32(pg, src));
            }
        }
#else
        for (size_t i = 0; i < num_rows; i++) {
            const char *src = base + (start_index + i) * addr_space->vector_stride_bytes;
            memcpy(&results[i * vector_dim], src, vector_bytes);
        }
#endif
    }

    global_ub_client->total_requests += num_rows;
    return C_OK;
}

int ub_client_perform_scatter_store(ub_address_space_t *addr_space,
                                    uint64_t *indices,
                                    size_t num_indices,
                                    float *data,
                                    size_t vector_dim)
{
    char *base;
    size_t vector_bytes;
    size_t capacity;

    if (!global_ub_client || !addr_space || !indices || !data || vector_dim == 0) {
        return C_ERR;
    }

    vector_bytes = vector_dim * sizeof(float);
    if (addr_space->vector_stride_bytes < vector_bytes) {
        return C_ERR;
    }

    capacity = addr_space->size / addr_space->vector_stride_bytes;

    /* Validate all indices before any write */
    for (size_t i = 0; i < num_indices; i++) {
        if (indices[i] >= capacity) {
            serverLog(LL_WARNING, "UB scatter index %" PRIu64 " out of bounds", indices[i]);
            return C_ERR;
        }
    }

    /* Acquire write ownership if in ownership mode */
    if (maybe_set_write_ownership(addr_space) != C_OK) {
        serverLog(LL_WARNING, "UB scatter-store: failed to acquire write ownership");
        return C_ERR;
    }

    base = (char *)addr_space->mapped_addr;

#ifdef USE_ARM_SVE
    /*
     * SVE scatter-store path.
     * Each row is vector_dim floats; we copy one row per index using SVE
     * contiguous stores (ST1W) with a predicate covering the row width.
     */
    {
        const size_t vl_f32 = svcntw();   /* SVE vector length in float lanes */

        for (size_t i = 0; i < num_indices; i++) {
            uint64_t idx = indices[i];
            const float *src = data + i * vector_dim;
            float       *dst = (float *)(base + idx * addr_space->vector_stride_bytes);
            size_t       rem = vector_dim;

            /* Copy full SVE-width chunks */
            while (rem >= vl_f32) {
                svbool_t pg = svptrue_b32();
                svfloat32_t v = svld1_f32(pg, src);
                svst1_f32(pg, dst, v);
                src += vl_f32;
                dst += vl_f32;
                rem -= vl_f32;
            }

            /* Tail: predicated store for remaining elements */
            if (rem > 0) {
                svbool_t pg = svwhilelt_b32_u64(0UL, (uint64_t)rem);
                svfloat32_t v = svld1_f32(pg, src);
                svst1_f32(pg, dst, v);
            }
        }
    }
#else
    /* Scalar fallback */
    for (size_t i = 0; i < num_indices; i++) {
        uint64_t idx = indices[i];
        char *dst = base + (idx * addr_space->vector_stride_bytes);
        memcpy(dst, &data[i * vector_dim], vector_bytes);
    }
#endif

    /* Release ownership if in ownership mode */
    maybe_release_ownership(addr_space);

    global_ub_client->total_requests += num_indices;
    return C_OK;
}

int ub_client_set_config(const char *key, const char *value)
{
    UNUSED(key);
    UNUSED(value);
    serverLog(LL_WARNING, "Use CONFIG SET for UB client parameters");
    return C_ERR;
}

sds ub_client_get_config(const char *key)
{
    if (key == NULL) {
        return NULL;
    }
    if (!global_ub_client) {
        return NULL;
    }

    if (!strcasecmp(key, "ub-table-name")) {
        return global_ub_client->config->table_name ? sdsnew(global_ub_client->config->table_name) : NULL;
    }
    if (!strcasecmp(key, "ub-shm-path")) {
        return global_ub_client->config->shm_path ? sdsnew(global_ub_client->config->shm_path) : NULL;
    }
    if (!strcasecmp(key, "ub-shm-memid")) {
        return sdscatprintf(sdsempty(), "%llu", global_ub_client->config->shm_memid);
    }
    if (!strcasecmp(key, "ub-shm-size")) {
        return sdscatprintf(sdsempty(), "%zu", global_ub_client->config->shm_size);
    }
    if (!strcasecmp(key, "ub-table-offset")) {
        return sdscatprintf(sdsempty(), "%zu", global_ub_client->config->table_offset);
    }
    if (!strcasecmp(key, "ub-table-size")) {
        return sdscatprintf(sdsempty(), "%zu", global_ub_client->config->table_size);
    }
    if (!strcasecmp(key, "ub-vector-stride-bytes")) {
        return sdscatprintf(sdsempty(), "%zu", global_ub_client->config->vector_stride_bytes);
    }
    if (!strcasecmp(key, "vector-dimension")) {
        return sdscatprintf(sdsempty(), "%d", global_ub_client->config->vector_dimension);
    }
    if (!strcasecmp(key, "ub-cacheable")) {
        return sdsnew(global_ub_client->config->cacheable ? "yes" : "no");
    }
    if (!strcasecmp(key, "ub-use-ownership")) {
        return sdsnew(global_ub_client->config->use_ownership ? "yes" : "no");
    }
    if (!strcasecmp(key, "ub-element-index-mode")) {
        switch (global_ub_client->config->element_index_mode) {
        case UB_ELEMENT_INDEX_NUMERIC:
            return sdsnew("numeric");
        case UB_ELEMENT_INDEX_SUFFIX_NUMERIC:
            return sdsnew("suffix-numeric");
        case UB_ELEMENT_INDEX_HASH:
            return sdsnew("hash");
        default:
            return sdsnew("unknown");
        }
    }

    return NULL;
}

sds ub_client_get_stats(void)
{
    sds stats = sdsempty();

    if (!global_ub_client) {
        return sdscat(stats, "UB Client: Not initialized");
    }

    stats = sdscatprintf(stats, "UB Client Stats:\n");
    stats = sdscatprintf(stats, "  Initialized: %s\n",
                         global_ub_client->initialized ? "Yes" : "No");
    stats = sdscatprintf(stats, "  Cache Hits: %llu\n",
                         (unsigned long long)global_ub_client->cache_hits);
    stats = sdscatprintf(stats, "  Cache Misses: %llu\n",
                         (unsigned long long)global_ub_client->cache_misses);
    stats = sdscatprintf(stats, "  Total Requests: %llu\n",
                         (unsigned long long)global_ub_client->total_requests);

    if (global_ub_client->global_ubas) {
        ub_address_space_t *addr_space = global_ub_client->global_ubas;

        stats = sdscatprintf(stats, "  Device: %s\n", addr_space->device_path);
        stats = sdscatprintf(stats, "  MemId: %llu\n", addr_space->mem_id);
        stats = sdscatprintf(stats, "  Mapping Size: %zu\n", addr_space->mapping_size);
        stats = sdscatprintf(stats, "  Table Offset: %zu\n", addr_space->data_offset);
        stats = sdscatprintf(stats, "  Table Size: %zu\n", addr_space->size);
        stats = sdscatprintf(stats, "  Vector Stride: %zu\n", addr_space->vector_stride_bytes);
        stats = sdscatprintf(stats, "  Cacheable: %s\n", addr_space->cacheable ? "Yes" : "No");
        stats = sdscatprintf(stats, "  Ownership: %s\n", addr_space->use_ownership ? "Yes" : "No");
    } else {
        stats = sdscat(stats, "  Mapping: Not loaded\n");
    }

    return stats;
}
