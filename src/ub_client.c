/*
 * UB client data-plane implementation.
 * Export/import are managed out-of-process by obmmctl; Redis only consumes
 * an existing shmdev through configuration.
 */

#include "ub_client.h"
#include "server.h"

#include <ctype.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

typedef int (*obmm_set_ownership_func)(int fd, void *start, void *end, int prot);

ub_client_t *global_ub_client = NULL;

static void *obmm_handle = NULL;
static obmm_set_ownership_func obmm_set_ownership_ptr = NULL;

static int same_string(const char *left, const char *right)
{
    if (left == right) {
        return 1;
    }
    if (left == NULL || right == NULL) {
        return 0;
    }
    return strcmp(left, right) == 0;
}

static char *dup_config_string(const char *value)
{
    size_t len;
    char *copy;

    if (value == NULL) {
        return NULL;
    }
    len = strlen(value) + 1;
    copy = zcalloc(len);
    if (copy != NULL) {
        memcpy(copy, value, len);
    }
    return copy;
}

static void free_config_strings(ub_mem_config_t *cfg)
{
    if (cfg == NULL) {
        return;
    }
    if (cfg->table_name) {
        zfree(cfg->table_name);
        cfg->table_name = NULL;
    }
    if (cfg->shm_path) {
        zfree(cfg->shm_path);
        cfg->shm_path = NULL;
    }
}

static int copy_config(ub_mem_config_t *dst, const ub_mem_config_t *src)
{
    memset(dst, 0, sizeof(*dst));
    dst->vector_dimension = src->vector_dimension;
    dst->cacheable = src->cacheable;
    dst->use_ownership = src->use_ownership;
    dst->element_index_mode = src->element_index_mode;
    dst->shm_memid = src->shm_memid;
    dst->shm_size = src->shm_size;
    dst->table_offset = src->table_offset;
    dst->table_size = src->table_size;
    dst->vector_stride_bytes = src->vector_stride_bytes;
    dst->table_name = dup_config_string(src->table_name);
    if (src->table_name != NULL && dst->table_name == NULL) {
        return C_ERR;
    }
    dst->shm_path = dup_config_string(src->shm_path);
    if (src->shm_path != NULL && dst->shm_path == NULL) {
        free_config_strings(dst);
        return C_ERR;
    }
    return C_OK;
}

static int config_equals(const ub_mem_config_t *left, const ub_mem_config_t *right)
{
    return left->vector_dimension == right->vector_dimension &&
           left->cacheable == right->cacheable &&
           left->use_ownership == right->use_ownership &&
           left->element_index_mode == right->element_index_mode &&
           left->shm_memid == right->shm_memid &&
           left->shm_size == right->shm_size &&
           left->table_offset == right->table_offset &&
           left->table_size == right->table_size &&
           left->vector_stride_bytes == right->vector_stride_bytes &&
           same_string(left->table_name, right->table_name) &&
           same_string(left->shm_path, right->shm_path);
}

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

static int load_obmm_library(void)
{
    if (obmm_set_ownership_ptr) {
        return C_OK;
    }

    obmm_handle = dlopen("libobmm.so", RTLD_LAZY);
    if (obmm_handle == NULL) {
        _serverLog(LL_WARNING, "Failed to load libobmm.so for ownership control: %s",
                   dlerror());
        return C_ERR;
    }

    obmm_set_ownership_ptr = (obmm_set_ownership_func)dlsym(obmm_handle, "obmm_set_ownership");
    if (obmm_set_ownership_ptr == NULL) {
        _serverLog(LL_WARNING, "Failed to resolve obmm_set_ownership: %s",
                   dlerror());
        dlclose(obmm_handle);
        obmm_handle = NULL;
        return C_ERR;
    }
    return C_OK;
}

static void unload_obmm_library(void)
{
    obmm_set_ownership_ptr = NULL;
    if (obmm_handle) {
        dlclose(obmm_handle);
        obmm_handle = NULL;
    }
}

static int set_read_ownership(ub_address_space_t *addr_space)
{
    void *start = addr_space->mapping_addr;
    void *end = (char *)addr_space->mapping_addr + addr_space->mapping_size;

    if (!addr_space->cacheable || !addr_space->use_ownership) {
        return C_OK;
    }
    if (load_obmm_library() != C_OK) {
        return C_ERR;
    }
    if (obmm_set_ownership_ptr(addr_space->shm_fd, start, end, PROT_READ) != 0) {
        _serverLog(LL_WARNING,
                   "Failed to acquire OBMM read ownership for %s: %s",
                   addr_space->device_path, strerror(errno));
        return C_ERR;
    }

    addr_space->use_ownership = 1;
    return C_OK;
}

static void release_ownership(ub_address_space_t *addr_space)
{
    void *start = addr_space->mapping_addr;
    void *end = (char *)addr_space->mapping_addr + addr_space->mapping_size;

    if (!addr_space || !addr_space->use_ownership || !obmm_set_ownership_ptr) {
        return;
    }
    if (obmm_set_ownership_ptr(addr_space->shm_fd, start, end, PROT_NONE) != 0) {
        _serverLog(LL_WARNING,
                   "Failed to release OBMM ownership for %s: %s",
                   addr_space->device_path, strerror(errno));
    }
}

static void destroy_addr_space(ub_address_space_t *addr_space)
{
    if (!addr_space) {
        return;
    }

    release_ownership(addr_space);

    if (addr_space->mapping_addr && addr_space->mapping_addr != MAP_FAILED &&
        addr_space->mapping_size > 0) {
        munmap(addr_space->mapping_addr, addr_space->mapping_size);
    }
    if (addr_space->shm_fd >= 0) {
        close(addr_space->shm_fd);
    }

    zfree(addr_space);
}

static int addr_space_matches_config(const ub_address_space_t *addr_space,
                                     const ub_mem_config_t *cfg,
                                     const char *device_path,
                                     size_t table_size,
                                     size_t vector_stride)
{
    if (!addr_space || !device_path) {
        return 0;
    }

    return addr_space->mem_id == cfg->shm_memid &&
           addr_space->mapping_size == cfg->shm_size &&
           addr_space->data_offset == cfg->table_offset &&
           addr_space->size == table_size &&
           addr_space->vector_stride_bytes == vector_stride &&
           addr_space->cacheable == cfg->cacheable &&
           addr_space->use_ownership == cfg->use_ownership &&
           strcmp(addr_space->device_path, device_path) == 0;
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
        _serverLog(LL_WARNING, "Invalid UB table size/offset configuration");
        return C_ERR;
    }

    if (configured_device_path(cfg, device_path) != C_OK) {
        _serverLog(LL_WARNING, "Missing UB shmdev configuration");
        return C_ERR;
    }

    vector_stride = configured_vector_stride(cfg, cfg->vector_dimension);
    if (vector_stride == 0 || vector_stride < cfg->vector_dimension * sizeof(float)) {
        _serverLog(LL_WARNING, "Invalid UB vector stride configuration");
        return C_ERR;
    }
    if (table_size < vector_stride) {
        _serverLog(LL_WARNING, "UB table size is smaller than one vector row");
        return C_ERR;
    }

    open_flags = O_RDWR;
    if (!cfg->cacheable) {
        open_flags |= O_SYNC;
    }

    fd = open(device_path, open_flags);
    if (fd < 0) {
        _serverLog(LL_WARNING, "Failed to open %s: %s", device_path, strerror(errno));
        return C_ERR;
    }

    mmap_prot = (cfg->cacheable && cfg->use_ownership) ? PROT_NONE : PROT_READ;
    mapping = mmap(NULL, cfg->shm_size, mmap_prot, MAP_SHARED, fd, 0);
    if (mapping == MAP_FAILED) {
        _serverLog(LL_WARNING, "Failed to mmap %s: %s", device_path, strerror(errno));
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
    ub_mem_config_t next_cfg = {0};

    if (cfg == NULL) {
        errno = EINVAL;
        return C_ERR;
    }
    if (copy_config(&next_cfg, cfg) != C_OK) {
        return C_ERR;
    }

    if (global_ub_client) {
        if (!config_equals(&global_ub_client->config, &next_cfg)) {
            destroy_addr_space(global_ub_client->global_ubas);
            global_ub_client->global_ubas = NULL;
            free_config_strings(&global_ub_client->config);
            global_ub_client->config = next_cfg;
            memset(&next_cfg, 0, sizeof(next_cfg));
        }
        free_config_strings(&next_cfg);
        return C_OK;
    }

    global_ub_client = zcalloc(sizeof(*global_ub_client));
    if (!global_ub_client) {
        return C_ERR;
    }

    global_ub_client->config = next_cfg;
    memset(&next_cfg, 0, sizeof(next_cfg));
    global_ub_client->initialized = 1;
    _serverLog(LL_NOTICE, "UB client initialized in data-plane mode");
    return C_OK;
}

void ub_client_cleanup(void)
{
    if (!global_ub_client) {
        return;
    }

    destroy_addr_space(global_ub_client->global_ubas);
    global_ub_client->global_ubas = NULL;
    free_config_strings(&global_ub_client->config);
    unload_obmm_library();

    zfree(global_ub_client);
    global_ub_client = NULL;

    _serverLog(LL_NOTICE, "UB client cleaned up");
}

int ub_client_load_embedding_table(const char *resource_name,
                                   ub_address_space_t **addr_space)
{
    const ub_mem_config_t *cfg;
    char device_path[UB_DEVICE_PATH_MAX];
    size_t table_size;
    size_t vector_stride;

    if (!addr_space || !global_ub_client || !global_ub_client->initialized) {
        return C_ERR;
    }
    cfg = &global_ub_client->config;
    if (!resource_name_matches(cfg, resource_name)) {
        _serverLog(LL_WARNING, "UB table resource mismatch for key %s",
                   resource_name ? resource_name : "(null)");
        return C_ERR;
    }
    if (configured_table_size(cfg, &table_size) != C_OK ||
        configured_device_path(cfg, device_path) != C_OK) {
        return C_ERR;
    }

    vector_stride = configured_vector_stride(cfg, cfg->vector_dimension);
    if (addr_space_matches_config(global_ub_client->global_ubas,
                                  cfg,
                                  device_path,
                                  table_size,
                                  vector_stride)) {
        global_ub_client->cache_hits++;
        *addr_space = global_ub_client->global_ubas;
        return C_OK;
    }

    global_ub_client->cache_misses++;
    destroy_addr_space(global_ub_client->global_ubas);
    global_ub_client->global_ubas = NULL;

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
    const ub_mem_config_t *cfg = global_ub_client ? &global_ub_client->config : NULL;
    ub_address_space_t *addr_space = global_ub_client ? global_ub_client->global_ubas : NULL;
    size_t vector_stride = configured_vector_stride(cfg, vector_dim);
    uint64_t capacity;
    uint64_t resolved = 0;

    if (!element_name || !index || !addr_space || cfg == NULL || vector_stride == 0) {
        return C_ERR;
    }

    capacity = addr_space->size / vector_stride;
    if (capacity == 0) {
        return C_ERR;
    }

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
        _serverLog(LL_WARNING, "Resolved UB index %" PRIu64 " out of range (capacity=%" PRIu64 ")",
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

    for (size_t i = 0; i < num_indices; i++) {
        uint64_t idx = indices[i];
        const char *src;

        if (idx >= capacity) {
            _serverLog(LL_WARNING, "UB index %" PRIu64 " out of bounds", idx);
            return C_ERR;
        }

        src = base + (idx * addr_space->vector_stride_bytes);
        __builtin_prefetch(src, 0, 1);
        memcpy(&results[i * vector_dim], src, vector_bytes);
    }

    global_ub_client->total_requests += num_indices;
    return C_OK;
}

int ub_client_set_config(const char *key, const char *value)
{
    UNUSED(key);
    UNUSED(value);
    _serverLog(LL_WARNING, "Use CONFIG SET for UB client parameters");
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
        return global_ub_client->config.table_name ? sdsnew(global_ub_client->config.table_name) : NULL;
    }
    if (!strcasecmp(key, "ub-shm-path")) {
        return global_ub_client->config.shm_path ? sdsnew(global_ub_client->config.shm_path) : NULL;
    }
    if (!strcasecmp(key, "ub-shm-memid")) {
        return sdscatprintf(sdsempty(), "%llu", global_ub_client->config.shm_memid);
    }
    if (!strcasecmp(key, "ub-shm-size")) {
        return sdscatprintf(sdsempty(), "%zu", global_ub_client->config.shm_size);
    }
    if (!strcasecmp(key, "ub-table-offset")) {
        return sdscatprintf(sdsempty(), "%zu", global_ub_client->config.table_offset);
    }
    if (!strcasecmp(key, "ub-table-size")) {
        return sdscatprintf(sdsempty(), "%zu", global_ub_client->config.table_size);
    }
    if (!strcasecmp(key, "ub-vector-stride-bytes")) {
        return sdscatprintf(sdsempty(), "%zu", global_ub_client->config.vector_stride_bytes);
    }
    if (!strcasecmp(key, "vector-dimension")) {
        return sdscatprintf(sdsempty(), "%d", global_ub_client->config.vector_dimension);
    }
    if (!strcasecmp(key, "ub-cacheable")) {
        return sdsnew(global_ub_client->config.cacheable ? "yes" : "no");
    }
    if (!strcasecmp(key, "ub-use-ownership")) {
        return sdsnew(global_ub_client->config.use_ownership ? "yes" : "no");
    }
    if (!strcasecmp(key, "ub-element-index-mode")) {
        switch (global_ub_client->config.element_index_mode) {
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
