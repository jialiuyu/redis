/*
 * ub_client_ut.c
 *
 * A standalone UT/integration harness for ub_client data-plane behavior.
 * Export / import are handled externally by obmmctl. This test only consumes
 * an existing shmdev path or memid and validates read-side behavior.
 */

#include "../src/ub_client.h"

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef USE_CC_MODE
#include "../deps/libobmm/obmm_ownership.h"
#endif

typedef enum {
    UB_UT_MODE_NONE = 0,
    UB_UT_MODE_SELFTEST,
    UB_UT_MODE_WRITE_FIXTURE,
    UB_UT_MODE_READ_VERIFY,
    UB_UT_MODE_GATHER,
} ub_ut_mode_t;

typedef struct {
    ub_ut_mode_t mode;
    const char *shm_path;
    unsigned long long shm_memid;
    size_t shm_size;
    size_t table_offset;
    size_t table_size;
    size_t vector_stride_bytes;
    size_t vector_dimension;
    size_t fill_rows;
    const char *table_name;
    const char *element;
    /* comma-separated list of indices for gather mode */
    const char *gather_indices_str;
    ub_element_index_mode_t resolver_mode;
    uint64_t expected_index;
    int has_expected_index;
    int cacheable;
    int use_ownership;
    int verify;   /* for gather mode: check values against write-fixture pattern */
    int verbose;
} ub_ut_options_t;

static void ut_log(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
}

void serverLog(int level, const char *fmt, ...)
{
    va_list ap;

    (void)level;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
}

void *zcalloc(size_t size)
{
    return calloc(1, size);
}

void zfree(void *ptr)
{
    free(ptr);
}

sds sdsempty(void)
{
    char *buf = malloc(1);

    if (buf == NULL) {
        return NULL;
    }
    buf[0] = '\0';
    return buf;
}

sds sdsnew(const char *init)
{
    size_t len;
    char *buf;

    if (init == NULL) {
        init = "";
    }
    len = strlen(init);
    buf = malloc(len + 1);
    if (buf == NULL) {
        return NULL;
    }
    memcpy(buf, init, len + 1);
    return buf;
}

sds sdscat(sds s, const char *t)
{
    size_t slen = s ? strlen(s) : 0;
    size_t tlen = t ? strlen(t) : 0;
    char *buf = realloc(s, slen + tlen + 1);

    if (buf == NULL) {
        free(s);
        return NULL;
    }
    if (tlen != 0) {
        memcpy(buf + slen, t, tlen);
    }
    buf[slen + tlen] = '\0';
    return buf;
}

sds sdscatprintf(sds s, const char *fmt, ...)
{
    va_list ap;
    va_list ap_copy;
    int needed;
    size_t slen = s ? strlen(s) : 0;
    char *buf;

    va_start(ap, fmt);
    va_copy(ap_copy, ap);
    needed = vsnprintf(NULL, 0, fmt, ap_copy);
    va_end(ap_copy);
    if (needed < 0) {
        va_end(ap);
        free(s);
        return NULL;
    }

    buf = realloc(s, slen + (size_t)needed + 1);
    if (buf == NULL) {
        va_end(ap);
        free(s);
        return NULL;
    }
    vsnprintf(buf + slen, (size_t)needed + 1, fmt, ap);
    va_end(ap);
    return buf;
}

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s selftest\n"
            "  %s write-fixture --shm-path <path>|--shm-memid <id> --vector-dimension <n>\n"
            "      [--fill-rows <n>] [--shm-size <bytes>] [--table-offset <bytes>]\n"
            "      [--table-size <bytes>] [--vector-stride-bytes <bytes>]\n"
            "      [--cacheable yes|no] [--use-ownership yes|no]\n"
            "  %s read-verify --table-name <name> --element <name>\n"
            "      --shm-path <path>|--shm-memid <id> --vector-dimension <n>\n"
            "      [--expected-index <n>] [--resolver numeric|suffix-numeric|hash]\n"
            "      [--shm-size <bytes>] [--table-offset <bytes>] [--table-size <bytes>]\n"
            "      [--vector-stride-bytes <bytes>] [--cacheable yes|no] [--use-ownership yes|no]\n"
            "  %s gather --table-name <name> --gather-indices <i0,i1,...>\n"
            "      --shm-path <path>|--shm-memid <id> --vector-dimension <n>\n"
            "      [--shm-size <bytes>] [--table-offset <bytes>] [--table-size <bytes>]\n"
            "      [--vector-stride-bytes <bytes>] [--cacheable yes|no] [--use-ownership yes|no]\n"
            "      [--verify] [--verbose]\n"
            "\n"
            "  gather mode: reads the specified row indices via ub_client_perform_gather_load\n"
            "  and prints each vector. With --verify, checks values against the write-fixture\n"
            "  pattern (row*1000+col). Use on the reader node (111 or 112) after write-fixture\n"
            "  has been run on the writer node.\n",
            prog, prog, prog, prog);
}

static int parse_yesno(const char *text, int *value)
{
    if (!strcasecmp(text, "yes") || !strcasecmp(text, "true") || !strcmp(text, "1")) {
        *value = 1;
        return 0;
    }
    if (!strcasecmp(text, "no") || !strcasecmp(text, "false") || !strcmp(text, "0")) {
        *value = 0;
        return 0;
    }
    return -1;
}

static int parse_size_arg(const char *text, size_t *value)
{
    char *end = NULL;
    unsigned long long base;
    unsigned long long scale = 1;

    errno = 0;
    base = strtoull(text, &end, 0);
    if (errno != 0 || end == text) {
        return -1;
    }
    if (*end != '\0') {
        if (end[1] != '\0') {
            return -1;
        }
        switch (*end) {
        case 'k':
        case 'K':
            scale = 1024ULL;
            break;
        case 'm':
        case 'M':
            scale = 1024ULL * 1024ULL;
            break;
        case 'g':
        case 'G':
            scale = 1024ULL * 1024ULL * 1024ULL;
            break;
        default:
            return -1;
        }
    }
    if (base > SIZE_MAX / scale) {
        return -1;
    }
    *value = (size_t)(base * scale);
    return 0;
}

static int parse_u64_arg(const char *text, uint64_t *value)
{
    char *end = NULL;
    unsigned long long parsed;

    errno = 0;
    parsed = strtoull(text, &end, 0);
    if (errno != 0 || end == text || *end != '\0') {
        return -1;
    }
    *value = parsed;
    return 0;
}

static int parse_index_mode(const char *text, ub_element_index_mode_t *mode)
{
    if (!strcmp(text, "numeric")) {
        *mode = UB_ELEMENT_INDEX_NUMERIC;
        return 0;
    }
    if (!strcmp(text, "suffix-numeric")) {
        *mode = UB_ELEMENT_INDEX_SUFFIX_NUMERIC;
        return 0;
    }
    if (!strcmp(text, "hash")) {
        *mode = UB_ELEMENT_INDEX_HASH;
        return 0;
    }
    return -1;
}

static int ensure_device_path(const ub_ut_options_t *opts, char *path, size_t path_len)
{
    int written;

    if (opts->shm_path != NULL) {
        if (strlen(opts->shm_path) >= path_len) {
            return -1;
        }
        memcpy(path, opts->shm_path, strlen(opts->shm_path) + 1);
        return 0;
    }

    if (opts->shm_memid == 0) {
        return -1;
    }
    written = snprintf(path, path_len, "/dev/obmm_shmdev%llu", opts->shm_memid);
    return (written > 0 && (size_t)written < path_len) ? 0 : -1;
}

static size_t effective_stride(const ub_ut_options_t *opts)
{
    if (opts->vector_stride_bytes != 0) {
        return opts->vector_stride_bytes;
    }
    return opts->vector_dimension * sizeof(float);
}

static size_t effective_table_size(const ub_ut_options_t *opts)
{
    if (opts->table_size != 0) {
        return opts->table_size;
    }
    if (opts->fill_rows != 0) {
        return opts->fill_rows * effective_stride(opts);
    }
    if (opts->shm_size > opts->table_offset) {
        return opts->shm_size - opts->table_offset;
    }
    return 0;
}

static size_t capacity_rows(const ub_ut_options_t *opts)
{
    size_t stride = effective_stride(opts);
    size_t bytes = effective_table_size(opts);

    return (stride == 0) ? 0 : (bytes / stride);
}

static float expected_value(uint64_t row, size_t dim_idx)
{
    return (float)(row * 1000ULL + (uint64_t)dim_idx);
}

static uint64_t fnv1a64_local(const char *text)
{
    const unsigned char *p = (const unsigned char *)text;
    uint64_t hash = UINT64_C(1469598103934665603);

    while (*p) {
        hash ^= (uint64_t)*p++;
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}

static void fill_fixture_vectors(float *table,
                                 size_t rows,
                                 size_t dim,
                                 size_t stride_bytes)
{
    unsigned char *base = (unsigned char *)table;

    memset(base, 0xA5, rows * stride_bytes);
    for (size_t row = 0; row < rows; row++) {
        float *dst = (float *)(base + row * stride_bytes);

        for (size_t col = 0; col < dim; col++) {
            dst[col] = expected_value((uint64_t)row, col);
        }
    }
}

static int map_existing_region(const ub_ut_options_t *opts,
                               int write_access,
                               int *fd_out,
                               void **mapping_out,
                               size_t *mapping_size_out)
{
    char path[UB_DEVICE_PATH_MAX];
    int fd;
    int flags;
    int prot;
    void *mapping;

    if (ensure_device_path(opts, path, sizeof(path)) != 0) {
        ut_log("failed to resolve shm path");
        return -1;
    }

    flags = write_access ? O_RDWR : O_RDONLY;
    if (!opts->cacheable) {
        flags |= O_SYNC;
    }

    fd = open(path, flags, 0);
    if (fd < 0) {
        ut_log("open(%s) failed: %s", path, strerror(errno));
        return -1;
    }

    prot = write_access ? (PROT_READ | PROT_WRITE) : PROT_READ;
    if (opts->cacheable && opts->use_ownership) {
        prot = PROT_NONE;
    }

    mapping = mmap(NULL, opts->shm_size, prot, MAP_SHARED, fd, 0);
    if (mapping == MAP_FAILED) {
        ut_log("mmap(%s) failed: %s", path, strerror(errno));
        close(fd);
        return -1;
    }

    *fd_out = fd;
    *mapping_out = mapping;
    *mapping_size_out = opts->shm_size;
    return 0;
}

static int maybe_set_ownership(const ub_ut_options_t *opts,
                               int fd,
                               void *mapping,
                               size_t mapping_size,
                               int prot)
{
    if (!opts->cacheable || !opts->use_ownership) {
        return 0;
    }
#ifdef USE_CC_MODE
    if (obmm_set_ownership(fd, mapping, (char *)mapping + mapping_size, prot) != 0) {
        ut_log("obmm_set_ownership failed: %s", strerror(errno));
        return -1;
    }
    return 0;
#else
    (void)fd; (void)mapping; (void)mapping_size; (void)prot;
    ut_log("ownership control requires USE_CC_MODE=yes at build time");
    return -1;
#endif
}

static void build_ub_config(const ub_ut_options_t *opts, ub_mem_config_t *cfg)
{
    memset(cfg, 0, sizeof(*cfg));
    cfg->vector_dimension = (int)opts->vector_dimension;
    cfg->cacheable = opts->cacheable;
    cfg->use_ownership = opts->use_ownership;
    cfg->element_index_mode = opts->resolver_mode;
    cfg->shm_memid = opts->shm_memid;
    cfg->shm_size = opts->shm_size;
    cfg->table_offset = opts->table_offset;
    cfg->table_size = effective_table_size(opts);
    cfg->vector_stride_bytes = opts->vector_stride_bytes;
    cfg->table_name = (char *)(opts->table_name ? opts->table_name : "ut_vectors");
    cfg->shm_path = (char *)opts->shm_path;
}

static int run_write_fixture(const ub_ut_options_t *opts)
{
    int fd = -1;
    void *mapping = MAP_FAILED;
    size_t mapping_size = 0;
    size_t stride = effective_stride(opts);
    size_t table_bytes = effective_table_size(opts);
    size_t rows = opts->fill_rows ? opts->fill_rows : capacity_rows(opts);
    unsigned char *table_base;

    if (opts->vector_dimension == 0 || stride < opts->vector_dimension * sizeof(float)) {
        ut_log("invalid vector dimension/stride");
        return 1;
    }
    if (rows == 0 || table_bytes < rows * stride) {
        ut_log("invalid rows/table size");
        return 1;
    }

    if (map_existing_region(opts, 1, &fd, &mapping, &mapping_size) != 0) {
        return 1;
    }

    if (maybe_set_ownership(opts, fd, mapping, mapping_size, PROT_WRITE) != 0) {
        munmap(mapping, mapping_size);
        close(fd);
        return 1;
    }

    table_base = (unsigned char *)mapping + opts->table_offset;
    fill_fixture_vectors((float *)table_base, rows, opts->vector_dimension, stride);

    /* Print written data when --verbose, same format as gather for easy diff */
    if (opts->verbose) {
        for (size_t row = 0; row < rows; row++) {
            const float *vec = (const float *)(table_base + row * stride);
            fprintf(stdout, "row=%zu [", row);
            for (size_t d = 0; d < opts->vector_dimension; d++) {
                fprintf(stdout, "%s%.1f", d ? ", " : "", vec[d]);
            }
            fprintf(stdout, "]\n");
        }
    }

    /* Flush written data. msync works on mmap regions; fsync may fail on
     * device files (e.g. OBMM shmdev returns EINVAL), so treat it as
     * non-fatal. */
    if (msync(mapping, mapping_size, MS_SYNC) != 0) {
        ut_log("warning: msync failed: %s (non-fatal)", strerror(errno));
    }

    if (opts->cacheable && opts->use_ownership) {
        if (maybe_set_ownership(opts, fd, mapping, mapping_size, PROT_NONE) != 0) {
            munmap(mapping, mapping_size);
            close(fd);
            return 1;
        }
    }

    ut_log("fixture written: rows=%zu dim=%zu stride=%zu table_offset=%zu",
           rows, opts->vector_dimension, stride, opts->table_offset);

    munmap(mapping, mapping_size);
    close(fd);
    return 0;
}

static int verify_vector_row(uint64_t row, const float *values, size_t dim)
{
    for (size_t i = 0; i < dim; i++) {
        float expected = expected_value(row, i);
        if (values[i] != expected) {
            ut_log("vector mismatch at row=%" PRIu64 " col=%zu expected=%f actual=%f",
                   row, i, expected, values[i]);
            return -1;
        }
    }
    return 0;
}

static int run_read_verify(const ub_ut_options_t *opts)
{
    ub_mem_config_t cfg;
    ub_address_space_t *addr_space = NULL;
    uint64_t resolved_index = 0;
    float *buffer = NULL;
    sds config_val = NULL;
    sds stats = NULL;

    if (opts->table_name == NULL || opts->element == NULL || opts->vector_dimension == 0) {
        ut_log("missing --table-name / --element / --vector-dimension");
        return 1;
    }

    build_ub_config(opts, &cfg);

    if (ub_client_init(&cfg) != 0) {
        ut_log("ub_client_init failed");
        return 1;
    }

    if (ub_client_load_embedding_table("wrong_table", &addr_space) == 0) {
        ut_log("expected wrong table name to fail");
        ub_client_cleanup();
        return 1;
    }

    if (ub_client_load_embedding_table(opts->table_name, &addr_space) != 0 || addr_space == NULL) {
        ut_log("ub_client_load_embedding_table failed");
        ub_client_cleanup();
        return 1;
    }

    if (ub_client_load_embedding_table(opts->table_name, &addr_space) != 0 ||
        global_ub_client == NULL || global_ub_client->cache_hits == 0) {
        ut_log("expected second table load to hit cache");
        ub_client_cleanup();
        return 1;
    }

    config_val = ub_client_get_config("ub-table-name");
    if (config_val == NULL || strcmp(config_val, opts->table_name) != 0) {
        ut_log("ub_client_get_config(ub-table-name) mismatch");
        free(config_val);
        ub_client_cleanup();
        return 1;
    }
    free(config_val);
    config_val = NULL;

    if (ub_client_resolve_element_index(opts->element, &resolved_index, opts->vector_dimension) != 0) {
        ut_log("ub_client_resolve_element_index failed for %s", opts->element);
        ub_client_cleanup();
        return 1;
    }

    if (opts->has_expected_index && resolved_index != opts->expected_index) {
        ut_log("resolved index mismatch expected=%" PRIu64 " actual=%" PRIu64,
               opts->expected_index, resolved_index);
        ub_client_cleanup();
        return 1;
    }

    buffer = calloc(opts->vector_dimension, sizeof(float));
    if (buffer == NULL) {
        ub_client_cleanup();
        return 1;
    }

    if (ub_client_perform_gather_load(addr_space,
                                      &resolved_index,
                                      1,
                                      buffer,
                                      opts->vector_dimension) != 0) {
        ut_log("ub_client_perform_gather_load failed");
        free(buffer);
        ub_client_cleanup();
        return 1;
    }

    if (verify_vector_row(resolved_index, buffer, opts->vector_dimension) != 0) {
        free(buffer);
        ub_client_cleanup();
        return 1;
    }

    stats = ub_client_get_stats();
    if (stats == NULL || strstr(stats, "UB Client Stats:") == NULL) {
        ut_log("ub_client_get_stats returned unexpected output");
        free(stats);
        free(buffer);
        ub_client_cleanup();
        return 1;
    }

    if (opts->verbose) {
        ut_log("%s", stats);
    }

    free(stats);
    free(buffer);
    ub_client_cleanup();
    ut_log("read verification succeeded for element=%s index=%" PRIu64,
           opts->element, resolved_index);
    return 0;
}

/*
 * gather mode: parse a comma-separated list of row indices, call
 * ub_client_perform_gather_load for all of them in one shot, then
 * print (and optionally verify) each result vector.
 *
 * Node 111 (writer): run write-fixture to stamp the shared memory.
 * Node 112 (reader): run gather to pull rows via the UB link.
 */
static int run_gather(const ub_ut_options_t *opts)
{
    ub_mem_config_t cfg;
    ub_address_space_t *addr_space = NULL;
    uint64_t *indices = NULL;
    float *results = NULL;
    size_t num_indices = 0;
    size_t i;
    int rc = 1;

    if (opts->table_name == NULL || opts->gather_indices_str == NULL ||
        opts->vector_dimension == 0) {
        ut_log("gather requires --table-name, --gather-indices, --vector-dimension");
        return 1;
    }

    /* Count and parse comma-separated indices */
    {
        const char *p = opts->gather_indices_str;
        num_indices = 1;
        while (*p) {
            if (*p++ == ',') num_indices++;
        }
        indices = calloc(num_indices, sizeof(uint64_t));
        if (!indices) {
            ut_log("OOM allocating indices");
            return 1;
        }
        p = opts->gather_indices_str;
        for (i = 0; i < num_indices; i++) {
            char *end;
            errno = 0;
            indices[i] = strtoull(p, &end, 10);
            if (errno != 0 || end == p) {
                ut_log("invalid index at position %zu in '%s'", i, opts->gather_indices_str);
                free(indices);
                return 1;
            }
            p = (*end == ',') ? end + 1 : end;
        }
    }

    results = calloc(num_indices * opts->vector_dimension, sizeof(float));
    if (!results) {
        ut_log("OOM allocating results");
        free(indices);
        return 1;
    }

    build_ub_config(opts, &cfg);

    if (ub_client_init(&cfg) != 0) {
        ut_log("ub_client_init failed");
        goto out;
    }

    if (ub_client_load_embedding_table(opts->table_name, &addr_space) != 0 ||
        addr_space == NULL) {
        ut_log("ub_client_load_embedding_table failed");
        ub_client_cleanup();
        goto out;
    }

    if (ub_client_perform_gather_load(addr_space,
                                      indices,
                                      num_indices,
                                      results,
                                      opts->vector_dimension) != 0) {
        ut_log("ub_client_perform_gather_load failed");
        ub_client_cleanup();
        goto out;
    }

    /* Print and optionally verify each row */
    rc = 0;
    for (i = 0; i < num_indices; i++) {
        uint64_t row = indices[i];
        const float *vec = results + i * opts->vector_dimension;

        if (opts->verbose) {
            fprintf(stdout, "row=%" PRIu64 " [", row);
            for (size_t d = 0; d < opts->vector_dimension; d++) {
                fprintf(stdout, "%s%.1f", d ? ", " : "", vec[d]);
            }
            fprintf(stdout, "]\n");
        }

        if (opts->verify) {
            if (verify_vector_row(row, vec, opts->vector_dimension) != 0) {
                rc = 1;
            }
        }
    }

    if (rc == 0) {
        sds stats = ub_client_get_stats();
        ut_log("gather OK: %zu rows, dim=%zu", num_indices, opts->vector_dimension);
        if (opts->verbose && stats) {
            ut_log("%s", stats);
        }
        free(stats);
    }

    ub_client_cleanup();
out:
    free(results);
    free(indices);
    return rc;
}

static int run_selftest(void)
{
    ub_ut_options_t opts;
    char tmp_template[] = "/tmp/ub_client_ut.XXXXXX";
    int fd = -1;
    size_t stride;
    uint64_t hash_expected;
    int rc = 1;

    memset(&opts, 0, sizeof(opts));
    opts.mode = UB_UT_MODE_SELFTEST;
    opts.vector_dimension = 4;
    opts.fill_rows = 8;
    opts.table_offset = 128;
    opts.vector_stride_bytes = 32;
    opts.table_name = "ut_vectors";
    opts.cacheable = 0;
    opts.use_ownership = 0;
    opts.resolver_mode = UB_ELEMENT_INDEX_SUFFIX_NUMERIC;
    stride = effective_stride(&opts);
    opts.table_size = opts.fill_rows * stride;
    opts.shm_size = opts.table_offset + opts.table_size;

    fd = mkstemp(tmp_template);
    if (fd < 0) {
        ut_log("mkstemp failed: %s", strerror(errno));
        return 1;
    }
    if (ftruncate(fd, (off_t)opts.shm_size) != 0) {
        ut_log("ftruncate failed: %s", strerror(errno));
        close(fd);
        unlink(tmp_template);
        return 1;
    }
    close(fd);
    opts.shm_path = tmp_template;

    if (run_write_fixture(&opts) != 0) {
        goto cleanup;
    }

    opts.element = "row:3";
    opts.has_expected_index = 1;
    opts.expected_index = 3;
    if (run_read_verify(&opts) != 0) {
        goto cleanup;
    }

    opts.resolver_mode = UB_ELEMENT_INDEX_NUMERIC;
    opts.element = "6";
    opts.expected_index = 6;
    if (run_read_verify(&opts) != 0) {
        goto cleanup;
    }

    opts.resolver_mode = UB_ELEMENT_INDEX_HASH;
    opts.element = "hash-key";
    hash_expected = fnv1a64_local(opts.element) % opts.fill_rows;
    opts.expected_index = hash_expected;
    if (run_read_verify(&opts) != 0) {
        goto cleanup;
    }

    ut_log("selftest succeeded");
    rc = 0;

cleanup:
    unlink(tmp_template);
    return rc;
}

static int parse_mode(const char *text, ub_ut_mode_t *mode)
{
    if (!strcmp(text, "selftest")) {
        *mode = UB_UT_MODE_SELFTEST;
        return 0;
    }
    if (!strcmp(text, "write-fixture")) {
        *mode = UB_UT_MODE_WRITE_FIXTURE;
        return 0;
    }
    if (!strcmp(text, "read-verify")) {
        *mode = UB_UT_MODE_READ_VERIFY;
        return 0;
    }
    if (!strcmp(text, "gather")) {
        *mode = UB_UT_MODE_GATHER;
        return 0;
    }
    return -1;
}

static int parse_args(int argc, char **argv, ub_ut_options_t *opts)
{
    static const struct option long_opts[] = {
        {"shm-path", required_argument, NULL, 'p'},
        {"shm-memid", required_argument, NULL, 'm'},
        {"shm-size", required_argument, NULL, 's'},
        {"table-offset", required_argument, NULL, 'o'},
        {"table-size", required_argument, NULL, 't'},
        {"vector-stride-bytes", required_argument, NULL, 'S'},
        {"vector-dimension", required_argument, NULL, 'd'},
        {"fill-rows", required_argument, NULL, 'r'},
        {"table-name", required_argument, NULL, 'n'},
        {"element", required_argument, NULL, 'e'},
        {"gather-indices", required_argument, NULL, 'g'},
        {"expected-index", required_argument, NULL, 'i'},
        {"resolver", required_argument, NULL, 'R'},
        {"cacheable", required_argument, NULL, 'c'},
        {"use-ownership", required_argument, NULL, 'u'},
        {"verify", no_argument, NULL, 'V'},
        {"verbose", no_argument, NULL, 'v'},
        {"help", no_argument, NULL, 'h'},
        {0, 0, 0, 0},
    };
    int ch;
    uint64_t tmp_u64;
    size_t tmp_size;
    int tmp_bool;

    memset(opts, 0, sizeof(*opts));
    opts->table_name = "ut_vectors";
    opts->resolver_mode = UB_ELEMENT_INDEX_SUFFIX_NUMERIC;

    if (argc < 2 || parse_mode(argv[1], &opts->mode) != 0) {
        return -1;
    }
    if (opts->mode == UB_UT_MODE_SELFTEST) {
        return 0;
    }

    optind = 2;
    while ((ch = getopt_long(argc, argv, "p:m:s:o:t:S:d:r:n:e:g:i:R:c:u:Vvh",
                             long_opts, NULL)) != -1) {
        switch (ch) {
        case 'p':
            opts->shm_path = optarg;
            break;
        case 'm':
            if (parse_u64_arg(optarg, &tmp_u64) != 0) {
                return -1;
            }
            opts->shm_memid = tmp_u64;
            break;
        case 's':
            if (parse_size_arg(optarg, &tmp_size) != 0) {
                return -1;
            }
            opts->shm_size = tmp_size;
            break;
        case 'o':
            if (parse_size_arg(optarg, &tmp_size) != 0) {
                return -1;
            }
            opts->table_offset = tmp_size;
            break;
        case 't':
            if (parse_size_arg(optarg, &tmp_size) != 0) {
                return -1;
            }
            opts->table_size = tmp_size;
            break;
        case 'S':
            if (parse_size_arg(optarg, &tmp_size) != 0) {
                return -1;
            }
            opts->vector_stride_bytes = tmp_size;
            break;
        case 'd':
            if (parse_size_arg(optarg, &tmp_size) != 0) {
                return -1;
            }
            opts->vector_dimension = tmp_size;
            break;
        case 'r':
            if (parse_size_arg(optarg, &tmp_size) != 0) {
                return -1;
            }
            opts->fill_rows = tmp_size;
            break;
        case 'n':
            opts->table_name = optarg;
            break;
        case 'e':
            opts->element = optarg;
            break;
        case 'g':
            opts->gather_indices_str = optarg;
            break;
        case 'i':
            if (parse_u64_arg(optarg, &tmp_u64) != 0) {
                return -1;
            }
            opts->expected_index = tmp_u64;
            opts->has_expected_index = 1;
            break;
        case 'R':
            if (parse_index_mode(optarg, &opts->resolver_mode) != 0) {
                return -1;
            }
            break;
        case 'c':
            if (parse_yesno(optarg, &tmp_bool) != 0) {
                return -1;
            }
            opts->cacheable = tmp_bool;
            break;
        case 'u':
            if (parse_yesno(optarg, &tmp_bool) != 0) {
                return -1;
            }
            opts->use_ownership = tmp_bool;
            break;
        case 'V':
            opts->verify = 1;
            break;
        case 'v':
            opts->verbose = 1;
            break;
        case 'h':
            return 1;
        default:
            return -1;
        }
    }

    if (opts->shm_path == NULL && opts->shm_memid == 0) {
        return -1;
    }
    if (opts->vector_dimension == 0) {
        return -1;
    }
    if (opts->mode == UB_UT_MODE_WRITE_FIXTURE) {
        if (opts->fill_rows == 0) {
            return -1;
        }
        if (opts->table_size == 0) {
            opts->table_size = opts->fill_rows * effective_stride(opts);
        }
        if (opts->shm_size == 0) {
            opts->shm_size = opts->table_offset + opts->table_size;
        }
    } else if (opts->mode == UB_UT_MODE_READ_VERIFY) {
        if (opts->element == NULL) {
            return -1;
        }
        if (opts->table_size == 0 && opts->shm_size > opts->table_offset) {
            opts->table_size = opts->shm_size - opts->table_offset;
        }
    } else if (opts->mode == UB_UT_MODE_GATHER) {
        if (opts->gather_indices_str == NULL) {
            return -1;
        }
        if (opts->table_size == 0 && opts->shm_size > opts->table_offset) {
            opts->table_size = opts->shm_size - opts->table_offset;
        }
    }

    return 0;
}

int main(int argc, char **argv)
{
    ub_ut_options_t opts;
    int parse_rc = parse_args(argc, argv, &opts);

    if (parse_rc != 0) {
        usage(argv[0]);
        return (parse_rc > 0) ? 0 : 1;
    }

    switch (opts.mode) {
    case UB_UT_MODE_SELFTEST:
        return run_selftest();
    case UB_UT_MODE_WRITE_FIXTURE:
        return run_write_fixture(&opts);
    case UB_UT_MODE_READ_VERIFY:
        return run_read_verify(&opts);
    case UB_UT_MODE_GATHER:
        return run_gather(&opts);
    default:
        usage(argv[0]);
        return 1;
    }
}
