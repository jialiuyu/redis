/*
 * sve_gather_ub_bench.c
 *
 * Benchmark comparing per-embedding serial read vs cross-embedding SVE gather
 * read on real UB.MEM device (or local malloc mock).
 *
 * Modes:
 *   serial       — sve_serial_contiguous_read (baseline)
 *   cross-gather — sve_cross_emb_gather_read (new SVE gather)
 *   ub-gather    — ub_client_perform_gather_load (per-row SVE contiguous)
 *   memcpy       — plain memcpy per row
 *
 * Build:
 *   make sve_gather_ub_bench USE_SVE=yes [USE_CC_MODE=yes]
 */

#include "../src/ub_client.h"
#include "../src/sve_operation.h"
#include "../src/sve_config.h"

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

#ifdef USE_CC_MODE
#include "../deps/libobmm/obmm_ownership.h"
#endif

#include "test_runtime_shim.h"

/* ============================================================
 * Benchmark configuration
 * ============================================================ */

typedef enum {
    BENCH_MODE_SERIAL = 0,
    BENCH_MODE_CROSS_GATHER,
    BENCH_MODE_MEMCPY,
    BENCH_MODE_COUNT
} bench_mode_t;

static const char *mode_names[BENCH_MODE_COUNT] = {
    "serial", "cross-gather", "memcpy"
};

typedef struct {
    unsigned long long shm_memid;
    size_t shm_size;
    size_t vector_dimension;
    size_t vector_stride_bytes;
    size_t table_offset;
    size_t table_size;
    const char *table_name;
    const char *batch_sizes_str;
    int warmup;
    int iters;
    int cacheable;
    int use_ownership;
    int mock_local;
    size_t fill_rows;
    int verify;
    int verbose;
    int threads;
} bench_opts_t;

/* ============================================================
 * Helpers
 * ============================================================ */

static double elapsed_ns(const struct timespec *t0, const struct timespec *t1)
{
    return (t1->tv_sec - t0->tv_sec) * 1e9 +
           (t1->tv_nsec - t0->tv_nsec);
}

static float expected_value(uint64_t row, size_t col)
{
    return (float)(row * 1000ULL + (uint64_t)col);
}

static void fill_fixture_vectors(float *table, size_t rows,
                                 size_t dim, size_t stride_bytes)
{
    unsigned char *base = (unsigned char *)table;
    memset(base, 0xA5, rows * stride_bytes);
    for (size_t row = 0; row < rows; row++) {
        float *dst = (float *)(base + row * stride_bytes);
        for (size_t col = 0; col < dim; col++)
            dst[col] = expected_value((uint64_t)row, col);
    }
}

static size_t parse_size(const char *text)
{
    char *end = NULL;
    unsigned long long base = strtoull(text, &end, 0);
    if (end == text) return 0;
    switch (*end) {
    case 'k': case 'K': return (size_t)(base * 1024ULL);
    case 'm': case 'M': return (size_t)(base * 1024ULL * 1024ULL);
    case 'g': case 'G': return (size_t)(base * 1024ULL * 1024ULL * 1024ULL);
    }
    return (size_t)base;
}

static size_t effective_stride(const bench_opts_t *opts)
{
    return opts->vector_stride_bytes ? opts->vector_stride_bytes
                                     : opts->vector_dimension * sizeof(float);
}

static void *alloc_results(size_t num_ids, size_t dim)
{
    return calloc(num_ids * dim, sizeof(float));
}

static uint64_t *generate_random_ids(size_t num_ids, size_t max_row)
{
    uint64_t *ids = calloc(num_ids, sizeof(uint64_t));
    if (!ids) return NULL;
    for (size_t i = 0; i < num_ids; i++)
        ids[i] = (uint64_t)(rand() % max_row);
    return ids;
}

static int verify_results(const float *results, const uint64_t *ids,
                          size_t num_ids, size_t dim)
{
    int errors = 0;
    for (size_t i = 0; i < num_ids; i++) {
        const float *vec = results + i * dim;
        for (size_t d = 0; d < dim; d++) {
            float exp = expected_value(ids[i], d);
            if (vec[d] != exp) {
                if (errors < 5)
                    fprintf(stderr,
                            "  MISMATCH row=%" PRIu64 " dim=%zu: got %.1f expected %.1f\n",
                            ids[i], d, vec[d], exp);
                errors++;
            }
        }
    }
    return errors;
}

/* ============================================================
 * Benchmark runners
 * ============================================================ */

typedef struct {
    double avg_ns;
    double min_ns;
    double max_ns;
    double per_emb_ns;
    double throughput_mbs;
    int verify_errors;
} bench_result_t;

static void run_memcpy_baseline(sve_gather_ctx_t *ctx,
                                const uint64_t *ids, size_t num_ids,
                                size_t dim, float *results)
{
    const char *base = (const char *)ctx->ubas->mapped_addr;
    size_t stride = ctx->vector_stride_bytes;
    size_t row_bytes = dim * sizeof(float);
    for (size_t i = 0; i < num_ids; i++) {
        while (bitmap_try_acquire(ctx->bitmap, ids[i]) != 0) {
            __asm__ __volatile__("yield" ::: "memory");
        }
        if (ids[i] >= ctx->table_row_capacity) {
            bitmap_release(ctx->bitmap, ids[i]);
            continue;
        }
        memcpy(results + i * dim,
               base + ids[i] * stride, row_bytes);
        bitmap_release(ctx->bitmap, ids[i]);
    }
}

static void run_bench_mode(bench_mode_t mode,
                           sve_gather_ctx_t *ctx,
                           uint64_t *ids, size_t num_ids,
                           size_t dim, float *results,
                           int iters,
                           bench_result_t *out)
{
    double total_ns = 0, min_ns = 1e18, max_ns = 0;
    size_t total_bytes = num_ids * dim * sizeof(float);

    for (int it = 0; it < iters; it++) {
        memset(results, 0, num_ids * dim * sizeof(float));
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);

        switch (mode) {
        case BENCH_MODE_SERIAL:
            sve_serial_contiguous_read(ctx, ids, num_ids, results);
            break;
        case BENCH_MODE_CROSS_GATHER:
            sve_cross_emb_gather_read(ctx, ids, num_ids, results);
            break;
        case BENCH_MODE_MEMCPY:
            run_memcpy_baseline(ctx, ids, num_ids, dim, results);
            break;
        default:
            break;
        }

        clock_gettime(CLOCK_MONOTONIC, &t1);
        double ns = elapsed_ns(&t0, &t1);
        total_ns += ns;
        if (ns < min_ns) min_ns = ns;
        if (ns > max_ns) max_ns = ns;
    }

    out->avg_ns = total_ns / iters;
    out->min_ns = min_ns;
    out->max_ns = max_ns;
    out->per_emb_ns = out->avg_ns / (double)num_ids;
    out->throughput_mbs = (out->avg_ns > 0)
        ? (double)total_bytes / (out->avg_ns / 1e3)  /* bytes/us = MB/s */
        : 0;
    out->verify_errors = 0;
}

typedef struct {
    bench_mode_t mode;
    sve_gather_ctx_t *ctx;
    size_t dim;
    size_t max_rows;
    size_t batch;
    int iters;
    double total_ns;
    uint64_t lock_success;
    uint64_t lock_failure;
} thread_arg_t;

static void *thread_worker(void *arg)
{
    thread_arg_t *ta = (thread_arg_t *)arg;
    uint64_t *ids = generate_random_ids(ta->batch, ta->max_rows);
    float *results = alloc_results(ta->batch, ta->dim);
    if (!ids || !results) {
        free(ids);
        free(results);
        return NULL;
    }

    uint64_t succ_before = atomic_load_explicit(
        &ta->ctx->stats->lock_success, memory_order_relaxed);
    uint64_t fail_before = atomic_load_explicit(
        &ta->ctx->stats->lock_failure, memory_order_relaxed);

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int it = 0; it < ta->iters; it++) {
        for (size_t j = 0; j < ta->batch; j++)
            ids[j] = (uint64_t)(rand() % ta->max_rows);

        switch (ta->mode) {
        case BENCH_MODE_SERIAL:
            sve_serial_contiguous_read(ta->ctx, ids, ta->batch, results);
            break;
        case BENCH_MODE_CROSS_GATHER:
            sve_cross_emb_gather_read(ta->ctx, ids, ta->batch, results);
            break;
        case BENCH_MODE_MEMCPY:
            run_memcpy_baseline(ta->ctx, ids, ta->batch, ta->dim, results);
            break;
        default:
            break;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    ta->total_ns = elapsed_ns(&t0, &t1);
    ta->lock_success = atomic_load_explicit(
        &ta->ctx->stats->lock_success, memory_order_relaxed) - succ_before;
    ta->lock_failure = atomic_load_explicit(
        &ta->ctx->stats->lock_failure, memory_order_relaxed) - fail_before;

    free(ids);
    free(results);
    return NULL;
}

/* ============================================================
 * Main benchmark loop
 * ============================================================ */

static void print_csv_header(FILE *f)
{
    fprintf(f, "mode,batch_size,avg_ns,min_ns,max_ns,per_emb_ns,"
               "throughput_MB_s,verify_errors\n");
}

static void print_csv_row(FILE *f, const char *mode, size_t batch,
                          const bench_result_t *r)
{
    fprintf(f, "%s,%zu,%.1f,%.1f,%.1f,%.1f,%.1f,%d\n",
            mode, batch, r->avg_ns, r->min_ns, r->max_ns,
            r->per_emb_ns, r->throughput_mbs, r->verify_errors);
}

static void usage(const char *prog)
{
    fprintf(stderr,
        "Usage: %s [options]\n"
        "\n"
        "UB device options:\n"
        "  --shm-memid <id>          UB device memid\n"
        "  --shm-size <bytes>        Mapping size (e.g. 8G)\n"
        "  --table-name <name>       Table name for ub_client\n"
        "  --table-offset <bytes>    Table offset in mapping\n"
        "  --table-size <bytes>      Table byte size\n"
        "\n"
        "Vector config:\n"
        "  --vector-dimension <n>    Dim per embedding (default: 300)\n"
        "  --vector-stride-bytes <n> Row stride\n"
        "\n"
        "Benchmark config:\n"
        "  --batch-sizes <list>      Comma-separated batch sizes (default: 8,16,64,256,1024)\n"
        "  --warmup <n>              Warmup iterations (default: 50)\n"
        "  --iters <n>               Measurement iterations (default: 200)\n"
        "  --cacheable yes|no        Cacheable mapping\n"
        "  --use-ownership yes|no    Read ownership\n"
        "  --mock-local              Use local malloc instead of UB.MEM\n"
        "  --fill-rows <n>           Rows to pre-fill with test data\n"
        "  --verify                  Verify results against fixture pattern\n"
        "  --verbose                 Verbose output\n"
        "  --csv <file>              Append results to CSV file\n"
        "  --threads <n>             Multi-threaded contention test (default: 1)\n",
        prog);
}

int main(int argc, char **argv)
{
    bench_opts_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.vector_dimension = 300;
    opts.batch_sizes_str = "8,16,64,256,1024";
    opts.warmup = 50;
    opts.iters = 200;
    opts.table_name = "ut_vectors";

    static struct option long_opts[] = {
        {"shm-memid",          required_argument, NULL, 'M'},
        {"shm-size",           required_argument, NULL, 'S'},
        {"table-name",         required_argument, NULL, 'T'},
        {"table-offset",       required_argument, NULL, 'O'},
        {"table-size",         required_argument, NULL, 'Z'},
        {"vector-dimension",   required_argument, NULL, 'd'},
        {"vector-stride-bytes",required_argument, NULL, 'r'},
        {"batch-sizes",        required_argument, NULL, 'b'},
        {"warmup",             required_argument, NULL, 'w'},
        {"iters",              required_argument, NULL, 'i'},
        {"cacheable",          required_argument, NULL, 'c'},
        {"use-ownership",      required_argument, NULL, 'o'},
        {"mock-local",         no_argument,       NULL, 'l'},
        {"fill-rows",          required_argument, NULL, 'f'},
        {"verify",             no_argument,       NULL, 'v'},
        {"verbose",            no_argument,       NULL, 'V'},
        {"csv",                required_argument, NULL, 'C'},
        {"threads",            required_argument, NULL, 't'},
        {"help",               no_argument,       NULL, 'h'},
        {NULL, 0, NULL, 0}
    };

    const char *csv_path = NULL;
    int ch;
    while ((ch = getopt_long(argc, argv, "h", long_opts, NULL)) != -1) {
        switch (ch) {
        case 'M': opts.shm_memid = strtoull(optarg, NULL, 0); break;
        case 'S': opts.shm_size = parse_size(optarg); break;
        case 'T': opts.table_name = optarg; break;
        case 'O': opts.table_offset = parse_size(optarg); break;
        case 'Z': opts.table_size = parse_size(optarg); break;
        case 'd': opts.vector_dimension = (size_t)strtoul(optarg, NULL, 0); break;
        case 'r': opts.vector_stride_bytes = parse_size(optarg); break;
        case 'b': opts.batch_sizes_str = optarg; break;
        case 'w': opts.warmup = atoi(optarg); break;
        case 'i': opts.iters = atoi(optarg); break;
        case 'c': opts.cacheable = (strcmp(optarg, "yes") == 0); break;
        case 'o': opts.use_ownership = (strcmp(optarg, "yes") == 0); break;
        case 'l': opts.mock_local = 1; break;
        case 'f': opts.fill_rows = (size_t)strtoul(optarg, NULL, 0); break;
        case 'v': opts.verify = 1; break;
        case 'V': opts.verbose = 1; break;
        case 'C': csv_path = optarg; break;
        case 't': opts.threads = atoi(optarg); break;
        case 'h': usage(argv[0]); return 0;
        default: usage(argv[0]); return 1;
        }
    }

    /* Parse batch sizes */
    size_t batch_sizes[64];
    size_t num_batch_sizes = 0;
    {
        char buf[512];
        strncpy(buf, opts.batch_sizes_str, sizeof(buf) - 1);
        buf[sizeof(buf) - 1] = '\0';
        char *tok = strtok(buf, ",");
        while (tok && num_batch_sizes < 64) {
            batch_sizes[num_batch_sizes++] = (size_t)strtoul(tok, NULL, 0);
            tok = strtok(NULL, ",");
        }
    }

    srand(42);

    const size_t dim = opts.vector_dimension;
    const size_t stride = effective_stride(&opts);
    const size_t max_rows = (opts.fill_rows > 0) ? opts.fill_rows : 100000;

    fprintf(stderr, "=== SVE Gather UB Benchmark ===\n");
    fprintf(stderr, "  dim=%zu  stride=%zu  source=%s\n",
            dim, stride, opts.mock_local ? "local malloc" : "UB.MEM");
#ifdef USE_ARM_SVE
    fprintf(stderr, "  SVE enabled\n");
#else
    fprintf(stderr, "  SVE disabled (scalar)\n");
#endif
    fprintf(stderr, "  warmup=%d  iters=%d  verify=%d\n",
            opts.warmup, opts.iters, opts.verify);
    fprintf(stderr, "  batch_sizes: %s\n\n", opts.batch_sizes_str);

    /* ---- Setup UB or mock address space ---- */
    ub_address_space_t *addr_space = NULL;
    ub_address_space_t mock_as;
    unsigned char *mock_table = NULL;
    ub_mem_config_t cfg;
    state_bitmap_t bitmap;
    sve_operation_stats_t stats;
    sve_gather_ctx_t ctx;

    memset(&cfg, 0, sizeof(cfg));
    memset(&mock_as, 0, sizeof(mock_as));
    memset(&bitmap, 0, sizeof(bitmap));
    memset(&stats, 0, sizeof(stats));
    memset(&ctx, 0, sizeof(ctx));

    if (opts.mock_local) {
        size_t table_bytes = max_rows * stride;
        mock_table = calloc(1, table_bytes);
        if (!mock_table) {
            fprintf(stderr, "OOM: mock table\n");
            return 1;
        }
        fill_fixture_vectors((float *)mock_table, max_rows, dim, stride);

        mock_as.mapped_addr         = mock_table;
        mock_as.mapping_addr        = mock_table;
        mock_as.mapping_size        = table_bytes;
        mock_as.size                = table_bytes;
        mock_as.data_offset         = 0;
        mock_as.vector_stride_bytes = stride;
        mock_as.shm_fd              = -1;
        addr_space = &mock_as;

        /* Still need ub_client_init for ub_client_perform_gather_load */
        cfg.vector_dimension  = (int)dim;
        cfg.vector_stride_bytes = stride;
        cfg.shm_size = table_bytes;
        cfg.table_size = table_bytes;
        cfg.table_name = (char *)opts.table_name;
        if (ub_client_init(&cfg) != 0) {
            fprintf(stderr, "ub_client_init failed (mock)\n");
            free(mock_table);
            return 1;
        }
    } else {
        if (opts.shm_memid == 0 && !opts.mock_local) {
            fprintf(stderr, "ERROR: --shm-memid required for real UB.MEM "
                    "(or use --mock-local)\n");
            return 1;
        }

        cfg.vector_dimension    = (int)dim;
        cfg.cacheable           = opts.cacheable;
        cfg.use_ownership       = opts.use_ownership;
        cfg.shm_memid           = opts.shm_memid;
        cfg.shm_size            = opts.shm_size;
        cfg.table_offset        = opts.table_offset;
        cfg.table_size          = opts.table_size;
        cfg.vector_stride_bytes = stride;
        cfg.table_name          = (char *)opts.table_name;

        if (ub_client_init(&cfg) != 0) {
            fprintf(stderr, "ub_client_init failed\n");
            return 1;
        }
        if (ub_client_load_embedding_table(opts.table_name, &addr_space) != 0) {
            fprintf(stderr, "ub_client_load_embedding_table failed\n");
            ub_client_cleanup();
            return 1;
        }
    }

    /* Init bitmap large enough for max_rows (no contention in bench) */
    if (bitmap_init(&bitmap, max_rows) != 0) {
        fprintf(stderr, "bitmap_init failed\n");
        if (opts.mock_local) free(mock_table);
        ub_client_cleanup();
        return 1;
    }

    sve_gather_ctx_init(&ctx, addr_space, &bitmap, dim, stride,
                        (uint64_t)max_rows, &stats);

    /* ---- CSV output ---- */
    FILE *csv_file = NULL;
    if (csv_path) {
        csv_file = fopen(csv_path, "a");
        if (!csv_file) {
            fprintf(stderr, "Cannot open CSV: %s\n", csv_path);
        } else {
            print_csv_header(csv_file);
        }
    }

    /* ---- Print table header ---- */
    fprintf(stderr, "%-14s %8s %12s %12s %12s %10s %12s %8s\n",
            "mode", "batch", "avg_ns", "min_ns", "max_ns",
            "ns/emb", "MB/s", "errors");
    fprintf(stderr, "%s\n",
            "----------------------------------------------------------------------"
            "-------------------");

    /* ---- Run benchmarks ---- */
    for (size_t bi = 0; bi < num_batch_sizes; bi++) {
        size_t batch = batch_sizes[bi];
        if (batch > max_rows) {
            fprintf(stderr, "batch=%zu > max_rows=%zu, skipping\n",
                    batch, max_rows);
            continue;
        }

        uint64_t *ids = generate_random_ids(batch, max_rows);
        float *results = alloc_results(batch, dim);
        if (!ids || !results) {
            fprintf(stderr, "OOM batch=%zu\n", batch);
            free(ids);
            free(results);
            continue;
        }

        /* Warmup */
        for (int w = 0; w < opts.warmup; w++) {
            sve_serial_contiguous_read(&ctx, ids, batch, results);
        }

        /* Measure each mode */
        bench_result_t results_arr[BENCH_MODE_COUNT];

        for (int m = 0; m < BENCH_MODE_COUNT; m++) {
            run_bench_mode((bench_mode_t)m, &ctx,
                           ids, batch, dim, results,
                           opts.iters, &results_arr[m]);

            if (opts.verify) {
                memset(results, 0, batch * dim * sizeof(float));
                switch ((bench_mode_t)m) {
                case BENCH_MODE_SERIAL:
                    sve_serial_contiguous_read(&ctx, ids, batch, results);
                    break;
                case BENCH_MODE_CROSS_GATHER:
                    sve_cross_emb_gather_read(&ctx, ids, batch, results);
                    break;
                case BENCH_MODE_MEMCPY:
                    run_memcpy_baseline(&ctx, ids, batch, dim, results);
                    break;
                default:
                    break;
                }
                results_arr[m].verify_errors =
                    verify_results(results, ids, batch, dim);
            }

            fprintf(stderr, "%-14s %8zu %12.1f %12.1f %12.1f %10.1f %12.1f %8d\n",
                    mode_names[m], batch,
                    results_arr[m].avg_ns, results_arr[m].min_ns,
                    results_arr[m].max_ns, results_arr[m].per_emb_ns,
                    results_arr[m].throughput_mbs,
                    results_arr[m].verify_errors);

            if (csv_file) {
                print_csv_row(csv_file, mode_names[m], batch,
                              &results_arr[m]);
            }
        }

        /* Speedup vs serial baseline */
        if (results_arr[BENCH_MODE_SERIAL].avg_ns > 0) {
            double speedup = results_arr[BENCH_MODE_SERIAL].avg_ns /
                             results_arr[BENCH_MODE_CROSS_GATHER].avg_ns;
            fprintf(stderr, "  -> cross-gather vs serial speedup: %.3fx\n\n",
                    speedup);
        }

        free(ids);
        free(results);
    }

    /* ---- Multi-threaded contention benchmark ---- */
    if (opts.threads > 1) {
        int nt = opts.threads;
        fprintf(stderr, "\n=== Multi-threaded contention test (%d threads) ===\n", nt);
        fprintf(stderr, "%-14s %8s %12s %12s %12s %10s %10s %10s\n",
                "mode", "batch", "avg_ns/call", "ns/emb",
                "MB/s", "ops/ops", "lock_ok", "lock_fail");
        fprintf(stderr, "%s\n",
                "----------------------------------------------------------------------"
                "-------------------");

        size_t mt_batches[] = {8, 64, 256, 1024};
        size_t num_mt = sizeof(mt_batches) / sizeof(mt_batches[0]);

        for (size_t bi = 0; bi < num_mt; bi++) {
            size_t batch = mt_batches[bi];
            if (batch > max_rows) continue;

            for (int m = 0; m < BENCH_MODE_COUNT; m++) {
                atomic_store_explicit(&stats.lock_success, 0,
                                      memory_order_relaxed);
                atomic_store_explicit(&stats.lock_failure, 0,
                                      memory_order_relaxed);

                pthread_t *threads = calloc(nt, sizeof(pthread_t));
                thread_arg_t *args = calloc(nt, sizeof(thread_arg_t));

                for (int t = 0; t < nt; t++) {
                    args[t].mode = (bench_mode_t)m;
                    args[t].ctx = &ctx;
                    args[t].dim = dim;
                    args[t].max_rows = max_rows;
                    args[t].batch = batch;
                    args[t].iters = opts.iters;
                    args[t].total_ns = 0;
                    args[t].lock_success = 0;
                    args[t].lock_failure = 0;
                    pthread_create(&threads[t], NULL, thread_worker, &args[t]);
                }

                double sum_ns = 0;
                for (int t = 0; t < nt; t++) {
                    pthread_join(threads[t], NULL);
                    sum_ns += args[t].total_ns;
                }

                double avg_call_ns = sum_ns / (double)(nt * opts.iters);
                double per_emb_ns = avg_call_ns / (double)batch;
                size_t total_bytes = batch * dim * sizeof(float);
                double mbs = (avg_call_ns > 0)
                    ? (double)total_bytes / (avg_call_ns / 1e3) : 0;
                uint64_t total_ops = (uint64_t)nt * opts.iters * batch;

                fprintf(stderr,
                        "%-14s %8zu %12.1f %10.1f %12.1f %10zu %10llu %10llu\n",
                        mode_names[m], batch, avg_call_ns, per_emb_ns, mbs,
                        total_ops,
                        (unsigned long long)atomic_load_explicit(
                            &stats.lock_success, memory_order_relaxed),
                        (unsigned long long)atomic_load_explicit(
                            &stats.lock_failure, memory_order_relaxed));

                free(threads);
                free(args);
            }

            fprintf(stderr, "\n");
        }
    }

    /* ---- Cleanup ---- */
    if (csv_file) fclose(csv_file);
    bitmap_destroy(&bitmap);
    ub_client_cleanup();
    if (mock_table) free(mock_table);

    fprintf(stderr, "=== Done ===\n");
    return 0;
}
