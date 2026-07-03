/*
 * Direct f32 copy benchmark for SVE load variants.
 *
 * No UB client, no bitmap, no sve_gather_ctx_t. The test allocates local rows
 * and compares row-wise copies with a vertical column gather across rows.
 */

#include "../src/sve_operation.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef enum {
    COPY_STREAMING = 0,
    COPY_GATTHER,
    COPY_GATHER_SCATTER,
    COPY_COLUMN_GATHER,
    COPY_CASE_COUNT
} copy_case_id_t;

typedef struct {
    copy_case_id_t id;
    const char *name;
} copy_case_t;

static volatile float g_sink;

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static size_t round_up(size_t value, size_t align) {
    return ((value + align - 1u) / align) * align;
}

static int parse_size(const char *text, size_t *out) {
    char *end = NULL;
    errno = 0;
    unsigned long long base = strtoull(text, &end, 0);
    if (errno || end == text) return -1;

    switch (*end) {
    case '\0': *out = (size_t)base; return 0;
    case 'k': case 'K': *out = (size_t)(base * 1024ULL); return 0;
    case 'm': case 'M': *out = (size_t)(base * 1024ULL * 1024ULL); return 0;
    case 'g': case 'G': *out = (size_t)(base * 1024ULL * 1024ULL * 1024ULL); return 0;
    default: return -1;
    }
}

static size_t parse_sizes(const char *text, size_t *sizes, size_t cap) {
    char buf[512];
    strncpy(buf, text, sizeof(buf) - 1u);
    buf[sizeof(buf) - 1u] = '\0';

    size_t count = 0;
    char *tok = strtok(buf, ",");
    while (tok && count < cap) {
        size_t size = 0;
        if (parse_size(tok, &size) == 0 && size > 0) {
            sizes[count++] = size;
        }
        tok = strtok(NULL, ",");
    }
    return count;
}

static float value_for(size_t row, size_t col) {
    return (float)((row + 1u) * 1000003u + (col % 1009u));
}

static void fill_rows(float **rows, size_t num_rows, size_t floats_per_row) {
    for (size_t row = 0; row < num_rows; row++) {
        for (size_t col = 0; col < floats_per_row; col++) {
            rows[row][col] = value_for(row, col);
        }
    }
}

static void clear_rows(float **rows, size_t num_rows, size_t alloc_floats) {
    for (size_t row = 0; row < num_rows; row++) {
        for (size_t col = 0; col < alloc_floats; col++) {
            rows[row][col] = -12345.0f;
        }
    }
}

static int verify_rows(float **src_rows, float **dst_rows,
                       size_t num_rows, size_t floats_per_row,
                       size_t alloc_floats) {
    for (size_t row = 0; row < num_rows; row++) {
        for (size_t col = 0; col < floats_per_row; col++) {
            if (src_rows[row][col] != dst_rows[row][col]) {
                fprintf(stderr,
                        "mismatch row=%zu col=%zu: got %.1f expected %.1f\n",
                        row, col, dst_rows[row][col], src_rows[row][col]);
                return -1;
            }
        }
        for (size_t col = floats_per_row; col < alloc_floats; col++) {
            if (dst_rows[row][col] != -12345.0f) {
                fprintf(stderr, "tail dirty row=%zu col=%zu got %.1f\n",
                        row, col, dst_rows[row][col]);
                return -1;
            }
        }
    }
    return 0;
}

static float checksum_rows(float **rows, size_t num_rows, size_t floats_per_row) {
    float sum = 0.0f;
    for (size_t row = 0; row < num_rows; row++) {
        for (size_t col = 0; col < floats_per_row; col += 17u) {
            sum += rows[row][col];
        }
    }
    return sum;
}

static void run_case(copy_case_id_t id, float **src_rows, float **dst_rows,
                     const float **src_ptrs, float **dst_ptrs,
                     size_t rows, size_t floats_per_row, size_t row_bytes) {
    switch (id) {
    case COPY_STREAMING:
        for (size_t row = 0; row < rows; row++)
            sve_streaming_load_f32(src_rows[row], dst_rows[row], row_bytes);
        break;
    case COPY_GATTHER:
        for (size_t row = 0; row < rows; row++)
            sve_gatther_load_f32(src_rows[row], dst_rows[row], row_bytes);
        break;
    case COPY_GATHER_SCATTER:
        for (size_t row = 0; row < rows; row++)
            sve_gather_scatter_load_f32(src_rows[row], dst_rows[row], row_bytes);
        break;
    case COPY_COLUMN_GATHER:
        sve_column_gather_load_f32(src_ptrs, dst_ptrs, rows, floats_per_row);
        break;
    default:
        break;
    }
}

static int alloc_row_ptrs(float ***rows_out, size_t rows, size_t alloc_bytes) {
    float **rows_ptr = calloc(rows, sizeof(*rows_ptr));
    if (!rows_ptr) return -1;

    for (size_t row = 0; row < rows; row++) {
        rows_ptr[row] = aligned_alloc(64, alloc_bytes);
        if (!rows_ptr[row]) {
            for (size_t i = 0; i < row; i++) free(rows_ptr[i]);
            free(rows_ptr);
            return -1;
        }
    }

    *rows_out = rows_ptr;
    return 0;
}

static void free_row_ptrs(float **rows, size_t num_rows) {
    if (!rows) return;
    for (size_t row = 0; row < num_rows; row++) free(rows[row]);
    free(rows);
}

static int run_one_size(size_t row_bytes, size_t rows, int warmup, int iters) {
    const size_t floats_per_row = row_bytes / sizeof(float);
    const size_t alloc_bytes = round_up(row_bytes + 64u, 64u);
    const size_t alloc_floats = alloc_bytes / sizeof(float);
    float **src_rows = NULL;
    float **dst_rows = NULL;
    if (alloc_row_ptrs(&src_rows, rows, alloc_bytes) != 0 ||
        alloc_row_ptrs(&dst_rows, rows, alloc_bytes) != 0) {
        fprintf(stderr, "allocation failed rows=%zu row_bytes=%zu\n", rows, row_bytes);
        free_row_ptrs(src_rows, rows);
        free_row_ptrs(dst_rows, rows);
        return -1;
    }

    const float **src_ptrs = calloc(rows, sizeof(*src_ptrs));
    float **dst_ptrs = calloc(rows, sizeof(*dst_ptrs));
    if (!src_ptrs || !dst_ptrs) {
        free(src_ptrs);
        free(dst_ptrs);
        free_row_ptrs(src_rows, rows);
        free_row_ptrs(dst_rows, rows);
        return -1;
    }

    for (size_t row = 0; row < rows; row++) {
        src_ptrs[row] = src_rows[row];
        dst_ptrs[row] = dst_rows[row];
    }

    copy_case_t cases[] = {
        {COPY_STREAMING, "streaming"},
        {COPY_GATTHER, "gatther"},
        {COPY_GATHER_SCATTER, "gather_scatter"},
        {COPY_COLUMN_GATHER, "column_gather"},
    };

    fill_rows(src_rows, rows, floats_per_row);

    for (size_t c = 0; c < COPY_CASE_COUNT; c++) {
        clear_rows(dst_rows, rows, alloc_floats);
        run_case(cases[c].id, src_rows, dst_rows, src_ptrs, dst_ptrs,
                 rows, floats_per_row, row_bytes);
        if (verify_rows(src_rows, dst_rows, rows, floats_per_row, alloc_floats) != 0) {
            free(src_ptrs);
            free(dst_ptrs);
            free_row_ptrs(src_rows, rows);
            free_row_ptrs(dst_rows, rows);
            return -1;
        }
    }

    for (size_t c = 0; c < COPY_CASE_COUNT; c++) {
        for (int i = 0; i < warmup; i++) {
            run_case(cases[c].id, src_rows, dst_rows, src_ptrs, dst_ptrs,
                     rows, floats_per_row, row_bytes);
        }

        uint64_t start = now_ns();
        for (int i = 0; i < iters; i++) {
            run_case(cases[c].id, src_rows, dst_rows, src_ptrs, dst_ptrs,
                     rows, floats_per_row, row_bytes);
        }
        uint64_t elapsed = now_ns() - start;
        g_sink += checksum_rows(dst_rows, rows, floats_per_row);

        double avg_ns = (double)elapsed / (double)iters;
        double elapsed_ms = (double)elapsed / 1000000.0;
        double bytes = (double)rows * (double)row_bytes;
        double bytes_per_ns = bytes / avg_ns;
        printf("%-15s %8zu %10zu %12.2f %12.3f %12.2f %12.2f\n",
               cases[c].name, rows, row_bytes, avg_ns, elapsed_ms,
               bytes_per_ns, bytes_per_ns * 1000.0);
    }

    free(src_ptrs);
    free(dst_ptrs);
    free_row_ptrs(src_rows, rows);
    free_row_ptrs(dst_rows, rows);
    return 0;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [--rows n] [--sizes bytes[,bytes...]] [--warmup n] [--iters n]\n"
            "Example: %s --rows 64 --sizes 1200,4K,64K --warmup 100 --iters 1000\n",
            prog, prog);
}

int main(int argc, char **argv) {
    size_t rows = 64;
    const char *sizes_arg = "1200,4K,64K";
    int warmup = 100;
    int iters = 1000;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--rows") == 0 && i + 1 < argc) {
            rows = (size_t)strtoull(argv[++i], NULL, 0);
        } else if (strcmp(argv[i], "--sizes") == 0 && i + 1 < argc) {
            sizes_arg = argv[++i];
        } else if (strcmp(argv[i], "--warmup") == 0 && i + 1 < argc) {
            warmup = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--iters") == 0 && i + 1 < argc) {
            iters = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return 0;
        } else {
            usage(argv[0]);
            return 1;
        }
    }

    if (rows == 0 || warmup < 0 || iters <= 0) {
        usage(argv[0]);
        return 1;
    }

    size_t sizes[64];
    size_t num_sizes = parse_sizes(sizes_arg, sizes, sizeof(sizes) / sizeof(sizes[0]));
    if (num_sizes == 0) {
        fprintf(stderr, "no valid sizes parsed from '%s'\n", sizes_arg);
        return 1;
    }

#ifdef USE_ARM_SVE
    fprintf(stderr, "SVE enabled, f32_lanes=%zu, u64base_lanes=%zu\n",
            (size_t)svcntw(), (size_t)svcntd());
#else
    fprintf(stderr, "SVE disabled, scalar fallback paths are used\n");
#endif
    fprintf(stderr, "rows=%zu warmup=%d iters=%d sizes=%s\n",
            rows, warmup, iters, sizes_arg);
    printf("%-15s %8s %10s %12s %12s %12s %12s\n",
           "case", "rows", "row_bytes", "avg_ns", "elapsed_ms", "bytes/ns", "MB/s");

    for (size_t i = 0; i < num_sizes; i++) {
        size_t row_bytes = sizes[i] & ~(sizeof(float) - 1u);
        if (row_bytes == 0) continue;
        if (run_one_size(row_bytes, rows, warmup, iters) != 0) return 1;
    }

    return g_sink == 0.12345f ? 1 : 0;
}
