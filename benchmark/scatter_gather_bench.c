/*
 * scatter_gather_bench.c
 *
 * Benchmark: per-embedding serial read vs cross-embedding SVE gather read.
 * Uses the standalone sve_operation module (no server.h dependency).
 *
 * Build:
 *   cd benchmark && make scatter_gather_bench
 */
#define _GNU_SOURCE
#include "../src/sve_operation.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

/* ---- Timing ---- */
static inline uint64_t bench_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* ---- Baseline: per-embedding serial read ---- */
static int baseline_serial_read(sve_ub_mem_t *mem,
                                state_bitmap_t *bmp,
                                sve_counters_t *stats,
                                uint64_t *emb_ids,
                                size_t num_ids,
                                float *results,
                                uint8_t *valid_mask)
{
    (void)stats;
    const size_t dim = SVE_EMBEDDING_DIM;
    for (size_t i = 0; i < num_ids; i++) {
        if (state_bitmap_try_acquire(bmp, emb_ids[i]) != 0) {
            valid_mask[i] = 0;
            memset(&results[i * dim], 0, dim * sizeof(float));
            continue;
        }
        embedding_entry_t *emb = (embedding_entry_t *)
            ((uint8_t *)mem->base_addr + emb_ids[i] * sizeof(embedding_entry_t));
        if ((uint8_t *)emb + sizeof(embedding_entry_t) <= (uint8_t *)mem->base_addr + mem->size) {
            memcpy(&results[i * dim], emb->data, dim * sizeof(float));
            valid_mask[i] = 1;
        } else {
            valid_mask[i] = 0;
            memset(&results[i * dim], 0, dim * sizeof(float));
        }
        bitmap_release(bmp, emb_ids[i]);
    }
    return 0;
}

/* ---- Harness ---- */
typedef int (*read_fn_t)(sve_ub_mem_t *, state_bitmap_t *, sve_counters_t *,
                         uint64_t *, size_t, float *, uint8_t *);

typedef struct {
    const char *label;
    read_fn_t fn;
    double total_ns, best_ns, worst_ns;
} bench_result_t;

static void run_bench(bench_result_t *r, sve_ub_mem_t *mem, state_bitmap_t *bmp,
                      sve_counters_t *stats, uint64_t *ids, size_t n,
                      float *res, uint8_t *mask, int warmup, int iters)
{
    r->total_ns = 0; r->best_ns = 1e18; r->worst_ns = 0;
    for (int i = 0; i < warmup; i++) r->fn(mem, bmp, stats, ids, n, res, mask);
    for (int i = 0; i < iters; i++) {
        uint64_t t0 = bench_now_ns();
        r->fn(mem, bmp, stats, ids, n, res, mask);
        uint64_t t1 = bench_now_ns();
        double e = (double)(t1 - t0);
        r->total_ns += e;
        if (e < r->best_ns) r->best_ns = e;
        if (e > r->worst_ns) r->worst_ns = e;
    }
}

static void print_result(bench_result_t *r, int iters, size_t n) {
    double avg = r->total_ns / iters;
    printf("  %-35s  avg %8.0f ns  best %8.0f ns  worst %8.0f ns  "
           "per-emb %6.1f ns  %.2f M/s\n",
           r->label, avg, r->best_ns, r->worst_ns,
           avg / n, (double)n / (avg / 1e9) / 1e6);
}

int main(int argc, char *argv[]) {
    size_t num_emb = 100000;
    int warmup = 50, iters = 200;
    int batch_sizes[] = {8, 16, 32, 64, 128, 256, 512, 1024, 3000};
    int nbatch = sizeof(batch_sizes) / sizeof(batch_sizes[0]);

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--embeddings") && i+1 < argc) num_emb = (size_t)atol(argv[++i]);
        else if (!strcmp(argv[i], "--iters") && i+1 < argc) iters = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--warmup") && i+1 < argc) warmup = atoi(argv[++i]);
    }

    printf("╔═══════════════════════════════════════════════════════════════╗\n");
    printf("║  Scatter/Gather vs Baseline Benchmark                        ║\n");
    printf("║  dim=%d  table=%zu entries (%.1f MB)                        ║\n",
           SVE_EMBEDDING_DIM, num_emb,
           (double)(num_emb * sizeof(embedding_entry_t)) / (1024*1024));
#ifdef USE_ARM_SVE
    printf("║  SVE: ENABLED (%d floats/vec)                                ║\n", SVE_OP_VL);
#else
    printf("║  SVE: DISABLED (scalar)                                      ║\n");
#endif
    printf("╚═══════════════════════════════════════════════════════════════╝\n\n");

    /* Setup */
    sve_ub_mem_t mem;
    memset(&mem, 0, sizeof(mem));
    mem.size = num_emb * sizeof(embedding_entry_t);
    mem.base_addr = calloc(num_emb, sizeof(embedding_entry_t));
    if (!mem.base_addr) { fprintf(stderr, "OOM\n"); return 1; }

    embedding_entry_t *table = (embedding_entry_t *)mem.base_addr;
    for (size_t i = 0; i < num_emb; i++)
        for (int d = 0; d < SVE_EMBEDDING_DIM; d++)
            table[i].data[d] = (float)(i * 1000 + d);

    state_bitmap_t bmp;
    if (bitmap_init(&bmp, num_emb) != 0) { fprintf(stderr, "bitmap init failed\n"); return 1; }

    sve_counters_t stats;
    sve_counters_init(&stats);

    unsigned seed = 42;

    printf("  %-35s  %8s  %8s  %8s  %6s  %s\n",
           "Method", "avg", "best", "worst", "per-emb", "throughput");
    printf("  -----------------------------------  --------  --------  --------  ------  ----------\n");

    for (int bi = 0; bi < nbatch; bi++) {
        size_t batch = (size_t)batch_sizes[bi];
        if (batch > num_emb) continue;

        printf("\n  --- Batch size: %zu ---\n", batch);

        /* Unique random IDs (Fisher-Yates partial shuffle) */
        uint64_t *ids = malloc(batch * sizeof(uint64_t));
        uint64_t *pool = malloc(num_emb * sizeof(uint64_t));
        for (size_t i = 0; i < num_emb; i++) pool[i] = i;
        for (size_t i = 0; i < batch; i++) {
            size_t j = i + (rand_r(&seed) % (num_emb - i));
            uint64_t tmp = pool[i]; pool[i] = pool[j]; pool[j] = tmp;
            ids[i] = pool[i];
        }
        free(pool);

        float *res_a = calloc(batch * SVE_EMBEDDING_DIM, sizeof(float));
        float *res_b = calloc(batch * SVE_EMBEDDING_DIM, sizeof(float));
        uint8_t *mask_a = calloc(batch, 1);
        uint8_t *mask_b = calloc(batch, 1);

        bench_result_t bl = { .label = "Baseline (per-emb serial)", .fn = baseline_serial_read };
        run_bench(&bl, &mem, &bmp, &stats, ids, batch, res_a, mask_a, warmup, iters);
        print_result(&bl, iters, batch);

        bench_result_t sg = { .label = "Scatter/Gather (cross-emb)", .fn = sve_gather_read };
        run_bench(&sg, &mem, &bmp, &stats, ids, batch, res_b, mask_b, warmup, iters);
        print_result(&sg, iters, batch);

        /* Correctness */
        int ok = 1;
        for (size_t i = 0; i < batch && ok; i++) {
            if (mask_a[i] != mask_b[i]) { ok = 0; break; }
            if (mask_a[i] && memcmp(&res_a[i * SVE_EMBEDDING_DIM],
                                    &res_b[i * SVE_EMBEDDING_DIM],
                                    SVE_EMBEDDING_DIM * sizeof(float)) != 0) ok = 0;
        }
        printf("  → Speedup: %.2fx  Correctness: %s\n",
               bl.total_ns / sg.total_ns, ok ? "OK ✓" : "MISMATCH ✗");

        free(ids); free(res_a); free(res_b); free(mask_a); free(mask_b);
    }

    printf("\n  Gather counters: ops=%llu  elements=%llu",
           (unsigned long long)atomic_load(&stats.gather_ops),
           (unsigned long long)atomic_load(&stats.gather_elements));
    if (atomic_load(&stats.gather_ops) > 0)
        printf("  lane-util=%.1f%%",
               100.0 * (double)atomic_load(&stats.gather_elements) /
               ((double)atomic_load(&stats.gather_ops) * SVE_OP_VL));
    printf("\n\n✅ Benchmark complete.\n");

    bitmap_destroy(&bmp);
    free(mem.base_addr);
    return 0;
}
