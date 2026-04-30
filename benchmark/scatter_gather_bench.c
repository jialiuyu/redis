/*
 * scatter_gather_bench.c
 *
 * Benchmark comparing two SuperNode embedding read strategies:
 *
 *   1. BASELINE (sve2_gather_with_bitmap_check):
 *      Per-embedding serial: acquire bitmap → contiguous SVE load 300 floats
 *      → release bitmap.  One embedding at a time.
 *
 *   2. SCATTER/GATHER (sve2_scatter_gather_read):
 *      Cross-embedding parallel: group 8 emb_ids, batch bitmap acquire →
 *      for each of 300 dims, one SVE gather loads that dim from all 8
 *      embeddings → batch bitmap release.
 *
 * Build (Kunpeng 920, in-tree):
 *   cd src && make scatter_gather_bench
 *   or:
 *   gcc -O3 -march=armv8.2-a+sve -pthread -std=gnu11 \
 *       -I../deps/lua/src -I../deps/hiredis -I../deps/linenoise \
 *       -I../deps/hdr_histogram -I../deps/fpconv -I../deps/xxhash \
 *       -o scatter_gather_bench scatter_gather_bench.c \
 *       supernode_worker.c proxy_aggregator.c server.c ... -lm -lpthread
 *
 * Simpler: this file is self-contained with a mock UB region.
 * It includes supernode_worker.h (which pulls server.h) for real types,
 * but implements its own main() and mock setup.
 */
#define _GNU_SOURCE
#include "../src/supernode_worker.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>

/* ============================================================
 * Timing
 * ============================================================ */
static inline uint64_t bench_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* ============================================================
 * Mock UB memory setup (no real OBMM device needed)
 * ============================================================ */

static ub_memory_space_t *mock_ub_mem_create(size_t num_embeddings) {
    ub_memory_space_t *ub = zmalloc(sizeof(*ub));
    if (!ub) return NULL;

    size_t total = num_embeddings * sizeof(embedding_entry_t);
    ub->base_addr = zcalloc(total);
    if (!ub->base_addr) { zfree(ub); return NULL; }

    ub->size = total;
    ub->physical_base = 0;
    ub->token_id = 0;
    ub->numa_node = 0;

    /* Fill with deterministic fixture data */
    embedding_entry_t *table = (embedding_entry_t *)ub->base_addr;
    for (size_t i = 0; i < num_embeddings; i++) {
        for (int d = 0; d < SUPERNODE_EMBEDDING_DIM; d++) {
            table[i].data[d] = (float)(i * 1000 + d);
        }
    }
    return ub;
}

static void mock_ub_mem_destroy(ub_memory_space_t *ub) {
    if (!ub) return;
    zfree(ub->base_addr);
    zfree(ub);
}

/* ============================================================
 * Benchmark harness
 * ============================================================ */

typedef int (*gather_fn_t)(sve_worker_context_t *, uint64_t *, size_t,
                           float *, uint8_t *);

typedef struct {
    const char *label;
    gather_fn_t fn;
    double total_ns;
    double best_ns;
    double worst_ns;
} bench_result_t;

static void run_bench(bench_result_t *r,
                      sve_worker_context_t *ctx,
                      uint64_t *ids, size_t num_ids,
                      float *results, uint8_t *valid_mask,
                      int warmup_iters, int bench_iters)
{
    r->total_ns = 0;
    r->best_ns = 1e18;
    r->worst_ns = 0;

    for (int i = 0; i < warmup_iters; i++)
        r->fn(ctx, ids, num_ids, results, valid_mask);

    for (int i = 0; i < bench_iters; i++) {
        uint64_t t0 = bench_now_ns();
        r->fn(ctx, ids, num_ids, results, valid_mask);
        uint64_t t1 = bench_now_ns();
        double elapsed = (double)(t1 - t0);
        r->total_ns += elapsed;
        if (elapsed < r->best_ns) r->best_ns = elapsed;
        if (elapsed > r->worst_ns) r->worst_ns = elapsed;
    }
}

static void print_result(bench_result_t *r, int iters, size_t num_ids) {
    double avg = r->total_ns / iters;
    double per_emb = avg / num_ids;
    double tput = (double)num_ids / (avg / 1e9);

    printf("  %-40s  avg %8.0f ns  best %8.0f ns  worst %8.0f ns  "
           "per-emb %6.1f ns  %.2f M emb/s\n",
           r->label, avg, r->best_ns, r->worst_ns, per_emb, tput / 1e6);
}

/* ============================================================
 * Main
 * ============================================================ */

int main(int argc, char *argv[]) {
    size_t num_embeddings = 100000;
    int batch_sizes[] = {8, 16, 32, 64, 128, 256, 512, 1024, 3000};
    int num_batch_sizes = sizeof(batch_sizes) / sizeof(batch_sizes[0]);
    int warmup = 50;
    int iters = 200;

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--embeddings") && i+1 < argc)
            num_embeddings = (size_t)atol(argv[++i]);
        else if (!strcmp(argv[i], "--iters") && i+1 < argc)
            iters = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--warmup") && i+1 < argc)
            warmup = atoi(argv[++i]);
    }

    printf("╔═══════════════════════════════════════════════════════════════════════╗\n");
    printf("║  SuperNode Scatter/Gather vs Baseline Benchmark                      ║\n");
    printf("║  Embedding dim: %d   Table: %zu entries (%.1f MB)%*s║\n",
           SUPERNODE_EMBEDDING_DIM, num_embeddings,
           (double)(num_embeddings * sizeof(embedding_entry_t)) / (1024*1024),
           10, "");
    printf("║  Warmup: %d   Iterations: %d%*s║\n", warmup, iters, 40, "");
#ifdef USE_ARM_SVE
    printf("║  SVE: ENABLED (256-bit, %d floats/vec)%*s║\n",
           SVE_ELEMENTS_PER_VECTOR, 28, "");
#else
    printf("║  SVE: DISABLED (scalar fallback)%*s║\n", 35, "");
#endif
    printf("╚═══════════════════════════════════════════════════════════════════════╝\n\n");

    /* Setup mock UB memory */
    ub_memory_space_t *ub_mem = mock_ub_mem_create(num_embeddings);
    if (!ub_mem) { fprintf(stderr, "OOM\n"); return 1; }

    /* Setup bitmap (all free — no contention for clean comparison) */
    state_bitmap_t *bmp = bitmap_create(num_embeddings);
    if (!bmp) { fprintf(stderr, "bitmap_create failed\n"); mock_ub_mem_destroy(ub_mem); return 1; }

    /* Setup worker context */
    sve_worker_context_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.worker_id = 0;
    ctx.ub_mem = ub_mem;
    ctx.bitmap = bmp;
    ctx.sve_vl = SVE_VECTOR_BITS / 8;

    printf("  %-40s  %8s  %8s  %8s  %6s  %s\n",
           "Method", "avg", "best", "worst", "per-emb", "throughput");
    printf("  %-40s  %8s  %8s  %8s  %6s  %s\n",
           "----------------------------------------",
           "--------", "--------", "--------", "------", "----------");

    unsigned seed = 42;

    for (int bi = 0; bi < num_batch_sizes; bi++) {
        size_t batch = (size_t)batch_sizes[bi];
        if (batch > num_embeddings) continue;

        printf("\n  --- Batch size: %zu ---\n", batch);

        /* Generate unique random embedding IDs (Fisher-Yates partial shuffle) */
        uint64_t *ids = zmalloc(batch * sizeof(uint64_t));
        {
            uint64_t *pool = zmalloc(num_embeddings * sizeof(uint64_t));
            for (size_t i = 0; i < num_embeddings; i++) pool[i] = i;
            for (size_t i = 0; i < batch; i++) {
                size_t j = i + (rand_r(&seed) % (num_embeddings - i));
                uint64_t tmp = pool[i]; pool[i] = pool[j]; pool[j] = tmp;
                ids[i] = pool[i];
            }
            zfree(pool);
        }

        float *res_a = zcalloc(batch * SUPERNODE_EMBEDDING_DIM * sizeof(float));
        float *res_b = zcalloc(batch * SUPERNODE_EMBEDDING_DIM * sizeof(float));
        uint8_t *mask_a = zcalloc(batch);
        uint8_t *mask_b = zcalloc(batch);

        /* Baseline: per-embedding serial (existing implementation) */
        bench_result_t baseline = {
            .label = "Baseline (per-emb serial)",
            .fn = sve2_gather_with_bitmap_check
        };
        run_bench(&baseline, &ctx, ids, batch, res_a, mask_a, warmup, iters);
        print_result(&baseline, iters, batch);

        /* New: cross-embedding scatter/gather */
        bench_result_t sg = {
            .label = "Scatter/Gather (cross-emb)",
            .fn = sve2_scatter_gather_read
        };
        run_bench(&sg, &ctx, ids, batch, res_b, mask_b, warmup, iters);
        print_result(&sg, iters, batch);

        /* Correctness check */
        int mismatch = 0;
        for (size_t i = 0; i < batch && !mismatch; i++) {
            if (mask_a[i] != mask_b[i]) { mismatch = 1; break; }
            if (mask_a[i] == 0) continue;
            if (memcmp(&res_a[i * SUPERNODE_EMBEDDING_DIM],
                       &res_b[i * SUPERNODE_EMBEDDING_DIM],
                       SUPERNODE_EMBEDDING_DIM * sizeof(float)) != 0)
                mismatch = 1;
        }

        double speedup = baseline.total_ns / sg.total_ns;
        printf("  → Speedup: %.2fx  Correctness: %s\n",
               speedup, mismatch ? "MISMATCH ✗" : "OK ✓");

        zfree(ids); zfree(res_a); zfree(res_b); zfree(mask_a); zfree(mask_b);
    }

    /* Print scatter/gather counters */
    printf("\n  Scatter/Gather counters:\n");
    printf("    gather_ops:      %llu\n", (unsigned long long)atomic_load(&ctx.gather_ops));
    printf("    gather_elements: %llu\n", (unsigned long long)atomic_load(&ctx.gather_elements));
    if (atomic_load(&ctx.gather_ops) > 0) {
        double util = (double)atomic_load(&ctx.gather_elements) /
                      ((double)atomic_load(&ctx.gather_ops) * SVE_ELEMENTS_PER_VECTOR);
        printf("    lane utilization: %.1f%%\n", util * 100.0);
    }

    printf("\n✅ Benchmark complete.\n");

    bitmap_destroy(bmp);
    mock_ub_mem_destroy(ub_mem);
    return 0;
}
