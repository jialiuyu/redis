/*
 * TLC Network Benchmark Client
 *
 * Connects to Redis via hiredis, exercises TLC.PUT/GET/MGET
 * with real 1200B data over the network interface.
 * Multi-threaded with pipeline support.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <stdint.h>
#include <math.h>
#include "hiredis.h"

#define VALUE_SIZE 1200
#define DEFAULT_OPS 500000
#define DEFAULT_THREADS 8
#define DEFAULT_PIPELINE 16
#define DEFAULT_PORT 6380

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static uint64_t zipf_key(unsigned int *seed, uint64_t max_key) {
    double u = (double)rand_r(seed) / RAND_MAX;
    return (uint64_t)(pow(u, 1.0 / 1.2) * (double)max_key) % max_key;
}

typedef struct {
    int thread_id;
    int port;
    size_t num_ops;
    int pipeline;
    int write_pct;  /* 0-100 */
    uint64_t max_key;
    uint64_t elapsed_ns;
    uint64_t put_ok, get_ok, get_miss;
} bench_thread_t;

static void *bench_worker(void *arg) {
    bench_thread_t *t = arg;
    unsigned int seed = (unsigned int)(t->thread_id + 42);

    redisContext *c = redisConnect("127.0.0.1", t->port);
    if (!c || c->err) {
        fprintf(stderr, "Thread %d: connect failed: %s\n",
                t->thread_id, c ? c->errstr : "NULL");
        t->elapsed_ns = 0;
        return NULL;
    }

    /* Generate a random 1200B value */
    char value[VALUE_SIZE];
    for (int i = 0; i < VALUE_SIZE; i++)
        value[i] = (char)(rand_r(&seed) & 0xFF);

    uint64_t start = now_ns();

    if (t->pipeline <= 1) {
        /* Non-pipeline mode */
        for (size_t i = 0; i < t->num_ops; i++) {
            uint64_t key = zipf_key(&seed, t->max_key);
            int is_write = (rand_r(&seed) % 100) < t->write_pct;

            if (is_write) {
                redisReply *r = redisCommand(c, "TLC.PUT %llu %b",
                                             (unsigned long long)key, value, (size_t)VALUE_SIZE);
                if (r && r->type == REDIS_REPLY_STATUS) t->put_ok++;
                if (r) freeReplyObject(r);
            } else {
                redisReply *r = redisCommand(c, "TLC.GET %llu",
                                             (unsigned long long)key);
                if (r) {
                    if (r->type == REDIS_REPLY_STRING) t->get_ok++;
                    else t->get_miss++;
                    freeReplyObject(r);
                }
            }
        }
    } else {
        /* Pipeline mode */
        size_t pending = 0;
        int *is_write_arr = malloc(t->pipeline * sizeof(int));

        for (size_t i = 0; i < t->num_ops; i++) {
            uint64_t key = zipf_key(&seed, t->max_key);
            int is_write = (rand_r(&seed) % 100) < t->write_pct;
            is_write_arr[pending] = is_write;

            if (is_write) {
                redisAppendCommand(c, "TLC.PUT %llu %b",
                                   (unsigned long long)key, value, (size_t)VALUE_SIZE);
            } else {
                redisAppendCommand(c, "TLC.GET %llu",
                                   (unsigned long long)key);
            }
            pending++;

            if (pending >= (size_t)t->pipeline || i == t->num_ops - 1) {
                for (size_t p = 0; p < pending; p++) {
                    redisReply *r = NULL;
                    redisGetReply(c, (void **)&r);
                    if (r) {
                        if (is_write_arr[p]) {
                            if (r->type == REDIS_REPLY_STATUS) t->put_ok++;
                        } else {
                            if (r->type == REDIS_REPLY_STRING) t->get_ok++;
                            else t->get_miss++;
                        }
                        freeReplyObject(r);
                    }
                }
                pending = 0;
            }
        }
        free(is_write_arr);
    }

    t->elapsed_ns = now_ns() - start;
    redisFree(c);
    return NULL;
}

static void run_bench(const char *label, int port, size_t total_ops,
                      int num_threads, int pipeline, int write_pct,
                      uint64_t max_key) {
    printf("\n  %s\n", label);
    printf("  Ops:%zu Thr:%d Pipeline:%d W%%:%d MaxKey:%lu\n",
           total_ops, num_threads, pipeline, write_pct, max_key);

    bench_thread_t *threads = calloc(num_threads, sizeof(*threads));
    pthread_t *pts = calloc(num_threads, sizeof(*pts));
    size_t ops_per = total_ops / num_threads;

    for (int i = 0; i < num_threads; i++) {
        threads[i] = (bench_thread_t){
            .thread_id = i, .port = port, .num_ops = ops_per,
            .pipeline = pipeline, .write_pct = write_pct,
            .max_key = max_key, .elapsed_ns = 0,
            .put_ok = 0, .get_ok = 0, .get_miss = 0
        };
        pthread_create(&pts[i], NULL, bench_worker, &threads[i]);
    }
    for (int i = 0; i < num_threads; i++)
        pthread_join(pts[i], NULL);

    uint64_t max_ns = 0, tot_put = 0, tot_get = 0, tot_miss = 0;
    for (int i = 0; i < num_threads; i++) {
        if (threads[i].elapsed_ns > max_ns) max_ns = threads[i].elapsed_ns;
        tot_put += threads[i].put_ok;
        tot_get += threads[i].get_ok;
        tot_miss += threads[i].get_miss;
    }

    double secs = (double)max_ns / 1e9;
    double qps = (double)total_ops / secs;
    double lat_us = (double)max_ns / total_ops / 1e3;

    printf("  → %.2f QPS (%.2f M/s)  Lat: %.1f μs  PUT:%lu GET:%lu MISS:%lu\n",
           qps, qps / 1e6, lat_us, tot_put, tot_get, tot_miss);

    free(threads);
    free(pts);
}

/* Also benchmark standard SET/GET for comparison */
static void *std_worker(void *arg) {
    bench_thread_t *t = arg;
    unsigned int seed = (unsigned int)(t->thread_id + 42);

    redisContext *c = redisConnect("127.0.0.1", t->port);
    if (!c || c->err) { t->elapsed_ns = 0; return NULL; }

    char value[VALUE_SIZE];
    for (int i = 0; i < VALUE_SIZE; i++) value[i] = (char)(rand_r(&seed) & 0xFF);

    char keybuf[32];
    uint64_t start = now_ns();

    if (t->pipeline <= 1) {
        for (size_t i = 0; i < t->num_ops; i++) {
            uint64_t key = zipf_key(&seed, t->max_key);
            int is_write = (rand_r(&seed) % 100) < t->write_pct;
            snprintf(keybuf, sizeof(keybuf), "k:%lu", key);

            if (is_write) {
                redisReply *r = redisCommand(c, "SET %s %b", keybuf, value, (size_t)VALUE_SIZE);
                if (r && r->type == REDIS_REPLY_STATUS) t->put_ok++;
                if (r) freeReplyObject(r);
            } else {
                redisReply *r = redisCommand(c, "GET %s", keybuf);
                if (r) {
                    if (r->type == REDIS_REPLY_STRING) t->get_ok++;
                    else t->get_miss++;
                    freeReplyObject(r);
                }
            }
        }
    } else {
        size_t pending = 0;
        int *is_write_arr = malloc(t->pipeline * sizeof(int));
        for (size_t i = 0; i < t->num_ops; i++) {
            uint64_t key = zipf_key(&seed, t->max_key);
            int is_write = (rand_r(&seed) % 100) < t->write_pct;
            is_write_arr[pending] = is_write;
            snprintf(keybuf, sizeof(keybuf), "k:%lu", key);
            if (is_write)
                redisAppendCommand(c, "SET %s %b", keybuf, value, (size_t)VALUE_SIZE);
            else
                redisAppendCommand(c, "GET %s", keybuf);
            pending++;
            if (pending >= (size_t)t->pipeline || i == t->num_ops - 1) {
                for (size_t p = 0; p < pending; p++) {
                    redisReply *r = NULL;
                    redisGetReply(c, (void **)&r);
                    if (r) {
                        if (is_write_arr[p]) { if (r->type == REDIS_REPLY_STATUS) t->put_ok++; }
                        else { if (r->type == REDIS_REPLY_STRING) t->get_ok++; else t->get_miss++; }
                        freeReplyObject(r);
                    }
                }
                pending = 0;
            }
        }
        free(is_write_arr);
    }

    t->elapsed_ns = now_ns() - start;
    redisFree(c);
    return NULL;
}

static void run_std_bench(const char *label, int port, size_t total_ops,
                          int num_threads, int pipeline, int write_pct,
                          uint64_t max_key) {
    printf("\n  %s\n", label);
    printf("  Ops:%zu Thr:%d Pipeline:%d W%%:%d MaxKey:%lu\n",
           total_ops, num_threads, pipeline, write_pct, max_key);

    bench_thread_t *threads = calloc(num_threads, sizeof(*threads));
    pthread_t *pts = calloc(num_threads, sizeof(*pts));
    size_t ops_per = total_ops / num_threads;

    for (int i = 0; i < num_threads; i++) {
        threads[i] = (bench_thread_t){
            .thread_id = i, .port = port, .num_ops = ops_per,
            .pipeline = pipeline, .write_pct = write_pct,
            .max_key = max_key
        };
        pthread_create(&pts[i], NULL, std_worker, &threads[i]);
    }
    for (int i = 0; i < num_threads; i++) pthread_join(pts[i], NULL);

    uint64_t max_ns = 0, tot_put = 0, tot_get = 0, tot_miss = 0;
    for (int i = 0; i < num_threads; i++) {
        if (threads[i].elapsed_ns > max_ns) max_ns = threads[i].elapsed_ns;
        tot_put += threads[i].put_ok;
        tot_get += threads[i].get_ok;
        tot_miss += threads[i].get_miss;
    }
    double secs = (double)max_ns / 1e9;
    double qps = (double)total_ops / secs;
    printf("  → %.2f QPS (%.2f M/s)  Lat: %.1f μs  PUT:%lu GET:%lu MISS:%lu\n",
           qps, qps / 1e6, (double)max_ns / total_ops / 1e3, tot_put, tot_get, tot_miss);
    free(threads); free(pts);
}

int main(int argc, char *argv[]) {
    size_t ops = DEFAULT_OPS;
    int threads = DEFAULT_THREADS;
    int pipeline = DEFAULT_PIPELINE;

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--ops") && i+1 < argc) ops = (size_t)atol(argv[++i]);
        else if (!strcmp(argv[i], "--threads") && i+1 < argc) threads = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--pipeline") && i+1 < argc) pipeline = atoi(argv[++i]);
    }

    printf("╔═══════════════════════════════════════════════════════════════════╗\n");
    printf("║  V10 TLC Network Benchmark (hiredis client)                      ║\n");
    printf("║  Ops: %zu  Threads: %d  Pipeline: %d  Value: %dB               ║\n",
           ops, threads, pipeline, VALUE_SIZE);
    printf("╚═══════════════════════════════════════════════════════════════════╝\n");

    uint64_t max_key = 1100000;  /* Match the TLC.FILL count */

    /* ---- Baseline Redis (port 6379) ---- */
    printf("\n╔══════════════════════════════════════╗\n");
    printf("║  Baseline Redis (port 6379)          ║\n");
    printf("╚══════════════════════════════════════╝\n");

    /* Pre-fill baseline with 1M keys */
    printf("\n  Pre-filling baseline with 1M keys (1200B)...\n");
    run_std_bench("Baseline Pre-fill (100%% write)", 6379, 1000000, threads, pipeline, 100, max_key);

    run_std_bench("Baseline SET/GET 80R/20W (no pipeline)", 6379, ops, threads, 1, 20, max_key);
    run_std_bench("Baseline SET/GET 80R/20W (P=16)", 6379, ops, threads, pipeline, 20, max_key);
    run_std_bench("Baseline 100%% GET (P=16)", 6379, ops, threads, pipeline, 0, max_key);

    /* ---- TLC Module (port 6380) ---- */
    printf("\n╔══════════════════════════════════════╗\n");
    printf("║  TLC Module (port 6380, UB+SVE2)     ║\n");
    printf("╚══════════════════════════════════════╝\n");

    run_bench("TLC.PUT/GET 80R/20W (no pipeline)", 6380, ops, threads, 1, 20, max_key);
    run_bench("TLC.PUT/GET 80R/20W (P=16)", 6380, ops, threads, pipeline, 20, max_key);
    run_bench("TLC 100%% GET (P=16)", 6380, ops, threads, pipeline, 0, max_key);

    /* Also test standard SET/GET on optimized server for io-threads comparison */
    printf("\n╔══════════════════════════════════════╗\n");
    printf("║  Optimized Redis SET/GET (port 6380)  ║\n");
    printf("╚══════════════════════════════════════╝\n");

    run_std_bench("Optimized Pre-fill (100%% write)", 6380, 1000000, threads, pipeline, 100, max_key);
    run_std_bench("Optimized SET/GET 80R/20W (no pipeline)", 6380, ops, threads, 1, 20, max_key);
    run_std_bench("Optimized SET/GET 80R/20W (P=16)", 6380, ops, threads, pipeline, 20, max_key);
    run_std_bench("Optimized 100%% GET (P=16)", 6380, ops, threads, pipeline, 0, max_key);

    printf("\n✅ V10 Network Benchmark complete.\n");
    return 0;
}
