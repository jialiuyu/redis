#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>

#include "../src/server.h"

void _serverLog(int level, const char *fmt, ...) {
    (void)level;
    (void)fmt;
}

void serverLogFromHandler(int level, const char *fmt, ...) {
    (void)level;
    (void)fmt;
}

void _serverPanic(const char *file, int line, const char *fmt, ...) {
    (void)file;
    (void)line;
    (void)fmt;
    abort();
}

void bugReportStart(void) {
}

void _serverAssert(const char *estr, const char *file, int line) {
    fprintf(stderr, "assertion failed: %s (%s:%d)\n", estr, file, line);
    abort();
}

void *zmalloc(size_t size) {
    return malloc(size ? size : 1);
}

void *zcalloc(size_t size) {
    return calloc(1, size ? size : 1);
}

void zfree(void *ptr) {
    free(ptr);
}

char *zstrdup(const char *s) {
    size_t len = strlen(s) + 1;
    char *out = malloc(len);
    if (!out) {
        return NULL;
    }
    memcpy(out, s, len);
    return out;
}

#include "../src/monotonic.c"

static uint64_t ustime_gettimeofday(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return ((uint64_t)tv.tv_sec) * 1000000ULL + (uint64_t)tv.tv_usec;
}

static uint64_t monotonic_posix_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((uint64_t)ts.tv_sec) * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

static uint64_t wall_time_seconds_us(void) {
    return (uint64_t)time(NULL) * 1000000ULL;
}

static void bench_one(const char *name, uint64_t (*fn)(void), size_t iterations) {
    struct timespec t0, t1;
    volatile uint64_t sink = 0;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (size_t i = 0; i < iterations; i++) {
        sink += fn();
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    uint64_t elapsed_ns =
        ((uint64_t)(t1.tv_sec - t0.tv_sec) * 1000000000ULL) +
        (uint64_t)(t1.tv_nsec - t0.tv_nsec);
    double avg_ns = (double)elapsed_ns / (double)iterations;

    printf("%-28s total=%" PRIu64 " ns  avg=%.2f ns/call  sink=%" PRIu64 "\n",
           name, elapsed_ns, avg_ns, sink);
}

int main(int argc, char **argv) {
    size_t iterations = 10000000;

    if (argc > 1) {
        char *end = NULL;
        unsigned long long parsed = strtoull(argv[1], &end, 10);
        if (end == NULL || *end != '\0' || parsed == 0) {
            fprintf(stderr, "usage: %s [iterations]\n", argv[0]);
            return 1;
        }
        iterations = (size_t)parsed;
    }

    const char *clock_info = monotonicInit();

    printf("Monotonic clock info: %s\n", clock_info);
    printf("Iterations: %zu\n\n", iterations);

    bench_one("getMonotonicUs()", getMonotonicUs, iterations);
    bench_one("clock_gettime(MONOTONIC)", monotonic_posix_us, iterations);
    bench_one("gettimeofday() wall clock", ustime_gettimeofday, iterations);
    bench_one("time(NULL) wall clock", wall_time_seconds_us, iterations);

    return 0;
}
