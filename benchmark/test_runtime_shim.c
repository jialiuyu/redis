#include "test_runtime_shim.h"

#include "../src/sds.h"
#include "../src/monotonic.h"

#ifdef TEST_RUNTIME_WITH_SERVER
#include "../src/server.h"
#endif

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

static void test_runtime_vpanic(const char *prefix,
                                const char *file,
                                int line,
                                const char *fmt,
                                va_list ap) {
    fprintf(stderr, "%s at %s:%d: ", prefix, file, line);
    vfprintf(stderr, fmt, ap);
    fputc('\n', stderr);
    abort();
}

void test_runtime_reset_server(void) {
#ifdef TEST_RUNTIME_WITH_SERVER
    memset(&server, 0, sizeof(server));
#endif
}

#ifdef TEST_RUNTIME_WITH_SERVER
struct redisServer server = {0};
#endif

void _serverLog(int level, const char *fmt, ...) {
    (void)level;
    (void)fmt;
}

#ifndef TEST_RUNTIME_WITH_SERVER
void serverLog(int level, const char *fmt, ...) {
    va_list ap;

    (void)level;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    fputc('\n', stderr);
    va_end(ap);
}
#endif

void serverLogFromHandler(int level, const char *fmt, ...) {
    (void)level;
    (void)fmt;
}

void _serverAssert(const char *estr, const char *file, int line) {
    fprintf(stderr, "assertion failed at %s:%d: %s\n", file, line, estr);
    abort();
}

void _serverPanic(const char *file, int line, const char *msg, ...) {
    va_list ap;

    va_start(ap, msg);
    test_runtime_vpanic("panic", file, line, msg, ap);
    va_end(ap);
}

long long ustime(void) {
    struct timeval tv;

    gettimeofday(&tv, NULL);
    return (long long)tv.tv_sec * 1000000LL + tv.tv_usec;
}

mstime_t mstime(void) {
    return ustime() / 1000;
}

static monotime test_get_monotonic_us(void) {
    return (monotime)ustime();
}

static monotime test_get_monotonic_ns(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return ((uint64_t)tv.tv_sec) * 1000000000ULL + ((uint64_t)tv.tv_usec) * 1000ULL;
}

monotime (*getMonotonicUs)(void) = test_get_monotonic_us;
monotime (*getMonotonicNs)(void) = test_get_monotonic_ns;

const char *monotonicInit(void) {
    getMonotonicUs = test_get_monotonic_us;
    getMonotonicNs = test_get_monotonic_ns;
    return "test monotonic";
}

const char *monotonicInfoString(void) {
    return "test monotonic";
}

monotonic_clock_type monotonicGetType(void) {
    return MONOTONIC_CLOCK_POSIX;
}

void *zmalloc(size_t size) {
    return malloc(size ? size : 1);
}

void *zcalloc(size_t size) {
    return calloc(1, size ? size : 1);
}

void *zrealloc(void *ptr, size_t size) {
    return realloc(ptr, size ? size : 1);
}

void zfree(void *ptr) {
    free(ptr);
}

char *zstrdup(const char *s) {
    char *out;
    size_t len;

    if (!s) return NULL;
    len = strlen(s) + 1;
    out = malloc(len);
    if (!out) return NULL;
    memcpy(out, s, len);
    return out;
}

sds sdsempty(void) {
    return zstrdup("");
}

sds sdsnew(const char *init) {
    if (!init) init = "";
    return zstrdup(init);
}

sds sdscat(sds s, const char *t) {
    size_t slen = s ? strlen(s) : 0;
    size_t tlen = t ? strlen(t) : 0;
    char *buf = realloc(s, slen + tlen + 1);

    if (!buf) {
        free(s);
        return NULL;
    }
    if (tlen) memcpy(buf + slen, t, tlen);
    buf[slen + tlen] = '\0';
    return buf;
}

sds sdscatprintf(sds s, const char *fmt, ...) {
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
    if (!buf) {
        va_end(ap);
        free(s);
        return NULL;
    }
    vsnprintf(buf + slen, (size_t)needed + 1, fmt, ap);
    va_end(ap);
    return buf;
}

void sdsfree(sds s) {
    free(s);
}
