#define _GNU_SOURCE

#include "vemb_v16_log.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

static int g_vemb_v16_log_level = LL_NOTICE;

void vemb_v16_set_log_level(int level) {
    if ((level & 0xff) < LL_DEBUG) level = LL_DEBUG;
    if ((level & 0xff) > LL_NOTHING) level = LL_NOTHING;
    g_vemb_v16_log_level = level;
}

int vemb_v16_parse_log_level(const char *name, int *level) {
    if (!name || !level) return -1;
    if (!strcmp(name, "debug")) *level = LL_DEBUG;
    else if (!strcmp(name, "verbose")) *level = LL_VERBOSE;
    else if (!strcmp(name, "notice")) *level = LL_NOTICE;
    else if (!strcmp(name, "warning")) *level = LL_WARNING;
    else if (!strcmp(name, "nothing")) *level = LL_NOTHING;
    else return -1;
    return 0;
}

void serverLog(int level, const char *fmt, ...) {
    int raw = level & LL_RAW;
    int base_level = level & 0xff;
    if (base_level < g_vemb_v16_log_level) return;

    FILE *fp = stderr;
    flockfile(fp);
    if (!raw) {
        struct timeval tv;
        struct tm tm;
        char tbuf[64];
        gettimeofday(&tv, NULL);
        localtime_r(&tv.tv_sec, &tm);
        strftime(tbuf, sizeof(tbuf), "%d %b %Y %H:%M:%S", &tm);
        fprintf(fp, "%d:%c %s.%03ld ",
                (int)getpid(),
                ".-*#"[base_level > LL_WARNING ? LL_WARNING : base_level],
                tbuf,
                (long)(tv.tv_usec / 1000));
    }

    va_list ap;
    va_start(ap, fmt);
    vfprintf(fp, fmt, ap);
    va_end(ap);
    fputc('\n', fp);
    fflush(fp);
    funlockfile(fp);
}
