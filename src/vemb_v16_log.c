#define _GNU_SOURCE

#include "vemb_v16_log.h"

#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>

typedef struct vemb_v16_log_state {
    int verbosity;
    int daemonize;
    int sentinel_mode;
    int syslog_enabled;
    int daylight_active;
    pid_t pid;
    long timezone;
    char logfile[256];
} vemb_v16_log_state_t;

static vemb_v16_log_state_t server = {
    .verbosity = LL_NOTICE,
    .daemonize = 0,
    .sentinel_mode = 0,
    .syslog_enabled = 0,
    .daylight_active = 0,
    .pid = 0,
    .timezone = 0,
    .logfile = "",
};

int vemb_v16_log_verbosity_value = LL_NOTICE;

/* Redis server.c uses this fork-safe localtime helper for logging. */
void nolocks_localtime(struct tm *tmp, time_t t, time_t tz, int dst);

/* Redis util.c helpers used by Redis server.c logging. */
static uint32_t vemb_v16_digits10(uint64_t v) {
    if (v < 10) return 1;
    if (v < 100) return 2;
    if (v < 1000) return 3;
    if (v < 1000000000000UL) {
        if (v < 100000000UL) {
            if (v < 1000000) {
                if (v < 10000) return 4;
                return 5 + (v >= 100000);
            }
            return 7 + (v >= 10000000UL);
        }
        if (v < 10000000000UL)
            return 9 + (v >= 1000000000UL);
        return 11 + (v >= 100000000000UL);
    }
    return 12 + vemb_v16_digits10(v / 1000000000000UL);
}

static int vemb_v16_ull2string(char *dst, size_t dstlen, unsigned long long value) {
    static const char digits[201] =
        "0001020304050607080910111213141516171819"
        "2021222324252627282930313233343536373839"
        "4041424344454647484950515253545556575859"
        "6061626364656667686970717273747576777879"
        "8081828384858687888990919293949596979899";

    uint32_t length = vemb_v16_digits10(value);
    if (length >= dstlen) goto err;

    uint32_t next = length - 1;
    dst[next + 1] = '\0';
    while (value >= 100) {
        int const i = (value % 100) * 2;
        value /= 100;
        dst[next] = digits[i + 1];
        dst[next - 1] = digits[i];
        next -= 2;
    }
    if (value < 10) {
        dst[next] = '0' + (uint32_t)value;
    } else {
        int i = (uint32_t)value * 2;
        dst[next] = digits[i + 1];
        dst[next - 1] = digits[i];
    }
    return length;

err:
    if (dstlen > 0) dst[0] = '\0';
    return 0;
}

static int ll2string(char *dst, size_t dstlen, long long svalue) {
    unsigned long long value;
    int negative = 0;

    if (svalue < 0) {
        if (svalue != LLONG_MIN)
            value = -svalue;
        else
            value = ((unsigned long long)LLONG_MAX) + 1;
        if (dstlen < 2) goto err;
        negative = 1;
        dst[0] = '-';
        dst++;
        dstlen--;
    } else {
        value = svalue;
    }

    int length = vemb_v16_ull2string(dst, dstlen, value);
    if (length == 0) return 0;
    return length + negative;

err:
    if (dstlen > 0) dst[0] = '\0';
    return 0;
}

static const char HEX[] = "0123456789abcdef";

static char *u2string_async_signal_safe(int _base, uint64_t val, char *buf) {
    uint32_t base = (uint32_t)_base;
    *buf-- = 0;
    do {
        *buf-- = HEX[val % base];
    } while ((val /= base) != 0);
    return buf + 1;
}

static char *i2string_async_signal_safe(int base, int64_t val, char *buf) {
    char *orig_buf = buf;
    const int32_t is_neg = (val < 0);
    *buf-- = 0;

    if (is_neg) val = -val;
    if (is_neg && base == 16) {
        val -= 1;
        for (int ix = 0; ix < 16; ++ix) buf[-ix] = '0';
    }

    do {
        *buf-- = HEX[val % base];
    } while ((val /= base) != 0);

    if (is_neg && base == 10) {
        *buf-- = '-';
    }

    if (is_neg && base == 16) {
        buf = orig_buf - 1;
        for (int ix = 0; ix < 16; ++ix, --buf) {
            switch (*buf) {
            case '0': *buf = 'f'; break;
            case '1': *buf = 'e'; break;
            case '2': *buf = 'd'; break;
            case '3': *buf = 'c'; break;
            case '4': *buf = 'b'; break;
            case '5': *buf = 'a'; break;
            case '6': *buf = '9'; break;
            case '7': *buf = '8'; break;
            case '8': *buf = '7'; break;
            case '9': *buf = '6'; break;
            case 'a': *buf = '5'; break;
            case 'b': *buf = '4'; break;
            case 'c': *buf = '3'; break;
            case 'd': *buf = '2'; break;
            case 'e': *buf = '1'; break;
            case 'f': *buf = '0'; break;
            }
        }
    }
    return buf + 1;
}

static const char *check_longlong_async_signal_safe(const char *fmt,
                                                    int32_t *have_longlong) {
    *have_longlong = 0;
    if (*fmt == 'l') {
        fmt++;
        if (*fmt != 'l')
            *have_longlong = (sizeof(long) == sizeof(int64_t));
        else {
            fmt++;
            *have_longlong = 1;
        }
    }
    return fmt;
}

static int vsnprintf_async_signal_safe(char *to, size_t size, const char *format,
                                       va_list ap) {
    char *start = to;
    char *end = start + size - 1;
    for (; *format; ++format) {
        int32_t have_longlong = 0;
        if (*format != '%') {
            if (to == end) break;
            *to++ = *format;
            continue;
        }
        ++format;
        format = check_longlong_async_signal_safe(format, &have_longlong);

        switch (*format) {
        case 'd':
        case 'i':
        case 'u':
        case 'x':
        case 'p': {
            int64_t ival = 0;
            uint64_t uval = 0;
            if (*format == 'p')
                have_longlong = (sizeof(void *) == sizeof(uint64_t));
            if (have_longlong) {
                if (*format == 'u')
                    uval = va_arg(ap, uint64_t);
                else
                    ival = va_arg(ap, int64_t);
            } else {
                if (*format == 'u')
                    uval = va_arg(ap, uint32_t);
                else
                    ival = va_arg(ap, int32_t);
            }

            char buff[22];
            const int base = (*format == 'x' || *format == 'p') ? 16 : 10;
            char *val_as_str = (*format == 'u') ?
                u2string_async_signal_safe(base, uval, &buff[sizeof(buff) - 1]) :
                i2string_async_signal_safe(base, ival, &buff[sizeof(buff) - 1]);
            if (*format == 'x' && !have_longlong && ival < 0)
                val_as_str += 8;
            while (*val_as_str && to < end)
                *to++ = *val_as_str++;
            continue;
        }
        case 's': {
            const char *val = va_arg(ap, char *);
            if (!val) val = "(null)";
            while (*val && to < end)
                *to++ = *val++;
            continue;
        }
        }
    }
    *to = 0;
    return (int)(to - start);
}

void vemb_v16_set_log_level(int level) {
    if ((level & 0xff) < LL_DEBUG) level = LL_DEBUG;
    if ((level & 0xff) > LL_NOTHING) level = LL_NOTHING;
    server.verbosity = level & 0xff;
    vemb_v16_log_verbosity_value = server.verbosity;
}

void vemb_v16_log_init(void) {
    tzset();
    server.pid = getpid();
#ifdef __APPLE__
    server.timezone = 0;
#else
    server.timezone = timezone;
#endif
    time_t t = time(NULL);
    struct tm tm;
    if (localtime_r(&t, &tm) != NULL)
        server.daylight_active = tm.tm_isdst;
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

/* Extracted from Redis server.c: serverLogRaw(). */
void serverLogRaw(int level, const char *msg) {
    const int syslogLevelMap[] = { LOG_DEBUG, LOG_INFO, LOG_NOTICE, LOG_WARNING };
    const char *c = ".-*#";
    FILE *fp;
    char buf[64];
    int rawmode = (level & LL_RAW);
    int log_to_stdout = server.logfile[0] == '\0';

    level &= 0xff;
    if (level < server.verbosity) return;

    fp = log_to_stdout ? stdout : fopen(server.logfile, "a");
    if (!fp) return;

    if (rawmode) {
        fprintf(fp, "%s", msg);
    } else {
        int off;
        struct timeval tv;
        int role_char;
        int daylight_active = server.daylight_active;
        pid_t pid = getpid();

        gettimeofday(&tv, NULL);
        struct tm tm;
        nolocks_localtime(&tm, tv.tv_sec, server.timezone, daylight_active);
        off = strftime(buf, sizeof(buf), "%d %b %Y %H:%M:%S.", &tm);
        snprintf(buf + off, sizeof(buf) - off, "%03d", (int)tv.tv_usec / 1000);
        if (server.sentinel_mode) {
            role_char = 'X';
        } else if (pid != server.pid) {
            role_char = 'C';
        } else {
            role_char = 'M';
        }
        fprintf(fp, "%d:%c %s %c %s\n", (int)getpid(), role_char, buf,
                c[level > LL_WARNING ? LL_WARNING : level], msg);
    }
    fflush(fp);

    if (!log_to_stdout) fclose(fp);
    if (server.syslog_enabled)
        syslog(syslogLevelMap[level > LL_WARNING ? LL_WARNING : level], "%s", msg);
}

/* Extracted from Redis server.c: _serverLog(). */
void _serverLog(int level, const char *fmt, ...) {
    va_list ap;
    char msg[LOG_MAX_LEN];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    serverLogRaw(level, msg);
}

/* Extracted from Redis server.c: serverLogRawFromHandler(). */
void serverLogRawFromHandler(int level, const char *msg) {
    int fd;
    int log_to_stdout = server.logfile[0] == '\0';
    char buf[64];

    if ((level & 0xff) < server.verbosity || (log_to_stdout && server.daemonize))
        return;
    fd = log_to_stdout ? STDOUT_FILENO :
                         open(server.logfile, O_APPEND | O_CREAT | O_WRONLY, 0644);
    if (fd == -1) return;
    if (level & LL_RAW) {
        if (write(fd, msg, strlen(msg)) == -1) goto err;
    } else {
        ll2string(buf, sizeof(buf), getpid());
        if (write(fd, buf, strlen(buf)) == -1) goto err;
        if (write(fd, ":signal-handler (", 17) == -1) goto err;
        ll2string(buf, sizeof(buf), time(NULL));
        if (write(fd, buf, strlen(buf)) == -1) goto err;
        if (write(fd, ") ", 2) == -1) goto err;
        if (write(fd, msg, strlen(msg)) == -1) goto err;
        if (write(fd, "\n", 1) == -1) goto err;
    }
err:
    if (!log_to_stdout) close(fd);
}

/* Extracted from Redis server.c: serverLogFromHandler(). */
void serverLogFromHandler(int level, const char *fmt, ...) {
    va_list ap;
    char msg[LOG_MAX_LEN];

    va_start(ap, fmt);
    vsnprintf_async_signal_safe(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    serverLogRawFromHandler(level, msg);
}
