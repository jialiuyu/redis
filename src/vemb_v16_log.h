#ifndef VEMB_V16_LOG_H
#define VEMB_V16_LOG_H

#ifndef LL_DEBUG
#define LL_DEBUG 0
#define LL_VERBOSE 1
#define LL_NOTICE 2
#define LL_WARNING 3
#define LL_NOTHING 4
#define LL_RAW (1 << 10)
#endif

#ifndef LOG_MAX_LEN
#define LOG_MAX_LEN 1024
#endif

extern int vemb_v16_log_verbosity_value;

void vemb_v16_log_init(void);
void vemb_v16_set_log_level(int level);
int vemb_v16_parse_log_level(const char *name, int *level);
void serverLogRaw(int level, const char *msg);
void serverLogRawFromHandler(int level, const char *msg);
void serverLogFromHandler(int level, const char *fmt, ...);

#ifdef __GNUC__
void _serverLog(int level, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
#else
void _serverLog(int level, const char *fmt, ...);
#endif

#define serverLog(level, ...) do {\
        if (((level)&0xff) < vemb_v16_log_verbosity_value) break;\
        _serverLog(level, __VA_ARGS__);\
    } while (0)

#endif
