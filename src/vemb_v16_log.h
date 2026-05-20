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

void vemb_v16_set_log_level(int level);
int vemb_v16_parse_log_level(const char *name, int *level);
void serverLog(int level, const char *fmt, ...);

#endif
