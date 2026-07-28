/*
 * SDK-internal macro.h — self-contained, does NOT depend on Redis config.h.
 *
 * This file is shipped alongside the SDK public headers (as macro.h) so that
 * SDK consumers do not need hpc-redis/src/config.h.  It is functionally
 * equivalent to src/macro.h but inlines likely/unlikely definitions.
 */
#ifndef __VEMB_V16_SDK_MACRO_H
#define __VEMB_V16_SDK_MACRO_H

#if defined(__GNUC__) || defined(__clang__)
#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define likely(x)   (x)
#define unlikely(x) (x)
#endif

#define RETURN_IF0(expr)     \
    do {                     \
        if (unlikely(expr)) { \
            return;          \
        }                    \
    } while (0)

#define RETURN_IF1(expr, rc) \
    do {                     \
        if (unlikely(expr)) { \
            return (rc);     \
        }                    \
    } while (0)

#define GOTO_IF(expr, label) \
    do {                     \
        if (unlikely(expr)) { \
            goto label;      \
        }                    \
    } while (0)

#define RETURN_IF_PICKER(_1, _2, NAME, ...) NAME

#define RETURN_IF(...) \
    RETURN_IF_PICKER(__VA_ARGS__, RETURN_IF1, RETURN_IF0, unused)(__VA_ARGS__)

#endif /* __VEMB_V16_SDK_MACRO_H */
