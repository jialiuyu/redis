#ifndef __MACRO_H
#define __MACRO_H

#include "config.h"

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

#endif // _MACRO_H
