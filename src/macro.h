#ifndef __MACRO_H
#define __MACRO_H

#define RETURN_IF0(expr)     \
    do {                     \
        if ((expr)) {        \
            return;          \
        }                    \
    } while (0)

#define RETURN_IF1(expr, rc) \
    do {                     \
        if ((expr)) {        \
            return (rc);     \
        }                    \
    } while (0)

#define RETURN_IF_PICKER(_1, _2, NAME, ...) NAME

#define RETURN_IF(...) \
    RETURN_IF_PICKER(__VA_ARGS__, RETURN_IF1, RETURN_IF0, unused)(__VA_ARGS__)

#endif // _MACRO_H