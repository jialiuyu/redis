#ifndef __MACRO_H
#define __MACRO_H

#define GET_ARG3(arg1, arg2, arg3, ...) arg3

#define RETURN_IF1(expr, rc) \
  do {                       \
    if ((expr)) {            \
      return rc;             \
    }                        \
  } while (0)

#define RETURN_IF0(expr) \
  do {                   \
    if ((expr)) {        \
      return;            \
    }                    \
  } while (0)

#define RETURN_IF(expr, ...) \
  GET_ARG3(1, ##__VA_ARGS__, RETURN_IF1, RETURN_IF0)(expr, ##__VA_ARGS__)

#endif // _MACRO_H