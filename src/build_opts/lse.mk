# src/build_opts/lse.mk — ARM LSE atomic instructions
# Usage: make USE_LSE=yes

USE_LSE ?= no

ifeq ($(USE_LSE),yes)
    FEATURE_MARCH_SUFFIX += +lse
    FEATURE_CFLAGS  +=
    FEATURE_LDFLAGS +=
    FEATURE_OBJS    +=
endif
