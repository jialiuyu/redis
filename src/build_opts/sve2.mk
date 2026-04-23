# src/build_opts/sve2.mk — ARM SVE2 vector instructions
# Usage: make USE_SVE2=yes

USE_SVE2 ?= no

ifeq ($(USE_SVE2),yes)
    FEATURE_MARCH_SUFFIX += +sve
    FEATURE_CFLAGS  += -DUSE_SVE2
    FEATURE_LDFLAGS +=
    FEATURE_OBJS    +=
endif
