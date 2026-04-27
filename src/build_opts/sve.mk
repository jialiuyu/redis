# src/build_opts/sve.mk — ARM SVE gather acceleration for ub_client
# Usage: make USE_SVE=yes

USE_SVE ?= no

ifeq ($(USE_SVE),yes)
    FEATURE_MARCH_SUFFIX += +sve
    FEATURE_CFLAGS  += -DUSE_SVE
endif
