# src/build_opts/tlc.mk — Three-layer cache module
# Usage: make USE_TLC=yes (requires USE_SVE2=yes)

USE_TLC ?= no

ifeq ($(USE_TLC),yes)
    ifneq ($(USE_SVE2),yes)
        $(error USE_TLC=yes requires USE_SVE2=yes — three_layer_cache_ub.c uses SVE intrinsics)
    endif
    FEATURE_CFLAGS  += -DUSE_TLC
    FEATURE_LDFLAGS +=
    FEATURE_OBJS    += three_layer_cache_ub.o
endif
