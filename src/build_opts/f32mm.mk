# src/build_opts/f32mm.mk — SVE F32MM matrix extension
# Usage: make USE_SVE2=yes USE_F32MM=yes

USE_F32MM ?= no

ifeq ($(USE_F32MM),yes)
    ifneq ($(USE_SVE2),yes)
        $(error USE_F32MM=yes requires USE_SVE2=yes)
    endif
    FEATURE_MARCH_SUFFIX += +f32mm
    FEATURE_CFLAGS  +=
    FEATURE_LDFLAGS +=
    FEATURE_OBJS    +=
endif
