# src/build_opts/cc_mode.mk — OBMM cacheable ownership (CC mode)
# Usage: make USE_CC_MODE=yes

USE_CC_MODE ?= no

ifeq ($(USE_CC_MODE),yes)
    DEPENDENCY_TARGETS += libobmm
    FEATURE_CFLAGS  += -DUSE_CC_MODE -I../deps/libobmm
    FEATURE_LIBS    += ../deps/libobmm/libobmm_ownership.a
endif
