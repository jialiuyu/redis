# src/build_opts/ub.mk — UB bus components
# Usage: make USE_UB=yes

USE_UB ?= no

ifeq ($(USE_UB),yes)
    FEATURE_CFLAGS  += -DUSE_UB
    FEATURE_LDFLAGS +=
    FEATURE_OBJS    += proxy_aggregator.o supernode_worker.o
endif
