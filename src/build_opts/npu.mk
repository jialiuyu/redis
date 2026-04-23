# src/build_opts/npu.mk — Huawei Ascend NPU
# Usage: make USE_NPU=yes
# Override: make USE_NPU=yes ASCEND_HOME=/opt/ascend

USE_NPU ?= no
ASCEND_HOME ?= /usr/local/Ascend/ascend-toolkit/latest

ifeq ($(USE_NPU),yes)
    FEATURE_CFLAGS  += -DUSE_NPU -I$(ASCEND_HOME)/include
    FEATURE_LDFLAGS += -L$(ASCEND_HOME)/lib64 -Wl,-rpath,$(ASCEND_HOME)/lib64
    FEATURE_LIBS    += -lascendcl -lacl_cblas
    FEATURE_OBJS    +=
endif
