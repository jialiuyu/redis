# src/build_opts/ub.mk — UB bus components
# Usage: make USE_UB=yes

USE_UB ?= no

ifeq ($(USE_UB),yes)
    FEATURE_CFLAGS  += -DUSE_UB
    FEATURE_LDFLAGS +=
    FEATURE_OBJS    += consistent_hash.o ring_buffer.o ring_buffer_mgr.o proxy_router.o proxy_flush_scheduler.o proxy_batch_bucket.o proxy_active_bucket_heap.o proxy_flush_executor.o proxy_aggregator.o supernode_worker.o sve_operation.o
endif
