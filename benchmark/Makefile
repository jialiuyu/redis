# Makefile for Redis UB+SVE Benchmark Suite
# 编译所有基准测试程序

CC = gcc
CFLAGS = -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../deps/hiredis
LDFLAGS = -pthread -lm -L../deps/hiredis -lhiredis

# 目标文件
TARGETS = redis_traditional_benchmark supernode_benchmark compare_results supernode_real_benchmark supernode_server three_layer_benchmark_v4 supernode_benchmark_optimized tlc_aeron_bench tlc_fc_bench tlc_kcp_bench tlc_raw_bench tlc_transport_bench tlc_all_transport_bench tlc_client_bench tlc_v10_bench tlc_v13_bench tlc_v14_bench ring_buffer_ut ring_buffer_batch_bench ring_buffer_compare_bench proxy_components_ut proxy_scheduler_ut proxy_aggregator_ut vector_proxy_completion_ut monotonic_clock_bench

# 所有目标
all: $(TARGETS)
	@echo ""
	@echo "✅ All benchmarks compiled successfully!"
	@echo ""
	@echo "Run benchmarks:"
	@echo "  ./redis_traditional_benchmark --help"
	@echo "  ./supernode_benchmark --help"
	@echo "  ./run_full_benchmark.sh"
	@echo ""

# 传统 Redis 基准测试
redis_traditional_benchmark: redis_traditional_benchmark.c benchmark_common.h
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)
	@echo "✅ Built: $@"

# SuperNode 基准测试（使用真实 SVE2）
supernode_benchmark: supernode_benchmark.c benchmark_common.h sve_compute_standalone.c sve_compute_standalone.h
	$(CC) $(CFLAGS) -o $@ supernode_benchmark.c sve_compute_standalone.c $(LDFLAGS)
	@echo "✅ Built: $@"

# 结果对比工具
compare_results: compare_results.c benchmark_common.h
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)
	@echo "✅ Built: $@"

# SuperNode 真实基准测试
supernode_real_benchmark: supernode_real_benchmark.c benchmark_common.h
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)
	@echo "✅ Built: $@"

# SuperNode 服务器
supernode_server: supernode_server.c benchmark_common.h
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)
	@echo "✅ Built: $@"

# Three-Layer Cache v4 (local memory)
three_layer_benchmark_v4: three_layer_benchmark.c ../src/three_layer_cache.c ../src/three_layer_cache.h
	$(CC) -O3 -Wall -Wno-format -march=armv8.2-a+lse -pthread -std=c11 -o $@ three_layer_benchmark.c ../src/three_layer_cache.c -lm -lpthread
	@echo "✅ Built: $@"

# Three-Layer Cache v5 (UB memory backend, cache-only)
# NOTE: requires Ascend Toolkit, not in default `make all`
three_layer_benchmark_ub: three_layer_benchmark_ub.c ../src/three_layer_cache_ub.c ../src/three_layer_cache_ub.h ../src/sve2_gemm.h
	. /usr/local/Ascend/ascend-toolkit/set_env.sh 2>/dev/null; \
	$(CC) -O3 -Wall -Wno-format -march=armv8.2-a+sve -pthread -std=c11 \
		-I/usr/local/Ascend/ascend-toolkit/latest/include \
		-L/usr/local/Ascend/ascend-toolkit/latest/lib64 \
		-Wl,-rpath,/usr/local/Ascend/ascend-toolkit/latest/lib64 \
		-o $@ three_layer_benchmark_ub.c ../src/three_layer_cache_ub.c \
		-lm -lpthread -lascendcl -lacl_cblas
	@echo "✅ Built: $@ (v8 UB + SVE2 Optimized + NPU GEMM)"

# GEMM v7-vs-v8 comparison tool
gemm_compare: gemm_compare.c
	$(CC) -O3 -Wall -Wno-format -march=armv8.2-a+sve -pthread -std=c11 -o $@ $< -lm -lpthread
	@echo "✅ Built: $@"

# SuperNode 优化版基准测试
supernode_benchmark_optimized: supernode_benchmark_optimized.c benchmark_common.h sve_compute_standalone_optimized.c sve_compute_standalone.h
	$(CC) $(CFLAGS) -o $@ supernode_benchmark_optimized.c sve_compute_standalone_optimized.c $(LDFLAGS)
	@echo "✅ Built: $@"

# TLC Aeron 传输基准测试
tlc_aeron_bench: tlc_aeron_bench.c ../src/aeron_ipc.h
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "✅ Built: $@"

# TLC FC 传输基准测试
tlc_fc_bench: tlc_fc_bench.c ../src/aeron_ipc.h
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "✅ Built: $@"

# TLC KCP 传输基准测试
tlc_kcp_bench: tlc_kcp_bench.c ../src/kcp_lite.h
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "✅ Built: $@"

# TLC Raw TCP 传输基准测试
tlc_raw_bench: tlc_raw_bench.c
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -o $@ $< -pthread -lm
	@echo "✅ Built: $@"

# TLC Transport 层基准测试
tlc_transport_bench: tlc_transport_bench.c ../src/tlc_transport.h
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "✅ Built: $@"

# TLC 全传输对比基准测试
tlc_all_transport_bench: tlc_all_transport_bench.c ../src/aeron_ipc.h ../src/tlc_transport.h
	$(CC) -O3 -Wall -Wextra -Wno-unused-function -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "✅ Built: $@"

# TLC 客户端基准测试（依赖 hiredis）
tlc_client_bench: tlc_client_bench.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)
	@echo "✅ Built: $@"

# TLC v10 传输基准测试
tlc_v10_bench: tlc_v10_bench.c ../src/aeron_ipc.h
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "✅ Built: $@"

# TLC v13 传输基准测试
tlc_v13_bench: tlc_v13_bench.c ../src/aeron_ipc.h
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "✅ Built: $@"

# TLC v14 传输基准测试
tlc_v14_bench: tlc_v14_bench.c ../src/aeron_ipc.h
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "✅ Built: $@"

# Ring buffer unit test
ring_buffer_ut: ring_buffer_ut.c test_runtime_shim.c ../src/ring_buffer.c
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=gnu11 -DTEST_RUNTIME_WITH_SERVER \
		-I../src -I../deps/hiredis -I../deps/linenoise -I../deps/lua/src \
		-I../deps/hdr_histogram -I../deps/fpconv -I../deps/fast_float -I../deps/xxhash \
		-o $@ test_runtime_shim.c $< ../src/ring_buffer.c -pthread -lm
	@echo "✅ Built: $@"

ring_buffer_batch_bench: ring_buffer_batch_bench.c test_runtime_shim.c ../src/ring_buffer.c ../src/proxy_batch_bucket.c ../src/proxy_flush_executor.c ../src/batch_latency_trace.c
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=gnu11 -DTEST_RUNTIME_WITH_SERVER \
		-I../src -I../deps/hiredis -I../deps/linenoise -I../deps/lua/src \
		-I../deps/hdr_histogram -I../deps/fpconv -I../deps/fast_float -I../deps/xxhash \
		-o $@ test_runtime_shim.c $< ../src/ring_buffer.c ../src/proxy_batch_bucket.c ../src/proxy_flush_executor.c ../src/batch_latency_trace.c -pthread -lm
	@echo "✅ Built: $@"

ring_buffer_compare_bench: ring_buffer_compare_bench.c test_runtime_shim.c ../src/ring_buffer.c ../src/proxy_batch_bucket.c ../src/proxy_flush_executor.c ../src/batch_latency_trace.c
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=gnu11 -DTEST_RUNTIME_WITH_SERVER \
		-I../src -I../deps/hiredis -I../deps/linenoise -I../deps/lua/src \
		-I../deps/hdr_histogram -I../deps/fpconv -I../deps/fast_float -I../deps/xxhash \
		-o $@ test_runtime_shim.c $< ../src/ring_buffer.c ../src/proxy_batch_bucket.c ../src/proxy_flush_executor.c ../src/batch_latency_trace.c -pthread -lm
	@echo "✅ Built: $@"

proxy_components_ut: proxy_components_ut.c test_runtime_shim.c ../src/consistent_hash.c ../src/proxy_router.c ../src/proxy_flush_scheduler.c ../src/proxy_batch_bucket.c ../src/proxy_flush_executor.c ../src/ring_buffer.c ../src/batch_latency_trace.c
	$(CC) -O3 -Wall -Wextra -pthread -std=gnu11 -DTEST_RUNTIME_WITH_SERVER \
		-I../src -I../deps/hiredis -I../deps/linenoise -I../deps/lua/src \
		-I../deps/hdr_histogram -I../deps/fpconv -I../deps/fast_float -I../deps/xxhash \
		-o $@ \
		test_runtime_shim.c \
		proxy_components_ut.c \
		../src/consistent_hash.c \
		../src/proxy_router.c \
		../src/proxy_flush_scheduler.c \
		../src/proxy_batch_bucket.c \
		../src/proxy_flush_executor.c \
		../src/ring_buffer.c \
		../src/batch_latency_trace.c \
		-lpthread -lm
	@echo "✅ Built: $@"

proxy_scheduler_ut: proxy_scheduler_ut.c test_runtime_shim.c ../src/ring_buffer_mgr.c
	$(CC) -O3 -Wall -Wextra -pthread -std=gnu11 -DTEST_RUNTIME_WITH_SERVER \
		-I../src -I../deps/hiredis -I../deps/linenoise -I../deps/lua/src \
		-I../deps/hdr_histogram -I../deps/fpconv -I../deps/fast_float -I../deps/xxhash \
		-o $@ test_runtime_shim.c $< ../src/ring_buffer_mgr.c -lpthread -lm
	@echo "✅ Built: $@"

proxy_aggregator_ut: proxy_aggregator_ut.c test_runtime_shim.c
	$(CC) -O3 -Wall -Wextra -pthread -std=gnu11 -DTEST_RUNTIME_WITH_SERVER \
		-I../src -I../deps/hiredis -I../deps/linenoise -I../deps/lua/src \
		-I../deps/hdr_histogram -I../deps/fpconv -I../deps/fast_float -I../deps/xxhash \
		-o $@ test_runtime_shim.c proxy_aggregator_ut.c -lpthread -lm
	@echo "✅ Built: $@"

vector_proxy_completion_ut: vector_proxy_completion_ut.c test_runtime_shim.c ../src/vector_proxy_completion.c ../src/vector_proxy_request.c ../src/dict.c ../src/siphash.c ../src/mt19937-64.c
	$(CC) -O3 -Wall -Wextra -pthread -std=gnu11 -DTEST_RUNTIME_WITH_SERVER \
		-I../src -I../deps/hiredis -I../deps/linenoise -I../deps/lua/src \
		-I../deps/hdr_histogram -I../deps/fpconv -I../deps/fast_float -I../deps/xxhash \
		-o $@ \
		test_runtime_shim.c \
		vector_proxy_completion_ut.c \
		../src/vector_proxy_completion.c \
		../src/vector_proxy_request.c \
		../src/dict.c \
		../src/siphash.c \
		../src/mt19937-64.c \
		-lpthread -lm
	@echo "✅ Built: $@"

monotonic_clock_bench: monotonic_clock_bench.c test_runtime_shim.c
	$(CC) -O3 -Wall -Wextra -pthread -std=gnu11 -DTEST_RUNTIME_WITH_SERVER \
		-I../src -I../deps/hiredis -I../deps/linenoise -I../deps/lua/src \
		-I../deps/hdr_histogram -I../deps/fpconv -I../deps/fast_float -I../deps/xxhash \
		-o $@ test_runtime_shim.c $< -lpthread -lm
	@echo "✅ Built: $@"

# Scatter/Gather vs Baseline benchmark
# 独立模块，不依赖 server.h
# 编译: make scatter_gather_bench
# 可选: make scatter_gather_bench USE_SVE=yes (鲲鹏 920)
SVE_BENCH_CFLAGS = -O3 -Wall -march=native -pthread -std=gnu11 -I../src
SVE_BENCH_SRCS   = scatter_gather_bench.c ../src/sve_operation.c

ifeq ($(USE_SVE),yes)
    SVE_BENCH_CFLAGS += -DUSE_SVE -march=armv8.2-a+sve
endif

scatter_gather_bench: $(SVE_BENCH_SRCS) ../src/sve_operation.h ../src/sve_config.h
	$(CC) $(SVE_BENCH_CFLAGS) -o $@ $(SVE_BENCH_SRCS) -lm -lpthread
	@echo "✅ Built: $@"

.PHONY: scatter_gather_bench

# SVE row copy comparison benchmark
# 编译: make sve_row_copy_compare USE_SVE=yes
SVE_ROW_COPY_CFLAGS = -O3 -Wall -Wextra -march=native -pthread -std=gnu11

ifeq ($(USE_SVE),yes)
    SVE_ROW_COPY_CFLAGS += -DUSE_SVE -march=armv8.2-a+sve
endif

sve_row_copy_compare: sve_row_copy_compare.c
ifeq ($(USE_SVE),yes)
	$(CC) $(SVE_ROW_COPY_CFLAGS) -o $@ $< -lm -lpthread
	@echo "✅ Built: $@"
else
	@echo "❌ sve_row_copy_compare requires USE_SVE=yes"
	@exit 1
endif

.PHONY: sve_row_copy_compare

# UB client 跨节点验证工具
# 编译: make ub_client_ut
# 可选: make ub_client_ut USE_CC_MODE=yes USE_SVE=yes
UB_UT_CFLAGS = -O3 -Wall -march=native -std=gnu11 -I../src -I../deps/libobmm -DUB_CLIENT_STANDALONE
UB_UT_LIBS   = -lm
UB_UT_SRCS   = ub_client_ut.c ../src/ub_client.c

ifeq ($(USE_CC_MODE),yes)
    UB_UT_CFLAGS += -DUSE_CC_MODE
    UB_UT_LIBS   += ../deps/libobmm/libobmm_ownership.a
endif
ifeq ($(USE_SVE),yes)
    UB_UT_CFLAGS += -DUSE_SVE -march=armv8.2-a+sve
endif

ub_client_ut: test_runtime_shim.c $(UB_UT_SRCS)
	$(CC) $(UB_UT_CFLAGS) -o $@ test_runtime_shim.c $(UB_UT_SRCS) $(UB_UT_LIBS)
	@echo "✅ Built: $@"

.PHONY: ub_client_ut

# SVE cross-embedding gather benchmark on UB.MEM
# make sve_gather_ub_bench USE_SVE=yes [USE_CC_MODE=yes]
SVE_GATHER_CFLAGS = -O3 -Wall -march=native -pthread -std=gnu11 \
    -I../src -I../deps/libobmm -DSVE_OP_STANDALONE -DUB_CLIENT_STANDALONE
SVE_GATHER_SRCS = sve_gather_ub_bench.c ../src/sve_operation.c ../src/ub_client.c
SVE_GATHER_LIBS = -lm -lpthread

ifeq ($(USE_CC_MODE),yes)
    SVE_GATHER_CFLAGS += -DUSE_CC_MODE
    SVE_GATHER_LIBS += ../deps/libobmm/libobmm_ownership.a
endif
ifeq ($(USE_SVE),yes)
    SVE_GATHER_CFLAGS += -DUSE_SVE -march=armv8.2-a+sve
endif

sve_gather_ub_bench: test_runtime_shim.c $(SVE_GATHER_SRCS)
	$(CC) $(SVE_GATHER_CFLAGS) -o $@ test_runtime_shim.c $(SVE_GATHER_SRCS) $(SVE_GATHER_LIBS)
	@echo "✅ Built: $@"

.PHONY: sve_gather_ub_bench

# 清理
clean:
	rm -f $(TARGETS) gemm_compare ub_client_ut scatter_gather_bench sve_row_copy_compare sve_gather_ub_bench
	rm -f *.o
	rm -f results/*.txt
	@echo "✅ Cleaned build artifacts"

# 创建结果目录
results:
	mkdir -p results

# 运行快速测试（1M 查询）
test: all results
	@echo ""
	@echo "Running quick test (1M queries)..."
	@echo ""
	./redis_traditional_benchmark --queries 1000000 --threads 8 > results/redis_quick.txt
	./supernode_benchmark --queries 1000000 --threads 8 > results/supernode_quick.txt
	./compare_results results/redis_quick.txt results/supernode_quick.txt
	@echo ""
	@echo "✅ Quick test completed!"
	@echo ""

# 运行完整测试（10M 查询）
benchmark: all results
	@echo ""
	@echo "Running full benchmark (10M queries)..."
	@echo ""
	./redis_traditional_benchmark --queries 10000000 --threads 16 > results/redis_full.txt
	./supernode_benchmark --queries 10000000 --threads 16 > results/supernode_full.txt
	./compare_results results/redis_full.txt results/supernode_full.txt | tee results/comparison_report.txt
	@echo ""
	@echo "✅ Full benchmark completed!"
	@echo "📊 Results saved to results/"
	@echo ""

# 运行压力测试（100M 查询）
stress: all results
	@echo ""
	@echo "Running stress test (100M queries)..."
	@echo ""
	./redis_traditional_benchmark --queries 100000000 --threads 32 > results/redis_stress.txt
	./supernode_benchmark --queries 100000000 --threads 32 > results/supernode_stress.txt
	./compare_results results/redis_stress.txt results/supernode_stress.txt | tee results/stress_report.txt
	@echo ""
	@echo "✅ Stress test completed!"
	@echo ""

# 帮助信息
help:
	@echo "Redis UB+SVE Benchmark Suite"
	@echo ""
	@echo "Targets:"
	@echo "  make              - Build all benchmarks"
	@echo "  make test         - Run quick test (1M queries)"
	@echo "  make benchmark    - Run full benchmark (10M queries)"
	@echo "  make stress       - Run stress test (100M queries)"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make help         - Show this help"
	@echo ""
	@echo "Individual benchmarks:"
	@echo "  ./redis_traditional_benchmark [options]"
	@echo "  ./supernode_benchmark [options]"
	@echo "  ./supernode_benchmark_optimized [options]"
	@echo "  ./compare_results <redis_output> <supernode_output>"
	@echo ""
	@echo "TLC transport benchmarks:"
	@echo "  ./tlc_aeron_bench / tlc_fc_bench / tlc_kcp_bench / tlc_raw_bench"
	@echo "  ./tlc_transport_bench / tlc_all_transport_bench / tlc_client_bench"
	@echo "  ./tlc_v10_bench / tlc_v13_bench / tlc_v14_bench"
	@echo ""
	@echo "UB cross-node validation:"
	@echo "  make ub_client_ut [USE_CC_MODE=yes] [USE_SVE=yes]"
	@echo ""
	@echo "Scatter/Gather benchmark:"
	@echo "  make scatter_gather_bench [USE_SVE=yes]"
	@echo "  ./scatter_gather_bench [--embeddings N] [--iters N] [--warmup N]"
	@echo ""
	@echo "SVE row-copy compare:"
	@echo "  make sve_row_copy_compare USE_SVE=yes"
	@echo "  ./sve_row_copy_compare"
	@echo ""
	@echo "SVE cross-embedding gather UB benchmark:"
	@echo "  make sve_gather_ub_bench USE_SVE=yes [USE_CC_MODE=yes]"
	@echo "  ./sve_gather_ub_bench --shm-memid <id> --shm-size 8G --vector-dimension 300"
	@echo "  ./sve_gather_ub_bench --mock-local --verify"
	@echo "  ./ub_client_ut selftest"
	@echo "  ./ub_client_ut write-fixture --shm-memid <id> --shm-size <N> --vector-dimension 128 --fill-rows 1024"
	@echo "  ./ub_client_ut gather --shm-memid <id> --shm-size <N> --vector-dimension 128 --table-name ut_vectors --gather-indices 0,1,42 --verify"
	@echo ""
	@echo "Options:"
	@echo "  --queries N    Number of queries"
	@echo "  --threads N    Number of threads"
	@echo "  --servers N    Number of servers (Redis)"
	@echo "  --supernodes N Number of supernodes (SuperNode)"
	@echo "  --help         Show help"
	@echo ""

.PHONY: all clean test benchmark stress help results
