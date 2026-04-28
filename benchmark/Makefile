# Makefile for Redis UB+SVE Benchmark Suite
# 编译所有基准测试程序

CC = gcc
CFLAGS = -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../deps/hiredis
LDFLAGS = -pthread -lm -L../deps/hiredis -lhiredis

# Output directory
OUT_DIR := ../output/benchmark

# 目标文件 (all output to OUT_DIR, include new targets from HEAD)
TARGETS = $(addprefix $(OUT_DIR)/,redis_traditional_benchmark supernode_benchmark compare_results supernode_real_benchmark supernode_server three_layer_benchmark_v4 tlc_aeron_bench tlc_fc_bench tlc_kcp_bench tlc_raw_bench tlc_transport_bench tlc_all_transport_bench tlc_client_bench tlc_v10_bench tlc_v13_bench tlc_v14_bench tlc_v16_bench tlc_hash_bench)

# 所有目标
all: $(TARGETS)
	@echo ""
	@echo "All benchmarks compiled successfully!"
	@echo ""
	@echo "Run benchmarks:"
	@echo "  $(OUT_DIR)/redis_traditional_benchmark --help"
	@echo "  $(OUT_DIR)/supernode_benchmark --help"
	@echo "  ./run_full_benchmark.sh"
	@echo ""

# 传统 Redis 基准测试
$(OUT_DIR)/redis_traditional_benchmark: redis_traditional_benchmark.c benchmark_common.h | $(OUT_DIR)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)
	@echo "Built: $@"

# SuperNode 基准测试（使用真实 SVE2）
$(OUT_DIR)/supernode_benchmark: supernode_benchmark.c benchmark_common.h sve_compute_standalone.c sve_compute_standalone.h | $(OUT_DIR)
	$(CC) $(CFLAGS) -o $@ supernode_benchmark.c sve_compute_standalone.c $(LDFLAGS)
	@echo "Built: $@"

# 结果对比工具
$(OUT_DIR)/compare_results: compare_results.c benchmark_common.h | $(OUT_DIR)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)
	@echo "Built: $@"

# SuperNode 真实基准测试
$(OUT_DIR)/supernode_real_benchmark: supernode_real_benchmark.c benchmark_common.h | $(OUT_DIR)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)
	@echo "Built: $@"

# SuperNode 服务器
$(OUT_DIR)/supernode_server: supernode_server.c benchmark_common.h | $(OUT_DIR)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)
	@echo "Built: $@"

# Three-Layer Cache v4 (local memory)
$(OUT_DIR)/three_layer_benchmark_v4: three_layer_benchmark.c ../src/three_layer_cache.c ../src/three_layer_cache.h | $(OUT_DIR)
	$(CC) -O3 -Wall -Wno-format -march=armv8.2-a+lse -pthread -std=c11 -o $@ three_layer_benchmark.c ../src/three_layer_cache.c -lm -lpthread
	@echo "Built: $@"

# Three-Layer Cache v5 (UB memory backend, cache-only)
# NOTE: requires Ascend Toolkit, not in default `make all`
$(OUT_DIR)/three_layer_benchmark_ub: three_layer_benchmark_ub.c ../src/three_layer_cache_ub.c ../src/three_layer_cache_ub.h ../src/sve2_gemm.h | $(OUT_DIR)
	. /usr/local/Ascend/ascend-toolkit/set_env.sh 2>/dev/null; \
	$(CC) -O3 -Wall -Wno-format -march=armv8.2-a+sve -pthread -std=c11 \
		-I/usr/local/Ascend/ascend-toolkit/latest/include \
		-L/usr/local/Ascend/ascend-toolkit/latest/lib64 \
		-Wl,-rpath,/usr/local/Ascend/ascend-toolkit/latest/lib64 \
		-o $@ three_layer_benchmark_ub.c ../src/three_layer_cache_ub.c \
		-lm -lpthread -lascendcl -lacl_cblas
	@echo "Built: $@ (v8 UB + SVE2 Optimized + NPU GEMM)"

# GEMM v7-vs-v8 comparison tool
$(OUT_DIR)/gemm_compare: gemm_compare.c | $(OUT_DIR)
	$(CC) -O3 -Wall -Wno-format -march=armv8.2-a+sve -pthread -std=c11 -o $@ $< -lm -lpthread
	@echo "Built: $@"

# SuperNode 优化版基准测试
$(OUT_DIR)/supernode_benchmark_optimized: supernode_benchmark_optimized.c benchmark_common.h sve_compute_standalone_optimized.c sve_compute_standalone.h | $(OUT_DIR)
	$(CC) $(CFLAGS) -o $@ supernode_benchmark_optimized.c sve_compute_standalone_optimized.c $(LDFLAGS)
	@echo "Built: $@"

# TLC Aeron 传输基准测试
$(OUT_DIR)/tlc_aeron_bench: tlc_aeron_bench.c ../src/aeron_ipc.h | $(OUT_DIR)
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "Built: $@"

# TLC FC 传输基准测试
$(OUT_DIR)/tlc_fc_bench: tlc_fc_bench.c ../src/aeron_ipc.h | $(OUT_DIR)
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "Built: $@"

# TLC KCP 传输基准测试
$(OUT_DIR)/tlc_kcp_bench: tlc_kcp_bench.c ../src/kcp_lite.h | $(OUT_DIR)
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "Built: $@"

# TLC Raw TCP 传输基准测试
$(OUT_DIR)/tlc_raw_bench: tlc_raw_bench.c | $(OUT_DIR)
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -o $@ $< -pthread -lm
	@echo "Built: $@"

# TLC Transport 层基准测试
$(OUT_DIR)/tlc_transport_bench: tlc_transport_bench.c ../src/tlc_transport.h | $(OUT_DIR)
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "Built: $@"

# TLC 全传输对比基准测试
$(OUT_DIR)/tlc_all_transport_bench: tlc_all_transport_bench.c ../src/aeron_ipc.h ../src/tlc_transport.h | $(OUT_DIR)
	$(CC) -O3 -Wall -Wextra -Wno-unused-function -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "Built: $@"

# TLC 客户端基准测试（依赖 hiredis）
$(OUT_DIR)/tlc_client_bench: tlc_client_bench.c | $(OUT_DIR)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)
	@echo "Built: $@"

# TLC v10 传输基准测试
$(OUT_DIR)/tlc_v10_bench: tlc_v10_bench.c ../src/aeron_ipc.h | $(OUT_DIR)
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "Built: $@"

# TLC v13 传输基准测试
$(OUT_DIR)/tlc_v13_bench: tlc_v13_bench.c ../src/aeron_ipc.h | $(OUT_DIR)
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "Built: $@"

# TLC v14 传输基准测试
$(OUT_DIR)/tlc_v14_bench: tlc_v14_bench.c ../src/aeron_ipc.h | $(OUT_DIR)
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "Built: $@"

# TLC v16 传输基准测试
$(OUT_DIR)/tlc_v16_bench: tlc_v16_bench.c ../src/aeron_ipc.h | $(OUT_DIR)
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "Built: $@"

# Create output directory
$(OUT_DIR):
	mkdir -p $(OUT_DIR)

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

ub_client_ut: $(UB_UT_SRCS)
	$(CC) $(UB_UT_CFLAGS) -o $@ $(UB_UT_SRCS) $(UB_UT_LIBS)
	@echo "Built: $@"

.PHONY: ub_client_ut

# 清理
clean:
	rm -rf $(OUT_DIR)
	rm -f results/*.txt
	@echo "Cleaned build artifacts"

# 创建结果目录
results:
	mkdir -p results

# 运行快速测试（1M 查询）
test: all results
	@echo ""
	@echo "Running quick test (1M queries)..."
	@echo ""
	$(OUT_DIR)/redis_traditional_benchmark --queries 1000000 --threads 8 > results/redis_quick.txt
	$(OUT_DIR)/supernode_benchmark --queries 1000000 --threads 8 > results/supernode_quick.txt
	$(OUT_DIR)/compare_results results/redis_quick.txt results/supernode_quick.txt
	@echo ""
	@echo "Quick test completed!"
	@echo ""

# 运行完整测试（10M 查询）
benchmark: all results
	@echo ""
	@echo "Running full benchmark (10M queries)..."
	@echo ""
	$(OUT_DIR)/redis_traditional_benchmark --queries 10000000 --threads 16 > results/redis_full.txt
	$(OUT_DIR)/supernode_benchmark --queries 10000000 --threads 16 > results/supernode_full.txt
	$(OUT_DIR)/compare_results results/redis_full.txt results/supernode_full.txt | tee results/comparison_report.txt
	@echo ""
	@echo "Full benchmark completed!"
	@echo ""

# 运行压力测试（100M 查询）
stress: all results
	@echo ""
	@echo "Running stress test (100M queries)..."
	@echo ""
	$(OUT_DIR)/redis_traditional_benchmark --queries 100000000 --threads 32 > results/redis_stress.txt
	$(OUT_DIR)/supernode_benchmark --queries 100000000 --threads 32 > results/supernode_stress.txt
	$(OUT_DIR)/compare_results results/redis_stress.txt results/supernode_stress.txt | tee results/stress_report.txt
	@echo ""
	@echo "Stress test completed!"
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
	@echo "  $(OUT_DIR)/redis_traditional_benchmark [options]"
	@echo "  $(OUT_DIR)/supernode_benchmark [options]"
	@echo "  $(OUT_DIR)/supernode_benchmark_optimized [options]"
	@echo "  $(OUT_DIR)/compare_results <redis_output> <supernode_output>"
	@echo ""
	@echo "TLC transport benchmarks:"
	@echo "  $(OUT_DIR)/tlc_aeron_bench / tlc_fc_bench / tlc_kcp_bench / tlc_raw_bench"
	@echo "  $(OUT_DIR)/tlc_transport_bench / tlc_all_transport_bench / tlc_client_bench"
	@echo "  $(OUT_DIR)/tlc_v10_bench / tlc_v13_bench / tlc_v14_bench / tlc_v16_bench"
	@echo ""
	@echo "UB cross-node validation:"
	@echo "  make ub_client_ut [USE_CC_MODE=yes] [USE_SVE=yes]"
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

# TLC Hash Strategy Benchmark Client
$(OUT_DIR)/tlc_hash_bench: tlc_hash_bench.c ../src/aeron_ipc.h | $(OUT_DIR)
	$(CC) -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../src -o $@ $< -pthread -lm
	@echo "Built: $@"

hash-bench: $(OUT_DIR)/tlc_hash_bench
	@echo "Hash bench client built. Run with:"
	@echo "  $(OUT_DIR)/tlc_hash_bench --ops 2000000 --threads 8 --max-key 1100000"
