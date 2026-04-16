# Makefile for Redis UB+SVE Benchmark Suite
# 编译所有基准测试程序

CC = gcc
CFLAGS = -O3 -Wall -Wextra -march=native -pthread -std=c11 -I../deps/hiredis
LDFLAGS = -pthread -lm -L../deps/hiredis -lhiredis

# 目标文件
TARGETS = redis_traditional_benchmark supernode_benchmark compare_results supernode_real_benchmark supernode_server three_layer_benchmark_v4 three_layer_benchmark_ub

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

# 清理
clean:
	rm -f $(TARGETS)
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
	@echo "  ./compare_results <redis_output> <supernode_output>"
	@echo ""
	@echo "Options:"
	@echo "  --queries N    Number of queries"
	@echo "  --threads N    Number of threads"
	@echo "  --servers N    Number of servers (Redis)"
	@echo "  --supernodes N Number of supernodes (SuperNode)"
	@echo "  --help         Show help"
	@echo ""

.PHONY: all clean test benchmark stress help results
