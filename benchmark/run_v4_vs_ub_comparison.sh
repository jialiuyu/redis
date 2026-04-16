#!/bin/bash
# v4 vs v5(UB) Performance Comparison
# Run both benchmarks with identical parameters and compare

set -e

QUERIES=${1:-2000000}
THREADS=${2:-8}

echo "=============================================="
echo "  v4 (Local Memory) vs v5 (UB Memory) 对比"
echo "  Queries: $QUERIES  Threads: $THREADS"
echo "=============================================="

cd "$(dirname "$0")/.."

# Build both
echo ""
echo ">>> Building v4 (local memory)..."
gcc -O3 -Wall -Wno-format -march=armv8.2-a+lse -pthread -std=c11 \
    -o benchmark/three_layer_benchmark_v4 \
    benchmark/three_layer_benchmark.c src/three_layer_cache.c \
    -lm -lpthread

echo ">>> Building v5 (UB memory)..."
gcc -O3 -Wall -Wno-format -march=armv8.2-a+lse -pthread -std=c11 \
    -o benchmark/three_layer_benchmark_ub \
    benchmark/three_layer_benchmark_ub.c src/three_layer_cache_ub.c \
    -lm -lpthread

echo ""
echo ">>> Running v4 benchmark..."
echo "----------------------------------------------"
./benchmark/three_layer_benchmark_v4 --queries $QUERIES --threads $THREADS 2>&1 | tee /tmp/v4_result.txt

echo ""
echo ">>> Running v5 (UB) benchmark..."
echo "----------------------------------------------"
./benchmark/three_layer_benchmark_ub --queries $QUERIES --threads $THREADS 2>&1 | tee /tmp/ub_result.txt

# Extract key metrics
echo ""
echo "=============================================="
echo "  性能对比摘要"
echo "=============================================="

v4_qps=$(grep "Throughput:" /tmp/v4_result.txt | head -1 | awk '{print $2}')
ub_qps=$(grep "Throughput:" /tmp/ub_result.txt | head -1 | awk '{print $2}')
v4_lat=$(grep "Avg latency:" /tmp/v4_result.txt | head -1 | awk '{print $3}')
ub_lat=$(grep "Avg latency:" /tmp/ub_result.txt | head -1 | awk '{print $3}')
v4_time=$(grep "Total time:" /tmp/v4_result.txt | head -1 | awk '{print $3}')
ub_time=$(grep "Total time:" /tmp/ub_result.txt | head -1 | awk '{print $3}')

echo ""
printf "%-25s %15s %15s\n" "Metric" "v4 (Local)" "v5 (UB)"
printf "%-25s %15s %15s\n" "-------------------------" "---------------" "---------------"
printf "%-25s %15s %15s\n" "Read-Heavy QPS" "$v4_qps" "$ub_qps"
printf "%-25s %15s %15s\n" "Read-Heavy Latency (μs)" "$v4_lat" "$ub_lat"
printf "%-25s %15s %15s\n" "Read-Heavy Time (s)" "$v4_time" "$ub_time"

local_pct=$(grep "Local accesses:" /tmp/ub_result.txt | head -1 | grep -oP '\(\K[0-9.]+')
echo ""
echo "UB 本地访问率: ${local_pct}%"
echo ""
echo "✅ 对比完成"
