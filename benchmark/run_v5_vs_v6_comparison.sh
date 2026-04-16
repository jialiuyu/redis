#!/bin/bash
# v5 (UB cache-only) vs v6 (UB + SVE2 Fused Compute) Comparison
set -e

QUERIES=${1:-2000000}
THREADS=${2:-8}

echo "=============================================="
echo "  v5 (UB Cache) vs v6 (UB + SVE2 Compute)"
echo "  Queries: $QUERIES  Threads: $THREADS"
echo "=============================================="

cd "$(dirname "$0")/.."

echo ""
echo ">>> Building v5 (UB cache-only, original three_layer_cache)..."
gcc -O3 -Wall -Wno-format -march=armv8.2-a+sve -pthread -std=c11 \
    -o benchmark/bench_v5 \
    benchmark/three_layer_benchmark.c src/three_layer_cache.c \
    -lm -lpthread

echo ">>> Building v6 (UB + SVE2 fused compute)..."
gcc -O3 -Wall -Wno-format -march=armv8.2-a+sve -pthread -std=c11 \
    -o benchmark/bench_v6 \
    benchmark/three_layer_benchmark_ub.c src/three_layer_cache_ub.c \
    -lm -lpthread

echo ""
echo "=============================================="
echo "  Running v5 (cache-only baseline)..."
echo "=============================================="
./benchmark/bench_v5 --queries $QUERIES --threads $THREADS 2>&1 | tee /tmp/v5_result.txt

echo ""
echo "=============================================="
echo "  Running v6 (UB + SVE2 fused compute)..."
echo "=============================================="
./benchmark/bench_v6 --queries $QUERIES --threads $THREADS 2>&1 | tee /tmp/v6_result.txt

# Extract metrics
echo ""
echo "=============================================="
echo "  性能对比摘要 (v5 vs v6)"
echo "=============================================="

v5_qps=$(grep "Throughput:" /tmp/v5_result.txt | head -1 | grep -oP '[\d.]+(?= M QPS)')
v6_qps=$(grep "Throughput:" /tmp/v6_result.txt | head -1 | grep -oP '[\d.]+(?= M QPS)')
v5_lat=$(grep "Avg latency:" /tmp/v5_result.txt | head -1 | awk '{print $3}')
v6_lat=$(grep "Avg latency:" /tmp/v6_result.txt | head -1 | awk '{print $3}')

sim_rate=$(grep "Embeddings/sec:" /tmp/v6_result.txt | head -1 | awk '{print $2}')
sim_gflops=$(grep "GFLOPS" /tmp/v6_result.txt | head -1 | awk '{print $2}')
gemm_gflops=$(grep "GFLOPS" /tmp/v6_result.txt | tail -1 | awk '{print $2}')
gather_bw=$(grep "Bandwidth:" /tmp/v6_result.txt | awk '{print $2}')
combined_qps=$(grep "Throughput:" /tmp/v6_result.txt | grep "M ops" | awk '{print $2}')

echo ""
printf "%-35s %12s %12s\n" "Metric" "v5" "v6"
printf "%-35s %12s %12s\n" "-----------------------------------" "------------" "------------"
printf "%-35s %10s M %10s M\n" "Cache Read-Heavy QPS" "$v5_qps" "$v6_qps"
printf "%-35s %12s %12s\n" "Cache Read-Heavy Latency (μs)" "$v5_lat" "$v6_lat"
echo ""
echo "v6 SVE2 Compute (new capabilities):"
printf "  %-33s %s M emb/s\n" "Gather Load bandwidth" "$gather_bw GB/s"
printf "  %-33s %s M emb/s, %s GFLOPS\n" "Fused Similarity" "$sim_rate" "$sim_gflops"
printf "  %-33s %s GFLOPS\n" "Fused GEMM" "$gemm_gflops"
printf "  %-33s %s M ops/s\n" "Combined workload" "$combined_qps"

local_pct=$(grep "Local accesses:" /tmp/v6_result.txt | head -1 | grep -oP '\(\K[0-9.]+')
echo ""
echo "UB 本地访问率: ${local_pct}%"
echo ""
echo "✅ 对比完成"
