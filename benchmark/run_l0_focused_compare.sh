#!/bin/bash
# run_l0_focused_compare.sh — 聚焦对比：4 hash × 2 eviction × (baseline + L0)
# Usage: ./run_l0_focused_compare.sh [ops] [threads] [max_key] [rw]
set -e

OPS=${1:-6000000}
THREADS=${2:-8}
MAX_KEY=${3:-1100000}
FILL=$MAX_KEY
RW=${4:-80}
SOCK="/tmp/tlc_hash_bench.sock"
CSV="benchmark/results/l0_focused_$(date +%Y%m%d_%H%M%S).csv"
SRC_DIR="output/src"
BENCH="./output/benchmark/tlc_hash_bench"
if [ ! -x "$BENCH" ]; then
    BENCH="./benchmark/tlc_hash_bench"
fi
BENCH_TIMEOUT=300

mkdir -p benchmark/results

# 定义要测试的组合: hash_name:evict_name:HASH_STRATEGY:EVICTION_STRATEGY:filename_suffix
COMBOS=(
    "v1:blind:1:0:"
    "v1:clock:1:1:"
    "v2fixed:blind:3:0:"
    "v2fixed:clock:3:1:"
    "crc32:blind:6:0:_crc"
    "crc32:clock:6:1:_crc"
    "crc32fib:blind:7:0:_cf"
    "crc32fib:clock:7:1:_cf"
)

echo "=== L0 Focused Compare: ${OPS} ops, ${THREADS} threads, max-key ${MAX_KEY}, ${RW}R/${100-RW}W ==="
echo "csv: ${CSV}"

# CSV header（22 列，含 l0_size）
echo "hash,eviction,l0_size,ops,threads,max_key,qps,mops,avg_ns,get_miss,hits_1probe,hits_2probe,hits_3probe,hits_4probe,misses,warm_1probe,warm_miss,hot_util%,warm_util%,l0_hits,l0_misses,l0_hit_pct" > "$CSV"

kill_bench() {
    ps -eo pid=,args= | awk '/tlc_hash_bench/ && $0 !~ /awk/ {print $1}' | xargs -r kill -9 2>/dev/null || true
}

kill_server() {
    ps -eo pid=,args= | awk '/tlc_hash_fc_server/ && $0 !~ /awk/ {print $1}' | xargs -r kill -9 2>/dev/null || true
    sleep 1
    rm -f "$SOCK"
    for f in /dev/shm/tlc_hb_*; do rm -f "$f" 2>/dev/null; done
}

cleanup() {
    kill_bench
    kill_server
}
trap cleanup EXIT INT TERM HUP

run_single() {
    local server=$1
    local label=$2
    local evict_label=$3
    local csv_tag=$4

    if [ ! -x "$server" ]; then
        echo "  SKIP (no binary: ${server})"
        return 0
    fi

    kill_server
    echo "  Starting ${server}..."
    $server > /dev/null 2>&1 &
    local spid=$!
    sleep 2

    if ! kill -0 $spid 2>/dev/null; then
        echo "  ERROR: server failed to start"
        return 1
    fi

    echo "  Running bench: ${label}"
    timeout ${BENCH_TIMEOUT} ${BENCH} \
        --ops ${OPS} --threads ${THREADS} --max-key ${MAX_KEY} --fill ${FILL} --rw ${RW} \
        --eviction "${evict_label}" --csv-tag "${csv_tag}" --zipf --csv "${CSV}" 2>&1
    local rc=$?
    if [ $rc -eq 124 ]; then
        echo "  TIMEOUT"
    fi

    kill_server
    sleep 1
}

for combo in "${COMBOS[@]}"; do
    IFS=':' read -r hash_name evict_name hash_id evict_id suffix <<< "$combo"
    base_label="${hash_name}_${evict_name}${suffix}"

    echo ""
    echo "=== ${base_label} ==="

    # 1. 跑原版
    server_base="${SRC_DIR}/tlc_hash_fc_server_${base_label}_stats"
    run_single "$server_base" "${hash_name}+BASELINE" "$evict_name" "BASELINE"

    # 2. 跑 L0 版
    server_l0="${SRC_DIR}/tlc_hash_fc_server_${base_label}_stats_l0"
    run_single "$server_l0" "${hash_name}+BASELINE+L0" "$evict_name" "L0"
done

echo ""
echo "=== All done. CSV: ${CSV} ==="
