#!/bin/bash
# run_l0_size_compare.sh — 对比 baseline vs L0(8) vs L0(16) vs L0(32)
# Usage: ./run_l0_size_compare.sh [ops] [threads] [max_key] [rw]
set -e

OPS=${1:-6000000}
THREADS=${2:-8}
MAX_KEY=${3:-1100000}
FILL=$MAX_KEY
RW=${4:-100}
SOCK="/tmp/tlc_hash_bench.sock"
CSV="benchmark/results/l0_size_$(date +%Y%m%d_%H%M%S).csv"
SRC_DIR="output/src"
BENCH="./output/benchmark/tlc_hash_bench"
if [ ! -x "$BENCH" ]; then
    BENCH="./benchmark/tlc_hash_bench"
fi
BENCH_TIMEOUT=300

mkdir -p benchmark/results

# 定义要测试的组合: hash_name:evict_name:filename_suffix
COMBOS=(
    "v1:blind:"
    "v1:clock:"
    "v2fixed:blind:"
    "v2fixed:clock:"
    "crc32:blind:_crc"
    "crc32:clock:_crc"
    "crc32fib:blind:_cf"
    "crc32fib:clock:_cf"
)

# L0 size 列表
L0_SIZES=("" "_l0" "_l0_16" "_l0_32")
L0_LABELS=("BASELINE" "L0_8" "L0_16" "L0_32")

echo "=== L0 Size Compare: ${OPS} ops, ${THREADS} threads, max-key ${MAX_KEY}, ${RW}R/${100-RW}W ==="
echo "csv: ${CSV}"

# CSV header
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
    server=$1
    hash_label=$2
    evict_label=$3
    l0_label=$4

    if [ ! -x "$server" ]; then
        echo "  SKIP (no binary: ${server})"
        return 0
    fi

    kill_server
    echo "  Starting ${server}..."
    $server > /dev/null 2>&1 &
    spid=$!
    sleep 2

    if ! kill -0 $spid 2>/dev/null; then
        echo "  ERROR: server failed to start"
        return 1
    fi

    echo "  Running bench: ${hash_label}+${evict_label}+${l0_label}"
    timeout ${BENCH_TIMEOUT} ${BENCH} \
        --ops ${OPS} --threads ${THREADS} --max-key ${MAX_KEY} --fill ${FILL} --rw ${RW} \
        --eviction "${evict_label}" --csv-tag "${l0_name}" --zipf --csv "${CSV}" 2>&1
    rc=$?
    if [ $rc -eq 124 ]; then
        echo "  TIMEOUT"
    fi

    kill_server
    sleep 1
}

for combo in "${COMBOS[@]}"; do
    IFS=':' read -r hash_name evict_name suffix <<< "$combo"
    base_label="${hash_name}_${evict_name}${suffix}"

    echo ""
    echo "=== ${base_label} ==="

    for i in "${!L0_SIZES[@]}"; do
        size_suffix="${L0_SIZES[$i]}"
        l0_name="${L0_LABELS[$i]}"
        
        if [ -z "$size_suffix" ]; then
            server="${SRC_DIR}/tlc_hash_fc_server_${base_label}_stats"
        else
            server="${SRC_DIR}/tlc_hash_fc_server_${base_label}_stats${size_suffix}"
        fi
        
        run_single "$server" "$hash_name" "$evict_name" "$l0_name"
    done
done

echo ""
echo "=== All done. CSV: ${CSV} ==="
