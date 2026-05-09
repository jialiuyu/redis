#!/bin/bash
# run_hash_bench.sh — Full hash×eviction×placement benchmark
# Usage: ./run_hash_bench.sh [ops] [threads] [max_key] [rw]
set -e

OPS=${1:-6000000}
THREADS=${2:-8}
MAX_KEY=${3:-1100000}
FILL=$MAX_KEY
RW=${4:-100}
SOCK="/tmp/tlc_hash_bench.sock"
V16_SOCK="/tmp/tlc_v16.sock"
V16_SHM="tlc_v16"
CSV="benchmark/results/hash_bench_$(date +%Y%m%d_%H%M%S).csv"
SRC_DIR="output/src"
V16_SERVER="./output/src/tlc_v16_server"
BENCH="./benchmark/tlc_hash_bench"
if [ ! -x "$BENCH" ]; then
    BENCH="./benchmark/tlc_hash_bench"
if [ ! -x "$BENCH" ]; then
    BENCH="./output/benchmark/tlc_hash_bench"
fi
fi
# Timeout for each bench run (seconds)
BENCH_TIMEOUT=300
INCLUDE_V16=${INCLUDE_V16:-0}

HASHES=(v1 v2fixed v2full64 crc32 crc32fib)
EVICTIONS=(blind clock)
PLACEMENTS=("baseline:" "hopscotch:_hs")

declare -A HASH_SUFFIX=(
    [v1]="" [v2fixed]="" [v2full64]=""
    [crc32]="_crc" [crc32fib]="_cf"
)

mkdir -p benchmark/results

echo "=== Hash Bench: ${OPS} ops, ${THREADS} threads, max-key ${MAX_KEY}, ${RW}R/${100-RW}W ==="
echo "csv: ${CSV}"
echo "hash,eviction,ops,threads,max_key,qps,mops,avg_ns,get_miss,hits_1probe,hits_2probe,hits_3probe,hits_4probe,misses,warm_1probe,warm_miss,hot_util%,warm_util%,l0_hits,l0_misses,l0_hit_pct" > "$CSV"

kill_bench() {
    ps -eo pid=,args= | awk '/tlc_hash_bench/ && $0 !~ /awk/ {print $1}' | xargs -r kill -9 2>/dev/null || true
}

kill_server() {
    ps -eo pid=,args= | awk '/tlc_hash_fc_server_|tlc_v16_server/ && $0 !~ /awk/ {print $1}' | xargs -r kill -9 2>/dev/null || true
    sleep 1
    rm -f "$SOCK" "$V16_SOCK"
    # Clean up shm
    for f in /dev/shm/tlc_hb_*; do rm -f "$f" 2>/dev/null; done
    for f in /dev/shm/tlc_v16_*; do rm -f "$f" 2>/dev/null; done
}

cleanup() {
    kill_bench
    kill_server
}

trap cleanup EXIT INT TERM HUP

run_combo() {
    local hash=$1
    local evict=$2
    local placement_name=$3
    local placement_suffix=$4
    local hsfx="${HASH_SUFFIX[$hash]}"
    local label="${hash}_${evict}${hsfx}${placement_suffix}"

    echo ""
    echo "=== ${hash}:${evict}:${placement_name} ==="

    local server="${SRC_DIR}/tlc_hash_fc_server_${label}_stats"
    if [ ! -x "$server" ]; then
        echo "  SKIP (no binary: ${server})"
        return 0
    fi
    kill_server
    echo "  Starting ${server}..."
    ${server} > /dev/null 2>&1 &
    local spid=$!
    sleep 2

    if ! kill -0 $spid 2>/dev/null; then
        echo "  ERROR: server failed to start"
        return 1
    fi

    echo "  Running bench (${OPS} ops, timeout=${BENCH_TIMEOUT}s)..."
    timeout ${BENCH_TIMEOUT} ${BENCH} --ops ${OPS} --threads ${THREADS} --max-key ${MAX_KEY} --fill ${FILL} --rw ${RW} --zipf --eviction "${evict}" --csv "${CSV}" 2>&1
    local rc=$?
    if [ $rc -eq 124 ]; then
        echo "  TIMEOUT after ${BENCH_TIMEOUT}s — killing"
    elif [ $rc -ne 0 ]; then
        echo "  BENCH exit code: $rc"
    fi

    echo "  Done ${hash}:${evict}:${placement_name}"
    kill_server
}

for hash in "${HASHES[@]}"; do
    for evict in "${EVICTIONS[@]}"; do
        for placement in "${PLACEMENTS[@]}"; do
            IFS=':' read -r placement_name placement_suffix <<< "$placement"
            run_combo "$hash" "$evict" "$placement_name" "$placement_suffix"
        done
    done
done

if [ "$INCLUDE_V16" = "1" ]; then
    echo ""
    echo "=== v16 baseline ==="
    kill_server
    echo "  Starting v16..."
    ${V16_SERVER} > /dev/null 2>&1 &
    v16pid=$!
    sleep 2
    if kill -0 $v16pid 2>/dev/null; then
        timeout ${BENCH_TIMEOUT} ${BENCH} --ops ${OPS} --threads ${THREADS} --max-key ${MAX_KEY} --fill ${FILL} --rw ${RW} --zipf --sock "$V16_SOCK" --shm-prefix "$V16_SHM" --strategy "V16_BASELINE" --skip-collision --csv "${CSV}" 2>&1
    else
        echo "  ERROR: v16 server failed to start"
    fi
    kill_server
fi

echo ""
echo "=== All done. CSV: ${CSV} ==="
