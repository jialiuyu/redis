#!/bin/bash
set -e

OPS=6000000
THREADS=8
MAX_KEY=1100000
SOCK="/tmp/tlc_hash_bench.sock"
CSV="benchmark/results/fifo_vs_lru_$(date +%Y%m%d_%H%M%S).csv"
BENCH="./benchmark/tlc_hash_bench"

echo "hash,eviction,l0_size,ops,threads,max_key,qps,mops,avg_ns,get_miss,hits_1probe,hits_2probe,hits_3probe,hits_4probe,misses,warm_1probe,warm_miss,hot_util%,warm_util%,l0_hits,l0_misses,l0_hit_pct" > "$CSV"

kill_all() {
    pkill -9 -f tlc_hash_fc_server 2>/dev/null || true
    pkill -9 -f tlc_hash_bench 2>/dev/null || true
    sleep 2
    rm -f "$SOCK"
    rm -f /dev/shm/tlc_hb_* 2>/dev/null || true
}

run_one() {
    local server=$1
    local hash=$2
    local ev=$3
    local tag=$4
    
    kill_all
    echo "=== START $hash/$ev $tag ===" | tee -a /tmp/fifo_vs_lru.log
    $server > /dev/null 2>&1 &
    local spid=$!
    sleep 3
    
    if ! kill -0 $spid 2>/dev/null; then
        echo "ERROR: server failed to start" | tee -a /tmp/fifo_vs_lru.log
        return 1
    fi
    
    $BENCH --ops $OPS --threads $THREADS --max-key $MAX_KEY --fill $MAX_KEY --rw 100 \
        --zipf --zipf-s 1.2 --server-name "$hash" --eviction "$ev" --csv-tag "$tag" --csv "$CSV" 2>&1 | tee -a /tmp/fifo_vs_lru.log
    
    kill_all
    echo "=== DONE $hash/$ev $tag ===" | tee -a /tmp/fifo_vs_lru.log
    sleep 2
}

COMBOS=(
    "v1:V1_LINEAR:blind:"
    "v1:V1_LINEAR:clock:"
    "v2fixed:V2_FIXED:blind:"
    "v2fixed:V2_FIXED:clock:"
    "crc32:V4_CRC32:blind:_crc"
    "crc32:V4_CRC32:clock:_crc"
    "crc32fib:V5_CRC32FIB:blind:_cf"
    "crc32fib:V5_CRC32FIB:clock:_cf"
)

for combo in "${COMBOS[@]}"; do
    IFS=':' read -r base_name hash_name ev_name suffix <<< "$combo"
    base="${base_name}_${ev_name}${suffix}"
    
    run_one "./output/src/tlc_hash_fc_server_${base}_stats"       "$hash_name" "$ev_name" "BASELINE"
    run_one "./output/src/tlc_hash_fc_server_${base}_stats_l0_fifo" "$hash_name" "$ev_name" "FIFO"
    run_one "./output/src/tlc_hash_fc_server_${base}_stats_l0"    "$hash_name" "$ev_name" "LRU"
done

echo "ALL DONE. CSV: $CSV" | tee -a /tmp/fifo_vs_lru.log
cat "$CSV" | tee -a /tmp/fifo_vs_lru.log
