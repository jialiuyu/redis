#!/bin/bash

cd /root/gqs/codespace/UnifiedBus/memtier_benchmark

echo "========================================"
echo "  VEMB V16 memtier multi-client test"
echo "========================================"

run_test() {
    local name="$1"
    shift
    echo ""
    echo "--- $name ---"
    ./memtier_benchmark "$@" 2>&1 | awk '
        /^Gets/ || /^Totals/ {
            printf "  %-22s | OPS: %12s | Avg: %8s ms | p50: %8s ms | p99: %8s ms | p99.9: %8s ms | BW: %10s KB/s\n",
                type, $2, $5, $6, $7, $8, $9
        }
    ' type="$name"
}

# Prefill: write vectors first so queries can hit
run_test "prefill_64_keys" \
    --protocol vemb_v16 --vemb-v16-dim 300 \
    -s 127.0.0.1 -p 6391 --test-time 5 -c 1 -t 1 --ratio=1:0 \
    --key-prefix="item:" --key-minimum=1 --key-maximum=64

run_test "1_conn_baseline" \
    --protocol vemb_v16 --vemb-v16-dim 300 \
    -s 127.0.0.1 -p 6391 --test-time 5 -c 1 -t 1 --ratio=0:1 \
    --key-prefix="item:" --key-minimum=1 --key-maximum=64

run_test "8_conn_pipeline16" \
    --protocol vemb_v16 --vemb-v16-dim 300 \
    -s 127.0.0.1 -p 6391 --test-time 5 -c 1 -t 8 --ratio=0:1 --pipeline=16 \
    --key-prefix="item:" --key-minimum=1 --key-maximum=64

run_test "16_conn_pipeline32" \
    --protocol vemb_v16 --vemb-v16-dim 300 \
    -s 127.0.0.1 -p 6391 --test-time 5 -c 1 -t 16 --ratio=0:1 --pipeline=32 \
    --key-prefix="item:" --key-minimum=1 --key-maximum=64

echo ""
echo "Done"
