#!/usr/bin/env bash
# Phase 5 跨节点 aeron benchmark。HW01 server，HW02 client。
#
# 用法：
#   NUM_KEYS=100000 bash run_aeron_cross_node.sh run 64 4 30 32
#       # 端到端：起 server → prefill $NUM_KEYS keys (UDS) → VEMB_GET
#       # t=T c=C pipeline=PIPE dur=DURATION → 解析 deref → 关 server
#
#   bash run_aeron_cross_node.sh server         # 只起 server + prefill（保留 server）
#   bash run_aeron_cross_node.sh smoke          # HW02 跑 t=1 c=1 100 ops VADD smoke
#   bash run_aeron_cross_node.sh bench T C D [P] # 单次 bench（server 已起）
#
# prefill 与 read 的 key 范围都是 [1, NUM_KEYS]，prefix=item:
# 产物：benchmark/results/cross_node_aeron/<run_id>/

set -uo pipefail

HPC=${HPC:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)}
REDIS=$HPC/src/redis-server
MANIFEST=$HPC/examples/vemb_v16_warm_regions_111.yaml

SERVER_HOST="${SERVER_HOST:-HW01}"
SERVER_IP="${SERVER_IP:-192.168.1.111}"
SERVER_PORT="${SERVER_PORT:-6395}"
CLIENT_HOST="${CLIENT_HOST:-HW02}"
REMOTE_HPC="${REMOTE_HPC:-/root/gqs/codespace/UnifiedBus/hpc-redis}"
REMOTE_MEMTIER="${REMOTE_MEMTIER:-/root/gqs/codespace/UnifiedBus/hpc-redis/memtier_benchmark}"
RUN_ID="${RUN_ID:-cross_node_aeron_$(date +%Y%m%d_%H%M%S)}"
RESULTS="$HPC/benchmark/results/cross_node_aeron/${RUN_ID}"
MUTUAL_RESULTS="$REMOTE_HPC/benchmark/results/cross_node_aeron/${RUN_ID}"

DIM=${DIM:-300}
NUM_KEYS=${NUM_KEYS:-10000}
MAX_VECTORS=${MAX_VECTORS:-131072}
SERVER_MASK=${SERVER_MASK:-"0-47"}
CLIENT_MASK=${CLIENT_MASK:-"96-191"}

CMD="${1:-help}"
mkdir -p "$RESULTS"

# ── server start (run on HW01 via ssh) ────────────────────────────────
start_server() {
    local mode=$1  # local (loopback baseline) or cross (for cross-node)
    pkill -9 -f "redis-server.*:${SERVER_PORT}" 2>/dev/null || true
    rm -f /tmp/vemb_v16.sock
    sleep 0.5

    echo "=== start server on $SERVER_HOST mask=$SERVER_MASK pio=21 snw=21 ==="
    taskset -c "$SERVER_MASK" $REDIS \
        --port $SERVER_PORT --bind 0.0.0.0 --protected-mode no \
        --vemb-v16-enabled yes --vemb-v16-dim $DIM \
        --vemb-v16-cross-node-aeron yes \
        --vemb-v16-max-vectors $MAX_VECTORS \
        --vemb-v16-warm-regions-manifest "$MANIFEST" \
        --vemb-v16-reset-warm-regions yes \
        --vemb-v16-proxy-io-threads 21 \
        --vemb-v16-supernode-workers 21 \
        --daemonize yes --pidfile /tmp/vemb_cross.pid \
        --logfile "$RESULTS/server.log" --loglevel notice \
        >/dev/null 2>&1

    for _ in $(seq 1 50); do
        [ -S /tmp/vemb_v16.sock ] && break
        sleep 0.2
    done
    for _ in $(seq 1 50); do
        ss -tln | grep -q ":${SERVER_PORT} " && break
        sleep 0.2
    done
    if [ ! -S /tmp/vemb_v16.sock ] || ! ss -tln | grep -q ":${SERVER_PORT} "; then
        echo "FAIL: server did not come up"
        tail -30 "$RESULTS/server.log" 2>/dev/null
        exit 1
    fi
    echo "server up: pid=$(cat /tmp/vemb_cross.pid 2>/dev/null) socket=/tmp/vemb_v16.sock tcp=$SERVER_PORT"
}

# ── prefill via local UDS on HW01 (warmup) ────────────────────────────
prefill_local() {
    local MEMTIER_LOCAL=${MEMTIER_LOCAL:-/root/gqs/codespace/UnifiedBus/hpc-redis/memtier_benchmark/memtier_benchmark}
    echo "=== prefill $NUM_KEYS keys via local UDS ==="
    taskset -c "$CLIENT_MASK" $MEMTIER_LOCAL --protocol vemb_v16 \
        --vemb-v16-transport=aeron --vemb-v16-dim $DIM \
        -s 127.0.0.1 -p $SERVER_PORT \
        -t 1 -c 1 -n $NUM_KEYS --pipeline=32 \
        --ratio=1:0 --key-pattern=S:S \
        --key-prefix=item: --key-minimum=1 --key-maximum=$NUM_KEYS \
        2>&1 | grep -E "^Totals" | head -1
}

# ── cross-node bench (run on HW02 via ssh) ────────────────────────────
run_bench_remote() {
    local T=$1 C=$2 DURATION=$3 PIPELINE=$4
    ssh "$CLIENT_HOST" "mkdir -p '${MUTUAL_RESULTS}'"
    echo "=== cross-node bench on $CLIENT_HOST: t=$T c=$C pipeline=$PIPELINE dur=${DURATION}s ==="
    # scp the bench plan output back locally as it runs
    ssh "$CLIENT_HOST" "cd '${REMOTE_MEMTIER}' && \
        taskset -c '${CLIENT_MASK}' ./memtier_benchmark \
            --protocol vemb_v16 \
            --vemb-v16-transport=aeron-cross-node \
            --vemb-v16-endpoints=${SERVER_IP}:${SERVER_PORT} \
            --vemb-v16-dim $DIM \
            -t $T -c $C --pipeline=$PIPELINE \
            --ratio=0:1 --key-pattern=R:R \
            --key-prefix=item: --key-minimum=1 --key-maximum=$NUM_KEYS \
            --test-time=$DURATION --hide-histogram" \
        2>"$RESULTS/t${T}c${C}.stderr" \
        | tee "$RESULTS/t${T}c${C}.stdout" \
        | grep -E "^(Totals|RUN|aeron|===)" | head -20
}

# ── parse memtier output: Totals + handle_deref[ok=N fail=M] ──────────
parse_memtier_out() {
    local out=$1
    local line=$(grep "^Totals" "$out")
    local ops=$(echo "$line" | awk '{print $2}')
    local lat_avg=$(echo "$line" | awk '{print $5}')
    local lat_p50=$(echo "$line" | awk '{print $6}')
    local lat_p99=$(echo "$line" | awk '{print $7}')
    local wire=$(echo "$line" | awk '{print $9}')
    local deref_ok=$(grep 'handle_deref' "$out" | \
        sed -E 's/.*handle_deref\[ok=([0-9]+) fail=([0-9]+).*/\1/' | \
        awk '{s+=$1} END{print s+0}')
    local deref_fail=$(grep 'handle_deref' "$out" | \
        sed -E 's/.*handle_deref\[ok=([0-9]+) fail=([0-9]+).*/\2/' | \
        awk '{s+=$1} END{print s+0}')
    echo "  keys=$NUM_KEYS  t=$RUN_T  c=$RUN_C  pipeline=$RUN_PIPELINE  dur=${RUN_DURATION}s"
    echo "  ops/sec=$ops  avg=${lat_avg}ms  p50=${lat_p50}ms  p99=${lat_p99}ms  wire=${wire}KB/s"
    echo "  deref_ok=$deref_ok  deref_fail=$deref_fail"
    # summary tsv
    {
        printf 'phase\tt\tc\tpipeline\tkeys\tdur\tops_sec\tavg_ms\tp50_ms\tp99_ms\twire_KB_sec\tderef_ok\tderef_fail\n'
        printf 'read\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$RUN_T" "$RUN_C" "$RUN_PIPELINE" "$NUM_KEYS" "$RUN_DURATION" \
            "$ops" "$lat_avg" "$lat_p50" "$lat_p99" "$wire" "$deref_ok" "$deref_fail"
    } > "$RESULTS/summary.tsv"
}

# ── shutdown server + remote memtier ──────────────────────────────────
shutdown_all() {
    pkill -9 -f "redis-server.*:${SERVER_PORT}" 2>/dev/null || true
    ssh "$CLIENT_HOST" "pkill -9 -f memtier_benchmark" 2>/dev/null || true
}

# ── main dispatch ─────────────────────────────────────────────────────
case "$CMD" in
    server)
        start_server cross
        echo "[cross] server ready, prefill via local UDS…"
        prefill_local
        echo "[cross] prefill done. Now run:"
        echo "  bash run_aeron_cross_node.sh smoke"
        echo "  bash run_aeron_cross_node.sh bench 4 1 30"
        # keep server up until killed
        echo "[cross] server log: $RESULTS/server.log (tail -f to watch)"
        echo "[cross] to tear down: pkill -9 -f 'redis-server.*:${SERVER_PORT}'"
        ;;

    smoke)
        # 1 thread, 1 client, 100 ops, no time limit
        ssh "$CLIENT_HOST" "mkdir -p '${MUTUAL_RESULTS}'"
        echo "=== cross-node smoke: t=1 c=1 n=100 pipeline=1 ==="
        ssh "$CLIENT_HOST" "cd '${REMOTE_MEMTIER}' && \
            taskset -c '${CLIENT_MASK}' ./memtier_benchmark \
                --protocol vemb_v16 \
                --vemb-v16-transport=aeron-cross-node \
                --vemb-v16-endpoints=${SERVER_IP}:${SERVER_PORT} \
                --vemb-v16-dim $DIM \
                -t 1 -c 1 --pipeline=1 -n 100 \
                --ratio=1:0 --key-pattern=S:S \
                --key-prefix=item: --key-minimum=1 --key-maximum=100 \
                --hide-histogram" \
            2>"$RESULTS/smoke.stderr" \
            | tee "$RESULTS/smoke.stdout"
        echo "--- stderr tail ---"
        tail -20 "$RESULTS/smoke.stderr"
        ;;

    bench)
        T=${2:-4}; C=${3:-1}; DURATION=${4:-30}; PIPELINE=${5:-32}
        run_bench_remote "$T" "$C" "$DURATION" "$PIPELINE"
        ;;

    run)
        # End-to-end: server + prefill + single VEMB_GET bench + deref
        # parse + shutdown. Caller controls NUM_KEYS / T / C / duration.
        RUN_T=${2:-64}; RUN_C=${3:-4}
        RUN_DURATION=${4:-30}; RUN_PIPELINE=${5:-32}

        start_server cross || { shutdown_all; exit 1; }
        prefill_local
        sleep 2  # drain in-flight prefill ops

        echo "=== cross-node VEMB_GET t=$RUN_T c=$RUN_C pipeline=$RUN_PIPELINE dur=${RUN_DURATION}s ==="
        ssh "$CLIENT_HOST" "mkdir -p '${MUTUAL_RESULTS}'"
        ssh "$CLIENT_HOST" "cd '${REMOTE_MEMTIER}' && \
            taskset -c '${CLIENT_MASK}' ./memtier_benchmark \
                --protocol vemb_v16 \
                --vemb-v16-transport=aeron-cross-node \
                --vemb-v16-endpoints=${SERVER_IP}:${SERVER_PORT} \
                --vemb-v16-dim $DIM \
                -t $RUN_T -c $RUN_C --pipeline=$RUN_PIPELINE \
                --ratio=0:1 --key-pattern=R:R \
                --key-prefix=item: --key-minimum=1 --key-maximum=$NUM_KEYS \
                --test-time=$RUN_DURATION --hide-histogram" \
            > "$RESULTS/t${RUN_T}c${RUN_C}.out" 2>&1

        echo
        echo "=== result ==="
        parse_memtier_out "$RESULTS/t${RUN_T}c${RUN_C}.out"

        shutdown_all
        echo "[done] results in $RESULTS"
        ;;

    *)
        echo "usage: $0 {run|server|smoke|bench} [args]"
        echo "  run [T=64] [C=4] [dur=30] [pipe=32]"
        echo "         end-to-end: server + prefill(\$NUM_KEYS) + VEMB_GET + deref + shutdown"
        echo "  server                start server + prefill, keep running for manual bench"
        echo "  smoke                 t=1 c=1 n=100 VADD smoke"
        echo "  bench T C dur [pipe]  single bench (server must already be running)"
        echo
        echo "env: NUM_KEYS=\$keys DIM=300 SERVER_IP=192.168.1.111 SERVER_PORT=6395"
        echo "     SERVER_HOST=HW01 CLIENT_HOST=HW02 SERVER_MASK=0-47 CLIENT_MASK=96-191"
        exit 1
        ;;
esac
