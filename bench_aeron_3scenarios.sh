#!/bin/bash
# bench_transport_matrix.sh — 4 拓扑 × 2 transport = 8 组 memtier 基准
#
# 用法: bash bench_transport_matrix.sh <manifest_name> <transport> [num_keys] [test_time]
#   manifest_name: pure_local_10k | mix_7to1_10k | mix_1to1_10k | allremote_10k
#   transport:     tcp | aeron
#   num_keys:      默认 10000
#   test_time:     默认 30
#
# 配置: server pio=21 snw=21 mask=0-47 / client t=64 c=4 pipeline=32 mask=96-191
set -uo pipefail

HPC=/root/gqs/codespace/UnifiedBus/hpc-redis
REDIS=$HPC/src/redis-server
MEMTIER=$HPC/memtier_benchmark/memtier_benchmark

MANIFEST_NAME=${1:-mix_7to1_10k}
TRANSPORT=${2:-aeron}
NUM_KEYS=${3:-10000}
TEST_TIME=${4:-30}
MANIFEST=$HPC/examples/vemb_v16_warm_regions_${MANIFEST_NAME}.yaml

PORT=6395
DIM=300
MAX_VECTORS=131072
PIPELINE=32
KEY_PREFIX="item:"

SERVER_MASK="0-47"
CLIENT_MASK="96-191"
T=64; C=4

SOCKET=/tmp/vemb_v16.sock
PIDFILE=/tmp/vemb_bench_${MANIFEST_NAME}_${TRANSPORT}.pid
SERVER_LOG=/tmp/vemb_bench_${MANIFEST_NAME}_${TRANSPORT}.server.log
BENCH_OUT=/tmp/vemb_bench_${MANIFEST_NAME}_${TRANSPORT}.stdout
BENCH_ERR=/tmp/vemb_bench_${MANIFEST_NAME}_${TRANSPORT}.stderr

get_cpu_jiffies() {
    local pid=$1 sum=0 f rest
    for f in /proc/$pid/task/*/stat; do
        [ -r "$f" ] || continue
        rest=$(sed 's/.*)//' "$f")
        set -- $rest
        sum=$(( sum + ${12:-0} + ${13:-0} ))
    done
    echo "$sum"
}

cleanup() {
    if [ -f "$PIDFILE" ]; then
        local p=$(cat "$PIDFILE" 2>/dev/null)
        [ -n "$p" ] && { kill "$p" 2>/dev/null; sleep 0.5; kill -9 "$p" 2>/dev/null; }
        rm -f "$PIDFILE"
    fi
    pkill -9 -f "redis-server.*:$PORT " 2>/dev/null || true
    rm -f "$SOCKET"
}
trap cleanup EXIT
cleanup
sleep 1

echo "============================================================"
echo "  Manifest : $MANIFEST_NAME"
echo "  Transport: $TRANSPORT"
echo "  Keys: $NUM_KEYS  Dim: $DIM  Test time: ${TEST_TIME}s"
echo "  Server: pio=21 snw=21 mask=$SERVER_MASK"
echo "  Client: t=$T c=$C pipeline=$PIPELINE mask=$CLIENT_MASK"
echo "============================================================"

# ── 启动 server ──
echo ">>> starting server..."
taskset -c "$SERVER_MASK" $REDIS \
    --port $PORT --bind 0.0.0.0 --protected-mode no \
    --vemb-v16-enabled yes --vemb-v16-dim $DIM \
    --vemb-v16-max-vectors $MAX_VECTORS \
    --vemb-v16-warm-regions-manifest "$MANIFEST" \
    --vemb-v16-reset-warm-regions yes \
    --vemb-v16-proxy-io-threads 21 \
    --vemb-v16-supernode-workers 21 \
    --daemonize yes --pidfile $PIDFILE --logfile "$SERVER_LOG" --loglevel notice \
    >/dev/null 2>&1

for _ in $(seq 1 50); do [ -S "$SOCKET" ] && break; sleep 0.2; done
for _ in $(seq 1 50); do ss -tln | grep -q ":$PORT " && break; sleep 0.2; done
if ! ss -tln | grep -q ":$PORT "; then
    echo "FAIL: server port not ready"; tail -20 "$SERVER_LOG"; exit 1
fi
echo "    server up: pid=$(cat $PIDFILE)"
sleep 1

# ── prefill (TCP single-thread sequential) ──
echo ">>> prefilling $NUM_KEYS keys via TCP..."
PREFILL_OUT=$(taskset -c "$CLIENT_MASK" $MEMTIER \
    --protocol vemb_v16 --vemb-v16-transport=tcp \
    --vemb-v16-dim $DIM -s 127.0.0.1 -p $PORT \
    -t 1 -c 1 -n $NUM_KEYS --pipeline=32 \
    --ratio=1:0 --key-pattern=S:S \
    --key-prefix=$KEY_PREFIX --key-minimum=1 --key-maximum=$NUM_KEYS \
    2>&1)
PREFILL_OPS=$(echo "$PREFILL_OUT" | grep "^Totals" | tail -1 | awk '{print $2}')
echo "    prefill done: ops=$PREFILL_OPS"
sleep 1

# ── bench ──
echo ">>> bench: ${TRANSPORT} READ t=$T c=$C pipeline=$PIPELINE ${TEST_TIME}s..."
SRV_PID=$(cat $PIDFILE 2>/dev/null)
J0=$(get_cpu_jiffies "$SRV_PID")

taskset -c "$CLIENT_MASK" $MEMTIER \
    --protocol vemb_v16 --vemb-v16-transport=${TRANSPORT} \
    --vemb-v16-dim $DIM -s 127.0.0.1 -p $PORT \
    -t $T -c $C --pipeline=$PIPELINE \
    --ratio=0:1 --key-pattern=R:R \
    --key-prefix=$KEY_PREFIX --key-minimum=1 --key-maximum=$NUM_KEYS \
    --test-time=$TEST_TIME \
    >$BENCH_OUT 2>$BENCH_ERR

J1=$(get_cpu_jiffies "$SRV_PID")

# ── 汇总 ──
echo ""
echo "============================================================"
echo "  RESULT: $MANIFEST_NAME / $TRANSPORT"
echo "============================================================"
BENCH_TOTALS=$(grep "^Totals" "$BENCH_OUT" | tail -1)
OPS_SEC=$(echo "$BENCH_TOTALS" | awk '{print $2}')
HITS_SEC=$(echo "$BENCH_TOTALS" | awk '{print $3}')
MISS_SEC=$(echo "$BENCH_TOTALS" | awk '{print $4}')
P50=$(echo "$BENCH_TOTALS" | awk '{print $6}')
P99=$(echo "$BENCH_TOTALS" | awk '{print $8}')
KBSEC=$(echo "$BENCH_TOTALS" | awk '{print $9}')

CPU_CORES=$(awk -v d=$((J1 - J0)) -v t=$TEST_TIME 'BEGIN{ if(d<0||t<=0) print "NA"; else printf "%.2f", d/100.0/t }')
OPS_PER_CORE=$(awk -v o="$OPS_SEC" -v c="$CPU_CORES" 'BEGIN{ if(c=="NA"||c==0) print "NA"; else printf "%.0f", o/c }')
GBSEC=$(awk -v k="$KBSEC" 'BEGIN{ printf "%.2f", k/1024/1024 }')

DERF_OK=$(grep -oE "handle_deref\[ok=[0-9]+ fail=[0-9]+\]" "$BENCH_ERR" | \
          awk -F"ok=" '{split($2,a," "); sum+=a[1]} END {print sum+0}')
DERF_FAIL=$(grep -oE "handle_deref\[ok=[0-9]+ fail=[0-9]+\]" "$BENCH_ERR" | \
            awk -F"fail=" '{split($2,a,"]"); sum+=a[1]} END {print sum+0}')
STATUS_NF=$(grep -oE "status\[ok=[0-9]+ nf=[0-9]+" "$BENCH_ERR" | \
            awk -F"nf=" '{sum+=$2} END {print sum+0}')

printf '  ops/sec         : %s\n' "$OPS_SEC"
printf '  hits/sec        : %s\n' "$HITS_SEC"
printf '  misses/sec      : %s  (status nf: %s)\n' "$MISS_SEC" "$STATUS_NF"
printf '  p50 / p99       : %s / %s ms\n' "$P50" "$P99"
printf '  wire throughput : %s GB/sec\n' "$GBSEC"
printf '  server CPU cores: %s  (over %ss)\n' "$CPU_CORES" "$TEST_TIME"
printf '  ops/core/sec    : %s\n' "$OPS_PER_CORE"
printf '  handle_deref    : ok=%s fail=%s\n' "$DERF_OK" "$DERF_FAIL"

echo ""
echo "  stdout: $BENCH_OUT"
echo "  stderr: $BENCH_ERR"
echo "  server: $SERVER_LOG"
echo "============================================================"
