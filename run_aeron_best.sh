#!/bin/bash
# ============================================================================
# run_aeron_best.sh
# Aeron transport (UDS+SHM) local loopback VEMB 17-config sweep
#
# 用法:
#   bash run_aeron_best.sh                          # 默认 17 档
#   TEST_TIME=10 bash run_aeron_best.sh              # smoke
#   TS="64" CS="4" PS="32" bash run_aeron_best.sh   # 单档
# ============================================================================

set -uo pipefail

HPC=${HPC:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)}
REDIS=$HPC/src/redis-server
MEMTIER=$HPC/memtier_benchmark/memtier_benchmark
MANIFEST=$HPC/examples/vemb_v16_warm_regions_111.yaml

PORT=${PORT:-6395}
DIM=${DIM:-300}
NUM_KEYS=${NUM_KEYS:-100000}
MAX_VECTORS=${MAX_VECTORS:-131072}
TEST_TIME=${TEST_TIME:-30}
KEY_PREFIX=${KEY_PREFIX:-"item:"}

SERVER_MASK=${SERVER_MASK:-"0-47"}
CLIENT_MASK=${CLIENT_MASK:-"96-191"}
HPC_PIO=${HPC_PIO:-8}
HPC_SNW=${HPC_SNW:-8}

# === 17 档配置矩阵 ===
TS_DEFAULT=(1 1 1 1  1  2  4  8  16 32 64 64 64 64 64 64 64)
CS_DEFAULT=(1 1 1 1  1  1  1  1  1  1  1  2  4  8  16 32 64)
PS_DEFAULT=(1 4 8 16 32 32 32 32 32 32 32 32 32 32 32 32 32)
TS=( ${TS:-${TS_DEFAULT[*]}} )
CS=( ${CS:-${CS_DEFAULT[*]}} )
PS=( ${PS:-${PS_DEFAULT[*]}} )
NCONFIGS=${#TS[@]}

# === 输出 ===
OUTDIR=${OUTDIR:-benchmark/results/aeron_sweep}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RAWDIR="$OUTDIR/$TIMESTAMP/raw"
TSV="$OUTDIR/$TIMESTAMP/summary.tsv"

PIDFILE=/tmp/vemb_aeron_sweep.pid
SERVER_LOG=/tmp/vemb_aeron_sweep.log
SOCKET=/tmp/vemb_v16.sock

ulimit -n 200000
mkdir -p "$RAWDIR"

log() { echo "[$(date +%H:%M:%S)] $*"; }

get_cpu_jiffies() {
    local pid=$1 sum=0 rest
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
        [ -n "$p" ] && { kill "$p" 2>/dev/null; sleep 0.3; kill -9 "$p" 2>/dev/null; }
    fi
    pkill -9 -f "redis-server.*:$PORT " 2>/dev/null || true
    rm -f "$SOCKET" "$PIDFILE"
}

# ============================================================================
trap cleanup EXIT
cleanup
sleep 0.5

if [ "$NUM_KEYS" -gt "$MAX_VECTORS" ]; then
    echo "ERROR: NUM_KEYS=$NUM_KEYS > MAX_VECTORS=$MAX_VECTORS"
    exit 2
fi

# === Start server ===
log "=== start server: mask=$SERVER_MASK pio=$HPC_PIO snw=$HPC_SNW ==="
taskset -c "$SERVER_MASK" $REDIS \
    --port $PORT --bind 0.0.0.0 --protected-mode no \
    --vemb-v16-enabled yes --vemb-v16-dim $DIM \
    --vemb-v16-max-vectors $MAX_VECTORS \
    --vemb-v16-warm-regions-manifest "$MANIFEST" \
    --vemb-v16-reset-warm-regions yes \
    --vemb-v16-proxy-io-threads $HPC_PIO \
    --vemb-v16-supernode-workers $HPC_SNW \
    --daemonize yes --pidfile "$PIDFILE" --logfile "$SERVER_LOG" --loglevel notice \
    >/dev/null 2>&1

for _ in $(seq 1 50); do
    [ -S "$SOCKET" ] && break
    sleep 0.2
done
for _ in $(seq 1 50); do
    ss -tln | grep -q ":$PORT " && break
    sleep 0.2
done
if ! ss -tln | grep -q ":$PORT "; then
    echo "FAIL: server not up"
    tail -20 "$SERVER_LOG" 2>/dev/null
    exit 1
fi
SRV_PID=$(cat "$PIDFILE" 2>/dev/null)
log "server up: pid=$SRV_PID"

# === Prefill ===
log "=== prefill: $NUM_KEYS keys, dim=$DIM ==="
taskset -c "$CLIENT_MASK" $MEMTIER --protocol vemb_v16 --vemb-v16-transport=aeron \
    --vemb-v16-dim $DIM -s 127.0.0.1 -p $PORT \
    -t 1 -c 1 -n $NUM_KEYS --pipeline=32 \
    --ratio=1:0 --key-pattern=S:S \
    --key-prefix=$KEY_PREFIX --key-minimum=1 --key-maximum=$NUM_KEYS \
    > "$RAWDIR/prefill.log" 2>&1
PREFILL_TOTALS=$(grep "^Totals" "$RAWDIR/prefill.log" | tail -1)
PREFILL_OPS=$(echo "$PREFILL_TOTALS" | awk '{print $2}')
log "prefill done: ${PREFILL_OPS:-N/A} sets/sec"

# === TSV header ===
printf "op\tserver_type\tt\tc\tpipeline\tops_sec\tavg_lat_ms\tp50_ms\tp99_ms\tkb_sec\tcores\n" > "$TSV"

# === Sweep ===
for ((idx=0; idx<NCONFIGS; idx++)); do
    t=${TS[$idx]}; c=${CS[$idx]}; p=${PS[$idx]}
    log "--- config $((idx+1))/${NCONFIGS}: t=$t c=$c pipeline=$p ---"

    J0=$(get_cpu_jiffies "$SRV_PID")

    taskset -c "$CLIENT_MASK" $MEMTIER --protocol vemb_v16 --vemb-v16-transport=aeron \
        --vemb-v16-dim $DIM -s 127.0.0.1 -p $PORT \
        -t $t -c $c --pipeline=$p \
        --ratio=0:1 --key-pattern=R:R \
        --key-prefix=$KEY_PREFIX --key-minimum=1 --key-maximum=$NUM_KEYS \
        --test-time=$TEST_TIME \
        > "$RAWDIR/t${t}_c${c}_p${p}.log" 2>&1

    J1=$(get_cpu_jiffies "$SRV_PID")
    CPU_CORES=$(awk -v d=$((J1 - J0)) -v s=$TEST_TIME 'BEGIN{printf "%.2f", d/100.0/s}')

    totals=$(grep "^Totals" "$RAWDIR/t${t}_c${c}_p${p}.log" | tail -1)
    read ops avg p50 p99 kb < <(
        echo "$totals" | awk '{if(NF>=9) printf "%s %s %s %s %s", $2,$5,$6,$8,$9; else printf "0 NA NA NA NA"}'
    )

    printf "VEMB\taeron_local\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
        "$t" "$c" "$p" "$ops" "$avg" "$p50" "$p99" "$kb" "$CPU_CORES" >> "$TSV"
    log "  => ops/s=$ops  avg=${avg}ms  p50=${p50}ms  p99=${p99}ms  cores=$CPU_CORES"
done

log "=== DONE ==="
log "TSV : $TSV"
log "raw : $RAWDIR/*.log"
echo "----- summary -----"
column -t -s $'\t' "$TSV"
