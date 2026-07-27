#!/bin/bash
# 用途：复现 aeron transport 最优核数配置下的 memtier 基准测试
# 默认配置：server mask 0-47 (48核), pio=21 snw=21, client t64 c4 pipeline=32 test-time=60
# 期望：~50M+ ops/sec, handle_deref[ok=N fail=0]
#
# 使用方法（所有参数都有默认值，按需覆盖）：
#   bash run_aeron_best.sh
#   TEST_TIME=60 T=64 C=4 PIPELINE=32 PIO=21 SNW=21 bash run_aeron_best.sh
#   ssh HW01 'TEST_TIME=60 bash /root/gqs/codespace/UnifiedBus/test_hpc/run_aeron_best.sh'
#
# 可调参数（环境变量）：
#   TEST_TIME     bench 持续秒数         (默认 60)
#   T C           client -t / -c         (默认 64 / 4)
#   PIPELINE      每 channel in-flight   (默认 32)
#   NUM_KEYS      prefill key 数         (默认 10000)
#   MAX_VECTORS   server vector 容量上限 (默认 131072=128K；NUM_KEYS 不能超过这个)
#   DIM           vector 维度            (默认 300)
#   SERVER_MASK   server taskset         (默认 "0-47")
#   PIO           vemb-v16 proxy IO 线程数 (默认 21)
#   SNW           vemb-v16 supernode worker 数 (默认 21)
#   CLIENT_MASK   client taskset         (默认 "96-191")
#   SERVER_HOST   client 连接的 server IP (默认 127.0.0.1)
#   PORT          server 端口            (默认 6395)
#   ROLE          both|server|client     (默认 both)

set -uo pipefail

HPC=${HPC:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)}
REDIS=$HPC/src/redis-server
MEMTIER=$HPC/memtier_benchmark/memtier_benchmark
MANIFEST=$HPC/examples/vemb_v16_warm_regions_111.yaml

PORT=${PORT:-6395}
SERVER_HOST=${SERVER_HOST:-127.0.0.1}
ROLE=${ROLE:-both}
DIM=${DIM:-300}
NUM_KEYS=${NUM_KEYS:-10000}
MAX_VECTORS=${MAX_VECTORS:-131072}
PIPELINE=${PIPELINE:-32}
TEST_TIME=${TEST_TIME:-60}
KEY_PREFIX=${KEY_PREFIX:-"item:"}
PIO=${PIO:-21}
SNW=${SNW:-21}

# Sanity: NUM_KEYS 不能超过 MAX_VECTORS（server 软上限），也不能超过物理 warm region 容量（~894K）
if [ "$NUM_KEYS" -gt "$MAX_VECTORS" ]; then
    echo "ERROR: NUM_KEYS=$NUM_KEYS > MAX_VECTORS=$MAX_VECTORS"
    echo "       set MAX_VECTORS >= NUM_KEYS (max physical ~894K @ dim=300)"
    exit 2
fi

SERVER_MASK=${SERVER_MASK:-"0-47"}      # 48 核最优（pio=21 snw=21 + main + helpers 共约 45~50）
CLIENT_MASK=${CLIENT_MASK:-"96-191"}    # 跑在 node1，和 server 隔离
T=${T:-64}; C=${C:-4}                    # client -t / -c

SOCKET=/tmp/vemb_v16.sock
PIDFILE=/tmp/vemb_best.pid
SERVER_LOG=/tmp/vemb_best_server.log
BENCH_OUT=/tmp/vemb_bench.stdout
BENCH_ERR=/tmp/vemb_bench.stderr

# ── CPU jiffies: 把 server PID 所有线程的 utime+stime 求和 ──
# /proc/PID/task/TID/stat 字段：... utime(14) stime(15) ...
# 用 sed 去掉 "pid (comm)" 前缀（comm 可能含空格），剩下从 state 起，$12=utime $13=stime
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

# ── cleanup ──
cleanup() {
    [ "$ROLE" = "client" ] && return
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
sleep 0.5

# ── 启动 server ──
if [ "$ROLE" = "both" ] || [ "$ROLE" = "server" ]; then
    echo "=== start server: mask=$SERVER_MASK pio=$PIO snw=$SNW ==="
    taskset -c "$SERVER_MASK" $REDIS \
        --port $PORT --bind 0.0.0.0 --protected-mode no \
        --vemb-v16-enabled yes --vemb-v16-dim $DIM \
        --vemb-v16-max-vectors $MAX_VECTORS \
        --vemb-v16-warm-regions-manifest "$MANIFEST" \
        --vemb-v16-reset-warm-regions yes \
        --vemb-v16-proxy-io-threads $PIO \
        --vemb-v16-supernode-workers $SNW \
        --daemonize yes --pidfile $PIDFILE --logfile "$SERVER_LOG" --loglevel notice \
        >/dev/null 2>&1

    # ── 等 UDS listener 就绪 ──
    for _ in $(seq 1 50); do
        [ -S "$SOCKET" ] && break
        sleep 0.2
    done
    if [ ! -S "$SOCKET" ]; then
        echo "FAIL: UDS socket $SOCKET not ready"
        echo "--- server log tail ---"
        tail -30 "$SERVER_LOG" 2>/dev/null
        exit 1
    fi
    # 同时确认 TCP 端口监听
    for _ in $(seq 1 50); do
        ss -tln | grep -q ":$PORT " && break
        sleep 0.2
    done
    echo "server up: pid=$(cat $PIDFILE) socket=$SOCKET tcp=$SERVER_HOST:$PORT"
    sleep 1
fi

if [ "$ROLE" = "server" ]; then
    echo "server-only mode: leaving server running at $SERVER_HOST:$PORT"
    trap - EXIT
    exit 0
fi

if [ "$ROLE" != "both" ] && [ "$ROLE" != "client" ]; then
    echo "ERROR: ROLE must be both, server, or client (got $ROLE)"
    exit 2
fi

# ── prefill 10K keys (S:S 顺序写入) ──
echo ""
echo "=== prefill: $NUM_KEYS keys, dim=$DIM server=$SERVER_HOST:$PORT ==="
taskset -c "$CLIENT_MASK" $MEMTIER --protocol vemb_v16 --vemb-v16-transport=aeron \
    --vemb-v16-dim $DIM -s $SERVER_HOST -p $PORT \
    -t 1 -c 1 -n $NUM_KEYS --pipeline=32 \
    --ratio=1:0 --key-pattern=S:S \
    --key-prefix=$KEY_PREFIX --key-minimum=1 --key-maximum=$NUM_KEYS \
    2>&1 | grep -E '^(Totals|==============)' | head -5

# ── bench ──
echo ""
echo "=== bench: mask=$CLIENT_MASK t=$T c=$C pipeline=$PIPELINE test-time=$TEST_TIME server=$SERVER_HOST:$PORT ==="
SRV_PID=$(cat $PIDFILE 2>/dev/null)
J0=0
[ -n "$SRV_PID" ] && J0=$(get_cpu_jiffies "$SRV_PID")

taskset -c "$CLIENT_MASK" $MEMTIER --protocol vemb_v16 --vemb-v16-transport=aeron \
    --vemb-v16-dim $DIM -s $SERVER_HOST -p $PORT \
    -t $T -c $C --pipeline=$PIPELINE \
    --ratio=0:1 --key-pattern=R:R \
    --key-prefix=$KEY_PREFIX --key-minimum=1 --key-maximum=$NUM_KEYS \
    --test-time=$TEST_TIME \
    >$BENCH_OUT 2>$BENCH_ERR

J1=0
[ -n "$SRV_PID" ] && J1=$(get_cpu_jiffies "$SRV_PID")

# ── 汇总 ──
echo ""
echo "=== summary ==="
# bench Totals 行：ops/sec hits/sec misses/sec avg p50 p95 p99 KB/sec
BENCH_TOTALS=$(grep "^Totals" "$BENCH_OUT" | tail -1)
OPS_SEC=$(echo "$BENCH_TOTALS" | awk '{print $2}')
HITS_SEC=$(echo "$BENCH_TOTALS" | awk '{print $3}')
MISS_SEC=$(echo "$BENCH_TOTALS" | awk '{print $4}')
P50=$(echo "$BENCH_TOTALS" | awk '{print $6}')
P99=$(echo "$BENCH_TOTALS" | awk '{print $8}')
KBSEC=$(echo "$BENCH_TOTALS" | awk '{print $9}')

# CPU 核数 = jiffies 差 / 100 / duration
CPU_CORES=$(awk -v d=$((J1 - J0)) -v t=$TEST_TIME -v pid="$SRV_PID" 'BEGIN{ if(pid==""||d<0||t<=0) print "NA"; else printf "%.2f", d/100.0/t }')
OPS_PER_CORE=$(awk -v o="$OPS_SEC" -v c="$CPU_CORES" 'BEGIN{ if(c=="NA"||c==0) print "NA"; else printf "%.0f", o/c }')
GBSEC=$(awk -v k="$KBSEC" 'BEGIN{ printf "%.2f", k/1024/1024 }')

# handle_deref ok/fail 汇总（across all workers）
DERF_OK=$(grep -oE "handle_deref\[ok=[0-9]+ fail=[0-9]+\]" "$BENCH_ERR" | \
          awk -F"ok=" '{split($2,a," "); sum+=a[1]} END {print sum+0}')
DERF_FAIL=$(grep -oE "handle_deref\[ok=[0-9]+ fail=[0-9]+\]" "$BENCH_ERR" | \
            awk -F"fail=" '{split($2,a,"]"); sum+=a[1]} END {print sum+0}')
# bench 阶段 status 分布（NOT_FOUND 等）
STATUS_NF=$(grep -oE "status\[ok=[0-9]+ nf=[0-9]+" "$BENCH_ERR" | \
            awk -F"nf=" '{sum+=$2} END {print sum+0}')

printf '  ops/sec         : %s\n' "$OPS_SEC"
printf '  hits/sec        : %s\n' "$HITS_SEC"
printf '  misses/sec      : %s  (status nf total: %s)\n' "$MISS_SEC" "$STATUS_NF"
printf '  p50 / p99       : %s / %s ms\n' "$P50" "$P99"
printf '  wire throughput : %s GB/sec\n' "$GBSEC"
printf '  server CPU cores: %s  (over %ss)\n' "$CPU_CORES" "$TEST_TIME"
printf '  ops/core/sec    : %s\n' "$OPS_PER_CORE"
printf '  handle_deref    : ok=%s fail=%s\n' "$DERF_OK" "$DERF_FAIL"

echo ""
echo "=== done ==="
echo "bench stdout: $BENCH_OUT"
echo "bench stderr: $BENCH_ERR"
echo "server log  : $SERVER_LOG"
