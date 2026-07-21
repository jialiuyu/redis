#!/bin/bash
# 注意：不能用 set -e —— vanilla redis VEMB 高并发偶发 connection reset，
# 一档失败会触发 cleanup 杀全部 server。用 || true 兜底。
#
# ============================================================================
# Multi-Instance Redis 8.6.3 Baseline (核数拉平 hpc-redis)
#
#   N 个 Redis 实例，各绑 IO_THREADS 核，总核数 ≈ hpc-redis 的 46 核
#   数据分片：每实例持有不交叠的 key 范围
#   N 个 memtier 并行打，各连各的端口，汇总吞吐
#
# 拓扑（同 run_cross_node_redis_baseline.sh）：
#   JUMP=HW01 / SERVER=HW01 / CLIENT=HW02(192.168.90.112) / data=192.168.1.111
#
# 用法:
#   NUM_INSTANCES=12 IO_THREADS=4 bash run_multi_instance_redis_baseline.sh
# ============================================================================

JUMP="${JUMP:-HW01}"
SERVER="${SERVER:-HW01}"
CLIENT="${CLIENT:-192.168.90.112}"
SERVER_HOST="${SERVER_HOST:-192.168.1.111}"
BASE_PORT=${BASE_PORT:-7001}
CODE_DIR="${CODE_DIR:-/root/gqs/codespace/redis-8.6.3}"
MEMTIER_DIR="${MEMTIER_DIR:-/root/gqs/codespace/UnifiedBus/memtier_benchmark_origin}"

# ───────── 实验参数 ─────────
# LOCAL_BENCH=1: 跑本地回环（server+client 都在 HW01，memtier 连 127.0.0.1）
LOCAL_BENCH=${LOCAL_BENCH:-0}
NUM_INSTANCES=${NUM_INSTANCES:-12}
IO_THREADS=${IO_THREADS:-4}
DIM=${DIM:-300}
NUM_KEYS=${NUM_KEYS:-100000}
PIPELINE=${PIPELINE:-16}
TEST_TIME=${TEST_TIME:-30}
# RAW=1 走 VEMB raw 二进制路径（INT8 量化字节直传，server 跳过反量化+DIM 次 sprintf）
RAW=${RAW:-0}
RAW_SUFFIX=""
[ "$RAW" = "1" ] && RAW_SUFFIX=" raw"
# 每实例 memtier 的 -t 和 -c
MEMTIER_T=${MEMTIER_T:-16}
MEMTIER_C=${MEMTIER_C:-4}

# 客户端核范围（local: HW01 NUMA1; cross-node: HW02 NUMA1）
CLIENT_CPU_START=${CLIENT_CPU_START:-97}
CLIENT_CPU_END=${CLIENT_CPU_END:-191}
# CLIENT_CPUSET: 任意 taskset 范围字符串（如 HW06 NUMA1 "24-47,72-95"）。
# 如果设置，覆盖 CLIENT_CPU_START-END。
CLIENT_CPUSET=${CLIENT_CPUSET:-}
# 派生：脚本里绑核用 CLIENT_CPU_SPEC
if [ -n "$CLIENT_CPUSET" ]; then
    CLIENT_CPU_SPEC="$CLIENT_CPUSET"
else
    CLIENT_CPU_SPEC="$CLIENT_CPU_START-$CLIENT_CPU_END"
fi

# 派生值
CORES_PER_INSTANCE=$IO_THREADS
# 每实例 memtier 独占核组大小（保证调度公平，避免 12 进程争抢 95 核造成实例间不均衡）
TOTAL_CLIENT_CORES=$((CLIENT_CPU_END - CLIENT_CPU_START + 1))
CORES_PER_MEMTIER=$(( TOTAL_CLIENT_CORES / NUM_INSTANCES ))

LOCAL_RESULT_DIR="benchmark/results/vemb_multi_instance_baseline"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_PREFIX="multi_inst_${TIMESTAMP}"

mkdir -p "$LOCAL_RESULT_DIR"

log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

get_ts() {
    python3 -c 'import time; print("%.9f" % time.time())' 2>/dev/null || date +%s
}

# 环境变量前缀：传给 HW01 上的 orchestrator
ORCH_ENV="NUM_INSTANCES=$NUM_INSTANCES IO_THREADS=$IO_THREADS CORES_PER=$CORES_PER_INSTANCE DIM=$DIM NUM_KEYS=$NUM_KEYS BASE_PORT=$BASE_PORT CODE_DIR=$CODE_DIR"

# ============================================================================
# Step 0: Upload helpers to HW01
# ============================================================================
log "=== Step 0: Upload helpers ==="

# jiffies helper
ssh "$JUMP" "cat > /tmp/get_jiffies.sh" <<'JIFFIES_EOF'
#!/bin/bash
pid=$1
s=0
for f in /proc/$pid/task/*/stat; do
    [ -r "$f" ] || continue
    r=$(sed 's/.*)//' "$f")
    set -- $r
    s=$((s + ${12:-0} + ${13:-0}))
done
echo $s
JIFFIES_EOF

# multi-instance orchestrator on HW01（读 env var，由 ORCH_ENV 传入）
ssh "$JUMP" "cat > /tmp/multi_instance_server.sh" <<'ORCH_EOF'
#!/bin/bash
# 用法: multi_instance_server.sh <cmd> <iid>
# env: NUM_INSTANCES IO_THREADS CORES_PER DIM NUM_KEYS BASE_PORT CODE_DIR
cmd=$1
iid=$2

PORT=$((BASE_PORT + iid))
CORE_START=$((iid * CORES_PER))
CORE_END=$((CORE_START + CORES_PER - 1))
KEYS_PER=$(( (NUM_KEYS + NUM_INSTANCES - 1) / NUM_INSTANCES ))
KEY_MIN=$((iid * KEYS_PER + 1))
KEY_MAX=$(( (iid + 1) * KEYS_PER ))
[ $KEY_MAX -gt $NUM_KEYS ] && KEY_MAX=$NUM_KEYS
DATA_DIR="/tmp/redis_multi_inst_${iid}"
SERVER_LOG="${DATA_DIR}/redis.log"

case "$cmd" in
start)
    cd "$CODE_DIR"
    rm -rf "$DATA_DIR" && mkdir -p "$DATA_DIR"
    numactl --membind=0 taskset -c $CORE_START-$CORE_END ./src/redis-server \
        --port $PORT --protected-mode no \
        --dir "$DATA_DIR" \
        --tcp-keepalive 1800 --timeout 0 \
        --io-threads $IO_THREADS --io-threads-do-reads yes \
        --daemonize yes --logfile "$SERVER_LOG" \
        --pidfile "${DATA_DIR}/redis.pid"
    ;;
stop)
    redis-cli -p $PORT --timeout 2 SHUTDOWN NOSAVE 2>/dev/null || true
    # 双重 kill：pkill 模式 + ss 精确找 PID（pkill "redis-server.*:PORT" 末尾带或不带空格都匹配）
    pkill -9 -f "redis-server.*:$PORT" 2>/dev/null || true
    # 兜底：ss 找端口对应 PID（处理 pkill 未匹配的残留）
    for pid in $(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | sort -u); do
        kill -9 "$pid" 2>/dev/null || true
    done
    ;;
prefill)
    cd "$CODE_DIR"
    VEC300=$(seq -s " " 1 $DIM | sed "s/[0-9]*/0.1/g")
    expected=$((KEY_MAX - KEY_MIN + 1))
    # 单 redis-cli --pipe + VCARD 验证 + VEMB probe（不只查数量，验证 key 真的在）+ 失败重试
    for attempt in 1 2 3 4 5; do
        # 先 DEL 旧 vset 避免脏数据（VCARD 可能来自上次残留）
        ./src/redis-cli -p $PORT DEL myvectors >/dev/null 2>&1
        sleep 0.2
        {
            for i in $(seq $KEY_MIN $KEY_MAX); do
                echo "VADD myvectors VALUES $DIM $VEC300 item:$i"
            done
        } | ./src/redis-cli -p $PORT --pipe >/dev/null 2>&1
        # VCARD 验证
        card=$(./src/redis-cli -p $PORT VCARD myvectors 2>/dev/null)
        [ -z "$card" ] && card=0
        # VEMB probe：查一个应该存在的 key，验证响应非空
        probe=$(./src/redis-cli -p $PORT VEMB myvectors item:$KEY_MIN 2>/dev/null | wc -c)
        if [ "$card" -ge "$expected" ] && [ "$probe" -gt 100 ]; then
            echo "instance $iid: prefill OK card=$card expected=$expected probe_bytes=$probe (attempt $attempt)"
            break
        fi
        echo "instance $iid: prefill attempt $attempt card=$card expected=$expected probe_bytes=$probe, retrying..."
        sleep 2
    done
    ;;
pid)
    ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1
    ;;
keyrange)
    echo "$KEY_MIN $KEY_MAX"
    ;;
esac
ORCH_EOF

# Cleanup any lingering instances
log "Cleaning up any lingering instances..."
for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    ssh "$JUMP" "$ORCH_ENV bash /tmp/multi_instance_server.sh stop $i" 2>/dev/null &
done
wait

# ============================================================================
# Step 1: Start N Redis instances
# ============================================================================
log "=== Step 1: Start $NUM_INSTANCES Redis instances (io-threads=$IO_THREADS, ${CORES_PER_INSTANCE} cores each) ==="

for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    PORT=$((BASE_PORT + i))
    CORE_START=$((i * CORES_PER_INSTANCE))
    CORE_END=$((CORE_START + CORES_PER_INSTANCE - 1))
    log "  instance $i: port=$PORT cores=$CORE_START-$CORE_END"
    ssh "$JUMP" "$ORCH_ENV bash /tmp/multi_instance_server.sh start $i" 2>/dev/null || true
done

# Wait for all to listen — single SSH poll loop on HW01
log "Waiting for all instances to listen..."
ALL_OK=1
for attempt in $(seq 1 60); do
    result=$(ssh "$JUMP" "$ORCH_ENV bash -c '
        up=0
        for i in \$(seq 0 \$((NUM_INSTANCES - 1))); do
            p=\$((BASE_PORT + i))
            ss -tln | grep -q \":\$p \" && up=\$((up + 1))
        done
        echo \$up
    '" 2>/dev/null)
    if [ "$result" = "$NUM_INSTANCES" ]; then
        log "All $NUM_INSTANCES instances are listening."
        ALL_OK=1
        break
    fi
    ALL_OK=0
    sleep 1
done
if [ "$ALL_OK" -ne 1 ]; then
    log "ABORT: not all instances started (only ${result:-0}/$NUM_INSTANCES listening)"
    exit 1
fi

# Connectivity check
if [ "$LOCAL_BENCH" = "1" ]; then
    log "Local mode: skipping cross-node connectivity check"
else
    log "Checking $CLIENT -> $SERVER_HOST connectivity..."
    if ! ssh "$JUMP" "ssh $CLIENT \"timeout 3 bash -c ': <>/dev/tcp/$SERVER_HOST/$BASE_PORT'\"" 2>/dev/null; then
        log "ERROR: $CLIENT cannot reach $SERVER_HOST:$BASE_PORT"
        exit 1
    fi
fi
log "Connectivity OK"

# ============================================================================
# Step 2: Prefill (parallel across instances)
# ============================================================================
log "=== Step 2: Prefill $NUM_KEYS vectors across $NUM_INSTANCES instances ==="

for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    log "  prefill instance $i..."
    ssh "$JUMP" "$ORCH_ENV bash /tmp/multi_instance_server.sh prefill $i"
done
log "Prefill done."

# ============================================================================
# Step 3: Upload bench script to HW01 (scp to CLIENT each iteration)
# ============================================================================
log "=== Step 3: Deploy bench script ==="

BENCH_SCRIPT_NAME="multi_inst_bench.sh"
ssh "$JUMP" "cat > /tmp/$BENCH_SCRIPT_NAME" <<'BENCH_SCRIPT'
#!/bin/bash
# 不用 set -e：vanilla redis VEMB 偶发 connection reset 是正常的
threads=$1
clients=$2
test_time=$3
host=$4
port=$5
memtier_dir=$6
key_min=$7
key_max=$8
pipeline=$9
out_file=${10}
raw_suffix=${11:-}

cd "$memtier_dir"
./memtier_benchmark \
    -h "$host" -p "$port" \
    --hide-histogram --test-time="$test_time" --select-db=0 \
    -c "$clients" -t "$threads" --pipeline="$pipeline" \
    --command="VEMB myvectors __key__${raw_suffix}" \
    --command-key-pattern=R \
    --key-prefix=item: \
    --key-minimum="$key_min" --key-maximum="$key_max" \
    > "$out_file" 2>&1 || true
BENCH_SCRIPT

# ============================================================================
# Step 4: Run parallel benchmarks
# ============================================================================
log "=== Step 4: Run $NUM_INSTANCES parallel memtier benchmarks ==="
log "  per-instance: -t $MEMTIER_T -c $MEMTIER_C --pipeline=$PIPELINE → $((MEMTIER_T * MEMTIER_C)) conn/inst"
log "  total connections: $((NUM_INSTANCES * MEMTIER_T * MEMTIER_C))"

# scp bench script to CLIENT once (skip in local mode — already on HW01)
if [ "$LOCAL_BENCH" != "1" ]; then
    ssh "$JUMP" "scp /tmp/$BENCH_SCRIPT_NAME $CLIENT:/tmp/$BENCH_SCRIPT_NAME" 2>/dev/null
fi

# Determine target host for memtier
if [ "$LOCAL_BENCH" = "1" ]; then
    BENCH_HOST="127.0.0.1"
else
    BENCH_HOST="$SERVER_HOST"
fi

# Get PIDs + J0 for all instances
declare -a INSTANCE_PIDS J0_VALUES
TOTAL_J0=0
for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    INSTANCE_PIDS[$i]=$(ssh "$JUMP" "$ORCH_ENV bash /tmp/multi_instance_server.sh pid $i" 2>/dev/null)
    pid="${INSTANCE_PIDS[$i]}"
    if [ -n "$pid" ]; then
        J0_VALUES[$i]=$(ssh "$JUMP" "bash /tmp/get_jiffies.sh $pid" 2>/dev/null)
    else
        J0_VALUES[$i]=0
        log "  WARN: no PID for instance $i"
    fi
done

RUN_START=$(get_ts)

# Launch all memtier in parallel
# SHARED_CLIENT_CPU=1（默认）: 所有 memtier 共享 CLIENT_CPU_START-END（跟历史一致，可能不均但峰值高）
# SHARED_CLIENT_CPU=0: 每实例独立绑核组（稳定可复现，但单实例上限略低）
SHARED_CLIENT_CPU=${SHARED_CLIENT_CPU:-1}
PIDS=""
for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    PORT=$((BASE_PORT + i))
    read KEY_MIN_I KEY_MAX_I <<< $(ssh "$JUMP" "$ORCH_ENV bash /tmp/multi_instance_server.sh keyrange $i" 2>/dev/null)
    remote_out="/tmp/${RESULT_PREFIX}_inst${i}.log"

    if [ "$SHARED_CLIENT_CPU" = "1" ]; then
        # 共享绑核：所有 memtier 共享 CLIENT_CPU_SPEC（支持离散范围如 HW06 NUMA1 "24-47,72-95"）
        if [ "$LOCAL_BENCH" = "1" ]; then
            ssh "$JUMP" "numactl --membind=1 taskset -c $CLIENT_CPU_SPEC bash /tmp/$BENCH_SCRIPT_NAME $MEMTIER_T $MEMTIER_C $TEST_TIME $BENCH_HOST $PORT $MEMTIER_DIR $KEY_MIN_I $KEY_MAX_I $PIPELINE $remote_out '$RAW_SUFFIX'" 2>/dev/null &
        else
            ssh "$JUMP" "ssh $CLIENT \"numactl --membind=1 taskset -c $CLIENT_CPU_SPEC bash /tmp/$BENCH_SCRIPT_NAME $MEMTIER_T $MEMTIER_C $TEST_TIME $BENCH_HOST $PORT $MEMTIER_DIR $KEY_MIN_I $KEY_MAX_I $PIPELINE $remote_out '$RAW_SUFFIX'\"" 2>/dev/null &
        fi
    else
        # 独立绑核：每实例独占核组（仅支持 CLIENT_CPU_START-END 连续范围）
        CPM=$CORES_PER_MEMTIER
        CSTART=$((CLIENT_CPU_START + i * CPM))
        CEND=$((CSTART + CPM - 1))
        if [ "$LOCAL_BENCH" = "1" ]; then
            ssh "$JUMP" "numactl --membind=1 taskset -c $CSTART-$CEND bash /tmp/$BENCH_SCRIPT_NAME $MEMTIER_T $MEMTIER_C $TEST_TIME $BENCH_HOST $PORT $MEMTIER_DIR $KEY_MIN_I $KEY_MAX_I $PIPELINE $remote_out '$RAW_SUFFIX'" 2>/dev/null &
        else
            ssh "$JUMP" "ssh $CLIENT \"numactl --membind=1 taskset -c $CSTART-$CEND bash /tmp/$BENCH_SCRIPT_NAME $MEMTIER_T $MEMTIER_C $TEST_TIME $BENCH_HOST $PORT $MEMTIER_DIR $KEY_MIN_I $KEY_MAX_I $PIPELINE $remote_out '$RAW_SUFFIX'\"" 2>/dev/null &
        fi
    fi
    PIDS="$PIDS $!"
done

log "  All $NUM_INSTANCES benchmarks launched, waiting..."
wait $PIDS
RUN_END=$(get_ts)

# ============================================================================
# Step 5: Collect + aggregate results
# ============================================================================
log "=== Step 5: Collecting results ==="

TOTAL_OPS=0
TOTAL_KB_SEC=0
AVG_LAT_SUM=0
P99_SUM=0
VALID_INSTANCES=0
TOTAL_CORES=0

for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    remote_out="/tmp/${RESULT_PREFIX}_inst${i}.log"
    local_out="$LOCAL_RESULT_DIR/${RESULT_PREFIX}_inst${i}.log"
    if [ "$LOCAL_BENCH" = "1" ]; then
        ssh "$JUMP" "cat $remote_out" > "$local_out" 2>/dev/null || true
    else
        ssh "$JUMP" "ssh $CLIENT \"cat $remote_out\"" > "$local_out" 2>/dev/null || true
    fi

    totals=$(grep "^Totals" "$local_out" 2>/dev/null | tail -1)
    last_progress=$(tr '\r' '\n' < "$local_out" 2>/dev/null | \
        grep -E "^\[RUN #[0-9]+ +[0-9]+%," | tail -1)

    if [ -n "$totals" ] && [ "$(echo "$totals" | awk '{print $2}')" != "0.00" ]; then
        ops=$(echo "$totals" | awk '{print $2}')
        avg=$(echo "$totals" | awk '{print $3}')
        p99=$(echo "$totals" | awk '{print $5}')
        kb=$(echo "$totals" | awk '{print $7}')
    elif [ -n "$last_progress" ]; then
        ops=$(echo "$last_progress" | sed -n 's/.*(avg: *\([0-9][0-9]*\)) ops\/sec.*/\1/p')
        avg=$(echo "$last_progress" | sed -n 's/.*(avg: *\([0-9.][0-9.]*\)) msec latency.*/\1/p')
        kb=$(echo "$last_progress" | sed -n 's/.*(avg: *\([0-9.][0-9.]*\)\([MK]\)B\/sec).*/\1\2B\/sec/p')
        p99="N/A"
    else
        ops=0; avg=0; p99=0; kb=0
        log "  WARN: instance $i no data"
    fi

    # Per-instance cores via jiffies
    pid="${INSTANCE_PIDS[$i]}"
    inst_cores="NA"
    if [ -n "$pid" ]; then
        j1_i=$(ssh "$JUMP" "bash /tmp/get_jiffies.sh $pid" 2>/dev/null || echo 0)
        inst_cores=$(awk "BEGIN{ printf \"%.2f\", ($j1_i - ${J0_VALUES[$i]:-0}) / 100.0 / $TEST_TIME }")
        TOTAL_CORES=$(awk "BEGIN{ printf \"%.2f\", $TOTAL_CORES + $inst_cores }")
    fi

    # Sum ops
    ops_int=$(echo "$ops" | awk '{printf "%.0f", $1}')
    TOTAL_OPS=$((TOTAL_OPS + ops_int))

    # Sum KB/sec (normalize to KB/sec numeric)
    # Totals line field 7 is plain KB/sec number; progress fallback has "NNNKB/sec" or "NNNMB/sec"
    if echo "$kb" | grep -q "MB"; then
        kb_sec=$(echo "$kb" | sed 's/[^0-9.]//g' | awk '{printf "%.0f", $1 * 1024}')
    elif echo "$kb" | grep -q "KB"; then
        kb_sec=$(echo "$kb" | sed 's/[^0-9.]//g' | awk '{printf "%.0f", $1}')
    else
        # Totals line: plain number already in KB/sec
        kb_sec=$(echo "$kb" | awk '{printf "%.0f", $1}')
    fi
    TOTAL_KB_SEC=$((TOTAL_KB_SEC + kb_sec))

    AVG_LAT_SUM=$(awk "BEGIN{printf \"%.4f\", $AVG_LAT_SUM + ${avg:-0}}")
    if [ "$p99" != "N/A" ]; then
        P99_SUM=$(awk "BEGIN{printf \"%.4f\", $P99_SUM + $p99}")
    fi
    VALID_INSTANCES=$((VALID_INSTANCES + 1))

    log "  inst $i: ops/sec=$ops lat=${avg}ms cores=$inst_cores"
done

# Aggregate
AVG_LAT=$(awk "BEGIN{ if($VALID_INSTANCES>0) printf \"%.3f\", $AVG_LAT_SUM / $VALID_INSTANCES; else print \"N/A\" }")
AVG_P99=$(awk "BEGIN{ if($VALID_INSTANCES>0) printf \"%.3f\", $P99_SUM / $VALID_INSTANCES; else print \"N/A\" }")
TOTAL_GB_SEC=$(awk "BEGIN{ printf \"%.2f\", $TOTAL_KB_SEC / 1024.0 / 1024.0 }")

echo ""
log "=== Aggregate Results ==="
{
    printf "%-12s %14s %12s %12s %12s %10s\n" \
        "instances" "total_ops/sec" "avg_lat(ms)" "avg_p99(ms)" "wire_GB/s" "cores"
    printf "%-12s %14s %12s %12s %12s %10s\n" \
        "$NUM_INSTANCES" "$TOTAL_OPS" "$AVG_LAT" "$AVG_P99" "$TOTAL_GB_SEC" "$TOTAL_CORES"
} | tee "$LOCAL_RESULT_DIR/${RESULT_PREFIX}_summary.txt"

echo ""
echo "Config: $NUM_INSTANCES × io-threads=$IO_THREADS × ${CORES_PER_INSTANCE}cores = $((NUM_INSTANCES * CORES_PER_INSTANCE)) cores total"
echo "        per-instance memtier: -t $MEMTIER_T -c $MEMTIER_C --pipeline=$PIPELINE ($((MEMTIER_T * MEMTIER_C)) conn)"
echo "        total connections: $((NUM_INSTANCES * MEMTIER_T * MEMTIER_C)),  test_time=${TEST_TIME}s"
echo ""
echo "Result files:"
ls -la "$LOCAL_RESULT_DIR/${RESULT_PREFIX}"*

# ============================================================================
# Cleanup
# ============================================================================
log "Cleaning up $NUM_INSTANCES instances..."
for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    ssh "$JUMP" "$ORCH_ENV bash /tmp/multi_instance_server.sh stop $i" 2>/dev/null &
done
wait
ssh "$JUMP" "ssh $CLIENT \"rm -f /tmp/${RESULT_PREFIX}_*.log /tmp/$BENCH_SCRIPT_NAME\"" 2>/dev/null || true

log "=== All done ==="
