#!/usr/bin/env bash
# ============================================================================
# run_redis_cluster_vemb.sh
# 4 节点 redis cluster (baseline redis-8.6.3) VEMB 吞吐 / 延迟测试
#
# 拓扑: HW01 / HW02 / HW05 / HW06 每节点 1 个 redis 实例, cluster mode
# 网络: 192.168.1.x (100G mlx5 直连), client 端口 7000, cluster bus 17000
# 测试: prefill 多 vset (分散到 4 节点) -> memtier --cluster-mode VEMB 聚合测
#
# 用法:
#   bash benchmark/run_redis_cluster_vemb.sh                  # 完整 (默认 30s/档)
#   TEST_TIME=3 bash benchmark/run_redis_cluster_vemb.sh       # smoke 快验
#   THREADS="1 8" bash benchmark/run_redis_cluster_vemb.sh     # 指定线程档
#
# 编译口径 (四节点一致):
#   make -C deps jemalloc && \
#   make CFLAGS="-O2 -pipe -fno-lto" LDFLAGS="-O2 -pipe -fno-lto" CC="gcc -fuse-ld=bfd"
# ============================================================================

set -uo pipefail   # 不用 -e: cluster 偶发 MOVED / 重连不应整体退出

# === 节点 (ssh Host 别名 + cluster announce IP) ===
declare -a NODES=("HW01" "HW02" "HW05" "HW06")
declare -a IPS=("192.168.1.111" "192.168.1.112" "192.168.1.20" "192.168.1.93")
NNODES=${#NODES[@]}

# === 路径 ===
REDIS_DIR=${REDIS_DIR:-/root/gqs/codespace/redis-8.6.3}
MEMTIER=${MEMTIER:-/root/gqs/codespace/UnifiedBus/memtier_benchmark_origin/memtier_benchmark}
DATA_DIR=${DATA_DIR:-/tmp/redis-cluster-data}

# === cluster 参数 ===
PORT=${PORT:-7000}
CLUSTER_TIMEOUT=${CLUSTER_TIMEOUT:-10000}

# === 测试参数 ===
TEST_TIME=${TEST_TIME:-30}
CLIENTS=${CLIENTS:-50}
THREADS=${THREADS:-"1 4 8 16"}
PIPELINE=${PIPELINE:-1}

# === 数据规模 ===
NUM_VSETS=${NUM_VSETS:-16}                 # vset 数 (分散到 4 节点, 每节点 ~4)
VECTORS_PER_VSET=${VECTORS_PER_VSET:-1000} # 每 vset 向量数
DIM=${DIM:-300}                            # 向量维度 (VEMB 响应 ~1KB/op)

# === 输出 ===
OUTDIR=${OUTDIR:-/tmp/redis_cluster_vemb}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RAWDIR="$OUTDIR/raw"
TSV="$OUTDIR/summary_${TIMESTAMP}.tsv"

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"
mkdir -p "$RAWDIR"

log()  { echo "[$(date +%H:%M:%S)] $*"; }

ssh_node() { local i=$1; shift; ssh $SSH_OPTS "${NODES[$i]}" "$@" 2>&1 | grep -v "Authorized users"; }

# ----------------------------------------------------------------------------
cleanup_node() {
    local i=$1
    # 直接 ssh + 静默 (不经 ssh_node 的 grep 管道, 避免 trap 时 "Killed" 噪音)
    ssh $SSH_OPTS "${NODES[$i]}" "pkill -9 -f 'redis-server.*:$PORT ' 2>/dev/null; \
                  rm -f $DATA_DIR/nodes.conf $DATA_DIR/*.rdb $DATA_DIR/*.aof $DATA_DIR/redis.log 2>/dev/null; true" \
        >/dev/null 2>&1
}

cleanup_all() {
    log "cleanup all nodes..."
    for ((i=0; i<NNODES; i++)); do cleanup_node $i; done
}

# ----------------------------------------------------------------------------
start_node() {
    local i=$1 ip=${IPS[$i]}
    log "start redis on ${NODES[$i]} ($ip:$PORT)"
    ssh_node $i "mkdir -p $DATA_DIR && cd $REDIS_DIR && \
        ./src/redis-server \
            --port $PORT --bind 0.0.0.0 --protected-mode no \
            --cluster-enabled yes \
            --cluster-config-file nodes.conf \
            --cluster-node-timeout $CLUSTER_TIMEOUT \
            --cluster-announce-ip $ip \
            --appendonly no --save '' \
            --dir $DATA_DIR --logfile $DATA_DIR/redis.log \
            --daemonize yes" >/dev/null
}

wait_port() {
    local ip=$1 count=0
    while ! ($REDIS_DIR/src/redis-cli -h $ip -p $PORT PING 2>/dev/null | grep -q PONG); do
        sleep 0.5; ((count++))
        [ $count -gt 60 ] && { log "TIMEOUT waiting $ip:$PORT"; return 1; }
    done
}

# ----------------------------------------------------------------------------
create_cluster() {
    log "create cluster (--cluster-replicas 0, $NNODES masters)..."
    local endpoints=""
    for ip in "${IPS[@]}"; do endpoints="$endpoints $ip:$PORT"; done
    # 必须 echo yes (输出 "yes"); 用 yes|输出 "y" 会被 redis-cli 拒绝 (要求 "yes")
    echo yes | $REDIS_DIR/src/redis-cli --cluster create $endpoints --cluster-replicas 0 2>&1 \
        | grep -E "Slots|Master|slots:|OK|All|coverage|agree|Can't|err" | head -30
}

check_cluster() {
    log "cluster info:"
    $REDIS_DIR/src/redis-cli -h ${IPS[0]} -p $PORT CLUSTER INFO 2>/dev/null \
        | grep -E "cluster_state|cluster_slots_ok|cluster_known_nodes|cluster_size"
    log "nodes:"
    $REDIS_DIR/src/redis-cli -h ${IPS[0]} -p $PORT CLUSTER NODES 2>/dev/null \
        | awk '{print $2, $3, $NF}' | head -20
    log "vset slot distribution (前 $NUM_VSETS):"
    local slot_seen=""
    for v in $(seq 1 $NUM_VSETS); do
        local slot=$($REDIS_DIR/src/redis-cli -h ${IPS[0]} -p $PORT CLUSTER KEYSLOT vset$v 2>/dev/null)
        printf "  vset%-3d -> slot %s\n" "$v" "$slot"
    done
}

# ----------------------------------------------------------------------------
prefill() {
    log "prefill: $NUM_VSETS vsets x $VECTORS_PER_VSET vectors (dim=$DIM)..."
    # FLUSHALL 清掉重跑残留 (cluster FLUSHALL 同步所有节点)
    $REDIS_DIR/src/redis-cli -c -h ${IPS[0]} -p $PORT FLUSHALL >/dev/null 2>&1
    # awk 生成 VADD 命令文本, 喂给 redis-cli -c (cluster-aware: 缓存 slot map 后直接路由)
    awk -v m=$NUM_VSETS -v k=$VECTORS_PER_VSET -v dim=$DIM 'BEGIN{
        srand(42);
        for (v=1; v<=m; v++) {
            for (e=0; e<k; e++) {
                printf "VADD vset%d VALUES %d", v, dim;
                for (j=0; j<dim; j++) printf " %f", rand()*0.001;
                printf " elem%d\n", e;
            }
        }
    }' | $REDIS_DIR/src/redis-cli -c -h ${IPS[0]} -p $PORT >/dev/null 2>&1
    log "prefill done. VCARD sample:"
    for v in 1 $NUM_VSETS; do
        printf "  vset%d VCARD=%s\n" "$v" \
            "$($REDIS_DIR/src/redis-cli -c -h ${IPS[0]} -p $PORT VCARD vset$v 2>/dev/null)"
    done
}

# ----------------------------------------------------------------------------
run_vemb_test() {
    log "VEMB cluster test (memtier --cluster-mode, VEMB __key__ elem0)..."
    printf "threads\tclients\tpipeline\tops_sec\tavg_lat_ms\tp50_ms\tp99_ms\tp999_ms\tkb_sec\n" > "$TSV"
    for t in $THREADS; do
        local raw="$RAWDIR/vemb_t${t}.log"
        log "  t=$t c=$CLIENTS pipeline=$PIPELINE time=${TEST_TIME}s"
        # memtier cluster 模式: __key__=vset 名 (第一位 key, memtier 与 redis 都用它算 slot -> 一致)
        # elem0 固定; --cluster-mode 自动路由 vset1..N 到各 owner 节点
        $MEMTIER -s ${IPS[0]} -p $PORT --cluster-mode \
            -t $t -c $CLIENTS --pipeline=$PIPELINE \
            --command="VEMB __key__ elem0" --command-key-pattern=R \
            --key-prefix=vset --key-minimum=1 --key-maximum=$NUM_VSETS \
            --data-size=128 \
            --test-time=$TEST_TIME --hide-histogram --select-db=0 \
            > "$raw" 2>&1 || true
        # cluster 模式 Totals: ops MOVED/sec ASK/sec avg p50 p99 p999 kb (9 tokens 含 "Totals")
        # -> $2=ops $5=avg $6=p50 $7=p99 $8=p999 $9=kb
        local totals ops moved ask avg p50 p99 p999 kb
        totals=$(grep "^Totals" "$raw" | tail -1)
        read ops moved ask avg p50 p99 p999 kb < <(
            echo "$totals" | awk '{
                if (NF>=9) printf "%s %s %s %s %s %s %s %s", $2,$3,$4,$5,$6,$7,$8,$9
                else       printf "0 NA NA NA NA NA NA NA"
            }'
        )
        printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
            "$t" "$CLIENTS" "$PIPELINE" "$ops" "$avg" "$p50" "$p99" "$p999" "$kb" >> "$TSV"
        log "    => ops/s=$ops  avg=${avg}ms  p50=${p50}ms  p99=${p99}ms  kb/s=$kb"
    done
}

# ============================================================================
trap 'cleanup_all' EXIT INT TERM

log "=== STEP 1: cleanup residuals ==="
cleanup_all
sleep 1

log "=== STEP 2: start $NNODES redis instances ==="
for ((i=0; i<NNODES; i++)); do start_node $i; done
sleep 2
for ip in "${IPS[@]}"; do
    wait_port $ip || { log "FAIL: $ip:$PORT not up"; exit 1; }
done
log "all $NNODES instances up."

log "=== STEP 3: create cluster ==="
create_cluster
sleep 3
# 必须确认 cluster_state=ok 再继续, 否则 prefill 的 redis-cli -c 会因无路由而 hang 死
cstate=$($REDIS_DIR/src/redis-cli -h ${IPS[0]} -p $PORT CLUSTER INFO 2>/dev/null \
         | awk -F: '/cluster_state/{gsub(/[[:space:]]/,"",$2);print $2}')
if [ "$cstate" != "ok" ]; then
    log "FAIL: cluster_state=$cstate (expect ok). abort before prefill."
    exit 1
fi
check_cluster

log "=== STEP 4: prefill VEMB data ==="
prefill

log "=== STEP 5: VEMB throughput/latency ==="
run_vemb_test

log "=== DONE ==="
log "TSV  : $TSV"
log "raw  : $RAWDIR/vemb_t*.log"
echo "----- summary -----"
cat "$TSV"
