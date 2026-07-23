#!/bin/bash
# hpc_redis scaleout 扩容过程吞吐测试（双节点 0→1 扩容，三段采集）
#
# 三段采集法：
#   段1 baseline    — 稳态 VEMB 读 TEST_TIME 秒
#   段2 during      — 后台 VEMB 读 + 触发扩容，等完成后停止
#   段3 after       — 新稳态 VEMB 读 TEST_TIME 秒
#
# 用法: bash benchmark/hpc_redis_scaleout_throughput.sh
#   smoke: TEST_TIME=3 PREFILL_KEYS=1000 bash benchmark/hpc_redis_scaleout_throughput.sh
#
# 所有产物（yaml、log、tsv、raw）写 benchmark/results/scaleout/<run_id>/ 下，不放 /tmp。
set -uo pipefail

# ============================================================================
# 网络拓扑
# ============================================================================
NODE0_HOST="${NODE0_HOST:-192.168.90.111}"
NODE1_HOST="${NODE1_HOST:-192.168.90.112}"
SSH_USER="${SSH_USER:-root}"
REMOTE_DIR="${REMOTE_DIR:-/root/gqs/codespace/UnifiedBus/hpc-redis}"
MEMTIER="${MEMTIER:-/root/gqs/codespace/UnifiedBus/memtier_benchmark/memtier_benchmark}"

# ============================================================================
# server 参数
# ============================================================================
PORT="${PORT:-6391}"
COORD_PORT="${COORD_PORT:-7391}"
DIM="${DIM:-300}"
MAX_VECTORS="${MAX_VECTORS:-65536}"
PIO="${PIO:-21}"
SNW="${SNW:-21}"

# ============================================================================
# 测试参数
# ============================================================================
PREFILL_KEYS="${PREFILL_KEYS:-10000}"
TEST_TIME="${TEST_TIME:-30}"
# 后台 memtier 时长：必须 > 扩容耗时，让进程能自然结束输出 Totals
BG_TIME_SCALEOUT="${BG_TIME_SCALEOUT:-60}"
PIPELINE="${PIPELINE:-32}"
MEMTIER_T="${MEMTIER_T:-64}"
MEMTIER_C="${MEMTIER_C:-4}"

# ============================================================================
# Epoch（用时间戳避免与历史测试冲突）
# ============================================================================
EPOCH_BASE=${EPOCH_BASE:-$(date +%s)}
INIT_EPOCH=$((EPOCH_BASE + 1))
MIGRATION_EPOCH=$((EPOCH_BASE + 101))
CUTOVER_EPOCH=$((EPOCH_BASE + 102))
CONTROL_TIMEOUT=5000
COMBINED_TIMEOUT=180000

# ============================================================================
# 产物路径（本地 + 远端都落在 benchmark/results/scaleout/<run_id>/ 下，不放 /tmp）
# ============================================================================
RESULTS_DIR="${RESULTS_DIR:-$(cd "$(dirname "$0")" && pwd)/results/scaleout}"
RUN_ID="${RUN_ID:-$(date +%Y%m%d_%H%M%S)}"

# 本地产物（TSV 汇总）
LOCAL_RUN_DIR="$RESULTS_DIR/$RUN_ID"
mkdir -p "$LOCAL_RUN_DIR"
TSV="$LOCAL_RUN_DIR/summary.tsv"

# 远端产物子目录（相对 REMOTE_DIR；node0/node1 同路径）
REMOTE_SUBDIR="benchmark/results/scaleout/$RUN_ID"
NODE0_MANIFEST="$REMOTE_DIR/$REMOTE_SUBDIR/node0.yaml"
NODE1_MANIFEST="$REMOTE_DIR/$REMOTE_SUBDIR/node1.yaml"
NODE0_PEER_MAP="$REMOTE_DIR/$REMOTE_SUBDIR/node0_peermap.yaml"
NODE0_LOG="$REMOTE_DIR/$REMOTE_SUBDIR/server_node0.log"
NODE1_LOG="$REMOTE_DIR/$REMOTE_SUBDIR/server_node1.log"
COORD_OUT="$REMOTE_DIR/$REMOTE_SUBDIR/coord.out"
COORD_ERR="$REMOTE_DIR/$REMOTE_SUBDIR/coord.err"
PREFILL_LOG="$REMOTE_DIR/$REMOTE_SUBDIR/prefill.log"
RAW_DIR="$REMOTE_DIR/$REMOTE_SUBDIR/raw"

# 工具函数
ssh_run() { ssh -p 22 "${SSH_USER}@$1" "${@:2}"; }
log() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

# 在两个节点上提前创建子目录
ssh_run "$NODE0_HOST" "mkdir -p $REMOTE_DIR/$REMOTE_SUBDIR/raw"
ssh_run "$NODE1_HOST" "mkdir -p $REMOTE_DIR/$REMOTE_SUBDIR/raw"

# TSV 表头
printf 'phase\tops_sec\thits\tp50_ms\tp99_ms\twall_s\tnote\n' > "$TSV"

# ============================================================================
# Manifest：直接从 examples/ 拷静态 yaml 到远端产物目录（改参数改 yaml 文件即可）
# ============================================================================
EXAMPLES_DIR="$(cd "$(dirname "$0")/.." && pwd)/examples"
NODE0_YAML="$EXAMPLES_DIR/vemb_v16_scaleout_node0.yaml"
NODE1_YAML="$EXAMPLES_DIR/vemb_v16_scaleout_node1.yaml"
NODE0_PEERMAP_YAML="$EXAMPLES_DIR/vemb_v16_scaleout_node0_peermap.yaml"

# 把 3 份 yaml 拷到远端节点产物目录
write_manifests() {
    log "Phase 1: Copy manifests from examples/ to $REMOTE_SUBDIR/"
    ssh_run "$NODE0_HOST" "cat >$NODE0_MANIFEST" < "$NODE0_YAML"
    ssh_run "$NODE1_HOST" "cat >$NODE1_MANIFEST" < "$NODE1_YAML"
    ssh_run "$NODE0_HOST" "cat >$NODE0_PEER_MAP" < "$NODE0_PEERMAP_YAML"
}

# ============================================================================
# 启停 server / 端口探测
# ============================================================================
# 在指定节点启动集成 redis-server（numactl + taskset 绑 NUMA0 0-95 核）
start_node() {
    local host=$1 manifest=$2 logfile=$3 reset=$4
    local reset_flag=""
    [ "$reset" = "1" ] && reset_flag="--vemb-v16-reset-warm-regions yes"
    ssh_run "$host" "cd $REMOTE_DIR && rm -f $logfile && \
        numactl --membind=0 taskset -c 0-95 ./src/redis-server \
        --port $PORT --bind 0.0.0.0 --protected-mode no \
        --vemb-v16-enabled yes --vemb-v16-dim $DIM \
        --vemb-v16-max-vectors $MAX_VECTORS \
        --vemb-v16-warm-regions-manifest $manifest \
        $reset_flag \
        --vemb-v16-proxy-io-threads $PIO --vemb-v16-supernode-workers $SNW \
        --daemonize yes --logfile $logfile --loglevel notice \
        >/dev/null 2>&1"
}

# 停指定节点上的集成 redis-server（按端口匹配）
stop_node() {
    local host=$1
    ssh_run "$host" "pkill -9 -f 'redis-server.*:$PORT ' 2>/dev/null; sleep 0.5" || true
}

# 等 redis-server 在节点上 listen（最多 25s）
wait_port() {
    local host=$1
    for _ in $(seq 1 50); do
        ssh_run "$host" "ss -tln | grep -q ':$PORT '" && return 0
        sleep 0.5
    done
    return 1
}

# ============================================================================
# memtier 负载工具
# ============================================================================
# 前台跑 memtier 拿 Totals（baseline/after 段用）
run_memtier() {
    local host=$1 tt=$2 outfile=$3 extra=$4
    ssh_run "$host" "numactl --membind=1 taskset -c 96-191 \
        $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
        -s $host -p $PORT -t $MEMTIER_T -c $MEMTIER_C --pipeline=$PIPELINE \
        --ratio=0:1 --key-pattern=R:R --key-prefix=item: \
        --key-minimum=1 --key-maximum=$PREFILL_KEYS \
        --test-time=$tt $extra >$outfile 2>&1" || true
    local tot; tot=$(ssh_run "$host" "grep '^Totals' $outfile 2>/dev/null | tail -1")
    local ops hits p50 p99
    ops=$(echo "$tot" | awk '{print $2}')
    hits=$(echo "$tot" | awk '{print $3}')
    p50=$(echo "$tot" | awk '{print $6}')
    p99=$(echo "$tot" | awk '{print $7}')
    [ -z "$ops" ] && ops=0
    echo "$ops $hits $p50 $p99"
}

# 后台启 memtier（during 段用，test-time 后自然退出）
run_memtier_bg() {
    local host=$1 outfile=$2 bg_time=$3
    ssh_run "$host" "numactl --membind=1 taskset -c 96-191 \
        $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
        -s $host -p $PORT -t $MEMTIER_T -c $MEMTIER_C --pipeline=$PIPELINE \
        --ratio=0:1 --key-pattern=R:R --key-prefix=item: \
        --key-minimum=1 --key-maximum=$PREFILL_KEYS \
        --test-time=$bg_time >$outfile 2>&1 &" || true
}

# 强制 kill 后台 memtier（清理用）
stop_memtier_bg() {
    ssh_run "$NODE0_HOST" "pkill -9 memtier_benchmark 2>/dev/null" || true
}

# 从后台 memtier 日志提取 Totals（与 baseline/after 口径一致；不回退瞬时速率）
parse_bg_out() {
    local host=$1 outfile=$2
    local tot ops hits p50 p99
    # memtier 偶尔在 sleep 后才 flush Totals，给 15s 重试窗口
    local i
    for i in $(seq 1 15); do
        tot=$(ssh_run "$host" "grep '^Totals' $outfile 2>/dev/null | tail -1")
        [ -n "$tot" ] && break
        sleep 1
    done
    if [ -z "$tot" ]; then
        echo "ERROR: no Totals in $outfile (memtier killed?)" >&2
        echo "0 0 NA NA"
        return 1
    fi
    ops=$(echo "$tot" | awk '{print $2}')
    hits=$(echo "$tot" | awk '{print $3}')
    p50=$(echo "$tot" | awk '{print $6}')
    p99=$(echo "$tot" | awk '{print $7}')
    [ -z "$ops" ] && ops=0
    echo "$ops $hits $p50 $p99"
}

# 把一段结果写 TSV 并打 log
record_phase() {
    local phase=$1 ops=$2 hits=$3 p50=$4 p99=$5 wall=$6 note=$7
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$phase" "$ops" "$hits" "$p50" "$p99" "$wall" "$note" >> "$TSV"
    log "$phase: ops=$ops p99=$p99 wall=${wall}s ($note)"
}

# 用 SET prefill PREFILL_KEYS 条向量到 node0
prefill_data() {
    log "Prefilling $PREFILL_KEYS vectors to node0"
    ssh_run "$NODE0_HOST" "numactl --membind=1 taskset -c 96-191 \
        $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
        -s $NODE0_HOST -p $PORT -t 32 -c 4 --pipeline=32 \
        --ratio=1:0 --key-pattern=S:S --key-prefix=item: \
        --key-minimum=1 --key-maximum=$PREFILL_KEYS -n $PREFILL_KEYS \
        >$PREFILL_LOG 2>&1"
}

# ============================================================================
# 主流程
# ============================================================================

# --- Phase 1: Cleanup + Manifests ---
log "Phase 1: Cleanup + Manifests"
stop_node "$NODE0_HOST"
stop_node "$NODE1_HOST"
write_manifests

# --- Phase 2: Start servers ---
log "Phase 2: Start servers (node0, node1)"
start_node "$NODE0_HOST" "$NODE0_MANIFEST" "$NODE0_LOG" 1
wait_port "$NODE0_HOST" || { echo "FAIL: node0 not listening"; exit 1; }

start_node "$NODE1_HOST" "$NODE1_MANIFEST" "$NODE1_LOG" 1
wait_port "$NODE1_HOST" || { echo "FAIL: node1 not listening"; exit 1; }
sleep 2

log "Verify startup"
ssh_run "$NODE0_HOST" "grep -E 'remote meta ready|ub rpc ready|server ready' $NODE0_LOG" || true
ssh_run "$NODE1_HOST" "grep -E 'remote meta ready|registered.*remote meta|ub rpc ready|server ready' $NODE1_LOG" || true

# --- Phase 3: Initial topology + prefill ---
log "Phase 3: Initial topology (active={0}, epoch=$INIT_EPOCH) + prefill"
ssh_run "$NODE0_HOST" "cd $REMOTE_DIR && ./benchmark/vemb_v16_topology_ctl --set --transport tcp \
    --host $NODE0_HOST --port $PORT --epoch $INIT_EPOCH --min-write-epoch $INIT_EPOCH \
    --active 0 --standby 0 \
    --owner-endpoints 0=$NODE0_HOST:$PORT \
    --timeout-ms $CONTROL_TIMEOUT" || { echo "FAIL: initial topology"; exit 1; }

prefill_data

# ============================================================================
# Phase 4-5: 扩容三段采集
# ============================================================================
log "Phase 4-5: Scaleout sweep (baseline / during / after)"

# 段1: baseline (active={0})
log "段1: baseline VEMB read (${TEST_TIME}s)"
T0=$(date +%s)
result=$(run_memtier "$NODE0_HOST" "$TEST_TIME" "$RAW_DIR/scaleout_baseline.txt" "")
T1=$(date +%s)
read ops hits p50 p99 <<< "$result"
record_phase "scaleout_baseline" "$ops" "$hits" "$p50" "$p99" "$((T1-T0))" "active={0}"

# 段2: during scaleout (后台 memtier + 触发扩容)
log "段2: during scaleout (background VEMB ${BG_TIME_SCALEOUT}s + topology change)"
run_memtier_bg "$NODE0_HOST" "$RAW_DIR/scaleout_during.txt" "$BG_TIME_SCALEOUT"
T0=$(date +%s)

# 启动 coordinator（在 node0 上 setsid -f 后台跑）
ssh_run "$NODE0_HOST" "cd $REMOTE_DIR && setsid -f ./benchmark/vemb_v16_topology_ctl \
    --coordinator-listen --transport tcp --host $NODE0_HOST --port $COORD_PORT \
    --expected-sources 0 --migration-epoch $MIGRATION_EPOCH --cutover-epoch $CUTOVER_EPOCH \
    --standby 0,1 --owner-endpoints 0=$NODE0_HOST:$PORT,1=$NODE1_HOST:$PORT \
    --wait-ms 60000 --timeout-ms $CONTROL_TIMEOUT >$COORD_OUT 2>$COORD_ERR </dev/null"
sleep 1

# 发布候选拓扑到 node1
ssh_run "$NODE1_HOST" "cd $REMOTE_DIR && ./benchmark/vemb_v16_topology_ctl --set --transport tcp \
    --host $NODE1_HOST --port $PORT --epoch $MIGRATION_EPOCH --min-write-epoch $MIGRATION_EPOCH \
    --active 0 --standby 0,1 --dual-write --auto-scaleout --coordinated-scaleout \
    --owner-endpoints 0=$NODE0_HOST:$PORT,1=$NODE1_HOST:$PORT \
    --coordinator-endpoint $NODE0_HOST:$COORD_PORT --timeout-ms $CONTROL_TIMEOUT"

# 发布候选拓扑到 node0 (带 peer-view-map)
ssh_run "$NODE0_HOST" "cd $REMOTE_DIR && ./benchmark/vemb_v16_topology_ctl \
    --set-with-peer-view-map $NODE0_PEER_MAP --transport tcp \
    --host $NODE0_HOST --port $PORT --epoch $MIGRATION_EPOCH --min-write-epoch $MIGRATION_EPOCH \
    --active 0 --standby 0,1 --dual-write --auto-scaleout --coordinated-scaleout \
    --owner-endpoints 0=$NODE0_HOST:$PORT,1=$NODE1_HOST:$PORT \
    --coordinator-endpoint $NODE0_HOST:$COORD_PORT --timeout-ms $COMBINED_TIMEOUT"

# 等扩容完成（最多 120s）
ssh_run "$NODE0_HOST" "for _ in \$(seq 1 120); do \
    if grep -q '^scaleout_all_sources_done=1$' $COORD_OUT 2>/dev/null && \
       grep -q '^scaleout_full_active_published=' $COORD_OUT 2>/dev/null; then exit 0; fi; \
    sleep 1; done; exit 1"
T1=$(date +%s)
SCALEOUT_WALL=$((T1-T0))

# 等后台 memtier 跑完（让它自然结束输出 Totals）
log "等待后台 memtier 自然结束（剩 $((BG_TIME_SCALEOUT - SCALEOUT_WALL))s）"
REMAIN=$((BG_TIME_SCALEOUT - SCALEOUT_WALL))
if [ "$REMAIN" -gt 0 ]; then
    sleep "$REMAIN"
fi
sleep 2  # 给 memtier 输出 Totals 的时间

result=$(parse_bg_out "$NODE0_HOST" "$RAW_DIR/scaleout_during.txt")
read ops hits p50 p99 <<< "$result"
record_phase "during_scaleout" "$ops" "$hits" "${p50:-NA}" "${p99:-NA}" "$SCALEOUT_WALL" "active={0}->{0,1}"

ssh_run "$NODE0_HOST" "cat $COORD_OUT; echo '---'; cat $COORD_ERR 2>/dev/null" || true

# 段3: after scaleout (active={0,1})
log "段3: after scaleout VEMB read (${TEST_TIME}s)"
T0=$(date +%s)
result=$(run_memtier "$NODE0_HOST" "$TEST_TIME" "$RAW_DIR/scaleout_after.txt" "")
T1=$(date +%s)
read ops hits p50 p99 <<< "$result"
record_phase "scaleout_after" "$ops" "$hits" "$p50" "$p99" "$((T1-T0))" "active={0,1}"

# --- Phase 6: Cleanup ---
log "Phase 6: Cleanup"
stop_memtier_bg || true
stop_node "$NODE0_HOST"
stop_node "$NODE1_HOST"

log "DONE — $TSV"
cat "$TSV"
