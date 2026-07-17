#!/bin/bash
# hpc_redis scaleout 扩容/缩容过程吞吐测试
#
# 双机集成 redis-server，测量扩容（0→1 节点加入）和缩容（1→0 节点移除）
# 过程中 VEMB 读负载能维持的吞吐量。
#
# 三段采集法：
#   段1 baseline    — 稳态 VEMB 读 30s
#   段2 during      — 后台 VEMB 读 + 触发扩容/缩容，等完成后停止
#   段3 after       — 新稳态 VEMB 读 30s
#
# 用法: bash benchmark/hpc_redis_scaleout_throughput.sh
#   smoke: TEST_TIME=3 PREFILL_KEYS=1000 bash benchmark/hpc_redis_scaleout_throughput.sh
set -uo pipefail

NODE0_HOST="${NODE0_HOST:-192.168.90.111}"
NODE1_HOST="${NODE1_HOST:-192.168.90.112}"
SSH_USER="${SSH_USER:-root}"
REMOTE_DIR="${REMOTE_DIR:-/root/gqs/codespace/UnifiedBus/hpc-redis}"
MEMTIER="${MEMTIER:-/root/gqs/codespace/UnifiedBus/memtier_benchmark/memtier_benchmark}"

PORT="${PORT:-6391}"
COORD_PORT="${COORD_PORT:-7391}"
DIM="${DIM:-300}"
PREFILL_KEYS="${PREFILL_KEYS:-10000}"
TEST_TIME="${TEST_TIME:-30}"
# 后台 memtier 时长：必须 > 扩容/缩容耗时，让进程能自然结束输出 Totals
BG_TIME_SCALEOUT="${BG_TIME_SCALEOUT:-180}"
BG_TIME_SHRINK="${BG_TIME_SHRINK:-60}"
PIPELINE="${PIPELINE:-32}"
MEMTIER_T="${MEMTIER_T:-64}"
MEMTIER_C="${MEMTIER_C:-4}"
MAX_VECTORS="${MAX_VECTORS:-65536}"
PIO="${PIO:-21}"
SNW="${SNW:-21}"

# UB 设备路径
PAYLOAD_LOCAL="/dev/obmm_shmdev1"
PAYLOAD_PEER="/dev/obmm_shmdev5"
REQUEST_LOCAL="/dev/obmm_shmdev2"
REQUEST_PEER="/dev/obmm_shmdev6"
RESPONSE_LOCAL="/dev/obmm_shmdev4"
RESPONSE_PEER="/dev/obmm_shmdev8"

# warm region 容量
WR_BYTES=134217728  # 128MB
VALUE_SIZE=1200     # dim=300 × 4
META_OFFSET=268435456
META_ENTRIES=16384
META_BUCKETS=32768
UB_RPC_TIMEOUT=2000

# 迁移 epoch：用时间戳避免与历史测试冲突
EPOCH_BASE=${EPOCH_BASE:-$(date +%s)}
INIT_EPOCH=$((EPOCH_BASE + 1))
MIGRATION_EPOCH=$((EPOCH_BASE + 101))
CUTOVER_EPOCH=$((EPOCH_BASE + 102))
SHRINK_EPOCH=$((EPOCH_BASE + 201))
SHRINK_CUTOVER_EPOCH=$((EPOCH_BASE + 202))
CONTROL_TIMEOUT=5000
COMBINED_TIMEOUT=180000

NODE0_MANIFEST="/tmp/hpc_scaleout_node0.yaml"
NODE1_MANIFEST="/tmp/hpc_scaleout_node1.yaml"
NODE0_PEER_MAP="/tmp/hpc_scaleout_node0_peermap.yaml"
NODE0_LOG="/tmp/hpc_scaleout_node0.log"
NODE1_LOG="/tmp/hpc_scaleout_node1.log"
COORD_OUT="/tmp/hpc_scaleout_coord.out"
COORD_ERR="/tmp/hpc_scaleout_coord.err"
TSV="/tmp/hpc_scaleout_summary.tsv"

ssh_run() { ssh -p 22 "${SSH_USER}@$1" "${@:2}"; }
log() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

printf 'phase\tops_sec\thits\tp50_ms\tp99_ms\twall_s\tnote\n' > "$TSV"

# ============================================================================
# Manifest 生成
# ============================================================================
write_manifests() {
    log "Writing manifests"
    # Node0: 只有本地 warm region
    ssh_run "$NODE0_HOST" "cat >$NODE0_MANIFEST" <<'YAML'
local_ub_node_id: 0
local_region_weight: 4
remote_meta_provider: ub
remote_meta_path: PAYLOAD_LOCAL
remote_meta_mmap_offset: META_OFFSET
remote_meta_entries: META_ENTRIES
remote_meta_buckets: META_BUCKETS
ub_rpc_timeout_ms: UB_RPC_TIMEOUT
warm_regions:
  - region_id: 100
    provider: ub
    path: PAYLOAD_LOCAL
    mmap_offset: 0
    bytes: WR_BYTES
    value_size: VALUE_SIZE
    home_ub_node_id: 0
    weight: 1
YAML
    # 用 sed 替换占位符（heredoc 里变量不展开）
    ssh_run "$NODE0_HOST" "sed -i \
        -e 's|PAYLOAD_LOCAL|$PAYLOAD_LOCAL|g' \
        -e 's|META_OFFSET|$META_OFFSET|g' \
        -e 's|META_ENTRIES|$META_ENTRIES|g' \
        -e 's|META_BUCKETS|$META_BUCKETS|g' \
        -e 's|UB_RPC_TIMEOUT|$UB_RPC_TIMEOUT|g' \
        -e 's|WR_BYTES|$WR_BYTES|g' \
        -e 's|VALUE_SIZE|$VALUE_SIZE|g' \
        $NODE0_MANIFEST"

    # Node1: 本地 warm region + node0 的远端 warm region + ub_rpc_peers
    ssh_run "$NODE1_HOST" "cat >$NODE1_MANIFEST" <<'YAML'
local_ub_node_id: 1
local_region_weight: 4
remote_meta_provider: ub
remote_meta_path: PAYLOAD_LOCAL
remote_meta_mmap_offset: META_OFFSET
remote_meta_entries: META_ENTRIES
remote_meta_buckets: META_BUCKETS
ub_rpc_timeout_ms: UB_RPC_TIMEOUT
warm_regions:
  - region_id: 101
    provider: ub
    path: PAYLOAD_LOCAL
    mmap_offset: 0
    bytes: WR_BYTES
    value_size: VALUE_SIZE
    home_ub_node_id: 1
    weight: 1
  - region_id: 100
    provider: ub
    path: PAYLOAD_PEER
    mmap_offset: 0
    bytes: WR_BYTES
    value_size: VALUE_SIZE
    home_ub_node_id: 0
    weight: 1
remote_meta_views:
  - owner_id: 0
    provider: ub
    path: PAYLOAD_PEER
    mmap_offset: META_OFFSET
    entries: META_ENTRIES
    buckets: META_BUCKETS
ub_rpc_peers:
  - owner_id: 0
    provider: ub
    request_path: REQUEST_LOCAL
    request_mmap_offset: 8388608
    response_path: RESPONSE_PEER
    response_mmap_offset: 16777216
    inbound_request_path: REQUEST_PEER
    inbound_request_mmap_offset: 8388608
    outbound_response_path: RESPONSE_LOCAL
    outbound_response_mmap_offset: 16777216
YAML
    ssh_run "$NODE1_HOST" "sed -i \
        -e 's|PAYLOAD_LOCAL|$PAYLOAD_LOCAL|g' \
        -e 's|PAYLOAD_PEER|$PAYLOAD_PEER|g' \
        -e 's|REQUEST_LOCAL|$REQUEST_LOCAL|g' \
        -e 's|REQUEST_PEER|$REQUEST_PEER|g' \
        -e 's|RESPONSE_LOCAL|$RESPONSE_LOCAL|g' \
        -e 's|RESPONSE_PEER|$RESPONSE_PEER|g' \
        -e 's|META_OFFSET|$META_OFFSET|g' \
        -e 's|META_ENTRIES|$META_ENTRIES|g' \
        -e 's|META_BUCKETS|$META_BUCKETS|g' \
        -e 's|UB_RPC_TIMEOUT|$UB_RPC_TIMEOUT|g' \
        -e 's|WR_BYTES|$WR_BYTES|g' \
        -e 's|VALUE_SIZE|$VALUE_SIZE|g' \
        $NODE1_MANIFEST"

    # Node0 peer-view map（扩容时用）
    ssh_run "$NODE0_HOST" "cat >$NODE0_PEER_MAP" <<'YAML'
expected_local_owner_id: 0
attach_now: true
ub_rpc_timeout_ms: UB_RPC_TIMEOUT
warm_regions:
  - region_id: 101
    provider: ub
    path: PAYLOAD_PEER
    mmap_offset: 0
    bytes: WR_BYTES
    value_size: VALUE_SIZE
    home_ub_node_id: 1
    weight: 1
remote_meta_views:
  - owner_id: 1
    provider: ub
    path: PAYLOAD_PEER
    mmap_offset: META_OFFSET
    entries: META_ENTRIES
    buckets: META_BUCKETS
ub_rpc_peers:
  - owner_id: 1
    provider: ub
    request_path: REQUEST_LOCAL
    request_mmap_offset: 8388608
    response_path: RESPONSE_PEER
    response_mmap_offset: 16777216
    inbound_request_path: REQUEST_PEER
    inbound_request_mmap_offset: 8388608
    outbound_response_path: RESPONSE_LOCAL
    outbound_response_mmap_offset: 16777216
YAML
    ssh_run "$NODE0_HOST" "sed -i \
        -e 's|PAYLOAD_PEER|$PAYLOAD_PEER|g' \
        -e 's|REQUEST_LOCAL|$REQUEST_LOCAL|g' \
        -e 's|REQUEST_PEER|$REQUEST_PEER|g' \
        -e 's|RESPONSE_PEER|$RESPONSE_PEER|g' \
        -e 's|RESPONSE_LOCAL|$RESPONSE_LOCAL|g' \
        -e 's|META_OFFSET|$META_OFFSET|g' \
        -e 's|META_ENTRIES|$META_ENTRIES|g' \
        -e 's|META_BUCKETS|$META_BUCKETS|g' \
        -e 's|UB_RPC_TIMEOUT|$UB_RPC_TIMEOUT|g' \
        -e 's|WR_BYTES|$WR_BYTES|g' \
        -e 's|VALUE_SIZE|$VALUE_SIZE|g' \
        $NODE0_PEER_MAP"
}

# ============================================================================
# 启动/停止集成 redis-server
# ============================================================================
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

stop_node() {
    local host=$1
    ssh_run "$host" "pkill -9 -f 'redis-server.*:$PORT ' 2>/dev/null; sleep 0.5" || true
}

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
run_memtier() {
    # $1=host $2=test_time $3=outfile $4=extra_flags
    local host=$1 tt=$2 outfile=$3 extra=$4
    ssh_run "$host" "numactl --membind=1 taskset -c 96-191 \
        $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
        -s $host -p $PORT -t $MEMTIER_T -c $MEMTIER_C --pipeline=$PIPELINE \
        --ratio=0:1 --key-pattern=R:R --key-prefix=item: \
        --key-minimum=1 --key-maximum=$PREFILL_KEYS \
        --test-time=$tt $extra >$outfile 2>&1" || true
    # 解析结果
    local tot; tot=$(ssh_run "$host" "grep '^Totals' $outfile 2>/dev/null | tail -1")
    local ops hits p50 p99
    ops=$(echo "$tot" | awk '{print $2}')
    hits=$(echo "$tot" | awk '{print $3}')
    p50=$(echo "$tot" | awk '{print $6}')
    p99=$(echo "$tot" | awk '{print $7}')
    [ -z "$ops" ] && ops=0
    echo "$ops $hits $p50 $p99"
}

run_memtier_bg() {
    # 后台启动 memtier，test-time 由调用方决定
    local host=$1 outfile=$2 bg_time=$3
    ssh_run "$host" "numactl --membind=1 taskset -c 96-191 \
        $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
        -s $host -p $PORT -t $MEMTIER_T -c $MEMTIER_C --pipeline=$PIPELINE \
        --ratio=0:1 --key-pattern=R:R --key-prefix=item: \
        --key-minimum=1 --key-maximum=$PREFILL_KEYS \
        --test-time=$bg_time >$outfile 2>&1 &" || true
}

stop_memtier_bg() {
    ssh_run "$NODE0_HOST" "pkill -9 memtier_benchmark 2>/dev/null" || true
}

# 从后台 memtier 日志提取结果：Totals 行优先；若无（被 kill），从最后一行进度提取
parse_bg_out() {
    local host=$1 outfile=$2
    local tot ops hits p50 p99
    tot=$(ssh_run "$host" "grep '^Totals' $outfile 2>/dev/null | tail -1")
    if [ -n "$tot" ]; then
        ops=$(echo "$tot" | awk '{print $2}')
        hits=$(echo "$tot" | awk '{print $3}')
        p50=$(echo "$tot" | awk '{print $6}')
        p99=$(echo "$tot" | awk '{print $7}')
    else
        # 从最后一行进度提取: [RUN #1 53%, 160 secs] 64 threads: N ops, X ops/sec
        local last
        last=$(ssh_run "$host" "grep -E '^\[RUN #1 [0-9]+%, *[0-9]+ secs\]' $outfile 2>/dev/null | tail -1")
        ops=$(echo "$last" | sed -E 's/.*: *([0-9]+) ops,.*/\1/')
        # 用最后一秒的 ops/sec（更准确反映稳态）+ 总 ops 反推 avg
        local last_rate
        last_rate=$(echo "$last" | sed -E 's/.*, *([0-9]+) \(avg.*ops\/sec.*/\1/')
        ops="${last_rate}"  # 用最后一秒的瞬时速率
        hits="$ops"
        p50="NA"
        p99="NA"
    fi
    [ -z "$ops" ] && ops=0
    echo "$ops $hits $p50 $p99"
}

record_phase() {
    local phase=$1 ops=$2 hits=$3 p50=$4 p99=$5 wall=$6 note=$7
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$phase" "$ops" "$hits" "$p50" "$p99" "$wall" "$note" >> "$TSV"
    log "$phase: ops=$ops p99=$p99 wall=${wall}s ($note)"
}

prefill_data() {
    log "Prefilling $PREFILL_KEYS vectors to node0"
    ssh_run "$NODE0_HOST" "numactl --membind=1 taskset -c 96-191 \
        $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
        -s $NODE0_HOST -p $PORT -t 32 -c 4 --pipeline=32 \
        --ratio=1:0 --key-pattern=S:S --key-prefix=item: \
        --key-minimum=1 --key-maximum=$PREFILL_KEYS -n $PREFILL_KEYS \
        >/tmp/hpc_scaleout_prefill.log 2>&1"
}

# ============================================================================
# 主流程
# ============================================================================

log "Stopping old processes"
stop_node "$NODE0_HOST"
stop_node "$NODE1_HOST"

write_manifests

log "Starting node0 (集成 redis-server)"
start_node "$NODE0_HOST" "$NODE0_MANIFEST" "$NODE0_LOG" 1
wait_port "$NODE0_HOST" || { echo "FAIL: node0 not listening"; exit 1; }

log "Starting node1 (集成 redis-server)"
start_node "$NODE1_HOST" "$NODE1_MANIFEST" "$NODE1_LOG" 1
wait_port "$NODE1_HOST" || { echo "FAIL: node1 not listening"; exit 1; }
sleep 2

log "Verify startup"
ssh_run "$NODE0_HOST" "grep -E 'remote meta ready|ub rpc ready|server ready' $NODE0_LOG" || true
ssh_run "$NODE1_HOST" "grep -E 'remote meta ready|registered.*remote meta|ub rpc ready|server ready' $NODE1_LOG" || true

log "Publish initial topology active={0} (epoch=$INIT_EPOCH)"
ssh_run "$NODE0_HOST" "cd $REMOTE_DIR && ./benchmark/vemb_v16_topology_ctl --set --transport tcp \
    --host $NODE0_HOST --port $PORT --epoch $INIT_EPOCH --min-write-epoch $INIT_EPOCH \
    --active 0 --standby 0 \
    --owner-endpoints 0=$NODE0_HOST:$PORT \
    --timeout-ms $CONTROL_TIMEOUT" || { echo "FAIL: initial topology"; exit 1; }

prefill_data

OUTDIR="/tmp/hpc_scaleout_raw"
ssh_run "$NODE0_HOST" "mkdir -p $OUTDIR"

# ============================================================================
# 扩容过程：三段采集
# ============================================================================
log "========== 扩容过程 =========="

# 段1: baseline (active={0})
log "段1: baseline VEMB read (${TEST_TIME}s)"
T0=$(date +%s)
result=$(run_memtier "$NODE0_HOST" "$TEST_TIME" "$OUTDIR/scaleout_baseline.txt" "")
T1=$(date +%s)
read ops hits p50 p99 <<< "$result"
record_phase "scaleout_baseline" "$ops" "$hits" "$p50" "$p99" "$((T1-T0))" "active={0}"

# 段2: during scaleout (后台 memtier + 触发扩容)
log "段2: during scaleout (background VEMB ${BG_TIME_SCALEOUT}s + topology change)"
run_memtier_bg "$NODE0_HOST" "$OUTDIR/scaleout_during.txt" "$BG_TIME_SCALEOUT"
T0=$(date +%s)

# 启动 coordinator
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

# 等扩容完成
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

result=$(parse_bg_out "$NODE0_HOST" "$OUTDIR/scaleout_during.txt")
read ops hits p50 p99 <<< "$result"
record_phase "during_scaleout" "$ops" "$hits" "${p50:-NA}" "${p99:-NA}" "$SCALEOUT_WALL" "active={0}->{0,1}"

ssh_run "$NODE0_HOST" "cat $COORD_OUT; echo '---'; cat $COORD_ERR 2>/dev/null" || true

# 段3: after scaleout (active={0,1})
log "段3: after scaleout VEMB read (${TEST_TIME}s)"
T0=$(date +%s)
result=$(run_memtier "$NODE0_HOST" "$TEST_TIME" "$OUTDIR/scaleout_after.txt" "")
T1=$(date +%s)
read ops hits p50 p99 <<< "$result"
record_phase "scaleout_after" "$ops" "$hits" "$p50" "$p99" "$((T1-T0))" "active={0,1}"

# ============================================================================
# 缩容过程：三段采集
# ============================================================================
log "========== 缩容过程 =========="

# 段1 已经是 active={0,1} 的 baseline（就是上面的 scaleout_after）

# 段2: during shrink
log "段2: during shrink (background VEMB ${BG_TIME_SHRINK}s + topology change)"
run_memtier_bg "$NODE0_HOST" "$OUTDIR/shrink_during.txt" "$BG_TIME_SHRINK"
T0=$(date +%s)

# 发布缩容拓扑: active={0} (移除 node1)
ssh_run "$NODE0_HOST" "cd $REMOTE_DIR && ./benchmark/vemb_v16_topology_ctl --set --transport tcp \
    --host $NODE0_HOST --port $PORT --epoch $SHRINK_EPOCH --min-write-epoch $SHRINK_EPOCH \
    --active 0 --standby 0 \
    --owner-endpoints 0=$NODE0_HOST:$PORT \
    --timeout-ms $COMBINED_TIMEOUT"

# 等待缩容完成（top_ctl 阻塞返回即完成）
T1=$(date +%s)
SHRINK_WALL=$((T1-T0))

# 等后台 memtier 跑完
log "等待后台 memtier 自然结束（剩 $((BG_TIME_SHRINK - SHRINK_WALL))s）"
REMAIN=$((BG_TIME_SHRINK - SHRINK_WALL))
if [ "$REMAIN" -gt 0 ]; then
    sleep "$REMAIN"
fi
sleep 2

result=$(parse_bg_out "$NODE0_HOST" "$OUTDIR/shrink_during.txt")
read ops hits p50 p99 <<< "$result"
record_phase "during_shrink" "$ops" "$hits" "${p50:-NA}" "${p99:-NA}" "$SHRINK_WALL" "active={0,1}->{0}"

# 段3: after shrink (active={0})
log "段3: after shrink VEMB read (${TEST_TIME}s)"
T0=$(date +%s)
result=$(run_memtier "$NODE0_HOST" "$TEST_TIME" "$OUTDIR/shrink_after.txt" "")
T1=$(date +%s)
read ops hits p50 p99 <<< "$result"
record_phase "shrink_after" "$ops" "$hits" "$p50" "$p99" "$((T1-T0))" "active={0}"

# ============================================================================
# 清理
# ============================================================================
log "Cleanup"
stop_memtier_bg || true
stop_node "$NODE0_HOST"
stop_node "$NODE1_HOST"

log "DONE — $TSV"
cat "$TSV"
