#!/usr/bin/env bash
# ============================================================================
# run_redis_cluster_vemb.sh
# 4 节点 redis cluster (baseline redis-8.6.3) VEMB 吞吐 / 延迟测试
#
# 拓扑: HW01 / HW02 / HW05 / HW04 每节点 N 个 redis 实例, cluster mode
# 网络: 192.168.1.x (100G mlx5 直连), client 端口 7000+, cluster bus 17000+
# 测试: prefill 多 vset (分散到 4 节点) -> memtier --cluster-mode VEMB 聚合测
#
# 用法:
#   bash benchmark/run_redis_cluster_vemb.sh                       # 完整 (默认 30s/档)
#   TEST_TIME=3 bash benchmark/run_redis_cluster_vemb.sh           # smoke 快验
#   THREADS="16" bash benchmark/run_redis_cluster_vemb.sh          # 指定线程档
#   IO_THREADS=4 bash benchmark/run_redis_cluster_vemb.sh          # 指定 io-threads (默认 4)
#   INSTANCES_PER_NODE=1 bash benchmark/run_redis_cluster_vemb.sh  # 每节点实例数 (默认 1)
#   KEY_OFFSET=5 bash benchmark/run_redis_cluster_vemb.sh  # 手动指定 key 起始 (默认自动选均匀分布)
#
# 编译口径 (四节点一致):
#   make -C deps jemalloc && \
#   make CFLAGS="-O2 -pipe -fno-lto" LDFLAGS="-O2 -pipe -fno-lto" CC="gcc -fuse-ld=bfd"
# ============================================================================

set -uo pipefail   # 不用 -e: cluster 偶发 MOVED / 重连不应整体退出

# === 节点 (ssh Host 别名 + cluster announce IP) ===
declare -a NODES=("HW01" "HW02" "HW05" "HW04")
declare -a IPS=("192.168.1.111" "192.168.1.112" "192.168.1.20" "192.168.1.21")
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
PIPELINE=${PIPELINE:-32}
IO_THREADS=${IO_THREADS:-4}
INSTANCES_PER_NODE=${INSTANCES_PER_NODE:-1}

# === 数据规模 ===
NUM_VSETS=${NUM_VSETS:-16}                 # vset 数 (分散到 4 节点, 每节点 ~4)
VECTORS_PER_VSET=${VECTORS_PER_VSET:-6250} # 每 vset 向量数
DIM=${DIM:-300}                            # 向量维度 (VEMB 响应 ~1KB/op)

# === CPU 绑核 ===
CORES_PER_NODE=${CORES_PER_NODE:-96}       # 每节点核数 (node0: 0-95)

# === 输出 ===
OUTDIR=${OUTDIR:-/tmp/redis_cluster_vemb}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RAWDIR="$OUTDIR/raw"
TSV="$OUTDIR/summary_${TIMESTAMP}.tsv"

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"
ulimit -n 65536   # t16×c50×多实例 需要 >1024 fd (默认软限 1024 会 "Too many open files")
mkdir -p "$RAWDIR"

log()  { echo "[$(date +%H:%M:%S)] $*"; }

ssh_node() { local i=$1; shift; ssh $SSH_OPTS "${NODES[$i]}" "$@" 2>&1 | grep -v "Authorized users"; }

# 搜索使 vset{off}..vset{off+N-1} 在 4 节点 (cluster 默认连续 slot 区间) 分布最均匀的起始 offset
# 找到精确均匀 (每节点 N/4) 立即返回; N 不能被 4 整除时取偏差最小
pick_key_offset() {
    NUM_VSETS=$NUM_VSETS python3 -c "
import os
def crc16(data):
    crc = 0
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if (crc & 0x8000) else (crc << 1)
            crc &= 0xFFFF
    return crc
N = int(os.environ.get('NUM_VSETS', '16'))
best_off, best_spread = 1, 1 << 30
for off in range(1, 5000):
    cnt = [0, 0, 0, 0]
    for i in range(N):
        cnt[(crc16(('vset%d' % (off + i)).encode()) % 16384) // 4096] += 1
    spread = max(cnt) - min(cnt)
    if spread < best_spread:
        best_spread, best_off = spread, off
        if spread == 0:
            break
print(best_off)
" 2>/dev/null
}

# ----------------------------------------------------------------------------
cleanup_node() {
    local i=$1
    # 直接 ssh + 静默 (不经 ssh_node 的 grep 管道, 避免 trap 时 "Killed" 噪音)
    # 用 -x 按进程名精确匹配; -f 模式会匹配到自己的 bash 命令行(含 "redis-server" 字串)导致自杀,
    # rm 永远不执行, 旧 nodes.conf 残留, 下次 --cluster create 看到旧集群状态而失败
    # memtier_benchmark 进程名被内核截断为 "memtier_benchm" (15 字符上限), 必须用截断名匹配
    ssh $SSH_OPTS "${NODES[$i]}" "pkill -9 -x redis-server 2>/dev/null; \
                  pkill -9 -x memtier_benchm 2>/dev/null; \
                  rm -rf $DATA_DIR/inst* 2>/dev/null; true" \
        >/dev/null 2>&1
}

cleanup_all() {
    log "cleanup all nodes..."
    for ((i=0; i<NNODES; i++)); do cleanup_node $i; done
}

# ----------------------------------------------------------------------------
start_node() {
    local i=$1 ip=${IPS[$i]}
    local cores_per_inst=$((CORES_PER_NODE / INSTANCES_PER_NODE))
    for ((j=0; j<INSTANCES_PER_NODE; j++)); do
        local port=$((PORT + j))
        local c0=$((j * cores_per_inst))
        local c1=$(((j + 1) * cores_per_inst - 1))
        local ddir="$DATA_DIR/inst${j}"
        log "  ${NODES[$i]} inst$j: port=$port cores=$c0-$c1 dir=$ddir"
        ssh_node $i "mkdir -p $ddir && cd $REDIS_DIR && \
            numactl --membind=0 taskset -c $c0-$c1 \
            ./src/redis-server \
                --port $port --bind 0.0.0.0 --protected-mode no \
                --cluster-enabled yes \
                --cluster-config-file nodes.conf \
                --cluster-node-timeout $CLUSTER_TIMEOUT \
                --cluster-announce-ip $ip \
                --io-threads $IO_THREADS --io-threads-do-reads yes \
                --appendonly no --save '' \
                --dir $ddir --logfile $ddir/redis.log \
                --daemonize yes" >/dev/null
    done
}

wait_port() {
    local ip=$1 port=$2 count=0
    while ! ($REDIS_DIR/src/redis-cli -h $ip -p $port PING 2>/dev/null | grep -q PONG); do
        sleep 0.5; ((count++))
        [ $count -gt 60 ] && { log "TIMEOUT waiting $ip:$port"; return 1; }
    done
}

# ----------------------------------------------------------------------------
create_cluster() {
    local ntotal=$((NNODES * INSTANCES_PER_NODE))
    log "create cluster (--cluster-replicas 0, $ntotal masters)..."
    local endpoints=""
    for ((i=0; i<NNODES; i++)); do
        for ((j=0; j<INSTANCES_PER_NODE; j++)); do
            endpoints="$endpoints ${IPS[$i]}:$((PORT + j))"
        done
    done
    # 必须 echo yes (输出 "yes"); 用 yes|输出 "y" 会被 redis-cli 拒绝 (要求 "yes")
    echo yes | $REDIS_DIR/src/redis-cli --cluster create $endpoints --cluster-replicas 0 2>&1 \
        | grep -E "Slots|Master|slots:|OK|All|coverage|agree|Can't|err" | head -40
}

check_cluster() {
    log "cluster info:"
    $REDIS_DIR/src/redis-cli -h ${IPS[0]} -p $PORT CLUSTER INFO 2>/dev/null \
        | grep -E "cluster_state|cluster_slots_ok|cluster_known_nodes|cluster_size"
    log "nodes:"
    $REDIS_DIR/src/redis-cli -h ${IPS[0]} -p $PORT CLUSTER NODES 2>/dev/null \
        | awk '{print $2, $3, $NF}' | head -30
    log "vset slot distribution (vset$KEY_OFFSET..vset$((KEY_OFFSET+NUM_VSETS-1))):"
    for v in $(seq $KEY_OFFSET $((KEY_OFFSET + NUM_VSETS - 1))); do
        local slot=$($REDIS_DIR/src/redis-cli -h ${IPS[0]} -p $PORT CLUSTER KEYSLOT vset$v 2>/dev/null)
        printf "  vset%-3d -> slot %s\n" "$v" "$slot"
    done
}

# ----------------------------------------------------------------------------
prefill() {
    local v0=$KEY_OFFSET v1=$((KEY_OFFSET + NUM_VSETS - 1))
    log "prefill: vset$v0..vset$v1 ($NUM_VSETS vsets) x $VECTORS_PER_VSET vectors (dim=$DIM)..."
    # FLUSHALL 清掉重跑残留 (cluster FLUSHALL 同步所有节点)
    $REDIS_DIR/src/redis-cli -c -h ${IPS[0]} -p $PORT FLUSHALL >/dev/null 2>&1
    # awk 生成 VADD 命令文本, 喂给 redis-cli -c (cluster-aware: 缓存 slot map 后直接路由)
    awk -v off=$KEY_OFFSET -v m=$NUM_VSETS -v k=$VECTORS_PER_VSET -v dim=$DIM 'BEGIN{
        srand(42);
        for (i=0; i<m; i++) {
            v = off + i;
            for (e=0; e<k; e++) {
                printf "VADD vset%d VALUES %d", v, dim;
                for (j=0; j<dim; j++) printf " %f", rand()*0.001;
                printf " elem%d\n", e;
            }
        }
    }' | $REDIS_DIR/src/redis-cli -c -h ${IPS[0]} -p $PORT >/dev/null 2>&1
    log "prefill done. VCARD sample:"
    for v in $v0 $v1; do
        printf "  vset%d VCARD=%s\n" "$v" \
            "$($REDIS_DIR/src/redis-cli -c -h ${IPS[0]} -p $PORT VCARD vset$v 2>/dev/null)"
    done
}

# ----------------------------------------------------------------------------
# 采集节点上所有 redis-server 进程的 jiffies 总和 (utime+stime, 所有线程求和)
# 用于计算 redis-server 实际占用核数 (不含 memtier)
snapshot_jiffies_node() {
    local i=$1
    ssh $SSH_OPTS "${NODES[$i]}" \
        "total=0; \
         for pid in \$(pgrep -x redis-server); do \
             j=\$(awk '{s+=\$14+\$15} END{print s+0}' /proc/\$pid/task/*/stat 2>/dev/null); \
             total=\$((total + \${j:-0})); \
         done; \
         echo \$total" \
        2>/dev/null | tail -1
}

# ----------------------------------------------------------------------------
run_vemb_test() {
    log "VEMB cluster test (memtier --cluster-mode, VEMB __key__ elem0)..."
    printf "threads\tclients\tpipeline\tops_sec\tavg_lat_ms\tp50_ms\tp99_ms\tp999_ms\tkb_sec\tcores_used\n" > "$TSV"
    for t in $THREADS; do
        local raw="$RAWDIR/vemb_t${t}.log"
        log "  t=$t c=$CLIENTS pipeline=$PIPELINE io=$IO_THREADS time=${TEST_TIME}s"
        # memtier 前采 jiffies (4 节点 redis-server 总和)
        local jb=0
        for ((i=0; i<NNODES; i++)); do
            jb=$((jb + $(snapshot_jiffies_node $i)))
        done
        # memtier cluster 模式: __key__=vset 名 (第一位 key, memtier 与 redis 都用它算 slot -> 一致)
        # elem0 固定; --cluster-mode 自动路由 vset1..N 到各 owner 节点
        $MEMTIER -s ${IPS[0]} -p $PORT --cluster-mode \
            -t $t -c $CLIENTS --pipeline=$PIPELINE \
            --command="VEMB __key__ elem0" --command-key-pattern=R \
            --key-prefix=vset --key-minimum=$KEY_OFFSET --key-maximum=$((KEY_OFFSET + NUM_VSETS - 1)) \
            --data-size=128 \
            --test-time=$TEST_TIME --hide-histogram --select-db=0 \
            > "$raw" 2>&1 || true
        # memtier 后采 jiffies
        local ja=0
        for ((i=0; i<NNODES; i++)); do
            ja=$((ja + $(snapshot_jiffies_node $i)))
        done
        # 实际核数 = jiffies 差 / 100 / 持续秒 (CLK_TCK=100); 仅 redis-server, 不含 memtier
        local cores=$(awk -v d=$((ja - jb)) -v s=$TEST_TIME 'BEGIN{printf "%.2f", d/100.0/s}')
        # cluster 模式 Totals: ops MOVED/sec ASK/sec avg p50 p99 p999 kb (9 tokens 含 "Totals")
        local totals ops moved ask avg p50 p99 p999 kb
        totals=$(grep "^Totals" "$raw" | tail -1)
        read ops moved ask avg p50 p99 p999 kb < <(
            echo "$totals" | awk '{
                if (NF>=9) printf "%s %s %s %s %s %s %s %s", $2,$3,$4,$5,$6,$7,$8,$9
                else       printf "0 NA NA NA NA NA NA NA"
            }'
        )
        printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
            "$t" "$CLIENTS" "$PIPELINE" "$ops" "$avg" "$p50" "$p99" "$p999" "$kb" "$cores" >> "$TSV"
        log "    => ops/s=$ops  avg=${avg}ms  p50=${p50}ms  p99=${p99}ms  kb/s=$kb  cores=$cores"
    done
}

# ============================================================================
trap 'cleanup_all' EXIT INT TERM

log "=== STEP 1: cleanup residuals ==="
if [ -z "${KEY_OFFSET+x}" ]; then
    KEY_OFFSET=$(pick_key_offset)
    KEY_OFFSET=${KEY_OFFSET:-1}
    log "auto KEY_OFFSET=$KEY_OFFSET (NUM_VSETS=$NUM_VSETS) -> 4 节点均匀分布"
else
    log "user KEY_OFFSET=$KEY_OFFSET (NUM_VSETS=$NUM_VSETS)"
fi
cleanup_all
sleep 1

log "=== STEP 2: start $NNODES x $INSTANCES_PER_NODE redis instances ==="
for ((i=0; i<NNODES; i++)); do start_node $i; done
sleep 2
for ((i=0; i<NNODES; i++)); do
    for ((j=0; j<INSTANCES_PER_NODE; j++)); do
        wait_port ${IPS[$i]} $((PORT + j)) || { log "FAIL: ${IPS[$i]}:$((PORT+j)) not up"; exit 1; }
    done
done
log "all $((NNODES * INSTANCES_PER_NODE)) instances up."

log "=== STEP 3: create cluster ==="
create_cluster
# 轮询等待 cluster_state=ok (最长 30s); --cluster create 后立即查可能还是 fail
cstate=""
w=0
for ((w=0; w<30; w++)); do
    cstate=$($REDIS_DIR/src/redis-cli -h ${IPS[0]} -p $PORT CLUSTER INFO 2>/dev/null \
             | awk -F: '/cluster_state/{gsub(/[[:space:]]/,"",$2);print $2}')
    [ "$cstate" = "ok" ] && break
    sleep 1
done
if [ "$cstate" != "ok" ]; then
    log "FAIL: cluster_state=$cstate (expect ok). abort before prefill."
    exit 1
fi
log "cluster_state=ok after ${w}s"
check_cluster

log "=== STEP 4: prefill VEMB data ==="
prefill

log "=== STEP 5: VEMB throughput/latency ==="
run_vemb_test

log "=== DONE ==="
log "TSV  : $TSV"
log "raw  : $RAWDIR/vemb_t*.log"
echo "----- summary -----"
awk -F'\t' '{printf "%7s %7s %9s %13s %12s %10s %10s %10s %13s %11s\n", $1,$2,$3,$4,$5,$6,$7,$8,$9,$10}' "$TSV"
