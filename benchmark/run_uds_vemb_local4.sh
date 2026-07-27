#!/usr/bin/env bash
# ============================================================================
# run_uds_vemb_local4.sh
# 本地 4 redis-server (无 cluster) + UDS VEMB 吞吐/延迟测试
#
# 用途: 给分布式 TCP cluster baseline (run_redis_cluster_vemb.sh) 提供 UDS
#       单机上限对照 —— 消除网卡 + cluster bus + slot map routing 开销后,
#       redis-server + memtier 在纯 UDS 路径上能跑到多少。
#
# 拓扑: 本机起 4 个独立 redis-server (不开 cluster), 每个绑不同 socket path,
#       各占 node0 的 1/4 核 (默认 0-23/24-47/48-71/72-95)。4 个 memtier
#       并发各连一个 UDS, 占 node1 的 1/4 核 (96-119/120-143/144-167/168-191)。
#       prefill 时按 vset 段路由到 4 实例 (inst0=vset[O..O+N/4-1] 等),
#       memtier 各查自己的 vset 段, 聚合 ops/s = 4 实例之和。
#
# 用法:
#   bash benchmark/run_uds_vemb_local4.sh                  # 默认完整跑
#   TEST_TIME=3 bash benchmark/run_uds_vemb_local4.sh      # smoke
#   THREADS="16" bash benchmark/run_uds_vemb_local4.sh     # 单档线程
#   RAW=1 CLIENTS=200 IO_THREADS=4 VECTORS_PER_VSET=625 \
#     bash benchmark/run_uds_vemb_local4.sh                # 跟 cluster baseline 同参数
#
# 编译口径 (同 baseline):
#   make -C deps jemalloc && \
#   make CFLAGS="-O2 -pipe -fno-lto" LDFLAGS="-O2 -pipe -fno-lto" CC="gcc -fuse-ld=bfd"
# ============================================================================

set -uo pipefail

# === 路径 ===
REDIS_DIR=${REDIS_DIR:-/root/gqs/codespace/redis-8.6.3}
MEMTIER=${MEMTIER:-/root/gqs/codespace/UnifiedBus/memtier_benchmark_origin/memtier_benchmark}
DATA_DIR=${DATA_DIR:-/tmp/redis-uds-local4-data}
SOCK_DIR=${SOCK_DIR:-/tmp/redis-uds-local4}

# === 实例 ===
NUM_INST=${NUM_INST:-4}
PORT_BASE=${PORT_BASE:-7000}    # TCP 也开, 便于 redis-cli 管理与排错 (memtier 走 UDS)

# === 测试参数 ===
TEST_TIME=${TEST_TIME:-30}
CLIENTS=${CLIENTS:-50}
THREADS=${THREADS:-"1 4 8 16"}
PIPELINE=${PIPELINE:-32}
IO_THREADS=${IO_THREADS:-4}

# === 数据规模 (默认与 baseline 对齐, 总量 = NUM_VSETS * VECTORS_PER_VSET * DIM * 4B) ===
NUM_VSETS=${NUM_VSETS:-16}                 # 必须能被 NUM_INST 整除 (每实例 NUM_VSETS/NUM_INST)
VECTORS_PER_VSET=${VECTORS_PER_VSET:-6250}
DIM=${DIM:-300}

# === CPU 绑核 (本地场景: server 占 node0, memtier 占 node1) ===
CORES_PER_INST=${CORES_PER_INST:-24}            # 每实例独占核数
MEMTIER_CORE_OFFSET=${MEMTIER_CORE_OFFSET:-96}  # memtier 起始核 (默认 node1 起点)
NUMA_NODE=${NUMA_NODE:-0}                       # redis-server membind 节点

# === RESP / RAW ===
PROTO=${PROTO:-resp2}
case "$PROTO" in resp2|resp3) ;; *) echo "ERROR: PROTO must be resp2 or resp3 (got: $PROTO)"; exit 2;; esac
RAW=${RAW:-0}
RAW_SUFFIX=""
[ "$RAW" = "1" ] && RAW_SUFFIX=" raw"

# === 输出 (按规则放 benchmark/results/<scenario>/<run_id>/) ===
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR=${OUTDIR:-benchmark/results/uds_local4_vemb/${TIMESTAMP}}
RAWDIR="$OUTDIR/raw"
TSV="$OUTDIR/summary.tsv"

ulimit -n 65536
mkdir -p "$RAWDIR" "$DATA_DIR" "$SOCK_DIR"

log()  { echo "[$(date +%H:%M:%S)] $*"; }

# ----------------------------------------------------------------------------
cleanup_all() {
    log "cleanup residuals..."
    pkill -9 -x redis-server 2>/dev/null
    pkill -9 -x memtier_benchm 2>/dev/null   # 内核进程名截断为 15 字符
    rm -rf "$DATA_DIR"/inst* "$SOCK_DIR"/*.sock 2>/dev/null
    mkdir -p "$SOCK_DIR"
}

# ----------------------------------------------------------------------------
start_inst() {
    local i=$1
    local port=$((PORT_BASE + i))
    local sock="$SOCK_DIR/inst${i}.sock"
    local c0=$((i * CORES_PER_INST))
    local c1=$(((i + 1) * CORES_PER_INST - 1))
    local ddir="$DATA_DIR/inst${i}"
    mkdir -p "$ddir"
    log "  inst$i: port=$port sock=$sock cores=$c0-$c1 dir=$ddir"
    numactl --membind=$NUMA_NODE taskset -c $c0-$c1 \
        $REDIS_DIR/src/redis-server \
            --port $port --bind 127.0.0.1 --protected-mode no \
            --unixsocket "$sock" --unixsocketperm 700 \
            --tcp-backlog 16384 \
            --io-threads $IO_THREADS --io-threads-do-reads yes \
            --appendonly no --save '' \
            --dir "$ddir" --logfile "$ddir/redis.log" \
            --daemonize yes
}

wait_sock() {
    local i=$1 sock="$SOCK_DIR/inst${i}.sock" count=0
    while ! $REDIS_DIR/src/redis-cli -s "$sock" PING 2>/dev/null | grep -q PONG; do
        sleep 0.2; ((count++))
        [ $count -gt 50 ] && { log "TIMEOUT waiting $sock"; return 1; }
    done
}

# ----------------------------------------------------------------------------
# 每实例分到一段连续 vset: inst_i 持有 [LO_i, HI_i]
vset_lo_for_inst() { echo $(( (KEY_OFFSET + $1 * VSETS_PER_INST) )); }
vset_hi_for_inst() { echo $(( (KEY_OFFSET + ($1 + 1) * VSETS_PER_INST - 1) )); }

prefill() {
    log "prefill: $NUM_VSETS vsets x $VECTORS_PER_VSET vectors (dim=$DIM) split across $NUM_INST instances..."
    local i
    for ((i=0; i<NUM_INST; i++)); do
        local sock="$SOCK_DIR/inst${i}.sock"
        local lo=$(vset_lo_for_inst $i)
        local hi=$(vset_hi_for_inst $i)
        log "  inst$i <- vset[$lo..$hi]"
        # FLUSHDB 每实例 (不是 FLUSHALL, 避免误伤)
        $REDIS_DIR/src/redis-cli -s "$sock" FLUSHDB >/dev/null 2>&1
        # awk 生成 VADD 命令直接喂给对应实例的 redis-cli
        awk -v lo=$lo -v hi=$hi -v k=$VECTORS_PER_VSET -v dim=$DIM 'BEGIN{
            srand(42 + lo);
            for (v=lo; v<=hi; v++) {
                for (e=0; e<k; e++) {
                    printf "VADD vset%d VALUES %d", v, dim;
                    for (j=0; j<dim; j++) printf " %f", rand()*0.001;
                    printf " elem%d\n", e;
                }
            }
        }' | $REDIS_DIR/src/redis-cli -s "$sock" >/dev/null 2>&1
        log "    inst$i VCARD: $(for v in $lo $hi; do printf "vset%s=%s " $v "$($REDIS_DIR/src/redis-cli -s "$sock" VCARD vset$v 2>/dev/null)"; done)"
    done
}

# ----------------------------------------------------------------------------
# 采单实例 redis-server jiffies 总和 (所有线程)
# 注意: redis-server --daemonize yes 后 /proc/pid/cmdline 会被改写成
#       "redis-server 127.0.0.1:PORT" 形式 (原始 --port/--dir 全丢)
#       所以按 ":PORT" 后缀匹配; 用 -E 词界避免 :7000 误匹 :70001
snapshot_jiffies_inst() {
    local i=$1
    local port=$((PORT_BASE + i))
    local total=0 j
    for pid in $(pgrep -x redis-server); do
        if tr '\0' ' ' < /proc/$pid/cmdline 2>/dev/null | grep -Eq ":${port}\$|:${port} "; then
            j=$(awk '{s+=$14+$15} END{print s+0}' /proc/$pid/task/*/stat 2>/dev/null)
            total=$((total + ${j:-0}))
        fi
    done
    echo $total
}

# ----------------------------------------------------------------------------
# 单线程档: 启动 NUM_INST 个 memtier 并发, 各连自己 UDS, 查自己 vset 段
run_vemb_test() {
    log "VEMB UDS test [PROTO=$PROTO RAW=$RAW]: $NUM_INST memtier x (t=$THREADS c=$CLIENTS pipeline=$PIPELINE) ..."
    printf "proto\traw\tthreads\tclients\tpipeline\tops_sec_total\tavg_lat_ms\tp50_ms\tp99_ms\tp999_ms\tkb_sec_total\tcores_total\n" > "$TSV"
    local proto_flag=""
    [ "$PROTO" = "resp3" ] && proto_flag="--protocol=$PROTO"

    for t in $THREADS; do
        log "  t=$t c=$CLIENTS pipeline=$PIPELINE io=$IO_THREADS time=${TEST_TIME}s"
        # 前采 jiffies (4 实例总和)
        local jb=0
        for ((i=0; i<NUM_INST; i++)); do
            jb=$((jb + $(snapshot_jiffies_inst $i)))
        done

        # 每个 memtier 绑到 node1 的 1/NUM_INST 段; 后台启动; stdout 落各自 raw
        local pids=()
        for ((i=0; i<NUM_INST; i++)); do
            local sock="$SOCK_DIR/inst${i}.sock"
            local lo=$(vset_lo_for_inst $i)
            local hi=$(vset_hi_for_inst $i)
            local c0=$((MEMTIER_CORE_OFFSET + i * CORES_PER_INST))
            local c1=$(((MEMTIER_CORE_OFFSET + (i + 1) * CORES_PER_INST - 1)))
            local raw="$RAWDIR/vemb_t${t}_inst${i}.log"
            # memtier UDS: -s socket (不指定 -p); --command 用引号包住空格
            # memtier UDS 用 -S/--unix-socket (不是 -s; -s 是 hostname 会触发 DNS)
            # 显式 bash 包一层 ulimit: 高 c (200) × t (16) = 3200 conn > 默认 1024 fd
            # taskset 后 bash -c 子进程 ulimit 提到 200000, exec memtier 继承
            taskset -c $c0-$c1 bash -c "ulimit -n 200000; exec $MEMTIER -S '$sock' \
                $proto_flag \
                -t $t -c $CLIENTS --pipeline=$PIPELINE \
                --command='VEMB __key__ elem0${RAW_SUFFIX}' --command-key-pattern=R \
                --key-prefix=vset --key-minimum=$lo --key-maximum=$hi \
                --data-size=128 \
                --test-time=$TEST_TIME --hide-histogram --select-db=0" \
                > "$raw" 2>&1 &
            pids+=($!)
        done
        # 等所有 memtier 结束 (test-time 控制自然退出)
        for pid in "${pids[@]}"; do wait $pid || true; done

        # 后采 jiffies
        local ja=0
        for ((i=0; i<NUM_INST; i++)); do
            ja=$((ja + $(snapshot_jiffies_inst $i)))
        done
        local cores=$(awk -v d=$((ja - jb)) -v s=$TEST_TIME 'BEGIN{printf "%.2f", d/100.0/s}')

        # 聚合 4 个 memtier 的 Totals (ops/s 与 kb/s 求和; 延迟取 4 路最大值代表最差路径)
        local ops_sum=0 kb_sum=0 avg_max=0 p50_max=0 p99_max=0 p999_max=0
        for ((i=0; i<NUM_INST; i++)); do
            local raw="$RAWDIR/vemb_t${t}_inst${i}.log"
            local totals ops avg p50 p99 p999 kb
            totals=$(grep "^Totals" "$raw" | tail -1)
            read ops avg p50 p99 p999 kb < <(
                echo "$totals" | awk '{
                    if (NF>=7) printf "%s %s %s %s %s %s", $2,$3,$4,$5,$6,$7
                    else       printf "0 NA NA NA NA NA"
                }'
            )
            ops_sum=$(awk -v s=$ops_sum -v o=$ops 'BEGIN{printf "%.2f", s+o}')
            kb_sum=$(awk -v s=$kb_sum -v k=$kb 'BEGIN{printf "%.2f", s+k}')
            avg_max=$(awk -v m=$avg_max -v a=$avg 'BEGIN{print (a+0>m+0)?a:m}')
            p50_max=$(awk -v m=$p50_max -v a=$p50 'BEGIN{print (a+0>m+0)?a:m}')
            p99_max=$(awk -v m=$p99_max -v a=$p99 'BEGIN{print (a+0>m+0)?a:m}')
            p999_max=$(awk -v m=$p999_max -v a=$p999 'BEGIN{print (a+0>m+0)?a:m}')
        done
        printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
            "$PROTO" "$RAW" "$t" "$CLIENTS" "$PIPELINE" \
            "$ops_sum" "$avg_max" "$p50_max" "$p99_max" "$p999_max" "$kb_sum" "$cores" >> "$TSV"
        log "    => [${PROTO}/raw=${RAW}] total_ops/s=$ops_sum  max_avg=${avg_max}ms  max_p50=${p50_max}ms  max_p99=${p99_max}ms  total_kb/s=$kb_sum  cores=$cores"
    done
}

# ============================================================================
trap 'cleanup_all' EXIT INT TERM

log "=== STEP 1: cleanup residuals ==="
if [ $((NUM_VSETS % NUM_INST)) -ne 0 ]; then
    echo "ERROR: NUM_VSETS=$NUM_VSETS must be divisible by NUM_INST=$NUM_INST (each instance holds NUM_VSETS/NUM_INST vsets)"
    exit 2
fi
VSETS_PER_INST=$((NUM_VSETS / NUM_INST))
KEY_OFFSET=${KEY_OFFSET:-1}
log "KEY_OFFSET=$KEY_OFFSET NUM_VSETS=$NUM_VSETS VSETS_PER_INST=$VSETS_PER_INST"
cleanup_all
sleep 1

log "=== STEP 2: start $NUM_INST local redis instances (UDS) ==="
for ((i=0; i<NUM_INST; i++)); do start_inst $i; done
sleep 1
for ((i=0; i<NUM_INST; i++)); do
    wait_sock $i || { log "FAIL: inst$i socket not up"; exit 1; }
done
log "all $NUM_INST instances up."

log "=== STEP 3: prefill VEMB data (split by vset range) ==="
prefill

log "=== STEP 4: VEMB throughput/latency (4 memtier x UDS) ==="
run_vemb_test

log "=== DONE ==="
log "TSV  : $TSV"
log "raw  : $RAWDIR/vemb_t*_inst*.log"
echo "----- summary [PROTO=$PROTO RAW=$RAW] -----"
awk -F'\t' '{printf "%6s %4s %7s %7s %9s %14s %12s %10s %10s %10s %14s %11s\n", $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12}' "$TSV"
