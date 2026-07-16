#!/bin/bash
# ============================================================================
# AIGCode 命令字 baseline (redis-8.6.3) vs hpc-redis 性能对比
#
# 覆盖 12 个 RESP 标准命令：SET/GET/SET_P/GET_P/HSET/HGET/
#                            LPUSH/SADD/ZADD/LRANGE/SMEMBERS/ZRANGE
# （不含向量命令 VEMB/VSIM/VADD）
#
# 配对：
#   baseline  = redis-8.6.3               + memtier_benchmark_origin
#   hpc       = hpc-redis/src/redis-server + memtier_benchmark (with VEMB V16 SDK)
#
# 拓扑：本地回环（HW01）
#   server = numactl -N 0 -l taskset -c 0-95
#   client = numactl -N 1    taskset -c 96-191
#
# 协议：全部走 RESP（公平对比 —— hpc-redis 启动不带 --vemb-v16-enabled）
#
# 用法：
#   bash benchmark/compare_redis_basic_cmds.sh                # 完整跑（默认 60s/档）
#   TEST_TIME=3 bash benchmark/compare_redis_basic_cmds.sh    # smoke
#   ONLY_CMD=SET,GET TEST_TIME=10 bash ...                    # 只跑指定命令
#
# 可调 env var 见下方"可配置参数"段。
# ============================================================================

# 注意：不用 set -e —— 高并发偶发 connection reset，单档失败不能中断全轮
set -uo pipefail

# ============================================================================
# 可配置参数
# ============================================================================

# Binary 路径（HW01）
BASELINE_DIR=${BASELINE_DIR:-/root/gqs/codespace/redis-8.6.3}
BASELINE_SERVER=$BASELINE_DIR/src/redis-server
# redis-cli 统一用 baseline 的（RESP 兼容 hpc-redis，无需编译 hpc 自己的 cli）
CLI=$BASELINE_DIR/src/redis-cli
HPC_DIR=${HPC_DIR:-/root/gqs/codespace/UnifiedBus/hpc-redis}
HPC_SERVER=$HPC_DIR/src/redis-server
MEMTIER_ORIGIN=${MEMTIER_ORIGIN:-/root/gqs/codespace/UnifiedBus/memtier_benchmark_origin/memtier_benchmark}
MEMTIER_HPC=${MEMTIER_HPC:-/root/gqs/codespace/UnifiedBus/memtier_benchmark/memtier_benchmark}

# 运行参数
PORT=${PORT:-6389}
TEST_TIME=${TEST_TIME:-60}
SERVER_CPUSET=${SERVER_CPUSET:-0-95}
CLIENT_CPUSET=${CLIENT_CPUSET:-96-191}
SERVER_NUMA=${SERVER_NUMA:-0}
CLIENT_NUMA=${CLIENT_NUMA:-1}
DATA_SIZE=${DATA_SIZE:-128}
IO_THREADS=${IO_THREADS:-16}        # baseline redis --io-threads

# 数据规模
STRING_KEY_MAX=${STRING_KEY_MAX:-10000000}        # String/Hash key 范围上限
COLLECTION_KEY_MAX=${COLLECTION_KEY_MAX:-100000}  # List/Set/ZSet key 范围上限

# 过滤
ONLY_CMD=${ONLY_CMD:-}   # 逗号分隔的命令名白名单，空表示全跑

# 复用已有 baseline 数据：设成上一次跑的 summary TSV 路径，则跳过 baseline 实跑，
# 直接从该文件导入 baseline 行 + 填充 RESULT_CACHE，只跑 hpc 阶段。
# 用于 baseline 已完整、只需补跑 hpc 的场景（如 server 中途被外部杀掉）。
BASELINE_TSV=${BASELINE_TSV:-}

# 输出
OUTDIR=${OUTDIR:-/tmp/aigcode_comparison}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RAWDIR=$OUTDIR/raw
TSV=$OUTDIR/summary_${TIMESTAMP}.tsv
COMPARISON=$OUTDIR/comparison_${TIMESTAMP}.txt

mkdir -p "$RAWDIR"

# ============================================================================
# 命令矩阵
#   字段：name | prefill_mode | memtier_args | threads | clients | pipeline
#   prefill_mode: none | set | set_p | hset | lpush | sadd | zadd
# ============================================================================

MATRIX=(
"SET      | none  | --ratio=1:0 --key-pattern=R:R --data-size=$DATA_SIZE --key-minimum=1 --key-maximum=$STRING_KEY_MAX | 5 10 20 | 200 | 1"
"GET      | set   | --ratio=0:1 --key-pattern=R:R --data-size=$DATA_SIZE --key-minimum=1 --key-maximum=$STRING_KEY_MAX | 5 10 20 | 200 | 1"
"SET_P    | none  | --ratio=1:0 --key-pattern=R:R --data-size=$DATA_SIZE --key-minimum=1 --key-maximum=$STRING_KEY_MAX | 5 10 20 | 200 | 16"
"GET_P    | set_p | --ratio=0:1 --key-pattern=R:R --data-size=$DATA_SIZE --key-minimum=1 --key-maximum=$STRING_KEY_MAX | 5 10 20 | 200 | 16"
"HSET     | none  | --command=\"HSET __key__ field __data__\" --command-key-pattern=R --data-size=$DATA_SIZE --key-minimum=1 --key-maximum=$STRING_KEY_MAX | 5 10 20 | 200 | 1"
"HGET     | hset  | --command=\"HGET __key__ field\" --command-key-pattern=R --data-size=$DATA_SIZE --key-minimum=1 --key-maximum=$STRING_KEY_MAX | 5 10 20 | 200 | 1"
"LPUSH    | none  | --command=\"LPUSH __key__ __data__\" --command-key-pattern=R --data-size=$DATA_SIZE --key-minimum=1 --key-maximum=$COLLECTION_KEY_MAX | 20 | 20 | 1"
"SADD     | none  | --command=\"SADD __key__ __data__\" --command-key-pattern=R --data-size=$DATA_SIZE --key-minimum=1 --key-maximum=$COLLECTION_KEY_MAX | 20 | 20 | 1"
"ZADD     | none  | --command=\"ZADD __key__ INCR 1 __data__\" --command-key-pattern=R --data-size=$DATA_SIZE --key-minimum=1 --key-maximum=$COLLECTION_KEY_MAX | 20 | 20 | 1"
"LRANGE   | lpush | --command=\"LRANGE mylist 0 10000\" --pipeline=5 | 20 | 20 | 5"
"SMEMBERS | sadd  | --command=\"SMEMBERS __key__\" --key-prefix=set: --command-key-pattern=R --data-size=$DATA_SIZE --key-minimum=1 --key-maximum=$COLLECTION_KEY_MAX | 20 | 20 | 1"
"ZRANGE   | zadd  | --command=\"ZRANGE __key__ 0 -1 WITHSCORES\" --key-prefix=zset: --command-key-pattern=R --data-size=$DATA_SIZE --key-minimum=1 --key-maximum=$COLLECTION_KEY_MAX | 20 | 20 | 1"
)

# ============================================================================
# 工具函数
# ============================================================================

log() {
    printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

# 解析 MATRIX 行
parse_row() {
    local row="$1"
    NAME=$(echo "$row"      | awk -F'|' '{gsub(/ /,"",$1); print $1}')
    PREFILL=$(echo "$row"   | awk -F'|' '{gsub(/ /,"",$2); print $2}')
    MEMTIER_ARGS=$(echo "$row" | cut -d'|' -f3 | sed 's/^ *//; s/ *$//')
    THREADS=$(echo "$row"  | cut -d'|' -f4 | sed 's/^ *//; s/ *$//')
    CLIENTS=$(echo "$row"  | cut -d'|' -f5 | sed 's/^ *//; s/ *$//')
    PIPELINE=$(echo "$row" | cut -d'|' -f6 | sed 's/^ *//; s/ *$//')
}

# 等 port 就绪
wait_port() {
    local port=$1
    for _ in $(seq 1 60); do
        ss -tln | grep -q ":$port " && return 0
        sleep 0.3
    done
    return 1
}

# 通过 ss 拿 server PID
get_server_pid() {
    ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1
}

# 汇总某 PID 所有 TID 的 (utime+stime) jiffies
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

# ============================================================================
# Server 控制
# ============================================================================

start_baseline() {
    log "=== 启动 baseline redis-8.6.3 (port $PORT) ==="
    [ -x "$BASELINE_SERVER" ] || { log "FAIL: $BASELINE_SERVER 不存在或不可执行"; exit 1; }
    cd "$BASELINE_DIR"
    rm -f dump.rdb appendonly.aof
    rm -rf appendonlydir
    numactl -N $SERVER_NUMA -l taskset -c $SERVER_CPUSET ./src/redis-server \
        --port $PORT --bind 0.0.0.0 --protected-mode no \
        --dir ./ \
        --tcp-keepalive 1800 --timeout 0 \
        --io-threads $IO_THREADS --io-threads-do-reads yes \
        --daemonize yes --logfile "$RAWDIR/baseline_server.log" \
        >/dev/null 2>&1
    wait_port $PORT || { log "FAIL: baseline server 启动失败"; tail -30 "$RAWDIR/baseline_server.log"; exit 1; }
    log "baseline server up"
}

start_hpc() {
    log "=== 启动 hpc-redis (port $PORT) ==="
    [ -x "$HPC_SERVER" ] || { log "FAIL: $HPC_SERVER 不存在或不可执行"; exit 1; }
    cd "$HPC_DIR"
    rm -f dump.rdb appendonly.aof
    rm -rf appendonlydir
    # 不带 --vemb-v16-enabled：公平 RESP 对比
    numactl -N $SERVER_NUMA -l taskset -c $SERVER_CPUSET ./src/redis-server \
        --port $PORT --bind 0.0.0.0 --protected-mode no \
        --dir ./ \
        --tcp-keepalive 1800 --timeout 0 \
        --io-threads $IO_THREADS --io-threads-do-reads yes \
        --daemonize yes --logfile "$RAWDIR/hpc_server.log" \
        >/dev/null 2>&1
    wait_port $PORT || { log "FAIL: hpc server 启动失败"; tail -30 "$RAWDIR/hpc_server.log"; exit 1; }
    log "hpc-redis server up"
}

stop_server() {
    local cli="$1"
    "$cli" -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
    sleep 0.5
    pkill -9 -f "redis-server.*:$PORT " 2>/dev/null || true
    sleep 0.5
}

cleanup() {
    log "cleanup: 关闭 server"
    pkill -9 -f "redis-server.*:$PORT " 2>/dev/null || true
}
trap cleanup EXIT

# ============================================================================
# Prefill
# ============================================================================

# 通用 memtier 预填充（用对应系统的 memtier）
# 参数：memtier_bin  args
prefill_with_memtier() {
    local memtier_bin="$1"; shift
    local prefill_args="$*"
    # eval 用于解析 --command="..." 里的转义双引号
    local cmd="numactl -N $CLIENT_NUMA taskset -c $CLIENT_CPUSET '$memtier_bin' \
        -s 127.0.0.1 -p $PORT -t 1 -c 1 \
        --hide-histogram --select-db=0 \
        $prefill_args"
    eval "$cmd" >/dev/null 2>&1 || true
}

# redis-cli pipe 批量导入
# 参数：cli_bin
prefill_collections() {
    local cli="$1"
    local mode="$2"

    case "$mode" in
        lpush)
            # 造一个长 list：mylist，包含 COLLECTION_KEY_MAX 个 128B 元素
            local data=$(head -c $DATA_SIZE /dev/urandom | base64 | head -c $DATA_SIZE)
            local chunk=$((COLLECTION_KEY_MAX / 8))
            for w in $(seq 0 7); do
                local start=$((w * chunk + 1))
                local end=$((start + chunk - 1))
                [ $end -gt $COLLECTION_KEY_MAX ] && end=$COLLECTION_KEY_MAX
                (
                    for i in $(seq $start $end); do
                        printf '*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$%d\r\n%s\r\n' \
                            $((${#data})) "$data"
                    done | "$cli" -p $PORT --pipe
                ) &
            done
            wait
            ;;
        sadd)
            # 造 COLLECTION_KEY_MAX 个 set，每个 set 1 个元素
            # key 格式 set:N，元素 data
            local data=$(head -c $DATA_SIZE /dev/urandom | base64 | head -c $DATA_SIZE)
            local chunk=$((COLLECTION_KEY_MAX / 8))
            for w in $(seq 0 7); do
                local start=$((w * chunk + 1))
                local end=$((start + chunk - 1))
                [ $end -gt $COLLECTION_KEY_MAX ] && end=$COLLECTION_KEY_MAX
                (
                    for i in $(seq $start $end); do
                        printf '*3\r\n$4\r\nSADD\r\n$%d\r\nset:%d\r\n$%d\r\n%s\r\n' \
                            $((${#i} + 4)) "$i" $((${#data})) "$data"
                    done | "$cli" -p $PORT --pipe
                ) &
            done
            wait
            ;;
        zadd)
            # 造 COLLECTION_KEY_MAX 个 zset，每个 1 个元素
            local data=$(head -c $DATA_SIZE /dev/urandom | base64 | head -c $DATA_SIZE)
            local chunk=$((COLLECTION_KEY_MAX / 8))
            for w in $(seq 0 7); do
                local start=$((w * chunk + 1))
                local end=$((start + chunk - 1))
                [ $end -gt $COLLECTION_KEY_MAX ] && end=$COLLECTION_KEY_MAX
                (
                    for i in $(seq $start $end); do
                        printf '*4\r\n$4\r\nZADD\r\n$%d\r\nzset:%d\r\n$1\r\n1\r\n$%d\r\n%s\r\n' \
                            $((${#i} + 5)) "$i" $((${#data})) "$data"
                    done | "$cli" -p $PORT --pipe
                ) &
            done
            wait
            ;;
    esac
}

# ============================================================================
# 跑 memtier 单档
# 参数：system memtier_bin t c pipeline memtier_args
# ============================================================================

# 全局结果缓存：${RESULT_CACHE[$system,$cmd,$t]}="ops|avg|p50|p99|kb|cores"
declare -A RESULT_CACHE

run_memtier_case() {
    local system=$1 memtier_bin=$2 t=$3 c=$4 pipeline=$5 args=$6
    local raw="$RAWDIR/${system}_${NAME}_t${t}.log"

    local server_pid=$(get_server_pid)
    local j0="" j1="" cores="NA"
    [ -n "$server_pid" ] && j0=$(get_cpu_jiffies "$server_pid")

    # 用 eval 重组命令行：matrix 里 --command="..." 的转义双引号需要二次解析
    # 才能作为单个 arg 传给 memtier
    local cmd="numactl -N $CLIENT_NUMA taskset -c $CLIENT_CPUSET \
        '$memtier_bin' -s 127.0.0.1 -p $PORT -t $t -c $c \
        --pipeline=$pipeline \
        --test-time=$TEST_TIME --hide-histogram --select-db=0 \
        $args"
    eval "$cmd" > "$raw" 2>&1 || true

    [ -n "$server_pid" ] && {
        j1=$(get_cpu_jiffies "$server_pid")
        cores=$(awk -v d=$(( j1 - j0 )) -v tt=$TEST_TIME 'BEGIN{ if(d<0) print "NA"; else printf "%.2f", d/100.0/tt }')
    }

    # 解析 Totals 行 —— memtier 有两种格式，按 NF 自适应：
    #   标准 SET/GET 模式 (NF=9): ops hits misses avg p50 p99 p99.9 kb
    #   --command 自定义模式 (NF=7): ops avg p50 p99 p99.9 kb
    local totals last_progress ops hits misses avg p50 p99 p999 kb
    totals=$(grep "^Totals" "$raw" 2>/dev/null | tail -1)
    last_progress=$(tr '\r' '\n' < "$raw" 2>/dev/null | grep -E "^\[RUN #[0-9]+ +[0-9]+%," | tail -1)

    if [ -n "$totals" ] && [ "$(echo "$totals" | awk '{print $2}')" != "0.00" ]; then
        read ops hits misses avg p50 p99 p999 kb < <(
            echo "$totals" | awk '{
                if (NF >= 9)
                    printf "%s %s %s %s %s %s %s %s", $2,$3,$4,$5,$6,$7,$8,$9
                else if (NF >= 7)
                    printf "%s NA NA %s %s %s %s %s", $2,$3,$4,$5,$6,$7
                else
                    printf "0 NA NA NA NA NA NA NA"
            }'
        )
    elif [ -n "$last_progress" ]; then
        ops=$(echo "$last_progress" | sed -n 's/.*(avg: *\([0-9][0-9]*\)) ops\/sec.*/\1/p')
        avg=$(echo "$last_progress" | sed -n 's/.*(avg: *\([0-9.][0-9.]*\)) msec latency.*/\1/p')
        hits="NA"; misses="NA"; p50="NA"; p99="NA"; p999="NA"; kb="NA"
    else
        ops=0; hits="NA"; misses="NA"; avg="NA"; p50="NA"; p99="NA"; p999="NA"; kb="NA"
    fi

    # 内存（baseline 和 hpc-redis 都不走 warm region，RSS 是全部内存）
    local base_mb="NA" peak_mb="NA"
    if [ -n "$server_pid" ]; then
        base_mb=$(awk '/^VmRSS:/{printf "%.0f", $2/1024}' /proc/$server_pid/status 2>/dev/null)
        peak_mb=$(awk '/^VmHWM:/{printf "%.0f", $2/1024}' /proc/$server_pid/status 2>/dev/null)
    fi

    # 写 TSV（全字段：ops/hits/misses/avg/p50/p99/p99.9/kb/cores/mem）
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$system" "$NAME" "$t" "$c" "$pipeline" "$ops" "$hits" "$misses" \
        "$avg" "$p50" "$p99" "$p999" "$kb" "$cores" "$base_mb" "$peak_mb" \
        >> "$TSV"

    # 缓存（供对比表用）：ops|avg|p50|p99|p999|kb
    RESULT_CACHE["$system,$NAME,$t"]="${ops}|${avg}|${p50}|${p99}|${p999}|${kb}"

    log "  $system $NAME t=$t c=$c pipe=$pipeline  ops=${ops}  avg=${avg}  p99=${p99}  p99.9=${p999}  cores=${cores}"
}

# ============================================================================
# 单系统跑完整矩阵
# 参数：system cli memtier_bin
# ============================================================================

run_matrix_for_system() {
    local system=$1 cli=$2 memtier_bin=$3

    for row in "${MATRIX[@]}"; do
        parse_row "$row"

        # 白名单过滤
        if [ -n "$ONLY_CMD" ]; then
            local match=0
            IFS=',' read -ra _whitelist <<< "$ONLY_CMD"
            for w in "${_whitelist[@]}"; do
                [ "$w" = "$NAME" ] && match=1 && break
            done
            [ "$match" -eq 0 ] && continue
        fi

        log "--- $system: $NAME (prefill=$PREFILL) ---"

        # FLUSHDB 清掉前用例数据
        "$cli" -p $PORT FLUSHDB >/dev/null 2>&1 || true

        # Prefill
        case "$PREFILL" in
            none) ;;
            set|set_p)
                prefill_with_memtier "$memtier_bin" \
                    "--ratio=1:0 --key-pattern=S:S --data-size=$DATA_SIZE -n $STRING_KEY_MAX \
                     --key-minimum=1 --key-maximum=$STRING_KEY_MAX"
                ;;
            hset)
                prefill_with_memtier "$memtier_bin" \
                    "--ratio=1:0 --command=\"HSET __key__ field __data__\" --command-key-pattern=S \
                     --data-size=$DATA_SIZE -n $STRING_KEY_MAX \
                     --key-minimum=1 --key-maximum=$STRING_KEY_MAX"
                ;;
            lpush|sadd|zadd)
                prefill_collections "$cli" "$PREFILL"
                ;;
        esac

        # 跑各并发档
        for t in $THREADS; do
            run_memtier_case "$system" "$memtier_bin" "$t" "$CLIENTS" "$PIPELINE" "$MEMTIER_ARGS"
        done
    done
}

# ============================================================================
# 输出对比表
# ============================================================================

print_comparison() {
    {
        echo ""
        echo "================================================================="
        echo "       AIGCode 命令字 baseline vs hpc-redis 对比"
        echo "       (test_time=${TEST_TIME}s, 全部走 RESP)"
        echo "================================================================="

        # ---- 表 1: 吞吐量 ----
        echo ""
        echo "【吞吐量】"
        printf "%-10s %-8s %-5s %14s %14s %8s %12s %12s\n" \
            "命令" "t×c" "pipe" "base_ops/s" "hpc_ops/s" "倍数" "base_KB/s" "hpc_KB/s"
        echo "----------------------------------------------------------------------------------------"

        for row in "${MATRIX[@]}"; do
            parse_row "$row"
            for t in $THREADS; do
                local b="${RESULT_CACHE[baseline,$NAME,$t]:-}"
                local h="${RESULT_CACHE[hpc,$NAME,$t]:-}"
                [ -z "$b" ] && [ -z "$h" ] && continue

                # cache 格式: ops|avg|p50|p99|p999|kb
                local b_ops=$(echo "$b" | cut -d'|' -f1) b_kb=$(echo "$b" | cut -d'|' -f6)
                local h_ops=$(echo "$h" | cut -d'|' -f1) h_kb=$(echo "$h" | cut -d'|' -f6)
                [ -z "$b_ops" ] && b_ops="NA"
                [ -z "$h_ops" ] && h_ops="NA"

                local ratio="NA"
                if [ "$b_ops" != "NA" ] && [ "$h_ops" != "NA" ] && [ "$b_ops" != "0" ]; then
                    ratio=$(awk "BEGIN{printf \"%.2fx\", $h_ops / $b_ops}")
                fi

                printf "%-10s %-8s %-5s %14s %14s %8s %12s %12s\n" \
                    "$NAME" "${t}×${CLIENTS}" "$PIPELINE" \
                    "${b_ops}" "${h_ops}" "$ratio" "${b_kb:-NA}" "${h_kb:-NA}"
            done
        done

        # ---- 表 2: 延迟 (msec) ----
        echo ""
        echo "【延迟 (msec)】"
        printf "%-10s %-8s %9s %9s %9s %9s %9s %9s %10s %10s\n" \
            "命令" "t×c" "base_avg" "hpc_avg" "base_p50" "hpc_p50" "base_p99" "hpc_p99" "base_p999" "hpc_p999"
        echo "------------------------------------------------------------------------------------------------------"

        for row in "${MATRIX[@]}"; do
            parse_row "$row"
            for t in $THREADS; do
                local b="${RESULT_CACHE[baseline,$NAME,$t]:-}"
                local h="${RESULT_CACHE[hpc,$NAME,$t]:-}"
                [ -z "$b" ] && [ -z "$h" ] && continue

                # cache 格式: ops|avg|p50|p99|p999|kb
                local b_avg=$(echo "$b" | cut -d'|' -f2) b_p50=$(echo "$b" | cut -d'|' -f3) \
                      b_p99=$(echo "$b" | cut -d'|' -f4) b_p999=$(echo "$b" | cut -d'|' -f5)
                local h_avg=$(echo "$h" | cut -d'|' -f2) h_p50=$(echo "$h" | cut -d'|' -f3) \
                      h_p99=$(echo "$h" | cut -d'|' -f4) h_p999=$(echo "$h" | cut -d'|' -f5)

                printf "%-10s %-8s %9s %9s %9s %9s %9s %9s %10s %10s\n" \
                    "$NAME" "${t}×${CLIENTS}" \
                    "${b_avg:-NA}" "${h_avg:-NA}" \
                    "${b_p50:-NA}" "${h_p50:-NA}" \
                    "${b_p99:-NA}" "${h_p99:-NA}" \
                    "${b_p999:-NA}" "${h_p999:-NA}"
            done
        done

        echo "================================================================="
        echo "吞吐量表：倍数 >1.00x 表示 hpc-redis 更快，<1.00x 表示落后"
        echo "延迟表：数值越小越好"
        echo ""
        echo "明细 TSV（含 hits/sec, misses/sec, cores, RSS）: $TSV"
        echo "原始日志: $RAWDIR/"
    } | tee "$COMPARISON"
}

# ============================================================================
# Main
# ============================================================================

# TSV 表头
printf 'system\tcmd\tt\tc\tpipeline\tops_sec\thits_sec\tmisses_sec\tavg_lat_ms\tp50_ms\tp99_ms\tp999_ms\tkb_sec\tcores\tbase_MB\tpeak_MB\n' > "$TSV"

log "AIGCode 13 命令字对比启动"
log "  TEST_TIME=${TEST_TIME}s  PORT=$PORT  IO_THREADS=$IO_THREADS"
log "  server=numa$SERVER_NUMA/$SERVER_CPUSET  client=numa$CLIENT_NUMA/$CLIENT_CPUSET"
log "  输出: $OUTDIR/"

# ---------- baseline ----------
if [ -n "$BASELINE_TSV" ] && [ -f "$BASELINE_TSV" ]; then
    log "BASELINE_TSV=$BASELINE_TSV 已设，从该文件导入 baseline 数据（跳过 baseline 实跑）"
    # 1) 把旧行 baseline 数据追加到新 TSV
    awk -F'\t' -v OFS='\t' 'NR>1 && $1=="baseline"' "$BASELINE_TSV" >> "$TSV"
    # 2) 填充 RESULT_CACHE：cache 值格式 ops|avg|p50|p99|p999|kb
    #    TSV 列：$2=cmd $3=t $6=ops $9=avg $10=p50 $11=p99 $12=p999 $13=kb
    while IFS=$'\t' read -r _ cmd t _ _ ops _ _ avg p50 p99 p999 kb _ _; do
        [ "$cmd" = "cmd" ] && continue
        RESULT_CACHE["baseline,$cmd,$t"]="${ops}|${avg}|${p50}|${p99}|${p999}|${kb}"
    done < <(awk -F'\t' 'NR==1 || $1=="baseline"' "$BASELINE_TSV")
    log "导入 $(awk -F'\t' 'NR>1 && $1=="baseline"' "$BASELINE_TSV" | wc -l) 行 baseline 数据"
else
    start_baseline
    BASELINE_PID_AFTER_START=$(get_server_pid)
    log "baseline server pid=$BASELINE_PID_AFTER_START"

    run_matrix_for_system "baseline" "$CLI" "$MEMTIER_ORIGIN"

    stop_server "$CLI"
fi

# ---------- hpc-redis ----------
start_hpc
HPC_PID_AFTER_START=$(get_server_pid)
log "hpc server pid=$HPC_PID_AFTER_START"

run_matrix_for_system "hpc" "$CLI" "$MEMTIER_HPC"

stop_server "$CLI"

# ---------- 对比表 ----------
print_comparison

log "=== All done ==="
log "结果：$COMPARISON"
