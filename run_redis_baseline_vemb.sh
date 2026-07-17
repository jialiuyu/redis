#!/bin/bash
# vanilla redis 8.6.3 VEMB max-tput sweep：io-threads N + memtier -t×-c sweep
#
#   对照 hpc_redis_max_tput.sh，看 vanilla redis RESP VEMB（取向量）
#   跟 hpc_redis VEMB V16 二进制协议的吞吐差。
#   同样 10000 向量 prefill, 100% VEMB 读。
#
#   VEMB 走 RESP 自定义命令：memtier --command="VEMB myvectors __key__"
#   vanilla redis 8.6.3 内置 vector-sets 模块（VEMB/VADD 是 module 命令）
#
# 用法: PORT=6392 ./run_redis_baseline_vemb.sh
#   单点: NS="4" TS="32" CS="32" TEST_TIME=30 ./run_redis_baseline_vemb.sh
set -uo pipefail

REDIS=/root/gqs/codespace/redis-8.6.3
REDIS_CLI=$REDIS/src/redis-cli
MEMTIER=/root/gqs/codespace/UnifiedBus/memtier_benchmark/memtier_benchmark

# CPU 绑核：server 独占 node0(0-95)，memtier/redis-cli 独占 node1(96-191)
SERVER_BIND="numactl --membind=0 taskset -c 0-95"
CLIENT_BIND="numactl --membind=1 taskset -c 96-191"

DIM=300
NUM_KEYS=10000
KEY_PREFIX="item:"
PORT=${PORT:-6392}
TEST_TIME=${TEST_TIME:-5}
PIPELINE=32
# io-threads to sweep
NS=( ${NS:-1 2 4 8 16 32 64} )
# memtier -t values
TS=( ${TS:-1 1 1 4 8 16 32 32 32} )
# memtier -c values (paired with TS)
CS=( ${CS:-1 64 200 64 32 16 8 2 1} )

OUTDIR=${OUTDIR:-/tmp/redis_baseline_vemb_max_tput}
RAWDIR=$OUTDIR/raw
mkdir -p "$RAWDIR"
TSV=$OUTDIR/summary.tsv
PIDFILE=/tmp/redis_vemb_baseline.pid
VADD_PIPE=/tmp/vadd_pipe_vemb.txt

printf 'N\tt\tc\ttxc\tops_sec\tp50_ms\tp99_ms\tcpu_cores\trun_ops\tmem_base_mb\tmem_peak_mb\tmem_avg_mb\n' > "$TSV"
declare -A OPS
BEST_OPS=0; BEST_KEY=""

log() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

kill_server() {
    [ -f "$PIDFILE" ] && { local p; p=$(cat "$PIDFILE" 2>/dev/null); [ -n "$p" ] && kill "$p" 2>/dev/null; sleep 0.5; kill -9 "$p" 2>/dev/null; rm -f "$PIDFILE"; }
    ps -eo pid=,args= | awk '/redis-server.*:'"$PORT"' / && !/awk/ {print $1}' | xargs -r kill -9 2>/dev/null
    sleep 0.5
}

wait_port() {
    for _ in $(seq 1 50); do ss -tln | grep -q ":$PORT " && return 0; sleep 0.2; done
    return 1
}

start_server() {
    local n=$1
    local logfile=$RAWDIR/N${n}.server.log
    kill_server
    rm -f "$REDIS/dump.rdb"
    cd "$REDIS"
    $SERVER_BIND ./src/redis-server \
        --port $PORT --bind 0.0.0.0 --protected-mode no \
        --io-threads $n --io-threads-do-reads yes \
        --daemonize yes --pidfile $PIDFILE --logfile "$logfile" --loglevel notice \
        >/dev/null 2>&1
    wait_port || { echo "FAIL: N=$n server did not listen (see $logfile)"; return 1; }
    SERVER_PID=$(cat "$PIDFILE" 2>/dev/null)
}

gen_vadd_pipe() {
    # 生成 VADD pipe 文件（只一次，复用）。每个向量 300 维，浮点值由 awk 伪随机生成
    [ -f "$VADD_PIPE" ] && return 0
    local i
    for i in $(seq 1 $NUM_KEYS); do
        awk -v seed=$i -v prefix=$KEY_PREFIX 'BEGIN{
            srand(seed);
            out = "VADD myvectors VALUES 300 ";
            for (j = 0; j < 300; j++) out = out sprintf("%.5f ", rand() * j);
            print out prefix seed;
        }'
    done > "$VADD_PIPE"
}

prefill() {
    local n=$1
    $CLIENT_BIND $REDIS_CLI -p $PORT DEL myvectors >/dev/null
    $CLIENT_BIND $REDIS_CLI -p $PORT --pipe < "$VADD_PIPE" >$RAWDIR/prefill.N${n}.log 2>&1
    local card; card=$($REDIS_CLI -p $PORT VCARD myvectors 2>/dev/null)
    [ "${card:-0}" -ne "$NUM_KEYS" ] && echo "WARN: prefill N=$n got card=$card expected=$NUM_KEYS"
}

# ───────── 汇总某 PID 所有 TID 的 (utime+stime) jiffies ─────────
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

run_client() {
    local n=$1 t=$2 c=$3
    local tag=N${n}_t${t}_c${c}
    local raw=$RAWDIR/${tag}.txt
    local j0 j1 cores
    local mem_base_mb mem_peak_mb mem_avg_mb mem_samples_file mem_sampler_pid
    j0=$(get_cpu_jiffies "$SERVER_PID")
    # ── 内存采集 - 启动 sampler ──
    mem_base_mb=$(awk '/^VmRSS:/{printf "%.0f", $2/1024}' /proc/$SERVER_PID/status 2>/dev/null)
    echo 1 > /proc/$SERVER_PID/clear_refs 2>/dev/null || true
    mem_samples_file=/tmp/mem_samples_redis_vemb_${tag}.log
    rm -f "$mem_samples_file"
    ( while kill -0 "$SERVER_PID" 2>/dev/null; do
        awk '/^VmRSS:/{print $2}' /proc/$SERVER_PID/status 2>/dev/null
        sleep 0.5
      done > "$mem_samples_file" ) &
    mem_sampler_pid=$!
    # VEMB 通过 RESP module 命令走；用 memtier --command 自定义命令
    #   VEMB myvectors __key__ — 取 key 对应的向量
    #   __key__ 会被 memtier 替换为按 key-pattern 生成的 element 名
    timeout $((TEST_TIME + 30))s $CLIENT_BIND $MEMTIER --command="VEMB myvectors __key__" --command-key-pattern=R \
        --key-prefix=$KEY_PREFIX --key-minimum=1 --key-maximum=$NUM_KEYS \
        -s 127.0.0.1 -p $PORT -t $t -c $c --pipeline=$PIPELINE --test-time=$TEST_TIME >"$raw" 2>&1
    j1=$(get_cpu_jiffies "$SERVER_PID")
    # ── 内存采集 - 停止 sampler + 读结果 ──
    kill "$mem_sampler_pid" 2>/dev/null || true
    wait "$mem_sampler_pid" 2>/dev/null || true
    sleep 0.2
    mem_peak_mb=$(awk '/^VmHWM:/{printf "%.0f", $2/1024}' /proc/$SERVER_PID/status 2>/dev/null)
    mem_avg_mb=$(awk '{s+=$1;n++} END{if(n>0) printf "%.0f", s/n/1024}' "$mem_samples_file" 2>/dev/null)
    rm -f "$mem_samples_file"
    cores=$(awk -v d=$(( j1 - j0 )) -v tt=$TEST_TIME 'BEGIN{ if(d<0) print "NA"; else printf "%.2f", d/100.0/tt }')
    local tot; tot=$(grep '^Totals' "$raw" | tail -1)
    local ops p50 p99
    ops=$(echo "$tot" | awk '{print $2}')
    p50=$(echo "$tot" | awk '{print $6}')
    p99=$(echo "$tot" | awk '{print $7}')
    local runops; runops=$(grep 'RUN #1 100%' "$raw" | grep -oE 'avg: *[0-9.,]+' | head -1 | grep -oE '[0-9.,]+')
    [ -z "$ops" ] && ops=0; [ -z "$runops" ] && runops=0
    [ -z "$mem_base_mb" ] && mem_base_mb=0; [ -z "$mem_peak_mb" ] && mem_peak_mb=0; [ -z "$mem_avg_mb" ] && mem_avg_mb=0
    local eff=$ops
    awk "BEGIN{exit !($ops < 1)}" && [ "$runops" != 0 ] && eff=$runops
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$n" "$t" "$c" "$((t*c))" "$ops" "$p50" "$p99" "$cores" "$runops" \
        "$mem_base_mb" "$mem_peak_mb" "$mem_avg_mb" >> "$TSV"
    local k="N${n}|t${t}|c${c}"
    OPS[$k]=$eff
    awk "BEGIN{exit !($eff > $BEST_OPS)}" && { BEST_OPS=$eff; BEST_KEY="$k"; }
    printf '   N=%-2s t%-2s c%-2s  ops=%-12s p50=%-8s p99=%-8s cores=%-5s  mem=%s/%sMB\n' \
        "$n" "$t" "$c" "$eff" "$p50" "$p99" "$cores" "$mem_base_mb" "$mem_peak_mb"
}

log "vanilla redis VEMB max-tput sweep — ${#NS[@]} N × ${#TS[@]} memtier pts = $(( ${#NS[@]}*${#TS[@]} )) runs"
gen_vadd_pipe
for n in "${NS[@]}"; do
    log "N=$n (io-threads) — starting server"
    start_server "$n" || continue
    sleep 1
    prefill "$n"
    n_pts=${#TS[@]}
    for ((i=0; i<n_pts; i++)); do
        run_client "$n" "${TS[$i]}" "${CS[$i]}"
    done
    kill_server
done
kill_server

log "DONE — $TSV"
printf '\n\033[1m── 各 N 峰值 ──\033[0m\n'
for n in "${NS[@]}"; do
    lb=0; lk=""
    for k in "${!OPS[@]}"; do
        case "$k" in "N${n}|"*) ;; *) continue;; esac
        v=${OPS[$k]}
        awk "BEGIN{exit !($v > $lb)}" && { lb=$v; lk="$k"; }
    done
    [ -n "$lk" ] && printf '  N=%-2s best: %s = %s ops/sec\n' "$n" "$lk" "$lb"
done
printf '\n全局最优: %s = %s ops/sec\n' "$BEST_KEY" "$BEST_OPS"
