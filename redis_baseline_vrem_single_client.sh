#!/bin/bash
# vanilla redis 8.6.3 VREM 单 client pipeline 扫描（方法论 A）
#
#   -t 1 -c 1 -n FIXED_N，扫 pipeline。每个 pipeline 点不重叠 key 区间（分片），
#   100% 命中。测 HNSW 真删除（主线程写、改图结构）单连接最大吞吐。
#
#   FIXED_N 默认 10000（HNSW VREM 慢，小批量够测）；6 点分片共 60K <= 100K prefill
#
# 用法: PORT=6391 ./redis_baseline_vrem_single_client.sh
#   smoke: NS="8" PIPELINES="32" FIXED_N=2000 ./redis_baseline_vrem_single_client.sh
set -uo pipefail

REDIS=/root/gqs/codespace/redis-8.6.3
REDIS_CLI=$REDIS/src/redis-cli
MEMTIER=/root/gqs/codespace/UnifiedBus/memtier_benchmark/memtier_benchmark

# CPU 绑核：server 独占 node0(0-95)，memtier/redis-cli 独占 node1(96-191)
SERVER_BIND="numactl --membind=0 taskset -c 0-95"
CLIENT_BIND="numactl --membind=1 taskset -c 96-191"

DIM=300
PREFILL_KEYS=100000
FIXED_N=${FIXED_N:-10000}
KEY_PREFIX="item:"
PORT=${PORT:-6391}
PIPELINES=( ${PIPELINES:-1 8 16 32 64 128} )
NS=( ${NS:-1 2 4 8 16 32 64} )

OUTDIR=/tmp/redis_baseline_vrem_single
RAWDIR=$OUTDIR/raw
mkdir -p "$RAWDIR"
TSV=$OUTDIR/summary.tsv
PIDFILE=/tmp/redis_vrem_single.pid
VADD_PIPE=/tmp/vadd_pipe_100k.txt

printf 'N\tpipeline\tn\tkey_min\tkey_max\tops_sec\thits\tp50_ms\tp99_ms\tcpu_cores\telapsed\n' > "$TSV"
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
    wait_port || { echo "FAIL: N=$n server did not listen"; return 1; }
    SERVER_PID=$(cat "$PIDFILE" 2>/dev/null)
}
gen_vadd_pipe() {
    [ -f "$VADD_PIPE" ] && { echo "  reuse $VADD_PIPE ($(wc -l < $VADD_PIPE) lines)"; return 0; }
    echo "  generating $VADD_PIPE ($PREFILL_KEYS vectors)..."
    local i
    for i in $(seq 1 $PREFILL_KEYS); do
        awk -v seed=$i -v prefix=$KEY_PREFIX 'BEGIN{
            srand(seed); out="VADD myset VALUES 300 ";
            for(j=0;j<300;j++) out=out sprintf("%.5f ", rand()*j);
            print out prefix seed;
        }'
    done > "$VADD_PIPE"
}
prefill() {
    $CLIENT_BIND $REDIS_CLI -p $PORT DEL myset >/dev/null
    $CLIENT_BIND $REDIS_CLI -p $PORT --pipe < "$VADD_PIPE" >$RAWDIR/prefill.log 2>&1
    local card; card=$($REDIS_CLI -p $PORT VCARD myset 2>/dev/null)
    [ "${card:-0}" -ne "$PREFILL_KEYS" ] && echo "WARN: prefill got card=$card expected=$PREFILL_KEYS"
}
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

total_shard=$(( ${#PIPELINES[@]} * FIXED_N ))
[ "$total_shard" -gt "$PREFILL_KEYS" ] && { echo "FAIL: ${#PIPELINES[@]} pipelines × FIXED_N=$FIXED_N = $total_shard > PREFILL_KEYS=$PREFILL_KEYS"; exit 1; }

log "vanilla redis VREM single-client sweep — ${#NS[@]} N × ${#PIPELINES[@]} pipelines = $(( ${#NS[@]}*${#PIPELINES[@]} )) runs (FIXED_N=$FIXED_N, 分片不重叠)"
gen_vadd_pipe
for n in "${NS[@]}"; do
    log "N=$n — starting server + prefill $PREFILL_KEYS"
    start_server "$n" || continue
    sleep 1
    prefill
    base=0
    for pl in "${PIPELINES[@]}"; do
        kmin=$((base + 1))
        kmax=$((base + FIXED_N))
        base=$((base + FIXED_N))
        tag=N${n}_pl${pl}
        raw=$RAWDIR/${tag}.txt
        j0=$(get_cpu_jiffies "$SERVER_PID")
        t0=$(date +%s.%N)
        timeout 300s $CLIENT_BIND $MEMTIER --command="VREM myset __key__" --command-key-pattern=S \
            --key-prefix=$KEY_PREFIX --key-minimum=$kmin --key-maximum=$kmax \
            -s 127.0.0.1 -p $PORT -t 1 -c 1 --pipeline=$pl -n $FIXED_N >"$raw" 2>&1
        t1=$(date +%s.%N)
        j1=$(get_cpu_jiffies "$SERVER_PID")
        elapsed=$(awk "BEGIN{printf \"%.3f\", $t1-$t0}")
        true_time=$(awk -v n=$FIXED_N -v o=$ops 'BEGIN{ if(o>0) printf "%.3f", n/o; else print "0"}')
        cores=$(awk -v d=$(( j1 - j0 )) -v t=$true_time 'BEGIN{ if(d<0||t<=0) print "NA"; else printf "%.2f", d/100.0/t }')
        tot=$(grep '^Totals' "$raw" | tail -1)
        ops=$(echo "$tot" | awk '{print $2}')
        hits=$(echo "$tot" | awk '{print $3}')
        p50=$(echo "$tot" | awk '{print $6}')
        p99=$(echo "$tot" | awk '{print $7}')
        [ -z "$ops" ] && ops=0
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$n" "$pl" "$FIXED_N" "$kmin" "$kmax" "$ops" "$hits" "$p50" "$p99" "$cores" "$elapsed" >> "$TSV"
        k="N${n}|pl${pl}"
        OPS[$k]=$ops
        awk "BEGIN{exit !($ops > $BEST_OPS)}" && { BEST_OPS=$ops; BEST_KEY="$k"; }
        printf '   N=%-2s pl%-3s  ops=%-12s hits=%-10s p50=%-8s cores=%-5s elapsed=%ss\n' \
            "$n" "$pl" "$ops" "$hits" "$p50" "$cores" "$elapsed"
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
