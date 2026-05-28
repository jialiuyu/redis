#!/bin/bash
set -e

cd /root/gqs/codespace/UnifiedBus/hpc-redis

DIM=${VEMB_DIM:-300}
HOST=${VEMB_HOST:-127.0.0.1}
PORT=${VEMB_PORT:-6391}

# 构建 query vector（逗号分隔）
QUERY_VEC=$(seq -s ',' 1 "$DIM" | sed 's/[0-9]*/0.1/g')

# 确保server在运行
if ! ss -tlnp | grep -q ":6391"; then
    echo "redis-server not running, starting..."
    pkill -f redis-server 2>/dev/null || true
    sleep 1
    ./src/redis-server \
      --port 6379 --vemb-v16-enabled yes --vemb-v16-dim 300 \
      --vemb-v16-max-vectors 131072 --vemb-v16-vector-region /dev/obmm_shmdev2 \
      --vemb-v16-warm-backend ub --vemb-v16-proxy-io-threads 16 \
      --vemb-v16-supernode-workers 64 --vemb-v16-tcp-host 127.0.0.1 \
      --vemb-v16-tcp-port 6391 --daemonize yes --loglevel notice \
      --vector-engine vemb-v16
    sleep 3
fi

# 检查vector-engine
VE=$(./src/redis-cli CONFIG GET vector-engine 2>/dev/null | tail -1)
if [ "$VE" != "vemb-v16" ]; then
    echo "Setting vector-engine to vemb-v16"
    ./src/redis-cli CONFIG SET vector-engine vemb-v16 >/dev/null 2>&1
fi

# 预填充（如果WARM region为空）
echo "Prefilling if needed..."
VEC300=$(seq -s ' ' 1 300 | sed 's/[0-9]*/0.1/g')
for k in $(seq 1 32); do
    (for i in $(seq 1 3125); do
        echo "VADD item:$k VALUES 300 $VEC300 elem$i"
    done | ./src/redis-cli --pipe >/dev/null 2>&1) &
done
wait
sleep 1

echo ""
echo "=========================================="
echo "  VEMB V16 VSIM TCP Fast Path Multi-Client"
echo "=========================================="
echo ""

for N in 1 4 8 16 32 64; do
    echo "--- $N clients ---"
    rm -f /tmp/vsim_client_*.time

    PIDS=""
    for i in $(seq 1 $N); do
        set_id=$(( (i - 1) % 32 + 1 ))
        elem_id=$((i % 1000 + 1))
        (
            { time ./src/redis-cli --vemb-v16-tcp-host 127.0.0.1 \
                --vemb-v16-tcp-port "$PORT" --vemb-v16-dim "$DIM" \
                -r 200000 VSIM item:${set_id} elem${elem_id} "$QUERY_VEC" >/dev/null 2>&1; } \
            2>/tmp/vsim_client_${i}.time
        ) &
        PIDS="$PIDS $!"
    done
    for pid in $PIDS; do wait $pid; done

    max_real=0
    for i in $(seq 1 $N); do
        real_sec=$(cat /tmp/vsim_client_${i}.time 2>/dev/null | grep real | awk '{print $2}' | sed 's/m/*60+/;s/s//' | bc -l 2>/dev/null || echo 0)
        if [ "$(echo "$real_sec > $max_real" | bc -l 2>/dev/null || echo 0)" = "1" ]; then
            max_real=$real_sec
        fi
    done

    total_ops=$((N * 200000))
    if [ "$(echo "$max_real > 0" | bc -l 2>/dev/null || echo 0)" = "1" ]; then
        qps=$(echo "scale=2; $total_ops / $max_real" | bc -l)
        avg_ns=$(echo "scale=2; $max_real * 1000000000 / $total_ops" | bc -l)
        printf "  Clients: %2d | Total ops: %8d | Max real: %.3fs | OPS: %12s | Avg latency: %10s ns/op\n" \
            $N $total_ops $max_real "$qps" "$avg_ns"
    else
        echo "  Clients: $N | ERROR: could not parse time"
    fi
    echo ""
done

echo "Done"
