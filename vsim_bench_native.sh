#!/bin/bash
set -e

cd /root/gqs/codespace/UnifiedBus/hpc-redis

DIM=${VEMB_DIM:-300}
HOST=${VEMB_HOST:-127.0.0.1}
PORT=${VEMB_PORT:-6391}
PREFILL=${VEMB_PREFILL:-100000}
OPS=${VEMB_OPS:-50000}
VECTORS=${VEMB_VECTORS:-262144}

echo "========================================"
echo "  VEMB V16 Native VSIM Bench"
echo "========================================"
echo "  dim=$DIM host=$HOST port=$PORT"
echo "  prefill=$PREFILL ops=$OPS vectors=$VECTORS"
echo ""

# 清理可能占用端口的进程
pkill -f redis-server 2>/dev/null || true
pkill -f vemb_v16_server 2>/dev/null || true
sleep 2

# 启动 standalone vemb_v16_server
echo "Starting vemb_v16_server..."
./src/vemb_v16_server \
  --transport tcp --tcp-host "$HOST" --tcp-port "$PORT" \
  --dim "$DIM" --vectors "$VECTORS" --loglevel notice > /tmp/vsim_bench_server.log 2>&1 &
SERVER_PID=$!
sleep 2

cd benchmark

echo "--- VSIM inline, pipeline=1 ---"
for T in 1 4 8 16 32 64 96; do
    ./vemb_v16_bench --transport tcp --host "$HOST" --port "$PORT" \
        --dim "$DIM" --prefill "$PREFILL" --ops "$OPS" --threads "$T" \
        --mode vsim-inline 2>&1 | awk -v t="$T" '
        /\[done\]/ {
            split($4, a, "="); ok=a[2]
            split($5, a, "="); fail=a[2]
            split($6, a, "="); qps=a[2]
            split($7, a, "="); avg_ns=a[2]
            printf "  threads=%3s | ok=%10s | fail=%6s | qps=%12s | avg_ns=%12s\n", t, ok, fail, qps, avg_ns
        }
    '
done

echo ""
echo "--- VSIM inline, pipeline=32 ---"
for T in 8 16 32 64 96; do
    ./vemb_v16_bench --transport tcp --host "$HOST" --port "$PORT" \
        --dim "$DIM" --prefill "$PREFILL" --ops "$OPS" --threads "$T" \
        --pipeline 32 --mode vsim-inline 2>&1 | awk -v t="$T" '
        /\[done\]/ {
            split($4, a, "="); ok=a[2]
            split($5, a, "="); fail=a[2]
            split($6, a, "="); qps=a[2]
            split($7, a, "="); avg_ns=a[2]
            printf "  threads=%3s | ok=%10s | fail=%6s | qps=%12s | avg_ns=%12s\n", t, ok, fail, qps, avg_ns
        }
    '
done

echo ""
echo "Done"
kill $SERVER_PID 2>/dev/null || true
