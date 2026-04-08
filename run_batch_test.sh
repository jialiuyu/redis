#!/bin/bash

# Batch Embedding Test Runner
# Tests Redis UB vector engine with massive concurrent requests

set -e

REDIS_SERVER="./src/redis-server"
REDIS_CLI="./src/redis-cli"
TEST_PROGRAM="./batch_embedding_test"
TEST_CONFIG="/tmp/redis_ub_test.conf"
TEST_PORT=6381

echo "=== Redis UB Vector Engine Batch Test ==="

# Create test config
cat > "$TEST_CONFIG" << EOF
port $TEST_PORT
save ""
appendonly no
vector-engine ub
daemonize no
loglevel notice
EOF

echo "1. Starting Redis server with UB engine..."
$REDIS_SERVER "$TEST_CONFIG" &
REDIS_PID=$!

# Wait for Redis to start
sleep 3

echo "2. Compiling test program..."
gcc -I./deps/hiredis -I./src -o "$TEST_PROGRAM" simple_ub_test.c ./deps/hiredis/libhiredis.a -lm

echo "3. Running simple UB test..."
$TEST_PROGRAM

echo "4. Checking engine status..."
$REDIS_CLI -p $TEST_PORT VENGINE STATS

echo "5. Testing manual VEMB commands..."
for i in {1..5}; do
    START_TIME=$(date +%s%N)
    RESULT=$($REDIS_CLI -p $TEST_PORT VEMB test_vectors emb:00000001)
    END_TIME=$(date +%s%N)
    LATENCY_US=$(( (END_TIME - START_TIME) / 1000 ))
    echo "VEMB request $i: ${LATENCY_US}μs"
done

echo "6. Cleanup..."
kill $REDIS_PID 2>/dev/null || true
wait $REDIS_PID 2>/dev/null || true
rm -f "$TEST_CONFIG" "$TEST_PROGRAM"

echo "=== Test completed ==="