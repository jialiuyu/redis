#!/bin/bash

# Test script for Vector Engine Integration
# Tests both Redis and UB vector engine implementations

set -e

REDIS_SERVER="./redis/src/redis-server"
REDIS_CLI="./redis/src/redis-cli"
TEST_PORT=6379
TEST_CONFIG="/tmp/redis_test.conf"

echo "=== Redis Vector Engine Integration Test ==="

# Create test config
cat > "$TEST_CONFIG" << EOF
port $TEST_PORT
save ""
appendonly no
vector-engine redis
EOF

echo "1. Testing Redis vector engine..."

# Start Redis with vector engine
$REDIS_SERVER "$TEST_CONFIG" &
REDIS_PID=$!

# Wait for Redis to start
sleep 2

# Test VADD command
echo "   Testing VADD..."
$REDIS_CLI -p $TEST_PORT VADD test_vectors VALUES 3 1.0 2.0 3.0 test_elem1
$REDIS_CLI -p $TEST_PORT VADD test_vectors VALUES 3 4.0 5.0 6.0 test_elem2

# Test VCARD command
echo "   Testing VCARD..."
CARD_RESULT=$($REDIS_CLI -p $TEST_PORT VCARD test_vectors)
echo "   Vector set cardinality: $CARD_RESULT"

# Test VEMB command
echo "   Testing VEMB..."
EMB_RESULT=$($REDIS_CLI -p $TEST_PORT VEMB test_vectors test_elem1)
echo "   Embedding result: $EMB_RESULT"

# Test VENGINE command
echo "   Testing VENGINE..."
ENGINE_RESULT=$($REDIS_CLI -p $TEST_PORT VENGINE GET)
echo "   Current engine: $ENGINE_RESULT"

echo "2. Testing VENGINE SET command..."
$REDIS_CLI -p $TEST_PORT VENGINE SET UB
ENGINE_RESULT=$($REDIS_CLI -p $TEST_PORT VENGINE GET)
echo "   Switched to engine: $ENGINE_RESULT"

# Test VEMB with UB engine (should fallback to Redis for now)
echo "   Testing VEMB with UB engine..."
EMB_RESULT=$($REDIS_CLI -p $TEST_PORT VEMB test_vectors test_elem1)
echo "   Embedding result: $EMB_RESULT"

# Test VENGINE STATS
echo "   Testing VENGINE STATS..."
STATS_RESULT=$($REDIS_CLI -p $TEST_PORT VENGINE STATS)
echo "   Engine stats: $STATS_RESULT"

# Cleanup
echo "3. Cleaning up..."
kill $REDIS_PID
wait $REDIS_PID 2>/dev/null || true
rm -f "$TEST_CONFIG"

echo "=== Test completed successfully ==="