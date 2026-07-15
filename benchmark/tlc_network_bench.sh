#!/bin/bash
# V10 Network Benchmark: Baseline Redis SET/GET vs TLC.PUT/GET via redis-benchmark
# Uses real data, real network, real 1M+ entries
set -e

REDIS_CLI="./output/src/redis-cli"
REDIS_BENCH="./output/src/redis-benchmark"
N=${1:-500000}
C=${2:-50}
T=${3:-8}

echo "╔═══════════════════════════════════════════════════════════════════╗"
echo "║  V10 Network Benchmark: Baseline Redis vs TLC Module            ║"
echo "║  Ops: $N  Clients: $C  Threads: $T                             ║"
echo "╚═══════════════════════════════════════════════════════════════════╝"
echo ""

# Verify both servers
echo "Checking servers..."
$REDIS_CLI -p 6379 ping > /dev/null 2>&1 || { echo "ERROR: Baseline (6379) not running"; exit 1; }
$REDIS_CLI -p 6380 ping > /dev/null 2>&1 || { echo "ERROR: Optimized (6380) not running"; exit 1; }
echo "  Baseline (6379): OK"
echo "  Optimized+TLC (6380): OK"
echo ""

# ================================================================
# Section 1: Baseline Redis — standard SET/GET with 1200B values
# ================================================================
echo "╔══════════════════════════════════════╗"
echo "║  Baseline Redis (port 6379)          ║"
echo "╚══════════════════════════════════════╝"

echo ""
echo "--- Pre-fill 1M keys with 1200B values ---"
$REDIS_BENCH -p 6379 -t set -n 1000000 -c $C --threads $T -d 1200 -q 2>&1 | tail -1

echo ""
echo "--- SET 1200B ---"
$REDIS_BENCH -p 6379 -t set -n $N -c $C --threads $T -d 1200 -q 2>&1

echo "--- GET 1200B ---"
$REDIS_BENCH -p 6379 -t get -n $N -c $C --threads $T -q 2>&1

echo "--- Pipeline SET 1200B (P=16) ---"
$REDIS_BENCH -p 6379 -t set -n $N -c $C --threads $T -d 1200 -P 16 -q 2>&1

echo "--- Pipeline GET (P=16) ---"
$REDIS_BENCH -p 6379 -t get -n $N -c $C --threads $T -P 16 -q 2>&1

# ================================================================
# Section 2: TLC Module — TLC.PUT/GET via custom commands
# ================================================================
echo ""
echo "╔══════════════════════════════════════╗"
echo "║  TLC Module (port 6380)              ║"
echo "╚══════════════════════════════════════╝"

echo ""
echo "--- TLC.FILL 1M entries (1200B each, UB memory) ---"
$REDIS_CLI -p 6380 TLC.FILL 1000000 2>&1

echo ""
echo "--- TLC.PUT benchmark (redis-benchmark custom cmd) ---"
# Use redis-benchmark with custom command for TLC.PUT
# Generate random 1200-byte value
VAL=$(python3 -c "import os; print(os.urandom(1200).hex())" 2>/dev/null || head -c 2400 /dev/urandom | od -An -tx1 | tr -d ' \n' | head -c 2400)
$REDIS_BENCH -p 6380 -n $N -c $C --threads $T -q \
    EVAL "redis.call('TLC.PUT', KEYS[1], ARGV[1]); return 1" 1 __rand_int__ "${VAL:0:1200}" 2>&1

echo ""
echo "--- TLC.GET benchmark (redis-benchmark custom cmd) ---"
$REDIS_BENCH -p 6380 -n $N -c $C --threads $T -q \
    EVAL "return redis.call('TLC.GET', KEYS[1])" 1 __rand_int__ 2>&1

echo ""
echo "--- Standard SET/GET on optimized server (for io-threads comparison) ---"
echo "SET 1200B:"
$REDIS_BENCH -p 6380 -t set -n $N -c $C --threads $T -d 1200 -q 2>&1
echo "GET:"
$REDIS_BENCH -p 6380 -t get -n $N -c $C --threads $T -q 2>&1

echo ""
echo "--- TLC.STATS ---"
$REDIS_CLI -p 6380 TLC.STATS 2>&1

echo ""
echo "╔══════════════════════════════════════╗"
echo "║  Benchmark Complete                  ║"
echo "╚══════════════════════════════════════╝"
