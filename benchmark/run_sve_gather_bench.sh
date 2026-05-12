#!/bin/bash
# run_sve_gather_bench.sh
# Build and run SVE cross-embedding gather benchmark on real UB.MEM
#
# Usage:
#   ./run_sve_gather_bench.sh [memid] [shm_size] [dim]
#   ./run_sve_gather_bench.sh              # defaults: memid=1, size=8G, dim=300
#   ./run_sve_gather_bench.sh 1 8G 300
#
# Mock-local mode (no UB device):
#   ./run_sve_gather_bench.sh mock
#
# With CSV output:
#   ./run_sve_gather_bench.sh 1 8G 300 results/$(date +%Y%m%d_%H%M)_gather.csv

set -euo pipefail
cd "$(dirname "$0")"

MEMID=${1:-1}
SHM_SIZE=${2:-8G}
DIM=${3:-300}
TABLE_NAME="ut_vectors"
BATCH_SIZES="8,16,32,64,128,256,512,1024"
WARMUP=50
ITERS=200
CSV_ARG=""

MOCK_LOCAL=0
if [ "$MEMID" = "mock" ]; then
    MOCK_LOCAL=1
    shift 2>/dev/null || true
    # Optional CSV for mock mode
    if [ $# -ge 1 ]; then
        CSV_ARG="--csv $1"
    fi
else
    # Optional CSV for real UB mode
    if [ $# -ge 4 ]; then
        CSV_ARG="--csv $4"
    fi
fi

echo "=== Building NC mode (sve_gather_ub_bench) ==="
make sve_gather_ub_bench USE_SVE=yes

echo "=== Building CC mode (sve_gather_ub_bench_cc) ==="
make sve_gather_ub_bench USE_SVE=yes USE_CC_MODE=yes
cp sve_gather_ub_bench sve_gather_ub_bench_nc
cp sve_gather_ub_bench sve_gather_ub_bench_cc 2>/dev/null || true

if [ "$MOCK_LOCAL" -eq 1 ]; then
    echo ""
    echo "=== Running mock-local benchmark ==="
    ./sve_gather_ub_bench \
        --mock-local \
        --vector-dimension "$DIM" \
        --batch-sizes "$BATCH_SIZES" \
        --warmup "$WARMUP" \
        --iters "$ITERS" \
        --verify \
        $CSV_ARG
    echo ""
    echo "=== Done (mock-local) ==="
    exit 0
fi

echo ""
echo "=== Running NC mode benchmark ==="
echo "  memid=$MEMID  size=$SHM_SIZE  dim=$DIM"
./sve_gather_ub_bench \
    --shm-memid "$MEMID" \
    --shm-size "$SHM_SIZE" \
    --vector-dimension "$DIM" \
    --table-name "$TABLE_NAME" \
    --batch-sizes "$BATCH_SIZES" \
    --warmup "$WARMUP" \
    --iters "$ITERS" \
    --verify \
    --csv "results/gather_nc_$(date +%Y%m%d_%H%M).csv" \
    $CSV_ARG

echo ""
echo "=== Done (NC mode) ==="
echo ""
echo "For CC mode, run on the consumer node:"
echo "  ./sve_gather_ub_bench --shm-memid $MEMID --shm-size $SHM_SIZE \\"
echo "    --vector-dimension $DIM --table-name $TABLE_NAME \\"
echo "    --batch-sizes $BATCH_SIZES --cacheable yes --use-ownership yes \\"
echo "    --csv results/gather_cc_\$(date +%Y%m%d_%H%M).csv"
