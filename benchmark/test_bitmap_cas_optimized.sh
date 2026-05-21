#!/bin/bash
#
# Bitmap CAS 优化测试脚本
#

set -e

BENCHMARK_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "========================================="
echo "Bitmap CAS Optimization Test"
echo "========================================="
echo ""

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查架构
ARCH=$(uname -m)
echo "Architecture: $ARCH"

if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
    ARCH_FLAGS="-march=armv8-a"
    echo -e "${GREEN}✅ ARM64 architecture detected${NC}"
elif [ "$ARCH" = "x86_64" ]; then
    ARCH_FLAGS="-march=native"
    echo -e "${GREEN}✅ x86_64 architecture detected${NC}"
else
    ARCH_FLAGS=""
    echo -e "${YELLOW}⚠️  Unknown architecture: $ARCH${NC}"
fi

echo ""

# 编译测试程序
echo "Compiling test program..."
echo "-------------------------------------------"

gcc -O3 $ARCH_FLAGS -pthread \
    -std=c11 \
    -Wall -Wextra \
    "$BENCHMARK_DIR/test_bitmap_cas_optimized.c" \
    -o "$BENCHMARK_DIR/test_bitmap_cas_optimized"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Compilation successful${NC}"
else
    echo -e "${RED}❌ Compilation failed${NC}"
    exit 1
fi

echo ""

# 运行测试
echo "Running tests..."
echo "-------------------------------------------"

"$BENCHMARK_DIR/test_bitmap_cas_optimized"

TEST_RESULT=$?

echo ""
echo "========================================="
echo "Test Summary"
echo "========================================="

if [ $TEST_RESULT -eq 0 ]; then
    echo -e "${GREEN}✅ All tests passed!${NC}"
    echo ""
    echo "Performance Summary:"
    echo "  - Alignment: 64 bytes (cache line)"
    echo "  - Memory order: acquire/release/relaxed"
    echo "  - Compared implementations: compare_exchange_weak vs fetch_or"
    echo "  - Includes both partitioned and high-contention hotspot modes"
    echo "  - CPU pause/yield: enabled in CAS path"
    echo ""
    echo "Review the throughput, latency, and retry counts above"
    echo "to compare the current fetch_or path against the optimized CAS path."
else
    echo -e "${RED}❌ Tests failed${NC}"
    exit 1
fi

echo ""
echo "Next steps:"
    echo "  1. Review docs/BITMAP_CAS_OPTIMIZATION.md for details"
echo "  2. Integrate optimized code into Redis"
echo "  3. Run full system tests"
echo "  4. Measure real-world performance"

exit 0
