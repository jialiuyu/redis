#!/bin/bash
#
# SVE2性能测试脚本
# 用于在ARM SVE2硬件上验证性能
#

set -e

# 颜色输出
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}SVE2 性能测试脚本${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 检测CPU架构
ARCH=$(uname -m)
echo -e "${BLUE}检测CPU架构...${NC}"
echo "架构: $ARCH"

# 检测SVE支持
if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
    echo -e "${GREEN}✅ ARM64架构检测到${NC}"
    
    # 检查SVE特性
    if grep -q sve /proc/cpuinfo 2>/dev/null; then
        echo -e "${GREEN}✅ SVE支持检测到${NC}"
        SVE_AVAILABLE=1
    else
        echo -e "${YELLOW}⚠️  SVE未检测到，将使用标量fallback${NC}"
        SVE_AVAILABLE=0
    fi
else
    echo -e "${YELLOW}⚠️  非ARM架构 ($ARCH)，将使用标量fallback${NC}"
    SVE_AVAILABLE=0
fi

echo ""

# 编译测试程序
echo -e "${BLUE}编译测试程序...${NC}"
make clean
make supernode_benchmark

echo ""

# 创建结果目录
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="results/sve2_test_${TIMESTAMP}"
mkdir -p "$RESULTS_DIR"

echo -e "${BLUE}结果目录: $RESULTS_DIR${NC}"
echo ""

# 测试1: 快速测试 (100K查询)
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}测试1: 快速测试 (100K查询)${NC}"
echo -e "${BLUE}========================================${NC}"
./supernode_benchmark --queries 100000 --threads 4 | tee "$RESULTS_DIR/test_100k.txt"
echo ""

# 测试2: 中等规模 (1M查询)
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}测试2: 中等规模 (1M查询)${NC}"
echo -e "${BLUE}========================================${NC}"
./supernode_benchmark --queries 1000000 --threads 8 | tee "$RESULTS_DIR/test_1m.txt"
echo ""

# 测试3: 大规模 (10M查询)
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}测试3: 大规模 (10M查询)${NC}"
echo -e "${BLUE}========================================${NC}"
./supernode_benchmark --queries 10000000 --threads 16 | tee "$RESULTS_DIR/test_10m.txt"
echo ""

# 提取性能数据
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}性能数据汇总${NC}"
echo -e "${BLUE}========================================${NC}"

echo "测试配置:"
echo "  CPU架构: $ARCH"
echo "  SVE支持: $([ $SVE_AVAILABLE -eq 1 ] && echo '是' || echo '否')"
echo ""

echo "性能结果:"
echo ""

# 从测试结果中提取QPS和延迟
for test_file in "$RESULTS_DIR"/test_*.txt; do
    test_name=$(basename "$test_file" .txt)
    echo "[$test_name]"
    
    qps=$(grep "Throughput:" "$test_file" | awk '{print $2, $3, $4}')
    latency=$(grep "Average:" "$test_file" | head -1 | awk '{print $2, $3}')
    
    echo "  吞吐量: $qps"
    echo "  延迟: $latency"
    echo ""
done

# 生成汇总报告
SUMMARY_FILE="$RESULTS_DIR/summary.txt"
cat > "$SUMMARY_FILE" << EOF
SVE2性能测试汇总报告
==================

测试时间: $(date '+%Y-%m-%d %H:%M:%S')
CPU架构: $ARCH
SVE支持: $([ $SVE_AVAILABLE -eq 1 ] && echo '是' || echo '否')

测试结果:
EOF

for test_file in "$RESULTS_DIR"/test_*.txt; do
    echo "" >> "$SUMMARY_FILE"
    basename "$test_file" .txt >> "$SUMMARY_FILE"
    echo "---" >> "$SUMMARY_FILE"
    grep -A 10 "Results" "$test_file" >> "$SUMMARY_FILE" || true
done

echo -e "${GREEN}✅ 测试完成！${NC}"
echo ""
echo "结果保存在: $RESULTS_DIR"
echo "  - test_100k.txt  : 100K查询测试"
echo "  - test_1m.txt    : 1M查询测试"
echo "  - test_10m.txt   : 10M查询测试"
echo "  - summary.txt    : 汇总报告"
echo ""

# 如果是ARM SVE2硬件，显示加速比
if [ $SVE_AVAILABLE -eq 1 ]; then
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}SVE2硬件加速验证${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo "请对比以下数据验证SVE2加速效果:"
    echo ""
    echo "预期标量性能: ~16.5M QPS, 0.30 μs延迟"
    echo "预期SVE2性能: ~264M QPS, 0.019 μs延迟"
    echo "预期加速比: 16x"
    echo ""
    echo "实际测试结果请查看上述输出"
    echo ""
fi

echo -e "${BLUE}查看详细报告:${NC}"
echo "  cat $SUMMARY_FILE"
echo ""
