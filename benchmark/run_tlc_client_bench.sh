#!/bin/bash
#
# tlc_client_bench 一键执行脚本
# 自动启动服务器、填充数据、运行测试、生成报告
#

set -e

# ============================================================
# 配置
# ============================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
REDIS_SERVER="$PROJECT_DIR/src/redis-server"
REDIS_CLI="$PROJECT_DIR/src/redis-cli"
TLC_MODULE="$PROJECT_DIR/src/modules/tlc_module.so"
BENCHMARK="$SCRIPT_DIR/tlc_client_bench"

OPS=${1:-500000}
THREADS=${2:-8}
PIPELINE=${3:-16}
RESULTS_DIR="$PROJECT_DIR/benchmark/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_FILE="$RESULTS_DIR/tlc_bench_$TIMESTAMP.log"

# ============================================================
# 颜色输出
# ============================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_header() {
    echo ""
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║  $1${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# ============================================================
# 检查依赖
# ============================================================
check_dependencies() {
    print_header "检查依赖"
    
    if [ ! -f "$REDIS_SERVER" ]; then
        print_error "redis-server not found: $REDIS_SERVER"
        exit 1
    fi
    print_success "redis-server: $REDIS_SERVER"
    
    if [ ! -f "$REDIS_CLI" ]; then
        print_error "redis-cli not found: $REDIS_CLI"
        exit 1
    fi
    print_success "redis-cli: $REDIS_CLI"
    
    if [ ! -f "$TLC_MODULE" ]; then
        print_error "TLC module not found: $TLC_MODULE"
        exit 1
    fi
    print_success "TLC module: $TLC_MODULE"
    
    if [ ! -f "$BENCHMARK" ]; then
        print_error "benchmark not found: $BENCHMARK"
        exit 1
    fi
    print_success "benchmark: $BENCHMARK"
    
    mkdir -p "$RESULTS_DIR"
    print_success "Results directory: $RESULTS_DIR"
}

# ============================================================
# 清理旧进程
# ============================================================
cleanup() {
    print_header "清理旧进程"
    
    pkill -9 redis-server 2>/dev/null || true
    pkill -9 tlc-server 2>/dev/null || true
    sleep 2
    
    # 确认清理完成
    if pgrep redis-server > /dev/null; then
        print_error "redis-server still running"
        exit 1
    fi
    
    print_success "旧进程已清理"
}

# ============================================================
# 启动 Baseline Redis (6379)
# ============================================================
start_baseline() {
    print_header "启动 Baseline Redis (6379)"
    
    # 创建配置文件
    cat > /tmp/redis-baseline-6379.conf << 'EOF'
port 6379
daemonize yes
bind 0.0.0.0
save ""
appendonly no
io-threads 4
io-threads-do-reads yes
pidfile /tmp/redis-6379.pid
logfile /tmp/redis-6379.log
dir /tmp
EOF
    
    $REDIS_SERVER /tmp/redis-baseline-6379.conf
    sleep 5
    
    if $REDIS_CLI -p 6379 ping > /dev/null 2>&1; then
        print_success "Baseline Redis 启动成功 (端口 6379)"
    else
        print_error "Baseline Redis 启动失败"
        cat /tmp/redis-6379.log
        exit 1
    fi
}

# ============================================================
# 启动 Optimized Redis + TLC Module (6380)
# ============================================================
start_optimized() {
    print_header "启动 Optimized Redis + TLC Module (6380)"
    
    # 创建配置文件
    cat > /tmp/redis-optimized-6380.conf << 'EOF'
port 6380
daemonize yes
bind 0.0.0.0
save ""
appendonly no
io-threads 4
io-threads-do-reads yes
maxmemory 8589934592
pidfile /tmp/redis-6380.pid
logfile /tmp/redis-6380.log
dir /tmp
EOF
    
    $REDIS_SERVER /tmp/redis-optimized-6380.conf --loadmodule $TLC_MODULE
    sleep 10
    
    if $REDIS_CLI -p 6380 ping > /dev/null 2>&1; then
        print_success "Optimized Redis + TLC 启动成功 (端口 6380)"
    else
        print_error "Optimized Redis 启动失败"
        cat /tmp/redis-6380.log | tail -30
        exit 1
    fi
}

# ============================================================
# 填充数据
# ============================================================
fill_data() {
    print_header "填充数据"
    
    print_info "填充 TLC cache (1.1M entries, port 6380)..."
    $REDIS_CLI -p 6380 TLC.FILL 1100000
    
    print_success "数据填充完成"
}

# ============================================================
# 运行 Benchmark
# ============================================================
run_benchmark() {
    print_header "运行 tlc_client_bench"
    
    print_info "参数: Ops=$OPS Threads=$THREADS Pipeline=$PIPELINE"
    print_info "结果保存至: $RESULT_FILE"
    
    # 验证服务器状态
    print_info "验证服务器状态..."
    $REDIS_CLI -p 6379 ping || { print_error "Baseline (6379) 未响应"; exit 1; }
    $REDIS_CLI -p 6380 ping || { print_error "Optimized (6380) 未响应"; exit 1; }
    print_success "服务器状态正常"
    
    # 运行 benchmark
    $BENCHMARK --ops $OPS --threads $THREADS --pipeline $PIPELINE | tee "$RESULT_FILE"
    
    print_success "Benchmark 完成"
}

# ============================================================
# 收集统计信息
# ============================================================
collect_stats() {
    print_header "收集统计信息"
    
    echo "" >> "$RESULT_FILE"
    echo "=== TLC Cache Statistics ===" >> "$RESULT_FILE"
    $REDIS_CLI -p 6380 TLC.STATS >> "$RESULT_FILE"
    
    echo "" >> "$RESULT_FILE"
    echo "=== Redis Info (6379) ===" >> "$RESULT_FILE"
    $REDIS_CLI -p 6379 INFO memory >> "$RESULT_FILE"
    
    echo "" >> "$RESULT_FILE"
    echo "=== Redis Info (6380) ===" >> "$RESULT_FILE"
    $REDIS_CLI -p 6380 INFO memory >> "$RESULT_FILE"
    
    print_success "统计信息已保存"
}

# ============================================================
# 停止服务器
# ============================================================
stop_servers() {
    print_header "停止服务器"
    
    $REDIS_CLI -p 6379 shutdown nosave 2>/dev/null || true
    $REDIS_CLI -p 6380 shutdown nosave 2>/dev/null || true
    sleep 2
    
    if pgrep redis-server > /dev/null; then
        print_info "强制终止残留进程..."
        pkill -9 redis-server
    fi
    
    print_success "所有服务器已停止"
}

# ============================================================
# 生成报告
# ============================================================
generate_report() {
    print_header "生成报告"
    
    print_info "结果文件: $RESULT_FILE"
    
    echo ""
    echo -e "${GREEN}============================================================${NC}"
    echo -e "${GREEN}  测试完成！${NC}"
    echo -e "${GREEN}============================================================${NC}"
    echo ""
    echo "查看结果:"
    echo "  cat $RESULT_FILE"
    echo ""
}

# ============================================================
# 主流程
# ============================================================
main() {
    print_header "TLC Client Benchmark 一键执行"
    
    echo "配置:"
    echo "  Ops:       $OPS"
    echo "  Threads:   $THREADS"
    echo "  Pipeline:  $PIPELINE"
    echo ""
    
    check_dependencies
    cleanup
    start_baseline
    start_optimized
    fill_data
    run_benchmark
    collect_stats
    stop_servers
    generate_report
}

# ============================================================
# 帮助信息
# ============================================================
show_help() {
    echo "Usage: $0 [ops] [threads] [pipeline]"
    echo ""
    echo "Arguments:"
    echo "  ops       Number of operations (default: 500000)"
    echo "  threads   Number of threads (default: 8)"
    echo "  pipeline  Pipeline size (default: 16)"
    echo ""
    echo "Examples:"
    echo "  $0                    # 默认配置"
    echo "  $0 1000000 16 32      # 自定义配置"
    echo ""
    exit 0
}

# ============================================================
# 参数解析
# ============================================================
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    show_help
fi

# ============================================================
# 执行
# ============================================================
main