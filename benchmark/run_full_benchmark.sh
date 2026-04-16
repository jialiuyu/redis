#!/bin/bash
#
# 完整基准测试脚本
# 运行传统 Redis 和 SuperNode 的完整性能对比测试
#

set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_header() {
    echo ""
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║  $1${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

# 检查依赖
check_dependencies() {
    print_header "检查依赖"
    
    # 检查 gcc
    if ! command -v gcc &> /dev/null; then
        print_error "gcc not found. Please install gcc."
        exit 1
    fi
    print_success "gcc found: $(gcc --version | head -n1)"
    
    # 检查 make
    if ! command -v make &> /dev/null; then
        print_error "make not found. Please install make."
        exit 1
    fi
    print_success "make found"
    
    # 检查 pthread
    if ! gcc -pthread -xc - -o /dev/null <<< 'int main(){}' 2>/dev/null; then
        print_error "pthread library not found"
        exit 1
    fi
    print_success "pthread library found"
}

# 编译基准测试
build_benchmarks() {
    print_header "编译基准测试程序"
    
    if [ ! -f "Makefile" ]; then
        print_error "Makefile not found. Please run from benchmark directory."
        exit 1
    fi
    
    print_info "Cleaning previous builds..."
    make clean 2>/dev/null || true
    
    print_info "Building benchmarks..."
    if make all; then
        print_success "All benchmarks built successfully"
    else
        print_error "Build failed"
        exit 1
    fi
}

# 创建结果目录
setup_results_dir() {
    print_header "准备结果目录"
    
    RESULTS_DIR="results"
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    RUN_DIR="${RESULTS_DIR}/run_${TIMESTAMP}"
    
    mkdir -p "${RUN_DIR}"
    print_success "Results directory: ${RUN_DIR}"
}

# 运行传统 Redis 基准测试
run_redis_benchmark() {
    print_header "运行传统 Redis 基准测试"
    
    local queries=${1:-10000000}
    local threads=${2:-16}
    local servers=${3:-350000}
    
    print_info "Configuration:"
    echo "  Queries: ${queries}"
    echo "  Threads: ${threads}"
    echo "  Servers: ${servers}"
    echo ""
    
    print_info "Starting Redis benchmark..."
    
    local output_file="${RUN_DIR}/redis_results.txt"
    
    if ./redis_traditional_benchmark \
        --queries ${queries} \
        --threads ${threads} \
        --servers ${servers} \
        | tee "${output_file}"; then
        print_success "Redis benchmark completed"
        print_info "Results saved to: ${output_file}"
    else
        print_error "Redis benchmark failed"
        return 1
    fi
}

# 运行 SuperNode 基准测试
run_supernode_benchmark() {
    print_header "运行 SuperNode 基准测试"
    
    local queries=${1:-10000000}
    local threads=${2:-16}
    local supernodes=${3:-150}
    
    print_info "Configuration:"
    echo "  Queries: ${queries}"
    echo "  Threads: ${threads}"
    echo "  SuperNodes: ${supernodes}"
    echo ""
    
    print_info "Starting SuperNode benchmark..."
    
    local output_file="${RUN_DIR}/supernode_results.txt"
    
    if ./supernode_benchmark \
        --queries ${queries} \
        --threads ${threads} \
        --supernodes ${supernodes} \
        | tee "${output_file}"; then
        print_success "SuperNode benchmark completed"
        print_info "Results saved to: ${output_file}"
    else
        print_error "SuperNode benchmark failed"
        return 1
    fi
}

# 生成对比报告
generate_comparison() {
    print_header "生成性能对比报告"
    
    local redis_file="${RUN_DIR}/redis_results.txt"
    local supernode_file="${RUN_DIR}/supernode_results.txt"
    local comparison_file="${RUN_DIR}/comparison_report.txt"
    
    if [ ! -f "${redis_file}" ] || [ ! -f "${supernode_file}" ]; then
        print_error "Result files not found"
        return 1
    fi
    
    print_info "Comparing results..."
    
    if ./compare_results "${redis_file}" "${supernode_file}" | tee "${comparison_file}"; then
        print_success "Comparison report generated"
        print_info "Report saved to: ${comparison_file}"
    else
        print_error "Comparison failed"
        return 1
    fi
}

# 生成 HTML 报告（可选）
generate_html_report() {
    print_header "生成 HTML 报告"
    
    local comparison_file="${RUN_DIR}/comparison_report.txt"
    local html_file="${RUN_DIR}/report.html"
    
    if [ ! -f "${comparison_file}" ]; then
        print_warning "Comparison report not found, skipping HTML generation"
        return 0
    fi
    
    cat > "${html_file}" <<'EOF'
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Redis UB+SVE 性能对比报告</title>
    <style>
        body {
            font-family: 'Courier New', monospace;
            background-color: #1e1e1e;
            color: #d4d4d4;
            padding: 20px;
            line-height: 1.6;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: #252526;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        }
        h1 {
            color: #4ec9b0;
            border-bottom: 2px solid #4ec9b0;
            padding-bottom: 10px;
        }
        pre {
            background-color: #1e1e1e;
            padding: 20px;
            border-radius: 4px;
            overflow-x: auto;
            border: 1px solid #3e3e42;
        }
        .timestamp {
            color: #858585;
            font-size: 0.9em;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Redis UB+SVE 性能对比报告</h1>
        <p class="timestamp">生成时间: $(date '+%Y-%m-%d %H:%M:%S')</p>
        <pre>
EOF
    
    cat "${comparison_file}" >> "${html_file}"
    
    cat >> "${html_file}" <<'EOF'
        </pre>
    </div>
</body>
</html>
EOF
    
    print_success "HTML report generated: ${html_file}"
}

# 打印系统信息
print_system_info() {
    print_header "系统信息"
    
    echo "Hostname: $(hostname)"
    echo "OS: $(uname -s) $(uname -r)"
    echo "Architecture: $(uname -m)"
    
    if [ -f /proc/cpuinfo ]; then
        echo "CPU: $(grep 'model name' /proc/cpuinfo | head -n1 | cut -d: -f2 | xargs)"
        echo "CPU Cores: $(nproc)"
    fi
    
    if [ -f /proc/meminfo ]; then
        local mem_total_kb
        mem_total_kb=$(awk '/MemTotal/{print $2}' /proc/meminfo)
        local mem_total_gb
        mem_total_gb=$(awk "BEGIN{printf \"%d\", ${mem_total_kb}/1024/1024}")
        echo "Memory: ${mem_total_gb} GB"
    fi
    
    echo ""
}

# 主函数
main() {
    print_header "Redis UB+SVE 完整基准测试"
    
    # 解析参数
    QUERIES=10000000
    THREADS=16
    REDIS_SERVERS=350000
    SUPERNODES=150
    SKIP_BUILD=0
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --queries)
                QUERIES="$2"
                shift 2
                ;;
            --threads)
                THREADS="$2"
                shift 2
                ;;
            --redis-servers)
                REDIS_SERVERS="$2"
                shift 2
                ;;
            --supernodes)
                SUPERNODES="$2"
                shift 2
                ;;
            --skip-build)
                SKIP_BUILD=1
                shift
                ;;
            --quick)
                QUERIES=1000000
                THREADS=8
                shift
                ;;
            --stress)
                QUERIES=100000000
                THREADS=32
                shift
                ;;
            --help)
                echo "Usage: $0 [options]"
                echo ""
                echo "Options:"
                echo "  --queries N         Number of queries (default: 10000000)"
                echo "  --threads N         Number of threads (default: 16)"
                echo "  --redis-servers N   Number of Redis servers (default: 350000)"
                echo "  --supernodes N      Number of supernodes (default: 150)"
                echo "  --skip-build        Skip compilation step"
                echo "  --quick             Quick test (1M queries, 8 threads)"
                echo "  --stress            Stress test (100M queries, 32 threads)"
                echo "  --help              Show this help"
                echo ""
                echo "Examples:"
                echo "  $0                          # Run default benchmark"
                echo "  $0 --quick                  # Quick test"
                echo "  $0 --stress                 # Stress test"
                echo "  $0 --queries 50000000       # Custom query count"
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
    
    # 打印系统信息
    print_system_info
    
    # 检查依赖
    check_dependencies
    
    # 编译（如果需要）
    if [ ${SKIP_BUILD} -eq 0 ]; then
        build_benchmarks
    else
        print_warning "Skipping build step"
    fi
    
    # 准备结果目录
    setup_results_dir
    
    # 运行基准测试
    if ! run_redis_benchmark ${QUERIES} ${THREADS} ${REDIS_SERVERS}; then
        print_error "Redis benchmark failed"
        exit 1
    fi
    
    if ! run_supernode_benchmark ${QUERIES} ${THREADS} ${SUPERNODES}; then
        print_error "SuperNode benchmark failed"
        exit 1
    fi
    
    # 生成对比报告
    if ! generate_comparison; then
        print_error "Failed to generate comparison report"
        exit 1
    fi
    
    # 生成 HTML 报告
    generate_html_report
    
    # 完成
    print_header "测试完成"
    print_success "All benchmarks completed successfully!"
    echo ""
    print_info "Results directory: ${RUN_DIR}"
    echo "  - redis_results.txt       : Redis benchmark results"
    echo "  - supernode_results.txt   : SuperNode benchmark results"
    echo "  - comparison_report.txt   : Performance comparison"
    echo "  - report.html             : HTML report"
    echo ""
    print_info "View comparison report:"
    echo "  cat ${RUN_DIR}/comparison_report.txt"
    echo ""
    print_info "View HTML report:"
    echo "  firefox ${RUN_DIR}/report.html"
    echo ""
}

# 运行主函数
main "$@"
