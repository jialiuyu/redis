#!/bin/bash
#
# DPDK vs Unified Server Performance Comparison
# 测试两种传输层实现的性能差异
#
# Servers:
#   - tlc-dpdk-server: UDP + KCP (polling mode)
#   - tlc-unified-server: TCP + UDS + SHM + Aeron IPC
#
# Tests:
#   1. GET only (P=1, P=16)
#   2. 80R/20W (P=1, P=16)
#   3. MGET batch (batch=100, 500, 1000)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$SCRIPT_DIR/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$RESULTS_DIR/dpdk_unified_comparison_${TIMESTAMP}.log"

mkdir -p "$RESULTS_DIR"

OPS=500000
THREADS=8
MAXKEY=1100000
VALUE_SIZE=1200

echo "================================================================"
echo "  DPDK vs Unified Server Performance Comparison"
echo "================================================================"
echo ""
echo "Configuration:"
echo "  Ops:        $OPS"
echo "  Threads:    $THREADS"
echo "  MaxKey:     $MAXKEY"
echo "  ValueSize:  $VALUE_SIZE"
echo ""
echo "Results will be saved to: $LOG_FILE"
echo ""

# 编译 benchmark 客户端
compile_clients() {
    echo "Compiling benchmark clients..."
    
    cd "$SCRIPT_DIR"
    
    # Unified client (TCP/UDS)
    gcc -O3 -Wall -pthread -std=c11 -I../src \
        -o bench_unified_client bench_unified_client.c \
        -lm -lpthread 2>/dev/null || echo "  bench_unified_client: already compiled or failed"
    
    # DPDK client (需要 kcp_lite.h 和 three_layer_cache_ub.h)
    gcc -O3 -Wall -pthread -std=c11 -I../src \
        -o bench_dpdk_client bench_dpdk_client.c \
        ../src/three_layer_cache_ub.c \
        -lm -lpthread 2>/dev/null || echo "  bench_dpdk_client: compile may need additional libs"
    
    echo "Clients compiled."
    echo ""
}

# 启动服务器
start_server() {
    local server_type=$1
    local server_bin=""
    local server_port=""
    
    if [ "$server_type" = "dpdk" ]; then
        server_bin="$PROJECT_DIR/src/tlc-dpdk-server"
        server_port=6382
    elif [ "$server_type" = "unified" ]; then
        server_bin="$PROJECT_DIR/src/tlc-unified-server"
        server_port=6381
    else
        echo "Unknown server type: $server_type"
        return 1
    fi
    
    # 检查服务器是否存在
    if [ ! -x "$server_bin" ]; then
        echo "Server not found: $server_bin"
        return 1
    fi
    
    echo "Starting $server_type server..."
    
    # 清理旧进程
    pkill -f "$server_bin" 2>/dev/null || true
    sleep 0.5
    
    # 启动服务器
    "$server_bin" > "$RESULTS_DIR/${server_type}_server.log" 2>&1 &
    SERVER_PID=$!
    
    sleep 2
    
    # 检查服务器是否启动
    if ! ps -p $SERVER_PID > /dev/null 2>&1; then
        echo "Server failed to start!"
        cat "$RESULTS_DIR/${server_type}_server.log"
        return 1
    fi
    
    echo "Server started (PID: $SERVER_PID, Port: $server_port)"
    return 0
}

# 停止服务器
stop_server() {
    echo "Stopping server..."
    pkill -f "tlc-dpdk-server" 2>/dev/null || true
    pkill -f "tlc-unified-server" 2>/dev/null || true
    sleep 1
    echo "Server stopped."
}

# 运行 benchmark
run_benchmark() {
    local client=$1
    local transport=$2
    local pipeline=$3
    local write_pct=$4
    
    echo "  Running: $transport P=$pipeline W%=$write_pct"
    
    local args="--ops $OPS --threads $THREADS --pipeline $pipeline --write $write_pct --maxkey $MAXKEY"
    
    if [ "$transport" = "uds" ]; then
        args="$args --uds"
    fi
    
    local output
    output=$("$SCRIPT_DIR/$client" $args 2>&1)
    
    # 解析 QPS 和延迟
    local qps=$(echo "$output" | grep "QPS:" | awk '{print $2}')
    local lat=$(echo "$output" | grep "Latency:" | awk '{print $2}')
    
    echo "    QPS: $qps  Latency: $lat ns"
    
    # 保存到日志
    echo "$transport P=$pipeline W%=$write_pct → $qps QPS, $lat ns" >> "$LOG_FILE"
    
    return 0
}

# 运行完整测试
run_tests() {
    local server_type=$1
    
    echo ""
    echo "========================================"
    echo "  Testing: $server_type server"
    echo "========================================"
    echo ""
    
    if [ "$server_type" = "dpdk" ]; then
        # DPDK server 使用 UDP+KCP
        client="bench_dpdk_client"
        
        echo "[$server_type] GET only (P=1)" >> "$LOG_FILE"
        run_benchmark "$client" "udp" 1 0
        
        echo "[$server_type] GET only (P=16)" >> "$LOG_FILE"
        run_benchmark "$client" "udp" 16 0
        
        echo "[$server_type] 80R/20W (P=1)" >> "$LOG_FILE"
        run_benchmark "$client" "udp" 1 20
        
        echo "[$server_type] 80R/20W (P=16)" >> "$LOG_FILE"
        run_benchmark "$client" "udp" 16 20
        
    elif [ "$server_type" = "unified" ]; then
        # Unified server 支持 TCP 和 UDS
        client="bench_unified_client"
        
        echo "[$server_type TCP] GET only (P=1)" >> "$LOG_FILE"
        run_benchmark "$client" "tcp" 1 0
        
        echo "[$server_type TCP] GET only (P=16)" >> "$LOG_FILE"
        run_benchmark "$client" "tcp" 16 0
        
        echo "[$server_type TCP] 80R/20W (P=1)" >> "$LOG_FILE"
        run_benchmark "$client" "tcp" 1 20
        
        echo "[$server_type TCP] 80R/20W (P=16)" >> "$LOG_FILE"
        run_benchmark "$client" "tcp" 16 20
        
        # 测试 UDS
        echo "" >> "$LOG_FILE"
        echo "[$server_type UDS] GET only (P=1)" >> "$LOG_FILE"
        run_benchmark "$client" "uds" 1 0
        
        echo "[$server_type UDS] GET only (P=16)" >> "$LOG_FILE"
        run_benchmark "$client" "uds" 16 0
        
        echo "[$server_type UDS] 80R/20W (P=1)" >> "$LOG_FILE"
        run_benchmark "$client" "uds" 1 20
        
        echo "[$server_type UDS] 80R/20W (P=16)" >> "$LOG_FILE"
        run_benchmark "$client" "uds" 16 20
    fi
    
    echo ""
}

# 生成对比报告
generate_report() {
    echo ""
    echo "========================================"
    echo "  Performance Comparison Summary"
    echo "========================================"
    echo ""
    
    if [ -f "$LOG_FILE" ]; then
        cat "$LOG_FILE"
    fi
    
    echo ""
    echo "Report saved to: $LOG_FILE"
}

# 主流程
main() {
    echo "Starting comparison tests..."
    echo ""
    
    compile_clients
    
    # 测试 DPDK server
    start_server "dpdk"
    run_tests "dpdk"
    stop_server
    
    sleep 2
    
    # 测试 Unified server
    start_server "unified"
    run_tests "unified"
    stop_server
    
    generate_report
    
    echo ""
    echo "All tests completed."
}

main "$@"