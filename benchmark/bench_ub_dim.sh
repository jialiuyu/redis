#!/bin/bash
# bench_ub_dim.sh — 测试指定 vector dimension 下 UB gather 性能
#
# 用法:
#   ./bench_ub_dim.sh write <dim> [shm_memid] [shm_size] [fill_rows]
#   ./bench_ub_dim.sh read  <dim> [shm_memid] [shm_size] [fill_rows] [--no-mock]
#   ./bench_ub_dim.sh all   <dim> [shm_memid] [shm_size] [fill_rows] [--no-mock]
#
# 示例:
#   节点 111:  ./bench_ub_dim.sh write 128 5 8G 1024
#   节点 112:  ./bench_ub_dim.sh read  128 5 8G 1024           # UB.MEM + mock 对比
#   节点 112:  ./bench_ub_dim.sh read  128 5 8G 1024 --no-mock # 只显示 UB.MEM
#   单节点:    ./bench_ub_dim.sh all   128 5 8G 1024
#
#   多 dim 扫描:
#   for dim in 16 64 128 256 512 1024; do ./bench_ub_dim.sh all $dim; done

MODE=${1:-all}
DIM=${2:-128}
SHM_MEMID=${3:-5}
SHM_SIZE=${4:-8G}
FILL_ROWS=${5:-1024}
SHOW_MOCK=yes   # set to 'no' via --no-mock to skip local baseline
UT=./ub_client_ut

# parse optional --no-mock flag anywhere in args
for arg in "$@"; do
    [ "$arg" = "--no-mock" ] && SHOW_MOCK=no
done

if [ ! -x "$UT" ]; then
    echo "ERROR: $UT not found, run: make ub_client_ut [USE_SVE=yes]"
    exit 1
fi

if [ "$MODE" != "write" ] && [ "$MODE" != "read" ] && [ "$MODE" != "all" ]; then
    echo "Usage: $0 write|read|all <dim> [shm_memid] [shm_size] [fill_rows]"
    exit 1
fi

if ! echo "$DIM" | grep -qE '^[0-9]+$'; then
    echo "ERROR: dim must be a positive integer, got: $DIM"
    exit 1
fi

# ============================================================
# WRITE: 写入 fixture 数据
# ============================================================
do_write() {
    echo "========================================================"
    echo " WRITE fixture: dim=$DIM memid=$SHM_MEMID size=$SHM_SIZE rows=$FILL_ROWS"
    echo "========================================================"
    result=$($UT write-fixture \
        --shm-memid "$SHM_MEMID" \
        --shm-size  "$SHM_SIZE" \
        --vector-dimension "$DIM" \
        --fill-rows "$FILL_ROWS" \
        --cacheable false \
        --use-ownership false 2>&1)
    status=$?
    if [ $status -eq 0 ]; then
        echo "  dim=$DIM  OK  (rows=$FILL_ROWS stride=$((DIM * 4)) bytes)"
    else
        echo "  dim=$DIM  FAILED: $result"
        return 1
    fi
    echo "========================================================"
    echo " Write complete. Run 'bench_ub_dim.sh read $DIM' on the reader node."
    echo "========================================================"
}

# ============================================================
# 解析 gather 输出中的关键指标
# ============================================================
parse_gather_output() {
    local output="$1"
    echo "$output" | grep -E 'source|method|gather_load|memcpy|data|throughput|per row'
    local verify
    verify=$(echo "$output" | grep 'mismatch' | wc -l | tr -d ' ')
    if [ "$verify" -gt 0 ]; then
        echo "  verify      : MISMATCH ($verify rows)"
    else
        echo "  verify      : OK"
    fi
}

# ============================================================
# READ: gather 并测速（UB.MEM 真实读取 + mock 本地对比）
# ============================================================
do_read() {
    echo "========================================================"
    echo " READ benchmark: dim=$DIM memid=$SHM_MEMID size=$SHM_SIZE rows=$FILL_ROWS"
    echo "========================================================"

    # --- 真实 UB.MEM 读取 ---
    echo ""
    echo "[ UB.MEM read ]"
    output=$($UT gather \
        --shm-memid "$SHM_MEMID" \
        --shm-size  "$SHM_SIZE" \
        --vector-dimension "$DIM" \
        --table-name ut_vectors \
        --gather-indices all \
        --fill-rows "$FILL_ROWS" \
        --cacheable false \
        --use-ownership false \
        --verify 2>&1)
    parse_gather_output "$output"

    # --- mock 本地内存对比（排除 UB 链路，测纯 gather_load 基线）---
    if [ "$SHOW_MOCK" = "yes" ]; then
        echo ""
        echo "[ local mock (baseline, no UB) ]"
        output_mock=$($UT gather \
            --shm-memid "$SHM_MEMID" \
            --shm-size  "$SHM_SIZE" \
            --vector-dimension "$DIM" \
            --table-name ut_vectors \
            --gather-indices all \
            --fill-rows "$FILL_ROWS" \
            --cacheable false \
            --use-ownership false \
            --mock-local \
            --verify 2>&1)
        parse_gather_output "$output_mock"
    fi

    echo "========================================================"
}

case "$MODE" in
    write) do_write ;;
    read)  do_read  ;;
    all)   do_write && echo && do_read ;;
esac
