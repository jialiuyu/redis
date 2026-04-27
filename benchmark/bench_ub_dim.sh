#!/bin/bash
# bench_ub_dim.sh — 测试不同 vector dimension 下 UB gather 性能
#
# 用法:
#   ./bench_ub_dim.sh write [shm_memid] [shm_size] [fill_rows]   # 节点 111: 写入数据
#   ./bench_ub_dim.sh read  [shm_memid] [shm_size] [fill_rows]   # 节点 112: 读取并测速
#   ./bench_ub_dim.sh all   [shm_memid] [shm_size] [fill_rows]   # 单节点: 写+读
#
# 示例:
#   节点 111:  ./bench_ub_dim.sh write 5 8G 1024
#   节点 112:  ./bench_ub_dim.sh read  5 8G 1024

MODE=${1:-all}
SHM_MEMID=${2:-5}
SHM_SIZE=${3:-8G}
FILL_ROWS=${4:-1024}
UT=./ub_client_ut

if [ ! -x "$UT" ]; then
    echo "ERROR: $UT not found, run: make ub_client_ut [USE_SVE=yes]"
    exit 1
fi

if [ "$MODE" != "write" ] && [ "$MODE" != "read" ] && [ "$MODE" != "all" ]; then
    echo "Usage: $0 write|read|all [shm_memid] [shm_size] [fill_rows]"
    exit 1
fi

# ============================================================
# WRITE: 节点 111 写入 fixture 数据
# ============================================================
do_write() {
    echo "========================================================"
    echo " WRITE fixture: memid=$SHM_MEMID size=$SHM_SIZE rows=$FILL_ROWS"
    echo "========================================================"
    for dim in 16 64 128 256 512 1024; do
        result=$($UT write-fixture \
            --shm-memid "$SHM_MEMID" \
            --shm-size  "$SHM_SIZE" \
            --vector-dimension "$dim" \
            --fill-rows "$FILL_ROWS" \
            --cacheable false \
            --use-ownership false 2>&1)
        status=$?
        if [ $status -eq 0 ]; then
            echo "  dim=$dim  OK  (rows=$FILL_ROWS stride=$((dim * 4)) bytes)"
        else
            echo "  dim=$dim  FAILED: $result"
        fi
    done
    echo "========================================================"
    echo " Write complete. Run 'bench_ub_dim.sh read' on the reader node."
    echo "========================================================"
}

# ============================================================
# READ: 节点 112 gather 并测速
# ============================================================
do_read() {
    echo "========================================================"
    echo " READ benchmark: memid=$SHM_MEMID size=$SHM_SIZE rows=$FILL_ROWS"
    echo "========================================================"
    printf "%-8s  %-10s  %-16s  %-14s  %-14s\n" \
        "dim" "data(MB)" "gather_load(us)" "UB.MEM(us)" "throughput"
    echo "--------  ----------  ----------------  --------------  --------------"

    for dim in 16 64 128 256 512 1024; do
        output=$($UT gather \
            --shm-memid "$SHM_MEMID" \
            --shm-size  "$SHM_SIZE" \
            --vector-dimension "$dim" \
            --table-name ut_vectors \
            --gather-indices all \
            --fill-rows "$FILL_ROWS" \
            --cacheable false \
            --use-ownership false \
            --verify 2>&1)

        gather_us=$(echo "$output" | grep -E 'gather_load|memcpy' | grep -oP '[\d.]+(?= us)')
        ubmem_us=$(echo  "$output" | grep 'UB.MEM read'           | grep -oP '[\d.]+(?= us)')
        bw=$(echo        "$output" | grep 'throughput'             | grep -oP '[\d.]+(?= MB/s)')
        data_mb=$(echo   "$output" | grep 'data'                   | grep -oP '[\d.]+(?= MB)')
        verify=$(echo    "$output" | grep -c 'mismatch' || true)

        verify_str="OK"
        [ "$verify" -gt 0 ] && verify_str="MISMATCH($verify)"

        printf "%-8s  %-10s  %-16s  %-14s  %-14s  %s\n" \
            "$dim" "${data_mb:-?}" "${gather_us:-?}" "${ubmem_us:-?}" \
            "${bw:-?} MB/s" "$verify_str"
    done
    echo "========================================================"
}

case "$MODE" in
    write) do_write ;;
    read)  do_read  ;;
    all)   do_write; echo; do_read ;;
esac
