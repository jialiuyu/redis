#!/bin/bash
# bench_ub_dim.sh — 测试 UB gather 性能并输出结构化结果
#
# 兼容旧用法:
#   ./bench_ub_dim.sh write <dim> [shm_memid] [shm_size] [fill_rows]
#   ./bench_ub_dim.sh read  <dim> [shm_memid] [shm_size] [fill_rows] [--no-mock]
#   ./bench_ub_dim.sh all   <dim> [shm_memid] [shm_size] [fill_rows] [--no-mock]
#
# 新增:
#   ./bench_ub_dim.sh sweep rows <dim> [shm_memid] [shm_size] [--no-mock]
#   ./bench_ub_dim.sh sweep dim  [fill_rows] [shm_memid] [shm_size] [--no-mock]
#
# 可选环境变量:
#   UB_WARMUP=3
#   UB_REPEAT=5
#   UB_ROW_SWEEP_LIST="64 256 1024 4096 16384 65536"
#   UB_DIM_SWEEP_LIST="16 64 128 256 300 512 1024"

set -u
set -o pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
RESULT_DIR="$SCRIPT_DIR/results"
UT="$SCRIPT_DIR/ub_client_ut"

DEFAULT_DIM=300
DEFAULT_SHM_MEMID=5
DEFAULT_SHM_SIZE=8G
DEFAULT_FILL_ROWS=1024
DEFAULT_ROW_SWEEP_LIST="${UB_ROW_SWEEP_LIST:-64 256 1024 4096 16384 65536}"
DEFAULT_DIM_SWEEP_LIST="${UB_DIM_SWEEP_LIST:-16 64 128 256 300 512 1024}"
WARMUP_RUNS="${UB_WARMUP:-3}"
REPEAT_RUNS="${UB_REPEAT:-5}"
SHOW_MOCK=yes
CSV_PATH=
SUMMARY_PATH=

usage() {
    cat <<EOF
Usage:
  $0 write <dim> [shm_memid] [shm_size] [fill_rows]
  $0 read  <dim> [shm_memid] [shm_size] [fill_rows] [--no-mock]
  $0 all   <dim> [shm_memid] [shm_size] [fill_rows] [--no-mock]
  $0 sweep rows <dim> [shm_memid] [shm_size] [--no-mock]
  $0 sweep dim  [fill_rows] [shm_memid] [shm_size] [--no-mock]

Recommended default benchmark:
  $0 sweep rows ${DEFAULT_DIM}

Environment:
  UB_WARMUP=${WARMUP_RUNS}
  UB_REPEAT=${REPEAT_RUNS}
  UB_ROW_SWEEP_LIST="${DEFAULT_ROW_SWEEP_LIST}"
  UB_DIM_SWEEP_LIST="${DEFAULT_DIM_SWEEP_LIST}"
EOF
}

is_non_negative_int() {
    [[ "${1:-}" =~ ^[0-9]+$ ]]
}

is_positive_int() {
    is_non_negative_int "${1:-}" && [ "${1:-0}" -gt 0 ]
}

require_positive_int() {
    local name="$1"
    local value="$2"

    if ! is_positive_int "$value"; then
        echo "ERROR: $name must be a positive integer, got: $value" >&2
        exit 1
    fi
}

require_non_negative_int() {
    local name="$1"
    local value="$2"

    if ! is_non_negative_int "$value"; then
        echo "ERROR: $name must be a non-negative integer, got: $value" >&2
        exit 1
    fi
}

require_ut() {
    if [ ! -x "$UT" ]; then
        echo "ERROR: $UT not found, run: (cd $SCRIPT_DIR && make ub_client_ut [USE_SVE=yes])" >&2
        exit 1
    fi
}

init_results() {
    local timestamp

    mkdir -p "$RESULT_DIR"
    timestamp=$(date '+%Y%m%d_%H%M%S')
    CSV_PATH="$RESULT_DIR/ub_dim_bench_${timestamp}.csv"
    SUMMARY_PATH="$RESULT_DIR/ub_dim_bench_${timestamp}.summary.txt"
}

write_fixture() {
    local dim="$1"
    local fill_rows="$2"

    echo "========================================================"
    echo " WRITE fixture: dim=$dim memid=$SHM_MEMID size=$SHM_SIZE rows=$fill_rows"
    echo "========================================================"
    "$UT" write-fixture \
        --shm-memid "$SHM_MEMID" \
        --shm-size "$SHM_SIZE" \
        --vector-dimension "$dim" \
        --fill-rows "$fill_rows" \
        --cacheable false \
        --use-ownership false
    local status=$?
    if [ $status -eq 0 ]; then
        echo "  dim=$dim  OK  (rows=$fill_rows stride=$((dim * 4)) bytes)"
    else
        echo "  dim=$dim  FAILED"
        return $status
    fi
    echo "========================================================"
    return 0
}

run_gather() {
    local label="$1"
    local case_name="$2"
    local dim="$3"
    local fill_rows="$4"
    shift 4

    echo
    echo "[ $label ]"
    "$UT" gather \
        --shm-memid "$SHM_MEMID" \
        --shm-size "$SHM_SIZE" \
        --vector-dimension "$dim" \
        --table-name ut_vectors \
        --gather-indices all \
        --fill-rows "$fill_rows" \
        --cacheable false \
        --use-ownership false \
        --warmup "$WARMUP_RUNS" \
        --repeat "$REPEAT_RUNS" \
        --csv "$CSV_PATH" \
        --case-name "$case_name" \
        --verify \
        "$@"
}

run_read_case() {
    local case_name="$1"
    local dim="$2"
    local fill_rows="$3"
    local rc=0

    echo "========================================================"
    echo " READ benchmark: dim=$dim memid=$SHM_MEMID size=$SHM_SIZE rows=$fill_rows"
    echo " CSV: $CSV_PATH"
    echo "========================================================"

    if ! run_gather "UB.MEM read" "$case_name" "$dim" "$fill_rows"; then
        rc=1
    fi

    if [ "$SHOW_MOCK" = "yes" ]; then
        if ! run_gather "local mock (baseline, no UB)" "$case_name" "$dim" "$fill_rows" --mock-local; then
            rc=1
        fi
    fi

    echo "========================================================"
    return $rc
}

generate_summary() {
    local csv_path="$1"
    local summary_path="$2"

    awk -F, '
        BEGIN {
            fmt = "%12s %8s %6s %12s %14s %12s %10s %16s %18s\n";
            printf fmt,
                   "bytes",
                   "rows",
                   "dim",
                   "ub_median_ns",
                   "mock_median_ns",
                   "delta_ns",
                   "delta_pct",
                   "ub_median_mb_s",
                   "mock_median_mb_s";
        }
        NR == 1 { next }
        NF < 16 { next }
        {
            key = $1;
            if (!(key in seen)) {
                seen[key] = 1;
                order[++count] = key;
            }
            bytes[key] = $5;
            rows[key] = $4;
            dims[key] = $3;
            if ($2 == "ub") {
                ub_ns[key] = $11;
                ub_mb[key] = $16;
            } else if ($2 == "mock_local") {
                mock_ns[key] = $11;
                mock_mb[key] = $16;
            }
        }
        END {
            for (i = 1; i <= count; i++) {
                key = order[i];
                ubv = (key in ub_ns) ? ub_ns[key] : "NA";
                mockv = (key in mock_ns) ? mock_ns[key] : "NA";
                ubmb = (key in ub_mb) ? ub_mb[key] : "NA";
                mockmb = (key in mock_mb) ? mock_mb[key] : "NA";
                if ((key in ub_ns) && (key in mock_ns)) {
                    delta_ns = sprintf("%.0f", ub_ns[key] - mock_ns[key]);
                    if (mock_ns[key] + 0 == 0) {
                        delta_pct = "NA";
                    } else {
                        delta_pct = sprintf("%.2f%%", ((ub_ns[key] - mock_ns[key]) / mock_ns[key]) * 100.0);
                    }
                } else {
                    delta_ns = "NA";
                    delta_pct = "NA";
                }
                printf fmt,
                       bytes[key],
                       rows[key],
                       dims[key],
                       ubv,
                       mockv,
                       delta_ns,
                       delta_pct,
                       ubmb,
                       mockmb;
            }
        }
    ' "$csv_path" > "$summary_path"
}

emit_results() {
    if [ ! -s "$CSV_PATH" ]; then
        echo "No CSV results were generated." >&2
        return 1
    fi

    generate_summary "$CSV_PATH" "$SUMMARY_PATH" || return 1

    echo
    echo "Results written:"
    echo "  CSV     : $CSV_PATH"
    echo "  Summary : $SUMMARY_PATH"
    echo
    cat "$SUMMARY_PATH"
    return 0
}

do_write_mode() {
    write_fixture "$DIM" "$FILL_ROWS"
    local rc=$?
    if [ $rc -eq 0 ]; then
        echo " Write complete. Run '$0 read $DIM $SHM_MEMID $SHM_SIZE $FILL_ROWS' on the reader node."
    fi
    return $rc
}

do_read_mode() {
    init_results
    run_read_case "read_dim${DIM}_rows${FILL_ROWS}" "$DIM" "$FILL_ROWS"
    local rc=$?
    emit_results || rc=1
    return $rc
}

do_all_mode() {
    init_results
    write_fixture "$DIM" "$FILL_ROWS" || return 1
    echo
    run_read_case "all_dim${DIM}_rows${FILL_ROWS}" "$DIM" "$FILL_ROWS"
    local rc=$?
    emit_results || rc=1
    return $rc
}

do_sweep_rows_mode() {
    local rc=0
    local fill_rows

    init_results
    echo "========================================================"
    echo " SWEEP rows: dim=$DIM memid=$SHM_MEMID size=$SHM_SIZE"
    echo " rows : $DEFAULT_ROW_SWEEP_LIST"
    echo " warmup=$WARMUP_RUNS repeat=$REPEAT_RUNS mock=$SHOW_MOCK"
    echo "========================================================"

    for fill_rows in $DEFAULT_ROW_SWEEP_LIST; do
        write_fixture "$DIM" "$fill_rows" || return 1
        echo
        run_read_case "rows_dim${DIM}_rows${fill_rows}" "$DIM" "$fill_rows" || rc=1
        echo
    done

    emit_results || rc=1
    return $rc
}

do_sweep_dim_mode() {
    local rc=0
    local dim

    init_results
    echo "========================================================"
    echo " SWEEP dim: rows=$FILL_ROWS memid=$SHM_MEMID size=$SHM_SIZE"
    echo " dims : $DEFAULT_DIM_SWEEP_LIST"
    echo " warmup=$WARMUP_RUNS repeat=$REPEAT_RUNS mock=$SHOW_MOCK"
    echo "========================================================"

    for dim in $DEFAULT_DIM_SWEEP_LIST; do
        write_fixture "$dim" "$FILL_ROWS" || return 1
        echo
        run_read_case "dim_rows${FILL_ROWS}_dim${dim}" "$dim" "$FILL_ROWS" || rc=1
        echo
    done

    emit_results || rc=1
    return $rc
}

ARGS=()
for arg in "$@"; do
    case "$arg" in
        --no-mock) SHOW_MOCK=no ;;
        --help|-h)
            usage
            exit 0
            ;;
        *) ARGS+=("$arg") ;;
    esac
done

set -- "${ARGS[@]}"

MODE=${1:-all}
SHM_MEMID=$DEFAULT_SHM_MEMID
SHM_SIZE=$DEFAULT_SHM_SIZE
DIM=$DEFAULT_DIM
FILL_ROWS=$DEFAULT_FILL_ROWS

require_non_negative_int "warmup" "$WARMUP_RUNS"
require_positive_int "repeat" "$REPEAT_RUNS"
require_ut

case "$MODE" in
    write|read|all)
        DIM=${2:-$DEFAULT_DIM}
        SHM_MEMID=${3:-$DEFAULT_SHM_MEMID}
        SHM_SIZE=${4:-$DEFAULT_SHM_SIZE}
        FILL_ROWS=${5:-$DEFAULT_FILL_ROWS}
        require_positive_int "dim" "$DIM"
        require_positive_int "fill_rows" "$FILL_ROWS"
        ;;
    sweep)
        SWEEP_MODE=${2:-rows}
        case "$SWEEP_MODE" in
            rows)
                DIM=${3:-$DEFAULT_DIM}
                SHM_MEMID=${4:-$DEFAULT_SHM_MEMID}
                SHM_SIZE=${5:-$DEFAULT_SHM_SIZE}
                require_positive_int "dim" "$DIM"
                ;;
            dim)
                FILL_ROWS=${3:-$DEFAULT_FILL_ROWS}
                SHM_MEMID=${4:-$DEFAULT_SHM_MEMID}
                SHM_SIZE=${5:-$DEFAULT_SHM_SIZE}
                require_positive_int "fill_rows" "$FILL_ROWS"
                ;;
            *)
                echo "ERROR: sweep mode must be 'rows' or 'dim', got: $SWEEP_MODE" >&2
                usage
                exit 1
                ;;
        esac
        ;;
    *)
        usage
        exit 1
        ;;
esac

case "$MODE" in
    write) do_write_mode ;;
    read)  do_read_mode ;;
    all)   do_all_mode ;;
    sweep)
        case "$SWEEP_MODE" in
            rows) do_sweep_rows_mode ;;
            dim)  do_sweep_dim_mode ;;
        esac
        ;;
esac
