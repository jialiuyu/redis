#!/bin/bash
# sve_streaming_load_f32_region_ut.sh
# Four-way UT for sve_streaming_load_f32():
#   USE_SVE on/off x local/remote UB path
#
# This script builds the benchmark twice:
#   - SVE build in the current tree
#   - memcpy fallback build in a temporary worktree with SVE auto-detect disabled
#
# It then runs the benchmark against:
#   - examples/vemb_v16_warm_regions_111.yaml
#   - examples/vemb_v16_warm_regions_111_remote.yaml

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HPC="$(cd "$SCRIPT_DIR/.." && pwd)"
[ -f "$HPC/src/server.c" ] || {
    echo "ERROR: 找不到 $HPC/src/server.c，请在 hpc-redis 项目根或 benchmark/ 下运行" >&2
    exit 2
}

LOCAL_MANIFEST="$HPC/examples/vemb_v16_warm_regions_111.yaml"
REMOTE_MANIFEST="$HPC/examples/vemb_v16_warm_regions_111_remote.yaml"
WARMUP=${WARMUP:-20000}
ITERS=${ITERS:-500000}
WORKDIR=${WORKDIR:-/tmp/sve_streaming_load_f32_region_ut}
NOSVE_TREE="$WORKDIR/nosve-tree"
SVE_BIN="$HPC/benchmark/sve_streaming_load_f32_ut"
NOSVE_BIN="$NOSVE_TREE/benchmark/sve_streaming_load_f32_ut"
TABLE_FILE="${TABLE_FILE:-/tmp/sve_streaming_load_f32_region_ut.tsv}"

mkdir -p "$WORKDIR"

cleanup() {
    pkill -9 -f "sve_streaming_load_f32_ut" 2>/dev/null || true
    git -C "$HPC" worktree remove "$NOSVE_TREE" --force 2>/dev/null || true
    git -C "$HPC" worktree prune 2>/dev/null || true
}
trap cleanup EXIT

build_sve() {
    echo "[build] SVE build"
    make -C "$HPC/benchmark" -j8 USE_SVE=yes sve_streaming_load_f32_ut >/tmp/sve_streaming_load_f32_ut_sve.build.log 2>&1
}

build_nosve() {
    echo "[build] memcpy build"
    rm -rf "$NOSVE_TREE"
    git -C "$HPC" worktree add --detach "$NOSVE_TREE" HEAD >/tmp/sve_streaming_load_f32_ut_nosve.worktree.log 2>&1
    # The UT and its Makefile target are intentionally uncommitted while this
    # experiment is being prepared, so copy the current versions into the
    # detached worktree before building the fallback variant.
    cp "$HPC/benchmark/sve_streaming_load_f32_ut.c" "$NOSVE_TREE/benchmark/sve_streaming_load_f32_ut.c"
    cp "$HPC/benchmark/Makefile" "$NOSVE_TREE/benchmark/Makefile"
    make -C "$NOSVE_TREE/benchmark" -j8 USE_SVE=no sve_streaming_load_f32_ut >/tmp/sve_streaming_load_f32_ut_nosve.build.log 2>&1
}

run_case() {
    local bin=$1
    local manifest=$2
    local region_flag=$3
    local mode=$4
    local log="$WORKDIR/${mode}_${region_flag}.log"
    "$bin" --manifest "$manifest" "$region_flag" --warmup "$WARMUP" --iters "$ITERS" >"$log" 2>&1
    grep '^mode=' "$log" | tail -1
}

build_sve
build_nosve

printf "mode\tregion\tpath\tavg_ns\tMB_per_s\tchecksum\n" >"$TABLE_FILE"

for mode in sve nosve; do
    bin="$SVE_BIN"
    [ "$mode" = "nosve" ] && bin="$NOSVE_BIN"

    line=$(run_case "$bin" "$LOCAL_MANIFEST" --local "$mode")
    printf "%s\n" "$line" | awk '
        {
            for (i = 1; i <= NF; i++) {
                split($i, kv, "=");
                a[kv[1]] = kv[2];
            }
            printf "%s\t%s\t%s\t%s\t%s\t%s\n", a["mode"], a["region"], a["path"], a["avg_ns"], a["MB/s"], a["checksum"];
        }
    ' >>"$TABLE_FILE"

    line=$(run_case "$bin" "$REMOTE_MANIFEST" --remote "$mode")
    printf "%s\n" "$line" | awk '
        {
            for (i = 1; i <= NF; i++) {
                split($i, kv, "=");
                a[kv[1]] = kv[2];
            }
            printf "%s\t%s\t%s\t%s\t%s\t%s\n", a["mode"], a["region"], a["path"], a["avg_ns"], a["MB/s"], a["checksum"];
        }
    ' >>"$TABLE_FILE"
done

awk -F'\t' 'NR == 1 { next } { printf "%-8s %-8s %-24s %-12s %-12s %-12s\n", $1, $2, $3, $4, $5, $6 }' "$TABLE_FILE"

echo ""
echo "table_tsv=$TABLE_FILE"
echo "workdir=$WORKDIR"
