#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT0="${VEMB_V16_SMOKE_PORT0:-6420}"
PORT1="${VEMB_V16_SMOKE_PORT1:-6421}"
DIM="${VEMB_V16_SMOKE_DIM:-8}"
PREFILL="${VEMB_V16_SMOKE_PREFILL:-64}"
OPS="${VEMB_V16_SMOKE_OPS:-32}"
TMPDIR="${TMPDIR:-/tmp}/vemb_v16_remote_vsim_smoke_$$"

mkdir -p "$TMPDIR"

MANIFEST0="$TMPDIR/sn0.yaml"
MANIFEST1="$TMPDIR/sn1.yaml"
LOG0="$TMPDIR/sn0.log"
LOG1="$TMPDIR/sn1.log"
BENCH_LOG="$TMPDIR/bench.log"
PID0=""
PID1=""

cleanup() {
    if [[ -n "$PID0" ]] && kill -0 "$PID0" 2>/dev/null; then
        kill "$PID0" 2>/dev/null || true
        wait "$PID0" 2>/dev/null || true
    fi
    if [[ -n "$PID1" ]] && kill -0 "$PID1" 2>/dev/null; then
        kill "$PID1" 2>/dev/null || true
        wait "$PID1" 2>/dev/null || true
    fi
}
trap cleanup EXIT

cat > "$MANIFEST0" <<EOF_MANIFEST
local_ub_node_id: 0
local_region_weight: 4
remote_meta_provider: shm
remote_meta_path: /v16_rvsim_rm0
remote_meta_entries: 256
remote_meta_buckets: 512
warm_regions:
  - region_id: 100
    provider: shm
    path: /v16_rvsim_payload0
    mmap_offset: 0
    bytes: 8192
    value_size: 32
    home_ub_node_id: 0
    weight: 1
  - region_id: 200
    provider: shm
    path: /v16_rvsim_payload1
    mmap_offset: 0
    bytes: 8192
    value_size: 32
    home_ub_node_id: 1
    weight: 1
remote_meta_views:
  - owner_id: 1
    provider: shm
    path: /v16_rvsim_rm1
    entries: 256
    buckets: 512
EOF_MANIFEST

cat > "$MANIFEST1" <<EOF_MANIFEST
local_ub_node_id: 1
local_region_weight: 4
remote_meta_provider: shm
remote_meta_path: /v16_rvsim_rm1
remote_meta_entries: 256
remote_meta_buckets: 512
warm_regions:
  - region_id: 100
    provider: shm
    path: /v16_rvsim_payload0
    mmap_offset: 0
    bytes: 8192
    value_size: 32
    home_ub_node_id: 0
    weight: 1
  - region_id: 200
    provider: shm
    path: /v16_rvsim_payload1
    mmap_offset: 0
    bytes: 8192
    value_size: 32
    home_ub_node_id: 1
    weight: 1
remote_meta_views:
  - owner_id: 0
    provider: shm
    path: /v16_rvsim_rm0
    entries: 256
    buckets: 512
EOF_MANIFEST

make -C "$ROOT/src" vemb_v16_server
make -C "$ROOT/benchmark" vemb_v16_bench

"$ROOT/src/vemb_v16_server" \
    --transport tcp \
    --tcp-host 127.0.0.1 \
    --tcp-port "$PORT0" \
    --proxy-io-threads 1 \
    --supernode-workers 1 \
    --warm-regions-manifest "$MANIFEST0" \
    --reset-warm-regions \
    --dim "$DIM" \
    --max-vectors 256 \
    --loglevel debug > "$LOG0" 2>&1 &
PID0="$!"

sleep 0.3

"$ROOT/src/vemb_v16_server" \
    --transport tcp \
    --tcp-host 127.0.0.1 \
    --tcp-port "$PORT1" \
    --proxy-io-threads 1 \
    --supernode-workers 1 \
    --warm-regions-manifest "$MANIFEST1" \
    --dim "$DIM" \
    --max-vectors 256 \
    --loglevel debug > "$LOG1" 2>&1 &
PID1="$!"

sleep 0.5

"$ROOT/benchmark/vemb_v16_bench" \
    --transport tcp \
    --endpoints "127.0.0.1:${PORT0},127.0.0.1:${PORT1}" \
    --mode vsim-key-key \
    --vsim-key2-owner remote \
    --dim "$DIM" \
    --prefill "$PREFILL" \
    --ops "$OPS" \
    --threads 1 \
    --pipeline 1 \
    --timeout-ms 5000 > "$BENCH_LOG" 2>&1

if ! grep -q "key2_source=2" "$LOG0" "$LOG1"; then
    echo "remote VSIM smoke failed: no REMOTE key2 source in server logs" >&2
    echo "logs kept at: $TMPDIR" >&2
    exit 1
fi

echo "remote VSIM smoke passed"
echo "logs: $TMPDIR"
