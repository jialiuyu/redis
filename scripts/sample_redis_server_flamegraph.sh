#!/usr/bin/env bash
# Attach to a running redis-server process with perf and emit a flamegraph.
#
# Defaults are tuned for the 111/112 benchmark hosts:
#   DURATION=30 FREQ=99 bash scripts/sample_redis_server_flamegraph.sh
#   PID=$(cat /tmp/vemb_best.pid) DURATION=60 bash scripts/sample_redis_server_flamegraph.sh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/perf}"
DURATION="${DURATION:-30}"
FREQ="${FREQ:-99}"
EVENT="${EVENT:-cycles:u}"
COMM="${COMM:-redis-server}"
FLAMEGRAPH_DIR="${FLAMEGRAPH_DIR:-/root/FlameGraph}"
KEEP_SCRIPT="${KEEP_SCRIPT:-0}"

usage() {
    cat <<USAGE
Usage:
  [PID=<pid>] [DURATION=30] [FREQ=99] [EVENT=cycles:u] [OUT_DIR=perf] \\
    bash scripts/sample_redis_server_flamegraph.sh

Environment:
  PID              target redis-server pid; default: newest pgrep -x redis-server
  DURATION         sampling seconds; default: 30
  FREQ             perf sample frequency; default: 99
  EVENT            perf event; default: cycles:u
  OUT_DIR          output directory; default: <repo>/perf
  FLAMEGRAPH_DIR   FlameGraph checkout; default: /root/FlameGraph
  KEEP_SCRIPT=1    keep perf script text output; default removes it
USAGE
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

find_flamegraph_tool() {
    local tool="$1"
    if [ -x "$FLAMEGRAPH_DIR/$tool" ]; then
        echo "$FLAMEGRAPH_DIR/$tool"
        return 0
    fi
    if command -v "$tool" >/dev/null 2>&1; then
        command -v "$tool"
        return 0
    fi
    return 1
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    usage
    exit 0
fi

need_cmd perf
need_cmd pgrep
need_cmd ps
need_cmd date
need_cmd mkdir

STACKCOLLAPSE="$(find_flamegraph_tool stackcollapse-perf.pl)" ||
    die "cannot find stackcollapse-perf.pl; set FLAMEGRAPH_DIR=/path/to/FlameGraph"
FLAMEGRAPH="$(find_flamegraph_tool flamegraph.pl)" ||
    die "cannot find flamegraph.pl; set FLAMEGRAPH_DIR=/path/to/FlameGraph"

PID="${PID:-}"
if [ -z "$PID" ]; then
    PID="$(pgrep -n -x "$COMM" || true)"
fi
[ -n "$PID" ] || die "no running process found for COMM=$COMM; pass PID=<pid>"
[ -r "/proc/$PID/status" ] || die "cannot read /proc/$PID/status"
ps -p "$PID" -o comm= | grep -qx "$COMM" ||
    die "PID=$PID is not $COMM"

mkdir -p "$OUT_DIR"
TS="$(date +%Y%m%d_%H%M%S)"
BASE="$OUT_DIR/redis_server_${PID}_${TS}"
PERF_DATA="$BASE.perf.data"
SCRIPT_OUT="$BASE.perf.script"
COLLAPSED="$BASE.collapsed.txt"
SVG="$BASE.svg"
META="$BASE.meta.txt"

echo "Sampling $COMM pid=$PID duration=${DURATION}s freq=$FREQ event=$EVENT"
echo "Output base: $BASE"

{
    echo "timestamp=$TS"
    echo "pid=$PID"
    echo "comm=$COMM"
    echo "duration=$DURATION"
    echo "freq=$FREQ"
    echo "event=$EVENT"
    echo "perf=$(command -v perf)"
    echo "stackcollapse=$STACKCOLLAPSE"
    echo "flamegraph=$FLAMEGRAPH"
    echo
    ps -p "$PID" -o pid,ppid,comm,args
} >"$META"

perf record \
    -F "$FREQ" \
    -e "$EVENT" \
    -g \
    -p "$PID" \
    -o "$PERF_DATA" \
    -- sleep "$DURATION"

perf script -i "$PERF_DATA" >"$SCRIPT_OUT"
"$STACKCOLLAPSE" "$SCRIPT_OUT" >"$COLLAPSED"
"$FLAMEGRAPH" \
    --title "redis-server pid=$PID ${DURATION}s ${EVENT}" \
    "$COLLAPSED" >"$SVG"

if [ "$KEEP_SCRIPT" != "1" ]; then
    rm -f "$SCRIPT_OUT"
fi

echo
echo "Generated:"
echo "  perf data : $PERF_DATA"
echo "  collapsed : $COLLAPSED"
echo "  flamegraph: $SVG"
echo "  meta      : $META"
