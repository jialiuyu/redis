#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT="${1:-$ROOT_DIR/compile_commands.json}"
TMP_REL=".compile_commands.vemb.tmp.json"
TMP_OUTPUT="$ROOT_DIR/$TMP_REL"
MAKE_JOBS="${MAKE_JOBS:-4}"

if ! command -v bear >/dev/null 2>&1; then
    echo "error: bear not found in PATH" >&2
    exit 1
fi

cleanup() {
    rm -f "$TMP_OUTPUT"
}
trap cleanup EXIT

cd "$ROOT_DIR"

echo "Generating compile_commands for native make -j ${MAKE_JOBS}..."
bear --output "$TMP_REL" -- make -j "${MAKE_JOBS}"

echo "Generating compile_commands for src/vemb_v16_server..."
bear --append --output "$TMP_REL" -- make -C src vemb_v16_server

echo "Appending compile_commands for benchmark/vemb_v16_bench..."
bear --append --output "$TMP_REL" -- make -C benchmark  vemb_v16_bench

python3 "$ROOT_DIR/scripts/fix_compile_commands.py" "$TMP_OUTPUT"
mv "$TMP_OUTPUT" "$OUTPUT"

echo "Wrote $OUTPUT"
