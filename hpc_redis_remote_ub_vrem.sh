#!/bin/bash
# Remote-UB VREM (vector delete) sweep
# 用法: bash hpc_redis_remote_ub_vrem.sh
set -uo pipefail
HPC=/root/gqs/codespace/UnifiedBus/hpc-redis
export MANIFEST=$HPC/examples/vemb_v16_warm_regions_111_remote.yaml
export OUTDIR=${OUTDIR:-/tmp/hpc_max_tput_remote_ub_vrem}
export OP_MODE=vrem
export PORT=${PORT:-6393}
exec bash "$HPC/hpc_redis_max_tput.sh" "$@"
