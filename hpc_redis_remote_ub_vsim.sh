#!/bin/bash
# Remote-UB VSIM (vector similarity search) sweep
# 用法: bash hpc_redis_remote_ub_vsim.sh
set -uo pipefail
HPC=/root/gqs/codespace/UnifiedBus/hpc-redis
export MANIFEST=$HPC/examples/vemb_v16_warm_regions_111_remote.yaml
export OUTDIR=${OUTDIR:-/tmp/hpc_max_tput_remote_ub_vsim}
export OP_MODE=vsim
export PORT=${PORT:-6392}
exec bash "$HPC/hpc_redis_max_tput.sh" "$@"
