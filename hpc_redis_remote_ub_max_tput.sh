#!/bin/bash
# Remote-UB VEMB (vector read) sweep — 聚焦甜点配置
# 用法: bash hpc_redis_remote_ub_max_tput.sh
set -uo pipefail
HPC=/root/gqs/codespace/UnifiedBus/hpc-redis
export MANIFEST=$HPC/examples/vemb_v16_warm_regions_111_remote.yaml
export OUTDIR=${OUTDIR:-/tmp/hpc_max_tput_remote_ub}
export OP_MODE=${OP_MODE:-vemb}
export PORT=${PORT:-6390}

# 绑核 (memory: benchmark_cpu_pinning.md — server=node0, client=node1)
export SERVER_CPUSET=${SERVER_CPUSET:-0-95}
export CLIENT_CPUSET=${CLIENT_CPUSET:-96-191}

# 已知本地甜点 pio=32 snw=64 (memory: hpc_redis_pio_snw_scaling.md)
# remote UB 是新场景，只测甜点 + 聚焦 t/c 找最优并发
export WORKERS=${WORKERS:-32:64}
# t/c 组合 (t×c≤64 避开 VEMB 连接 hang)
export TS=${TS:-'1 1 8 16 32 64'}
export CS=${CS:-'1 32 8 4 2 1'}

exec bash "$HPC/hpc_redis_max_tput.sh" "$@"
