# VEMB v16 Host MT Core Budget Test - 2026-07-16

## Goal

Reduce server CPU usage below 26 cores while keeping throughput near the
24:24 host-mt reference result.

Target workload:

```bash
NUM_KEYS=100000 WORKERS='<pio>:<snw>' TS='64' CS='4' TEST_TIME=30 bash hpc_redis_max_tput.sh
```

The test uses `memtier` with `t64 c4`, pipeline 32, VEMB v16 read mode
(`OP_MODE=vemb` default), and 100k keys.

## Environment

- Host: `root@192.168.90.111`
- Repo: `/root/szz/codespace/hpc-redis`
- Build command:

```bash
make -C src redis-server USE_UB=yes
```

The build passed on the remote host. Warnings were present, including existing
`redis_strlcpy` implicit declaration warnings in `vemb_v16_storage.c`, but the
remote build completed successfully.

## Reference Run

After adding the 64 KiB TCP response write budget, the documented 24:24 run was:

```text
pio=24 snw=24 t64 c4
ops=11765293.19
hits=11765293.19
p50=0.70300 ms
cores=27.32
mem=332/430 MB
```

Observation: the 64 KiB write budget did not materially reduce CPU usage by
itself. It is more useful as a fairness guard than as a throughput or CPU
optimization for this workload. A later repeat after removing the write budget
confirmed that there was no material regression or gain from the limit.

## Worker Sweep

First sweep:

```bash
NUM_KEYS=100000 WORKERS='20:24 22:24 24:20 22:22 18:24 24:18' TS='64' CS='4' TEST_TIME=30 OUTDIR=/tmp/hpc_max_tput_core_budget bash hpc_redis_max_tput.sh
```

| pio | snw | ops/sec | p50 ms | cores | Result |
| --- | --- | ---: | ---: | ---: | --- |
| 20 | 24 | 10017276.09 | 0.743 | 27.22 | Throughput too low |
| 22 | 24 | 10537368.59 | 0.743 | 29.34 | Throughput too low, CPU higher |
| 24 | 20 | 11032439.99 | 0.735 | 31.54 | Throughput too low, CPU higher |
| 22 | 22 | 11782151.83 | 0.711 | 26.12 | Close, but slightly above 26 cores |
| 18 | 24 | 9539783.19 | 0.775 | 24.96 | CPU OK, throughput too low |
| 24 | 18 | 11133970.02 | 0.719 | 31.14 | Throughput too low, CPU higher |

Second fine-grained sweep around 22:22:

```bash
NUM_KEYS=100000 WORKERS='21:22 22:21 21:21 23:22 22:23 23:21' TS='64' CS='4' TEST_TIME=30 OUTDIR=/tmp/hpc_max_tput_core_budget_fine bash hpc_redis_max_tput.sh
```

| pio | snw | ops/sec | p50 ms | p99 ms | cores | Result |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| 21 | 22 | 10481865.60 | 0.759 | 1.367 | 27.87 | Throughput too low |
| 22 | 21 | 10558116.86 | 0.735 | 1.335 | 29.59 | Throughput too low |
| 21 | 21 | 11716304.54 | 0.711 | 0.999 | 25.66 | Meets goal |
| 23 | 22 | 10915243.31 | 0.743 | 1.231 | 30.77 | Throughput too low |
| 22 | 23 | 10926863.05 | 0.751 | 1.215 | 28.88 | Throughput too low |
| 23 | 21 | 10770613.02 | 0.759 | 1.231 | 30.13 | Throughput too low |

## 21:21 Repeat Verification

Repeat command:

```bash
NUM_KEYS=100000 WORKERS='21:21 21:21' TS='64' CS='4' TEST_TIME=30 OUTDIR=/tmp/hpc_max_tput_core_budget_2121_repeat bash hpc_redis_max_tput.sh
```

| Run | ops/sec | p50 ms | p99 ms | cores | run_ops | mem base/peak MB |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 11761244.92 | 0.711 | 0.999 | 25.66 | 11010964 | 305/401 |
| 2 | 11677087.03 | 0.719 | 0.999 | 25.57 | 11065505 | 305/401 |

## 21:21 Repeat Without 64 KiB Write Limit

The `tcp_make_limited_write_iov` helper and the 64 KiB TCP response write limit
were removed, restoring direct `writev` on the original response iov.

Repeat command:

```bash
NUM_KEYS=100000 WORKERS='21:21 21:21' TS='64' CS='4' TEST_TIME=30 OUTDIR=/tmp/hpc_max_tput_no_write_limit_2121_repeat bash hpc_redis_max_tput.sh
```

| Run | ops/sec | p50 ms | p99 ms | cores | run_ops | mem base/peak MB |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 11697847.63 | 0.719 | 1.007 | 25.60 | 10783899 | 308/404 |
| 2 | 11771210.31 | 0.711 | 1.007 | 25.79 | 10981578 | 307/403 |

Observation: removing the helper had no material impact on the 21:21 target
point. Throughput and CPU stayed in the same range as the earlier repeat.

## Lower Equal-Pair Sweep

Additional sweep for lower symmetric worker counts:

```bash
NUM_KEYS=100000 WORKERS='16:16 18:18 20:20' TS='64' CS='4' TEST_TIME=30 OUTDIR=/tmp/hpc_max_tput_core_budget_low_pairs bash hpc_redis_max_tput.sh
```

| pio | snw | ops/sec | p50 ms | p99 ms | cores | run_ops | mem base/peak MB | Result |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| 16 | 16 | 9846696.89 | 0.855 | 1.455 | 26.22 | 9314107 | 263/359 | Throughput too low, CPU still above 26 |
| 18 | 18 | 10750788.83 | 0.767 | 1.335 | 27.85 | 10228447 | 280/371 | Throughput too low, CPU higher |
| 20 | 20 | 11482776.11 | 0.719 | 1.127 | 25.65 | 10715930 | 298/388 | CPU OK, but below 21:21 throughput |

Observation: `20:20` meets the CPU target, but throughput is about
`1.7-2.4%` below the repeated `21:21` runs. `16:16` and `18:18` do not meet the
combined CPU and throughput target.

## 2026-07-22 TCP Request Bypass Follow-Up

Change summary:

- Split the old unified `VEMB_V16_PROXY_BATCH` into independently tunable
  `PROXY_REQUEST_BATCH`, `PROXY_RESPONSE_BATCH`, and `PROXY_QUEUE_BATCH`.
- Exposed those values through `make`, for example:
  `make -C src vemb_v16_server PROXY_REQUEST_BATCH=16 PROXY_RESPONSE_BATCH=24 PROXY_QUEUE_BATCH=8`.
- Removed the TCP-only `vemb_v16_proxy_handle_request_batch()` wrapper.
  TCP now builds a request pointer batch and calls
  `vemb_v16_proxy_handle_request_ptr_batch()` directly, matching the Aeron
  request scheduling entry point.

Validation:

```bash
make -C src redis-server USE_UB=yes
NUM_KEYS=100000 WORKERS='21:21' TS='64' CS='4' TEST_TIME=30 bash hpc_redis_max_tput.sh
```

Result:

| Build / path | ops/sec | p50 ms | p99 ms | cores | ops/core/sec | Artifact |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Pre-bypass TCP inline reference, 2026-07-22 earlier run | 11585807.09 | 0.711 | NA | 20.92 | ~553814 | recorded in `docs/VEMB_V16_AERON_BEST_FLAMEGRAPH_20260722.md` |
| TCP ptr-batch bypass, 2026-07-22 | 11771274.05 | 0.703 | 0.863 | 21.10 | ~557880 | `perf/server_flamegraph_host_mt_read_21_21_t64_c4_20260722_153218.svg` |

The post-change flamegraph was captured with the same `21:21 t64 c4` host-mt
shape while running a 30s memtier read workload. The remote source artifact was:

```text
/tmp/host_mt_read_21_21_t64_c4_20260722_153218/server_flamegraph_host_mt_read_21_21_t64_c4_20260722_153218.svg
```

Local copy:

```text
perf/server_flamegraph_host_mt_read_21_21_t64_c4_20260722_153218.svg
```

Interpretation:

- Throughput improved by about `1.6%` versus the earlier same-day TCP inline
  reference (`11.586M -> 11.771M ops/sec`).
- CPU usage stayed essentially flat (`20.92 -> 21.10` cores), so per-core
  throughput improved slightly (`~553.8k -> ~557.9k ops/core/sec`).
- Latency did not regress; p50 moved from `0.711ms` to `0.703ms`, and the
  post-change p99 was `0.863ms`.
- The change is primarily a cleanup and dispatch-path simplification for TCP:
  it removes one request batch wrapping layer and makes TCP/Aeron feed the same
  pointer-batch scheduler. The measured gain is modest but positive on the
  target host-mt workload.

## Conclusion

`WORKERS='21:21'` meets the target:

- CPU drops from about `27.32` cores to `25.57-25.66` cores.
- Throughput remains in the `11.67M-11.76M ops/sec` range.
- Latency remains close to the reference run (`p50` around `0.711-0.719 ms`).
- Lower symmetric configs do not improve the result: `20:20` is CPU-acceptable
  but loses throughput, while `16:16` and `18:18` miss the combined target.

Recommended command:

```bash
NUM_KEYS=100000 WORKERS='21:21' TS='64' CS='4' TEST_TIME=30 bash hpc_redis_max_tput.sh
```

This command is also recorded in `scripts/test_host_mt.md`.
