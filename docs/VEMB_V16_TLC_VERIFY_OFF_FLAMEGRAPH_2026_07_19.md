# VEMB V16 TLC Verify-Off Flamegraph Notes 2026-07-19

## Scope

This note records the effect of disabling TLC warm-location verification in the
host multi-thread read benchmark from `scripts/test_host_mt.md`.

Test command on the remote host:

```bash
make -C src clean
make -C src redis-server USE_UB=yes TLC_VERIFY=no
NUM_KEYS=100000 WORKERS='21:21' TS='64' CS='4' TEST_TIME=30 bash hpc_redis_max_tput.sh
```

The build was verified to contain:

```text
-DTLC_CORE_ENABLE_WARM_LOCATION_VERIFY=0
```

## Code switch

Verification was disabled through:

- `src/tlc_core.h`
- `src/tlc_core.c`
- `src/build_opts/tlc.mk`

Affected read-path entry points:

- `tlc_core_copy_warm_location_value()`
- `tlc_core_validate_warm_location()`

## Throughput result

Clean rebuild, verify-off run:

- `ops/sec`: `11,784,043.54`
- `p50`: `0.719 ms`

Earlier script summary run with the same worker layout:

- `ops/sec`: `11,545,778.84`
- `cpu_cores`: `24.80`
- `p50`: `0.727 ms`

Conclusion:

- Turning verification off does remove the warm-slot seqlock validation work.
- End-to-end throughput improvement is small.
- The main bottleneck shifts to payload copy and network/request plumbing.

## Batch matrix after verify-off

All runs below use the host-mt read benchmark from `scripts/test_host_mt.md`
with:

```bash
NUM_KEYS=100000 WORKERS='21:21' TS='64' CS='4' TEST_TIME=30
```

Summary table:

| Request batch | Response batch | ops/sec | p50 | p99 | cpu_cores | Flamegraph |
|---|---:|---:|---:|---:|---:|---|
| 32 | 32 | 11,545,778.84 | 0.727 ms | - | 24.80 | `artifacts/flamegraphs/redis-server-tlc-verify-off-clean-20260719.svg` |
| 1 | 32 | 1,800,090.90 | 0.127 ms | 0.263 ms | 13.69 | `artifacts/flamegraphs/redis-server-no-batch-20260719.svg` |
| 32 | 1 | 2,801,513.28 | 2.863 ms | 3.647 ms | 14.17 | `artifacts/flamegraphs/redis-server-req32-resp1-20260719.svg` |
| 1 | 1 | 1,885,800.23 | 0.119 ms | 0.255 ms | 13.93 | `artifacts/flamegraphs/redis-server-req1-resp1-20260719.svg` |
| 64 | 64 | 11,697,341.94 | 1.431 ms | 1.727 ms | 30.90 | - |

## Proxy-side location cache experiment

### Implementation summary

Attempted a minimal-change experiment that moves the warm-location fast path
toward proxy:

- add a worker-local proxy cache keyed by `key_hash + key_len + fingerprint`
- proxy `VEMB_INLINE` requests try local cache first
- on hit, proxy directly runs TLC handle lookup/copy and returns inline payload
- on miss, proxy still falls back to the existing TLC lookup path
- remove the supernode-side `get_handle_hint()` fast path block for this test

Files touched for the experiment:

- `src/vemb_v16_proxy.c`
- `src/vemb_v16_proxy_internal.h`
- `src/vemb_v16_proxy_types.h`
- `src/vemb_v16_supernode.c`

### Batch setting used for this experiment

The hotspot test below was rerun after restoring:

- request batch: `32`
- response batch: `32`
- client pipeline: `32`

### Uniform-key result with proxy-side location cache

Measured result with `NUM_KEYS=100000`:

- `ops/sec`: `2,723,931.51`
- `p50`: `5.983 ms`
- `p99`: `7.007 ms`
- `cpu_cores`: `10.81`

Observed interpretation:

- this is much worse than the earlier `32 + 32` baseline
- the main loss is not cache hit rate alone
- the proxy fast path moved lookup/copy work onto proxy IO threads and did not
  preserve the original high-throughput batch response behavior

### Hot-key results with proxy-side location cache

All runs below keep:

```bash
WORKERS='21:21' TS='64' CS='4' TEST_TIME=30
```

| NUM_KEYS | ops/sec | p50 | p99 | cpu_cores |
|---:|---:|---:|---:|---:|
| 1 | 2,680,959.21 | 3.039 ms | 3.615 ms | 0.00 |
| 16 | 2,644,813.52 | 3.087 ms | 3.519 ms | 0.00 |
| 128 | 2,657,451.28 | 3.039 ms | 3.631 ms | 0.00 |
| 100000 | 2,723,931.51 | 5.983 ms | 7.007 ms | 10.81 |

Notes:

- `cpu_cores` for `NUM_KEYS=1/16/128` is not reliable in this round because the
  script-side `/proc/<pid>/clear_refs` sampling failed after server PID changes
- throughput and latency numbers are still usable

Observed interpretation:

- hot-key traffic did not unlock a meaningful gain for this implementation
- `NUM_KEYS=1/16/128` all stayed around `2.64M-2.68M ops/sec`
- that suggests the current bottleneck is the proxy-side execution/response path
  itself, not location-cache hit rate
- in other words, simply moving location cache toward proxy is not enough if the
  response side no longer benefits from the original batch-friendly completion
  path

## Batch 1/1/1 thread-count sweep

After the proxy-side cache experiment was reverted, the server was restored to:

- request batch: `1`
- response batch: `1`
- client pipeline: `1`

Test command:

```bash
NUM_KEYS=100000 WORKERS='2:2 4:4 8:8 12:12 16:16 21:21' TS='64' CS='4' TEST_TIME=30 bash hpc_redis_max_tput.sh
```

Measured result:

| proxy_io | supernode | ops/sec | p50 | p99 | cpu_cores |
|---:|---:|---:|---:|---:|---:|
| 2 | 2 | 2,039,948.93 | 0.111 ms | 0.239 ms | 0.00 |
| 4 | 4 | 2,063,611.64 | 0.111 ms | 0.239 ms | 0.00 |
| 8 | 8 | 2,033,094.00 | 0.111 ms | 0.239 ms | 0.00 |
| 12 | 12 | 2,067,891.12 | 0.111 ms | 0.231 ms | 0.00 |
| 16 | 16 | 2,054,204.06 | 0.111 ms | 0.239 ms | 0.00 |
| 21 | 21 | 2,057,481.38 | 0.111 ms | 0.239 ms | 0.00 |

Notes:

- the best QPS in this run was `12:12`, but the spread was very small
- `4:4`, `12:12`, `16:16`, and `21:21` are all close to each other
- `cpu_cores` is not usable in this round because the script-side `/proc/<pid>/clear_refs`
  sampling failed and the column stayed at `0.00`

Observed interpretation:

- under `batch=1/1/1`, `21:21` does not appear meaningfully better than lower
  thread counts
- this suggests the higher thread count is at least close to overprovisioned for
  the no-batch path
- if we want a cleaner CPU-based conclusion, the next step should be to rerun
  this sweep after fixing the script-side CPU sampling

### 1. Request 32 + Response 32

Measured result from the verify-off script run:

- `ops/sec`: `11,545,778.84`
- `p50`: `0.727 ms`
- `cpu_cores`: `24.80`

Reference clean-rebuild rerun:

- `ops/sec`: `11,784,043.54`
- `p50`: `0.719 ms`

Flamegraph artifact:

- `artifacts/flamegraphs/redis-server-tlc-verify-off-clean-20260719.svg`

Top functions:

- `sve_streaming_load_f32`: `10.45%`
- `channel_read_tcp_request_from_input` libc hot path: `6.39%`
- `publish_request_job`: `4.33%`
- kernel `dst_release`: `4.27%`

### 2. Request 1 + Response 32

Follow-up experiment:

- request side FC batch limit: `1`
- client pipeline: `1`
- response batch: still `32`

Code points:

- `src/proxy_aggregator.c`: `PROXY_FC_DEFAULT_BATCH_LIMIT 1`
- `hpc_redis_max_tput.sh`: `PIPELINE=1`
- `src/vemb_v16_proxy_internal.h`: `VEMB_V16_PROXY_BATCH 32u`

Measured result:

- `ops/sec`: `1,800,090.90`
- `p50`: `0.127 ms`
- `p99`: `0.263 ms`
- `cpu_cores`: `13.69`

Profile rerun with the same no-batch request setup:

- `ops/sec`: `1,808,613.67`
- `p50`: `0.127 ms`
- `p99`: `0.295 ms`

Flamegraph artifact:

- `artifacts/flamegraphs/redis-server-no-batch-20260719.svg`

Top functions for the no-batch request run:

- kernel `dst_release`: `8.38%`
- kernel `__dev_queue_xmit`: `7.95%`

Observed interpretation:

- Once request batching is removed, the dominant cost shifts away from TLC copy
  and toward TCP small-packet send/receive overhead.
- Throughput drops sharply, while latency and CPU usage both fall.
- The current "no-batch" measurement already reflects `response batch = 32`.

### 3. Request 32 + Response 1

Code points:

- `src/proxy_aggregator.c`: `PROXY_FC_DEFAULT_BATCH_LIMIT 32`
- `src/vemb_v16_proxy_internal.h`: `VEMB_V16_PROXY_RESPONSE_BATCH 1u`
- `src/vemb_v16_tcp_transport.c`
- `src/vemb_v16_proxy.c`
- `hpc_redis_max_tput.sh`: `PIPELINE=32`

Measured result:

- `ops/sec`: `2,801,513.28`
- `p50`: `2.863 ms`
- `p99`: `3.647 ms`
- `cpu_cores`: `14.17`

Profile rerun with the same setup:

- `ops/sec`: `2,804,053.59`
- `p50`: `2.879 ms`
- `p99`: `5.375 ms`

Flamegraph artifact:

- `artifacts/flamegraphs/redis-server-req32-resp1-20260719.svg`

Top functions:

- kernel `dst_release`: `10.14%`
- kernel `tcp_add_backlog`: `4.00%`

Observed interpretation:

- Keeping request batching while forcing response batching to `1` hurts
  throughput much more than the `response 32` case.
- The cost center is now clearly in TCP packet lifecycle and backlog handling,
  not in TLC copy/validation.

### 4. Request 1 + Response 1

Code points:

- `src/proxy_aggregator.c`: `PROXY_FC_DEFAULT_BATCH_LIMIT 1`
- `src/vemb_v16_proxy_internal.h`: `VEMB_V16_PROXY_RESPONSE_BATCH 1u`
- `src/vemb_v16_tcp_transport.c`
- `src/vemb_v16_proxy.c`
- `hpc_redis_max_tput.sh`: `PIPELINE=1`

Measured result:

- `ops/sec`: `1,885,800.23`
- `p50`: `0.119 ms`
- `p99`: `0.255 ms`
- `cpu_cores`: `13.93`

Profile rerun with the same setup:

- `ops/sec`: `1,902,272.10`
- `p50`: `0.119 ms`
- `p99`: `0.271 ms`

Flamegraph artifact:

- `artifacts/flamegraphs/redis-server-req1-resp1-20260719.svg`

Top functions:

- kernel `dst_release`: `6.32%`
- kernel `tcp_rcv_established`: `6.32%`

Observed interpretation:

- After both request and response sides are forced to `1`, throughput falls
  further, but latency remains very low and CPU stays around `14` cores.
- The hottest path remains kernel TCP/network bookkeeping rather than TLC data
  validation or copy logic.

### 5. Request 64 + Response 64

Code points:

- `src/proxy_aggregator.c`: `PROXY_FC_DEFAULT_BATCH_LIMIT 64`
- `src/vemb_v16_proxy_internal.h`: `VEMB_V16_PROXY_RESPONSE_BATCH 64u`
- `hpc_redis_max_tput.sh`: `PIPELINE=64`

Measured result:

- `ops/sec`: `11,697,341.94`
- `p50`: `1.431 ms`
- `p99`: `1.727 ms`
- `cpu_cores`: `30.90`

Observed interpretation:

- Compared with `32 + 32`, pushing both request and response batch to `64`
  does not improve throughput materially in this setup.
- `p50` and CPU both rise, which suggests the larger batch mostly trades more
  waiting/aggregation for similar end-to-end throughput.

## Flamegraph artifacts

Local SVG:

- `artifacts/flamegraphs/redis-server-tlc-verify-off-clean-20260719.svg`
- `artifacts/flamegraphs/redis-server-no-batch-20260719.svg`
- `artifacts/flamegraphs/redis-server-req32-resp1-20260719.svg`
- `artifacts/flamegraphs/redis-server-req1-resp1-20260719.svg`

Earlier rerun SVGs kept for comparison:

- `artifacts/flamegraphs/redis-server-tlc-verify-off-20260719.svg`
- `artifacts/flamegraphs/redis-server-tlc-verify-off-rerun-20260719.svg`

## Top functions after verify-off clean rebuild

From `perf report --no-children`:

- `sve_streaming_load_f32`: `10.45%`
- `channel_read_tcp_request_from_input` libc hot path: `6.39%`
- `publish_request_job`: `4.33%`
- kernel `dst_release`: `4.27%`

Observed interpretation:

- `tlc_core_copy_warm_location_value()` no longer shows the old validation-heavy
  self cost from the verify-on binary.
- After verification is removed, the dominant TLC-side cost becomes
  `sve_streaming_load_f32()`.
- The remaining end-to-end ceiling is mostly copy cost plus proxy/network path.
