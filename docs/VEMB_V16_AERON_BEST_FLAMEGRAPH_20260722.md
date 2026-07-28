# VEMB V16 Aeron Best Flamegraph Optimization Notes

## Context

This note records the `run_aeron_best.sh` flamegraph captured on 2026-07-22.
The run used the Aeron/SHM transport best-known local configuration:

```text
pio=21
snw=21
client t=64 c=4 pipeline=32
test_time=60s
```

Local artifacts:

```text
artifacts/aeron_best_flame_20260722_110432/redis-server-flamegraph.svg
artifacts/aeron_best_flame_20260722_110432/perf.folded
artifacts/aeron_best_flame_20260722_110432/summary.txt
```

Run summary:

```text
ops/sec         : 52792778.29
hits/sec        : 52792778.29
misses/sec      : 0.00
p50 / p99       : 0.15100 / 0.20700 ms
server CPU cores: 38.36
handle_deref    : ok=3167349983 fail=0
```

This is a clean read-hit run. The flamegraph is therefore useful for optimizing
the steady-state VEMB handle read path, not miss handling or migration behavior.

## Optimization Results Summary

### Performance Table

| Stage | Scope | Run | ops/sec | CPU cores | ops/core/sec | p50 / p99 ms | Artifact / output | Status |
| --- | --- | --- | ---: | ---: | ---: | --- | --- | --- |
| Baseline | Aeron VEMB_HANDLE | Flamegraph | 52,792,778 | 38.36 | ~1,376,246 | 0.151 / 0.207 | `artifacts/aeron_best_flame_20260722_110432` | Baseline |
| P0 stable read | TLC read path | Flamegraph | 50,878,859 | 36.34 | 1,400,079 | 0.151 / 3.999 | `artifacts/aeron_best_flame_p0_20260722_113104` | Kept, noisy flamegraph run |
| P0 stable read | TLC read path | Manual best run | 53,326,026 | 37.16 | 1,435,038 | 0.151 / 0.199 | User terminal run | Kept |
| P1 stack reqs | Aeron request poll | Flamegraph | 55,938,449 | 38.08 | 1,468,972 | 0.143 / 0.199 | `artifacts/aeron_best_flame_p0_stack_20260722_114055` | Superseded by P1 peek |
| P1 stack reqs | Aeron request poll | Repeat flamegraph | 55,400,112 | 37.90 | 1,461,744 | 0.135 / 0.215 | `artifacts/aeron_best_flame_p0_stack_20260722_114420` | Superseded by P1 peek |
| P1 peek | Aeron request ingress | Normal run | 61,004,440 | 36.25 | 1,682,881 | 0.119 / 0.191 | Remote `run_aeron_best.sh` | Kept |
| P1 peek | Aeron request ingress | Flamegraph | 61,352,162 | 36.77 | 1,668,539 | 0.119 / 0.191 | `artifacts/aeron_best_flame_p1_peek_20260722_115419` | Kept |
| P1 handle-only scheduler | Aeron request scheduler | Run 1 | 49,973,027 | 32.01 | 1,561,169 | 0.103 / 8.031 | Remote `run_aeron_best.sh` | Not retained, host later found polluted |
| P1 handle-only scheduler | Aeron request scheduler | Run 2 | 45,094,183 | 30.33 | 1,486,785 | 0.103 / 12.031 | Remote `run_aeron_best.sh` | Not retained |
| TCP key-only decode | TCP VEMB_INLINE default | hpc max-tput | 11,585,807 | 20.92 | ~553,814 | 0.711 / NA | `/tmp/hpc_max_tput/summary.tsv` | Kept, no clean pre-change A/B |
| P2 response batch | Aeron response publish | Normal run | 65,867,298 | 38.48 | 1,711,728 | 0.095 / 0.199 | Remote `run_aeron_best.sh` | Kept |
| P2 response batch | Aeron response publish | Flamegraph | 65,740,271 | 38.82 | 1,693,464 | 0.103 / 0.199 | `artifacts/aeron_best_flame_p2_resp_batch_20260722_123802` | Kept |

Notes:

- The TCP key-only decode run used the user's default command:
  `NUM_KEYS=100000 WORKERS='21:21' TS='64' CS='4' TEST_TIME=30 bash hpc_redis_max_tput.sh`.
- That TCP command defaults to `TRANSPORT=tcp`, `OP_MODE=vemb`, and `VEMB_READ_MODE=inline`, so it exercises `VEMB_INLINE`, not `vector-handle`.
- The TCP optimization covers `VEMB_INLINE`, `VEMB_HANDLE`, `VREM`, and `PING`, so the default TCP inline run does use the new fast decoder.
- The TCP number above is a post-change sanity/performance run, not a clean before/after comparison.
- P2 response batch raises total throughput clearly, but single-core efficiency is only slightly better than P1 peek in normal runs and roughly comparable in flamegraph runs.

### Hotspot Table

| Stage | Main hotspot before | Main change | Hotspot after | Interpretation |
| --- | --- | --- | --- | --- |
| P0 stable read | `key_meta_blocks_source_access` around `tlc_core_get_warm_location_raw` | Bypass source-fence/tombstone checks only when no migration/tombstone guards are active | `tlc_core_get_warm_location_raw` remains hot | Correctness-preserving cleanup; modest single-core gain in clean manual run |
| P1 stack reqs | `zmalloc` / `zfree` in Aeron request poll | Replace per-poll heap request buffer with stack batch buffer | allocator hotspot disappears | Useful but incomplete because full request-slot memcpy remains |
| P1 peek | `[libc.so.6]` leaf around 24.61% from full request-slot copy | Peek Aeron request ring slots and pass request pointers to proxy | `[libc.so.6]` leaf around 5.86%; request scheduler becomes top leaf | Biggest request-side win; kept |
| P1 handle-only scheduler | `vemb_v16_proxy_handle_request_ptr_batch_internal` | Specialized all-`VEMB_HANDLE` Aeron scheduler | Throughput and p99 regressed in polluted window | Not retained; retry only on idle host if needed |
| TCP key-only decode | Full `vemb_v16_req_t` clear/decode for key-only TCP requests | Decode key-only payload directly without clearing vector area | Follow-up host-mt run recorded | Helped TCP inline decode modestly; see host-mt follow-up |
| P2 response batch | `drain_completions` around 19.96%, `publish_response` around 12.86% leaf | Convert ready completions to `vemb_v16_resp_t[]` and batch publish response ring | `drain_completions` around 10.70%, batch publish leaf around 3.84% | Total throughput improves; per-core gain is small |

## Hotspots

Inclusive hotspots from `perf.folded`:

| Area | Representative frame | Approx. inclusive |
| --- | --- | ---: |
| SuperNode read lookup | `tlc_core_get_warm_location_raw` via `vemb_v16_tlc_get_handle` | `23% - 25%` |
| Proxy request poll | `vemb_v16_aeron_poll_shm_requests` | `33%` |
| Proxy completion/response | `drain_completions` / `publish_response` | `17% - 19%` |
| SuperNode job execution | `drain_shard_queues` / `vemb_v16_supernode_handle_vemb_job` | `27% - 39%` |
| Allocator overhead | `zmalloc` / `zfree` / jemalloc large alloc/free | several percent |
| Eventfd/syscall wakeups | `eventfd_read` / `eventfd_write` / `schedule` | several percent |

The most important observation is that the largest leaf is the TLC warm-location
lookup:

```text
tlc_core_get_warm_location_raw.lto_priv.0
```

The second important observation is that `vemb_v16_aeron_poll_shm_requests()`
currently allocates and frees a request batch buffer on each poll round, including
empty poll rounds. That is a lower-risk follow-up, but not the first item to
implement if we want to attack the biggest flamegraph block.

## Priority

### P0: TLC Stable Read Fast Path

Start here.

Current read path:

```text
vemb_v16_supernode_handle_vemb_job()
  -> vemb_v16_tlc_get_handle()
    -> tlc_core_get_warm_location()
      -> key_meta_blocks_source_access()
      -> tlc_core_get_warm_location_raw()
      -> key_meta_blocks_source_access()
```

Relevant files:

```text
src/vemb_v16_supernode.c
src/vemb_v16_tlc.c
src/tlc_core.c
```

The steady-state Aeron best run has:

```text
single-owner local read path
misses/sec = 0
no active migration
no active tombstone filter
VEMB_HANDLE read, no inline payload snapshot
```

In that state, the read path should be able to avoid the migration/tombstone
fence checks and go directly through the raw cached/warm lookup path. The
optimization should be explicit and contract-based:

```text
if no source fence is active and no tombstone filter is active:
    use raw warm-location lookup
else:
    keep the existing fenced read path
```

Expected implementation shape:

1. Add a TLC/core helper for stable local reads, for example:

```c
int tlc_core_get_warm_location_stable_read(tlc_core_t *core,
                                           const char *key,
                                           uint32_t key_len,
                                           uint64_t key_hash,
                                           tlc_warm_location_t *location);
```

2. The helper must still validate the key shape.

3. The helper may bypass `key_meta_blocks_source_access()` only when both
   `tlc_core_source_fence_active(core)` and `tombstone_filter_active(core)` are
   false.

4. If either guard is active, fall back to the existing
   `tlc_core_get_warm_location()` behavior.

5. Add a matching VEMB wrapper such as:

```c
int vemb_v16_tlc_get_handle_stable_read(...);
```

6. Use it only for the plain VEMB handle read path. Do not apply it to paths
   that need inline payload snapshots, write/delete operations, VSIM, migration
   redirect checks, or remote owner reads until separately validated.

Correctness constraints:

- Preserve source-side migration fences. `CUTOVER` and `SOURCE_GC` must still
  block source access.
- Preserve tombstone behavior. Tombstoned keys must not be served from old warm
  locations.
- Do not remove the existing fenced API. The new path should be an explicit fast
  path with a clear guard.
- Do not assume `region_id == region_index`.

Validation:

```text
make -C src vemb_v16_server
make -C benchmark vemb_v16_tlc_ut
TEST_TIME=60 ./run_aeron_best.sh
```

After implementation, recapture the same flamegraph. Success should show a
visible reduction in the `tlc_core_get_warm_location_raw` parent block or in the
overall `vemb_v16_tlc_get_handle` share, without introducing misses or migration
regressions.

### P1: Reuse Aeron Request Poll Buffer

Current code in `src/vemb_v16_aeron_transport.c` allocates `req_buf` inside
`vemb_v16_aeron_poll_shm_requests()` on every call:

```text
zmalloc(sizeof(*req_buf) * VEMB_V16_PROXY_BATCH)
zfree(req_buf)
```

This should become a proxy-worker or channel scratch buffer. It is a good
follow-up because it removes allocator noise from a very hot poll path. It is
not listed as P0 here only because the TLC lookup block is larger and directly
dominates the clean read-hit workload.

Implemented experiment:

```text
src/vemb_v16_aeron_transport.c
  vemb_v16_req_t reqs[VEMB_V16_PROXY_BATCH]
```

This removed the allocator from the request poll path. Later, the stronger P1
peek path below made this stack buffer unnecessary.

### P1: Aeron Request Ring Peek

After removing allocator noise, the next proxy-side issue was that
`vemb_v16_client_poll_batch()` copied the whole request ring slot into a local
`vemb_v16_req_t` array. For `dim=300`, the Aeron request slot includes the
inline vector area, so even read-only `VEMB_HANDLE` requests were paying a large
full-slot copy before the proxy copied only the key/base fields into a job slot.

Implemented experiment:

```text
src/vemb_v16_client_ring.h
  vemb_v16_client_peek_batch()
  vemb_v16_client_consume_batch()

src/vemb_v16_proxy.c
  vemb_v16_proxy_handle_request_ptr_batch()

src/vemb_v16_aeron_transport.c
  peek request ring slots
  handle requests through slot pointers
  consume the request ring after scheduling
```

This keeps the existing job pool, job_ref, shard queue, completion, and reclaim
semantics intact. Only the Aeron request ingress avoids the intermediate full
request copy.

Validation run:

```text
TEST_TIME=60 ./run_aeron_best.sh

ops/sec         : 61004440.28
hits/sec        : 61004440.28
misses/sec      : 0.00
p50 / p99       : 0.11900 / 0.19100 ms
server CPU cores: 36.25
ops/core/sec    : 1682881
handle_deref    : ok=3660184305 fail=0
```

Flamegraph artifact:

```text
artifacts/aeron_best_flame_p1_peek_20260722_115419/redis-server-flamegraph.svg
artifacts/aeron_best_flame_p1_peek_20260722_115419/perf.folded
artifacts/aeron_best_flame_p1_peek_20260722_115419/summary.txt
```

Flamegraph run:

```text
ops/sec         : 61352161.86
hits/sec        : 61352161.86
misses/sec      : 0.00
p50 / p99       : 0.11900 / 0.19100 ms
server CPU cores: 36.77
ops/core/sec    : 1668539
handle_deref    : ok=3680934980 fail=0
```

Key flamegraph change:

```text
Before P1 peek:
  [libc.so.6] leaf around 24.61%

After P1 peek:
  [libc.so.6] leaf around 5.86%
  vemb_v16_proxy_handle_request_ptr_batch_internal leaf around 21.19%
  tlc_core_get_warm_location_raw leaf around 17.91%
  publish_response leaf around 12.86%
```

Interpretation:

```text
The full request slot memcpy was a real bottleneck. Avoiding it moved the run
from roughly 55M ops/sec / 1.46M ops/core to roughly 61M ops/sec / 1.67M
ops/core while keeping p99 and correctness counters clean.
```

### P1 Experiment Not Retained: Aeron VEMB_HANDLE-Only Scheduler

Tried a narrower fast path that only handled all-`VEMB_HANDLE` Aeron batches:

```text
Aeron request ring slot pointers
  -> validate VEMB_HANDLE-only request shape
  -> allocate READ job slot
  -> fill read_job directly
  -> publish job_ref batch
```

This kept the existing job pool, generation, shard queue, completion, and
reclaim semantics, but bypassed the generic request pointer batch helper.

Observed runs before detecting external machine load:

```text
run 1:
  ops/sec      : 49973026.99
  p50 / p99    : 0.10300 / 8.03100 ms
  server cores : 32.01
  ops/core/sec : 1561169

run 2:
  ops/sec      : 45094182.70
  p50 / p99    : 0.10300 / 12.03100 ms
  server cores : 30.33
  ops/core/sec : 1486785
```

The experiment was not retained. Correctness counters stayed clean, but the
throughput/tail profile regressed sharply. A later check found unrelated heavy
remote load on the same host:

```text
/root/gqs/codespace/redis-8.6.3/src/redis-server 0.0.0.0:6390
/root/gqs/codespace/UnifiedBus/memtier_benchmark_origin/memtier_benchmark ...
```

Because the host was no longer clean, these numbers should not be treated as a
final microarchitectural verdict. For now, keep the proven P1 peek path and
retry any narrower scheduler experiment only on an idle machine.

## TCP Follow-Up: Key-Only Decode Fast Path

The Aeron P1 peek optimization does not map 1:1 to TCP because TCP receives a
byte stream rather than fixed SHM request slots. The analogous TCP cost is in
request decode:

```text
channel_read_tcp_request_from_input()
  -> memset(req, 0, sizeof(vemb_v16_req_t))
  -> vemb_v16_req_decode()
       -> memset(req, 0, sizeof(vemb_v16_req_t))
```

For key-only TCP requests such as `VEMB_HANDLE`, the payload is only:

```text
24 bytes header fields + key_len
```

but the old path cleared the full `vemb_v16_req_t`, including the large inline
vector area. This is the TCP-side equivalent of paying for a large request
object even when the hot workload is read-only.

Implemented experiment:

```text
src/vemb_v16_tcp_transport.c
  decode_tcp_key_only_request()
```

The fast decoder handles:

```text
VEMB_HANDLE
VEMB_INLINE
VREM
PING
```

It fills only the fields consumed by the existing proxy scheduling path:

```text
op, flags, req_id, channel_id, key_hash, key_len, key2_len,
key2_hash, topology_epoch, dim, vector_bytes, key
```

`VADD`, `VSIM_INLINE`, and `VSIM_KEY_KEY` still use the original full
`vemb_v16_req_decode()` path, preserving vector and two-key decode semantics.

Validation completed:

```text
make -C src vemb_v16_server
git diff --check
remote: make -C src redis-server USE_UB=yes
```

Benchmark status:

```text
Post-change default TCP inline run completed.
No clean pre-change A/B is available yet.
```

At implementation time, the remote host initially had an unrelated TCP sweep
running on the same machine and port family:

```text
/root/gqs/codespace/UnifiedBus/hpc-redis/src/redis-server 0.0.0.0:6390
/root/gqs/codespace/UnifiedBus/memtier_benchmark/memtier_benchmark ...
```

After the host became idle, this command was run:

```text
NUM_KEYS=100000 WORKERS='21:21' TS='64' CS='4' TEST_TIME=30 \
  bash hpc_redis_max_tput.sh
```

Result:

```text
ops/sec         : 11585807.09
hits/sec        : 11585807.09
p50             : 0.71100 ms
server CPU cores: 20.92
ops/core/sec    : ~553814
```

This default command uses TCP `VEMB_INLINE`, not `vector-handle`; the fast
decoder covers that path. Compare against a pre-fast-decode build with the same
command and an idle host if a precise TCP A/B is needed.

Follow-up on 2026-07-22:

```text
src/vemb_v16_tcp_transport.c
  TCP now builds a request pointer batch and calls
  vemb_v16_proxy_handle_request_ptr_batch() directly.

src/vemb_v16_proxy.c
  vemb_v16_proxy_handle_request_batch() wrapper removed.
```

Host-mt validation:

```text
NUM_KEYS=100000 WORKERS='21:21' TS='64' CS='4' TEST_TIME=30 \
  bash hpc_redis_max_tput.sh

ops/sec         : 11771274.05
hits/sec        : 11771274.05
p50 / p99       : 0.70300 / 0.86300 ms
server CPU cores: 21.10
ops/core/sec    : ~557880
```

Flamegraph artifact:

```text
perf/server_flamegraph_host_mt_read_21_21_t64_c4_20260722_153218.svg
```

Interpretation:

```text
Against the earlier same-day TCP inline run above, throughput moved from
11.586M to 11.771M ops/sec while CPU stayed roughly flat. This is a modest
positive result, and the larger value is that TCP and Aeron now share the same
pointer-batch scheduler entry instead of keeping a TCP-only request-batch
wrapper.
```

### P1: Batch Proxy Response Publishing

`drain_completions()` and `publish_response()` are a wide proxy-side block.
SuperNode already batches completion publication, but proxy-to-client response
publication still pays per-response overhead. A response-ring batch publish path
is the likely next larger optimization after the P0 TLC fast path.

Implemented experiment:

```text
src/vemb_v16_client_ring.h
  vemb_v16_client_publish_batch()

src/vemb_v16_aeron_transport.c
  vemb_v16_aeron_publish_response_batch()

src/vemb_v16_proxy.c
  Aeron branch of publish_completion_batch() builds vemb_v16_resp_t[] and
  publishes the response batch with one response-ring tail release.
```

Validation run:

```text
TEST_TIME=60 ./run_aeron_best.sh

ops/sec         : 65867297.62
hits/sec        : 65867297.62
misses/sec      : 0.00
p50 / p99       : 0.09500 / 0.19900 ms
server CPU cores: 38.48
ops/core/sec    : 1711728
handle_deref    : ok=3951993660 fail=0
```

Flamegraph artifact:

```text
artifacts/aeron_best_flame_p2_resp_batch_20260722_123802/redis-server-flamegraph.svg
artifacts/aeron_best_flame_p2_resp_batch_20260722_123802/perf.folded
artifacts/aeron_best_flame_p2_resp_batch_20260722_123802/summary.txt
```

Flamegraph run:

```text
ops/sec         : 65740270.60
hits/sec        : 65740270.60
misses/sec      : 0.00
p50 / p99       : 0.10300 / 0.19900 ms
server CPU cores: 38.82
ops/core/sec    : 1693464
handle_deref    : ok=3943694342 fail=0
```

Key flamegraph change:

```text
Before response batch:
  drain_completions inclusive around 19.96%
  publish_response inclusive around 18.45%
  publish_response leaf around 12.86%

After response batch:
  drain_completions inclusive around 10.70%
  vemb_v16_aeron_publish_response_batch leaf around 3.84%
```

Interpretation:

```text
Batching Aeron response-ring publication removed most of the per-response
publish_response cost and moved the stable run from roughly 61M ops/sec to
roughly 65.7M ops/sec while keeping p99 around 0.2 ms and correctness counters
clean.
```

### P2: Eventfd Wakeup Tuning

`eventfd_read`, `eventfd_write`, and scheduler frames are visible but should not
be tuned blindly. A previous adaptive-poll attempt reduced throughput in the
max-throughput workload, so any wakeup tuning must be guarded by counters first:

```text
job_notify_armed hits/sec
eventfd write/sec
eventfd read/sec
empty poll rounds/sec
avg drained jobs per wake
```

Only tune this path after the lookup and allocation issues are addressed.
