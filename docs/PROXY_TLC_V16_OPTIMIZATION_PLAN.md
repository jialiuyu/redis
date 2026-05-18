# Proxy Optimization Landing Plan Based on TLC V16

## 1. Current Problem

Recent traces show proxy-side latency is visible for both `VEMB` and `VSIM`.

Example `VEMB` trace:

```text
batch-trace batch=2378 op=1 req=1 done=1 proxy_wait_avg_us=74.0 proxy_flush_us=1 queue_us=24 vector_load_ns=5610 e2e_avg_us=148.0
```

This means a single-row `VEMB` spends about half of end-to-end latency waiting in the proxy bucket before the supernode sees it.

Example `VSIM` trace:

```text
batch-trace batch=2270 op=2 req=1 done=1 proxy_wait_avg_us=132.0 queue_us=30108 vector_load_ns=2967010 compute_ns=256390 e2e_avg_us=33672.0
```

For `VSIM`, the current proxy wait is especially wasteful because current packetization only supports one `VSIM` request per packet. Waiting for a bucket window does not create a real multi-request `VSIM` batch.

The high-level issues are:

1. Hot-key routing collapses parallelism.
   `proxy_router_route()` hashes only the Redis key. A benchmark against one key, such as `myvectors`, routes all requests to one worker/ring. With `supernode-workers 64`, the system can still effectively use only one worker for that hot key.

2. `VEMB` always pays bucket and flush coordination.
   The path is command thread -> bucket mutex -> active bucket heap -> flush decision -> request ring. This is good for batching under load, but too expensive for low-latency single-row reads.

3. `VSIM` is forced into a VEMB-style bucket model.
   `proxy_batch_bucket_fill_packet()` only builds a `BATCH_PACKET_OP_VSIM` packet when the bucket count is exactly 1. Therefore `proxy-batch-limit` does not help `VSIM` today.

4. Response processing is centralized.
   One result thread scans all response rings and then does completion lookup, result copy, trace update, and `RedisModule_UnblockClient()`.

5. `VSIM` returns full candidate results.
   The worker returns one `(row_id, score)` per candidate. For large sets, proxy and response rings carry much more data than most user-facing queries need.

## 2. TLC V16 Lessons To Borrow

`src/tlc_v16_server.c` should be borrowed as a set of design patterns, not copied as architecture.

Borrow:

1. Adaptive fast path vs batch path.
   TLC V16 treats one pending request differently from many pending requests. We should do the same:
   - single or latency-sensitive `VEMB`: direct submit
   - high-load `VEMB`: batch submit
   - current `VSIM`: direct submit by default

2. Remove unnecessary thread hops.
   TLC V16 avoids a dedicated batch worker. For Redis proxy, we should not make Redis command threads execute worker work, but we can remove unnecessary proxy flush-thread hops for requests that cannot benefit from waiting.

3. Publication-slot style aggregation.
   TLC V16 uses cache-line-aligned per-channel slots and a combiner. This is a useful Phase 2 model for `VEMB` aggregation because it reduces bucket mutex and active-heap churn.

4. TTAS combiner election.
   Test-and-test-and-set reduces cache-line bouncing versus blind CAS loops. If Phase 2 adds combiner-based proxy aggregation, use TTAS.

5. Compact response contracts.
   TLC returns compact identifiers instead of large payloads when possible. For UB, `VSIM` should eventually return only top-k `(row_id, score)` from supernode, with Redis-facing formatting left in proxy/module code.

Do not borrow directly:

1. Do not spin-wait in Redis command threads until a result is ready.
2. Do not replace Redis blocked-client lifecycle with TLC channel lifecycle.
3. Do not force `VSIM` large results through the same small-result path forever.

## 3. Target Architecture

The proxy should have operation-aware submission:

```text
VEMB
  -> route by key + request identity
  -> direct submit if low-latency path selected
  -> batch submit if enough compatible requests are available
  -> small fixed response

VSIM
  -> collect candidate rows in proxy
  -> direct submit to selected worker
  -> large or top-k response path
```

Control-plane and data-plane responsibilities remain separated:

```text
Redis/module/proxy:
  - command parsing
  - key/element metadata lookup
  - candidate row collection
  - blocked-client lifecycle
  - completion registry
  - Redis reply formatting

Supernode worker:
  - row_id[] -> vector load
  - query_vector + candidate rows -> scores
  - compact result packet emission
```

## 4. Phase 1: Low-Risk Proxy Fast Path

Phase 1 should be small enough to land safely and test quickly.

### 4.1 Hot-Key Worker Distribution

Current route:

```text
worker_id = hash(key) % workers_per_node
```

Problem:

```text
all VEMB/VSIM against myvectors -> same worker
```

Target:

```text
VEMB route hash = hash(key, row_id or request_id)
VSIM route hash = hash(key, request_id), configurable
```

Recommended first implementation:

```text
VEMB:
  worker_id = row_id % workers_per_node

VSIM:
  worker_id = request_id % workers_per_node
```

Rationale:

1. `VEMB` reads one row, so row-based striping is safe and improves worker utilization.
2. `VSIM` scans many rows, so spreading queries can improve CPU parallelism but may increase bitmap/cache contention. Keep this configurable if possible.

New config suggestion:

```conf
proxy-route-policy key | row | request
```

If a new config is too much for Phase 1, hard-code:

```text
VEMB=row
VSIM=request
```

### 4.2 VSIM Direct Submit

Current `VSIM` path:

```text
proxy_submit_vsim
  -> create proxy_vector_request
  -> proxy_enqueue_vector_request
  -> bucket
  -> flush thread or immediate flush
  -> request ring
```

Target:

```text
proxy_submit_vsim
  -> create proxy_vector_request
  -> register completion
  -> build batch_vsim_packet_t directly in target request ring
```

Expected effect:

1. Remove bucket wait for `VSIM`.
2. Remove active bucket heap operations for `VSIM`.
3. Remove the flush-thread hop for `VSIM`.
4. Keep a short per-target ring mutex while writing the packet, because the
   current ring buffer has a single-writer reservation contract.
5. Keep blocked-client and response semantics unchanged.

Trace expectation:

```text
VSIM proxy_wait_avg_us -> near 0
VSIM proxy_flush_us    -> direct submit cost only
```

### 4.3 VEMB Adaptive Direct Submit

For `VEMB`, batching is useful, but low-load single-row requests should not wait for a micro-batch window.

Implementation already landed:

```text
if proxy-batch-limit <= 1 or proxy-time-limit-us == 0:
    direct submit VEMB packet with one request
else:
    keep existing bucket batching
```

Second implementation:

```text
if adaptive is enabled and bucket is empty and recent traffic is sparse:
    direct submit
else:
    bucket batch
```

The first adaptive implementation should avoid busy waiting in Redis command
threads. It only decides whether the current request is worth waiting for:

```text
recent_gap_us = now - bucket.last_append_time_us
bucket.recent_gap_ewma_us = ewma(bucket.recent_gap_ewma_us, recent_gap_us)

if bucket.count == 0 and bucket.recent_gap_ewma_us >= proxy-vemb-direct-gap-us:
    direct submit
else:
    append to bucket
```

Default policy:

```conf
proxy-vemb-adaptive yes
proxy-vemb-direct-gap-us 20
```

The adaptive path still keeps the target bucket mutex while writing the request
ring. This is intentional: the current `ring_buffer_reserve()` API has a
single-writer reservation contract through `reservation_active`. The optimization
is to avoid bucket waiting and flush-thread scheduling when there is no useful
batch to form, not to make the ring buffer MPSC.

Trace expectation for single-request `VEMB`:

```text
proxy_wait_avg_us drops from tens of us to low single-digit us
e2e_avg_us drops accordingly
```

Trace expectation under high-concurrency `VEMB`:

```text
avg batch size stays above 1 when request gaps are small
wall qps does not regress versus the old bucket-only path
```

### 4.4 Result Thread Sharding

Current:

```text
one result_thread scans all response rings
```

Target:

```text
N result threads
each thread handles fixed ring range
```

Phase 1 option:

```conf
proxy-result-threads 1..N
```

Default can remain 1 for compatibility. Benchmark with 4 or 8 on `supernode-workers 64`.

Expected effect:

1. Lower result queue latency under high concurrency.
2. Less delay before `RedisModule_UnblockClient()`.

## 5. Phase 2: TLC-Style VEMB Combiner

Phase 2 replaces or augments bucket batching for `VEMB`.

Design:

```text
per lane:
  cache-line-aligned slot
  state: EMPTY -> PENDING -> CLAIMED
  request pointer or row_id/request_id descriptor

combiner:
  TTAS election
  scans pending slots
  builds VEMB batch_packet_t
  writes one request ring packet
```

Important difference from TLC:

```text
Redis command thread does not wait for DONE.
It only publishes work and returns REDISMODULE_OK after blocking the client.
Completion still happens through response rings.
```

This phase targets:

1. Lower bucket mutex contention.
2. Lower active bucket heap pressure.
3. Better high-load batching without fixed sleep/wakeup behavior.

Concrete proxy design:

```c
typedef struct proxy_vemb_fc_slot {
    atomic_int state;      /* EMPTY -> WRITING -> PENDING -> CLAIMED */
    uint64_t request_id;
    uint64_t row_id;
    uint64_t submit_time_us;
    proxy_vector_request_t *owner;
} proxy_vemb_fc_slot_t;

typedef struct proxy_vemb_fc_board {
    proxy_vemb_fc_slot_t *slots;
    size_t slot_count;
    atomic_int combiner_lock;
    atomic_uint_fast64_t pending_count;
    proxy_batch_bucket_t *target_bucket;
    ring_buffer_t *rb;
} proxy_vemb_fc_board_t;
```

Submit flow:

```text
proxy_submit_vemb
  -> create request and register completion
  -> publish row_id/request_id/owner into a per-thread or hashed FC slot
  -> try TTAS combiner election
  -> return REDISMODULE_OK after publishing work
```

Combiner flow:

```text
try combiner_lock with TTAS
  -> scan PENDING slots
  -> CAS PENDING -> CLAIMED
  -> collect up to proxy-batch-limit rows
  -> build one BATCH_PACKET_OP_VEMB packet
  -> write target request ring
  -> owner.batch_id = batch_id
  -> slot state = EMPTY
release combiner_lock
```

The proxy must not copy TLC V16's loser behavior. In TLC V16, losing channel
threads spin until their slot becomes `DONE`. In Redis proxy, losing command
threads must not wait for UB work to complete. They block the Redis client,
publish work, and return. Completion remains:

```text
supernode response ring -> result_thread -> vector_proxy_completion -> RedisModule_UnblockClient
```

Initial FC MVP can still use the existing bucket mutex only around
`ring_buffer_reserve()` / `ring_buffer_commit_write()`. That removes the
per-request bucket append and active-heap operations from the hot path while
avoiding a risky MPSC ring-buffer rewrite. A later ring-buffer patch can replace
that last mutex with an MPSC reservation API.

Proposed config:

```conf
proxy-vemb-submit-mode adaptive   # batch | direct | adaptive | fc
proxy-vemb-fc-slots 1024
proxy-vemb-fc-max-scan 1024
```

Proposed metrics:

```text
vemb_fc_published
vemb_fc_combines
vemb_fc_avg_batch
vemb_fc_slot_full
vemb_fc_ring_busy
```

## 6. Phase 3: VSIM Large Result and Top-K

Current `VSIM` returns all candidates:

```text
result_count = candidate_count
```

This is expensive in:

1. worker response packet size
2. response ring bandwidth
3. proxy result copy
4. Redis reply formatting

Target:

```text
supernode computes top-k by requested_count
response packet returns only k entries
```

Minimum semantic decision:

```text
COUNT N means return N best scores
if COUNT omitted, use existing default from VSIM parser
```

Expected effect:

1. Much smaller `VSIM` response packets.
2. Lower proxy result handling cost.
3. Lower client response size.

## 7. Test Plan

### 7.1 Unit Tests

Add focused tests for packet routing and packet generation:

1. VEMB route distribution:
   - same key, row ids 0..63
   - expect multiple worker ids, ideally row_id % workers

2. VSIM direct packet:
   - query_dim copied correctly
   - candidate_count copied correctly
   - candidate row array copied correctly
   - packet_size matches payload

3. VEMB direct packet:
   - request_id and row_id preserved
   - op_type is `BATCH_PACKET_OP_VEMB`
   - num_requests is 1

### 7.2 Integration Tests

Use a local Redis instance with UB engine:

1. Prefill:

```bash
go run benchmark/trigger_proxy_aggregation.go \
  --redis-cli ./src/redis-cli \
  --port 6391 \
  --key myvectors \
  --phase prefill \
  --mode vsim \
  -d 300 -p 1000
```

2. VEMB latency:

```bash
go run benchmark/trigger_proxy_aggregation.go \
  --redis-cli ./src/redis-cli \
  --port 6391 \
  --key myvectors \
  --phase query \
  --mode vemb \
  --raw \
  -c 64 -n 5000 -d 300 -p 1000
```

3. VSIM latency:

```bash
go run benchmark/trigger_proxy_aggregation.go \
  --redis-cli ./src/redis-cli \
  --port 6391 \
  --key myvectors \
  --phase query \
  --mode vsim \
  -c 64 -n 5000 -d 300 -p 1000
```

### 7.3 Trace Acceptance

For `VEMB` single-request traces:

```text
proxy_wait_avg_us should drop materially
result_queue_avg_us should not regress
e2e_avg_us should improve
```

For `VSIM` traces:

```text
proxy_wait_avg_us should approach direct submit overhead
queue_us should improve if worker routing spreads load
result_queue_avg_us should not become the new dominant bottleneck
```

For throughput:

```text
wall qps should improve or stay flat
failed should remain 0
```

## 8. Current Landing Status

This section captures the current implementation state before server-side bench
validation.

Completed:

1. Hot-key worker distribution.
   - `VEMB`: `worker_id = row_id % workers_per_node`
   - `VSIM`: `worker_id = request_id % workers_per_node`
   - Goal: avoid one Redis key mapping all proxy work to a single worker ring.

2. `VSIM` direct submit.
   - `VSIM` no longer enters the bucket/flush-thread path.
   - Proxy directly builds `batch_vsim_packet_t` and writes the target request
     ring.
   - The existing blocked-client, response-ring, and completion semantics are
     unchanged.

3. `VEMB` direct submit helper.
   - Single-row `BATCH_PACKET_OP_VEMB` direct packet path exists.
   - Config-forced direct path is available when `proxy-batch-limit <= 1`.
   - The `proxy-time-limit-us == 0` branch exists in code, but current config
     initialization still treats zero as "use default", so this is not yet a
     reliable external switch.

4. `VEMB` adaptive direct/batch.
   - New config:

   ```conf
   proxy-vemb-adaptive yes
   proxy-vemb-direct-gap-us 20
   ```

   - If the target bucket is empty and recent arrivals are sparse, proxy uses
     direct submit.
   - If arrivals are dense, proxy keeps the existing bucket batch path.
   - If adaptive direct write fails because the ring is busy/full, the request
     falls back to the bucket path instead of failing immediately.

5. `VSIM` completion ownership fix.
   - Result rows are stored separately in `result_rows`.
   - Original `candidate_rows` and `candidate_elements` remain owned by the
     request and are still available for Redis reply formatting.
   - This avoids overwriting candidate rows with result rows and prevents free
     path size mismatches.

6. Component tests and builds.
   - `proxy_components_ut` covers route distribution, VEMB packet layout, VSIM
     packet layout, and arrival-gap EWMA.
   - Ring-buffer benchmark targets were adjusted to the current packet header
     layout.

Verified locally:

```bash
make -C benchmark -B proxy_components_ut
./benchmark/proxy_components_ut
make -C src
make -C benchmark ring_buffer_batch_bench
make -C benchmark ring_buffer_compare_bench
git diff --check
```

Known local test gap:

1. `proxy_aggregator_ut` is currently stale.
   It includes `proxy_aggregator.c` directly, calls deprecated
   `proxy_enqueue_request()`, and does not provide a complete RedisModule API
   shim. It should be repaired separately before being used as a gate.

Server bench checklist:

1. Confirm config:

```bash
CONFIG GET proxy-vemb-adaptive
CONFIG GET proxy-vemb-direct-gap-us
CONFIG GET proxy-batch-limit
CONFIG GET proxy-time-limit-us
CONFIG GET supernode-workers
```

2. Recommended first run with defaults:

```bash
go run benchmark/trigger_proxy_aggregation.go \
  --redis-cli ./src/redis-cli \
  --port 6391 \
  --key myvectors \
  --phase query \
  --mode vemb \
  --raw \
  -c 64 -n 5000 -d 300 -p 1000
```

3. Compare with adaptive disabled:

```bash
./src/redis-cli -p 6391 CONFIG SET proxy-vemb-adaptive no
go run benchmark/trigger_proxy_aggregation.go \
  --redis-cli ./src/redis-cli \
  --port 6391 \
  --key myvectors \
  --phase query \
  --mode vemb \
  --raw \
  -c 64 -n 5000 -d 300 -p 1000
```

4. Re-enable adaptive and sweep the gap threshold if needed:

```bash
./src/redis-cli -p 6391 CONFIG SET proxy-vemb-adaptive yes
./src/redis-cli -p 6391 CONFIG SET proxy-vemb-direct-gap-us 5
./src/redis-cli -p 6391 CONFIG SET proxy-vemb-direct-gap-us 20
./src/redis-cli -p 6391 CONFIG SET proxy-vemb-direct-gap-us 50
```

Bench fields to compare:

```text
successful / failed
avg ms/op
wall qps
proxy_wait_avg_us
proxy_flush_us
queue_us
result_queue_avg_us
e2e_avg_us
```

Interpretation:

1. Low-concurrency or sparse `VEMB` should show lower `proxy_wait_avg_us`.
2. High-concurrency `VEMB` should keep a reasonable average batch size and avoid
   a throughput regression.
3. If `result_queue_avg_us` grows after proxy wait drops, the next bottleneck is
   likely the single result thread.
4. If `queue_us` grows after request-id/row-id worker spreading, check whether
   supernode workers are saturated or whether UB bitmap/cache contention became
   visible.

## 9. Remaining Work

Still remaining after the current landing:

1. Server-side benchmark validation.
   - Validate correctness and performance against real UB/shared-memory setup.
   - Compare adaptive on/off and threshold sweep.

2. TLC V16 style `VEMB` flat-combining implementation.
   - Add per-worker publication boards.
   - Add TTAS combiner election.
   - Let command threads publish work and return after blocking the Redis client.
   - Keep result completion asynchronous through response rings.

3. Remove the last ring-write mutex.
   - Current direct and batch writes still serialize with the target bucket
     mutex because `ring_buffer_reserve()` is not MPSC-safe.
   - Full removal requires an MPSC reservation API or per-producer publication
     model that preserves ring-buffer correctness.

4. Result thread sharding.
   - Add multiple response-ring scanning threads if `result_queue_avg_us`
     becomes visible.

5. `VSIM` top-k / compact response.
   - Current `VSIM` response still returns all candidates.
   - Supernode should eventually return only `requested_count` best results.

6. Completion registry sharding.
   - The completion registry currently has a global lock.
   - Shard by request id if result processing becomes concurrent or lock-bound.

7. Config polish.
   - Potential future configs:

   ```conf
   proxy-vemb-submit-mode batch | direct | adaptive | fc
   proxy-route-policy key | row | request
   proxy-result-threads 1..N
   ```

## 10. Rollout Order

Original recommended order:

1. Add route helper that can route by `row_id` or `request_id`.
2. Add direct submit helper for a single VEMB packet.
3. Add direct submit helper for VSIM packet.
4. Wire `VSIM` to direct submit.
5. Wire `VEMB` direct submit only when `proxy-batch-limit <= 1` or `proxy-time-limit-us == 0`.
6. Add adaptive `VEMB` direct/batch.
7. Add tests for route distribution, direct packet serialization, and arrival
   gap EWMA.
8. Benchmark VEMB and VSIM separately.
9. Add result-thread sharding if result queue becomes visible.
10. Implement Phase 2 combiner only after Phase 1 numbers are stable.

Current order status:

```text
1-7: implemented locally
8: pending server bench
9-10: pending benchmark evidence
```

## 11. Risks

1. Spreading `VSIM` by request id may increase UB bitmap/cache contention.
   Mitigation: make VSIM route policy configurable or keep VSIM key-routed until measured.

2. Direct `VEMB` submit can reduce batching throughput under heavy load.
   Mitigation: gate direct path by config first, then add adaptive policy.

3. More result threads can call `RedisModule_UnblockClient()` concurrently.
   Mitigation: verify Redis module API usage is safe from these threads in current code path; otherwise use result sharding only for packet processing and hand unblock to a safe executor.

4. Completion registry global mutex may become visible after result sharding.
   Mitigation: shard completion registry by request id in a later patch.

5. Adaptive direct may reduce high-load batching if the gap threshold is too
   large.
   Mitigation: compare `proxy-vemb-adaptive yes/no` and sweep
   `proxy-vemb-direct-gap-us`.

## 12. Definition of Done

Phase 1 is complete when:

1. `VEMB` and `VSIM` still return correct results.
2. `failed=0` in 5k request benchmark runs.
3. Single-request `VEMB` trace shows lower proxy wait.
4. `VSIM` trace no longer pays bucket wait.
5. Hot-key VEMB uses multiple worker rings.
6. Unit tests cover routing and direct packet serialization.
