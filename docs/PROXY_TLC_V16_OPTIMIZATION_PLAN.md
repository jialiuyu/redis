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

The clean FC proxy does not use the historical bucket mutex, active bucket heap,
or batch scheduler. The combiner writes directly to the worker request ring.

Proposed config:

```conf
proxy-vemb-submit-mode fc
proxy-vemb-fc-slots 0            # 0 uses the v2 default: 256
proxy-vemb-fc-max-scan 0         # 0 scans up to slot_count
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

This is the intended behavior for the current benchmark and correctness phase:
`VSIM` performs a full scan over the candidate set and computes a score for each
candidate. For `p=1000, dim=300`, that means each query loads and scores 1000
vectors.

This can become expensive in:

1. worker response packet size
2. response ring bandwidth
3. proxy result copy
4. Redis reply formatting

Future TODO:

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

Current decision:

```text
Do not optimize VSIM full-scan/top-k in the current phase.
Keep it as a TODO until proxy fast-path behavior is validated and VEMB/VSIM
baseline numbers are stable.
```

## 7. TLC V16 Borrowing Matrix

This section maps the concrete `tlc_v16_server` / `tlc_v16_bench` techniques to
the current proxy/supernode architecture.

### 7.1 Directly Useful

1. Flat-combining publication board.
   - Source pattern: `g_slots`, `g_combiner_lock`, and `fc_get()` in
     `src/tlc_v16_server.c`.
   - Proxy mapping: add per-worker `VEMB` publication boards.
   - Redis command threads publish work and attempt TTAS combiner election.
   - Losing command threads must not spin for the UB result. They block the
     Redis client, publish work, and return. Result completion stays
     asynchronous through the response rings.

2. TTAS combiner election.
   - Source pattern: relaxed load checks the lock first, then CAS only when it
     appears free.
   - Proxy mapping: use TTAS around each `VEMB` combiner board to reduce cache
     line bouncing versus blind CAS loops.

3. Adaptive direct versus batch behavior.
   - Source pattern: one pending request uses a direct-style path; multiple
     pending requests are combined.
   - Proxy mapping: keep current VEMB adaptive direct/batch and use the new
     observability counters to decide whether FC should replace or augment
     bucket batching.

4. CPU affinity and spin backoff.
   - Source pattern: server/client threads are pinned and hot rings use spin
     before sleeping.
   - Proxy/supernode mapping: pin supernode workers, proxy result threads, and
     benchmark clients to non-overlapping cores. Keep adaptive spin/backoff in
     supernode workers and avoid syscalls on the hot receive path.

5. Benchmark discipline.
   - Source pattern: `tlc_v16_bench` allocates channels once, uses shared-memory
     rings, pins client threads, and avoids spawning a client process per op.
   - Current Go trigger tool is useful for correctness and end-to-end Redis
     behavior, but it is not a pure proxy/supernode peak benchmark.
   - Add a C benchmark later if we need to compare against TLC V16-style peak
     numbers.

### 7.2 Useful But Requires Architecture Changes

1. SPSC rings per channel.
   - TLC V16 gets much of its performance from single-producer/single-consumer
     rings with no mutex, no MPSC reservation, and no data-path syscall.
   - Current proxy has many Redis command threads writing the same worker ring,
     so it still needs the bucket mutex around `ring_buffer_reserve()` /
     `ring_buffer_commit_write()`.
   - To remove the last ring-write mutex, choose one of:

   ```text
   A. producer/thread -> worker SPSC rings, supernode worker polls many rings
   B. true MPSC reserve/commit API for the current worker ring
   ```

   Option A is closer to TLC V16 and has lower correctness risk.

2. Compact VEMB response.
   - TLC V16 returns a compact identifier for GET rather than copying the full
     1200-byte value.
   - For UB VEMB, an equivalent future path is:

   ```text
   supernode response: status + row_id / offset / warm_idx
   proxy/client: read vector from shared memory when needed
   ```

   This can reduce response-ring bandwidth and result-thread copy cost, but it
   changes the response contract and should come after the current direct/batch
   path is stable.

3. Result-thread sharding.
   - TLC V16 has one response ring per channel.
   - Current proxy has one result thread scanning all worker response rings.
   - If `result_queue_avg_us` grows, shard result polling by worker range or
     supernode id. Keep `RedisModule_UnblockClient()` API safety in mind.

### 7.3 Do Not Borrow Directly

1. Loser spin-wait until result.
   TLC V16 channel threads spin until their slot reaches `DONE`. Redis command
   threads must not spin waiting for UB completion.

2. Replacing Redis blocked-client lifecycle.
   TLC V16 owns the entire channel lifecycle. Proxy must keep Redis blocked
   clients, completion registry, and unblock semantics.

3. Applying top-k/compact response to current VSIM phase.
   Current VSIM full-scan/full-result behavior is intentional for this phase and
   remains a TODO rather than an active optimization item.

### 7.4 Recommended Next Landing Order

1. Supernode `VEMB` per-worker scratch buffers.
   - Current supernode VEMB allocates temporary row/result buffers in the hot
     path.
   - Add reusable scratch buffers in `sve_worker_context_t`.
   - This is the lowest-risk TLC-style cleanup because it does not change proxy
     semantics or ring contracts.
   - Status: implemented. Each supernode worker now owns reusable VEMB row and
     vector scratch buffers and grows them only when a larger VEMB batch arrives.

2. Proxy `VEMB` FC publication board MVP.
   - Add per-worker publication slots and TTAS combiner.
   - Keep the existing response path.
   - Initially keep the bucket/ring mutex only around ring write to avoid an
     unsafe MPSC rewrite.

3. SPSC multi-input rings or MPSC ring reserve.
   - Remove the last request-ring write mutex after FC behavior is validated.
   - Prefer per-producer SPSC rings if memory and worker polling overhead are
     acceptable.

4. VEMB compact response.
   - Return row/offset metadata instead of copying the full vector where the
     caller can read from UB shared memory.
   - This is a larger API/contract decision and should follow measurements that
     show response copy or ring bandwidth is limiting throughput.

5. Dedicated peak benchmark.
   - Add a TLC-style C benchmark for proxy/supernode peak measurements.
   - Keep the Go Redis trigger benchmark as the correctness and integration
     benchmark.

## 8. Test Plan

### 8.1 Unit Tests

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

### 8.2 Integration Tests

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

### 8.3 Trace Acceptance

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

## 9. Current Landing Status

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

7. Initial server-side bench validation.
   - VEMB with `proxy-vemb-adaptive yes` and
     `proxy-vemb-direct-gap-us 50` reached about 24k wall QPS for
     `-c 64 -n 5000 -d 300 -p 1000` with `failed=0`.
   - Recent VEMB traces show `req=1`, `proxy_wait_avg_us` around 1-2 us,
     confirming the direct singleton path is active.
   - VSIM with `-c 64 -n 5000 -d 300 -p 1000` reached about 12.7k wall QPS
     with `failed=0`.
   - Recent VSIM traces show `req=1`, `proxy_wait_avg_us` around 1 us, and
     `vector_load_ns` around 3.0-3.4 ms. The main VSIM cost is now the intended
     full candidate vector load, not proxy waiting.

8. Proxy observability.
   - `batch-trace` now appends proxy path metadata:

   ```text
   path=direct|batch
   flush_reason=direct|limit|timeout
   flush_trigger=direct|background|capacity|append
   bucket_depth=<n>
   gap_us=<latest arrival gap>
   ewma_gap_us=<arrival EWMA>
   ```

   - `INFO`/UB stats now expose direct and adaptive counters:

   ```text
   VEMB direct submits
   VSIM direct submits
   VEMB batch enqueues
   VEMB adaptive checks
   VEMB adaptive selected
   VEMB adaptive bucket nonempty
   VEMB adaptive gap selected
   VEMB adaptive EWMA selected
   VEMB direct fallbacks
   Direct ring full
   Batch size histogram
   Average batch flush size
   ```

9. Supernode `VEMB` scratch buffers.
   - `sve_worker_context_t` now owns reusable VEMB row-id and vector result
     scratch buffers.
   - `supernode_process_vemb_batch()` reuses those buffers instead of allocating
     `emb_ids` and `results` for every batch.
   - VSIM allocation behavior is intentionally unchanged because VSIM full-scan
     optimization is a separate TODO.
   - Supernode stats now expose:

   ```text
   VEMB scratch grows
   VEMB scratch max rows
   ```

10. Phase 2 `VEMB` flat-combining clean proxy.
   - `proxy-vemb-submit-mode fc` publishes VEMB work into per-worker FC boards
     instead of appending every request to the old bucket array.
   - Each board has hashed publication slots and a TTAS combiner lock.
   - The combiner claims `PENDING` slots and writes one pointer-carrying
     `fc_vemb_packet_t` directly to the worker request ring.
   - The supernode worker writes results directly into `proxy_vector_request_t`
     and calls `RedisModule_UnblockClient()`.
   - Runtime VEMB no longer uses active buckets, proxy batch buckets, flush
     scheduler/executor, response result thread, or completion dict lookup.
   - Added tunables:

   ```conf
   proxy-vemb-submit-mode batch | direct | adaptive | fc
   proxy-vemb-fc-slots 0       # 0 uses the v2 default: 256, minimum 64
   proxy-vemb-fc-max-scan 0    # 0 scans all slots
   ```

   - `INFO`/UB stats now expose FC counters:

   ```text
   FC Proxy Stats
   Published
   Combine rounds
   Combined requests
   Direct rounds
   Batch rounds
   Slot busy
   Ring busy
   Submit failures
   Average FC batch size
   ```

Verified locally:

```bash
make -C benchmark -B proxy_components_ut
./benchmark/proxy_components_ut
make -C benchmark -B proxy_aggregator_ut
./benchmark/proxy_aggregator_ut
make -C src
make -C benchmark ring_buffer_batch_bench
make -C benchmark ring_buffer_compare_bench
git diff --check
```

Server bench checklist:

1. Confirm config:

```bash
CONFIG GET proxy-vemb-submit-mode
CONFIG GET proxy-vemb-adaptive
CONFIG GET proxy-vemb-direct-gap-us
CONFIG GET proxy-vemb-fc-slots
CONFIG GET proxy-vemb-fc-max-scan
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
path
flush_reason
flush_trigger
bucket_depth
gap_us
ewma_gap_us
```

Interpretation:

1. Low-concurrency or sparse `VEMB` should show lower `proxy_wait_avg_us`.
2. High-concurrency `VEMB` should keep a reasonable average batch size and avoid
   a throughput regression.
3. `path=direct` with `flush_reason=direct` confirms the singleton direct path.
4. `path=batch` plus `flush_reason=limit|timeout` shows real bucket batching.
5. `VEMB adaptive selected` should track direct-path decisions, while
   `VEMB direct fallbacks` and `Direct ring full` should stay low.
6. If `result_queue_avg_us` grows after proxy wait drops, the next bottleneck is
   likely the single result thread.
7. If `queue_us` grows after request-id/row-id worker spreading, check whether
   supernode workers are saturated or whether UB bitmap/cache contention became
   visible.
8. For FC mode, `VEMB FC published` should match submitted VEMB requests,
   `VEMB FC combined requests` should advance, and `VEMB FC fallbacks` should
   remain low.

## 10. Remaining Work

Still remaining after the current landing:

1. Broader server-side benchmark validation.
   - Repeat VEMB/VSIM with larger `n`, different `c`, and larger `p`.
   - Keep `failed=0` and compare wall QPS, `queue_us`, and `e2e_avg_us`.

2. Server-side validation for `proxy-vemb-submit-mode fc`.
   - Compare FC against `adaptive` and `batch` with VEMB raw output.
   - Track wall QPS, FC average batch size, `queue_us`, `result_queue_avg_us`,
     and fallback counters.

3. Remove the last ring-write mutex.
   - Current direct and batch writes still serialize with the target bucket
     mutex because `ring_buffer_reserve()` is not MPSC-safe.
   - Full removal requires an MPSC reservation API or per-producer publication
     model that preserves ring-buffer correctness.

4. Result thread sharding.
   - Add multiple response-ring scanning threads if `result_queue_avg_us`
     becomes visible.

5. TODO: `VSIM` top-k / compact response.
   - Current `VSIM` response still returns all candidates.
   - This is expected for the current phase because VSIM is intentionally doing
     full candidate computation.
   - Supernode can eventually return only `requested_count` best results, but
     this is not an active optimization item right now.

6. Completion registry sharding. `[completed locally in next-stage patch]`
   - The completion registry now uses 64 request-id shards instead of one
     global dict lock.
   - Completion keys are stored as `uint64_t`, avoiding temporary `sds`
     formatting/allocation on lookup, complete, and take.

7. Config polish.
   - Potential future configs:

   ```conf
   proxy-vemb-submit-mode batch | direct | adaptive | fc
   proxy-route-policy key | row | request
   proxy-result-threads 1..N
   ```

## 11. Rollout Order

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
8: server bench partially validated for adaptive/direct
9: completion registry sharding implemented locally; result-thread sharding still
   pending benchmark evidence
10: implemented locally; pending FC server bench validation
```

## 12. Risks

1. Spreading `VSIM` by request id may increase UB bitmap/cache contention.
   Mitigation: make VSIM route policy configurable or keep VSIM key-routed until measured.

2. Direct `VEMB` submit can reduce batching throughput under heavy load.
   Mitigation: gate direct path by config first, then add adaptive policy.

3. More result threads can call `RedisModule_UnblockClient()` concurrently.
   Mitigation: verify Redis module API usage is safe from these threads in current code path; otherwise use result sharding only for packet processing and hand unblock to a safe executor.

4. Completion registry lock contention may become visible after result sharding.
   Mitigation: completion registry is now sharded by request id; re-check only
   if a shard-level lock shows up in profiling.

5. Adaptive direct may reduce high-load batching if the gap threshold is too
   large.
   Mitigation: compare `proxy-vemb-adaptive yes/no` and sweep
   `proxy-vemb-direct-gap-us`.

## 13. Definition of Done

Phase 1 is complete when:

1. `VEMB` and `VSIM` still return correct results.
2. `failed=0` in 5k request benchmark runs.
3. Single-request `VEMB` trace shows lower proxy wait.
4. `VSIM` trace no longer pays bucket wait.
5. Hot-key VEMB uses multiple worker rings.
6. Unit tests cover routing and direct packet serialization.
