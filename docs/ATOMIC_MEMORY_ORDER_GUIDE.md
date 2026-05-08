# Atomic Memory Order Guide

## Goal

This document records the memory-order rules used by the current UB / proxy /
supernode path, so later cleanups do not accidentally weaken correctness by
changing synchronization atomics to `memory_order_relaxed`.

The short rule is:

- use `memory_order_relaxed` for counters, statistics, and ID generation
- use `memory_order_acquire` / `memory_order_release` for publish-consume data paths
- use `memory_order_acq_rel` for refcount decrement on destruction paths

## Category 1: Counters and Statistics

These atomics only answer:

- how many times something happened
- what the current accumulated total is
- what the next numeric ID should be

They do not publish payload data to another thread.

For these, `memory_order_relaxed` is sufficient.

Current examples:

- `src/proxy_aggregator.c`
  - `total_requests`
- `src/proxy_flush_executor.c`
  - `next_batch_id`
  - `total_flushes`
  - `total_batches`
  - `batch_full_flushes`
  - `timeout_flushes`
  - `immediate_flush_attempts`
  - `immediate_flush_successes`
  - `immediate_flush_deferred`
  - `enqueue_rejections_full`
- `src/supernode_worker.c`
  - `total_batches`
  - `total_requests`
  - `sve_operations`
  - `total_latency_us`
  - `op_stats.lock_success`
  - `op_stats.lock_failure`

Why relaxed is enough:

- readers may observe slightly stale values without breaking correctness
- no ordering with surrounding non-atomic writes is required
- atomicity matters, but synchronization does not

## Category 2: Publish / Consume Synchronization

These atomics control whether another thread may safely observe payload data.

This is the critical case in `ring_buffer`.

Required pattern:

1. producer writes payload bytes
2. producer performs `release` store to publish the new tail/head state
3. consumer performs `acquire` load of the published state
4. consumer may then safely read the payload bytes

Current examples in `src/ring_buffer.c`:

- producer publish:
  - `atomic_store_explicit(&rb->tail, ..., memory_order_release)`
- consumer observe:
  - `atomic_load_explicit(&rb->tail, memory_order_acquire)`
- producer space check:
  - `atomic_load_explicit(&rb->head, memory_order_acquire)`
- consumer commit:
  - `atomic_store_explicit(&rb->head, ..., memory_order_release)`

These must not be weakened to `memory_order_relaxed`.

If they are weakened, a consumer may observe:

- updated `tail`
- but not yet the fully visible payload bytes associated with that tail

That would break the ring-buffer protocol.

## Category 3: Reference Counting and Destruction

Refcounting is not the same as payload publication, but it still participates in
object lifetime management.

Current examples in `src/ring_buffer.c`:

- retain:
  - `atomic_fetch_add_explicit(&rb->refcount, 1, memory_order_relaxed)`
- destroy path decrement:
  - `atomic_fetch_sub_explicit(&rb->refcount, 1, memory_order_acq_rel)`

Guideline:

- increment can usually stay `relaxed`
- decrement on the path that may free the object should stay `acq_rel`

This is conservative and appropriate for lifetime management.

## Practical Rule of Thumb

Ask what the atomic variable means.

If it means:

- "how many"
- "how much"
- "what is the next ID"

then `relaxed` is probably correct.

If it means:

- "is this payload now visible"
- "has this write been published"
- "is this object still safely alive for access"

then `relaxed` is probably not enough.

## Current Project Policy

For the current codebase:

- statistics and counters should prefer `atomic_*_explicit(..., memory_order_relaxed)`
- ring-buffer protocol atomics should keep their acquire/release semantics
- refcount destroy-side decrement should keep `memory_order_acq_rel`

Do not bulk-replace atomic operations with `relaxed` without first checking
which category the atomic belongs to.
