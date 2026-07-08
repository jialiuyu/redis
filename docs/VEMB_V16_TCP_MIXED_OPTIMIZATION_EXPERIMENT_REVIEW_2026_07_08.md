# VEMB V16 TCP mixed-80r20w Optimization Experiment Review

Date: `2026-07-08`

## 1. Scope

This document records the recent optimization attempts made after the stable
commit `d308274` on the TCP `mixed-80r20w` path, together with benchmark data
and analysis of why the changes did not improve throughput.

The intent is to avoid re-testing the same local ideas repeatedly and to make
it explicit where the remaining optimization space is likely to be small or
high-risk.

## 2. Stable Baseline

Stable reference point:

- commit: `d308274`
- subject: `perf(vemb-v16): reduce completion drain copy overhead`
- validated benchmark host: `root@192.168.90.111`
- benchmark repo path: `/root/szz/codespace/hpc-redis`

Benchmark command set:

```sh
make -C src USE_SVE=yes vemb_v16_server
make -B -C benchmark USE_SVE=yes vemb_v16_bench

./src/vemb_v16_server \
  --transport tcp \
  --tcp-host 0.0.0.0 \
  --tcp-port 6391 \
  --proxy-io-threads 16 \
  --supernode-workers 32 \
  --warm-regions-manifest /tmp/warm-regions-manifest.yaml \
  --reset-warm-regions \
  --dim 300 \
  --max-vectors 131072 \
  --loglevel notice

./benchmark/vemb_v16_bench \
  --transport tcp \
  --host 127.0.0.1 \
  --port 6391 \
  --mode mixed-80r20w \
  --dim 300 \
  --prefill 65536 \
  --keyspace 65536 \
  --ops 200000 \
  --threads 128 \
  --pipeline 32 \
  --timeout-ms 30000 \
  --no-pin
```

Stable baseline result:

- average QPS over 5 runs: `3,724,940.21`

This is the comparison point used below unless noted otherwise.

## 3. What `d308274` Already Improved

`d308274` itself was the useful optimization point in this round. The main
effective changes were:

- completion drain stopped compacting whole `vemb_v16_completion_t` structs
  and switched to collecting `ready_indices`
- TCP batch publish consumed completion arrays through indices instead of
  copying completion structs again
- `notify_completion_consumer_from_proxy()` moved from null-return behavior to
  `assert`
- inline snapshot allocation/release was already reduced to one contiguous block

An earlier A/B on this line showed the stable version was still slightly better
than the reverted version:

- current vs reverted improvement: about `+0.89%`

This is important context: the obvious completion-drain copy reduction had
already been captured, and later experiments were trying to optimize the
remaining smaller fixed costs.

## 4. Experiment Summary

### 4.1 Results Table

| Attempt | Main idea | Average / outcome | Relative to baseline |
| --- | --- | ---: | ---: |
| completion ref/pool | keep one shared completion record and move refs | `3,566,625.44` | `-4.25%` |
| direct encode from completion metadata | avoid `completion -> resp` materialization | `3,542,074.24` | `-4.91%` |
| header + encoded_resp metablock packing | reorganize batch write metadata | `3,603,493.98` | `-3.26%` |
| local warm copy fast path | narrow local warm read helper in TLC path | `3,589,707.56` | `-3.63%` |
| inline snapshot pool | replace per-request inline snapshot `zmalloc/zfree` with pool | `3,640,381.72` | `-2.27%` |
| response encode fast path | narrow fast encoder for `OK + VADD/VEMB_*` | `3,600,665.67` | `-3.33%` |
| proxy publish success-loop merge | combine status-note and payload-release loops | invalid, server crashed after run 1 | no valid average |

### 4.2 Per-attempt Notes

#### A. Completion ref/pool

Intent:

- imitate `job_pool` style ownership
- keep completion-related data in one place
- reduce metadata copying

Observed result:

- average `3,566,625.44`

Conclusion:

- this saved small copies
- but it introduced worse shared-memory/cache-coherence behavior than the saved
  work was worth

#### B. Direct encode from completion metadata

Intent:

- bypass `vemb_v16_make_response_from()`
- encode directly from completion fields

Observed result:

- average `3,542,074.24`

Conclusion:

- removing a tiny struct-materialization step was not a real end-to-end
  bottleneck
- the extra special-case path likely cost more in branch/code layout than it
  saved

#### C. Header + encoded response metablock packing

Intent:

- reorganize batch response metadata for writev/backlog handling

Observed result:

- average `3,603,493.98`

Conclusion:

- metadata packing changed local organization but did not reduce the true fixed
  cost on the hot path enough to matter

#### D. Local warm copy fast path

Intent:

- add a narrower local warm copy helper for `VEMB_INLINE`
- avoid the generic `resolve_warm_region_for_location()` path when the read is
  clearly local

Observed result:

- run1 `3,664,258.18`
- run2 `3,574,700.75`
- run3 `3,531,403.47`
- run4 `3,592,909.41`
- run5 `3,585,265.97`
- average `3,589,707.56`

Conclusion:

- the generic local read path was already well behaved
- the added narrow helper did not remove enough real work
- the extra control flow and helper split probably hurt instruction locality

#### E. Inline snapshot pool

Intent:

- replace per-request inline snapshot `zmalloc/zfree`
- keep only one payload copy
- use a per-channel pool and fall back to heap if empty

Observed result:

- run1 `3,677,026.21`
- run2 `3,648,837.71`
- run3 `3,698,717.64`
- run4 `3,564,868.20`
- run5 `3,612,458.86`
- average `3,640,381.72`

Conclusion:

- heap allocation looked suspicious in isolation
- but pool bookkeeping and changed reuse behavior still lost to the current
  simple path
- in this workload, allocator replacement alone was not enough to lower the
  larger end-to-end fixed costs

#### F. Response encode fast path

Intent:

- keep protocol and transport behavior unchanged
- add a narrow encoder fast path only for the hottest successful response
  shapes:
  - `OK + VADD`
  - `OK + VEMB_HANDLE`
  - `OK + VEMB_INLINE`

Observed result:

- run1 `3,627,573.17`
- run2 `3,593,667.27`
- run3 `3,605,486.07`
- run4 `3,610,613.31`
- run5 `3,565,988.52`
- average `3,600,665.67`

Conclusion:

- even a very narrow encoder specialization still regressed
- the generic inline encoder was likely already small enough that the extra
  function split and branch routing were not worth it

#### G. Proxy publish success-loop merge

Intent:

- merge two success-path loops in `publish_completion_batch()`:
  - note response status
  - release payload

Observed result:

- run1 `3,584,154.29`
- server then crashed before more runs completed

Crash:

- assertion failed in `src/vemb_v16_supernode.c:167`
- assertion: `snapshot->payload_bytes == completion->inline_vector_bytes`

Conclusion:

- even this very small lifecycle reordering touched a sensitive payload-release
  boundary
- this experiment did not produce a valid throughput result
- it also showed that the remaining completion/payload lifecycle is easy to
  destabilize for negligible expected gain

## 5. Why These Attempts Failed

The repeated regressions are not random. Several patterns showed up:

### 5.1 The obvious copy win was already taken

The profitable part of this line was already captured by `d308274`:

- stop moving full completion structs around
- operate on ready indices instead

After that point, most remaining experiments only removed very small local work
items while adding new bookkeeping, control flow, or reuse behavior elsewhere.

### 5.2 End-to-end TCP cost dominates local micro-savings

The benchmark is not a tiny in-memory microbench. The hot path still includes:

- payload snapshot/copy
- response encode
- `writev` / socket send
- backlog handling under pressure
- proxy/supernode scheduling boundaries

Saving one metadata copy or one tiny helper call does not automatically show up
in end-to-end QPS when these larger fixed costs remain.

### 5.3 Cache and coherence costs matter more than small memcpy wins

Several ideas were logically attractive because they reduced copying, but they
changed ownership and memory-access patterns:

- shared refs
- pooled metadata
- reused payload slots

Those changes tend to increase:

- coherence traffic
- cache-line bouncing
- branch pressure
- instruction footprint

At this stage, those secondary costs were often larger than the saved local copy
or allocation.

### 5.4 The remaining lifecycle is already tight

The crash in the final loop-merge attempt is also informative. It suggests the
completion/inline-payload lifecycle is already near a tight correctness margin.

That does not prove the existing design is wrong. It does show that:

- this area is easy to destabilize
- the expected performance upside is already very small
- further micro-surgery here has poor risk/reward

## 6. What This Means

The practical conclusion is:

1. `d308274` should be treated as the current stable best point on this line.
2. Further small changes in completion organization, TLC local fast paths,
   payload pooling, or response encoder specialization are more likely to
   regress than improve this benchmark.
3. If future work continues, it should move up one level and target a larger
   fixed cost, not another local metadata tweak.

## 7. Recommended Next-Step Policy

For this specific TCP `mixed-80r20w` line:

- do not continue exploring completion-side ref/pool changes
- do not continue exploring TLC local warm micro-fast-paths
- do not continue exploring inline snapshot pooling on this path
- do not continue exploring narrow response encoder specializations

If another optimization pass is needed later, it should start from a different
level, for example:

- syscall / socket / backlog cost reduction with stronger evidence
- larger batching boundary changes
- a benchmark-meaningful transport/path change rather than local metadata
  reshaping

Until such a direction is justified, the best engineering decision is to keep
`d308274` as the stage-optimal implementation.
