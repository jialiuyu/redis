# VEMB V16 Batch + Payload Cache Experiment Summary

Date: 2026-07-19

## Context

This document summarizes the recent `payload batch + payload cache` design,
implementation experiments, and benchmark results on the multi-thread test
host.

The original motivation was:

1. Add a simple batch dedup path for same-key concurrent payload reads.
2. Add a storage payload cache so hot keys can avoid repeated UB payload
   reads.

During implementation and benchmark validation, we found that the original
`batch + cache` implementation caused a large regression under the baseline
throughput workload. We then split the problem, disabled `batch`, iterated on
the `payload cache` implementation, and tested several cache organizations.

## Intended Design

### 1. Payload Batch

The simplified batch idea was:

1. Reuse per-key metadata.
2. If a key already has an active payload reader, the first request becomes the
   leader.
3. Later requests for the same key become followers and wait for the leader to
   publish the payload.
4. The leader shares the payload with followers using reference-managed
   lifetime.

This design was intended to reduce duplicated UB payload loads under same-key
contention.

### 2. Payload Cache

The payload cache idea was:

1. Cache hot-key payloads in local memory.
2. Let TCP response path reference cached local payload memory.
3. Avoid direct TCP `writev` against UB memory.

We had already validated earlier that direct TCP `writev` over UB memory was
slower, so the optimization direction stayed on:

1. Copy from UB into local cache.
2. Reference local cached payload in response path.
3. Release reference after response publish completes.

## Benchmark Environment

Test host:

- `root@192.168.90.111`
- `/root/szz/codespace/hpc-redis`

Common benchmark parameters:

```bash
NUM_KEYS=100000 WORKERS='21:21' TS='64' CS='4' TEST_TIME=30
```

This workload was used as the main apples-to-apples comparison baseline.

## Baseline and Regression Confirmation

### Valid Baseline

Two non-cache commits were confirmed to be at the same performance level after
rebuild:

| Commit | Description | ops/sec | p50 | cores | mem |
| --- | --- | ---: | ---: | ---: | ---: |
| `e16bcc83e4367032639068996606429cf0e8a731` | rebuilt baseline | 11,739,187.86 | 0.711 ms | 25.66 | 307/404 MB |
| `a1fe6d6ffa579df605677f669a43fd21a1fd9bf7` | rebuilt baseline | 11,760,239.13 | 0.711 ms | 25.02 | 289/386 MB |

These results established that the baseline was around `11.74M ~ 11.76M`
ops/sec.

### Full Batch + Cache Regression

Commit:

- `7f7aa6ec97bd801a8d703b2ad53f73fdcb34f0f4`

Result:

| Variant | ops/sec | p50 | cores | mem |
| --- | ---: | ---: | ---: | ---: |
| `batch + cache` | 5,274,706.55 | 1.559 ms | 36.54 | 314/412 MB |

Regression vs baseline:

- about `-55%`

This confirmed a major performance regression and triggered component-level
isolation.

## Isolation Experiments

### 1. Disable Batch at Compile Time, Keep Cache

We added a compile-time switch to fully disable the `payload batch` path while
preserving `payload cache`.

Result:

| Variant | ops/sec | p50 | cores | mem |
| --- | ---: | ---: | ---: | ---: |
| `cache only`, original snapshot design | 5,647,015.15 | 1.455 ms | 35.93 | 314/412 MB |

Conclusion:

- Disabling `batch` only recovered a small part of the regression.
- The main problem was not only the batch logic.
- `payload cache` implementation itself was still too expensive.

### 2. Fixed-Entry Cache, Open Addressing

We replaced heap snapshot allocation on the hot path with:

1. preallocated fixed entries
2. preallocated payload storage
3. entry reuse

But the cache still used open addressing and shard-wide probing.

Result:

| Variant | ops/sec | p50 | cores | mem |
| --- | ---: | ---: | ---: | ---: |
| fixed-entry cache, open addressing | 4,402,007.59 | 1.903 ms | 37.46 | 315/409 MB |

Conclusion:

- Removing heap allocation alone did not solve the problem.
- Open-addressing probe cost and cache management overhead were still too high.

## Cache Reorganization: Bucket + Set-Way

We then changed the cache organization from:

1. shard-wide open addressing

to:

1. `hash -> bucket`
2. fixed `set-way` inside bucket
3. lookup/reserve/invalidate restricted to one bucket

This removed shard-wide scans and bounded per-lookup work to a small number of
ways.

### Initial 4-Way Result

| Variant | ops/sec | p50 | cores | mem |
| --- | ---: | ---: | ---: | ---: |
| bucket + 4-way | 9,238,523.57 | 0.871 ms | 35.92 | 313/409 MB |

This was the first major recovery and confirmed that the old shard-wide scan
was a large part of the problem.

## Way Tuning

We made `VEMB_V16_TLC_PAYLOAD_CACHE_WAYS` configurable and tested `2/4/8`.

### Results

| Ways | ops/sec | p50 | cores | mem |
| --- | ---: | ---: | ---: | ---: |
| `2` | 9,303,191.64 | 0.863 ms | 35.88 | 315/409 MB |
| `4` | 9,280,763.68 | 0.863 ms | 35.94 | 313/409 MB |
| `8` | 9,598,018.03 | 0.839 ms | 35.68 | 313/409 MB |

### Conclusion

- `8-way` performed best under the tested workload.
- It improved clearly over `2-way` and `4-way`.
- We therefore changed the default way count to `8`, while keeping environment
  variable override support.

## Runtime Cache Stats

To observe behavior without waiting for clean process shutdown, we added
periodic runtime stats logging:

- `payload_cache_hit`
- `payload_cache_miss`
- `payload_cache_fill`
- `payload_cache_update`
- `payload_cache_evict`
- `payload_cache_invalidate`
- `payload_cache_busy_skip`
- `payload_cache_inflight_peak`

These logs showed an important pattern:

1. `busy_skip` could be very high even when throughput was relatively good.
2. Lower `busy_skip` did not automatically mean higher throughput.
3. This implied that reducing conflict alone was not sufficient; locality and
   cache management cost also mattered.

## Cache Size Tuning

We next increased payload cache size by introducing
`VEMB_V16_TLC_PAYLOAD_CACHE_SCALE`, and also increased the default scale.

Tested under `ways=8`:

| Scale | ops/sec | p50 | cores | mem |
| --- | ---: | ---: | ---: | ---: |
| `1` | 9,598,018.03 | 0.839 ms | 35.68 | 313/409 MB |
| `2` | 6,074,667.43 | 1.343 ms | 37.49 | 336/432 MB |
| `4` | 5,358,264.84 | 1.543 ms | 37.79 | 382/478 MB |

### Important Finding

Larger cache size reduced conflict indicators significantly:

- `busy_skip` dropped sharply at larger scale

But throughput became worse, not better.

This means:

1. The system was no longer dominated only by conflict.
2. Increasing cache size increased memory footprint and likely worsened
   locality and cache-management cost.
3. For this workload, `scale=1` remained the best point.

## 5M-Range Regression Comparison

Several different implementations fell into the `5M` throughput range. These
results should not be treated as one single failure mode.

| Variant | Description | ops/sec | p50 | cores | mem | runtime stats |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| full `batch + cache` | commit `7f7aa6ec97bd801a8d703b2ad53f73fdcb34f0f4` | 5,274,706.55 | 1.559 ms | 36.54 | 314/412 MB | benchmark result recorded; runtime stat snapshot not separately archived |
| early `cache only` | compile-time disabled `batch`, kept the original cache path | 5,647,015.15 | 1.455 ms | 35.93 | 314/412 MB | benchmark result recorded; runtime stat snapshot not separately archived |
| fixed-entry open addressing | preallocated entries/snapshots, but still shard-wide probe | 4,402,007.59 | 1.903 ms | 37.46 | 315/409 MB | benchmark result recorded; runtime stat snapshot not separately archived |
| enlarged cache | `ways=8`, `scale=4` | 5,358,264.84 | 1.543 ms | 37.79 | 382/478 MB | benchmark result recorded; runtime stat snapshot not separately archived |
| approximate LRU | bucket-local approximate LRU trial | 5,634,076.58 | 1.455 ms | about 37.39 | 313/409 MB | benchmark result recorded; runtime stat snapshot not separately archived |

This comparison makes the pattern clearer:

1. Falling into the `5M` range happened in multiple independent designs.
2. The regression was not caused by one single switch such as `batch` alone.
3. Cache management overhead and locality effects both mattered.

## LRU Rollback Re-Test

After the approximate LRU experiment regressed badly, we rolled that part back
and restored the previously faster `bucket + set-way` structure:

1. removed hit-path access timestamp maintenance
2. removed global access clock bookkeeping
3. restored simple `access_count` based victim selection

Remote host:

- `root@192.168.90.111`
- `/root/szz/codespace/hpc-redis`

Rebuild and test:

```bash
make -C src redis-server USE_UB=yes
VEMB_V16_TLC_PAYLOAD_CACHE_WAYS=8 \
VEMB_V16_TLC_PAYLOAD_CACHE_SCALE=1 \
NUM_KEYS=100000 WORKERS='21:21' TS='64' CS='4' TEST_TIME=30 \
bash hpc_redis_max_tput.sh
```

Result:

| Variant | ops/sec | p50 | cores | mem |
| --- | ---: | ---: | ---: | ---: |
| post-LRU-rollback re-test | 7,126,497.65 | 1.135 ms | 36.52 | 313/408 MB |

This result did not return to the earlier `9.6M` level, which means the LRU
change was not the only factor behind the recent regression.

### Runtime Stats From This Re-Test

The periodic server log for this run showed:

```text
hit=39063913 miss=173278157 fill=16384 update=41545754 evict=30601945 invalidate=0 busy_skip=131816008 inflight_peak=1724
```

Intermediate log snapshots also showed `busy_skip` growing very quickly:

```text
hit=8364181 miss=21471904 fill=16384 update=15138701 evict=6249336 invalidate=0 busy_skip=6416815 inflight_peak=1724
hit=15135967 miss=49978019 fill=16384 update=23205207 evict=13234320 invalidate=0 busy_skip=26856418 inflight_peak=1724
hit=21288166 miss=80137284 fill=16384 update=28995080 evict=18636260 invalidate=0 busy_skip=51225815 inflight_peak=1724
hit=27285149 miss=110680502 fill=16384 update=33885503 evict=23275866 invalidate=0 busy_skip=76878609 inflight_peak=1724
hit=33198316 miss=141832328 fill=16384 update=38002813 evict=27207489 invalidate=0 busy_skip=103913131 inflight_peak=1724
hit=39063913 miss=173278157 fill=16384 update=41545754 evict=30601945 invalidate=0 busy_skip=131816008 inflight_peak=1724
```

This suggested:

1. `busy_skip` remained extremely high in the restored non-LRU version.
2. The currently running code still differed materially from the earlier
   `9.598M` state, or the earlier run was not produced by exactly the same
   effective binary/state combination.

## Current Best Result

Among the tested `cache-only` variants, the best result was:

| Variant | ops/sec | p50 | cores | mem |
| --- | ---: | ---: | ---: | ---: |
| bucket + `8-way`, `scale=1` | 9,598,018.03 | 0.839 ms | 35.68 | 313/409 MB |

Compared with the validated baseline:

- baseline: `11,760,239.13`
- best cache-only result: `9,598,018.03`
- gap: about `-18.4%`

## Design Conclusions

### Batch

The current `payload batch` design should not be enabled in its tested form.

Reasons:

1. It added measurable overhead.
2. Disabling it did not hurt much relative to the full regression.
3. The main optimization opportunity was not in the tested batch path.

### Cache

The `payload cache` direction is still valid, but implementation details matter
heavily.

Good findings:

1. Avoiding direct TCP `writev` to UB memory is still the correct direction.
2. Bucket + set-way organization is much better than shard-wide open
   addressing.
3. `8-way` outperformed `2-way` and `4-way` in the tested workload.

Bad findings:

1. Heap snapshot style cache objects were too expensive.
2. Shard-wide probing was too expensive.
3. Larger cache capacity alone did not help and could significantly hurt.

## Current Recommended Defaults

For the current code state and tested workload:

1. Keep `payload batch` disabled.
2. Use payload cache with:
   - `ways=8`
   - `scale=1`
3. Do not increase cache size by default.

## Suggested Next Steps

Recommended next optimization focus:

1. Reduce per-access cache management overhead.
2. Reduce `inflight` occupation impact on reusable cache entries.
3. Improve update/reuse behavior before trying larger cache capacity again.

Not recommended as the next step:

1. Re-enable current batch path.
2. Further increase cache size by default.
3. Go back to open addressing.

## Related Files

The main experiment touched these code paths:

- `src/vemb_v16_tlc.c`
- `src/vemb_v16_tlc.h`
- `src/vemb_v16_proxy.c`
- `src/vemb_v16_protocol.h`
- `src/vemb_v16_server.c`
- `src/vemb_v16_stats.c`
