# vemb_v16 single-node vsim-inline benchmark analysis

Date: 2026-06-02

## Test Scope

This document summarizes single-node `vsim-inline` benchmark data for `vemb_v16_server`.

Common benchmark parameters:

| Parameter | Value |
|---|---:|
| transport | tcp |
| endpoint | 127.0.0.1:6391 |
| mode | vsim-inline |
| dim | 300 |
| prefill | 65536 |
| ops/thread | 200000 |
| timeout_ms | 30000 |
| pin | no |
| vector_region | /vemb_v16_vectors |
| warm_backend | shm |
| max_vectors | 131072 |

## Server Configurations

| Config | proxy-io-threads | supernode-workers | transport | dim | max-vectors | Description |
|---|---:|---:|---|---:|---:|---|
| S1 | 8 | 32 | tcp | 300 | 131072 | Initial server parallelism |
| S2 | 16 | 64 | tcp | 300 | 131072 | Increased server parallelism |

S1 command:

```bash
./src/vemb_v16_server \
  --transport tcp \
  --tcp-host 127.0.0.1 \
  --tcp-port 6391 \
  --proxy-io-threads 8 \
  --supernode-workers 32 \
  --vector-region /vemb_v16_vectors \
  --warm-backend shm \
  --dim 300 \
  --max-vectors 131072 \
  --loglevel notice
```

S2 command:

```bash
./src/vemb_v16_server \
  --transport tcp \
  --tcp-host 127.0.0.1 \
  --tcp-port 6391 \
  --proxy-io-threads 16 \
  --supernode-workers 64 \
  --vector-region /vemb_v16_vectors \
  --warm-backend shm \
  --dim 300 \
  --max-vectors 131072 \
  --loglevel notice
```

## S1 Thread Scaling, Pipeline 16

| Server | Threads | Pipeline | Requests | QPS | Avg ns/op | Fail | Notes |
|---|---:|---:|---:|---:|---:|---:|---|
| S1 | 4 | 16 | 800,000 | 647,543.42 | 1542.5 | 0 | Low-concurrency baseline |
| S1 | 8 | 16 | 1,600,000 | 956,426.71 | 1044.8 | 0 | Still scaling |
| S1 | 16 | 16 | 3,200,000 | 1,215,763.26 | 822.4 | 0 | Scaling slows down |
| S1 | 32 | 16 | 6,400,000 | 1,460,015.80 | 684.6 | 0 | Near S1 plateau |
| S1 | 64 | 16 | 12,800,000 | 1,539,705.29 | 649.0 | 0 | Pipeline-16 plateau |

Observation: with S1 and `pipeline=16`, throughput reaches about 1.54M QPS at 64 client threads. The 32-to-64 thread step improves QPS by only about 5.4%, suggesting the initial server configuration is close to its effective parallelism limit.

## S1 Pipeline Scaling, Threads 64

| Server | Threads | Pipeline | Requests | QPS | Avg ns/op | Relative to p16 | Fail |
|---|---:|---:|---:|---:|---:|---:|---:|
| S1 | 64 | 16 | 12,800,000 | 1,539,705.29 | 649.0 | 1.000x | 0 |
| S1 | 64 | 32 | 12,800,000 | 1,688,354.42 | 591.9 | 1.097x | 0 |
| S1 | 64 | 64 | 12,800,000 | 1,671,127.88 | 598.0 | 1.085x | 0 |

Observation: `pipeline=32` is the best observed S1 pipeline depth. It improves QPS by about 9.65% over `pipeline=16`. `pipeline=64` regresses slightly, which indicates that deeper outstanding request queues no longer provide useful work and start adding overhead.

## Server Parallelism Impact

| Server | Threads | Pipeline | Requests | QPS | Avg ns/op | Relative to S1 | Fail |
|---|---:|---:|---:|---:|---:|---:|---:|
| S1 | 64 | 32 | 12,800,000 | 1,688,354.42 | 591.9 | 1.000x | 0 |
| S2 | 64 | 32 | 12,800,000 | 2,860,948.14 | 349.1 | 1.695x | 0 |

Observation: increasing server parallelism from `proxy-io-threads=8, supernode-workers=32` to `proxy-io-threads=16, supernode-workers=64` raises throughput from 1.69M QPS to 2.86M QPS, a gain of about 69.5%. This indicates that the earlier ceiling was primarily server-side parallelism, not client pipeline depth.

## S2 Client Thread Scaling, Pipeline 32

| Server | Threads | Pipeline | Requests | QPS | Avg ns/op | Relative to 64T | Fail |
|---|---:|---:|---:|---:|---:|---:|---:|
| S2 | 64 | 32 | 12,800,000 | 2,860,948.14 | 349.1 | 1.000x | 0 |
| S2 | 96 | 32 | 19,200,000 | 3,017,300.44 | 330.9 | 1.055x | 0 |
| S2 | 128 | 32 | 25,600,000 | 2,955,035.28 | 337.9 | 1.033x | 0 |

Observation: with S2, the best observed client setting is `threads=96, pipeline=32`, reaching 3.017M QPS. Increasing to 128 client threads regresses by about 2.1% compared with 96 threads, suggesting the client side has crossed its effective concurrency sweet spot.

## Key Results

| Stage | Server | Client Threads | Pipeline | QPS | Avg ns/op | Interpretation |
|---|---|---:|---:|---:|---:|---|
| Initial low concurrency | S1 8/32 | 4 | 16 | 647,543.42 | 1542.5 | Baseline |
| Initial high concurrency | S1 8/32 | 32 | 16 | 1,460,015.80 | 684.6 | Near S1 pipeline-16 plateau |
| Initial max client threads | S1 8/32 | 64 | 16 | 1,539,705.29 | 649.0 | Pipeline-16 limit |
| Pipeline tuning | S1 8/32 | 64 | 32 | 1,688,354.42 | 591.9 | Best S1 result |
| Over-deep pipeline | S1 8/32 | 64 | 64 | 1,671,127.88 | 598.0 | Slight regression |
| Server parallelism increase | S2 16/64 | 64 | 32 | 2,860,948.14 | 349.1 | Large server-side gain |
| Best observed result | S2 16/64 | 96 | 32 | 3,017,300.44 | 330.9 | Current single-node peak |
| Excess client threads | S2 16/64 | 128 | 32 | 2,955,035.28 | 337.9 | Past client sweet spot |

## Final Baseline

Best observed single-node `vsim-inline` configuration:

```text
server:
  proxy_io_threads=16
  supernode_workers=64

client:
  threads=96
  pipeline=32

result:
  qps=3,017,300.44
  avg_thread_ns/op=330.9
  fail=0
```

## Conclusions

1. `pipeline=32` is the best observed pipeline depth. `pipeline=64` does not improve throughput.
2. Increasing server parallelism from S1 to S2 is the largest observed improvement, raising QPS by about 69.5% under the same client settings.
3. With S2, 96 client threads is the current sweet spot. 128 threads regresses slightly.
4. All observed runs have `fail=0`, ring full counters at 0, and queue depth counters at 0. The bottleneck is therefore not explicit queue capacity or response backpressure. The remaining limits are more likely CPU parallelism, scheduling overhead, cache locality, shared-memory access patterns, and the internal inline-vsim compute path.

## Suggested Next Runs

Recommended fine-grained sweep around the current best point:

```text
threads: 80, 96, 112
pipeline: 24, 32, 48
```

Recommended server-side attribution matrix:

```text
proxy=8,  workers=32   observed: 1.688M QPS at 64T/p32
proxy=16, workers=32
proxy=8,  workers=64
proxy=16, workers=64   observed: 2.861M QPS at 64T/p32
```

The next most useful validation is to repeat the best S2 point with CPU pinning enabled and compare stability and peak QPS.
