# `trigger_proxy_aggregation.go`

Concurrent request generator for triggering proxy/supernode batching on the UB
vector engine.

## What It Does

- Runs `FLUSHALL`
- Prefills a vector key with synthetic basis vectors
- Fires many concurrent `VEMB` or `VSIM` requests against the same key
- Helps exercise proxy aggregation and supernode batch execution paths

The script supports split phases:

- `--phase prefill`: only prepare data
- `--phase query`: only run concurrent requests
- `--phase all`: prefill and then run requests

## Prerequisites

- A Redis server built from this repo
- Vector engine configured as `ub`
- `redis-cli` available, usually at `./src/redis-cli`
- A running server that accepts `VADD`, `VEMB`, and `VSIM`

## Parameters

- `--redis-cli`: path to `redis-cli`
- `--host`: Redis host
- `--port`: Redis port
- `--key`: target vector key
- `--mode`: `vemb` or `vsim`
- `--phase`: `prefill`, `query`, or `all`
- `--concurrency`, `-c`: number of concurrent in-flight requests
- `--requests`, `-n`: total number of requests to send
- `--dim`, `-d`: vector dimension
- `--prefill-count`, `-p`: number of vectors inserted before the test starts
- `--raw`: add `RAW` to `VEMB` requests

## Meaning of `--prefill-count`

This controls the dataset size created before the concurrent phase starts.

- For `vemb`, requests rotate through `item:0 ... item:N-1`
- For `vsim`, it controls how many candidate vectors exist under the key

Larger `--prefill-count` means a larger working set and, for `vsim`, more data
to score.

## Examples

### VEMB

```bash
go run ./benchmark/trigger_proxy_aggregation.go \
  --redis-cli ./src/redis-cli \
  --port 6391 \
  --key myvectors \
  --phase all \
  --mode vemb \
  --raw \
  -c 32 \
  -n 128 \
  -d 4 \
  -p 64
```

### VSIM

```bash
go run ./benchmark/trigger_proxy_aggregation.go \
  --redis-cli ./src/redis-cli \
  --port 6391 \
  --key myvectors \
  --phase all \
  --mode vsim \
  -c 32 \
  -n 128 \
  -d 4 \
  -p 64
```

### Split Insert And Query

Prepare data once:

```bash
go run ./benchmark/trigger_proxy_aggregation.go \
  --redis-cli ./src/redis-cli \
  --port 6391 \
  --key myvectors \
  --phase prefill \
  --mode vemb \
  -d 300 \
  -p 100000
```

Run query load without re-inserting:

```bash
go run ./benchmark/trigger_proxy_aggregation.go \
  --redis-cli ./src/redis-cli \
  --port 6391 \
  --key myvectors \
  --phase query \
  --mode vemb \
  --raw \
  -c 64 \
  -n 10000 \
  -d 300 \
  -p 100000
```

## Output

The script prints:

- setup progress
- run configuration
- summary with success/failure counts
- average latency
- wall-clock QPS
- sample failures when present

## Suggested Log Checks

```bash
tail -n 100 /tmp/redis-vemb-test/redis.log
tail -n 100 /tmp/redis-vsim-test/redis.log
```

Look for:

- `batch-trace`
- proxy batch sizes greater than 1
- supernode worker activity
