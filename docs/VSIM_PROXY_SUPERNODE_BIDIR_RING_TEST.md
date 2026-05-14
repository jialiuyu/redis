# VSIM Proxy/SuperNode Bi-Directional Ring Test

This document validates the current in-progress `VSIM` path built on top of the
bi-directional proxy/supernode ring transport.

Current scope of this test:

- verify command-layer forwarding into proxy
- verify request ring transport
- verify supernode-side similarity computation
- verify response ring transport
- verify proxy-side completion + unblock + reply

Out of scope for this phase:

- top-k
- sorting
- `COUNT` truncation
- `EPSILON` filtering
- large-result transport optimization

The current success criterion is:

- `query_vector + candidate_rows[] -> full score list` returns end to end

## 1. Build

From repo root:

```bash
make -C src redis-server redis-cli USE_UB=yes
```

## 2. Prepare file-backed UB storage

```bash
mkdir -p /tmp/redis-vsim-test
truncate -s 1048576 /tmp/redis-vsim-test/ub.mem
```

## 3. Create config

Create `/tmp/redis-vsim-test/redis-vsim-test.conf`:

```conf
bind 127.0.0.1
port 6392
protected-mode no
daemonize no
pidfile /tmp/redis-vsim-test/redis.pid
logfile /tmp/redis-vsim-test/redis.log
dir /tmp/redis-vsim-test
save ""
appendonly no

vector-engine ub
vector-dimension 4

ub-table-name myvectors
ub-shm-path /tmp/redis-vsim-test/ub.mem
ub-shm-size 1048576
ub-table-offset 0
ub-table-size 0
ub-vector-stride-bytes 0
ub-element-index-mode suffix-numeric
ub-cacheable no
ub-use-ownership no

supernode-workers 1
proxy-batch-limit 1
proxy-time-limit-us 1
proxy-max-supernodes 1
```

## 4. Start Redis

```bash
./src/redis-server /tmp/redis-vsim-test/redis-vsim-test.conf
```

Check startup log:

```bash
tail -n 120 /tmp/redis-vsim-test/redis.log
```

Expected startup signals:

- `Vector engine initialized: UB`
- `SVE Worker 0 started`
- `Proxy aggregator flush thread started`
- `Proxy aggregator result thread started`
- `Ready to accept connections`

## 5. Verify engine state

```bash
./src/redis-cli -p 6392 VENGINE GET
```

Expected:

```text
engine
UB
```

## 6. Prepare a small vector set

```bash
./src/redis-cli -p 6392 FLUSHALL
./src/redis-cli -p 6392 VADD myvectors VALUES 4 1 0 0 0 item:1
./src/redis-cli -p 6392 VADD myvectors VALUES 4 0 1 0 0 item:2
./src/redis-cli -p 6392 VADD myvectors VALUES 4 0 0 1 0 item:3
```

Expected:

- `FLUSHALL` -> `OK`
- each `VADD` -> `(integer) 1`

## 7. Run VSIM

Use a simple query vector:

```bash
./src/redis-cli -p 6392 VSIM myvectors VALUES 4 1 0 0 0 WITHSCORES
```

Current expected semantics:

- command should return successfully
- output should contain all current candidates from `myvectors`
- each candidate should have a score
- exact order is **not** a phase-1 requirement
- top-k behavior is **not** a phase-1 requirement

You can also try:

```bash
./src/redis-cli -p 6392 VSIM myvectors VALUES 4 1 0 0 0
```

## 8. Capture logs

```bash
tail -n 200 /tmp/redis-vsim-test/redis.log
```

## 9. Send me these outputs

Please send back:

1. `make -C src redis-server redis-cli USE_UB=yes`
2. startup terminal output
3. `tail -n 120 /tmp/redis-vsim-test/redis.log`
4. `./src/redis-cli -p 6392 VENGINE GET`
5. `./src/redis-cli -p 6392 FLUSHALL`
6. the three `VADD` outputs
7. `./src/redis-cli -p 6392 VSIM myvectors VALUES 4 1 0 0 0 WITHSCORES`
8. optional plain `VSIM` output without `WITHSCORES`
9. final `tail -n 200 /tmp/redis-vsim-test/redis.log`

## 10. Current known failure signals

If you hit any of these, send them back exactly:

- `ERR UB engine vsim failed`
- `ERR missing VSIM proxy request`
- `ERR missing VSIM completion`
- startup errors related to request/response rings
- hangs after request submission

## 11. What this test proves

If the command returns successfully, this proves:

- `VSIM_RedisCommand -> proxy_submit_vsim` forwarding works
- proxy can collect `candidate_rows[]`
- request packet can carry:
  - `query_vector`
  - `candidate_rows[]`
- supernode can compute similarity for all candidates
- response ring can return `(row_id, score)` data
- proxy can complete request lifecycle and reply to client
