# VEMB Proxy/SuperNode Bi-Directional Ring Test

This document validates the current in-progress transport refactor where:

- request path uses:
  - `proxy -> request ring -> supernode`
- response path uses:
  - `supernode -> response ring -> proxy`

and where:

- supernode no longer owns blocked-client lifecycle
- proxy result collector owns:
  - completion update
  - `RedisModule_UnblockClient`

## Scope

This test is for the current transport refactor only.

It is specifically checking:

1. UB engine starts successfully
2. proxy starts both:
   - flush thread
   - result thread
3. supernode worker starts successfully
4. `VADD` still works
5. `VEMB` and `VEMB RAW` still reply correctly
6. no regression from the previous one-way request ring design

## 1. Build

From repo root:

```bash
make -C src redis-server redis-cli USE_UB=yes
```

## 2. Prepare file-backed UB storage

```bash
mkdir -p /tmp/redis-vemb-test
truncate -s 1048576 /tmp/redis-vemb-test/ub.mem
```

## 3. Create config

Create `/tmp/redis-vemb-test/redis-vemb-test.conf`:

```conf
bind 127.0.0.1
port 6391
protected-mode no
daemonize no
pidfile /tmp/redis-vemb-test/redis.pid
logfile /tmp/redis-vemb-test/redis.log
dir /tmp/redis-vemb-test
save ""
appendonly no

vector-engine ub
vector-dimension 4

ub-table-name myvectors
ub-shm-path /tmp/redis-vemb-test/ub.mem
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
./src/redis-server /tmp/redis-vemb-test/redis-vemb-test.conf
```

Wait until startup settles, then inspect:

```bash
tail -n 100 /tmp/redis-vemb-test/redis.log
```

Expected startup signals:

- `Vector engine initialized: UB`
- `SVE Worker 0 started`
- `Proxy aggregator flush thread started`
- `Proxy aggregator result thread started`
- `Ready to accept connections`

If any of the following appear, stop and report:

- `Failed to initialize supernode worker`
- `Failed to create request ring`
- `Failed to create response ring`
- `Failed to create result thread`

## 5. Verify engine state

```bash
./src/redis-cli -p 6391 VENGINE GET
```

Expected:

```text
engine
UB
```

## 6. Run VEMB smoke test

```bash
./src/redis-cli -p 6391 FLUSHALL
./src/redis-cli -p 6391 VADD myvectors VALUES 4 1 2 3 4 item:1
./src/redis-cli -p 6391 VEMB myvectors item:1
./src/redis-cli -p 6391 VEMB myvectors item:1 RAW
```

Expected:

- `FLUSHALL` -> `OK`
- `VADD` -> `(integer) 1`
- `VEMB` -> vector values `1 2 3 4`
- `VEMB RAW` -> `fp32`, 16-byte blob, and `"1"`

## 7. Capture logs after test

```bash
tail -n 200 /tmp/redis-vemb-test/redis.log
```

Things to check:

- no `ERR missing VEMB proxy request`
- no `ERR missing VEMB completion`
- no worker-side request/response ring errors
- no response ring reserve/commit failures

## 8. Send back these outputs

Please send me:

1. `make -C src redis-server redis-cli USE_UB=yes`
2. startup terminal output
3. `tail -n 100 /tmp/redis-vemb-test/redis.log` after startup
4. `./src/redis-cli -p 6391 VENGINE GET`
5. `./src/redis-cli -p 6391 FLUSHALL`
6. `./src/redis-cli -p 6391 VADD myvectors VALUES 4 1 2 3 4 item:1`
7. `./src/redis-cli -p 6391 VEMB myvectors item:1`
8. `./src/redis-cli -p 6391 VEMB myvectors item:1 RAW`
9. `tail -n 200 /tmp/redis-vemb-test/redis.log`

## 9. What this test proves

If all commands succeed, it means:

- request ring path is intact
- response ring path is intact
- worker no longer needs direct completion/unblock ownership
- proxy result collector is working
- `VEMB` still completes end to end after the transport split
