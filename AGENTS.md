# AGENTS.md

## Project

HPC-Redis: Redis fork with high-performance vector computing engine on Huawei Kunpeng ARM64. Adds UB shared-memory bus, ARM SVE2 SIMD acceleration, Proxy+SuperNode batch aggregation, and a three-layer cache system on top of standard Redis.

Branch: `hpc-redis` (16 commits since initial import from 7u5/hpc-redis).

## Build Commands

Run from repo root. Makefile delegates to `src/Makefile`.

```bash
make                                          # standard build
make BUILD_WITH_MODULES=yes                   # include RedisBloom/RediSearch/RedisJSON/RedisTimeSeries/vector-sets
make SANITIZER=address                        # ASan build
make noopt                                    # debug build (-O0)
make valgrind                                 # valgrind-instrumented build
make clean                                    # remove objects, keep deps
make distclean                                # remove everything including deps
```

**Build deps**: Must build `deps/` first on fresh clone. xxhash needs manual build (see `BUILD_GUIDE.md`).

**Platform**: Primary target is openEuler 22.03 LTS-SP3 on ARM aarch64 (Kunpeng 930). GCC 10.3.1+, GNU Make 4.3+. Non-ARM builds work with scalar fallback (SVE code is `#ifdef __ARM_FEATURE_SVE` guarded).

**Build products** in `src/`: `redis-server`, `redis-cli`, `redis-benchmark`, `redis-check-rdb`, `redis-check-aof`. `redis-sentinel` is a symlink to `redis-server`.

**HPC objects in redis-server link**: `vector_engine.o`, `ub_client.o`, `sve_compute.o`, `batch_processor.o` are in `REDIS_SERVER_OBJ` (`src/Makefile:385`). Note: `proxy_aggregator.o`, `supernode_worker.o`, `three_layer_cache*.o` exist but are NOT in the standard link list — used by standalone TLC servers and benchmarks.

## Testing

```bash
make test                                     # full test suite
./runtest                                     # same
./runtest --single unit/type/string           # single test file
./runtest --host <h> --port <p>               # external server
./runtest -v                                  # verbose
./runtest-moduleapi                           # module API tests
./runtest-sentinel                            # sentinel tests
./runtest-cluster                             # cluster tests
```

Tcl 8.5+ required. Test dirs: `tests/unit/`, `tests/unit/type/`, `tests/integration/`, `tests/cluster/`, `tests/sentinel/`, `tests/vectorset/`.

## Architecture

### Traditional Redis (unmodified core)

```
main() [server.c] → initServer() → aeCreateEventLoop() → aeMain()
                                      ↓
                                 event loop [ae.c]
                                      ↓
                   ┌──────────────────┼──────────────────┐
                   ↓                  ↓                  ↓
            accept handler      read handler       time events
            [networking.c]      [networking.c]     [server.c cron]
                   ↓                  ↓
            create client     readQueryFromClient()
                                      ↓
                              processInputBuffer()
                                      ↓
                              processCommand() → command table lookup
                                      ↓
                              addReply*() → output buffer → I/O threads write
```

- **Single-threaded command execution** in main event loop (avoids data races)
- **I/O threads** (`src/iothread.c`) handle socket read/write in parallel; config: `io-threads`, `io-threads-do-reads`
- **Global state**: `struct redisServer server` in `server.h` (~4400 lines, monolithic)

### HPC-Redis Data Flow (vector path)

```
Client (RESP: VADD/VREM/VSIM/VEMB)
  → networking.c (standard RESP parse, unchanged)
  → vector_engine.c (abstraction: selects redis or ub backend)
  → batch_processor.c (50μs batch, up to 1024 reqs, background pthread)
  → proxy_aggregator.c (MurmurHash3 consistent hash, 3000-6000 req/batch, 200μs window, SPSC ring buffer)
  → supernode_worker.c (up to 16 SVE2 workers, Bitmap CAS lock-free, 4TB UB.mem)
  → sve_compute.c + sve2_gemm.h (ARM SVE2 gather+cosine, 1M embedding cache)
  → ub_client.c (user-space zero-copy, libubios.so runtime dlopen)
  → three_layer_cache*.c (HOT 16B → WARM 1200B → COLD append-only)
```

### Three-Layer Cache

| Layer | Capacity | Entry Size | Mechanism |
|-------|----------|------------|-----------|
| HOT   | 128K     | 16B        | Open-addressing hash, 4 entries/cache line, lock-free (ARM LDP/STP atomic) |
| WARM  | 1M       | 1200B      | Bitmap CAS locking, memcpy outside lock |
| COLD  | 64 segs  | 1200B      | Append-only write, offset-based index |

UB variant (`three_layer_cache_ub.c`): All layers backed by UB shared memory, consistent hash ring across 4 UB nodes, SVE2 fused gather+compute.

### Multi-Transport TLC Servers

Standalone cache servers in `src/tlc_*_server.c`, each implementing a different transport backend:
- `tlc_server.c` — base
- `tlc_unified_server.c` — unified transport
- `tlc_aeron_server.c` — Aeron (shared memory SPSC, <0.1μs target)
- `tlc_dpdk_server.c` — DPDK
- `tlc_fc_server.c` — FC (Fibre Channel)
- `tlc_urma_server.c` — URMA (userspace RDMA)
- `tlc_v10_server.c` through `tlc_v14_server.c` — versioned iterations

These are separate binaries (`src/tlc-*-server`), NOT part of redis-server.

## Key Files and Boundaries

### Redis Entry Points (read these to understand the integration)

| File | What |
|------|------|
| `src/server.h:49-52,1921-1923` | `vector_engine_type_t` enum, `vector_engine_type`/`vector_engine_enabled` in redisServer |
| `src/config.c:2623-2632` | `vector-engine` config directive (redis/ub), `updateVectorEngine()` callback |
| `src/Makefile:385` | REDIS_SERVER_OBJ includes HPC objects |
| `src/server.c:7566` | `main()` entry point |

### HPC Components (new code)

| File | Purpose | Linked into redis-server? |
|------|---------|--------------------------|
| `src/vector_engine.c/h` | Abstraction over Redis HNSW vs UB backend | Yes |
| `src/batch_processor.c/h` | 50μs batch collection, background thread | Yes |
| `src/ub_client.c/h` | UB bus client, dlopen libubios.so | Yes |
| `src/sve_compute.c/h` | SVE2 vector ops, 1M embedding cache | Yes |
| `src/sve2_gemm.h` | Header-only SVE2 matrix multiply | Yes (included) |
| `src/sve2_adam.h` | Header-only SVE2 Adam optimizer | Yes (included) |
| `src/proxy_aggregator.c/h` | Smart batching, consistent hash, ring buffer | No (standalone) |
| `src/supernode_worker.c/h` | Bitmap CAS workers, SVE2 gather load | No (standalone) |
| `src/three_layer_cache.c/h` | Three-layer cache base | No (standalone/bench) |
| `src/three_layer_cache_ub.c/h` | UB-backed three-layer cache | No (standalone/bench) |
| `src/aeron_ipc.h` | Aeron-style SPSC ring buffer | Header-only |
| `src/ikcp.c/h` | KCP reliable UDP | Standalone |

### Redis Commands

- Command definitions: `src/commands/*.json` → `utils/generate-command-code.py` → `src/commands.def`
- **No vadd/vrem/vsim/vemb JSON definitions in `src/commands/`** — vector commands come from the external `modules/vector-sets/` module (HNSW-based). The internal `vector_engine.c` is an abstraction that could replace that module's backend.

## Configuration

Key runtime config (redis.conf):
```
vector-engine redis|ub              # selects vector backend
io-threads <N>                      # I/O thread count (1-128)
io-threads-do-reads yes|no          # I/O threads also parse queries
```

HPC-specific config defined in code but requires redis.conf manual addition:
```
proxy-batch-limit 3000
proxy-timeout-us 200
supernode-num-workers 16
sve-vector-bits 256
bitmap-cas-enabled yes
```

## Benchmarks

```bash
cd benchmark && make                 # build all benchmark binaries
make test                            # quick 1M queries
make benchmark                       # full 10M queries
make stress                          # 100M queries
```

Key benchmarks: `three_layer_benchmark_v4` (local), `three_layer_benchmark_ub` (UB), `tlc_*_bench` (per-transport), `tlc_client_bench` (full client test).

Benchmark parse script: `benchmark/parse_tlc_bench.py`.

## Conventions

- C99/C11 (`_Atomic`/`stdatomic.h` required for lock-free code)
- Memory: `zmalloc`/`zfree` wrappers (tracks total memory)
- Logging: `serverLog(LL_NOTICE, ...)` from Redis
- Global state: `struct redisServer server` (extern)
- HPC globals: `global_ub_client`, `global_batch_processor`, `global_proxy_aggregator`, `global_supernode`, `global_sve_context`
- SVE code uses `#ifdef __ARM_FEATURE_SVE` with scalar `memcpy` fallback
- Non-temporal access: `#pragma omp simd` or `svstnt1_*` for streaming stores
- Bitmap CAS: `__atomic_compare_exchange_n` with `__ATOMIC_ACQ_REL`
- License: RSALv2/SSPLv1/AGPLv3 triple-license

## Gotchas

- **No .gitignore for build artifacts**: `src/*.o`, `src/*.d`, `src/redis-*` binaries, `src/tlc-*-server` binaries are committed. Running `make clean` then `git status` shows many deletions.
- **UB libs not in tree**: `ub_client.c` uses `dlopen("libubios.so")` and `dlopen("libsve.so")` — these are external Huawei libraries not in this repo.
- **Ascend toolkit dependency**: Some benchmarks link against `/usr/local/Ascend/ascend-toolkit/` (libascendcl, libacl_cblas) for NPU GEMM offload.
- **committed .o/.d files**: Initial commit included pre-built objects and binaries for ARM. Don't trust these on x86.
- **THREE_LAYER_BENCHMARK_GUIDE.md mentions Paxos/2x3 HA**: This is aspirational architecture documented but not fully implemented in the committed code.
- **WORK_COMPLETED.md date**: Says "2026-02-03" for initial UB+SVE completion, but git history starts 2026-04-08. The code was imported from an external source.

## Reference Docs

- `CLAUDE.md` — comprehensive architecture guide (Chinese + English)
- `README.redis.md` — upstream Redis quickstart
- `BUILD_GUIDE.md` — step-by-step ARM build instructions (Chinese)
- `WORK_COMPLETED.md` — implementation completion report with API signatures
- `THREE_LAYER_BENCHMARK_GUIDE.md` — three-layer cache test plan
- `V10_BENCHMARK_REPORT.md` — Aeron IPC performance results (3.79M QPS single, 11.88M keys/s batch)
- `README_UB_SVE.md` — UB+SVE integration documentation
- `QUICKSTART_UB_SVE.md` — 5-minute deployment guide