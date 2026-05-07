# Architecture Reference

> Agent-optimized: concise, precise file:line refs, no redundant prose.

## Project

HPC-Redis — Redis fork with high-performance vector computing engine for Huawei Kunpeng ARM64. Adds UB shared-memory bus, ARM SVE2 SIMD, Proxy+SuperNode batch aggregation, and three-layer cache on top of unmodified Redis 7.2.

Branch: `hpc-redis`. Target: openEuler 22.03 LTS-SP3 / ARM aarch64 (Kunpeng 930). Non-ARM builds use scalar fallback (`#ifdef __ARM_FEATURE_SVE` guarded).

## Build & Test

```bash
make                                    # standard build
make BUILD_WITH_MODULES=yes             # include RedisBloom/RediSearch/RedisJSON/RedisTimeSeries/vector-sets
make SANITIZER=address                  # ASan
make noopt                              # -O0 debug
make clean / make distclean             # clean (deps preserved / full)

make test                               # full Tcl test suite
./runtest --single unit/type/string     # single test file
./runtest --host <h> --port <p>         # external server
```

Build deps: build `deps/` first. xxhash needs manual build (`BUILD_GUIDE.md`).
Build products in `src/`: `redis-server`, `redis-cli`, `redis-benchmark`, `redis-check-rdb`, `redis-check-aof`.

## Redis Core (unmodified)

```
main() [server.c:7566] → initServer() → aeCreateEventLoop() → aeMain()
                                         ↓
                                    event loop [ae.c]
                                         ↓
                     ┌───────────────────┼───────────────────┐
                     ↓                   ↓                   ↓
              accept handler       read handler        time events
              [networking.c]       [networking.c]      [server.c cron]
                     ↓                   ↓
              createClient()      readQueryFromClient()
                                         ↓
                                 processInputBuffer()
                                         ↓
                                 processCommand() → lookup table
                                         ↓
                                 addReply*() → output buf → I/O threads
```

- Single-threaded command execution in main event loop
- I/O threads (`iothread.c`) handle socket read/write; config: `io-threads`, `io-threads-do-reads`
- Global state: `struct redisServer server` in `server.h`

## HPC Data Flow (Vector Path)

```
Client (RESP: VADD/VREM/VSIM/VEMB)
  → networking.c            (standard RESP parse, unchanged)
  → vector_engine.c         (abstraction: selects redis or ub backend)
  → batch_processor.c       (50μs batch, up to 1024 reqs, background pthread)
  → proxy_aggregator.c      (MurmurHash3 consistent hash, 3000-6000 req/batch, 200μs window, SPSC ring)
  → supernode_worker.c      (up to 16 SVE2 workers, Bitmap CAS lock-free, 4TB UB.mem)
  → sve_compute.c           (ARM SVE2 gather+cosine, 1M embedding cache)
  → ub_client.c             (user-space zero-copy, dlopen libubios.so)
  → three_layer_cache*.c    (HOT 16B → WARM 1200B → COLD append-only)
```

## Redis Core Modifications (minimal)

Only 3 files touched:

| File | Lines | Change |
|------|-------|--------|
| `server.h:49-52` | 4 lines | `VECTOR_ENGINE_REDIS=0`, `VECTOR_ENGINE_UB=1`, `typedef int vector_engine_type_t` |
| `server.h:1921-1923` | 3 lines | Added `vector_engine_type` + `vector_engine_enabled` to `redisServer` |
| `config.c:111-115,2623-2632,3198` | ~15 lines | `vector_engine_enum[]`, `updateVectorEngine()` callback, `createEnumConfig` registration |
| `Makefile:385` | 1 line | HPC objects appended to `REDIS_SERVER_OBJ` |

## Component Inventory

### Linked into redis-server

| Component | Files | Purpose |
|-----------|-------|---------|
| Vector Engine | `vector_engine.c/h` | Abstraction: dispatches to Redis HNSW or UB backend |
| Batch Processor | `batch_processor.c/h` | 50μs batch collection, background pthread |
| UB Client | `ub_client.c/h` | UB bus client, dlopen libubios.so |
| SVE Compute | `sve_compute.c/h` | SVE2 vector ops, 1M embedding cache |
| SVE2 GEMM | `sve2_gemm.h` | Header-only SVE2 matrix multiply |
| SVE2 Adam | `sve2_adam.h` | Header-only SVE2 Adam optimizer |
| Aeron IPC | `aeron_ipc.h` | Header-only SPSC ring buffer |

### Standalone (separate binaries / benchmarks)

| Component | Files | Purpose |
|-----------|-------|---------|
| Proxy Aggregator | `proxy_aggregator.c/h` | Smart batching, consistent hash, ring buffer |
| SuperNode Worker | `supernode_worker.c/h` | Bitmap CAS workers, SVE2 gather, UB.mem |
| Three-Layer Cache | `three_layer_cache.c/h` | HOT→WARM→COLD cache base |
| Three-Layer Cache UB | `three_layer_cache_ub.c/h` | UB-backed variant with SVE2 fused compute |
| TLC Servers | `tlc_*_server.c` (8 files) | Standalone cache servers (base/unified/aeron/dpdk/fc/urma/v10/v13/v14) |
| KCP | `ikcp.c/h` | Reliable UDP |
| URMA | `urma.c/h` | Userspace RDMA |

## Key Public APIs

### Vector Engine (`vector_engine.h`)

```
vector_engine_t          — vtable: init/cleanup/vadd/vrem/vsim/vemb/vcard/vdim/set_config/get_config/get_stats
vector_engine_create(type) → engine ptr
vector_engine_switch(type) → C_OK/C_ERR      (called by config.c updateVectorEngine)
vector_engine_init_from_config()
current_vector_engine    — global engine ptr
```

### Batch Processor (`batch_processor.h`)

```
MAX_BATCH_SIZE=1024, BATCH_TIMEOUT_US=50000, MAX_CONCURRENT_BATCHES=8
batch_processor_init() / batch_processor_shutdown()
batch_submit_vemb_request(key, element, result, timeout_us)
batch_processor_thread(arg)                   — background worker
global_batch_processor                        — global ptr
```

### UB Client (`ub_client.h`)

```
ub_entity_type_t: COMPUTE_NODE / MEMORY_TILE / FABRIC_MANAGER / STORAGE_NODE
ub_message_type_t: MEMORY_LOAD / MEMORY_QUERY / VECTOR_GATHER / VECTOR_SCATTER

ub_client_init() / ub_client_cleanup()
ub_client_connect_fabric_manager()
ub_client_enumerate_entities()
ub_client_load_embedding_table(name, &addr_space)
ub_client_perform_gather_load(addr_space, indices, n, results, dim)
ub_client_send_message(type, payload, size, target_eid)
ub_client_receive_message(&msg, timeout_ms)
ub_mmap_remote_memory(ubas_addr, size, token_id, &local_addr)
ub_unmap_memory(local_addr, size)
global_ub_client                              — global ptr
```

### SVE Compute (`sve_compute.h`)

```
SVE_MAX_VECTOR_LENGTH=256, SVE_EMBEDDING_CACHE_SIZE=1M, SVE_BATCH_SIZE=1024
sve_context_t   — vector_length, max_elements, cache{entries[], rwlock}, has_sve/sve2/bf16, stats

sve_compute_init() / sve_compute_cleanup()
sve_detect_capabilities(ctx)
sve_batch_gather_embeddings(ctx, table, indices, n, dim, results)
sve_batch_scatter_embeddings(ctx, table, indices, values, n, dim)
sve_compute_similarity(ctx, query, table, indices, n, dim, similarities)
sve_streaming_load_f32 / sve_streaming_store_f32
sve_cache_init / sve_cache_destroy / sve_cache_lookup / sve_cache_store
sve_quantize_f32_to_q8 / sve_dequantize_q8_to_f32
sve_pipeline_process_batch(ctx, request, result)
sve_prefetch_embeddings(table, indices, n, dim)
global_sve_context                            — global ptr
```

### SVE2 Header-Only (`sve2_gemm.h`, `sve2_adam.h`)

```
sve2_gemv_f32(x, W, y, K, N)                 — 4-accumulator GEMV micro-kernel
sve2_gemm_f32(A, B, C, M, N, K)              — L3-tiled GEMM (tile_K=256)
sve2_cosine_similarity(a, b, dim)
sve2_fused_gather_similarity(emb, idx, n, dim, query, sim)
sve2_fused_gather_gemm(emb, idx, n_rows, dim, W, N, out)
sve2_adam_update(param, grad, exp_avg, exp_avg_sq, N, cfg) — vectorized Adam step
```

### Proxy Aggregator (`proxy_aggregator.h`) — standalone

```
PROXY_BATCH_LIMIT=3000, PROXY_TIME_LIMIT_US=200, PROXY_MAX_SUPERNODES=150
proxy_aggregator_init(num_supernodes) / proxy_aggregator_shutdown()
proxy_enqueue_request(key, client_ctx, result_buffer, dim)
consistent_hash_init / get_node / add_node / remove_node
ring_buffer_create / push / pop
flush_batch / flush_thread_func
global_proxy_aggregator
```

### SuperNode Worker (`supernode_worker.h`) — standalone

```
SUPERNODE_MAX_WORKERS=16, UB_MEM_SIZE=4TB, SVE_VECTOR_BITS=256
state_bitmap_t — 64B-aligned atomic words, lock-free CAS
ub_memory_space_t — base_addr, physical_base, size, token_id

supernode_init(node_id, num_workers) / supernode_shutdown()
bitmap_try_acquire(bitmap, bit) → CAS 0→1
bitmap_release(bitmap, bit)     → atomic fetch_and, 1→0
ub_mem_init / ub_mem_get_embedding_addr
sve_worker_thread / sve_worker_process_batch
sve2_batch_gather_load / sve2_gather_with_bitmap_check
global_supernode
```

### Three-Layer Cache (`three_layer_cache.h`) — standalone

```
TLC_HOT_CAPACITY=128K (16B/entry), TLC_WARM_CAPACITY=1M (1200B/entry)
TLC_MAX_COLD_SEGMENTS=64 (append-only segments)

hot_index_t   — {uint64_t key; int32_t warm_idx}  (exactly 16 bytes)
warm_entry_t  — {key, uint8_t value[1200], state, ts, ttl, access_count}
cold_record_t — {key, value[1200], offset}

tlc_init(cache, idc, shard) / tlc_destroy(cache)
tlc_get(cache, key, out) / tlc_put(cache, key, val)
tlc_paxos_propose(cache, key, val, sid)
tlc_ha_failover / tlc_ha_recover
cold_append(cold, key, val)

UB variant (three_layer_cache_ub.h) adds:
tlc_sve2_similarity / tlc_sve2_gemm / tlc_sve2_gather
ub_mgr_init(mgr, node_id, num_nodes)
```

## Lock-Free Mechanisms

- **Bitmap CAS** (`three_layer_cache.h`, `supernode_worker.h`): `bmp_lock_acquire` — CAS spin loop `memory_order_acquire`; `bmp_lock_release` — `atomic fetch_and` `memory_order_release`. 64-byte aligned to prevent false sharing.
- **SPSC Ring Buffer** (`aeron_ipc.h`, `proxy_aggregator.h`): `__atomic_load_n`/`__atomic_store_n` with acquire/release semantics on volatile head/tail.
- **SVE2 SIMD**: Guarded by `#ifdef __aarch64__` / `#ifdef __ARM_FEATURE_SVE`. Uses `svld1_f32`, `svmla_f32_m`, `svwhilelt_b32_u64`, `svaddv_f32`. 4-way accumulator unroll in GEMV to hide FMA latency. Non-temporal via `sve_streaming_store`.

## Configuration

Registered in Redis config system (runtime-switchable):
```
vector-engine redis|ub        # selects vector backend
```

HPC-specific params in `redis-ub-sve.conf` (parsed by components, not Redis config system):
```
proxy-batch-limit 3000        proxy-timeout-us 200          proxy-num-supernodes 150
supernode-num-workers 16      supernode-ub-mem-size 4TB
sve-vector-bits 256           sve2-enabled yes              sve-non-temporal-access yes
bitmap-cas-enabled yes        bitmap-size 16777216
ub-firmware-lib /usr/lib/libubios.so
```

## Benchmark

```bash
cd benchmark && make          # build all benchmarks
make test                     # 1M queries (quick)
make benchmark                # 10M queries (full)
make stress                   # 100M queries
```

Key binaries: `three_layer_benchmark_v4` (local), `three_layer_benchmark_ub` (UB), `tlc_*_bench` (per-transport).
Parse results: `benchmark/parse_tlc_bench.py`.

## Conventions

- C99/C11 (`_Atomic`/`stdatomic.h` for lock-free)
- Memory: `zmalloc`/`zfree` (tracks total via `used_memory`)
- Logging: `serverLog(LL_NOTICE, ...)`
- Globals: `server` (Redis), `current_vector_engine`, `global_ub_client`, `global_sve_context`, `global_batch_processor`, `global_proxy_aggregator`, `global_supernode`
- Non-temporal: `#pragma omp simd` or `svstnt1_*`
- License: RSALv2/SSPLv1/AGPLv3

## Gotchas

1. **No .gitignore for build artifacts** — `src/*.o`, `src/*.d`, `src/redis-*`, `src/tlc-*-server` are committed. `make clean` + `git status` shows deletions.
2. **UB libs not in tree** — `ub_client.c` calls `dlopen("libubios.so")` and `dlopen("libsve.so")` (Huawei proprietary, not in repo).
3. **Committed pre-built .o/.d files** — Built for ARM, will NOT work on x86.
4. **Ascend toolkit dep** — Some benchmarks link `/usr/local/Ascend/ascend-toolkit/` (NPU GEMM offload).
5. **Vector commands** — VADD/VREM/VSIM/VEMB are NOT in `src/commands/*.json`; they come from `modules/vector-sets/` (HNSW module). Internal `vector_engine.c` is a backend abstraction.
6. **Paxos/HA aspirational** — `paxos_acceptor_t`, `ha_manager_t` exist in headers but not fully implemented.
7. **Partial stub implementations** — Some UB backend functions (e.g., `ub_engine_vadd`, `ub_engine_vsim`) return stub/mock data. SVE2 math in headers is production-quality.

## Key File Map

### Redis Core (unmodified)
`ae.c` (event loop) · `networking.c` (RESP) · `db.c` (keyspace) · `object.c` (robj) · `rdb.c` · `aof.c` · `replication.c` · `cluster.c` · `sentinel.c` · `config.c` (modified) · `server.h` (modified)

### Data Structures
`t_string.c` · `t_list.c` · `t_set.c` · `t_zset.c` · `t_hash.c` · `t_stream.c` · `sds.c` · `dict.c` · `kvstore.c` · `quicklist.c` · `listpack.c` · `rax.c` · `ebuckets.c` · `fwtree.c`

### HPC New Code
`vector_engine.c/h` · `batch_processor.c/h` · `ub_client.c/h` · `sve_compute.c/h` · `sve2_gemm.h` · `sve2_adam.h` · `aeron_ipc.h` · `proxy_aggregator.c/h` · `supernode_worker.c/h` · `three_layer_cache.c/h` · `three_layer_cache_ub.c/h` · `tlc_*_server.c` · `ikcp.c/h` · `urma.c/h`

### Docs
`AGENTS.md` · `CLAUDE.md` · `BUILD_GUIDE.md` · `WORK_COMPLETED.md` · `THREE_LAYER_BENCHMARK_GUIDE.md` · `V10_BENCHMARK_REPORT.md` · `README_UB_SVE.md` · `QUICKSTART_UB_SVE.md` · `architecture.md` · `architecture_CN.md`