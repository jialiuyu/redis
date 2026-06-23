# VEMB V16 RocksDB COLD Layer Fault Tolerance Design

Date: 2026-06-02

## Purpose

This document describes a RocksDB-backed COLD layer for `vemb_v16` storage, with fault-tolerance semantics for the three primary operations:

| Operation | Meaning | Storage behavior |
|---|---|---|
| `VADD` | Insert or update a vector by key | Writes logical vector data and updates WARM residency |
| `VEMB` | Fetch vector by key, optionally inline | Reads from WARM when resident, otherwise loads from COLD and promotes |
| `VSIM` | Compute similarity against vector by key | Computes from WARM when resident, otherwise loads from COLD and promotes before compute |

The design goal is:

```text
WARM = fast compute working set in shm/UB/mmap memory
COLD = durable full vector set in RocksDB
```

RocksDB should not be part of the hot path for WARM hits. It is used for:

1. Durable writes.
2. WARM miss lookup.
3. Promote from COLD to WARM.
4. Dirty WARM eviction flush.
5. Crash recovery.

## Current Storage Context

Current `vemb_v16_storage` is centered around:

```text
storage ctx
  -> warm_provider
  -> vector_region
  -> tlc/table
```

The current WARM table model is effectively:

```text
key/hash -> row_id -> vector_region[row_id]
```

To support a RocksDB COLD layer and fault tolerance, the table needs persistent-aware row metadata:

```text
key/hash -> metadata
metadata:
  row_id
  state
  version
  dirty
  cold_ref
  last_access
```

`row_id` is a WARM slot only. It must not be treated as the durable identity of a vector. The durable identity is the logical key.

## Data Model

### RocksDB Key

Use the logical vector key as the RocksDB key.

Recommended key encoding:

```text
rocksdb_key = key_hash:u64 + key_len:u32 + raw_key_bytes
```

This keeps lookup compact while preserving the original key for hash collision checks.

Avoid using only `key_hash`, because hash collisions would become data corruption unless an additional collision chain is stored.

### RocksDB Value

Store a self-describing vector payload:

```c
typedef struct vemb_v16_cold_value_header {
    uint32_t magic;
    uint16_t format_version;
    uint16_t dim;
    uint32_t vector_bytes;
    uint32_t flags;
    uint64_t logical_version;
    uint64_t update_time_ns;
    uint32_t header_crc32;
    uint32_t payload_crc32;
} vemb_v16_cold_value_header_t;
```

Value layout:

```text
cold_value = header + float[dim]
```

The header allows recovery code to reject invalid values, detect dim mismatch, and compare versions.

### Column Families

P0 can use a single column family:

| CF | Key | Value |
|---|---|---|
| `vectors` | encoded logical key | header + vector |

P1 can split metadata:

| CF | Purpose |
|---|---|
| `vectors` | Durable vector payloads |
| `meta` | Optional key metadata, tombstones, hotness hints |
| `manifest` | Format version, dim, generation, last recovery state |

For the initial implementation, `vectors` alone is enough.

## WARM Metadata State Machine

Each logical key should have one metadata entry:

```text
EMPTY
WARM
COLD
LOADING
EVICTING
DELETED
```

State meanings:

| State | Meaning |
|---|---|
| `EMPTY` | No known vector for this slot/entry |
| `WARM` | Vector is resident in WARM and can be used directly |
| `COLD` | Vector exists in RocksDB but is not resident in WARM |
| `LOADING` | One thread is loading from RocksDB and promoting to WARM |
| `EVICTING` | One thread is flushing/removing WARM residency |
| `DELETED` | Tombstoned logical key |

Suggested metadata:

```c
typedef struct vemb_v16_row_meta {
    uint64_t key_hash;
    uint64_t version;
    uint32_t key_len;
    uint32_t row_id;
    uint32_t state;
    uint32_t flags;
    uint64_t last_access_ns;
    uint64_t cold_version;
    uint64_t durable_lsn;
} vemb_v16_row_meta_t;
```

Important flags:

```text
DIRTY      WARM has data newer than RocksDB
PINNED     row should not be evicted
LOADING    duplicate of state if bit operations are preferred
```

## Operation Semantics

## VADD

`VADD(key, vector)` inserts or updates the logical vector.

### Durable Mode

Recommended for fault-tolerant baseline:

```text
1. Validate key and vector bytes.
2. Build RocksDB value with version = previous_version + 1.
3. RocksDB Put(key, value), with WAL enabled.
4. Allocate or locate WARM row.
5. Copy vector into vector_region[row_id].
6. Publish metadata:
     state=WARM
     dirty=false
     version=new_version
7. Return OK.
```

Crash behavior:

| Crash point | Recovery result |
|---|---|
| Before RocksDB Put | Old value remains |
| After RocksDB Put, before WARM copy | New value recovered from RocksDB |
| After WARM copy, before metadata publish | New value recovered from RocksDB |
| After metadata publish | New value in WARM and RocksDB |

This mode gives simple recovery semantics. The cost is that write latency includes RocksDB WAL work.

### Async Mode

Throughput-oriented mode:

```text
1. Copy vector into WARM.
2. Publish metadata with dirty=true.
3. Enqueue RocksDB Put to cold flusher.
4. Return OK.
5. Background flusher writes RocksDB and clears dirty when durable.
```

Crash behavior:

```text
Dirty vectors not yet flushed to RocksDB may be lost.
```

Async mode must expose:

```text
dirty_rows
dirty_bytes
cold_flush_queue_depth
durable_version
latest_warm_version
```

## VEMB

`VEMB(key)` returns the vector or an inline vector reference/bytes.

### WARM Hit

```text
1. Lookup metadata by key.
2. If state=WARM:
     read row_id
     return vector_region[row_id]
```

No RocksDB call is made.

### WARM Miss and COLD Hit

```text
1. Lookup metadata by key.
2. If metadata says COLD or key is absent from WARM index:
     enter promote path.
3. Promote loads vector from RocksDB.
4. Install vector in WARM.
5. Return vector from WARM.
```

Promote path:

```text
1. Acquire per-key loading ownership:
     state COLD -> LOADING
2. RocksDB Get(key).
3. If not found:
     state -> EMPTY or DELETED
     return NOT_FOUND
4. Validate header, dim, vector_bytes, crc.
5. Allocate WARM row.
6. Copy vector payload into vector_region[row_id].
7. Publish metadata:
     row_id=row_id
     state=WARM
     dirty=false
     version=cold_version
8. Return vector.
```

If another thread sees `LOADING`, it should wait, retry, or attach to the same load completion rather than issuing another RocksDB Get.

## VSIM

`VSIM(key, query_vector)` computes similarity between the query vector and the stored vector.

### WARM Hit

```text
1. Lookup metadata by key.
2. If state=WARM:
     compute similarity from vector_region[row_id].
3. Return score.
```

This is the current hot path and must remain free of RocksDB operations.

### WARM Miss and COLD Promote

```text
1. Lookup metadata.
2. If not WARM:
     promote from RocksDB using the same path as VEMB.
3. Compute similarity from newly promoted WARM row.
4. Return score.
```

This means a cold `VSIM` has two costs:

```text
RocksDB Get + WARM copy + SVE similarity compute
```

The benchmark/reporting layer should distinguish:

```text
vsim_warm_hit
vsim_cold_hit
vsim_cold_miss
vsim_promote_ns
vsim_compute_ns
```

Without this split, p99 latency analysis will be misleading.

## Promote Concurrency

Promote must handle miss storms.

Problem:

```text
N threads miss the same key at the same time.
Without coordination, all N call RocksDB Get and race to install WARM rows.
```

Recommended approach:

```text
per-key state CAS:
  COLD -> LOADING wins for one thread
  other threads observe LOADING and wait/retry
```

Simple P0 behavior:

```text
LOADING observer:
  spin briefly
  retry WARM lookup
  if timeout, return BUSY or continue with direct RocksDB Get disabled
```

Better P1 behavior:

```text
cold-loader task:
  one loader owns RocksDB Get
  waiters attach to completion
```

## WARM Eviction

RocksDB COLD makes WARM capacity elastic.

Eviction flow:

```text
1. Select victim row from WARM.
2. CAS state WARM -> EVICTING.
3. If dirty:
     RocksDB Put(key, vector)
     clear dirty after success.
4. Mark metadata:
     state=COLD
     row_id=INVALID
5. Release row_id to WARM free list.
```

Victim selection:

```text
P0: no eviction; return capacity error when WARM is full
P1: sampled LRU or clock
P2: hotness-aware eviction using access counters
```

Eviction should prefer:

```text
clean rows
non-pinned rows
low-access rows
rows not currently referenced by active requests
```

## Crash Recovery

With RocksDB, recovery is simpler than a custom append log.

Startup recovery:

```text
1. Open RocksDB.
2. Validate manifest:
     dim
     vector format version
     database generation
3. Initialize empty WARM provider and table metadata.
4. Option A: lazy recovery
     do not load vectors into WARM
     build no full index, use RocksDB Get on WARM miss
5. Option B: eager recovery
     scan RocksDB and rebuild key metadata
     optionally preload hot vectors into WARM
```

Recommended first implementation:

```text
lazy recovery
```

Lazy recovery means:

```text
After restart, WARM starts empty.
VEMB/VSIM miss in WARM and load from RocksDB on demand.
```

This gives fast restart and avoids a long full RocksDB scan.

Eager recovery can be added later for production:

```text
scan RocksDB
load first N hot keys or manifest-listed hot keys
rebuild metadata before accepting traffic
```

## Consistency Modes

Expose a clear write durability mode.

| Mode | VADD return condition | Crash safety | Performance |
|---|---|---|---|
| `warm-only` | WARM metadata published | No COLD safety | Fastest |
| `cold-async` | WARM metadata published and cold flush queued | Loses unflushed dirty rows | High |
| `cold-batch` | RocksDB WriteBatch accepted by flusher interval | Loses last batch window unless WAL sync policy protects it | Medium-high |
| `cold-sync` | RocksDB Put completed with WAL | Durable after return | Lower |

Recommended defaults:

```text
development/benchmark:
  --cold-write-mode cold-async

fault-tolerant baseline:
  --cold-write-mode cold-sync

production balanced:
  --cold-write-mode cold-batch
  --cold-flush-ms 10
  --cold-wal-sync-ms 1000
```

## RocksDB Options

For `dim=300`, each vector payload is about:

```text
300 * sizeof(float) = 1200 bytes
```

Recommended initial options:

```text
create_if_missing = true
create_missing_column_families = true
compression = none or lz4
write_buffer_size = 64MB or 128MB
max_write_buffer_number = 4
target_file_size_base = 64MB
level_compaction_dynamic_level_bytes = true
enable_pipelined_write = true
```

Read optimization:

```text
block cache enabled
bloom filter enabled
cache_index_and_filter_blocks = true
pin_l0_filter_and_index_blocks_in_cache = true
```

Write options by mode:

```text
cold-sync:
  disableWAL = false
  sync = true or controlled by policy

cold-async/cold-batch:
  disableWAL = false
  sync = false
```

Do not disable WAL for fault-tolerant modes.

## Failure Matrix

| Scenario | Expected behavior |
|---|---|
| Process crash after durable `VADD` | Value recoverable from RocksDB |
| Process crash after async `VADD` before flush | Latest WARM-only update may be lost |
| WARM shm lost | Rebuild lazily from RocksDB |
| RocksDB value corrupted | CRC/header validation fails, return error or not_found depending policy |
| Promote interrupted | On restart, WARM is empty; RocksDB value remains authoritative |
| Eviction interrupted before dirty flush | Dirty data may be lost in async mode; durable mode should not have dirty rows |
| Eviction interrupted after dirty flush | RocksDB value is authoritative; WARM row can be discarded |
| Duplicate promote race | Only one loader should install WARM row; waiters retry after state becomes WARM |

## Metrics

Add storage-level counters:

```text
cold_get_ops
cold_get_hit
cold_get_miss
cold_get_ns
cold_put_ops
cold_put_bytes
cold_put_ns
cold_write_batch_ops
cold_write_batch_bytes
cold_fsync_ops
cold_fsync_ns
cold_corrupt_values
cold_recover_ops
cold_recover_ns
```

Add promote counters:

```text
promote_attempts
promote_success
promote_not_found
promote_race_waits
promote_alloc_fail
promote_ns
```

Add WARM residency counters:

```text
warm_hit
warm_miss
warm_rows_used
warm_rows_free
warm_evict_attempts
warm_evict_success
warm_evict_dirty_flush
warm_evict_fail
dirty_rows
```

Add operation split counters:

```text
vadd_warm_update
vadd_cold_put
vemb_warm_hit
vemb_cold_hit
vemb_cold_miss
vsim_warm_hit
vsim_cold_hit
vsim_cold_miss
```

## Implementation Plan

### P0: RocksDB Durable Store

1. Add `vemb_v16_cold_provider` abstraction.
2. Implement RocksDB open/close/get/put.
3. Store `key -> header + vector`.
4. Add `--cold-backend rocksdb`.
5. Add `--cold-path <path>`.
6. Implement `VADD` durable RocksDB Put.
7. Keep WARM allocation model unchanged for now.

P0 does not need eviction. If WARM is full, return capacity error.

### P1: Promote on WARM Miss

1. Add row metadata state.
2. Add WARM miss path for `VEMB` and `VSIM`.
3. Implement RocksDB Get and promote.
4. Add per-key `LOADING` state to suppress duplicate promotes.
5. Add counters for warm/cold split.

### P2: WARM Capacity Management

1. Replace append-only `next_row_id` allocation with a reusable free list.
2. Add clean eviction.
3. Add dirty flush eviction.
4. Add high-watermark and low-watermark settings.
5. Add eviction metrics.

### P3: Async Write and Loader Pools

1. Add cold flusher thread pool.
2. Add write batching.
3. Add cold-loader pool for miss promote.
4. Add completion mechanism for blocked/pending `VEMB` and `VSIM`.
5. Tune RocksDB options for write-heavy and read-heavy workloads.

### P4: Production Recovery and Preload

1. Add manifest column family.
2. Add hot-key preload list.
3. Add startup eager preload mode.
4. Add RocksDB checkpoint support.
5. Add backup/restore hooks.

## Recommended First Baseline

The safest first version:

```text
--cold-backend rocksdb
--cold-write-mode cold-sync
--cold-path /data/vemb_v16_rocksdb
--warm-recovery lazy
--warm-eviction off
```

Semantics:

```text
VADD:
  durable in RocksDB before OK
  also resident in WARM if capacity allows

VEMB:
  WARM hit returns immediately
  WARM miss loads from RocksDB and promotes

VSIM:
  WARM hit computes immediately
  WARM miss loads from RocksDB, promotes, then computes

restart:
  WARM starts empty
  COLD RocksDB is authoritative
```

This baseline gives clear fault tolerance with minimum moving parts. Once correctness is stable, add batching, eviction, and async loader pools.
