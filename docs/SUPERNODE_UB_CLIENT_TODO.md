## SuperNode UB Client Integration TODO

### Goal

Replace the mock UB memory path in `supernode_worker` with the real `ub_client`
data-plane path, while keeping the current batch scheduler and bitmap-based
concurrency control model intact.

The current state is:

- `supernode_worker` uses anonymous `mmap` as a mock UB backing store.
- `sve_serial_contiguous_read()` reads from a local mock table layout.
- `ub_client` already supports real shmdev mapping, stride-aware table access,
  and gather/contiguous bulk loads.

The target state is:

- `supernode_worker` attaches to a real UB table through `ub_client`.
- worker batch reads use `ub_client_perform_gather_load()`.
- bitmap lock semantics remain in `sve_operation`.
- mock UB mapping code is removed after the real path is validated.

### Scope

In scope:

- `src/supernode_worker.h`
- `src/supernode_worker.c`
- `src/sve_operation.h`
- `src/sve_operation.c`
- optional helper extraction from `src/vector_engine_ub_impl.c`

Out of scope for the first pass:

- changing batch packet protocol
- replacing `key_hash % capacity` with `ub_client_resolve_element_index()`
- changing the shared ring buffer design
- introducing a new retry policy above bitmap `try_acquire`

### Patch Plan

#### Patch 1: Attach real UB address space in SuperNode

Goal:

- stop creating a mock anonymous UB mapping
- make `supernode` hold a real `ub_address_space_t *`

Files:

- `src/supernode_worker.h`
- `src/supernode_worker.c`

Changes:

- include `ub_client.h` in `supernode_worker.h`
- replace `sve_ub_mem_t *ub_mem` in worker context with:
  - `ub_address_space_t *ubas`
  - `size_t vector_dim`
  - `size_t vector_stride_bytes`
  - `uint64_t table_row_capacity`
- replace `sve_ub_mem_t *ub_mem` in `supernode_t` with `ub_address_space_t *ubas`
- add a small ownership flag if needed, for example `int ub_client_owned`
- update `supernode_init()` to:
  - call `ub_client_init(&server.ub)`
  - call `ub_client_load_embedding_table(server.ub.table_name, &global_supernode->ubas)`
  - derive:
    - `vector_dim = server.ub.vector_dimension`
    - `vector_stride_bytes = ubas->vector_stride_bytes`
    - `table_row_capacity = ubas->size / ubas->vector_stride_bytes`
- initialize bitmap size using `table_row_capacity` instead of `SUPERNODE_MAX_EMBEDDINGS`

Notes:

- keep `ub_mem_init()` and `ub_mem_cleanup()` only temporarily if that helps
  reduce churn; they should be removed in a later patch
- do not switch the read path yet in this patch

#### Patch 2: Add a real UB gather + bitmap path in sve_operation

Goal:

- keep bitmap control in `sve_operation`
- move actual data reads to `ub_client_perform_gather_load()`

Files:

- `src/sve_operation.h`
- `src/sve_operation.c`

Changes:

- add a small dedicated SVE operation context instead of passing many loose
  arguments. Do not pass the full `sve_worker_context_t` into `sve_operation`.
- use a dedicated context name that does not clash with the existing
  `sve_context_t` in `sve_compute.h`. A good option is `sve_gather_ctx_t`.
- add a new API, for example:

```c
typedef struct {
    ub_address_space_t *ubas;
    state_bitmap_t *bitmap;
    size_t vector_dim;
    sve_operation_stats_t *stats;
} sve_gather_ctx_t;
```

```c
int sve_gather_read_with_bitmap(sve_gather_ctx_t *sve_ctx,
                                uint64_t *emb_ids,
                                size_t num_ids,
                                float *results);
```

- implementation outline:
  - loop over `emb_ids`
  - `bitmap_try_acquire()` each id through `sve_ctx->bitmap`
  - collect successful ids into a compact `selected_ids[]`
  - collect matching original output slots into `selected_slots[]`
  - zero-fill failed slots immediately
  - call `ub_client_perform_gather_load()` once for the compact successful set
    using `sve_ctx->ubas` and `sve_ctx->vector_dim`
  - scatter gathered vectors back into the original `results`
  - release bitmap bits for successful acquisitions
  - on UB read failure, zero-fill those slots and still release the bits

Notes:

- keep `sve_serial_contiguous_read()` during migration
- do not remove mock helpers in this patch

#### Patch 3: Switch worker batch reads to the real UB path

Goal:

- make `supernode_worker` use the new gather path for real batch reads

Files:

- `src/supernode_worker.c`

Changes:

- in `sve_worker_process_batch()`:
  - compute ids as `key_hash % ctx->gather_ctx.table_row_capacity`
  - allocate results using `ctx->gather_ctx.vector_dim`
  - store `sve_gather_ctx_t gather_ctx` inside `sve_worker_context_t`
  - replace `sve_serial_contiguous_read(...)` with
    `sve_gather_read_with_bitmap(&ctx->gather_ctx, ...)`

Notes:

- keep current skip-on-lock-failure semantics unchanged
- do not add retries in this patch

#### Patch 4: Remove mock UB mapping and stale macros

Goal:

- remove the old mock UB path once real UB reads are stable

Files:

- `src/supernode_worker.h`
- `src/supernode_worker.c`

Changes:

- remove:
  - `SUPERNODE_EMBEDDING_DIM`
  - `SUPERNODE_MAX_EMBEDDINGS`
  - `UB_MEM_BASE_ADDR`
  - `UB_MEM_SIZE`
  - `UB_MEM_PAGE_SIZE`
  - `ub_mem_init()`
  - `ub_mem_cleanup()`
- update stats output to report:
  - `ubas->size`
  - `vector_dim`
  - `vector_stride_bytes`
  - `table_row_capacity`

Notes:

- `SUPERNODE_MAX_WORKERS` can stay for now
- keep the Linux-only CPU affinity guard as-is

### Per-file Detailed Checklist

#### `src/supernode_worker.h`

- [ ] include `ub_client.h`
- [ ] replace mock UB pointer in worker context
- [ ] add runtime UB metadata to worker context
- [ ] replace mock UB pointer in `supernode_t`
- [ ] remove mock UB lifecycle declarations in final cleanup patch

#### `src/supernode_worker.c`

- [ ] remove direct dependence on anonymous `mmap` for UB storage
- [ ] initialize `ub_client`
- [ ] load the configured UB table once
- [ ] derive runtime capacity and dimension from `server.ub` and `ubas`
- [ ] size the bitmap from real UB capacity
- [ ] size worker result buffers from runtime dimension
- [ ] route batch reads to the new `sve_gather_read_with_bitmap()`
- [ ] remove mock UB cleanup path in the final patch

#### `src/sve_operation.h`

- [ ] add `sve_gather_ctx_t`
- [ ] expose a new UB gather + bitmap API
- [ ] keep current bitmap helpers
- [ ] keep current `sve_operation_stats_t`

#### `src/sve_operation.c`

- [ ] implement compact-id gather path via `ub_client_perform_gather_load()`
- [ ] preserve current lock success/failure accounting
- [ ] zero-fill failed or unreadable rows
- [ ] release bitmap bits in all successful-acquire paths
- [ ] keep old contiguous mock path until migration is complete

### Ownership and Lifetime Decision

Before implementation, keep this rule explicit:

- `ub_client` is process-global
- `supernode` should reuse the global client
- `supernode_shutdown()` should not blindly call `ub_client_cleanup()` unless
  it explicitly owns initialization in the current process model

If needed, add one of:

- a reference count inside `ub_client`
- or a local ownership flag in `supernode`

### Configuration Requirements

The real UB path depends on these runtime configs being valid before
`supernode_init()`:

- `ub-table-name`
- `ub-shm-path` or `ub-shm-memid`
- `ub-shm-size`
- `ub-table-offset`
- `ub-table-size`
- `ub-vector-stride-bytes`
- `vector-dimension`
- `ub-element-index-mode`

Add early validation in `supernode_init()` for:

- `server.ub.vector_dimension > 0`
- `ubas->vector_stride_bytes >= vector_dimension * sizeof(float)`
- `table_row_capacity > 0`

### Validation Plan

#### After Patch 1

- `supernode_init()` succeeds with a real configured UB table
- bitmap size matches real UB capacity
- no worker read path changes yet

#### After Patch 2

- unit or targeted test confirms `sve_gather_read_with_bitmap()`:
  - zero-fills failed lock slots
  - zero-fills UB read failure slots
  - releases bitmap bits correctly

#### After Patch 3

- worker batches read real UB data
- lock success/failure stats still look reasonable
- batch throughput is stable

#### After Patch 4

- no remaining dependency on mock UB mapping
- no stale UB mock macros left in supernode code

### Known Risks

- global `ub_client` cleanup ordering may conflict with `vector_engine_ub_impl`
- runtime vector dimension may diverge from old compile-time assumptions
- batch packet `key_hash % capacity` indexing may not match long-term UB index policy
- shared ring buffer remains a separate concurrency concern and is not solved by this migration

### Recommended Commit Sequence

1. `supernode: attach real UB address space via ub_client`
2. `sve_operation: add bitmap-protected UB gather path`
3. `supernode: switch worker batch reads to ub_client gather`
4. `supernode: remove mock UB mmap path and stale macros`
