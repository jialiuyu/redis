# VEMB V16 Job Pool Slot UB Design

## Goal

Keep the current in-process scheduling path unchanged while allowing
`job_pool->slots` to live in UB-backed memory.

This round does not try to make the full request/return plane attachable across
processes. The only moved data is the per-proxy-worker job slot payload area.

## Current Scope

This design keeps the hot path structure as-is:

1. `job_shard_queues` stay as local `vemb_v16_aeron_ring_t` rings on heap.
2. `job_return_queues` stay as local `vemb_v16_aeron_ring_t` rings on heap.
3. `free_stack` stays on heap and remains proxy-owned allocator state.
4. `job_pool->slots` may be heap-backed or UB-backed depending on manifest.

The intent is to preserve the old single-process QPS characteristics and only
change where the slot payload bytes are allocated from.

## Manifest

Top-level manifest fields:

```yaml
job_plane_provider: ub
job_plane_path: /dev/obmm_shmdev4
job_plane_mmap_offset: 0
```

These field names are kept for compatibility with the previous exploration, but
their meaning is narrower in the current implementation:

- `job_plane_provider` / `job_plane_backend`: backend type for job slot storage
- `job_plane_path`: mapped object path used only for `job_pool->slots`
- `job_plane_mmap_offset`: base offset for slot payload layout

If `job_plane_path` is omitted, all job pools fall back to heap allocation.

## Memory Layout

The mapped region contains only slot payload bytes, packed by:

1. `proxy_io_worker_id`
2. `pool_type`

Offset calculation is:

```text
base_offset = job_plane_mmap_offset
offset(worker, pool_type) =
    base_offset +
    sum(all previous workers, all pool types, aligned pool bytes) +
    sum(current worker, previous pool types, aligned pool bytes)
```

Each pool region size is:

```text
align64(slot_count(pool_type) * slot_stride(pool_type))
```

Current pool types:

- `VEMB_V16_JOB_POOL_READ`
- `VEMB_V16_JOB_POOL_VSIM_KEY_KEY`
- `VEMB_V16_JOB_POOL_INLINE_VECTOR`

## Ownership Model

Runtime ownership remains unchanged:

- proxy allocates and releases job slots
- supernode reads and writes the slot payload through the existing job ref
- free-stack state remains private to the proxy process

Because allocator metadata is still process-local, the current implementation is
suited for the original single-process proxy/supernode runtime, plus UB-backed
slot storage for placement experiments.

## What This Does Not Solve Yet

This is not yet enough for a true split-process proxy/supernode design.

Still missing for that goal:

1. cross-process request queue transport
2. cross-process return queue transport
3. shared or externally coordinated slot allocation ownership
4. removal of direct in-process dependencies such as channel-local completion
   access patterns

## Why This Version Exists

The earlier full `shared-plane` prototype showed that the main regression came
from changing the scheduling plane abstraction itself, not from UB mapping
alone. This reduced design keeps the old queue/free-stack structure intact so we
can isolate the cost of moving only slot payload storage into UB.
