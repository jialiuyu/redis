# VEMB FC Clean Proxy Design

## Goal

Replace the historical Redis proxy data plane with a TLC V16-style FC path for
UB `VEMB`.

The clean path deliberately removes these from runtime VEMB:

- `active_bucket_heap`
- `proxy_batch_bucket`
- `proxy_flush_scheduler`
- `proxy_flush_executor`
- `proxy_router`
- bucket mutexes
- response result thread
- completion dict lookup

Legacy source files can remain for old benchmarks, but `USE_UB=yes` runtime no
longer links them.

## Runtime Path

```text
Redis VEMB command
  -> metadata lookup row_id
  -> RedisModule_BlockClient
  -> publish req pointer into per-worker FC board
  -> command returns

Supernode worker
  -> drains its FC board when batch limit or time limit is reached
  -> writes one fc_vemb_packet to its own request ring
  -> reads fc_vemb_packet from request ring
  -> gathers UB vectors for all rows in packet
  -> writes result directly into proxy_vector_request_t
  -> RedisModule_UnblockClient(req->bc, req)

Redis reply callback
  -> replies from req->result_vector
  -> frees req
```

There is no proxy background flush thread and Redis command threads do not
combine. Supernode workers drain their own FC boards, so requests can accumulate
while the worker is busy on the previous packet.

## Packet Contract

The clean proxy uses a pointer-carrying in-process packet:

```c
typedef struct fc_vemb_packet {
    batch_request_header_t hdr;
    struct {
        uint64_t row_id;
        proxy_vector_request_t *owner;
    } requests[];
} fc_vemb_packet_t;
```

This packet is valid only for the in-process Redis plus local supernode layout.
It is intentionally not a network ABI. The upside is that the worker can write
the result directly into the blocked request and avoid a response ring copy plus
completion map lookup.

## FC Board

Each active worker owns one FC board and one SPSC request ring.

```c
typedef struct proxy_fc_board {
    proxy_fc_slot_t *slots;
    proxy_vector_request_t **scratch_owners;
    size_t *scratch_indexes;
    ring_buffer_t *request_ring;
    size_t slot_count;
    size_t scratch_capacity;
    size_t scan_cursor;
    int supernode_id;
    int worker_id;
    atomic_int combiner_lock;
    atomic_size_t pending_count;
} proxy_fc_board_t;
```

Rules:

- slot count defaults to 256, minimum 64
- scratch arrays are allocated once at board init
- hot combine path has no malloc/free
- cleanup touches only claimed slots
- Redis command threads only publish slots; they do not write request-ring packets

## Combine Algorithm

1. Publish request into a hashed empty slot.
2. Supernode worker polls its FC board.
3. Drain when either:
   - pending count reaches `proxy-batch-limit`
   - oldest pending slot waited at least `proxy-time-limit-us`
4. Try TTAS combiner election:
   - relaxed read first
   - CAS only when the lock looks free
5. Scan from `scan_cursor` and claim up to `proxy-batch-limit` pending slots.
6. Write one `fc_vemb_packet_t` to the worker request ring.
7. Clear only claimed slots.

If the request ring is full, the worker keeps claimed slots pending and retries
on a later loop. The Redis command has already returned after publishing its
slot.

## Routing

Initial clean routing is local-supernode only:

```text
worker = row_id % active_fc_workers
```

`active_fc_workers` defaults to `supernode_workers` and can be reduced with
`proxy-vemb-fc-workers` to concentrate load enough to form batches.
Multi-supernode routing can be added later as a separate FC-board sharding layer,
not by reintroducing `proxy_router`.

## Config

Recommended server bench config:

```text
proxy-vemb-submit-mode fc
proxy-batch-limit 32
proxy-time-limit-us 20
proxy-vemb-fc-workers 16
proxy-vemb-fc-slots 0
proxy-vemb-fc-max-scan 0
```

The mode knob remains for config compatibility, but the clean proxy runtime only
implements the FC VEMB path. `VSIM` through proxy is intentionally disabled in
this phase.

## Observability

`INFO` exposes:

```text
FC Proxy Stats
Workers
Active FC workers
FC slots per worker
FC batch limit
FC max scan
FC time limit us
Total requests
Published
Combine rounds
Combined requests
Direct rounds
Batch rounds
Slot busy
Ring busy
Submit failures
Pending total
Pending max now
Pending max observed
Average FC batch size
```

Batch trace logging remains DEBUG-level so benchmarks are not dominated by log
I/O.
