# VEMB proxy/supernode 消息瘦身设计

日期：2026-07-06

本文从 `docs/HPC_REDIS_PERFORMANCE_ADVANTAGES.md` 的 `3.6.1` 拆出，专门说明 `proxy -> SuperNode -> proxy` 热路径上的内部消息裁剪方向。

## 1. 背景

当前 `proxy -> SuperNode -> proxy` 的内部消息传递，主要依赖 `job_shard_queue` 和 `completion_ring` 上的整 slot `memcpy`。这条链路的开销不来自 `shm` 映射本身，而是来自“大而全结构体”的 copy-in / copy-out 模型：producer 把整消息拷进 ring，consumer 再把整消息拷出到本地 batch buffer。

当前热路径里最值得先缩小的两个结构是：

| 结构 | 当前特点 | 热路径问题 |
| --- | --- | --- |
| `vemb_v16_vemb_job_t` | 直接复用 `vemb_v16_job_base_t`，同时携带 `key`、`key2`、`topology_epoch`、`dim`、`vector_bytes`、`vector_offset_or_staging_offset` 等字段 | `vemb-handle` / `vemb-inline` 热读并不需要其中很多字段，但 ring 仍按整 slot copy |
| `vemb_v16_completion_t` | 把 handle、inline payload、VSIM score、redirect 等不同语义压进同一份 completion | `vemb-handle` completion 也要背 `inline_vector`、`score`、`redirect_owner` 等非必要字段 |

## 2. 优先级判断

建议的落地顺序是：**先缩小热路径消息，再评估 queue 只传 `slot-id` / 小 descriptor**。

原因是当前 ring 仍是 fixed-slot copy 语义，如果先不拆结构，直接引入 `slot-id` / pool，只是把生命周期管理提前复杂化；而先把消息裁小，可以先用较低风险拿到一轮 copy 和 cache footprint 收益。

## 3. Job 侧瘦身

`job` 侧建议先做三件事：

1. **按 op 拆消息，而不是所有路径共用 `vemb_v16_job_base_t`**
   `vemb-handle` / `vemb-inline` 应该先拆出独立的 read job，避免继续携带 `key2`、staging offset、写侧/迁移专用字段。
2. **融合 `op | flags`**
   和 `req_t` 一样保留一个 `op_flags` 字节，减少热路径消息头宽度，也让内部消息与 wire/request 口径更一致。
3. **把 `key` 改成尾部变长数组**
   当前 `VEMB_V16_MAX_KEY_LEN=128`，但大量真实 key 远小于 128B。若内部 read job 继续使用固定 `char key[128]`，即使语义字段减少，ring copy 仍会被最大 key 长度放大。更合适的方向是 `char key[]` 配合显式 `encoded_len` / `slot_len`。

一个更适合后续实现的 read job 方向可以概括为：

```text
req_id
channel routing info
key_hash
op_flags
key_len
key[]
```

也就是说，先把以下字段从 `vemb-handle` / `vemb-inline` 热路径 job 中剥离出去：

- `key2_len`
- `key2_hash`
- `key2[]`
- `topology_epoch`
- `dim`
- `vector_bytes`
- `vector_offset_or_staging_offset`

这些字段可以保留在写路径、`VSIM_KEY_KEY` 或更专用的 job 结构里，而不必继续让普通 read job 承担。

## 4. Completion 侧瘦身

`completion` 侧也建议按语义拆分：

1. **`vemb-handle` completion 只保留 handle 所需字段**
   即 `status/op/flags/req_id + {region_id, local_slot, vector_bytes, offset, owner_generation}` 这一类最小集合。
2. **`inline` completion 单独携带 payload 指针 / bytes**
   `inline_vector` 和 `inline_vector_bytes` 只对 `TCP vector-inline` 有意义，不应继续让 `vemb-handle` completion 背这两个字段。
3. **`VSIM` completion 单独携带 `score`**
   `score` 只对 `VSIM` 有意义，也不应继续成为所有 completion slot 的固定负担。

如果短期不想把所有 call site 一次性改完，更稳的做法是先引入统一的小 header，再按 op 使用不同 payload：

```text
completion_hdr
  -> handle payload
  -> inline payload
  -> vsim payload
```

## 5. 预期收益

这样做的直接收益不是“逻辑变少”，而是：

1. **queue copy 更小**：`job_shard_queue` 和 `completion_ring` 的整 slot `memcpy` 规模下降。
2. **batch buffer 更紧凑**：batch poll 后本地 scratch/buffer 占用更少 cache line。
3. **热路径字段更少**：`vemb-handle` 不再为 `inline payload` 或 `VSIM score` 付固定布局成本。
4. **为后续 `slot-id` 化铺路**：先把消息语义拆干净，再把 ring 从“传整结构体”改成“传小 descriptor / pool slot id”会更容易。

## 6. 结论

这条优化路径的核心顺序是：

1. 先把 `job` / `completion` 从“大而全共享结构体”拆成按语义裁剪的热路径消息。
2. 再评估 ring slot 是否继续传整消息，还是演进到 `slot-id` / descriptor。

也就是说，`slot-id` 不是不值得做，而是更适合作为第二阶段；第一阶段先把消息本身做瘦，性价比更高、改动风险也更可控。
