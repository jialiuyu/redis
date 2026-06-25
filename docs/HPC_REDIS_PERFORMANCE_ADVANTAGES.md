# hpc-redis 相比原生 Redis 的性能优势总结

日期：2026-06-23

## 1. 核心判断

`hpc-redis` 的性能优势不是来自对 Redis 通用路径的小幅优化，而是为向量读写和相似度计算场景重做了一条专用数据面：

```text
client / bench
  -> TCP / SHM channel
  -> proxy I/O worker pool
  -> SuperNode worker pool
  -> TLC HOT/WARM/UB payload
  -> completion / response
```

这条路径把 `VADD / VEMB / VSIM` 这类固定形态请求压缩为固定二进制协议、固定向量布局、固定内存访问模式和固定 worker 调度模型。相对原生 Redis，它少走了 RESP 解析、Redis command framework、Redis object/SDS、RedisModule callback、通用 reply 编码、HNSW/attribute/filter 等复杂逻辑。

因此，hpc-redis 的收益本质是：

1. 用专用协议减少通用 Redis 请求成本。
2. 用 TLC/UB 固定布局减少对象和内存管理成本。
3. 用 worker pool 和 SPSC ring 减少线程膨胀和队列调度成本。
4. 用 handle/shared payload 模型减少大 vector 复制和网络返回。
5. 用 SVE/UB/bitmap 等机制贴近固定 300 维 FP32 向量 workload。
6. 用功能取舍换取更短、更稳定的热路径。

## 2. 性能口径说明

分析 hpc-redis 性能时必须区分三类读路径，不能混用 QPS：

| 口径 | benchmark mode | 是否通过 TCP 返回完整 vector | 说明 |
| --- | --- | --- | --- |
| handle/internal read | `vemb-supernode-read` | 否 | SuperNode 内部读或返回 handle，不返回完整 300 维 FP32 payload。该口径 QPS 最高，但不能代表 TCP 全量数据返回。 |
| TCP 全量 vector | `vemb-inline-vector` | 是 | SuperNode 在 completion 中携带 inline vector snapshot，TCP response frame 后追加完整 vector bytes。300 维 FP32 约 1200B。 |
| SHM/UB 本地读 vector | `vemb-read-vector` | 否 | response 返回 handle，client 通过 mmap WARM/UB region 读取 payload。不等价于跨主机 TCP 全量返回。 |
| TCP mixed 读写 | `mixed-80r20w` | TCP 下读侧应 inline vector | 80% 读、20% 写；需要确认对应报告是否为 full-vector 口径。旧报告中部分 mixed 数据仍是 handle 口径。 |

已有报告中的 TCP `vemb-supernode-read` 约 `3.39M QPS` 和 TCP `mixed-80r20w` 约 `1.82M - 1.84M QPS` 需要按上述口径理解：报告明确标注这些结果返回的是 handle 或内部读路径，不应直接作为 TCP 全量 vector 的最终倍数。

## 3. 已确认的主要收益点

| 层面 | hpc-redis 设计 | 相对原生 Redis 的收益 |
| --- | --- | --- |
| 协议 | VEMB V16 固定 binary frame，请求直接携带 `op / key_hash / key / dim / vector`，响应直接携带 `handle / score / vector_bytes`。 | 避免 RESP array/bulk string 解析、命令查表、参数对象化和通用 reply 编码。 |
| 数据模型 | 固定 300 维 FP32 vector，payload 大小稳定，metadata 与 payload 分离。 | 避免 Redis object/SDS/listpack/HNSW 节点等通用结构成本。 |
| VEMB handle | response 可只返回 `{region_id, offset, bytes}`，client 根据 handle 读 WARM/UB payload。 | 对非 TCP 全量模式，避免每次返回 1200B vector，显著减少网络和复制压力。 |
| TCP full-vector | `vemb-inline-vector` 返回完整 vector，但仍使用专用 frame 和 SuperNode snapshot。 | payload 仍要传输，收益小于 handle-only；优势主要来自少对象层、少 reply 构造、调度更稳。 |
| 线程模型 | `proxy I/O worker pool + SuperNode worker pool`，accept/control 只处理连接生命周期。 | 避免 per-channel thread 膨胀，降低高并发调度和 cache footprint。 |
| 慢客户端隔离 | TCP 写不动时进入 per-channel backlog，通过 `EPOLLOUT` flush。 | 慢连接不长期占住 worker，降低尾延迟扩散。 |
| 队列 | proxy 到 SuperNode 使用 shard queue，completion 仍按 channel 边界保存。SPSC ring 采用 64B head/tail、batch poll、acquire/release memory order。 | 比通用锁队列更轻，跨线程转发成本可控。 |
| TLC HOT/WARM | HOT 私有索引，WARM 私有 metadata + shared/UB payload。 | metadata cache-friendly，大块 vector payload 不污染控制结构。 |
| WARM slot 验证 | `owner_generation + write_seq + state` 防 stale handle 和半写 payload。 | 支持轻量并发读写一致性，不依赖 Redis 对象生命周期。 |
| VADD 写路径 | inline vector 直接写 WARM slot；same-key overwrite 尽量保持 location 稳定。 | 减少 SDS/object 分配、Redis dict/object 通用路径和 handle 抖动。 |
| VSIM | SuperNode 拿 handle 后直接对 payload 调用 SVE cosine。 | 避免原生 Vector Sets 的 HNSW/filter/attribute/quantization 通用开销。 |
| UB/SVE | UB mmap、固定 stride、SVE load/store/cosine。 | 固定维度向量可利用更直接的内存和 SIMD 路径。 |
| 扩容 | client-side consistent hash，proxy 不持拓扑、不做二次 hash、不 fan-out。 | 普通请求路径不被复杂拓扑控制污染。 |
| 迁移 | baseline + delta outbox + checkpoint barrier + final fence + lease；非迁移 fast path 通过 counter 跳过慢路径。 | 兼顾扩容一致性和普通读写热路径性能。 |
| 功能取舍 | UB/VEMB 路径不支持部分 Vector Sets 通用能力，如 `FILTER / WITHATTRIBS / EF / REDUCE / CAS / SETATTR` 等。 | 用更窄语义换更短热路径，是性能收益的重要来源。 |

## 4. 已有数据与结论边界

### 4.1 TCP / proxy / SuperNode

已有文档记录：

- TCP `vemb-supernode-read` 最佳约 `3,386,415 QPS`，server 配置为 `--proxy-io-threads 16 --supernode-workers 32`，bench 64 threads。
- TCP `mixed-80r20w` 在主验证配置下约 `1.82M - 1.84M QPS`，`fail=0`。
- 上述报告同时注明：这些结果返回的是 handle 或内部读路径，不是完整 300 维 vector payload。

因此，当前可严谨表述为：

1. handle/internal-read 口径已经证明 proxy/SuperNode/TLC 主链路具备百万级到数百万级 QPS 上限。
2. TCP 全量 vector 口径需要使用 `vemb-inline-vector` 或 TCP 下 full-vector `mixed-80r20w` 单独统计。
3. TCP 全量 vector 的收益预期低于 handle-only，因为 1200B payload 仍要通过 TCP 返回；但它仍可从固定协议、少对象层、worker pool、专用 WARM layout 中获得收益。

### 4.2 线程模型

SHM/Aeron 对照中，64 线程 `mixed-80r20w` 场景下：

- per-channel 约 `1.02M QPS`
- pooled 约 `7.07M QPS`
- 提升约 `+596.4%`

这说明高并发下，长期主路径应保持 pooled-only：固定 proxy I/O worker pool + SuperNode worker pool，比 per-channel thread 更稳定。

### 4.3 Bitmap CAS 测试

远端机器 `root@192.168.90.111:/root/szz/codespace/hpc-redis` 上运行 `benchmark/test_bitmap_cas_optimized.sh`，环境为 `aarch64`，分支 `feat/scale_out`，commit `1e9015f`。测试全部通过。

关键结果：

| 场景 | CAS optimized | CAS bounded | fetch_or 当前实现 |
| --- | ---: | ---: | ---: |
| 2 线程 Partitioned | 5.14 Mops/s, 194 ns | 9.48 Mops/s, 106 ns | 9.51 Mops/s, 105 ns |
| 4 线程 Partitioned | 4.11 Mops/s, 243 ns | 14.02 Mops/s, 71 ns | 16.50 Mops/s, 61 ns |
| 8 线程 Partitioned | 3.85 Mops/s, 260 ns | 8.91 Mops/s, 112 ns | 9.35 Mops/s, 107 ns |
| 16 线程 Partitioned | 5.07 Mops/s, 197 ns | 10.09 Mops/s, 99 ns | 11.57 Mops/s, 86 ns |
| 8 线程 Hotspot | 2.01 Mops/s, 497 ns, 81.39% success | 2.34 Mops/s, 427 ns, 85.65% success | 2.48 Mops/s, 403 ns, 91.04% success |
| 16 线程 Hotspot | 2.28 Mops/s, 439 ns, 77.34% success | 2.69 Mops/s, 372 ns, 78.95% success | 3.08 Mops/s, 325 ns, 86.79% success |

结论：

1. `fetch_or` 在 hotspot 场景下仍然优于两种 CAS 实现。
2. CAS bounded 比无限 CAS 稳，但不足以替代当前 `fetch_or`。
3. 当前生产 bitmap acquire 不建议改成 CAS。
4. 后续收益应来自减少 bitmap word 竞争，而不是把 acquire 原语 CAS 化。

## 5. SuperNode 当前主要瓶颈

绕过 Redis/RESP 后，瓶颈已经转移到 SuperNode 内部逐条固定成本：

```text
job dispatch
  -> HOT/WARM lookup
  -> bitmap acquire
  -> payload slice / vector load / vector copy
  -> VSIM compute 或 VEMB response build
  -> bitmap release
  -> completion publish
```

已知信号：

1. request/response/completion ring 不是主要瓶颈，已有报告中 ring full 多数为 0。
2. `vector_load_avg_ns` 相对稳定，读取 300 维 / 1200B vector 本身不是唯一瓶颈。
3. 高并发下 `bitmap_lock_avg_ns` / `bitmap_unlock_avg_ns` 明显增长，是清晰扩展性损耗。
4. table lookup 约几十 ns 级，在去掉 Redis 通用路径后成为显著固定成本。
5. TCP full-vector 下，SuperNode inline snapshot 的 per-request allocation/copy 也会变成重要成本。

## 6. 后续继续优化的收益点

| 优先级 | 优化项 | 说明 | 预期收益 |
| --- | --- | --- | --- |
| P0 | 补真实 batch 指标 | 增加 batch rounds、avg batch、max batch、HOT hit/miss、WARM probe、bitmap word 冲突、inline payload alloc/copy ns。 | 先量化瓶颈，避免盲调。 |
| P0 | TCP full-vector 去掉 per-request `zmalloc/memcpy` 抖动 | 当前 inline vector snapshot 存在每请求分配和复制，可改为 per-worker payload pool/slab。 | 降低 TCP `vemb-inline-vector` CPU 成本和尾延迟。 |
| P1 | SuperNode batch execute | 现在主要是 batch poll，执行仍偏逐条；后续按 op 分组，批量 lookup、批量 bitmap claim、批量 payload slice。 | 降低逐条固定成本，为 SVE/batch 优化创造入口。 |
| P1 | bitmap word 分组 | batch 内按 bitmap word/cacheline 分组处理。 | 减少同一 atomic word 的反复竞争。 |
| P1 | batch 内重复 key/slot 去重 | 同一批重复 key 或 slot 只 claim / load 一次。 | 降低热点 key 放大成本。 |
| P1 | VEMB optimistic read | 用 `write_seq / owner_generation` 双读验证，成功则绕过 bitmap lock，失败再 fallback。 | 读多场景可能显著减少 bitmap lock/unlock。 |
| P1 | HOT/WARM lookup 优化 | 更细 probe 统计、prefetch 调整、per-worker tiny cache、高频 key cache。 | 降低 table lookup 固定成本。 |
| P1 | VADD 非迁移 fast path 收敛 | migration inactive 时尽量完全跳过 delta/outbox/remote-meta 慢路径判断。 | mixed 写入场景降低普通写尾部成本。 |
| P2 | VSIM norm 预计算 | VADD 写入时维护 norm，VSIM 时少算一次 norm。 | 提升 key-key / inline VSIM compute。 |
| P2 | remote meta lookup batch 化 | 跨 SuperNode VSIM 的 key2 remote directory 查找批量化。 | 降低跨 owner VSIM 查询成本。 |
| P2 | VADD payload descriptor/staging | 大 vector 不在 request/job ring 中多次复制，改用 client-side staging 或 descriptor。 | 降低写路径大 payload 复制压力。 |
| P2 | COLD / WARM-first overflow 完整化 | 完善容量溢出、恢复、COLD append/replay。 | 提升容量和可靠性；短期吞吐收益低于热路径优化。 |

## 7. 推荐结论

当前 hpc-redis 相比原生 Redis 的性能优势可以归纳为：

1. **专用协议胜过通用 RESP**：固定 frame 减少解析、对象和 reply 编码成本。
2. **专用向量数据面胜过通用 key-value/object path**：TLC HOT/WARM/UB 对固定 vector 更友好。
3. **handle/shared payload 胜过每次完整返回**：非 TCP full-vector 场景收益最大。
4. **pooled worker 胜过 per-channel thread**：高并发下调度、cache 和慢客户端隔离更稳定。
5. **SuperNode 内部瓶颈已从 Redis 路径转移到 lookup/bitmap/copy/compute**：下一轮优化应聚焦批量执行、少锁、少分配、少复制。
6. **bitmap 不应 CAS 化**：远端 microbench 显示 `fetch_or` 仍是当前更好的默认实现；优化重点应是降低 bitmap word 竞争。

后续性能提升主线建议保持为：

```text
量化 batch 与 bitmap 冲突
  -> 去掉 full-vector per-request allocation
  -> SuperNode batch execute
  -> bitmap word 分组 / 重复 key 去重
  -> VEMB optimistic read
  -> VSIM / remote meta / VADD payload 继续批量化
```

## 8. 2026-06-24 P0/P1 落地与远端实测

远端机器：

- `root@192.168.90.111:/root/szz/codespace/hpc-redis`
- server 编译命令：`make -B -C src USE_SVE=yes vemb_v16_server`
- server 编译参数确认包含：`-DUSE_SVE -DUSE_ARM_SVE -march=armv8.2-a+sve`
- bench 编译命令：`make -B -C benchmark USE_SVE=yes vemb_v16_bench`
- benchmark 参数：`dim=300`，`prefill=8192`，`keyspace=8192`，`ops/thread=2000`，`threads=16`，`pipeline=16`
- server 参数：`proxy_io_threads=8`，`supernode_workers=16`，`max_vectors=32768`

### 8.1 已落地 / 已验证点

| 优先级 | 点位 | 处理结果 | 结论 |
| --- | --- | --- | --- |
| P0 | `publish_request_job()` 去掉每请求 heap job | 将 `zcalloc(sizeof(vemb_v16_vadd_job_t)) + zfree` 改为栈对象；`publish_shard_job()` 同步 copy 到 ring slot，栈对象生命周期安全。 | TCP inline 单轮曾从 `984,288 QPS` 到 `1,011,891 QPS`，约 `+2.80%`；mixed 单轮 `-0.56%` 且有 1 个失败，收益不稳定。 |
| P0 | TCP full-vector inline payload pool/slab | 尝试全局固定槽池替代 `zmalloc/zfree`。 | 实测明显负收益，已回退；全局 cursor/CAS 和 5MB 槽轮转比 allocator thread-cache 更差。 |
| P1 | VADD 单 owner fast path | 当 `remote_meta_view_count <= 1` 时，VADD 成功后跳过 `enqueue_remote_meta_publish()`。 | mixed 写入中 `remote_meta async_enqueue` 从 `6400` 降到 `0`；同轮保守收益约 `+1.97%`，另一轮为 `+6.38%`。 |
| 测试修复 | Aeron epoll worker 过滤条件 | epoll proxy I/O worker 原先只把 TCP channel 视为 active，导致 Aeron channel 分配后不 poll SHM request ring。修复后 Aeron `vemb-handle` 可跑通。 | Aeron 从 prefill 第 0 条超时变为可完成 `32000` 请求；这是本轮 Aeron 数据的前置修复。 |

### 8.2 具体数据

| 版本 | Aeron `vemb-handle` | TCP `vemb-inline-vector` | TCP `mixed-80r20w` |
| --- | ---: | ---: | ---: |
| SVE baseline，Aeron 修复前 | prefill fail | `984,288.48 QPS` | `971,123.17 QPS` |
| job 栈对象 | prefill fail | `1,011,891.08 QPS`，`+2.80%` | `965,696.44 QPS`，`-0.56%`，`fail=1` |
| job 栈对象 + payload 全局池 | prefill fail | `844,916.17 QPS`，`-14.16%` | `831,658.91 QPS`，`-14.36%` |
| Aeron 修复 + heap job + remote-meta publish 临时基线 | `1,363,982.01 QPS`，prefill `79.939s` | `996,938.84 QPS` | `930,506.54 QPS` |
| 最终版：Aeron 修复 + job 栈对象 + 单 owner remote-meta skip | `1,252,891.34 QPS`，prefill `20.921s` | `977,207.79 QPS` | `948,822.95 QPS` |

最终版相对同轮临时基线：

- Aeron `vemb-handle`：run QPS `-8.14%`，但 prefill 从 `79.939s` 降到 `20.921s`，耗时下降约 `73.8%`。Aeron run QPS 单轮波动较大，应继续用多轮 median + CPU pin 复测。
- TCP `vemb-inline-vector`：`-1.98%`。该模式无 VADD 写入，remote-meta fast path 不生效；收益主要只能来自 job 栈对象，本轮未稳定体现。
- TCP `mixed-80r20w`：`+1.97%`。另一轮最终版 mixed 为 `989,846.68 QPS`，相对同一临时基线为 `+6.38%`；保守结论是该点有正收益，但需要多轮 median 固化。

### 8.3 本轮结论

1. Aeron 当前不是性能优化问题，而是数据面轮询 bug：修复 epoll worker channel 过滤后，`vemb-handle` 可以正常跑通。
2. TCP full-vector 的 per-request payload allocation 不能用简单全局槽池优化；全局原子与冷缓存轮转会吃掉收益，下一版应做 per-worker / per-channel LIFO cache 或直接保留 allocator。
3. 单 owner 下跳过 VADD remote-meta publish 是 mixed 写入路径的有效小优化，能直接消除 `6400` 次 async enqueue。
4. job 栈对象是低风险改动，但在当前 16 线程 / pipeline 16 参数下收益受噪声影响，建议后续用更长 `ops/thread` 和多轮 median 重测。
5. P1 的 batch execute、bitmap word 分组、重复 key 去重、optimistic read 尚未落地；本轮数据里 bitmap lock 仍为 0，说明当前 full-vector/mixed 参数不是验证 bitmap 优化的好 workload，需要专门 hot-key / slot 冲突压测。
