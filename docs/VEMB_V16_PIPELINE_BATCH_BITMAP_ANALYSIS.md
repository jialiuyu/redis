# VEMB v16 Pipeline / Batch / Bitmap 压测结论

本文总结 `vemb_v16_bench --mode vemb-supernode-read` 在当前 `client -> proxy -> SuperNode -> proxy -> client` 路径上的压测结果，并梳理下一阶段优化方向。

## 热点路径

当前压测路径已经绕开 Redis command/module/block-client 路径，核心链路为：

```text
client -> proxy -> SuperNode -> completion -> proxy -> client
```

其中：

- client 通过 per-channel request ring 发送请求。
- proxy channel thread 批量 poll client request，再转发到 SuperNode job ring。
- SuperNode worker 执行 VEMB/VADD，其中 `vemb-supernode-read` 会进入 SuperNode 读向量路径。
- SuperNode 将 completion 写回 completion ring。
- proxy 批量 poll completion，再写回 client response ring。
- client 使用 pipeline window 保持多个 outstanding requests。

## 总体架构图

```mermaid
flowchart LR
    subgraph ClientSide["Client / benchmark"]
        C["worker threads<br/>pipeline window"]
        RQ["request ring<br/>per channel"]
        RS["response ring<br/>per channel"]
    end

    subgraph ProxySide["Proxy"]
        PI["channel thread<br/>request batch poll"]
        PO["completion batch poll<br/>response publish"]
    end

    subgraph SuperNodeSide["SuperNode"]
        J["job ring"]
        S["worker<br/>VEMB / VADD"]
        T["table + vector region"]
        B["bitmap"]
        CP["completion ring"]
    end

    C -->|"VEMB / VADD"| RQ
    RQ -->|"poll batch"| PI
    PI -->|"publish job"| J
    J -->|"poll batch"| S
    S -->|"lookup / load"| T
    S -->|"claim / release"| B
    S -->|"publish completion"| CP
    CP -->|"poll batch"| PO
    PO -->|"status / handle"| RS
    RS -->|"batch poll"| C

    classDef client fill:#e8f4ff,stroke:#2563eb,stroke-width:1px,color:#0f172a;
    classDef proxy fill:#fff7ed,stroke:#ea580c,stroke-width:1px,color:#0f172a;
    classDef supernode fill:#ecfdf5,stroke:#16a34a,stroke-width:1px,color:#0f172a;
    classDef ring fill:#f8fafc,stroke:#64748b,stroke-width:1px,color:#0f172a;

    class C client;
    class PI,PO proxy;
    class S,T,B supernode;
    class RQ,RS,J,CP ring;
```

## VEMB 细化时序图

```mermaid
sequenceDiagram
    autonumber
    participant C as Client worker
    participant Req as Request ring
    participant P as Proxy channel thread
    participant Job as Job ring
    participant S as SuperNode worker
    participant T as Table / vector region
    participant B as Bitmap
    participant Comp as Completion ring
    participant Resp as Response ring

    loop keep pipeline window
        C->>Req: publish VEMB(key, req_id)
    end

    P->>Req: poll request batch
    loop each request
        P->>Job: publish VEMB job
    end

    S->>Job: poll job batch
    loop each VEMB job
        S->>T: lookup key -> row_id
        S->>B: claim row
        S->>T: load 300-dim vector
        S->>B: release row
        S->>Comp: publish completion(status, handle)
    end

    P->>Comp: poll completion batch
    loop each completion
        P->>Resp: publish response(req_id, status)
    end

    C->>Resp: poll response batch
    C->>C: retire completed req_id and refill window
```

## 当前架构限制 / 适用边界

本报告中的 QPS 结果只代表当前 standalone vemb_v16 数据面，不等同于完整 multi-SuperNode 生产架构。

当前实现的主要限制如下：

- 当前是单进程内的 local dataplane：client channel 进入 proxy channel thread 后，转发到本地 SuperNode worker；不是独立 proxy 进程面对多个独立 SuperNode 进程。
- 当前没有引入 `consistent hash ring`，因此没有 `vector_key -> SuperNode shard` 的分片路由。
- 当前没有多 SuperNode shard 的 table/region 隔离；所有 worker 共享同一个本地 table 和同一个 vector region。
- 当前没有实现多 SuperNode 下的 request fan-out、completion fan-in、completion demux 和跨 SuperNode ring 扫描。
- 当前没有 hash ring 变更、SuperNode 增删、rebalance、route snapshot 或只读路由快照等生产控制面。
- 当前没有跨 SuperNode 的故障处理、超时重试、背压迁移和降级路由。
- 当前 VADD/VEMB 在同一个本地表中完成，尚未验证同一个 `vector_key` 在多 SuperNode 分片下的写读一致路由。
- 当前 vector region 是 POSIX shm mmap 出来的本地共享内存，不是真实 `/dev/obmm_shmdev*` UB region。
- 当前 bench 使用 vemb_v16 专用 channel 协议，不经过 Redis RESP/TCP、Redis command framework、module callback 或 blocked-client/unblock 路径。
- 当前 response 是 status/handle/offset，不返回完整 1200B vector payload；这与 tlc_v16 zero-copy GET 的轻量 response 对齐，但不是完整 payload 回包测试。

因此，本报告更适合回答：

```text
在去掉 Redis 路径后，client -> proxy -> local SuperNode -> completion -> proxy -> client 这条数据面能达到什么吞吐。
```

它还不能回答完整生产问题：

```text
client -> proxy -> consistent hash ring -> 多 SuperNode shard -> completion fan-in -> proxy -> client
```

后续如果加入 multi-SuperNode，需要单独重测 hash lookup、跨 SuperNode ring 扫描、completion demux、shard table 访问和 route snapshot 的额外成本。

## 压测数据结论

### pipeline=1 与 pipeline=16

小规模 `prefill=1000, ops=10000, threads=1,2` 下，pipeline 收益非常明显：

| threads | pipeline=1 QPS | pipeline=16 QPS | 提升 |
|---:|---:|---:|---:|
| 1 | 363,517 | 1,526,226 | 4.2x |
| 2 | 768,053 | 3,047,755 | 4.0x |

同时 `response_empty_polls` 从千万级下降到十万级以内，说明 client 不再逐条同步等待 response，proxy/SuperNode batch poll 才能吃到连续请求。

### pipeline=16 真实规模结果

`prefill=65536, ops/thread=200000, pipeline=16`：

| threads | QPS | avg/op | response_empty_polls | bitmap_lock_avg_ns | bitmap_unlock_avg_ns | vector_load_avg_ns |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,813,368 | 548.2ns | 7,183,181 | 45.3 | 34.1 | 48.2 |
| 2 | 3,959,647 | 251.9ns | 6,641,809 | 89.4 | 35.7 | 47.7 |
| 4 | 7,076,081 | 140.6ns | 24,324,647 | 119.6 | 46.5 | 49.7 |
| 8 | 6,659,660 | 149.6ns | 393,046,455 | 238.2 | 109.7 | 50.4 |
| 16 | 7,351,792 | 135.7ns | 3,491,947,591 | 381.4 | 226.2 | 49.8 |
| 32 | 8,477,727 | 117.6ns | 16,926,804,652 | 663.6 | 398.2 | 50.2 |

结论：

- 当前峰值约 `8.5M QPS`。
- `full vemb_ring / response_ring / completion_ring = 0`，ring 容量与 publish full 不是瓶颈。
- `request_publish_spins=0`，client 发请求没有被 request ring 阻塞。
- `vector_load_avg_ns` 稳定在 `48-52ns`，读 300 dim / 1200B 向量本身不是瓶颈。
- 高并发下 `bitmap_lock_avg_ns` 和 `bitmap_unlock_avg_ns` 明显增长，是最清晰的扩展性损耗信号。

### pipeline=32 对比

`pipeline=16 -> pipeline=32` 收益很小：

| threads | pipeline=16 QPS | pipeline=32 QPS | 变化 |
|---:|---:|---:|---:|
| 4 | 7,076,081 | 7,267,629 | +2.7% |
| 8 | 6,659,660 | 6,626,499 | -0.5% |
| 16 | 7,351,792 | 7,435,439 | +1.1% |
| 32 | 8,477,727 | 8,614,195 | +1.6% |

结论：`pipeline=16` 已经基本覆盖 client 等待开销，继续加深 pipeline 不是主要优化方向。默认建议继续使用 `pipeline=16`。

### mixed-80r20w 对标 tlc_v16

`mixed-80r20w` 模式在同一个 worker、同一个 channel、同一个 pipeline 内按 `4:1` 发送请求：

```text
80% VEMB_SUPERNODE_READ
20% VADD_INLINE
```

VADD 更新已有 `prefill` key，不持续插入新 key。这个语义更接近 tlc_v16 的 `80R/20W`，也避免表容量成为干扰项。

`prefill=65536, ops/thread=200000, pipeline=16`：

| threads | QPS | avg/op | sent_vemb | sent_vadd | write_ratio |
|---:|---:|---:|---:|---:|---:|
| 4 | 5,459,548 | 181.3ns | 640,000 | 160,000 | 20.00% |
| 8 | 6,358,668 | 156.6ns | 1,280,000 | 320,000 | 20.00% |
| 16 | 8,709,330 | 114.4ns | 2,560,000 | 640,000 | 20.00% |
| 32 | 9,731,370 | 102.5ns | 5,120,000 | 1,280,000 | 20.00% |

服务端计数完全对齐：

| threads | total | vemb | vadd | not_found | completed |
|---:|---:|---:|---:|---:|---:|
| 4 | 800,000 | 640,000 | 160,000 | 0 | 800,000 |
| 8 | 1,600,000 | 1,280,000 | 320,000 | 0 | 1,600,000 |
| 16 | 3,200,000 | 2,560,000 | 640,000 | 0 | 3,200,000 |
| 32 | 6,400,000 | 5,120,000 | 1,280,000 | 0 | 6,400,000 |

对比 tlc_v16 之前的 `80R/20W = 3.99M QPS`，当前最高 `9.73M QPS`：

```text
9.73M / 3.99M = 2.44x
```

结论：

- 如果目标是 tlc_v16 80R/20W 级别的 key/value IPC 调度吞吐，当前已经达标。
- 当前 `client -> proxy -> SuperNode -> completion -> proxy -> client` 多一层 proxy，仍超过 tlc_v16 80R/20W 基准。
- 当前 response 是 status/handle，不返回完整 1200B payload；这与 tlc_v16 zero-copy GET 的轻量 response 对齐。
- 后续重点不再是证明吞吐是否能达到 tlc_v16，而是优化高并发下 SuperNode bitmap 与 batch execute 的效率。

mixed 模式下的瓶颈信号仍然集中在 bitmap：

| threads | bitmap_lock_avg_ns | bitmap_unlock_avg_ns | vector_load_avg_ns | completion_publish_avg_ns |
|---:|---:|---:|---:|---:|
| 4 | 110.9 | 42.1 | 57.9 | 90.6 |
| 8 | 214.6 | 110.4 | 66.9 | 114.4 |
| 16 | 329.0 | 206.1 | 63.6 | 123.6 |
| 32 | 734.3 | 371.1 | 63.6 | 115.3 |

`vector_load_avg_ns` 和 `completion_publish_avg_ns` 相对稳定，`bitmap_lock_avg_ns` / `bitmap_unlock_avg_ns` 随并发明显升高。

### pin=yes 对比

`--pin yes` 已生效后的结果：

| threads | no pin QPS | pin=yes QPS | 变化 |
|---:|---:|---:|---:|
| 4 | 7,076,081 | 7,139,189 | 基本持平 |
| 8 | 6,659,660 | 6,639,105 | 基本持平 |
| 16 | 7,351,792 | 2,518,011 | 明显下降 |
| 32 | 8,477,727 | 2,459,573 | 明显下降 |

结论：当前 pin 策略是负收益，主压测暂时不要使用 `--pin yes`。

原因判断：

- 目前只 pin client worker，没有同时 pin proxy channel thread 和 SuperNode worker。
- client/server 可能抢同一批 CPU，调度器无法自动避让。
- 16/32 线程时 QPS 大幅下降，但 bitmap lock 并没有同步变坏，说明主要问题是 CPU 绑定布局，而不是 bitmap 本身。

## 当前瓶颈判断

当前链路已经不是 Redis/RESP/TCP 路径瓶颈，也不是 request/response ring full 瓶颈。主要瓶颈集中在 SuperNode 热路径的逐条固定成本：

```text
SuperNode poll VEMB job
  -> key/table lookup
  -> bitmap lock
  -> vector load
  -> bitmap unlock
  -> completion publish
```

从采样数据看：

- table lookup 大约 `65-80ns`。
- vector load 大约 `50ns`。
- completion publish 大约 `80-115ns`。
- bitmap lock/unlock 会随并发明显恶化，32 线程可到 `600ns+ / 400ns+`。

因此下一阶段重点应从 client pipeline 转向 SuperNode 内部 batch 执行与 bitmap 读路径优化。

## 后续优化方向

### P0: 补 batch 真实度量

当前 stats 统计的是 item 数，例如 `supernode_vemb_poll=6400000`，但不知道真实 poll 轮次和 batch size。需要增加：

- `proxy_request_poll_rounds`
- `proxy_request_batch_max`
- `proxy_completion_poll_rounds`
- `proxy_completion_batch_max`
- `supernode_vemb_poll_rounds`
- `supernode_vemb_batch_max`
- `supernode_completion_publish_rounds`

输出时计算：

```text
avg_batch = items / rounds
max_batch = observed max batch size
```

这些指标用于判断 batch poll 是否真的形成批量，而不是 API 上支持 batch、实际仍接近单条。

### P1: SuperNode batch 执行

目标不是只 batch poll，而是 batch execute：

```text
poll N jobs
  -> 批量 lookup row_id / offset
  -> 批量处理 bitmap claim
  -> 批量 vector load / compute
  -> 批量 publish completion
```

预期收益：

- 降低循环分支和函数调用固定成本。
- 降低 ring poll/publish 的 per-item 成本。
- 为后续 bitmap word 分组和 SVE 批处理创造入口。

### P1: bitmap 读路径优化

VEMB 是读，VADD 是写。bitmap 不能直接删除，因为它承担 VADD/VEMB 并发保护。但当前 VEMB 每条都做较重原子 lock/unlock，高并发下 cacheline 抖动明显。

可选方向：

- read-mostly 轻量 claim：VEMB 使用更轻的共享读标记，VADD 使用写保护。
- batch 内按 bitmap word 分组：同一个 bitmap word/cacheline 的 row 集中处理，减少反复抢同一 cacheline。
- batch 内重复 row 去重：同一批内重复 key/row 只做一次 bitmap claim。
- no-lock bench-only 对照：临时跳过 bitmap，只用于量化 bitmap 理论上限，不作为生产语义。

### P2: CPU 亲和性规划

当前 `--pin yes` 只 pin client，16/32 线程明显负收益。后续如果要做 pin，需要同时规划：

- client worker CPU set
- proxy channel thread CPU set
- SuperNode worker CPU set
- NUMA/cluster 分布

在此之前，主压测默认使用 `pin=no`。

## 建议默认压测命令

纯 VEMB read 基准：

```bash
./benchmark/vemb_v16_bench \
  --mode vemb-supernode-read \
  --dim 300 \
  --prefill 65536 \
  --ops 200000 \
  --threads 4,8,16,32 \
  --pipeline 16 \
  --timeout-ms 30000
```

tlc_v16 80R/20W 对标基准：

```bash
./benchmark/vemb_v16_bench \
  --mode mixed-80r20w \
  --dim 300 \
  --prefill 65536 \
  --ops 200000 \
  --threads 4,8,16,32 \
  --pipeline 16 \
  --timeout-ms 30000
```

如需验证 pin，请使用旧参数格式：

```bash
./benchmark/vemb_v16_bench \
  --mode vemb-supernode-read \
  --dim 300 \
  --prefill 65536 \
  --ops 200000 \
  --threads 4,8,16,32 \
  --pipeline 16 \
  --pin yes \
  --timeout-ms 30000
```

当前推荐结论：主线继续以 `pipeline=16, pin=no` 作为基准，下一轮优先落地 batch 真实度量和 SuperNode 内部 batch/bitmap 优化。
