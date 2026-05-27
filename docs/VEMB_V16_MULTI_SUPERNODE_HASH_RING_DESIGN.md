# VEMB V16 多 SuperNode 架构设计

日期：2026-05-22

## 核心结论

多 SuperNode 场景下，`consistent_hash` 在 CLI 中完成。Proxy 不负责 hash、不负责拓扑/region 配置、不做计算。

部署关系采用一一对应模型：

```text
Proxy 0 <-> SuperNode 0
Proxy 1 <-> SuperNode 1
...
Proxy N <-> SuperNode N
```

CLI 根据本地配置执行：

```text
vector_key -> consistent_hash(vector_key) -> supernode_id
supernode_id -> proxy_id
proxy_id == supernode_id
```

然后 CLI 向对应 proxy 申请 channel。Proxy 只负责：

```text
CLI channel 生命周期
channel_id 单调分配和回收
channel_id -> SuperNode 绑定
CLI request/response ring 生命周期
Proxy <-> SuperNode 数据交互
```

SuperNode 负责执行：

```text
VADD
VEMB
VSIM
TLC HOT/WARM/COLD
```

交付版本备注：

```text
交付版本会率先使用 TCP/IP 方式完成 CLI 与 SuperNode 之间的交互。
CLI 仍在本地执行 consistent_hash(vector_key)，选择目标 supernode_id。
选中目标后，CLI 直接通过 TCP/IP 向对应 SuperNode 发送 VADD/VEMB/VSIM 请求。
proxy/channel/shared-memory ring/WARM mmap read-by-handle 作为后续高性能数据面形态继续演进。
```

因此，交付版的功能语义先按“CLI hash route -> TCP/IP -> SuperNode -> TLC”闭环，先保证多 SuperNode 路由、请求语义和 TLC 存储语义正确；后续再把传输层替换为 proxy-managed channel 与共享内存/UB 读路径。

## Network Transport 选型

多 SuperNode 的第一阶段网络路径选择 TCP persistent connection + binary frame，不使用 `aeron_ipc`。

优先级：

```text
1. TCP: first implementation and default cross-node transport
2. DPDK: later high-end transport after TCP profiling identifies kernel stack bottleneck
3. KCP/UDP: experimental transport for lossy or high-jitter networks
```

取舍：

- TCP 最适合先交付：可靠、有序、跨机可用，部署与排障成本低。
- DPDK 只有在可以独占 CPU/NIC 队列、配置 hugepage、绑定 NUMA/core，并且 TCP 已被证明是瓶颈时才值得进入主线。
- KCP 更适合弱网，不适合作为机房内低丢包网络的默认高性能数据面。

第一阶段连接模型：

```text
bench/CLI worker
-> route vector_key by local consistent hash
-> connect target supernode TCP endpoint
-> HELLO/WELCOME creates per-connection channel
-> REQUEST frames carry vemb_v16_req_t
-> RESPONSE frames carry vemb_v16_resp_t
```

跨 SuperNode TCP 路由架构：

```mermaid
flowchart LR
    CLI[bench/CLI worker]
    HR[local consistent hash]

    subgraph N0[Node 0]
        EP0[TCP endpoint<br/>host0:port]
        P0[vemb_v16_proxy]
        CH0[TCP channel]
        SN0[SuperNode 0<br/>TLC]
        W0[WARM/UB region 0]
        EP0 --> P0 --> CH0 --> SN0 --> W0
    end

    subgraph N1[Node 1]
        EP1[TCP endpoint<br/>host1:port]
        P1[vemb_v16_proxy]
        CH1[TCP channel]
        SN1[SuperNode 1<br/>TLC]
        W1[WARM/UB region 1]
        EP1 --> P1 --> CH1 --> SN1 --> W1
    end

    CLI --> HR
    HR -->|vector_key -> SN0| EP0
    HR -->|vector_key -> SN1| EP1
```

每个 TCP 连接在 server 内部仍绑定一个 channel worker 和一个 SuperNode worker；server 内部继续使用进程内 typed SPSC ring，保持现有 `proxy -> SuperNode -> completion -> proxy` 执行边界。

TCP control 也走同一个 TCP endpoint：

```text
STATS
CLOSE_CHANNEL
CLOSE_ALL_CHANNELS
```

因此 `--transport tcp` 的 bench/CLI 可以在跨主机场景下完成 stats 与 channel cleanup，不需要访问目标机器上的 UDS control socket。TCP data connection 断开后，server 负责 reap inactive channel、join worker，并把 counters 合并进 closed stats。

## Multi-SuperNode 架构

```mermaid
flowchart LR
    CFG[CLI local config<br/>proxies<br/>supernodes<br/>consistent hash<br/>regions]
    CLI[CLI]
    HR[Consistent Hash Ring<br/>in CLI]

    P0[Proxy 0<br/>channel_id manager]
    P1[Proxy 1<br/>channel_id manager]

    C0[CLI Channel<br/>to Proxy 0<br/>target=SN0]
    C1[CLI Channel<br/>to Proxy 1<br/>target=SN1]

    subgraph SN0G[SuperNode 0]
        SN0[SuperNode 0<br/>TLC]
        HOT0[HOT private]
        WM0[WARM metadata private]
        WD0[WARM data region<br/>region_id=0]
        CD0[COLD append layer]
        SN0 --> HOT0
        SN0 --> WM0
        SN0 --> WD0
        SN0 --> CD0
    end

    subgraph SN1G[SuperNode 1]
        SN1[SuperNode 1<br/>TLC]
        HOT1[HOT private]
        WM1[WARM metadata private]
        WD1[WARM data region<br/>region_id=1]
        CD1[COLD append layer]
        SN1 --> HOT1
        SN1 --> WM1
        SN1 --> WD1
        SN1 --> CD1
    end

    CLI --> CFG
    CFG --> CLI
    CLI --> HR

    HR -->|vector_key -> SN0| P0
    HR -->|vector_key -> SN1| P1

    CLI -->|allocate_channel target=SN0| P0
    CLI -->|allocate_channel target=SN1| P1
    P0 --> C0
    P1 --> C1
    C0 --> P0
    C1 --> P1

    P0 -->|one-to-one data path| SN0
    P1 -->|one-to-one data path| SN1

    CLI -->|mmap/read handle region_id=0| WD0
    CLI -->|mmap/read handle region_id=1| WD1

    classDef group0 fill:#e8f3ff,stroke:#2563eb,stroke-width:2px,color:#102a56
    classDef group1 fill:#ecfdf3,stroke:#16a34a,stroke-width:2px,color:#063b1d
    classDef shared fill:#f8fafc,stroke:#64748b,stroke-width:1px,color:#0f172a

    class P0,C0,SN0,HOT0,WM0,WD0,CD0,SN0G group0
    class P1,C1,SN1,HOT1,WM1,WD1,CD1,SN1G group1
    class CFG,CLI,HR shared
```

说明：

- CLI 从本地配置读取 proxy 列表、SuperNode 列表、consistent hash 参数和 WARM region descriptors。
- CLI 在本地执行 `consistent_hash(vector_key)`，得到目标 `supernode_id`。
- `proxy_id == supernode_id`，CLI 连接对应 proxy。
- Proxy 和 SuperNode 一一对应，不存在一个 proxy 面对多个 SuperNode 的热路径 fan-out。
- Proxy 不做 hash，不持有 consistent hash ring。
- Proxy 不下发 region descriptors。
- 每个 SuperNode 有独立 TLC：HOT 私有、WARM metadata 私有、WARM data region 可被 CLI mmap 读取、COLD 独立 append。
- VEMB response 返回 `{region_id, offset, bytes}`，CLI 根据 `region_id` 读取对应 SuperNode 的 WARM data region。

## 初始化时序

```mermaid
sequenceDiagram
    participant CFG as CLI Config
    participant CLI as CLI
    participant HR as Consistent Hash Ring

    box rgba(232,243,255,0.95) SuperNode 0 shard
        participant P0 as Proxy 0
        participant SN0 as SuperNode 0
        participant W0 as WARM Region 0
    end

    box rgba(236,253,243,0.95) SuperNode 1 shard
        participant P1 as Proxy 1
        participant SN1 as SuperNode 1
        participant W1 as WARM Region 1
    end

    CLI->>CFG: load proxies, supernodes, hash, regions
    CLI->>HR: build consistent hash ring locally
    CLI->>W0: mmap/attach region 0
    CLI->>W1: mmap/attach region 1

    CLI->>P0: allocate_channel(target=SN0)
    P0->>SN0: bind channel to local SuperNode
    SN0-->>P0: ready
    P0-->>CLI: channel_id and CLI ring descriptors

    CLI->>P1: allocate_channel(target=SN1)
    P1->>SN1: bind channel to local SuperNode
    SN1-->>P1: ready
    P1-->>CLI: channel_id and CLI ring descriptors
```

## VADD 时序

```mermaid
sequenceDiagram
    participant CLI as CLI
    participant HR as Consistent Hash Ring in CLI
    participant P as Proxy i
    participant SN as SuperNode i
    participant TLC as TLC
    participant WMETA as WARM metadata
    participant WDATA as WARM data region

    CLI->>CLI: parse VADD and normalize vector_key
    CLI->>HR: consistent_hash(vector_key)
    HR-->>CLI: supernode_id=i
    CLI->>P: publish VADD on channel_i
    P->>SN: forward VADD to one-to-one SuperNode
    SN->>TLC: tlc_put(vector_key, value)
    TLC->>WMETA: find or allocate warm_slot
    TLC->>WDATA: write value at slot offset
    TLC->>WMETA: publish/update key -> warm_slot
    TLC-->>SN: OK
    SN-->>P: OK(req_id)
    P-->>CLI: response OK(req_id)
```

## VEMB 时序

```mermaid
sequenceDiagram
    participant CLI as CLI
    participant HR as Consistent Hash Ring in CLI
    participant P as Proxy i
    participant SN as SuperNode i
    participant TLC as TLC
    participant HOT as HOT private
    participant WMETA as WARM metadata
    participant WDATA as WARM data region

    CLI->>CLI: parse VEMB and normalize vector_key
    CLI->>HR: consistent_hash(vector_key)
    HR-->>CLI: supernode_id=i
    CLI->>P: publish VEMB on channel_i
    P->>SN: forward VEMB to one-to-one SuperNode
    SN->>TLC: tlc_get_handle(vector_key)
    TLC->>HOT: lookup key
    alt HOT miss
        TLC->>WMETA: lookup key -> warm_slot
    end
    TLC->>WMETA: validate slot state/key
    TLC->>TLC: offset = warm_slot * value_size
    TLC-->>SN: handle(region_id, offset, bytes)
    SN-->>P: response(req_id, handle)
    P-->>CLI: response(req_id, handle)
    CLI->>WDATA: read regions[region_id] + offset
```

## 约束

- `consistent_hash` 只在 CLI 中执行。
- Proxy 和 SuperNode 一一对应。
- Proxy 不参与 hash ring 构建、查找、发布。
- Proxy 不下发 SuperNode 拓扑和 region descriptors。
- Proxy 只管理 channel 生命周期和 `channel_id`。
- Proxy 只把已绑定 channel 的 request 转交给本地对应 SuperNode。
- SuperNode 执行 VADD/VEMB/VSIM 和 TLC。
- CLI 根据 VEMB 返回 handle 读取 WARM data region。

## 性能影响

相对单 SuperNode 场景，多 SuperNode 额外成本主要在 CLI 侧：

```text
murmur3_hash(vector_key)
consistent_hash ring lookup
选择 proxy/channel
```

Proxy 侧不新增 hash lookup，也不需要在多个 SuperNode 之间 fan-out。由于 proxy 与 SuperNode 一一对应，proxy 热路径仍保持固定目标的 channel 转发模型。
