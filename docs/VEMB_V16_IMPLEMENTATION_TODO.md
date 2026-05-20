# VEMB V16 实现 TODO

日期：2026-05-20

## 当前落地范围

实现一条独立 VEMB V16 数据面路径，不改造 `tlc_v16`，也不依赖 Redis command / RedisModule / blocked-client 路径：

```text
vemb_v16_bench / future redis-cli-vemb
-> UDS control alloc channel
-> shared-memory request ring
-> vemb_v16 proxy/channel worker
-> SPSC job ring
-> vemb_v16 SuperNode worker
-> SPSC completion ring
-> shared-memory response ring
-> optional client vector-region read
```

## P0/P1 当前状态

- P0 已完成：standalone path 具备 server 侧 stats，包括 proxy request/completion poll、VEMB/VADD publish、ring full、SuperNode poll/completion、sampled table lookup、bitmap lock/unlock、vector load、completion publish。
- P0 已完成：bench 侧输出 client request ring publish spins 和 response empty polls，用于判断瓶颈是在 client/proxy ring 还是 SuperNode/completion。
- P0 已完成：VEMB 在 SuperNode 内部先做 `vector_key -> row_id` lookup，再调用 `sve_serial_contiguous_read_traced()` 读取 300 dim vector；因此 baseline 会包含 bitmap acquire/release 和 SVE/标量 contiguous load 成本。
- P1 已完成：`vemb_v16_bench` 支持统一线程列表，例如 `--threads 1,2,4,8,16`。
- P1 已完成：`ping`、`vemb-handle`、`vemb-read-vector`、`vadd-inline` 四条模式可独立 baseline。
- P1 已完成：`--hot-key-id N` 可让所有 worker 命中同一 row，用于制造 bitmap lock 冲突并观察 `bitmap_lock_failure`。
- P1 已完成：bench 每轮结束会关闭 channel 后再拉 stats，`active_channels=0` 代表 channel 生命周期清理完成。

本地短基线（macOS，`--dim 300 --prefill 256 --ops 2000 --threads 1,2`，只用于验证统计闭环，不代表 Linux server 性能）：

| mode | threads=1 | threads=2 | 关键校验 |
| --- | ---: | ---: | --- |
| `ping` | 233k QPS | 429k QPS | 不进入 SuperNode，`total/vemb/vadd=0` |
| `vemb-handle` | 367k QPS | 521k QPS | `published=completed=vemb`，VEMB 进入 `sve_serial_contiguous_read_traced()` |
| `vemb-read-vector` | 321k QPS | 492k QPS | 在 `vemb-handle` 上增加 client 读 vector region |
| `vadd-inline` | 219k QPS | 368k QPS | VADD 全量传 1200B vector，进入 VADD full-vector ring |

## Phase 1

- `src/vemb_v16_protocol.h` 定义无 Redis 依赖的 channel descriptor、request、response、stats 协议。
- `src/vemb_v16_proxy.c` 实现 standalone proxy/channel worker、SPSC job ring、SPSC completion ring、内存 vector table 和 SuperNode worker。
- `src/vemb_v16_server.c` 是独立 main，只负责启动 proxy 和 signal 生命周期。
- `benchmark/vemb_v16_bench.c` 直接连接 standalone server，支持 `ping`、`vemb-handle`、`vemb-read-vector`、`vadd-inline`。
- 第一版用进程内 vector table 代替 UB vector table，保持 `vector_key -> handle/offset -> vector region` 模型，后续替换为 UB backend。
- 当前版本没有 `server.h`、`RedisModuleCtx`、`RedisModule_BlockClient()`、`RedisModule_UnblockClient()` 依赖。

## 当前运行方式

启动独立 server：

```bash
./src/vemb_v16_server --dim 300 --max-vectors 65536
```

`vadd-inline` 会向进程内 table 写入新 key。重复跑 VADD baseline 时，`--max-vectors` 需要大于本次进程生命周期内累计写入量；如果只是小容量验证，建议重启 server 清空 table。

运行 bench：

```bash
./benchmark/vemb_v16_bench --mode ping --dim 300 --prefill 0 --ops 200000 --threads 1,2,4,8,16
./benchmark/vemb_v16_bench --mode vemb-handle --dim 300 --prefill 65536 --ops 200000 --threads 1,2,4,8,16
./benchmark/vemb_v16_bench --mode vemb-read-vector --dim 300 --prefill 65536 --ops 200000 --threads 1,2,4,8,16
./benchmark/vemb_v16_bench --mode vadd-inline --dim 300 --prefill 0 --ops 200000 --threads 1,2,4,8,16
```

关键观察项：

- `request_publish_spins` 高：client -> proxy request ring 消费不及时。
- `response_empty_polls` 高：client 在等待 proxy 回包，需结合 SuperNode/completion 计数判断。
- `proxy_vemb_ring_full` / `proxy_vadd_ring_full` 高：proxy -> SuperNode job ring 是瓶颈。
- `supernode_completion_ring_full` 高：SuperNode -> proxy completion ring 是瓶颈。
- `proxy_response_ring_full` 高：proxy -> client response ring 是瓶颈。
- `bitmap_lock_failure` 高：多个 SuperNode worker 正在争抢同一 row，热点 key 或 row 映射冲突明显。
- `vector_load_avg_ns` 高：SuperNode 内部 contiguous read 是瓶颈；在 ARM SVE build 上这里对应 SVE load/store 路径，在非 SVE build 上是 scalar `memcpy` fallback。
- `published_jobs == completed_jobs == vemb_requests/vadd_requests`：完整路径没有丢请求。

热点冲突验证：

```bash
./benchmark/vemb_v16_bench --mode vemb-handle --dim 300 --prefill 65536 --ops 200000 --threads 2,4,8,16 --hot-key-id 0
```

该模式用于观察同一 `row_id` 的 bitmap acquire 冲突，不代表均匀 key 分布下的生产性能。

## 后续

## Phase 2：Aeron Ring 化

设计文档：`docs/VEMB_V16_PHASE2_AERON_RING_DESIGN.md`

核心要求：

- 采用 Aeron fixed-slot SPSC ring。
- 不复用 `src/ring_buffer.h/.c`。
- 不引入 `server.h` / Redis runtime。
- 将当前 `vemb_v16_proxy.c` 内部临时 `job_ring` / `completion_ring` 替换为 typed Aeron ring。
- 将 SuperNode worker loop 从 proxy 文件拆出。
- 将进程内 vector table 从 proxy 文件拆出，后续替换为 UB backend。
- VEMB 使用小 descriptor ring；VADD 暂时保留 full-vector job ring，全量传输 1200B vector。

落地顺序：

1. 新增 `src/vemb_v16_aeron_ring.h`。
2. 新增 `src/vemb_v16_dataplane.h`，放置 `vemb_v16_job_t` / `vemb_v16_completion_t`。
3. 用 Aeron typed ring 替换 `vemb_v16_proxy.c` 内部临时 ring。
4. 新增 `src/vemb_v16_supernode.c/.h`，SuperNode 通过 job ring 收请求，通过 completion ring 回结果。
5. 新增 `src/vemb_v16_table.c/.h`，把 table/backend 从 proxy 拆出。
6. 拆分 VEMB 小 job ring 和 VADD full-vector job ring，避免 VEMB 复制 1200B payload。
7. 补充性能计数：proxy poll、job ring、SuperNode 执行、completion、response ring。

## Phase 3+

- 将进程内 vector table 替换为 UB vector table。
- 增加文本命令 CLI，解析兼容 `VADD myvectors ... item:N` / `VEMB myvectors item:N RAW`。
- 在 VEMB 热路径稳定后，再把 VADD full-vector job ring 改为 staged VADD。
- 增加 consistent hash ring 和多 SuperNode 进程消费。
- 增加 VSIM query/result path。
