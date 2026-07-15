# 性能解释

## 实验方法

- **服务端 pin 核**：`taskset -c 1-96 numactl -N 0 -l`（NUMA node0 物理 1\-96 核，跳过 core 0）

- **客户端 pin 核**：`taskset -c 96-191`（NUMA node1，与 server 零重叠）

- **负载**：10000 keys × 300\-dim = 1200B/向量，100% read

- **测试时长**：每点 8 秒（`--test-time=8`）

- **memtier binary**：VEMB 路径用定制版 `memtier_benchmark`（`--protocol=vemb_v16`），RESP 路径用 upstream `memtier_benchmark_origin`（`--command="VEMB myvectors key"`）

- **预填充**：两边都用各自协议 VADD 10K 条向量；Redis baseline 走 RESP `VADD ... VALUES 300 ...`，hpc\-redis 走 `memtier --protocol=vemb_v16 --ratio=1:0`

- **两阶段扫描**：

    - **Part A**：服务端配置固定（hpc\-redis `pio=32 snw=64`，Redis `io-threads=32`），扫客户端 `-t ∈ {1,8,16,32,64}`，pipeline 跟随 `-t` 同步增加

    - **Part B**：客户端固定 `-t 32 -c 1 --pipeline=32`，扫服务端线程数（Redis `io-threads ∈ {1,4,8,16,32,64}`；hpc\-redis `pio ∈ {1,4,8,16,32,64}` \+ `snw=64`）

### 1\.2 启动脚本

**Redis baseline 单实例（默认单线程）**：

```Bash
cd /root/gqs/codespace/redis-8.6.3
taskset -c 1,2 ./src/redis-server --port 6379 --daemonize yes
```

**Redis baseline 启用 io\-threads**：

```Bash
taskset -c 1-4 ./src/redis-server \
    --port 6379 \
    --io-threads N \
    --io-threads-do-reads yes \
    --daemonize yes
```

**test\_hpc VEMB V16**：

```Bash
cd /root/gqs/codespace/UnifiedBus/test_hpc
taskset -c 1-10 ./src/redis-server \
    --port 6379 \
    --vemb-v16-enabled yes \
    --vemb-v16-dim 300 \
    --vemb-v16-max-vectors 131072 \
    --vemb-v16-warm-regions-manifest /root/gqs/codespace/UnifiedBus/hpc-redis/examples/vemb_v16_warm_regions_111.yaml \
    --vemb-v16-proxy-io-threads $PIO \
    --vemb-v16-supernode-workers $SNW \
    --daemonize yes
```

脚本：`/tmp/launch_test_hpc.sh PIO SNW`、`/tmp/redis_iothreads_sweep.sh`。

### 1\.3 每线程 CPU 测量方法

通过读取 `/proc/PID/task/TID/stat` 的字段 14 \(utime\) 和 15 \(stime\)，记录 benchmark 前后的差值：

```Bash
snapshot_cpu() {
    for tid in $(ls /proc/$PID/task); do
        awk '{print $1, $14, $15}' /proc/$PID/task/$tid/stat
    done > $SNAPSHOT_FILE
}
```

CLK\_TCK = 100 Hz（ARM64 上），所以 `tick/100 = 秒`。汇总所有线程的 `(utime+stime)` 差值，除以 benchmark 时长，得到使用的总核数：

```Plain Text
total_cores = sum_t((utime_t_after + stime_t_after) - (utime_t_before + stime_t_before)) / 100 / duration_sec
```

脚本：`/tmp/test_hpc_sweep.sh`、`/tmp/redis_bench_1200b.sh`、`/tmp/test_hpc_2core_sweep.sh`。

## 实验结果

### 服务端线程扩展（客户端固定 `-t 32 -c 1 --pipeline=32`）32, 200

##### `-t 32 -c 200 --pipeline=16`

##### `-t 32 -c 200 --pipeline=32`



## 性能优势来源分析

### 10\.4\.1 协议层 —— 二进制帧 vs RESP 文本（贡献约 1\.3x）

**机制**：

**代码位置**：

- VEMB V16 请求解析：`src/vemb_v16_proxy.c::proxy_io_worker` 读 `vemb_v16_net_hdr_t` \+ `vemb_v16_req_t`

- RESP `VEMB` 命令分发：redis\-8\.6\.3 `src/server.c::processCommand()` → `vector-sets/module.c`

**结论**：**协议优势固定 \~1\.3x**，在多线程下被其它机制放大优势稀释，但绝不是主导因素。



### 10\.4\.2 IO 并行模型 —— 全 datapath 并行 vs 仅 socket 并行（最大因素，贡献约 10x）

Redis 8\.6\.3 io\-threads 代码路径（参考 redis/8\.6\.3 networking\.c）：

```Plain Text
IO 线程池（io-threads=N）                     main thread
────────────────          ──────────────
readQueryFromClient()       ──┐     processCommand()
  解析 RESP 参数             │               ↓
IO 线程写回 response         └──→    command dispatch
                                        VEMB lookup
                                        addReplyToBlock()
                           ↓
                        IO 线程 write
```



VEMB 命令的执行（vector\-sets/vset\.c::vembedCommand\(\)）走 RedisModule API，必须在 main thread 持有 GIL 时跑。IO 线程再多也只是把 socket 读写并行了，命令本身仍然 1 个核串行 —— 这就是 Part B 里 io\-threads=4→8→32→64 全部封顶 350\-402K 的根因。hpc\-redis proxy\_io\_threads 代码路径（src/vemb\_v16\_proxy\.c:1603）：每个 proxy\_io\_worker 独立事件循环

─────────────────────────────

accept / drain inject\_pipe  ←  src/vemb\_v16\_proxy\.c:1660\-1684

recv\(proxy\_epoll\)           ←  epoll\_wait \+ readv

parse vemb\_v16\_req\_t        ←  本地解析

vemb\_v16\_storage\_lookup     ←  走 OBMM 共享内存

encode vemb\_v16\_resp\_t      ←  inline vector

writev                      ←  直接回包

完全没有 main thread 介入（除了最初的 sniff/handoff）。每多一个 pio worker，就多一条独立的命令处理流水线。数据证据：• Part B 斜率：Redis io\-1 → io\-4 涨 1\.42x（282K → 402K），之后完全持平。hpc\-redis pio=1 → pio=32 涨 15x（401K → 6\.03M）。• Part A \-t 32：Redis 已经触顶在 343K（main thread 满载），hpc\-redis 5\.82M（pio=32 全部吃满）。• 最直接的反证：Redis io=4 → io=32 时实际用核从 3\.5 涨到 26\.4（7\.5x），但吞吐反而从 402K 跌到 346K（\-14%）。如果 io\-threads 真的在并行有效工作，多花 7\.5 倍 CPU 不可能换来吞吐下降。这只能解释为：IO 线程确实在跑，但跑的是"等待 main thread 出队"的空转 \+ 跨核同步开销，没有转化为有效吞吐。结论：IO 并行模型是 17\.4x 优势的核心来源，单独贡献约 10x。Redis 的 io\-threads 设计本质是"并发 IO \+ 串行执行"，对计算密集型命令（VEMB 查向量）效果有限；hpc\-redis 是真正的"并发 IO \+ 并发执行"。



用 `top -H` \+ `perf stat` 抓了两组 server 在相同负载下的线程级 CPU 分布（脚本 `benchmark/perf_thread_breakdown.sh`，12 秒采样窗口）。

**Redis baseline io=32**（client `-t 32`，ops/sec=354K）—— 前 10 个最忙线程：

**hpc\-redis pio=32 snw=64**（client `-t 32`，ops/sec=5\.75M）—— 前 10 个最忙线程：

**关键观察**：

1. **Redis baseline 的 main thread 是 CPU% 最高的单一瓶颈点（98\.1%）**，明显高出 io\_threads 一档（83\-85%）。main thread 满载在做 `VEMB` 命令执行（dictFind \+ vector\-sets module lookup \+ reply buffer 组装），io\_threads 在 busy\-poll 等待 main thread 派发新任务。

2. **hpc\-redis 没有 main thread 在前 10**（main thread 在 sniff/handoff 后就让出 CPU），32 个 proxy\_io\_workers 均衡分担负载，最高的 77\.5% vs 最低的 76\.5%，**没有单点瓶颈**。

3. **perf stat 数据对比**（同样 12 秒窗口）：

- **Redis baseline 的 IPC 1\.29 明显高于 hpc\-redis 的 0\.99**，说明 Redis CPU 利用更"纯"（main thread 在做命令执行这种 compute\-bound 工作，cache 友好）；hpc\-redis 的 worker 在做更多 syscall \+ memory IO，IPC 较低。但**单核效率高不代表总吞吐高** —— Redis 受限于 1 个 main thread，IPC 再高也只有 1 个核在产生有效产出。

- **hpc\-redis context\-switch 32x 多**，因为每 op 含独立的 `readv`/`writev` 系统调用；Redis baseline 的 io\_threads 在 spin\-poll 模式不切换。这 32x 的 ctx switch 消耗了一部分 CPU，但带来了 16\.2x 的吞吐收益。

- **task\-clock 1\.81x ≠ 吞吐 16\.2x**：hpc\-redis 多用了 80% 的 CPU 时间，换来 16\.2 倍的吞吐。**多核并行的"杠杆"是 9x**。

- Redis baseline 的 main thread 在 `top -H` 里是**唯一一个达到 98% CPU 的线程**，且它就是 GIL 持有者，命令执行必须串行通过它。这是 Redis 单实例架构的物理上限，无法通过任何单实例内配置突破



### 10\.4\.3 存储层 —— UB 共享内存 mmap vs RedisModule heap（贡献约 1\.5x）

**机制**：

**代码位置**：

- hpc\-redis mmap：`src/vemb_v16_server_integration.c:92` `strncpy(region->path, vector_region, ...)`

- 存储查找：`src/vemb_v16_storage.c::vemb_v16_storage_lookup()` 走 warm region slot table

**数据证据**：单核 p50 延迟对比 —— Part B `pio=1` p50=2\.6ms vs `io=1` p50=4\.5ms，**同核同线程数下 hpc\-redis 延迟低 42%**。这部分差距无法用 IO 并行解释（两边都是单线程），只能归因于存储层 \+ 协议层的单 op 效率。

**结论**：**存储层贡献约 1\.5x**（含协议层 1\.3x 重叠）。OBMM mmap 的真正价值不是单 op 延迟，而是**多线程并发访问无锁**：Redis baseline 即使能绕开 main thread 限制，dict 全局锁也会成为瓶颈；hpc\-redis 的 slot table 是按 channel 分片的，无跨 worker 冲突。

