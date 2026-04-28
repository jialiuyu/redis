# HPC-Redis 项目总结报告

## 一、传统 Redis 项目架构

### 1.1 核心事件循环

Redis 采用经典的 **Reactor 模式**，基于事件驱动架构。核心实现在 `src/ae.c` 中：

```
main() [server.c:7566]
  → initServerConfig()     // 加载默认配置
  → initServer()           // 创建事件循环 aeCreateEventLoop()
  → aeMain(server.el)      // 进入主事件循环（无限循环直到 stop）
```

`aeMain()` 中的循环逻辑：
1. 调用 `beforesleep` 回调（处理 TLS 挂起数据、阻塞客户端、AOF 刷盘等）
2. 阻塞等待 I/O 事件（epoll/kqueue/evport/select 四选一）
3. 调用 `aftersleep` 回调
4. 遍历就绪的文件事件，调用对应的读/写回调函数
5. 处理时间事件（server cron 定时任务等）

### 1.2 线程模型

| 线程 | 职责 | 关键特性 |
|------|------|----------|
| **主线程** | 命令执行、事件循环 | 单线程顺序处理所有命令，避免数据竞争 |
| **I/O 线程** | Socket 读写 | `iothread.c`，可配置 1-128 个，由主线程派发任务 |
| **后台线程** | AOF 重写、RDB 保存、惰性删除等 | fork 子进程或后台线程执行 |

**I/O 线程工作流程**：
- 主线程在 `beforeSleep()` 中调用 `sendPendingClientsToIOThreads()`
- I/O 线程并行读取/写入客户端 Socket
- 主线程调用 `processClientsOfAllIOThreads()` 收集结果
- 配置项：`io-threads`（线程数）、`io-threads-do-reads`（是否也解析查询）

### 1.3 数据流图

```
                        ┌───────────────────────────────────┐
                        │           客户端连接               │
                        └──────────────┬────────────────────┘
                                       ↓
                        ┌───────────────────────────────────┐
  accept handler        │  acceptTcpHandler() [networking.c]│
  创建 client 结构      │  → acceptCommonHandler()           │
                        └──────────────┬────────────────────┘
                                       ↓
                        ┌───────────────────────────────────┐
  read handler          │  readQueryFromClient()             │
  读取到 querybuf       │  [networking.c:3610]               │
                        └──────────────┬────────────────────┘
                                       ↓
                        ┌───────────────────────────────────┐
  命令解析              │  processInputBuffer()              │
  RESP 协议解析         │  → processMultibulkBuffer()        │
                        │  → processInlineBuffer()           │
                        │  [networking.c:3428]               │
                        └──────────────┬────────────────────┘
                                       ↓
                        ┌───────────────────────────────────┐
  命令执行              │  processCommand() [server.c]       │
  查找命令表 → 执行      │  → redisCommandTable 查找          │
                        │  → 调用对应实现函数                 │
                        └──────────────┬────────────────────┘
                                       ↓
                        ┌───────────────────────────────────┐
  写入响应              │  addReply*() 系列函数              │
  写入 output buffer    │  → I/O 线程发送 或 直接 write      │
                        │  handleClientsWithPendingWrites()  │
                        └───────────────────────────────────┘
```

### 1.4 核心数据结构

| 文件 | 数据结构 | 用途 |
|------|----------|------|
| `sds.c` | SDS 简单动态字符串 | 所有字符串存储 |
| `dict.c` | 哈希表 (Dict) | 键空间、过期表、集群槽 |
| `kvstore.c` | Slot 哈希表 | 集群模式下的键空间 |
| `quicklist.c` + `listpack.c` | 快速链表 + 紧凑存储 | List 类型 |
| `rax.c` | 基数树 (Rax) | Stream 和 Key 过期索引 |
| `intset.c` | 整数集合 | 小型 Set 的紧凑编码 |
| `ziplist.c` / `listpack.c` | 紧凑列表 | Hash/Zset 的小规模编码 |
| `ebuckets.c` + `fwtree.c` | E-Buckets + Fenwick Tree | 分层过期管理 |

### 1.5 全局状态

Redis 的所有全局状态存储在一个巨型结构体 `struct redisServer server`（`server.h`，约 4400 行），包括：配置参数、数据库指针数组、客户端列表、持久化状态、集群状态、复制状态、统计信息等。

## 二、当前工作集中点：HPC-Redis 改造范围

### 2.1 改造概述

HPC-Redis 在**不改变 Redis 核心命令处理流程**的前提下，新增了面向大规模向量检索的高性能计算层。改造方式为**最小侵入式**：仅在三个现有文件中插入钩子，其余均为新增文件。

### 2.2 被修改的 Redis 原始文件

| 文件 | 修改内容 | 位置 |
|------|----------|------|
| `src/server.h` | 新增 `vector_engine_type_t` 枚举（REDIS/UB）、`vector_engine_type` 和 `vector_engine_enabled` 字段 | 行 49-52, 1921-1923 |
| `src/config.c` | 新增 `vector-engine` 配置指令（枚举类型 redis/ub）、`updateVectorEngine()` 应用回调 | 行 2623-2632 |
| `src/Makefile` | 将 HPC 对象文件加入 `REDIS_SERVER_OBJ` 链接列表 | 行 385 |

### 2.3 新增的 HPC 组件

```
                         客户端请求 (VADD/VREM/VSIM/VEMB)
                                ↓
                    ┌───────────────────────────┐
   配置选择后端      │  vector_engine.c/h        │  向量引擎抽象层
                    │  (redis HNSW | UB 后端)    │
                    └───────────┬───────────────┘
                                ↓ (UB 路径)
                    ┌───────────────────────────┐
   批量收集          │  batch_processor.c/h      │  50μs 超时
   最多 1024 请求    │  后台 pthread 线程         │  异步处理
                    └───────────┬───────────────┘
                                ↓
                    ┌───────────────────────────┐
   智能聚合          │  proxy_aggregator.c/h     │  MurmurHash3 一致性哈希
   3000-6000/批      │  200μs 时间窗口            │  SPSC Ring Buffer 零拷贝
                    └───────────┬───────────────┘
                                ↓
                    ┌───────────────────────────┐
   超节点计算        │  supernode_worker.c/h     │  最多 16 个 SVE2 Worker
   Bitmap CAS 无锁   │  4TB UB.mem 共享内存      │  Gather Load 批量读取
                    └───────────┬───────────────┘
                                ↓
                    ┌───────────────────────────┐
   SVE2 加速         │  sve_compute.c/h          │  256-bit SVE2 SIMD
   余弦/GEMM/Adam    │  sve2_gemm.h (header)     │  1M embedding 缓存
                    │  sve2_adam.h (header)     │  非 ARM 有标量回退
                    └───────────┬───────────────┘
                                ↓
                    ┌───────────────────────────┐
   零拷贝通信        │  ub_client.c/h            │  dlopen(libubios.so)
   用户态总线         │  ComputeNode/MemoryTile   │  共享内存 Ring Buffer
                    └───────────┬───────────────┘
                                ↓
                    ┌───────────────────────────┐
   三层缓存          │  three_layer_cache.c/h    │  HOT 16B → WARM 1200B
   索引加速           │  three_layer_cache_ub.c  │  → COLD 追加写
                    └───────────────────────────┘
```

### 2.4 关键技术创新

1. **全用户态零拷贝**：无内核态切换，`shm_open` + `mmap` 大页共享内存
2. **微秒级批量聚合**：200μs 积攒 → 3000 请求聚合为 1 次 UB 交互 → 交互次数减少 3000 倍
3. **Bitmap CAS 无锁并发**：`__atomic_compare_exchange_n` + `__ATOMIC_ACQ_REL`，遇锁跳过不等待
4. **ARM SVE2 向量化**：256-bit 宽度，8 个 float 并行，Fused Gather+Cosine 计算
5. **三层缓存分级**：HOT（16B 索引，128K，Lock-free LDP/STP）→ WARM（1200B，1M，Bitmap CAS）→ COLD（追加写）

### 2.5 当前工作阶段

根据提交历史，当前工作集中在**三层缓存多传输后端版本迭代**和**性能基准测试**：
- V8-V9：DPDK/FC/KCP 等多传输协议 TLC 服务器实现
- V10：Aeron IPC + UDS 传输（实测 3.79M QPS 单条，11.88M keys/s 批量）
- V13-V14：持续优化迭代
- 最新提交添加了 benchmark 分析脚本和 DPDK/Unified 对比测试

## 三、hpc-redis 分支提交摘要

本分支共有 **16 个提交**，从 2026-04-08 到 2026-04-24，主要贡献者：kevin259 (GitCode)、7u5、Wei Xu (华为)、jialiuyu。

### 3.1 按提交顺序详述

| # | 提交哈希 | 日期 | 作者 | 描述 | 内容概述 |
|---|----------|------|------|------|----------|
| 1 | `3224bf18d` | 04-08 | kevin259 | Initial commit: import from 7u5/hpc-redis | **初始导入**：从 7u5/hpc-redis 项目导入完整代码。包含标准 Redis 源码 + 所有 HPC 新增组件（proxy_aggregator、supernode_worker、sve_compute、ub_client、vector_engine、batch_processor、three_layer_cache 及其 UB 变体、Aeron IPC 头文件、ikcp KCP 协议等）+ 预编译的 ARM .o/.d 文件和二进制。共 2155 个文件、543K+ 行代码。 |
| 2 | `9cdd26b9f` | 04-08 | kevin259 | remove .codeshell | 清理提交：删除 `.codespell/` 目录（包含 `.codespellrc`、`requirements.txt`、`wordlist.txt`），移除 Codespell 拼写检查配置。 |
| 3 | `1dd8bb9a0` | 04-08 | kevin259 | guozhu analysis | 新增分析文档：可能是对代码库或性能的初步分析文档（Guozhu 可能是人名或项目名）。 |
| 4 | `9bef234d6` | 04-08 | kevin259 | remove empty file | 清理空文件。 |
| 5 | `6cb3d7248` | 04-08 | kevin259 | guozhu analysis rename to readme.md | 将分析文档重命名为 `README.md`。这个 README 后来演化成项目的长文档（当前约 2200 行，包含大量会话式开发记录）。 |
| 6 | `b9281670c` | 04-16 | 7u5 | update for v8-v9 | **版本迭代**：更新 V8 和 V9 版本。新增多个 TLC 传输服务器实现（`tlc_aeron_server.c`、`tlc_dpdk_server.c`、`tlc_urma_server.c`、`tlc_unified_server.c`）及其对应二进制。扩展了三层缓存的传输后端支持。引入 `aeron_ipc.h`（Aeron 风格 SPSC Ring Buffer 头文件）、`urma.c/h`（URMA 用户态 RDMA 接口）、`iouring_lite.h`、`kcp_lite.h`。更新 `sve2_gemm.h`（SVE2 矩阵乘法）和 `three_layer_cache.c`。新增 TLC 模块 `src/modules/tlc_module.c`。新增 benchmark 二进制和源码。 |
| 7 | `e81fdc72e` | 04-16 | 7u5 | update for Aeron IPC (data plane)+ UDS(control) 11M 85ns | **关键性能提交**：报告 Aeron IPC 数据面 + UDS 控制面达到 11M QPS / 85ns 延迟。更新 `QUICKSTART_UB_SVE.md`。可能调整了 Aeron 传输层的配置或实现以优化性能。 |
| 8 | `a6792585d` | 04-16 | 7u5 | update for md | 文档更新：可能是 README 或其他 Markdown 文档的排版/内容更新。 |
| 9 | `862b4ee55` | 04-17 | 7u5 | update readme.md | 更新 `README.md`：可能是完善项目说明或追加新的分析内容。 |
| 10 | `8c7f7b9b4` | 04-17 | 7u5 | merge TTAS partial push in libgqm for 150-170ns | **性能优化**：合并 TTAS (Test-Test-And-Set) 部分推送优化到 libgqm 库，目标延迟 150-170ns。TTAS 是一种自旋锁优化策略，先测试再尝试获取锁，减少无效 CAS 操作。这属于 Bitmap CAS 无锁机制的进一步优化。 |
| 11 | `86769a773` | 04-10 | Wei Xu | compile: add compile guide and build script | **构建支持**：新增 `BUILD_GUIDE.md`（294 行完整编译指南，中文，针对 openEuler 22.03 ARM aarch64）和 `hpc-redis-build.sh`（90 行自动化构建脚本）。包括依赖编译步骤、xxhash 手动编译、SVE 编译选项等详细说明。新增 `redis-baseline.conf` 基线配置文件。 |
| 12 | `aa62ec581` | 04-14 | Wei Xu | benchmark: add three layer benchmark explain | **测试文档**：新增 `THREE_LAYER_BENCHMARK_GUIDE.md`（383 行），详述三层缓存基准测试的架构说明、层级特性（HOT 128K/16B → WARM 1M/1200B → COLD 64段）、核心技术（Bitmap CAS、Ring Buffer、Paxos 一致性）、测试场景。新增 `V10_BENCHMARK_REPORT.md` 报告 Aeron IPC 性能：3.79M QPS 单条 / 11.88M keys/s 批量。 |
| 13 | `41135491e` | 04-21 | Wei Xu | add tlc client benchmark script | **基准测试**：新增 `benchmark/run_tlc_client_bench.sh`（314 行）、`benchmark/tlc_client_bench.c`（327 行）、编译后的 `benchmark/tlc_client_bench` 二进制（310KB）。全客户端端到端基准测试，覆盖不同传输后端的性能对比。 |
| 14 | `4bd3996d4` | 04-22 | Wei Xu | add tlc client benchmark log anlysys script | **分析工具**：新增 `benchmark/parse_tlc_bench.py`（366 行），Python 脚本用于解析 TLC 基准测试日志输出，生成结构化对比报告。支持多种传输后端结果的对比分析。 |
| 15 | `84fba6588` | 04-22 | Wei Xu | add dpdk unifedi test script | **测试脚本**：新增 `benchmark/run_dpdk_unified_comparison.sh`（251 行）和 `benchmark/bench_dpdk_client.c`、`benchmark/bench_unified_client.c`。对比 DPDK 传输后端与 Unified 传输后端的性能差异。 |
| 16 | `e8ad88a12` | 04-24 | jialiuyu | claude code init | **初始化 Claude Code 配置**：新增 `CLAUDE.md`（206 行中文架构指南）和 `AGENTS.md`，为 AI 辅助开发工具提供项目上下文。可能是本次会话产生的提交。 |

### 3.2 按工作阶段归纳

**阶段一：代码导入与整理（04-08，提交 1-5）**
- 从外部项目 7u5/hpc-redis 完整导入代码
- 清理无关配置（Codespell）和空文件
- 建立初始 README 文档

**阶段二：多传输后端开发（04-16，提交 6-8）**
- V8-V9 迭代：新增 Aeron、DPDK、URMA、Unified、FC 等传输后端的 TLC 服务器
- 实现 Aeron IPC 数据面 + UDS 控制面双通道，达到 11M QPS / 85ns
- 引入 SVE2 GEMM 矩阵乘法优化、URMA 用户态 RDMA 接口

**阶段三：性能优化（04-17，提交 9-10）**
- 合并 TTAS 自旋锁优化，进一步降低 Bitmap CAS 延迟到 150-170ns 级别
- 文档更新

**阶段四：构建与测试基础设施（04-10 ~ 04-22，提交 11-15）**
- 完整的 ARM 编译指南和自动化构建脚本
- 三层缓存基准测试详细说明和 V10 性能报告
- 端到端客户端基准测试框架（多传输对比）
- 日志分析脚本（Python 解析工具）
- DPDK vs Unified 专项对比测试

**阶段五：AI 辅助开发配置（04-24，提交 16）**
- 创建 `CLAUDE.md` 和 `AGENTS.md` 为 AI 工具提供项目上下文

### 3.3 关键贡献者

| 贡献者 | 邮箱 | 提交数 | 角色 |
|--------|------|--------|------|
| kevin259 | kevin259@noreply.gitcode.com | 5 | 代码导入、项目初始化（GitCode 平台） |
| 7u5 | 7u5@163.com | 5 | 核心架构开发：多传输后端、Aeron IPC、TTAS 优化 |
| Wei Xu | xuwei5@hisilicon.com | 5 | 构建系统、基准测试框架、测试分析工具（华为） |
| jialiuyu | jialiuyu1@gmail.com | 1 | AI 辅助开发配置 |

## 四、性能指标汇总

| 指标 | 传统 Redis | HPC-Redis | 提升 |
|------|-----------|-----------|------|
| 单条 GET QPS | 123K (TCP) | 3,794K (Aeron IPC) | 30.8x |
| 批量 GET keys/s | - | 11,880K (batch=500) | - |
| 每 key 延迟 | 8,100 ns | 264 ns (Aeron IPC) | 30.7x |
| 批量每 key 延迟 | - | 84 ns (batch=500) | - |
| Bitmap CAS 操作延迟 | - | 150-170 ns (TTAS 优化后) | - |

## 五、项目结构快速导航

```
hpc-redis/
├── src/
│   ├── server.c / server.h          # Redis 核心（含 vector_engine 字段）
│   ├── config.c                      # 配置（含 vector-engine 指令）
│   ├── ae.c                          # 事件循环
│   ├── networking.c                  # RESP 协议 + 客户端 I/O
│   ├── iothread.c                    # I/O 多线程
│   ├── vector_engine.c/h             # ★ 向量引擎抽象层
│   ├── batch_processor.c/h           # ★ 批量处理器
│   ├── proxy_aggregator.c/h          # ★ 代理聚合器（独立）
│   ├── supernode_worker.c/h          # ★ 超节点 Worker（独立）
│   ├── sve_compute.c/h               # ★ SVE2 计算引擎
│   ├── sve2_gemm.h                   # ★ SVE2 GEMM（header-only）
│   ├── sve2_adam.h                   # ★ SVE2 Adam 优化器（header-only）
│   ├── ub_client.c/h                 # ★ UB 总线客户端
│   ├── three_layer_cache.c/h         # ★ 三层缓存基础
│   ├── three_layer_cache_ub.c/h      # ★ UB 三层缓存
│   ├── aeron_ipc.h                   # Aeron SPSC Ring Buffer
│   ├── ikcp.c/h                      # KCP 协议
│   ├── tlc_*_server.c                # 各传输后端 TLC 服务器
│   ├── commands/*.json               # Redis 命令定义
│   └── Makefile                      # 构建配置
├── benchmark/
│   ├── Makefile                      # 基准测试构建
│   ├── *_bench.c                     # 各类基准测试源码
│   ├── parse_tlc_bench.py            # 日志分析脚本
│   └── run_*.sh                      # 运行脚本
├── modules/
│   └── vector-sets/                  # 外部 HNSW 向量搜索模块
├── deps/                             # 依赖库
├── BUILD_GUIDE.md                    # ARM 编译指南
├── CLAUDE.md                         # 架构指南（中英）
├── AGENTS.md                         # AI 辅助指南
├── WORK_COMPLETED.md                 # 实现完成报告
├── V10_BENCHMARK_REPORT.md           # 性能报告
├── THREE_LAYER_BENCHMARK_GUIDE.md    # 缓存测试说明
└── hpc-redis-build.sh                # 自动构建脚本
```

★ 标记为本项目新增的 HPC 组件。