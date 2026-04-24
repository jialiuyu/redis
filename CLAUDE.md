# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

这是一个基于 Redis 的 **高性能向量计算引擎**（HPC-Redis），在 Redis 上层构建了超大规模向量检索系统。核心特性包括：

- **UB（Unified Bus）高速总线**：用户态零拷贝通信，4TB 共享内存池
- **ARM SVE/SVE2 加速**：利用可伸缩向量扩展进行批量向量运算（余弦相似度、GEMM 等）
- **超节点（SuperNode）架构**：Bitmap CAS 无锁并发 + SVE2 Gather Load 批量读取
- **三层缓存系统（Three-Layer Cache）**：HOT(16B 索引) → WARM(1200B 值) → COLD(追加写)，支持多种传输后端
- **智能代理聚合器（Proxy Aggregator）**：微秒级批量聚合，一致性哈希分发到超节点
- **向量引擎抽象层（Vector Engine）**：统一 Redis HNSW 与 UB 总线两种向量搜索后端

## 构建命令

所有命令在项目根目录执行。Makefile 会委托给 `src/Makefile`。

```bash
# 标准构建
make

# 含模块构建（RedisBloom, RediSearch, RedisJSON, RedisTimeSeries, vector-sets）
make BUILD_WITH_MODULES=yes

# AddressSanitizer 构建
make SANITIZER=address

# 无优化调试构建
make noopt

# Valgrind 构建
make valgrind

# 清理构建产物（保留依赖）
make clean

# 完全清理（含依赖）
make distclean
```

构建产物在 `src/` 下：`redis-server`、`redis-cli`、`redis-benchmark`、`redis-check-rdb`、`redis-check-aof`。`redis-sentinel` 是 `redis-server` 的符号链接。

注意：`vector_engine.o`、`ub_client.o`、`sve_compute.o`、`batch_processor.o` 已集成到 `redis-server` 的标准链接列表中。

## 测试命令

测试使用 Tcl 编写，需要 tclsh 8.5+。

```bash
# 完整测试套件
make test
# 或直接：
./runtest

# 模块 API 测试
./runtest-moduleapi

# Sentinel 测试
./runtest-sentinel

# 集群测试
./runtest-cluster

# 运行单个测试文件
./runtest --single unit/type/string

# 对外部服务器运行测试
./runtest --host <host> --port <port>

# 详细输出
./runtest -v
```

测试目录：`tests/unit/`、`tests/unit/type/`、`tests/integration/`、`tests/cluster/`、`tests/sentinel/`、`tests/vectorset/`。

## 架构

### 数据流

```
客户端请求 → RESP 解析（networking.c）→ 命令查找 → 命令执行 →
  ↓ 向量相关
Vector Engine（vector_engine.c）→ Batch Processor（batch_processor.c）→
Proxy Aggregator（proxy_aggregator.c）→ SuperNode Worker（supernode_worker.c）→
SVE2 计算（sve_compute.c）→ UB 总线（ub_client.c）→ 三层缓存（three_layer_cache*.c）
```

### 核心事件循环（继承自 Redis）

- `src/ae.c` — 事件循环（epoll/kqueue/evport/select）
- `src/server.c` / `src/server.h` — 主服务器结构体 `redisServer`，入口 `main()` 在 server.c
- `src/networking.c` — 客户端连接与 RESP 协议解析

### 向量计算引擎（本项目新增）

| 文件 | 功能 |
|------|------|
| `src/vector_engine.c/h` | 向量引擎抽象层，统一 Redis HNSW 和 UB 两种后端，提供 `vadd`/`vrem`/`vsim`/`vemb` 接口 |
| `src/batch_processor.c/h` | 高吞吐量批量处理，50μs 超时机制，最大 1024 批次 |
| `src/proxy_aggregator.c/h` | 智能代理聚合器，一致性哈希(MurmurHash3)分片，3000-6000 请求/批，零拷贝 Ring Buffer |
| `src/supernode_worker.c/h` | 超节点计算引擎，Bitmap CAS 无锁并发，SVE2 Gather Load，UB.mem 共享内存池直接访问 |
| `src/sve_compute.c/h` | ARM SVE/SVE2 加速向量运算，1M embedding 缓存，支持余弦相似度/GEMM/Adam 优化器 |
| `src/sve2_gemm.h` | SVE2 矩阵乘法 |
| `src/sve2_adam.h` | SVE2 Adam 神经网络优化器 |
| `src/ub_client.c/h` | UB 总线客户端，用户态零拷贝通信，支持 ComputeNode/MemoryTile/FabricManager 实体类型 |

### 三层缓存系统

| 文件 | 功能 |
|------|------|
| `src/three_layer_cache.c/h` | 三层缓存基础实现（HOT 索引 → WARM 值 → COLD 追加写） |
| `src/three_layer_cache_ub.c/h` | UB 共享内存后端，SVE2 融合计算（Gather + Dot-Product），一致性哈希分片 |
| `src/tlc_server.c` | 缓存服务器主逻辑 |
| `src/tlc_unified_server.c` | 统一传输层服务器 |
| `src/tlc_aeron_server.c` | Aeron 传输后端 |
| `src/tlc_dpdk_server.c` | DPDK 传输后端 |
| `src/tlc_fc_server.c` | FC 传输后端 |
| `src/tlc_urma_server.c` | URMA 传输后端 |
| `src/tlc_v10_server.c` | V10 版本服务器 |
| `src/tlc_v13_server.c` | V13 版本服务器 |
| `src/tlc_v14_server.c` | V14 版本服务器 |

### IPC 与通信

| 文件 | 功能 |
|------|------|
| `src/aeron_ipc.h` | Aeron 风格无锁共享内存消息总线，SPSC Ring Buffer，目标延迟 < 0.1μs |
| `src/ikcp.c` | KCP 协议实现（可靠 UDP） |
| `src/urma.c` | URMA（用户态 RDMA）接口 |

### Redis 核心数据结构

每个 Redis 数据类型有专用 `t_*.c` 文件：`t_string.c`、`t_list.c`、`t_set.c`、`t_zset.c`、`t_hash.c`、`t_stream.c`

内部数据结构：
- `src/sds.c` — 简单动态字符串
- `src/dict.c` — 哈希表
- `src/kvstore.c` — 基于 Slot 的哈希表（集群模式）
- `src/quicklist.c` / `src/listpack.c` — List 的紧凑存储
- `src/rax.c` — 基数树（Stream 和 Key 过期使用）
- `src/ebuckets.c` / `src/estore.c` / `src/fwtree.c` — E-Buckets 分层过期管理 + Fenwick Tree 索引
- `src/mstr.c` / `src/entry.c` — MSTR 不可变字符串与 Hash 字段级过期

### 持久化与复制

- `src/rdb.c` — RDB 快照
- `src/aof.c` — AOF 追加持久化
- `src/replication.c` — 主从复制

### 集群

- `src/cluster.c` / `src/cluster.h` — 核心集群逻辑
- `src/cluster_legacy.c` — 旧版集群实现
- `src/cluster_slot_stats.c` — Slot 级别统计
- `src/sentinel.c` — Redis Sentinel 高可用

### 命令定义

- `src/commands/*.json` — 每个命令一个 JSON 文件
- `utils/generate-command-code.py` — 从 JSON 生成 `src/commands.def`
- 修改命令时：编辑 JSON → 运行 `python3 utils/generate-command-code.py`

### 模块系统

- `src/module.c` — 模块 API 实现
- `src/redismodule.h` — 模块 API 头文件
- `src/modules/` — 内置模块示例（helloacl、helloblock 等），含 `tlc_module.c`（三层缓存模块）
- `modules/` — 完整外部模块：RedisBloom、RediSearch、RedisJSON、RedisTimeSeries、vector-sets（HNSW 向量搜索）

### 其他关键基础设施

- `src/config.c` — 配置处理
- `src/db.c` — 数据库操作（键空间、查找、过期）
- `src/object.c` — Redis 对象（`robj`）
- `src/blocked.c` — 阻塞客户端管理
- `src/evict.c` — 键淘汰
- `src/expire.c` — 过期子系统
- `src/keymeta.c` — Key 元数据可扩展框架
- `src/hotkeys.c` — 热点 Key 追踪
- `src/memory_prefetch.c` — 命令批处理预取优化
- `src/iothread.c` — I/O 多线程
- `src/defrag.c` — 内存碎片整理

### 依赖（`deps/` 目录）

- `jemalloc` — 内存分配器（Linux 默认）
- `hiredis` — C 客户端库
- `lua` — Lua 脚本引擎
- `linenoise` — 行编辑
- `hdr_histogram` — 延迟直方图
- `fpconv`、`fast_float` — 浮点转换
- `xxhash` — 哈希函数

## 代码约定

- C99/C11（`_Atomic` 用于 vector-sets 模块，需要 C11 编译器）
- RESP 协议用于客户端-服务端通信
- 内存管理使用 `zmalloc` 包装器（`src/zmalloc.c`）跟踪总内存
- 主服务器状态位于全局 `struct redisServer server`（`server.h`）
- 许可证：RSALv2/SSPLv1/AGPLv3 三重许可

## Benchmark

`benchmark/` 目录包含性能测试：Redis 基线、SVE2 性能分析、三层缓存各版本基准测试、超节点性能测试、多种传输协议对比（Aeron、KCP、DPDK 等）。
