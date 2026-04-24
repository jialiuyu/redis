# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

这是 **Redis** — 用 C 语言编写的内存数据结构服务器。代码位于 `redis/` 子目录下。

## 构建命令

所有命令应在 `redis/` 目录下执行。

```bash
# 构建 Redis（首次会编译依赖）
make

# 构建含模块版本（RedisBloom, RediSearch, RedisJSON, RedisTimeSeries, vector-sets）
make BUILD_WITH_MODULES=yes

# 使用 AddressSanitizer 构建
make SANITIZER=address

# 无优化构建（调试用）
make noopt

# 为 Valgrind 构建
make valgrind

# 清理构建产物（保留依赖）
make clean

# 完全清理（含依赖）
make distclean
```

构建产物在 `redis/src/` 下：`redis-server`、`redis-cli`、`redis-benchmark`、`redis-check-rdb`、`redis-check-aof`。`redis-sentinel` 是 `redis-server` 的符号链接。

## 测试命令

测试使用 Tcl 编写，需要 tclsh 8.5+。在 `redis/` 目录下执行。

```bash
# 完整测试套件
make test
# 或直接运行：
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

# 使用 TLS 运行
./runtest --tls

# 详细输出
./runtest -v
```

测试目录：`tests/unit/`、`tests/unit/type/`、`tests/unit/moduleapi/`、`tests/unit/cluster/`、`tests/integration/`、`tests/cluster/`、`tests/sentinel/`。

## 架构

### 核心事件循环
- `src/ae.c` — 事件循环抽象（根据平台使用 epoll/kqueue/evport/select）
- `src/server.c` / `src/server.h` — 主服务器结构体（`redisServer`）、初始化和事件分发。`server.h` 是包含大部分其他头文件的中央头文件
- `src/networking.c` — 客户端连接处理和 RESP 协议解析

### 数据结构
每个 Redis 数据类型都有专用的 `t_*.c` 实现文件：
- `src/t_string.c`、`src/t_list.c`、`src/t_set.c`、`src/t_zset.c`、`src/t_hash.c`、`src/t_stream.c`

内部数据结构：
- `src/sds.c` — 简单动态字符串（Redis 的字符串库）
- `src/dict.c` — 哈希表
- `src/kvstore.c` — 基于 Slot 的哈希表封装（用于集群模式）
- `src/adlist.c` — 双向链表
- `src/quicklist.c` — listpack 的链表（用于 List 类型）
- `src/listpack.c` — 紧凑顺序数据结构
- `src/ziplist.c` — 旧版紧凑列表（部分路径仍在使用）
- `src/rax.c` — 基数树（用于 Stream 和 Key 过期）
- `src/intset.c` — 整数集合编码（用于小型 Set）

### 持久化
- `src/rdb.c` — RDB 快照格式
- `src/aof.c` — AOF（Append Only File）持久化
- `src/replication.c` — 主从复制

### 集群
- `src/cluster.c` / `src/cluster.h` — 核心集群逻辑
- `src/cluster_legacy.c` — 旧版集群实现
- `src/sentinel.c` — Redis Sentinel（高可用）

### 脚本与函数
- `src/eval.c`、`src/script.c` — Lua 脚本（EVAL/EVALSHA）
- `src/functions.c`、`src/function_lua.c` — Redis Functions（命名式持久化脚本）

### 模块系统
- `src/module.c` — 模块 API 实现
- `src/redismodule.h` — 模块 API 头文件
- `src/modules/` — 示例/内置模块源码（helloacl、helloblock 等）
- `modules/` — 完整模块：RedisBloom、RediSearch、RedisJSON、RedisTimeSeries、vector-sets

### 命令定义
- `src/commands/*.json` — 每个命令一个 JSON 文件，定义元数据、参数和标志
- `utils/generate-command-code.py` — 从 JSON 生成 `src/commands.def`
- `src/commands.c` — 包含 `commands.def`（所有 `redisCommand` 结构体）

**添加/修改命令时**：编辑 `src/commands/` 中对应的 JSON 文件，然后重新生成：`python3 utils/generate-command-code.py`

### 关键基础设施
- `src/config.c` — 配置文件和 CONFIG 命令处理
- `src/db.c` — 数据库操作（键空间、查找、过期）
- `src/object.c` — Redis 对象（`robj`）创建和操作
- `src/blocked.c` — 阻塞客户端管理（BLPOP 等）
- `src/evict.c` — 键淘汰策略
- `src/expire.c` / `src/ebuckets.c` / `src/estore.c` — 过期子系统
- `src/bio.c` — 后台 I/O 线程
- `src/iothread.c` — I/O 多线程
- `src/lazyfree.c` — 异步内存释放
- `src/defrag.c` — 内存碎片整理

### 依赖（`deps/` 目录）
- `jemalloc` — 内存分配器（Linux 默认）
- `hiredis` — C 客户端库（redis-cli 和 redis-benchmark 使用）
- `lua` — Lua 脚本引擎
- `linenoise` — redis-cli 的行编辑
- `hdr_histogram` — 延迟追踪直方图
- `fpconv`、`fast_float` — 浮点转换
- `xxhash` — 哈希函数（用于 DIGEST 等新命令）

## 代码约定

- C99/C11（当可用时使用 `_Atomic` 用于 vector-sets 模块）
- RESP 协议（REdis Serialization Protocol）用于客户端-服务端通信
- 内存管理使用 `zmalloc` 包装器（`src/zmalloc.c`）跟踪总内存使用量
- 主服务器状态位于全局 `struct redisServer server` 中，定义在 `server.h`
- 许可证：RSALv2/SSPLv1/AGPLv3（三重许可）

---

## 项目原理

### 整体架构

Redis 本质上是一个**单线程事件驱动**的内存键值数据库（I/O 多线程为可选增强），其核心运行原理：

1. **事件循环**（`ae.c`）：基于 `epoll`/`kqueue` 实现的 I/O 多路复用，在单个主线程中处理所有客户端连接、命令执行和定时任务
2. **RESP 协议**：客户端与服务端之间的通信协议，支持简单字符串、错误、整数、批量字符串、数组五种类型
3. **命令表驱动**：每个命令在 `commands/*.json` 中定义元数据，通过代码生成器自动生成 `commands.def`，命令执行通过查表分发
4. **数据类型抽象**：所有值统一封装为 `robj`（Redis Object），包含类型、编码、引用计数和 LRU 信息，同一类型可使用不同编码（如 Hash 可以是 listpack 或 dict 编码）
5. **内存管理**：使用 `zmalloc` 包装器跟踪内存总量，Linux 默认使用 jemalloc 分配器，支持内存碎片整理（defrag）

### 数据流

```
客户端请求 → RESP 解析（networking.c）→ 命令查找（commands.def）→
命令执行（t_*.c / db.c）→ 键空间操作（dict / kvstore）→
响应编码 → 事件循环写回客户端
```

### 持久化机制

- **RDB**：时间点快照，fork 子进程写磁盘，COW（Copy-On-Write）方式避免阻塞主线程
- **AOF**：追加写入日志，支持每秒 fsync 或每次写入 fsync，后台自动重写压缩
- **复制**：异步主从复制，支持部分重同步（PSYNC）

---

## 相较于传统 Redis 的新优化

以下是基于源码分析，本版本相较于传统 Redis（7.x 及更早版本）引入的重要新特性与优化：

### 1. 全新过期子系统（E-Buckets + EStore + Fenwick Tree）

传统 Redis 的过期机制使用简单的采样方式：随机抽取 20 个带 TTL 的 key 检查是否过期。本版本引入了全新的分层过期管理架构：

- **E-Buckets**（`src/ebuckets.c`）：基于 rax 树（基数树）+ 分段链表的过期时间索引。将带 TTL 的项按时间区间组织成桶（bucket），桶内使用段（segment）链表聚合。当段满时自动分裂桶，实现时间粒度的自适应细化。相比传统采样，可以实现**精确的主动过期扫描**，避免遗漏
- **Fenwick Tree / 二叉索引树**（`src/fwtree.c`）：O(log n) 时间复杂度的前缀和维护，支持快速定位第 N 个过期项，跳过空桶，避免在大量空桶上浪费时间
- **EStore**（`src/estore.c`）：在 E-Buckets 基础上构建的过期存储管理器，使用 Fenwick Tree 跟踪各桶的累计计数。支持集群模式下的分槽管理

**意义**：传统 Redis 的 `expire` 采样方式在大规模数据下容易导致内存泄漏（大量过期 key 未被及时清理）。新系统实现了高效的索引化过期管理。

### 2. Hash 字段级过期（HFE - Hash Field Expiration）

传统 Redis 只支持 Key 级别的 TTL。本版本引入了 Hash 字段级别的过期：

- **Entry 系统**（`src/entry.c` / `src/entry.h`）：紧凑的 field-value 对打包结构，支持 3 种内存布局——小字段嵌入值、中等字段带可选过期元数据、大字段使用指针引用值
- 配合 EStore 管理每个 Hash 对象内部的字段过期
- 新命令：`HSETEX`（设置字段并带过期）、`HGETEX`（获取字段并设置过期）、`HGETDEL`（获取并删除字段）、`HEXPIRE`/`HPEXPIRE`/`HTTL` 等

**意义**：实现 Hash 内部细粒度的生命周期管理，非常适合会话存储、带 TTL 的属性字段等场景。

### 3. MSTR（M-String）不可变字符串与元数据系统

- **MSTR**（`src/mstr.c` / `src/mstr.h`）：不同于 SDS（可变字符串），MSTR 是**不可变字符串**，但可附加元数据。内存布局为：`[元数据字段...] [元数据标志 16bit] [头部 | 字符串 | \0]`
- 通过 `mstrKind` 定义不同"种类"的 mstr，每种有自己的元数据布局（如 Hash 字段种类可附加 TTL 元数据）
- 每个实例通过 16 位标志位表示哪些元数据被实际附加，实现**按需分配**的内存优化

**意义**：为未来将 Key 级别的 TTL、LRU、引用计数、dictEntry 等元数据聚合到单一连续内存分配奠定基础，减少内存碎片和指针跳转。

### 4. Key 元数据框架（KeyMeta）

- **KeyMeta**（`src/keymeta.c` / `src/keymeta.h`）：统一的 Key 元数据管理框架
- 支持最多 8 种元数据类别，ID 0 保留给 TTL/过期
- 每个类别注册回调函数处理 copy、rename、unlink、free、RDB/AOF 持久化、defrag 等生命周期事件
- 模块可通过 `keyMetaClassCreate` 注册自定义元数据类别
- 每个键的 8 字节 slot 可以内联存储数据或指向外部结构

**意义**：传统 Redis 中 Key 的元数据（TTL、LRU 等）硬编码在 `robj` 和 `dictEntry` 中。新框架实现了元数据的可扩展、可插拔管理。

### 5. 向量集合（Vector Sets）数据类型

- **modules/vector-sets/**：全新的 Redis 数据类型，用于向量相似性搜索
- 基于 **HNSW**（分层可导航小世界图）算法实现高效的近似最近邻（ANN）搜索
- 支持 FP32 和二进制量化
- 支持过滤表达式进行条件搜索
- 命令：`VADD`（添加向量）、`VSIM`（相似性搜索）、`VSETATTR`（设置属性）

**意义**：原生支持向量搜索能力，无需外部模块（如 RedisStack 的 RediSearch），直接在 Redis 内实现 RAG、语义缓存等 AI 应用场景。

### 6. 内存预取优化（Memory Prefetch）

- **memory_prefetch.c**：命令批处理预取机制
- 在处理命令时，批量预取即将访问的键、哈希表条目和值到 CPU 缓存
- 使用状态机管理预取流程，可配置批量大小
- 将多次随机内存访问的缓存未命中开销**分摊**到批量操作中

**意义**：减少 CPU 缓存未命中，在大吞吐量场景下提升命令处理性能。

### 7. 新增实用命令

| 命令 | 用途 |
|------|------|
| `GCRA` | 基于 Generic Cell Rate Algorithm 的内置限流，支持突发流量，替代外部模块 redis-cell |
| `HOTKEYS` | 热点 Key 追踪（START/STOP/RESET/GET），帮助识别高频访问的 Key |
| `DIGEST` | 基于 XXH3 的字符串值哈希校验 |
| `MSETEX` | 批量设置多个 Key-Value 并带过期时间 |
| `SFLUSH` | 清空 Set 所有元素 |
| `DELEX` | 增强的删除命令 |
| `XACKDEL` / `XDELEX` / `XNACK` | Stream 消息的确认+删除、按 ID 删除、负面确认 |

### 8. 集群增强

- **CLUSTER MIGRATION**：结构化的 Slot 迁移管理（启动、监控、取消）
- **CLUSTER SLOT-STATS**（`src/cluster_slot_stats.c`）：Slot 级别的统计信息收集
- **CLUSTER SYNCSLOTS**：Slot 同步操作
- `cluster_asm.c`：汇编优化的集群操作（性能关键路径）

### 9. KVStore 优化

- **KVStore**（`src/kvstore.c`）：集群模式下，将同一个 Hash Slot 的键存储在独立的 dict 中，而不是所有键共享一个 dict
- 配合 Fenwick Tree 实现高效的跨 dict 操作：快速定位包含第 N 个 key 的 dict、遍历非空 dict、负载均衡和随机 Key 选择
- 支持空 dict 自动释放（`KVSTORE_FREE_EMPTY_DICTS`）

**意义**：集群模式下的 Slot 操作从全表扫描优化为 O(log n) 精确定位，大幅提升 `SCAN`、`RANDOMKEY`、`DBSIZE` 等操作的性能。

### 10. 新依赖引入

- **xxhash**：高性能哈希函数，用于 `DIGEST` 命令等需要快速哈希计算的场景
- **fast_float**：快速字符串到浮点数解析库
