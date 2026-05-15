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

详细架构参考见 [`architecture.md`](architecture.md)，包含：组件清单、API 签名、数据流、层间交互、file:line 引用、Gotchas。

**涉及 HPC 组件（vector_engine、batch_processor、proxy_aggregator、supernode_worker、sve_compute、ub_client、three_layer_cache）的任何工作，开始前必须先阅读 `architecture.md` 中对应章节。**

数据流概要：
```
Client (RESP) → networking.c → vector_engine.c → batch_processor.c →
proxy_aggregator.c → supernode_worker.c → sve_compute.c → ub_client.c → three_layer_cache*.c
```

## 语言与沟通偏好

- **对话与文档**：默认使用中文回复和撰写文档。对于行业标准术语（如 API 名称、数据结构、设计模式、技术专有名词），保留英文原文以保持准确性。例如："一致性哈希"比"consistent hashing"更自然就用中文，但"HNSW index"比"分层可导航小世界图"更清晰就保留英文。
- **代码注释**：全部使用英文，遵循 C 语言注释规范：
  - 使用 `/* */` 风格，不使用 `//`（与 Redis 原有代码风格一致）
  - 函数级注释写在函数定义上方，说明 purpose、parameters、return value
  - 行内注释简短精炼，只解释"为什么"而非"做什么"
  - 避免过度注释，代码本身应当自解释

## Git 工作流

- **完成工作后自动提交并推送**：每次完成一个任务（功能实现、bug 修复、重构等）后，主动 `git add` 相关文件、`git commit`、`git push`，无需等待用户指示。
- **Commit message 规范**：
  - 使用英文撰写
  - 格式：`<type>(<scope>): <description>`
  - type：`feat` / `fix` / `refactor` / `perf` / `test` / `docs` / `chore`
  - scope：模块或子系统名称（如 `vector_engine`、`sve_compute`、`tlc`、`proxy`、`ub`、`supernode`）
  - description：简短说明做了什么（祈使句），必要时在 body 中补充 why
  - 示例：`feat(vector_engine): add batch embedding lookup with SVE2 gather load`
- **不要自动创建 PR**，由用户自行决定何时创建。
- **不要 force push、不要 reset --hard、不要 rebase 已推送的提交**。

## 代码约定

- C99/C11（`_Atomic` 用于 vector-sets 模块，需要 C11 编译器）
- RESP 协议用于客户端-服务端通信
- 内存管理使用 `zmalloc` 包装器（`src/zmalloc.c`）跟踪总内存
- 主服务器状态位于全局 `struct redisServer server`（`server.h`）
- 许可证：RSALv2/SSPLv1/AGPLv3 三重许可

## Benchmark

`benchmark/` 目录包含性能测试：Redis 基线、SVE2 性能分析、三层缓存各版本基准测试、超节点性能测试、多种传输协议对比（Aeron、KCP、DPDK 等）。
