# Optimization Report Summary

本文汇总当前 VEMB v16 独立数据面优化状态，作为 `docs/` 目录保留的总览入口。

## 当前保留文档

- `VEMB_V16_STANDALONE_DATAPLANE_DESIGN.md`：单 SuperNode standalone 数据面设计。
- `VEMB_V16_MULTI_SUPERNODE_HASH_RING_DESIGN.md`：多 SuperNode + consistent hash ring 生产形态设计。
- `VEMB_V16_PIPELINE_BATCH_BITMAP_ANALYSIS.md`：pipeline、batch、bitmap 压测结论。
- `VEMB_V16_PHASE2_AERON_RING_DESIGN.md`：Aeron-style ring 第二阶段设计。
- `VEMB_V16_IMPLEMENTATION_TODO.md`：落地任务与阶段计划。
- `VEMB_V16_MULTI_SUPERNODE_TODO.md`：多 SuperNode 后续控制面与迁移计划。
- `BITMAP_OPT_TABLE.md`：bitmap 实现选择和压测结论。
- `ATOMIC_MEMORY_ORDER_GUIDE.md`：原子内存序使用说明。

## 当前优化结论

1. 新架构已经绕开 Redis command、RESP/TCP、module callback、blocked-client/unblock 路径。
2. 热路径变成 `client -> proxy -> SuperNode -> completion -> proxy -> client`。
3. `vemb_v16_bench --pipeline 16` 能显著提升吞吐，说明 client pipeline 能让 proxy/SuperNode batch drain 吃到连续请求。
4. `mixed-80r20w` 当前结果已经超过 `tlc_v16_bench` 的 80R/20W 基准，但该结论只适用于当前 standalone/local dataplane。
5. 高并发下最明确的扩展性信号仍是 bitmap lock/unlock 成本升高。

## 当前限制

当前测试报告还不是完整生产形态结果：

- 没有 multi-SuperNode shard。
- 没有 consistent hash ring 热路径路由。
- 没有跨 SuperNode completion fan-in / demux。
- 没有 SuperNode 增删、rebalance、route snapshot。
- vector region 当前是 POSIX shm mmap，不是真实 UB region。

## 下一步重点

1. 增加 batch round / avg batch / max batch 真实度量。
2. 优化 SuperNode 内部 batch execute，而不是只 batch poll。
3. 优化 bitmap read-mostly 路径和按 bitmap word 分组。
4. 落地 consistent hash ring + 多 SuperNode shard 后重新压测。
