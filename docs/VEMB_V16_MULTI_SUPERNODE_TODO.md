# VEMB V16 多 SuperNode TODO

日期：2026-05-19

## 第一版限制

```text
启动时固定 SuperNode 列表
启动时构建 consistent hash ring
不支持在线迁移
不支持跨 shard VSIM fan-out
```

## 后续阶段

```text
控制面更新 consistent hash ring
双写/迁移窗口
跨 shard VSIM fan-out/fan-in
SuperNode 故障摘除
按 shard 统计 QPS/latency
```
