# SuperNode Ring Buffer Batch/Worker 测试结果

## 测试对象

本次新增的验证程序：

- 原语级单元测试：
  [ring_buffer_ut.c](/Users/szza/codespace/work/hpc-redis/benchmark/ring_buffer_ut.c)
- batch/worker 集成压测：
  [ring_buffer_batch_bench.c](/Users/szza/codespace/work/hpc-redis/benchmark/ring_buffer_batch_bench.c)

## 1. ring_buffer_ut

### 运行方式

```bash
make -C benchmark ring_buffer_ut
./benchmark/ring_buffer_ut
```

### 覆盖范围

| 项目 | 说明 |
|------|------|
| basic push/pop | 基本消息写入/读取 |
| reserve/commit_write | producer 原地写入路径 |
| peek/commit_read | consumer 零拷贝读取路径 |
| padding-record wraparound | 尾部空间不足时 padding 回绕 |

### 结果

| 项目 | 值 |
|------|----|
| 执行结果 | `ring_buffer_ut: all tests passed` |
| 结论 | ring buffer 原语级路径在当前覆盖范围内通过验证 |

## 2. ring_buffer_batch_bench

### 运行方式

```bash
make -C benchmark ring_buffer_batch_bench
./benchmark/ring_buffer_batch_bench
```

### 覆盖范围

| 项目 | 说明 |
|------|------|
| per-worker queue routing | batch 按 worker 维度稳定路由 |
| in-place batch packet write | batch packet 在 queue 内原地构造 |
| zero-copy worker consume | worker 通过 `peek + commit_read` 直接消费 payload |
| padding wraparound under load | 批量流量下的 padding 回绕稳定性 |

### 实测结果

| 指标 | 值 |
|------|----|
| workers | 4 |
| batches | 50000 |
| requests per batch | 32 |
| consumed batches | 50000 |
| consumed requests | 1600000 |
| total time | 8.820 ms |
| batch throughput | 5.67 M batch/s |
| request throughput | 181.41 M req/s |

### 结果解读

1. `consumed_batches == 50000`，说明 batch 没有丢失。
2. `consumed_requests == 1600000`，说明 request 计数完整。
3. 4 worker 下，`per-worker queue + zero-copy consume + padding wraparound` 路径能够稳定完成。
4. 当前 standalone 集成压测下，队列侧吞吐达到 `181.41 M req/s`。

## 综合结论

1. ring buffer 重构后的原语级接口已经通过单元测试。
2. batch/worker 主路径已经通过 standalone 集成压测。
3. 当前验证结果支持如下判断：
   - `per-worker queue` 路由有效
   - `SPSC` 去锁主路径有效
   - `zero-copy` 读写主路径有效
   - `padding record` 回绕逻辑有效
4. 在进入更复杂的 Redis 端到端联调前，当前这组测试足以证明 ring buffer 主路径已经具备可用性和一致性。 
