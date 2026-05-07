# SuperNode Ring Buffer 测试结果

## 测试范围

当前针对 ring buffer 重构做了两层验证：

1. 原语级单元测试
   文件：[ring_buffer_ut.c](/Users/szza/codespace/work/hpc-redis/benchmark/ring_buffer_ut.c)
2. batch/worker 路径级集成压测
   文件：[ring_buffer_batch_bench.c](/Users/szza/codespace/work/hpc-redis/benchmark/ring_buffer_batch_bench.c)

## 1. 原语级单元测试

### 构建与运行

```bash
make -C benchmark ring_buffer_ut
./benchmark/ring_buffer_ut
```

### 覆盖点

| 测试项 | 说明 |
|--------|------|
| basic push/pop | 验证最基本的消息写入和读取路径 |
| reserve/commit_write + peek/commit_read | 验证零拷贝读写接口主路径 |
| padding-record wraparound | 验证尾部空间不足时写入 padding 并回绕读取 |

### 结果

| 项目 | 结果 |
|------|------|
| 执行结果 | `ring_buffer_ut: all tests passed` |
| 结论 | 原语级接口在当前覆盖范围内工作正常 |

## 2. batch/worker 集成压测

### 构建与运行

```bash
make -C benchmark ring_buffer_batch_bench
./benchmark/ring_buffer_batch_bench
```

### 覆盖点

| 测试项 | 说明 |
|--------|------|
| per-worker queue routing | 验证 batch 被稳定路由到对应 worker queue |
| in-place batch packet write | 验证 producer 原地填充 batch packet |
| zero-copy worker consume | 验证 worker 通过 `peek + commit_read` 直接消费 payload |
| padding wraparound under batch traffic | 验证批量消息场景下的 padding 回绕稳定性 |

### 结果表

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

1. `consumed_batches` 与 `batches` 完全一致，说明 batch 没有丢失。
2. `consumed_requests` 与 `50000 * 32` 完全一致，说明 request 计数没有缺口。
3. 在 4 worker、50000 个 batch 的压力下，`per-worker queue + zero-copy consume + padding wraparound` 路径能够稳定完成。
4. 当前 standalone 集成压测下，队列侧吞吐达到 `181.41 M req/s`。

## 综合结论

1. ring buffer 原语级路径已经通过单元测试验证。
2. batch/worker 主路径已经通过 standalone 集成压测验证。
3. 当前重构版本已经具备：
   `per-worker queue`、`SPSC 去锁`、`零拷贝主路径`、`padding 回绕`、`packet 瘦身第一版`
4. 在继续进入更复杂的 Redis 端到端联调前，当前这组测试结果足以证明 ring buffer 重构的主路径是自洽的。 
