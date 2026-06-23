# VEMB V16 SHM mixed-80r20w Benchmark

## 运行指令

### Server

```bash
./src/vemb_v16_server \
  --transport aeron \
  --socket /tmp/vemb_v16.sock \
  --proxy-io-threads 16 \
  --supernode-workers 32 \
  --vector-region /vemb_v16_vectors \
  --warm-backend shm \
  --dim 300 \
  --max-vectors 131072 \
  --loglevel nothing
```

### Bench

```bash
./benchmark/vemb_v16_bench \
  --transport aeron \
  --socket /tmp/vemb_v16.sock \
  --mode mixed-80r20w \
  --dim 300 \
  --prefill 65536 \
  --ops 200000 \
  --threads 4,8,16,32,64 \
  --pipeline 16 \
  --timeout-ms 30000 \
  --no-pin
```

## 固定配置

| 参数 | 值 |
| --- | ---: |
| transport | `shm` |
| mode | `mixed-80r20w` |
| proxy-io-threads | `16` |
| supernode-workers | `32` |
| vector-region | `/vemb_v16_vectors` |
| warm-backend | `shm` |
| dim | `300` |
| prefill | `65536` |
| ops/thread | `200000` |
| pipeline | `16` |
| timeout-ms | `30000` |
| pin | `no` |
| write ratio | `20%` |

## 吞吐结果

| Threads | Requests | OK | Fail | QPS | Avg ns/op | VEMB | VADD |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 800,000 | 800,000 | 0 | 4,503,764.33 | 220.3 | 640,000 | 160,000 |
| 8 | 1,600,000 | 1,600,000 | 0 | 5,414,712.47 | 184.4 | 1,280,000 | 320,000 |
| 16 | 3,200,000 | 3,200,000 | 0 | 5,925,714.07 | 168.5 | 2,560,000 | 640,000 |
| 32 | 6,400,000 | 6,400,000 | 0 | 7,067,416.95 | 141.2 | 5,120,000 | 1,280,000 |
| 64 | 12,800,000 | 12,800,000 | 0 | 7,096,721.24 | 140.7 | 10,240,000 | 2,560,000 |

## 关键观测指标

| Threads | response_empty_polls | bitmap_lock_failure | table_lookup_avg_ns | bitmap_lock_avg_ns | bitmap_unlock_avg_ns | vector_load_avg_ns | completion_publish_avg_ns |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 186,296,366 | 331 | 220.5 | 115.1 | 40.3 | 58.3 | 95.0 |
| 8 | 910,082,746 | 22,800 | 409.0 | 176.3 | 75.9 | 59.6 | 128.7 |
| 16 | 5,533,511,102 | 33,126 | 1,144.9 | 312.0 | 125.9 | 56.5 | 109.9 |
| 32 | 26,310,383,630 | 269,076 | 2,182.6 | 688.5 | 233.7 | 57.9 | 106.0 |
| 64 | 87,291,863,691 | 187,442 | 2,221.8 | 582.7 | 210.9 | 59.1 | 104.2 |

## 稳定性信号

| 指标 | 结果 |
| --- | --- |
| fail | 全部为 `0` |
| ok | 全部 `100%` |
| request_publish_spins | 全部为 `0` |
| vemb_ring full | 全部为 `0` |
| vadd_ring full | 全部为 `0` |
| response_ring full | 全部为 `0` |
| completion_ring full | 全部为 `0` |
| active_channels | 每轮结束均为 `0` |
| published/completed | 每轮均等于 total requests |

## 结论

| 项 | 结论 |
| --- | --- |
| 峰值吞吐 | `64 threads` 最高，约 `7.10M QPS` |
| 性价比拐点 | `32 threads` 已达到约 `7.07M QPS`，与 64 线程非常接近 |
| 推荐日常跑法 | 使用 `--threads 32`，吞吐接近峰值且等待轮询更少 |
| 推荐冲峰值跑法 | 使用 `--threads 64` |
| 队列压力 | 无 ring full，队列容量不是当前主要瓶颈 |
| 主要瓶颈信号 | 高并发下 `table_lookup_avg_ns` 和 `bitmap_lock_avg_ns` 明显升高 |

阶段性判断：当前 `shm + pooled 16/32 + mixed-80r20w` 路径稳定，吞吐在 32 线程后进入平台期。继续提升峰值时，应优先关注表查找成本、bitmap 锁争用和高并发下 response 等待轮询。
