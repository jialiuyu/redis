# V10 Final: Aeron IPC Default Transport + MGET SVE2 Gather

## 最终性能 (8 threads, 1200B values, 1.1M entries)

### 单条 GET/PUT

| Transport | 80R/20W QPS | Latency | vs Redis |
|-----------|------------:|--------:|---------:|
| Baseline Redis (TCP) | 123K | 8,100 ns | 1x |
| UDS (no pipeline) | 500K | 2,002 ns | 4.1x |
| **Aeron IPC (8 ch)** | **3,794K** | **264 ns** | **30.8x** |

### MGET 批量 + SVE2 Gather (Aeron IPC)

| Batch Size | Keys/s | 每批延迟 | 每 key 均摊 |
|-----------:|-------:|---------:|------------:|
| 100 | **11.77M** | 8.5 μs | 85 ns |
| 500 | **11.88M** | 42.1 μs | 84 ns |
| 1000 | **9.45M** | 105.8 μs | 106 ns |
| 3000 | 1.75M | 213.9 μs | 71 ns* |

*batch=3000 吞吐下降因为 3.6MB 响应超出 large ring slot 容量，部分数据丢失。

### 最佳配置

| 场景 | 推荐 | QPS/吞吐 | 延迟 |
|------|------|------:|-----:|
| **单条低延迟** | Aeron IPC | **3.79M QPS** | **264 ns** |
| **批量高吞吐** | Aeron MGET batch=500 | **11.88M keys/s** | **84 ns/key** |
| 跨机器 | TCP MGET | 9.92M keys/s | 101 ns/key |

## 延迟分解 (Aeron IPC GET, 264ns)

| 阶段 | 耗时 |
|------|-----:|
| Client: memcpy 9B → ring slot | ~5 ns |
| Client: atomic_store tail | ~8 ns |
| Server: atomic_load tail | ~8 ns |
| Server: memcpy 9B from slot | ~5 ns |
| **Server: tlc_get()** | **~100 ns** |
| Server: memcpy 1201B → resp slot | ~100 ns |
| Server: atomic_store tail | ~8 ns |
| Client: atomic_load tail | ~8 ns |
| Client: memcpy 1201B from slot | ~22 ns |
| **Total** | **~264 ns** |

## 架构

```
┌─────────────────────────────────────────────────────────────┐
│  Data Plane: Aeron IPC (DEFAULT, zero-syscall)              │
│                                                              │
│  Client[0]  ←→  /dev/shm/aeron_tlc_{req,resp,lresp}_0      │
│  Client[1]  ←→  /dev/shm/aeron_tlc_{req,resp,lresp}_1      │
│  ...        ←→  ...                                         │
│  Client[N]  ←→  /dev/shm/aeron_tlc_{req,resp,lresp}_N      │
│                                                              │
│  Per-channel: SPSC ring (no lock, no CAS, no syscall)       │
│  GET/PUT: small ring (24KB msg)                              │
│  MGET: large ring (3.6MB msg) + SVE2 8-ahead prefetch       │
│                                                              │
├─────────────────────────────────────────────────────────────┤
│  Control Plane: UDS /tmp/tlc.sock                            │
│  FILL, STATS, channel allocation                             │
├─────────────────────────────────────────────────────────────┤
│  Three-Layer Cache (UB Memory)                               │
│  HOT (lock-free 16B) → WARM (bitmap-CAS) → COLD             │
│  Consistent hash, 4 UB nodes, 59% local access              │
└─────────────────────────────────────────────────────────────┘
```

## 启动

```bash
cd /sharedata/qiuwu/redis
rm -f /tmp/tlc.sock /dev/shm/aeron_tlc_*
./src/tlc-aeron-server &
./benchmark/tlc_aeron_bench --ops 1000000 --threads 8
kill %1
```
