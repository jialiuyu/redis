$ ssh -p 22 root@192.168.90.111
$ directory:
$ - /root/szz/codespace/hpc-redis
$ - /root/FlameGraph/flamegraph.pl: only for redis-server
$ run
```
  make -C src redis-server USE_UB=yes
  NUM_KEYS=100000 WORKERS='21:21' TS='64' CS='4' TEST_TIME=30 bash hpc_redis_max_tput.sh
```
## Batch + Cache Test Plan
- 目标:
  验证 `payload batch` 是否发生同 key 并发去重，以及 `payload cache` 是否让热点 key 避免重复 UB 访问。
- 同步范围:
  仅同步本次相关代码文件，不同步文档、脚本和其他本地临时文件。
- 构建:
```bash
cd /root/szz/codespace/hpc-redis
make -C src redis-server USE_UB=yes
```
- 基线压测:
```bash
cd /root/szz/codespace/hpc-redis
NUM_KEYS=100000 WORKERS='21:21' TS='64' CS='4' TEST_TIME=30 bash hpc_redis_max_tput.sh
```
- 热点压测:
  目标是提升同 key / 小热点 key 的并发重读概率，让 `payload_batch_follower` 和 `payload_cache_hit` 更明显。
```bash
cd /root/szz/codespace/hpc-redis
NUM_KEYS=1024 WORKERS='21:21' TS='64' CS='4' TEST_TIME=30 bash hpc_redis_max_tput.sh
```
- 极热点压测:
  如果脚本支持更小 keyspace，进一步压缩到几十到几百个 key，观察 batch/cache 放大效果。
```bash
cd /root/szz/codespace/hpc-redis
NUM_KEYS=128 WORKERS='21:21' TS='64' CS='4' TEST_TIME=30 bash hpc_redis_max_tput.sh
```
- 重点观测项:
  - `payload_cache_hit`: 热点场景应明显上升。
  - `payload_cache_miss`: 首轮预热后占比应下降。
  - `payload_cache_fill/update`: 首次加载和写后更新次数。
  - `payload_cache_invalidate`: `VREM` / 删除路径是否正确失效。
  - `payload_batch_leader/follower`: 同 key 并发读是否形成 leader/follower。
  - `payload_batch_wait_hit`: follower 等 leader 发布 cache 后直接命中的次数。
  - `payload_batch_wait_fallback`: follower 等待失败后退化自读的次数，理想情况下应较低。
  - `sample_vector_load_ns`: 热点场景下应较基线下降。
  - `timing_payload_remote_slice_ns`: 若 cache 生效，远端 payload slice 时间应下降。
- 判定标准:
  - 热点压测相对基线，`payload_cache_hit` 明显增加。
  - `payload_batch_follower` 和 `payload_batch_wait_hit` 非 0，说明 batch 去重生效。
  - `payload_batch_wait_fallback` 远低于 `payload_batch_wait_hit`，说明 follower 多数没有退化。
  - 吞吐不低于基线，且 `sample_vector_load_ns` / `timing_payload_remote_slice_ns` 有下降趋势。
## Note:
- 仅同步代码，不同步文件
- 有问题及时反馈不要自己猜测
