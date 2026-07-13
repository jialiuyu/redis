# VEMB v16 VSIM UB CC/NC 可见性问题最小复现

## 1. 背景

VEMB v16 双节点 VSIM 场景中，VADD 在本地 UB region 写入 vector 和
remote meta，另一个节点通过远端 UB mapping 查询 remote meta 并读取
vector。

实际测试中，VADD 成功，但跨节点 VSIM 的 key2 remote-meta lookup 返回
`NOT_FOUND`。为了排除 VSIM、hash、owner 路由、atomic RMW 和数据结构等因素，
增加了一个只包含 `open`、`mmap`、64B 写入和 64B 读取的最小复现 UT。

## 2. UB 设备映射关系

| 本地 Export 设备 | 对端 Import 设备 |
|---|---|
| 111 `/dev/obmm_shmdev1` | 112 `/dev/obmm_shmdev5` |
| 111 `/dev/obmm_shmdev2` | 112 `/dev/obmm_shmdev6` |
| 111 `/dev/obmm_shmdev3` | 112 `/dev/obmm_shmdev7` |
| 111 `/dev/obmm_shmdev4` | 112 `/dev/obmm_shmdev8` |
| 112 `/dev/obmm_shmdev1` | 111 `/dev/obmm_shmdev5` |
| 112 `/dev/obmm_shmdev2` | 111 `/dev/obmm_shmdev6` |
| 112 `/dev/obmm_shmdev3` | 111 `/dev/obmm_shmdev7` |
| 112 `/dev/obmm_shmdev4` | 111 `/dev/obmm_shmdev8` |

当前设备配置模式为：

- 本地 Export：CC。
- 远端 Import：NC。
- `USE_CC_MODE=no`。
- 应用不调用 `obmm_set_ownership`，ownership 由 `/dev/obmm_shmdev*` 驱动管理。

`obmm_dev_mapping.jpeg` 与当前双机实测不符，不能作为当前调试依据。

## 3. 最小复现 UT

UT 源码：

```text
benchmark/ub_cc_nc_visibility_ut.c
```

构建目标已经加入：

```text
benchmark/Makefile
```

UT 有两个运行模式：

### Writer

- 操作本地 Export 设备。
- 使用 `open(path, O_RDWR)`，即本地 CC mapping。
- 使用 `MAP_SHARED` mmap。
- 向指定的 64B 对齐偏移一次性写入完整 64B cacheline。
- 写入后保持 mmap 存活，等待指定时间后才 `munmap`。

### Reader

- 操作对端 Import 设备。
- 使用 `open(path, O_RDWR | O_SYNC)`，即远端 NC mapping。
- 使用 `MAP_SHARED` mmap。
- 每秒读取并校验一次指定的完整 64B cacheline。

UT 特意不使用以下机制：

- VSIM 或 VEMB 业务代码。
- remote-meta 数据结构。
- ownership API。
- atomic RMW。
- `msync`。
- 显式 cache flush。

因此，该 UT 只验证以下访问模型：

```text
本地 CC mmap 普通 CPU store
    -> UB
    -> 远端 NC mmap 普通 CPU load
```

## 4. 构建方式

在 111 和 112 节点分别执行：

```bash
cd /root/szz/codespace/hpc-redis
make -C benchmark ub_cc_nc_visibility_ut
```

## 5. 复现方式

以下命令使用 8G region 内的 `7516192768` 偏移，只覆盖从该偏移开始的 64B。
执行前需要确认该偏移没有存放有效数据。

### 5.1 验证 111 本地 CC 写入，112 远端 NC 读取

先在 111 启动 writer：

```bash
cd /root/szz/codespace/hpc-redis/benchmark

./ub_cc_nc_visibility_ut writer \
    --path /dev/obmm_shmdev1 \
    --offset 7516192768 \
    --seed 0x1111000000000000 \
    --hold-seconds 60
```

writer 显示 `WRITER_READY` 后，在 112 启动 reader：

```bash
cd /root/szz/codespace/hpc-redis/benchmark

./ub_cc_nc_visibility_ut reader \
    --path /dev/obmm_shmdev5 \
    --offset 7516192768 \
    --seed 0x1111000000000000 \
    --watch-seconds 70
```

### 5.2 验证 112 本地 CC 写入，111 远端 NC 读取

在 112 启动 writer：

```bash
cd /root/szz/codespace/hpc-redis/benchmark

./ub_cc_nc_visibility_ut writer \
    --path /dev/obmm_shmdev1 \
    --offset 7516192768 \
    --seed 0x1122000000000000 \
    --hold-seconds 60
```

在 111 启动 reader：

```bash
cd /root/szz/codespace/hpc-redis/benchmark

./ub_cc_nc_visibility_ut reader \
    --path /dev/obmm_shmdev5 \
    --offset 7516192768 \
    --seed 0x1122000000000000 \
    --watch-seconds 70
```

其余 3 组 UB region 也应按同样方式验证：

```text
本地 /dev/obmm_shmdev2 -> 对端 /dev/obmm_shmdev6
本地 /dev/obmm_shmdev3 -> 对端 /dev/obmm_shmdev7
本地 /dev/obmm_shmdev4 -> 对端 /dev/obmm_shmdev8
```

## 6. 2026-07-09 远端实测结果

在以下真实机器上完成双向验证：

- `node0=192.168.90.111`
- `node1=192.168.90.112`

构建：

```bash
cd /root/szz/codespace/hpc-redis
make -C benchmark ub_cc_nc_visibility_ut
```

共验证 8 组方向：

- `111:1 -> 112:5`
- `111:2 -> 112:6`
- `111:3 -> 112:7`
- `111:4 -> 112:8`
- `112:1 -> 111:5`
- `112:2 -> 111:6`
- `112:3 -> 111:7`
- `112:4 -> 111:8`

结果全部通过，reader 在 `attempt=0` 即读到 `VISIBLE`。

示例输出：

```text
WRITER_READY path=/dev/obmm_shmdev1 flags=O_RDWR(CC) offset=7516192768 hold_seconds=4
written: 1111000000000000 ... 1111000000000007

READER_READY path=/dev/obmm_shmdev5 flags=O_RDWR|O_SYNC(NC) offset=7516192768 watch_seconds=8
expected: 1111000000000000 ... 1111000000000007
attempt=0 result=VISIBLE
```

## 7. 当前结论

当前这批真实双机上的有效映射关系应固定为：

- `1 -> 5`
- `2 -> 6`
- `3 -> 7`
- `4 -> 8`

并且在这次 UT 中，没有复现“必须等 `munmap` 后远端才可见”的旧现象。

## 8. 已确认的事实

1. `bench_ub_dim.sh` 已验证四组 Export/Import 映射关系，跨节点读取全部
   `verify: OK`。
2. `bench_ub_dim.sh` 的 writer 使用 `--cacheable false`，写完后立即
   `munmap`，其访问模型与 VSIM 不同。
3. 最小 UT 中，writer 使用本地 CC mapping，reader 使用远端 NC mapping。
4. writer 写入的是一条完整、64B 对齐的 cacheline。
5. 当前真实双机上，四组映射都能在 writer 持有 mmap 期间直接被远端 reader 读到。
6. 这次结果不支持“当前机器必须等 `munmap` 后才可见”的旧结论。
7. 该 UT 仍然不依赖 VSIM、remote meta、atomic RMW 或 ownership API。

## 9. 对当前扩容调试的影响

这次 UT 的意义主要是两点：

- 可以把当前双机环境的 UB 路径基线明确固定为
  `1/5, 2/6, 3/7, 4/8`
- 当前扩容问题不能再归因于“peer-view path 选错成 3/4 这一组”

## 10. 若后续仍出现可见性异常

若未来再次观测到 VSIM 或 remote-meta 可见性异常，应优先记录：

1. 具体使用的是哪一对 export/import 设备。
2. 是否仍然满足 `1/5, 2/6, 3/7, 4/8`。
3. UT 是否还能复现。
4. 是否只有业务路径异常，而最小 UT 正常。

## 11. 备注

本文已被更新为当前真实双机结果，不再保留旧的 `3/4` 映射结论。
