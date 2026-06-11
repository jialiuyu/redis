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
| 111 `/dev/obmm_shmdev1`，NUMA Node 0 | 112 `/dev/obmm_shmdev3` |
| 111 `/dev/obmm_shmdev2`，NUMA Node 1 | 112 `/dev/obmm_shmdev4` |
| 112 `/dev/obmm_shmdev1`，NUMA Node 0 | 111 `/dev/obmm_shmdev3` |
| 112 `/dev/obmm_shmdev2`，NUMA Node 1 | 111 `/dev/obmm_shmdev4` |

当前设备配置模式为：

- 本地 Export：CC。
- 远端 Import：NC。
- `USE_CC_MODE=no`。
- 应用不调用 `obmm_set_ownership`，ownership 由 `/dev/obmm_shmdev*` 驱动管理。

设备映射原图见仓库根目录的 `obmm_dev_mapping.jpeg`。

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
    --path /dev/obmm_shmdev3 \
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
    --path /dev/obmm_shmdev3 \
    --offset 7516192768 \
    --seed 0x1122000000000000 \
    --watch-seconds 70
```

另一组 UB region 可以使用相同方式验证：

```text
本地 /dev/obmm_shmdev2 -> 对端 /dev/obmm_shmdev4
```

## 6. 已观察到的现象

111 writer 输出：

```text
WRITER_READY path=/dev/obmm_shmdev1 flags=O_RDWR(CC) offset=7516192768 hold_seconds=60
written: 1111000000000000 1111000000000001 1111000000000002 1111000000000003 1111000000000004 1111000000000005 1111000000000006 1111000000000007
Run the reader while this process is sleeping.
WRITER_UNMAP
```

112 reader 在 writer mmap 存活期间持续读取到旧值：

```text
attempt=0 result=STALE
actual:   0000000000000000 0000000000000000 0000000000000000 0000000000000000 0000000000000000 0000000000000000 0000000000000000 0000000000000000

...

attempt=25 result=STALE
actual:   0000000000000000 0000000000000000 0000000000000000 0000000000000000 0000000000000000 0000000000000000 0000000000000000 0000000000000000
```

writer 执行 `WRITER_UNMAP` 后，reader 开始读取到完整的新值：

```text
attempt=26 result=VISIBLE
actual:   1111000000000000 1111000000000001 1111000000000002 1111000000000003 1111000000000004 1111000000000005 1111000000000006 1111000000000007
```

可见后的 64B 数据完整，没有观察到 partial write 或 torn write。

## 7. 已确认的事实

1. `bench_ub_dim.sh` 已验证四组 Export/Import 映射关系，跨节点读取全部
   `verify: OK`。
2. `bench_ub_dim.sh` 的 writer 使用 `--cacheable false`，写完后立即
   `munmap`，其访问模型与 VSIM 不同。
3. 最小 UT 中，writer 使用本地 CC mapping，reader 使用远端 NC mapping。
4. writer 写入的是一条完整、64B 对齐的 cacheline。
5. writer CC mmap 存活期间，远端 NC reader 持续读取旧值。
6. writer `munmap` 后，远端 NC reader 可以读取到完整的新值。
7. 该现象不依赖 VSIM、remote meta、atomic RMW 或 ownership API。

## 8. 对 VSIM 的影响

VSIM 服务会长期持有本地 CC mapping：

```text
VADD:
    本地 CC mapping 写入 payload 和 remote meta

VSIM:
    对端通过远端 NC mapping 实时读取 remote meta 和 payload
```

根据最小 UT 的现象，本地 CC mapping 写入的新 remote-meta cacheline 在 mapping
存活期间没有及时对远端 NC reader 可见。因此，对端执行 remote-meta lookup
时仍读取旧值，并返回 `NOT_FOUND`。

C11 `memory_order_release`、`memory_order_acquire`、atomic RMW 和
`atomic_thread_fence` 只能约束 CPU 内存访问顺序，不能替代 UB/设备要求的
cache writeback 或 publish 操作。

## 9. 当前结论

当前可以确认 VSIM 失败的直接原因：

> 本地 CC mapping 中的普通 CPU 写入，在 mapping 持续存活期间没有及时对远端
> NC mapping 可见；执行 `munmap` 后，远端才能读取到新数据。

当前尚不能仅根据应用侧测试确定驱动内部的根因，也不能确定 `munmap` 是否是
规范要求的唯一写回触发方式。

## 10. 需要驱动负责人确认的问题

1. 当前 Export CC、Import NC 配置是否支持本地 writer 和远端 reader 的运行期
   cache coherence？
2. 本地 CC mapping 执行普通 CPU store 后，应用应如何保证远端 NC mapping
   实时读取到最新数据？
3. 是否需要调用特定 ioctl、驱动接口或 cache flush/publish 操作？
4. 为什么当前测试中，数据在 writer `munmap` 后才对远端可见？
5. `MAP_SHARED`、64B 对齐并写满完整 cacheline，是否足以触发实时远端可见？
6. 本地 CC 与远端 NC 混合访问时，驱动提供的可见性和一致性语义是什么？
7. CPU atomic RMW 是否支持跨节点原子性？其结果何时对远端 NC reader 可见？
8. Export/Import region 是否需要额外配置 coherence、snoop 或 writeback 属性？
9. 是否存在可查询的接口，用于确认 dirty cacheline 已经发布到 UB home memory？
10. 对长期 mmap 的服务进程，驱动推荐的高性能 publish/writeback 方式是什么？

## 11. 驱动侧问题摘要

可向驱动负责人提供以下最小摘要：

```text
访问模式：
writer: local Export, open(O_RDWR), MAP_SHARED, aligned 64B memcpy,
        mapping remains alive
reader: remote Import, open(O_RDWR|O_SYNC), MAP_SHARED,
        repeated aligned 64B memcpy

未使用：
ownership API、VSIM、remote meta、atomic RMW、msync、显式 cache flush

现象：
writer 写入后，reader 在 writer mmap 存活期间持续读取旧值；
writer munmap 后，reader 立即读取到完整的新 64B 数据。

诉求：
确认本地 CC writer -> 远端 NC reader 的运行期可见性语义，
以及应用需要调用的 publish/cache-writeback 机制。
```
