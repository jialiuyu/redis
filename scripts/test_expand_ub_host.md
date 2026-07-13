# VEMB V16 UB Memory Expansion Test Host

本文记录“只扩 UB 内存、不扩节点”的 2 节点测试路径。

当前推荐配置策略：

- 启动阶段使用 `manifest`
- 扩容 UB 内存阶段使用 `topology_ctl --apply-peer-view-map`
- owner 集合与 topology epoch 保持不变

## 测试机器

- `node0`: `192.168.90.111`
- `node1`: `192.168.90.112`
- `SSH_USER`: `root`
- `REMOTE_DIR=/root/szz/codespace/hpc-redis`

## 设备前提

两台机器都具备：

- `/dev/obmm_shmdev1`
- `/dev/obmm_shmdev2`
- `/dev/obmm_shmdev3`
- `/dev/obmm_shmdev4`
- `/dev/obmm_shmdev5`
- `/dev/obmm_shmdev6`
- `/dev/obmm_shmdev7`
- `/dev/obmm_shmdev8`

若同机还有其他现场在使用 UB RPC 相关 path，也建议先清场，至少确认以下设备未被无关进程占用：

- `/dev/obmm_shmdev2`
- `/dev/obmm_shmdev4`
- `/dev/obmm_shmdev6`
- `/dev/obmm_shmdev8`

检查命令：

```bash
ssh root@192.168.90.111 'lsof /dev/obmm_shmdev2 /dev/obmm_shmdev4 /dev/obmm_shmdev6 /dev/obmm_shmdev8 2>/dev/null || true'
ssh root@192.168.90.112 'lsof /dev/obmm_shmdev2 /dev/obmm_shmdev4 /dev/obmm_shmdev6 /dev/obmm_shmdev8 2>/dev/null || true'
```

本次默认把新增 owner1 region 配成：

- node1 本地视角：`/dev/obmm_shmdev3`
- node0 peer 视角：`/dev/obmm_shmdev7`

## 正式脚本

执行脚本：

```bash
bash ./scripts/vemb_v16_expand_ub_memory_2node.sh
```

验证禁用 LRU eviction 后，通过缩小初始 owner1 region 触发切换到扩容 region：

```bash
bash ./scripts/vemb_v16_expand_ub_no_lru_region_switch_2node.sh
```

## 配置分层

### 启动 manifest

- node0 启动时带：
  - owner0 本地 region100
  - owner1 peer region101
  - owner1 remote meta view
  - owner1 UB RPC peer
- node1 启动时带：
  - owner1 本地 region101
  - owner0 peer region100
  - owner0 remote meta view
  - owner0 UB RPC peer

### 扩容 UB 内存阶段

扩容时不新增 owner，只新增 owner1 的 region102：

- 对 node1 下发本地 map
  - `region_id=102`
  - `path=/dev/obmm_shmdev3`
  - `home_ub_node_id=1`
- 对 node0 下发 peer map
  - `region_id=102`
  - `path=/dev/obmm_shmdev7`
  - `home_ub_node_id=1`

## 预期结果

- `--apply-peer-view-map` 在 node1 与 node0 都返回 `status=0`
- node1 日志出现：
  - `runtime warm region attached: local_owner=1 peer_owner=1 region_id=102`
- node0 日志出现：
  - `runtime warm region attached: local_owner=0 peer_owner=1 region_id=102`
- topology 保持不变：
  - `epoch=1`
  - `active={0,1}`
  - `standby={0,1}`
- post-write / post-read 输出里出现 `warm regions=3`

## 常用排查命令

```bash
ssh root@192.168.90.111 'tail -200 /tmp/v16_node0_expand_ub.log'
ssh root@192.168.90.112 'tail -200 /tmp/v16_node1_expand_ub.log'
ssh root@192.168.90.111 'cat /tmp/v16_expand_ub_post_write.out'
ssh root@192.168.90.111 'cat /tmp/v16_expand_ub_post_read.out'
```
