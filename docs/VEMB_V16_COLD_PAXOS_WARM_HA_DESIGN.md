# VEMB V16 COLD/Paxos/WARM 高可用设计

日期：2026-06-02

## 目标

本文档描述 `VADD`、`VEMB`、`VSIM` 三类操作下，COLD 层、Paxos 复制层和 WARM 层如何联动，以提供高可用与故障恢复能力。

核心原则：

```text
COLD + Paxos = 权威持久数据
WARM = 本地高速计算缓存
```

WARM 数据允许丢失。任何已经对外确认成功的写入，都必须能够通过复制后的 Paxos 日志和 RocksDB 状态从 COLD 层恢复。

本设计明确不使用 Raft。所有一致性术语、元数据和实施阶段都基于 Paxos/Multi-Paxos：

```text
使用：Paxos ballot、proposal、accept、chosen/commit index
不使用：Raft term、Raft log、Raft read-index、Raft election protocol
```

## 分层职责

| 层级 | 角色 | 持久性 | 高可用职责 |
|---|---|---|---|
| HOT | 本地 key 到 row 的加速索引 | 无 | 无 |
| WARM | 本地常驻 vector 工作集 | 可选/临时 | 只作为可恢复缓存 |
| COLD RocksDB | 持久化 key 到 vector 的状态机 | 本地持久化 | 保存已提交 vector 数据 |
| Paxos log | 复制写入顺序与提交状态 | 复制持久日志 | 定义已提交写入和 leader failover |

WARM 层绝不能作为 HA 的数据权威。进程重启、节点 failover、共享内存丢失后，WARM 都可以从 COLD 中 lazy rebuild。

## 推荐拓扑

对于两个数据副本，建议加入一个 witness voter：

```text
Data Node A:
  RocksDB COLD
  WARM vector region
  Paxos acceptor/voter

Data Node B:
  RocksDB COLD
  WARM vector region
  Paxos acceptor/voter

Witness Node W:
  Paxos acceptor/voter
  不保存 vector payload
```

投票模型：

```text
voters = A, B, W
quorum = 2
```

这样只保留两份完整数据副本，同时还能具备写入高可用能力。

| 可用投票者 | 是否可写 | 说明 |
|---|---|---|
| A + B + W | 是 | 正常模式 |
| A + B | 是 | witness 不可用 |
| A + W | 是 | B 不可用 |
| B + W | 是 | A 不可用 |
| A only | 否 | 无 quorum |
| B only | 否 | 无 quorum |
| W only | 否 | 无数据节点 |

如果只有两个投票副本且没有 witness：

```text
quorum = 2
```

这种方案是安全的，但不具备写入高可用能力。任意一个节点故障后，强一致写入必须停止。

## 分片模型

每个 COLD shard 对应一个 Paxos group：

```text
shard_id = hash(key) % num_shards
```

示例：

```text
shard 0:
  A = leader
  B = follower
  W = witness

shard 1:
  B = leader
  A = follower
  W = witness
```

不同 shard 的 leader 可以分散到不同数据节点上，用来均衡写入负载。

## 权威写路径：VADD

`VADD(key, vector)` 是 `VADD/VEMB/VSIM` 中唯一会修改权威数据的操作。

强 HA 写路径：

```text
1. 根据 key 路由到 shard。
2. 将请求发送给该 shard 的 leader。
3. leader 创建日志 entry：
     op=PUT
     key
     key_hash
     vector
     version
     log_index
     ballot
     crc
4. leader 将 entry append 到本地 Paxos log。
5. leader 将 entry 复制给 followers/witness。
6. quorum 确认后，entry 被视为 committed/chosen。
7. 数据节点将 committed entry apply 到 RocksDB。
8. 本地 WARM 可以更新或失效。
9. 只有满足提交策略后才返回 OK。
```

推荐确认点：

```text
Paxos quorum commit 后返回 OK。
```

如果 RocksDB apply 是同步的：

```text
quorum commit -> RocksDB Put -> return OK
```

如果 RocksDB apply 是异步的：

```text
quorum commit -> return OK
apply worker later writes RocksDB
```

在异步 apply 模式下，已提交的 Paxos log 必须持久且可 replay。RocksDB 此时是状态机，可以从 committed log 追平。

## VADD 故障矩阵

| 故障点 | 必须保证的结果 |
|---|---|
| quorum commit 之前 | 写入未确认，可以丢弃 |
| quorum commit 之后、RocksDB apply 之前 | 写入可从 Paxos log 恢复 |
| RocksDB apply 之后、WARM 更新之前 | 写入已持久化，WARM 可重建 |
| WARM 更新之后 | 写入已持久化，并且本地已 resident |
| leader 在 quorum commit 后崩溃 | 新 leader 必须保留并 apply 已提交写入 |
| leader 在 quorum commit 前崩溃 | 新 leader 可以丢弃未提交写入 |

重要规则：

```text
WARM 不能让未提交的 VADD 对 VEMB/VSIM 可见。
```

如果为了降低延迟做 speculative WARM update，该 row 必须标记为 speculative，并且在对应日志 entry commit 前对普通读隐藏。

## 读路径：VEMB

`VEMB(key)` 返回已存储的 vector。

普通本地读路径：

```text
1. 查询 HOT/WARM metadata。
2. 如果 WARM hit：
     从 WARM 返回 vector。
3. 如果 WARM miss：
     从本地 COLD RocksDB 执行 Get(key)。
4. 如果 COLD hit：
     分配 WARM row。
     将 vector copy 到 WARM。
     发布 key -> row_id metadata。
     返回 vector。
5. 如果 COLD miss：
     返回 NOT_FOUND。
```

强一致读路径：

```text
1. 向 leader 请求 Paxos read barrier 或当前 committed index。
2. 确认本地数据节点已经 apply 到该 index。
3. 读取 RocksDB 或 version >= required version 的 WARM row。
4. 如果需要，将数据 promote 到 WARM。
5. 返回 vector。
```

默认建议：

```text
VEMB 默认使用本地读。
只有调用方需要线性一致语义时才使用强一致读。
```

## 读路径：VSIM

`VSIM(key, query_vector)` 计算 query vector 与已存储 vector 的相似度。

普通本地读路径：

```text
1. 查询 HOT/WARM metadata。
2. 如果 WARM hit：
     直接基于 WARM row 计算 similarity。
3. 如果 WARM miss：
     从本地 COLD RocksDB 执行 Get(key)。
     将 vector promote 到 WARM。
     基于 promoted WARM row 计算 similarity。
4. 返回 score。
```

强一致读路径：

```text
1. 从 leader 获取 Paxos read barrier。
2. 确认本地 apply_index >= barrier commit index。
3. 确认 WARM row version >= required version。
4. 如果不满足，从 RocksDB reload。
5. 计算 similarity。
```

Paxos 不复制 `VSIM` 或 `VEMB` 请求。Paxos 只复制权威写入。

## WARM 恢复

WARM 是可恢复缓存。如果 WARM 丢失：

```text
1. 打开 Paxos metadata。
2. 打开 RocksDB。
3. replay 尚未 apply 到 RocksDB 的 committed log entries。
4. 初始化空 WARM vector region。
5. 清空 HOT/WARM index。
6. 接受流量。
7. 当 VEMB/VSIM 发生 WARM miss：
     从 RocksDB load。
     promote 到 WARM。
```

这就是 lazy WARM recovery。

影响：

```text
不会丢失已提交数据。
每个 key 的第一次访问会变慢。
随着流量逐步 promote vector 回 WARM，命中率会恢复。
```

可选 eager recovery：

```text
1. 读取 hot-key list 或 access-frequency metadata。
2. 从 RocksDB 预加载 top N vectors。
3. 在服务流量前或后台构建 WARM metadata。
```

推荐默认值：

```text
--warm-recovery lazy
```

## WARM 元数据要求

为了安全地与 Paxos/COLD 联动，每个 WARM row 应携带：

```text
key
key_hash
row_id
state
version
commit_index
dirty/speculative flag
last_access
```

建议状态：

```text
EMPTY
WARM
COLD
LOADING
EVICTING
SPECULATIVE
DELETED
```

可见性规则：

```text
只有 state=WARM 且 commit_index <= applied_index 的 row
可以对普通 VEMB/VSIM 可见。
```

如果某个已经 resident 在 WARM 的 key 收到更新版本的 committed VADD：

```text
覆盖 WARM row
或使旧 row 失效，并将 metadata 标记为 COLD
```

不能让 stale WARM data 满足强一致读。

## COLD Apply 路径

每个数据节点将 committed log entries apply 到 RocksDB：

```text
for entry in committed_log:
  if entry.index > applied_index:
    if entry.op == PUT:
      RocksDB Put(key, vector_with_header)
    if entry.op == DELETE:
      RocksDB Delete(key) or tombstone
    applied_index = entry.index
```

RocksDB value 应包含：

```text
format_version
dim
vector_bytes
logical_version
commit_index
payload_crc
vector payload
```

这样 COLD load 时可以确定性校验数据，识别损坏或格式不匹配。

## Promote 路径

Promote 是本地行为，不参与复制。

```text
WARM miss:
  RocksDB Get(key)
  validate value header
  allocate WARM row
  copy vector
  publish metadata
```

并发 promote 应使用 per-key ownership：

```text
COLD -> LOADING
```

同一个 key 只能有一个 loader 发起 RocksDB Get 并安装 WARM row。其他线程等待、重试，或挂到同一个 completion 上。

## Eviction 路径

Eviction 只改变本地 WARM residency：

```text
1. 选择 victim。
2. CAS state WARM -> EVICTING。
3. 如果 row 是 clean：
     state -> COLD
     free row_id。
4. 如果 row 是 dirty/speculative：
     只有在 committed/durable policy 允许时才能 evict。
```

在推荐 HA 设计中，`VADD` 只有在 Paxos commit 后才成为权威写入。因此 clean WARM row 总是可以 evict，因为 RocksDB/log 可以恢复它。

如果使用 async WARM-first 模式：

```text
dirty WARM rows are not HA-safe
dirty eviction must flush/commit before freeing
```

HA baseline 应避免 WARM-first acknowledgement。

## 防脑裂

在两个数据副本加一个 witness 的拓扑下：

```text
quorum = 2 of 3
```

只有能够联系到 quorum 的节点才能作为 leader 并接受 `VADD`。

规则：

```text
1. leader 必须周期性证明自己仍拥有 quorum。
2. 如果 leader 失去 quorum，必须 step down。
3. follower 拒绝 stale ballot proposals。
4. 需要强一致的读必须经过 Paxos read barrier。
```

这可以防止两个被隔离的数据节点同时接受写入。

## 恢复场景

### 进程重启

```text
1. 重新打开 Paxos log。
2. 重新打开 RocksDB。
3. replay applied_index 之后的 committed entries。
4. 丢弃 WARM。
5. 读路径 lazy promote。
```

### WARM Region 丢失

```text
1. 重建 WARM region。
2. 清空 HOT/WARM metadata。
3. 保留 COLD RocksDB 和 Paxos state。
4. VEMB/VSIM lazy promote。
```

### 数据节点故障

如果 A 故障：

```text
B + W 形成 quorum。
B 可以成为 leader。
B 服务写入和读取。
A 恢复后从 log/snapshot 追平。
```

如果 B 故障：

```text
A + W 形成 quorum。
A 继续服务。
```

### Witness 故障

```text
A + B 仍然形成 quorum。
系统继续服务。
```

### 数据节点永久丢失

```text
1. 添加 replacement node。
2. 从健康节点安装 RocksDB checkpoint/snapshot。
3. 从 snapshot index 之后 replay log。
4. catch up 后加入为 voter。
```

## 一致性模式

| 模式 | VADD 行为 | VEMB/VSIM 行为 | HA 保证 |
|---|---|---|---|
| `strong` | quorum commit 后返回 | 强一致读使用 Paxos read barrier | 按需提供线性一致 |
| `local-read` | quorum commit 后返回 | 读取本地 WARM/COLD | 读快，但 follower 上可能读到旧值 |
| `async-warm` | WARM update 后返回 | 读取 WARM | 对已确认写入不具备 HA 安全性 |

推荐默认值：

```text
VADD: strong quorum commit
VEMB/VSIM: 默认 local read
VEMB/VSIM strong mode: 可选 Paxos read barrier
```

## 必要指标

Paxos：

```text
leader_id
current_ballot
commit_index
applied_index
last_log_index
quorum_healthy
replication_lag
election_count
leader_stepdown_count
```

COLD：

```text
rocksdb_put_ops
rocksdb_get_ops
rocksdb_apply_ns
rocksdb_replay_entries
rocksdb_replay_ns
rocksdb_corrupt_values
```

WARM：

```text
warm_hit
warm_miss
warm_promote_success
warm_promote_not_found
warm_promote_ns
warm_rows_used
warm_rows_free
warm_evictions
warm_invalidations
```

操作拆分：

```text
vadd_commit_ns
vadd_apply_ns
vemb_warm_hit
vemb_cold_promote
vsim_warm_hit
vsim_cold_promote
```

## 实施阶段

### P0：明确权威边界

1. 将 WARM 只作为 cache。
2. 增加 WARM row version 和 commit index。
3. 确保已确认的 `VADD` 一定进入 COLD/Paxos state。
4. 重启时清空 WARM，并从 COLD lazy promote。

### P1：两个数据副本 + Witness

1. 增加 per-shard Paxos group。
2. 增加 leader 和 quorum 检查。
3. 将 `VADD` log entries 复制到 A/B/W。
4. 数据节点将 committed entries apply 到 RocksDB。
5. witness 不保存 payload。

### P2：强一致读选项

1. 增加 Paxos read barrier 支持。
2. 增加本地 apply 等待：`applied_index >= barrier_commit_index`。
3. 确保 WARM row version 满足强一致读要求。

### P3：WARM Promote 和 Eviction

1. 增加 `LOADING` 状态。
2. 增加重复 promote 抑制。
3. 增加 WARM free list。
4. 增加 clean eviction。
5. 增加 dirty/speculative eviction 保护。

### P4：Snapshot 和节点替换

1. 增加 RocksDB checkpoint snapshot。
2. snapshot 后进行 log compaction。
3. 增加 replacement node catch-up。
4. 增加 hot-key preload metadata。

## 推荐基线

第一版 HA baseline 建议：

```text
topology:
  2 data replicas + 1 witness

VADD:
  quorum commit before OK
  apply committed entry to RocksDB
  update WARM only after commit

VEMB/VSIM:
  local WARM hit
  local COLD RocksDB miss promote
  optional strong Paxos read barrier mode

WARM recovery:
  lazy recovery
  WARM can be dropped and rebuilt from COLD
```

该设计提供：

```text
已提交 VADD 不丢失
quorum 规则下防止 split-brain writes
WARM 丢失后可通过 COLD promote 恢复
在 witness 可用时容忍单个数据节点故障
WARM-hit 的 VSIM/VEMB 路径不引入 Paxos 开销
```
