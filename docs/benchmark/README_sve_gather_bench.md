# SVE Cross-Embedding Gather Benchmark

## 方案演进

### V1: 跨行逐维度 gather（已废弃）

对每个维度 d（0..dim-1），用一条 `svld1_gather_u32offset_f32` 同时从 VL=8 个不同 embedding 取第 d 维。

```
emb 0:  [d0] [d1] [d2] ... [d299]
emb 1:  [d0] [d1] [d2] ... [d299]
...
emb 7:  [d0] [d1] [d2] ... [d299]

gather 指令顺序：
第1条: 从 8 行各取 d0 → 8 个 float，地址间隔 stride=1200B
第2条: 从 8 行各取 d1 → 8 个 float，地址间隔 stride=1200B
...
第300条: 从 8 行各取 d299
```

问题：300 条 gather 指令，每条从 8 个不同 cache line 取 4 字节，总共 300 次跨行 stride 访问，cache 利用率极差。

NC 模式结果：**0.4–0.6x**（比 serial 慢 2 倍）。

### V2: 批量锁 + 交错连续读（当前方案）

核心思路：把多行的 bitmap 锁批量合并，然后对这批行做**整行连续读**，按 SVE 向量宽度（8 float = 32B）粒度**轮流读每行**。

```
serial（逐行）:
  lock(emb0) → 读 emb0 整行 → unlock(emb0)
  lock(emb1) → 读 emb1 整行 → unlock(emb1)
  ...

cross-gather V2（批量交错）:
  lock(emb0, emb1, ..., emb7)     ← 一次锁 8 行
  
  对每批 VL=8 个 float 轮流读：
    ld1w from emb0[0..7]   ← 连续 32B
    ld1w from emb1[0..7]
    ...
    ld1w from emb7[0..7]
    ld1w from emb0[8..15]
    ld1w from emb1[8..15]
    ...
  
  unlock(emb0, emb1, ..., emb7)   ← 一次解锁 8 行
```

与 V1 的区别：
- **V1**: 逐维度 gather，每条指令从 8 行取 1 个 float（stride=1200B 跨行）
- **V2**: 逐行连续读，每条指令从 1 行取 8 个连续 float（stride=4B 行内连续）

V2 的优势：
1. **连续访问**：每条 ld1w 读 32B 连续数据，整行只触发 ceil(1200/64)=19 次 cache line 填充（vs V1 的 300 次）
2. **交错调度**：轮流读 8 行，CPU 流水线可以 overlap 多行的访存延迟
3. **批量锁**：bitmap acquire/release 合并成块，减少原子操作次数

## 实测数据

### NC 模式（非缓存映射，shmdev3）

| batch | serial ns/emb | cross-gather ns/emb | **speedup** |
|-------|---------------|---------------------|-------------|
| 8     | 11,511        | 7,291               | **1.58x**   |
| 64    | 16,763        | 9,615               | **1.74x**   |
| 256   | 14,935        | 8,597               | **1.74x**   |
| 1024  | 14,071        | 6,230               | **2.26x**   |

NC 模式下 UB 链路延迟占主导（~11,000 ns/emb）。交错读让 CPU 流水线同时等待多行的链路返回，有效隐藏延迟。batch 越大加速越明显。

### CC 模式（缓存映射，shmdev4，无 ownership）

| batch | serial ns/emb | cross-gather ns/emb | speedup |
|-------|---------------|---------------------|---------|
| 8     | 54            | 64                  | 0.84x   |
| 64    | 64            | 80                  | 0.80x   |
| 256   | 83            | 95                  | 0.88x   |
| 1024  | 109           | 164                 | 0.66x   |

CC 模式下数据走 UB cache，serial 单行连续读已经极快（~60 ns/emb）。交错读多行的指针寻址开销反而拖慢，无法获得加速。

### 三种模式对比（NC, batch=1024）

| 模式 | ns/emb | 说明 |
|------|--------|------|
| serial | 14,071 | 逐行 bitmap 锁 + SVE ld1w 连续读 |
| cross-gather | 6,230 | 批量锁 + 8 行交错连续读 |
| memcpy | 9,847 | 逐行 bitmap 锁 + 标量 memcpy |

## 结论

- **NC 模式推荐使用 cross-gather**：1.7–2.3x 加速，在 UB 链路延迟主导的场景下有效
- **CC 模式继续使用 serial**：cache 命中后延迟太低，交错调度开销不划算
- **生产路径建议**：根据映射类型动态选择——NC 用 cross-gather，CC 用 serial

## 编译

```bash
cd benchmark

# NC 模式
make sve_gather_ub_bench USE_SVE=yes

# CC 模式
make sve_gather_ub_bench USE_SVE=yes USE_CC_MODE=yes
```

## 运行

### Mock-Local（本地 malloc，不需要 UB 设备）

```bash
./sve_gather_ub_bench --mock-local --verify --vector-dimension 300
```

### 真实 UB.MEM

写入端先填充数据：
```bash
./ub_client_ut write-fixture --shm-memid 3 --shm-size 2G \
    --vector-dimension 300 --fill-rows 100000
```

NC 模式：
```bash
./sve_gather_ub_bench \
    --shm-memid 3 --shm-size 2G \
    --vector-dimension 300 \
    --table-name ut_vectors \
    --batch-sizes 8,16,32,64,128,256,512,1024 \
    --warmup 50 --iters 200 \
    --verify --csv results/sve_gather_nc.csv
```

CC 模式（写入端也需要 `--cacheable yes`）：
```bash
./sve_gather_ub_bench \
    --shm-memid 4 --shm-size 2G \
    --vector-dimension 300 \
    --table-name ut_vectors \
    --cacheable yes \
    --batch-sizes 8,16,32,64,128,256,512,1024 \
    --warmup 50 --iters 200 \
    --verify --csv results/sve_gather_cc.csv
```

### 完整参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--shm-memid <id>` | - | UB 设备 memid（真实模式必填） |
| `--shm-size <bytes>` | - | 映射大小，如 `2G` |
| `--table-name <name>` | `ut_vectors` | 表名 |
| `--vector-dimension <n>` | 300 | 向量维度 |
| `--vector-stride-bytes <n>` | dim×4 | 行步长 |
| `--batch-sizes <list>` | `8,16,64,256,1024` | 逗号分隔的批量大小 |
| `--warmup <n>` | 50 | 热身轮次 |
| `--iters <n>` | 200 | 计量轮次 |
| `--cacheable yes` | no | 缓存映射 |
| `--mock-local` | off | 用本地 malloc 代替 UB |
| `--verify` | off | 校验 `row*1000+col` 模式 |
| `--csv <file>` | - | 追加 CSV 输出 |

## 三种模式含义

| 模式 | 函数 | 访问方式 |
|------|------|----------|
| `serial` | `sve_serial_contiguous_read` | 逐行：bitmap 锁 → SVE ld1w 连续读整行 → 解锁 |
| `cross-gather` | `sve_cross_emb_gather_read` | 批量：一次锁 VL 行 → 8 行交错连续读整行 → 批量解锁 |
| `memcpy` | `run_memcpy_baseline` | 逐行：bitmap 锁 → memcpy 整行 → 解锁（标量基线） |

## 输出数值含义

| 列 | 含义 |
|----|------|
| `avg_ns` | 每次调用平均耗时（纳秒），iters 轮取均值 |
| `min_ns` / `max_ns` | iters 轮中最快/最慢一次 |
| `ns/emb` | 平均每个 embedding 的耗时 = avg_ns / batch |
| `MB/s` | 吞吐量 = 总数据量(batch × dim × 4) / avg_ns |
| `errors` | verify 模式下与 `row*1000+col` 不匹配的元素数 |

`speedup` = serial avg_ns / cross-gather avg_ns。>1 表示 cross-gather 更快。
