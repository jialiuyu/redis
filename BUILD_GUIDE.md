# HPC-Redis 编译指南

## 系统环境

- **操作系统**: openEuler 22.03 (LTS-SP3)
- **架构**: ARM aarch64
- **编译器**: GCC 10.3.1
- **Make版本**: GNU Make 4.3

## 编译前置检查

### 1. 检查系统版本
```bash
cat /etc/os-release
```

### 2. 检查编译工具
```bash
gcc --version
make --version
pkg-config --version
```

### 3. 检查依赖项
```bash
# 检查是否已安装必要的开发工具
which gcc make pkg-config
```

## 编译步骤

### 步骤1: 编译依赖项

进入deps目录并编译所有必要的依赖库：

```bash
cd /home/xuwei/code/hpc-redis/deps

# 编译依赖项（跳过xxhash，稍后手动编译）
make hiredis linenoise lua hdr_histogram fpconv fast_float jemalloc
```

**注意**: xxhash库的编译在原始Makefile中存在问题，需要手动编译。

### 步骤2: 手动编译xxhash

```bash
cd /home/xuwei/code/hpc-redis/deps/xxhash

# 手动编译xxhash
gcc -c -fPIC xxhash.c -o xxhash.o
ar rcs libxxhash.a xxhash.o

# 验证编译结果
ls -la libxxhash.a
```

### 步骤3: 清理旧的编译文件

```bash
cd /home/xuwei/code/hpc-redis
make clean
```

### 步骤4: 编译主项目

```bash
# 使用-O2优化级别编译，避免LTO版本不匹配问题
make OPTIMIZATION=-O2
```

## 编译过程中遇到的问题及解决方案

### 问题1: xxhash编译失败

**错误信息**:
```
Makefile:104: build/make/multiconf.make: No such file or directory
make[1]: *** No rule to make target 'build/make/multiconf.make'.  Stop.
```

**原因**: xxhash的Makefile配置不完整，缺少必要的构建文件。

**解决方案**: 手动编译xxhash库
```bash
cd deps/xxhash
gcc -c -fPIC xxhash.c -o xxhash.o
ar rcs libxxhash.a xxhash.o
```

### 问题2: LTO版本不匹配

**错误信息**:
```
lto1: fatal error: bytecode stream in file 'lzf_c.o' generated with LTO version 11.3 instead of expected 9.3
compilation terminated.
```

**原因**: GCC的LTO（Link Time Optimization）版本在不同编译阶段不一致。

**解决方案**: 使用`OPTIMIZATION=-O2`参数禁用LTO优化
```bash
make OPTIMIZATION=-O2
```

### 问题3: 缺少jemalloc头文件

**错误信息**:
```
make[1]: *** No rule to make target '../deps/jemalloc/include/jemalloc/jemalloc.h', needed by 'threads_mngr.o'.  Stop.
```

**原因**: jemalloc依赖库未正确编译。

**解决方案**: 确保先编译所有依赖项
```bash
cd deps
make hiredis linenoise lua hdr_histogram fpconv fast_float jemalloc
```

## 编译结果

### 生成的可执行文件

| 文件名 | 大小 | 说明 |
|--------|------|------|
| redis-server | 26MB | Redis服务器主程序 |
| redis-cli | 11MB | 命令行客户端 |
| redis-benchmark | 9.3MB | 性能测试工具 |
| redis-sentinel | 26MB | 哨兵进程 |
| redis-check-rdb | 26MB | RDB文件检查工具 |
| redis-check-aof | 26MB | AOF文件检查工具 |

### 版本信息

```bash
./src/redis-server --version
```

**输出**:
```
Redis server v=255.255.255 sha=6cb3d724:1 malloc=jemalloc-5.3.0 bits=64 build=9d48fec1dc81b61d
```

### 可执行文件信息

```bash
file ./src/redis-server
```

**输出**:
```
ELF 64-bit LSB executable, ARM aarch64, version 1 (GNU/Linux), 
dynamically linked, interpreter /lib/ld-linux-aarch64.so.1
```

## 使用说明

### 启动Redis服务器

```bash
# 使用默认配置启动
./src/redis-server

# 使用指定配置文件启动
./src/redis-server /path/to/redis.conf

# 后台启动
./src/redis-server --daemonize yes
```

### 使用Redis客户端

```bash
# 连接到本地Redis服务器
./src/redis-cli

# 测试连接
./src/redis-cli ping

# 执行命令
./src/redis-cli SET mykey "Hello World"
./src/redis-cli GET mykey
```

### 运行性能测试

```bash
# 基本性能测试
./src/redis-benchmark

# 自定义测试参数
./src/redis-benchmark -h 127.0.0.1 -p 6379 -c 50 -n 10000
```

### 运行测试套件

```bash
# 运行所有测试
./runtest

# 运行特定测试
./runtest --single unit/type/string
```

## 完整编译脚本

为方便重复编译，可以使用以下脚本：

```bash
#!/bin/bash
# hpc-redis-build.sh

echo "开始编译HPC-Redis..."

# 设置工作目录
REDIS_HOME="/home/xuwei/code/hpc-redis"
cd $REDIS_HOME

# 步骤1: 编译依赖项
echo "步骤1: 编译依赖项..."
cd deps
make hiredis linenoise lua hdr_histogram fpconv fast_float jemalloc

# 步骤2: 手动编译xxhash
echo "步骤2: 编译xxhash..."
cd xxhash
gcc -c -fPIC xxhash.c -o xxhash.o
ar rcs libxxhash.a xxhash.o

# 步骤3: 清理旧文件
echo "步骤3: 清理旧编译文件..."
cd $REDIS_HOME
make clean

# 步骤4: 编译主项目
echo "步骤4: 编译主项目..."
make OPTIMIZATION=-O2

# 步骤5: 显示结果
echo "编译完成！生成的文件："
ls -lh src/redis-*

echo "版本信息："
./src/redis-server --version
```

**使用方法**:
```bash
chmod +x hpc-redis-build.sh
./hpc-redis-build.sh
```

## 注意事项

1. **编译时间**: 完整编译过程可能需要5-10分钟，取决于系统性能
2. **磁盘空间**: 确保至少有2GB的可用磁盘空间
3. **内存**: 建议至少4GB可用内存进行编译
4. **权限**: 确保对源代码目录有写权限
5. **优化级别**: 使用`-O2`而非默认的`-O3`以避免LTO问题

## 故障排除

### 编译失败

如果编译失败，请检查：

1. **GCC版本**: 确保使用GCC 10.3.1或更高版本
2. **磁盘空间**: 检查是否有足够的磁盘空间
3. **依赖项**: 确保所有依赖项都已正确编译
4. **权限**: 检查文件和目录权限

### 运行时错误

如果运行时遇到错误：

1. **库依赖**: 检查是否缺少必要的动态库
2. **配置文件**: 验证redis.conf配置是否正确
3. **端口占用**: 检查6379端口是否被占用
4. **内存**: 确保系统有足够内存

## 技术支持

如遇到编译问题，请检查：

1. 编译日志中的具体错误信息
2. 系统环境是否满足要求
3. 依赖项是否完整安装

---

**编译日期**: 2026年4月10日  
**文档版本**: 1.0  
**适用版本**: HPC-Redis (基于Valkey/Redis 8.8+)