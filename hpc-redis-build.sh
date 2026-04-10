#!/bin/bash
# HPC-Redis 编译脚本
# 基于openEuler 22.03 (LTS-SP3) ARM aarch64平台

set -e  # 遇到错误立即退出

echo "=========================================="
echo "  HPC-Redis 编译脚本"
echo "=========================================="
echo ""

# 设置工作目录
REDIS_HOME="/home/xuwei/code/hpc-redis"
cd $REDIS_HOME

# 检查环境
echo "步骤0: 检查编译环境..."
echo "当前目录: $(pwd)"
echo "系统信息: $(uname -a)"
echo "GCC版本: $(gcc --version | head -1)"
echo "Make版本: $(make --version | head -1)"
echo ""

# 步骤1: 编译依赖项
echo "步骤1: 编译依赖项..."
cd deps
echo "  编译 hiredis..."
make hiredis
echo "  编译 linenoise..."
make linenoise
echo "  编译 lua..."
make lua
echo "  编译 hdr_histogram..."
make hdr_histogram
echo "  编译 fpconv..."
make fpconv
echo "  编译 fast_float..."
make fast_float
echo "  编译 jemalloc..."
make jemalloc
echo "  依赖项编译完成！"
echo ""

# 步骤2: 手动编译xxhash
echo "步骤2: 手动编译xxhash..."
cd xxhash
echo "  编译xxhash.o..."
gcc -c -fPIC xxhash.c -o xxhash.o
echo "  创建libxxhash.a..."
ar rcs libxxhash.a xxhash.o
echo "  xxhash编译完成！"
echo ""

# 步骤3: 清理旧文件
echo "步骤3: 清理旧编译文件..."
cd $REDIS_HOME
make clean
echo "  清理完成！"
echo ""

# 步骤4: 编译主项目
echo "步骤4: 编译主项目（使用-O2优化）..."
make OPTIMIZATION=-O2
echo "  主项目编译完成！"
echo ""

# 步骤5: 显示结果
echo "=========================================="
echo "  编译完成！"
echo "=========================================="
echo ""
echo "生成的可执行文件："
ls -lh src/redis-* | grep -E "redis-server|redis-cli|redis-benchmark|redis-sentinel|redis-check"
echo ""
echo "版本信息："
./src/redis-server --version
echo ""
echo "可执行文件类型："
file ./src/redis-server
echo ""
echo "=========================================="
echo "  编译成功！"
echo "=========================================="
echo ""
echo "使用方法："
echo "  启动服务器: ./src/redis-server"
echo "  客户端连接: ./src/redis-cli"
echo "  性能测试:   ./src/redis-benchmark"
echo "  运行测试:   ./runtest"
echo ""