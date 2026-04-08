#!/usr/bin/env python3
"""
数据生成器 - 生成大规模 Embedding 测试数据

场景：
- 100 亿 UID Embedding
- 1000 亿 Item Embedding
- 每个 Embedding 300 维
"""

import os
import sys
import argparse
import numpy as np
import struct
import time
from multiprocessing import Pool, cpu_count
import hashlib

# 配置
EMBEDDING_DIM = 300
EMBEDDING_SIZE = EMBEDDING_DIM * 4  # 4 bytes per float32
UID_COUNT = 10_000_000_000   # 100 亿
ITEM_COUNT = 100_000_000_000  # 1000 亿

# 数据目录
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
UID_DIR = os.path.join(DATA_DIR, 'uid_embeddings')
ITEM_DIR = os.path.join(DATA_DIR, 'item_embeddings')
QUERY_DIR = os.path.join(DATA_DIR, 'test_queries')


def create_directories():
    """创建数据目录"""
    os.makedirs(UID_DIR, exist_ok=True)
    os.makedirs(ITEM_DIR, exist_ok=True)
    os.makedirs(QUERY_DIR, exist_ok=True)
    print(f"✅ Created directories: {DATA_DIR}")


def generate_embedding(seed):
    """
    生成一个 Embedding 向量
    使用确定性的伪随机生成，保证可重现
    """
    np.random.seed(seed)
    # 生成标准正态分布的向量
    embedding = np.random.randn(EMBEDDING_DIM).astype(np.float32)
    # L2 归一化
    norm = np.linalg.norm(embedding)
    if norm > 0:
        embedding = embedding / norm
    return embedding


def generate_embedding_batch(args):
    """
    批量生成 Embedding（用于多进程）
    
    Args:
        args: (start_id, end_id, output_file, prefix)
    """
    start_id, end_id, output_file, prefix = args
    
    batch_size = 10000
    count = 0
    
    with open(output_file, 'wb') as f:
        for batch_start in range(start_id, end_id, batch_size):
            batch_end = min(batch_start + batch_size, end_id)
            
            for emb_id in range(batch_start, batch_end):
                # 生成 Embedding
                seed = int(hashlib.md5(f"{prefix}:{emb_id}".encode()).hexdigest()[:8], 16)
                embedding = generate_embedding(seed)
                
                # 写入二进制格式：[id (8 bytes)] [embedding (1200 bytes)]
                f.write(struct.pack('Q', emb_id))  # uint64_t
                f.write(embedding.tobytes())
                
                count += 1
                
                if count % 100000 == 0:
                    print(f"  {prefix} Progress: {count:,} / {end_id - start_id:,}")
    
    return count


def generate_sample_data(uid_sample_size=1000000, item_sample_size=10000000):
    """
    生成样本数据（用于快速测试）
    
    Args:
        uid_sample_size: UID 样本数量（默认 100 万）
        item_sample_size: Item 样本数量（默认 1000 万）
    """
    print(f"\n{'='*60}")
    print(f"Generating Sample Data")
    print(f"{'='*60}")
    print(f"UID samples: {uid_sample_size:,}")
    print(f"Item samples: {item_sample_size:,}")
    print(f"Embedding dimension: {EMBEDDING_DIM}")
    print(f"{'='*60}\n")
    
    create_directories()
    
    # 生成 UID Embeddings
    print("Generating UID embeddings...")
    uid_file = os.path.join(UID_DIR, 'uid_sample.bin')
    start_time = time.time()
    
    with open(uid_file, 'wb') as f:
        for uid in range(uid_sample_size):
            seed = int(hashlib.md5(f"uid:{uid}".encode()).hexdigest()[:8], 16)
            embedding = generate_embedding(seed)
            f.write(struct.pack('Q', uid))
            f.write(embedding.tobytes())
            
            if (uid + 1) % 100000 == 0:
                print(f"  Progress: {uid + 1:,} / {uid_sample_size:,}")
    
    uid_time = time.time() - start_time
    print(f"✅ UID embeddings generated: {uid_file}")
    print(f"   Time: {uid_time:.2f}s, Size: {os.path.getsize(uid_file) / 1024 / 1024:.2f} MB\n")
    
    # 生成 Item Embeddings
    print("Generating Item embeddings...")
    item_file = os.path.join(ITEM_DIR, 'item_sample.bin')
    start_time = time.time()
    
    with open(item_file, 'wb') as f:
        for item_id in range(item_sample_size):
            seed = int(hashlib.md5(f"item:{item_id}".encode()).hexdigest()[:8], 16)
            embedding = generate_embedding(seed)
            f.write(struct.pack('Q', item_id))
            f.write(embedding.tobytes())
            
            if (item_id + 1) % 1000000 == 0:
                print(f"  Progress: {item_id + 1:,} / {item_sample_size:,}")
    
    item_time = time.time() - start_time
    print(f"✅ Item embeddings generated: {item_file}")
    print(f"   Time: {item_time:.2f}s, Size: {os.path.getsize(item_file) / 1024 / 1024:.2f} MB\n")
    
    # 生成测试查询
    print("Generating test queries...")
    generate_test_queries(uid_sample_size, item_sample_size)
    
    print(f"\n{'='*60}")
    print(f"✅ Sample data generation completed!")
    print(f"{'='*60}")


def generate_test_queries(uid_count, item_count, num_queries=1000000):
    """
    生成测试查询
    
    Args:
        uid_count: UID 总数
        item_count: Item 总数
        num_queries: 查询数量（默认 100 万）
    """
    query_file = os.path.join(QUERY_DIR, 'test_queries.txt')
    
    with open(query_file, 'w') as f:
        # 80% UID 查询，20% Item 查询（模拟真实场景）
        uid_queries = int(num_queries * 0.8)
        item_queries = num_queries - uid_queries
        
        # UID 查询
        for i in range(uid_queries):
            uid = np.random.randint(0, uid_count)
            f.write(f"uid:{uid}\n")
        
        # Item 查询
        for i in range(item_queries):
            item_id = np.random.randint(0, item_count)
            f.write(f"item:{item_id}\n")
    
    print(f"✅ Test queries generated: {query_file}")
    print(f"   Total queries: {num_queries:,} (80% UID, 20% Item)")


def generate_redis_compatible_data(uid_sample_size=1000000, item_sample_size=10000000):
    """
    生成 Redis 兼容的数据格式（用于传统 Redis 测试）
    
    格式：Redis Protocol (RESP)
    SET uid:12345 <binary_embedding>
    """
    print(f"\n{'='*60}")
    print(f"Generating Redis Compatible Data")
    print(f"{'='*60}\n")
    
    # UID Embeddings
    print("Generating Redis UID commands...")
    redis_uid_file = os.path.join(UID_DIR, 'uid_redis_commands.txt')
    
    with open(redis_uid_file, 'w') as f:
        for uid in range(min(uid_sample_size, 100000)):  # 限制文件大小
            seed = int(hashlib.md5(f"uid:{uid}".encode()).hexdigest()[:8], 16)
            embedding = generate_embedding(seed)
            
            # 转换为十六进制字符串
            hex_embedding = embedding.tobytes().hex()
            f.write(f"SET uid:{uid} {hex_embedding}\n")
            
            if (uid + 1) % 10000 == 0:
                print(f"  Progress: {uid + 1:,} / 100,000")
    
    print(f"✅ Redis UID commands: {redis_uid_file}\n")
    
    # Item Embeddings
    print("Generating Redis Item commands...")
    redis_item_file = os.path.join(ITEM_DIR, 'item_redis_commands.txt')
    
    with open(redis_item_file, 'w') as f:
        for item_id in range(min(item_sample_size, 100000)):  # 限制文件大小
            seed = int(hashlib.md5(f"item:{item_id}".encode()).hexdigest()[:8], 16)
            embedding = generate_embedding(seed)
            
            hex_embedding = embedding.tobytes().hex()
            f.write(f"SET item:{item_id} {hex_embedding}\n")
            
            if (item_id + 1) % 10000 == 0:
                print(f"  Progress: {item_id + 1:,} / 100,000")
    
    print(f"✅ Redis Item commands: {redis_item_file}\n")


def generate_metadata():
    """生成元数据文件"""
    metadata_file = os.path.join(DATA_DIR, 'metadata.txt')
    
    with open(metadata_file, 'w') as f:
        f.write(f"# Embedding Test Data Metadata\n")
        f.write(f"# Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"embedding_dim: {EMBEDDING_DIM}\n")
        f.write(f"embedding_size: {EMBEDDING_SIZE} bytes\n")
        f.write(f"uid_count: {UID_COUNT:,}\n")
        f.write(f"item_count: {ITEM_COUNT:,}\n")
        f.write(f"total_embeddings: {UID_COUNT + ITEM_COUNT:,}\n")
        f.write(f"total_size: {(UID_COUNT + ITEM_COUNT) * EMBEDDING_SIZE / 1024 / 1024 / 1024:.2f} GB\n")
    
    print(f"✅ Metadata: {metadata_file}")


def main():
    parser = argparse.ArgumentParser(description='Generate embedding test data')
    parser.add_argument('--mode', choices=['sample', 'full', 'redis'], default='sample',
                       help='Generation mode: sample (fast), full (slow), redis (Redis compatible)')
    parser.add_argument('--uid-count', type=int, default=1000000,
                       help='Number of UID embeddings (default: 1M)')
    parser.add_argument('--item-count', type=int, default=10000000,
                       help='Number of Item embeddings (default: 10M)')
    parser.add_argument('--num-queries', type=int, default=1000000,
                       help='Number of test queries (default: 1M)')
    
    args = parser.parse_args()
    
    print(f"\n{'='*60}")
    print(f"Embedding Data Generator")
    print(f"{'='*60}")
    print(f"Mode: {args.mode}")
    print(f"Embedding dimension: {EMBEDDING_DIM}")
    print(f"{'='*60}\n")
    
    if args.mode == 'sample':
        # 生成样本数据（快速测试）
        generate_sample_data(args.uid_count, args.item_count)
        generate_metadata()
        
    elif args.mode == 'redis':
        # 生成 Redis 兼容数据
        generate_sample_data(args.uid_count, args.item_count)
        generate_redis_compatible_data(args.uid_count, args.item_count)
        generate_metadata()
        
    elif args.mode == 'full':
        # 生成完整数据（需要大量时间和存储空间）
        print("⚠️  Warning: Full mode will generate ~330 TB of data!")
        print("    This will take a very long time.")
        response = input("Continue? (yes/no): ")
        if response.lower() != 'yes':
            print("Aborted.")
            return
        
        # TODO: 实现完整数据生成（需要分布式生成）
        print("❌ Full mode not implemented yet.")
        print("   Use 'sample' mode for testing.")
    
    print(f"\n{'='*60}")
    print(f"✅ Data generation completed!")
    print(f"{'='*60}")
    print(f"\nNext steps:")
    print(f"  1. Run traditional Redis benchmark:")
    print(f"     ./redis_traditional_benchmark.py")
    print(f"  2. Run SuperNode benchmark:")
    print(f"     ./supernode_benchmark.py")
    print(f"  3. Compare results:")
    print(f"     ./compare_results.py")


if __name__ == '__main__':
    main()
