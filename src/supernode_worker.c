/*
 * SuperNode Worker Implementation
 * 超节点 SVE2 计算引擎 - 核心实现
 */

#include "supernode_worker.h"
#include "server.h"
#include <sys/mman.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <sched.h>

/* 全局超节点实例 */
supernode_t *global_supernode = NULL;

/* ========== Bitmap 实现 - 高性能无锁版本 ========== */

/* 创建状态 Bitmap
 * 使用对齐的原子字数组，防止伪共享
 */
state_bitmap_t *bitmap_create(size_t num_bits) {
    state_bitmap_t *bitmap = zmalloc(sizeof(state_bitmap_t));
    if (!bitmap) return NULL;
    
    bitmap->num_words = (num_bits + BITMAP_BITS_PER_WORD - 1) / BITMAP_BITS_PER_WORD;
    bitmap->shm_size = bitmap->num_words * sizeof(aligned_atomic_word_t);
    
    /* 使用共享内存（跨 Worker 共享）*/
    char shm_name[256];
    snprintf(shm_name, sizeof(shm_name), "/redis_ub_bitmap_%d", getpid());
    
    int fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
    if (fd < 0) {
        serverLog(LL_WARNING, "Failed to create bitmap shared memory: %s", strerror(errno));
        zfree(bitmap);
        return NULL;
    }
    
    if (ftruncate(fd, bitmap->shm_size) < 0) {
        close(fd);
        shm_unlink(shm_name);
        zfree(bitmap);
        return NULL;
    }
    
    bitmap->shm_addr = mmap(NULL, bitmap->shm_size, PROT_READ | PROT_WRITE,
                           MAP_SHARED, fd, 0);
    close(fd);
    
    if (bitmap->shm_addr == MAP_FAILED) {
        shm_unlink(shm_name);
        zfree(bitmap);
        return NULL;
    }
    
    bitmap->bits = (aligned_atomic_word_t *)bitmap->shm_addr;
    
    /* 初始化所有原子字为 0（使用 relaxed 内存序，因为是单线程初始化）*/
    for (size_t i = 0; i < bitmap->num_words; i++) {
        atomic_init(&bitmap->bits[i].word, 0);
    }
    
    serverLog(LL_NOTICE, "Bitmap created: %zu bits, %zu words (aligned to 64 bytes)", 
              num_bits, bitmap->num_words);
    return bitmap;
}

/* 销毁 Bitmap */
void bitmap_destroy(state_bitmap_t *bitmap) {
    if (!bitmap) return;
    
    if (bitmap->shm_addr && bitmap->shm_addr != MAP_FAILED) {
        munmap(bitmap->shm_addr, bitmap->shm_size);
    }
    
    zfree(bitmap);
}

/* 测试位状态（非原子快照，仅用于调试或非严格检查）
 * 使用 relaxed 内存序，因为这只是一个快照
 */
int bitmap_test_bit(state_bitmap_t *bitmap, uint64_t bit_index) {
    if (!bitmap) return 0;
    
    uint64_t word_index = bit_index / BITMAP_BITS_PER_WORD;
    uint64_t bit_offset = bit_index % BITMAP_BITS_PER_WORD;
    
    if (word_index >= bitmap->num_words) return 0;
    
    uint64_t word = atomic_load_explicit(&bitmap->bits[word_index].word, 
                                        memory_order_relaxed);
    return (word & (1ULL << bit_offset)) != 0;
}

/* 尝试获取（占用）资源位：0 -> 1
 * 
 * 核心设计：
 * 1. 使用 CAS 循环，对整个 64-bit Word 进行原子操作
 * 2. 如果目标位已经是 1，立即返回失败
 * 3. 使用 compare_exchange_weak 在循环中性能更好
 * 4. 成功时使用 memory_order_acquire，确保后续操作不会被重排到获取之前
 * 5. 失败时使用 memory_order_relaxed，因为只是重试，不需要同步
 * 
 * @return C_OK 成功获取；C_ERR 已被占用
 */
int bitmap_try_acquire(state_bitmap_t *bitmap, uint64_t bit_index) {
    if (!bitmap) return C_ERR;
    
    uint64_t word_index = bit_index / BITMAP_BITS_PER_WORD;
    uint64_t bit_offset = bit_index % BITMAP_BITS_PER_WORD;
    
    if (word_index >= bitmap->num_words) return C_ERR;
    
    const uint64_t mask = 1ULL << bit_offset;
    atomic_uint_fast64_t *target_word = &bitmap->bits[word_index].word;
    
    /* 步骤 1: 读取当前值（使用 relaxed，因为后续 CAS 会负责同步）*/
    uint64_t old_val = atomic_load_explicit(target_word, memory_order_relaxed);
    
    /* CAS 循环 */
    do {
        /* 步骤 2: 检查目标位是否已被占用 */
        if ((old_val & mask) != 0) {
            /* 已被占用，立即返回失败 */
            return C_ERR;
        }
        
        /* 步骤 3: 计算新值（将目标位设置为 1）*/
        uint64_t new_val = old_val | mask;
        
        /* 步骤 4: CAS 操作
         * 
         * compare_exchange_weak 尝试原子地将 target_word 从 old_val 更新为 new_val
         * 
         * 成功时：
         *   - 返回 true，内存被更新
         *   - 使用 memory_order_acquire，确保后续对资源的操作不会被重排到获取之前
         * 
         * 失败时：
         *   - 返回 false，old_val 被自动更新为 target_word 的最新值
         *   - 使用 memory_order_relaxed，因为失败只需要重试，不需要同步
         * 
         * 注：在循环中使用 weak 版本性能更好，因为它允许虚假失败（spurious failure）
         *     而我们本来就在循环中处理重试
         */
        if (atomic_compare_exchange_weak_explicit(target_word, &old_val, new_val,
                                                  memory_order_acquire,
                                                  memory_order_relaxed)) {
            return C_OK; /* 获取成功 */
        }
        
        /* CAS 失败：说明在读取 old_val 和 CAS 之间，有其他线程修改了这个 64-bit Word
         * 此时 old_val 已被自动更新为最新值，循环继续重试
         * 
         * 优化点：在极度竞争的环境下，可以在这里加入 CPU pause 指令
         * 如 x86 的 _mm_pause()，以减少总线竞争
         */
#if defined(__x86_64__) || defined(__i386__)
        /* x86/x64: 使用 PAUSE 指令减少总线竞争 */
        __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(__arm__)
        /* ARM: 使用 YIELD 指令 */
        __asm__ __volatile__("yield" ::: "memory");
#endif
        
    } while (1);
}

/* 释放资源位：1 -> 0
 * 
 * 核心设计：
 * 1. 使用 fetch_and 原子操作，比 CAS 循环更高效
 * 2. 不需要检查旧值，直接清零目标位
 * 3. 使用 memory_order_release，确保在释放之前的操作对其他线程可见
 * 
 * 注：这个操作不会失败，因为我们只关心把 bit 清零，不关心其他 bit 的状态
 */
void bitmap_release(state_bitmap_t *bitmap, uint64_t bit_index) {
    if (!bitmap) return;
    
    uint64_t word_index = bit_index / BITMAP_BITS_PER_WORD;
    uint64_t bit_offset = bit_index % BITMAP_BITS_PER_WORD;
    
    if (word_index >= bitmap->num_words) return;
    
    /* 创建反掩码：目标位为 0，其他位为 1 */
    const uint64_t mask_complement = ~(1ULL << bit_offset);
    atomic_uint_fast64_t *target_word = &bitmap->bits[word_index].word;
    
    /* 原子地执行按位与操作
     * 使用 memory_order_release，确保在释放之前的对资源的操作：
     * 1. 已经完成
     * 2. 对其他线程可见
     * 然后才标记资源为释放状态
     */
    atomic_fetch_and_explicit(target_word, mask_complement, memory_order_release);
}

/* ========== UB.mem 实现 ========== */

/* 初始化 UB.mem 地址空间 */
ub_memory_space_t *ub_mem_init(uint64_t physical_base, size_t size) {
    ub_memory_space_t *ub_mem = zmalloc(sizeof(ub_memory_space_t));
    if (!ub_mem) return NULL;
    
    ub_mem->physical_base = physical_base;
    ub_mem->size = size;
    ub_mem->token_id = 0x12345678; /* 访问令牌 */
    ub_mem->numa_node = 0;
    
    /* 映射 UB.mem 到本地地址空间 */
    /* 在实际实现中，这里会调用 UB 固件 API */
    /* 现在使用模拟的 mmap */
    
    ub_mem->base_addr = mmap(NULL, size, PROT_READ | PROT_WRITE,
                            MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    
    if (ub_mem->base_addr == MAP_FAILED) {
        serverLog(LL_WARNING, "Failed to mmap UB.mem: %s", strerror(errno));
        
        /* 回退到普通 mmap */
        ub_mem->base_addr = mmap(NULL, size, PROT_READ | PROT_WRITE,
                                MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        
        if (ub_mem->base_addr == MAP_FAILED) {
            zfree(ub_mem);
            return NULL;
        }
    }
    
    serverLog(LL_NOTICE, "UB.mem initialized: base=0x%llx, size=%zu GB",
              (unsigned long long)physical_base, size / (1024 * 1024 * 1024));
    
    return ub_mem;
}

/* 清理 UB.mem */
void ub_mem_cleanup(ub_memory_space_t *ub_mem) {
    if (!ub_mem) return;
    
    if (ub_mem->base_addr && ub_mem->base_addr != MAP_FAILED) {
        munmap(ub_mem->base_addr, ub_mem->size);
    }
    
    zfree(ub_mem);
}

/* 获取 Embedding 地址 */
void *ub_mem_get_embedding_addr(ub_memory_space_t *ub_mem, uint64_t emb_id) {
    if (!ub_mem || !ub_mem->base_addr) return NULL;
    
    /* 计算 Embedding 在 UB.mem 中的偏移 */
    size_t offset = emb_id * sizeof(embedding_entry_t);
    
    if (offset >= ub_mem->size) {
        return NULL; /* 越界 */
    }
    
    return (uint8_t *)ub_mem->base_addr + offset;
}

/* ========== SVE2 批量操作 ========== */

/* SVE2 Gather Load with Bitmap Check
 * 这是核心函数：结合 Bitmap 检查和 SVE2 Gather Load
 * 
 * 核心设计：
 * 1. 先尝试获取锁（bitmap_try_acquire）
 * 2. 如果获取失败（正在写入），跳过该请求，避免流水线停顿
 * 3. 如果获取成功，执行 SVE2 Gather Load
 * 4. 读取完成后立即释放锁（bitmap_release）
 */
int sve2_gather_with_bitmap_check(sve_worker_context_t *ctx,
                                  uint64_t *emb_ids,
                                  size_t num_ids,
                                  float *results,
                                  uint8_t *valid_mask) {
    if (!ctx || !emb_ids || !results || num_ids == 0) return C_ERR;
    
    state_bitmap_t *bitmap = ctx->bitmap;
    ub_memory_space_t *ub_mem = ctx->ub_mem;
    
    /* 处理每个 Embedding ID */
    for (size_t i = 0; i < num_ids; i++) {
        uint64_t emb_id = emb_ids[i];
        
        /* 步骤 1: 尝试获取锁（CAS 0 -> 1）
         * 
         * 关键设计：使用 try_acquire 而不是 test_bit
         * 原因：
         * 1. 避免 TOCTOU（Time-of-Check-Time-of-Use）竞态条件
         * 2. 如果先 test 再 acquire，在两次操作之间可能被其他线程占用
         * 3. try_acquire 是原子的，一次操作完成检查和获取
         */
        int lock_acquired = bitmap_try_acquire(bitmap, emb_id);
        
        if (lock_acquired != C_OK) {
            /* 正在写入，跳过该请求
             * 
             * Lock-Free 策略：遇锁不等待
             * 优点：
             * 1. 避免流水线停顿
             * 2. 保持 SVE 流水线满载
             * 3. 最大化吞吐量
             * 
             * 处理方式：
             * - 返回默认值（全零）
             * - 或者标记为重试（由上层决定）
             */
            valid_mask[i] = 0;
            atomic_fetch_add(&ctx->locked_skips, 1);
            
            /* 填充默认值（全零）*/
            memset(&results[i * SUPERNODE_EMBEDDING_DIM], 0,
                   SUPERNODE_EMBEDDING_DIM * sizeof(float));
            continue;
        }
        
        /* 步骤 2: 获取 Embedding 地址 */
        embedding_entry_t *emb_addr = ub_mem_get_embedding_addr(ub_mem, emb_id);
        
        if (!emb_addr) {
            valid_mask[i] = 0;
            /* 释放锁 */
            bitmap_release(bitmap, emb_id);
            continue;
        }
        
        /* 步骤 3: SVE2 Gather Load（批量并行读取）
         * 
         * 使用非临时加载，避免污染 L3 Cache
         * 
         * 内存序说明：
         * - 我们已经通过 bitmap_try_acquire 获取了锁（memory_order_acquire）
         * - 这保证了在锁获取之后的所有内存操作都不会被重排到锁获取之前
         * - 因此这里的 memcpy/SVE load 是安全的
         */
        
#ifdef USE_ARM_SVE
        /* SVE2 实现 */
        svbool_t pg = svptrue_b32();
        
        /* 分批加载（每次 SVE_ELEMENTS_PER_VECTOR 个 float）*/
        size_t offset = 0;
        while (offset < SUPERNODE_EMBEDDING_DIM) {
            size_t remaining = SUPERNODE_EMBEDDING_DIM - offset;
            size_t count = remaining < SVE_ELEMENTS_PER_VECTOR ? remaining : SVE_ELEMENTS_PER_VECTOR;
            
            /* SVE Gather Load（非临时访问）
             * 
             * 注：ARM SVE 的 svld1_f32 默认就是非临时的（streaming load）
             *     数据不会进入 L3 Cache，只在寄存器中短暂驻留
             */
            svfloat32_t vec = svld1_f32(pg, &emb_addr->data[offset]);
            
            /* 存储到结果 */
            svst1_f32(pg, &results[i * SUPERNODE_EMBEDDING_DIM + offset], vec);
            
            offset += count;
        }
#else
        /* 标量回退实现 */
        memcpy(&results[i * SUPERNODE_EMBEDDING_DIM], emb_addr->data,
               SUPERNODE_EMBEDDING_DIM * sizeof(float));
#endif
        
        /* 步骤 4: 释放锁（CAS 1 -> 0）
         * 
         * 使用 bitmap_release（内部使用 fetch_and + memory_order_release）
         * 这保证了：
         * 1. 在释放锁之前的所有内存操作已经完成
         * 2. 这些操作对其他线程可见
         * 3. 然后才标记资源为释放状态
         */
        bitmap_release(bitmap, emb_id);
        
        valid_mask[i] = 1;
        atomic_fetch_add(&ctx->sve_operations, 1);
    }
    
    return C_OK;
}

/* SVE2 批量 Gather Load（简化版）*/
int sve2_batch_gather_load(sve_worker_context_t *ctx,
                           uint64_t *emb_ids,
                           size_t num_ids,
                           float *results) {
    uint8_t *valid_mask = zmalloc(num_ids);
    int ret = sve2_gather_with_bitmap_check(ctx, emb_ids, num_ids, results, valid_mask);
    zfree(valid_mask);
    return ret;
}

/* 非临时内存访问（Streaming Load）*/
void sve_streaming_load(const void *src, void *dst, size_t size) {
#ifdef USE_ARM_SVE
    /* 使用 SVE 非临时加载指令 */
    const uint8_t *s = (const uint8_t *)src;
    uint8_t *d = (uint8_t *)dst;
    
    svbool_t pg = svptrue_b8();
    size_t offset = 0;
    
    while (offset < size) {
        svuint8_t vec = svld1_u8(pg, &s[offset]);
        svst1_u8(pg, &d[offset], vec);
        offset += svcntb(); /* SVE 向量字节数 */
    }
#else
    memcpy(dst, src, size);
#endif
}

/* 非临时内存访问（Streaming Store）*/
void sve_streaming_store(const void *src, void *dst, size_t size) {
    sve_streaming_load(src, dst, size); /* 实现相同 */
}

/* ========== SVE Worker 线程 ========== */

/* 处理批量请求 */
int sve_worker_process_batch(sve_worker_context_t *ctx, batch_packet_t *packet) {
    if (!ctx || !packet) return C_ERR;
    
    uint64_t start_time = get_time_us();
    
    /* 验证魔数 */
    if (packet->magic != 0xCAC0BEEF) {
        serverLog(LL_WARNING, "Invalid batch packet magic: 0x%x", packet->magic);
        return C_ERR;
    }
    
    serverLog(LL_DEBUG, "Worker %d processing batch: %u requests, batch_id=%llu",
              ctx->worker_id, packet->num_requests, 
              (unsigned long long)packet->batch_id);
    
    /* 提取 Embedding IDs */
    uint64_t *emb_ids = zmalloc(packet->num_requests * sizeof(uint64_t));
    if (!emb_ids) return C_ERR;
    
    for (uint32_t i = 0; i < packet->num_requests; i++) {
        /* 使用 key_hash 作为 embedding ID（简化）*/
        emb_ids[i] = packet->requests[i].key_hash % SUPERNODE_MAX_EMBEDDINGS;
    }
    
    /* 分配结果缓冲区 */
    float *results = zmalloc(packet->num_requests * SUPERNODE_EMBEDDING_DIM * sizeof(float));
    if (!results) {
        zfree(emb_ids);
        return C_ERR;
    }
    
    /* SVE2 批量 Gather Load */
    int ret = sve2_batch_gather_load(ctx, emb_ids, packet->num_requests, results);
    
    /* 清理 */
    zfree(results);
    zfree(emb_ids);
    
    uint64_t end_time = get_time_us();
    uint64_t latency = end_time - start_time;
    
    atomic_fetch_add(&ctx->total_batches, 1);
    atomic_fetch_add(&ctx->total_requests, packet->num_requests);
    atomic_fetch_add(&ctx->total_latency_us, latency);
    
    serverLog(LL_DEBUG, "Worker %d completed batch in %llu μs",
              ctx->worker_id, (unsigned long long)latency);
    
    return ret;
}

/* SVE Worker 线程主函数 */
void *sve_worker_thread(void *arg) {
    sve_worker_context_t *ctx = (sve_worker_context_t *)arg;
    
    serverLog(LL_NOTICE, "SVE Worker %d started", ctx->worker_id);
    
    /* 设置 CPU 亲和性（可选）*/
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(ctx->worker_id, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    
    /* 主循环：轮询 Ring Buffer */
    while (ctx->running) {
        /* 从 Ring Buffer 读取批量 */
        uint8_t buffer[RING_BUFFER_BATCH_SIZE];
        size_t actual_len = 0;
        
        int ret = ring_buffer_pop(ctx->input_rb, buffer, sizeof(buffer), &actual_len);
        
        if (ret == C_OK && actual_len > 0) {
            batch_packet_t *packet = (batch_packet_t *)buffer;
            sve_worker_process_batch(ctx, packet);
        } else {
            /* 没有数据，短暂休眠 */
            usleep(10); /* 10 微秒 */
        }
    }
    
    serverLog(LL_NOTICE, "SVE Worker %d stopped", ctx->worker_id);
    return NULL;
}

/* ========== SuperNode API ========== */

/* 初始化超节点 */
int supernode_init(int node_id, int num_workers) {
    if (global_supernode) return C_OK;
    
    if (num_workers <= 0 || num_workers > SUPERNODE_MAX_WORKERS) {
        serverLog(LL_WARNING, "Invalid number of workers: %d", num_workers);
        return C_ERR;
    }
    
    global_supernode = zcalloc(sizeof(supernode_t));
    if (!global_supernode) return C_ERR;
    
    global_supernode->node_id = node_id;
    global_supernode->num_workers = num_workers;
    
    /* 初始化 UB.mem */
    global_supernode->ub_mem = ub_mem_init(UB_MEM_BASE_ADDR, UB_MEM_SIZE);
    if (!global_supernode->ub_mem) {
        supernode_shutdown();
        return C_ERR;
    }
    
    /* 初始化全局 Bitmap */
    global_supernode->global_bitmap = bitmap_create(SUPERNODE_MAX_EMBEDDINGS);
    if (!global_supernode->global_bitmap) {
        supernode_shutdown();
        return C_ERR;
    }
    
    /* 创建 Ring Buffer */
    char rb_name[64];
    snprintf(rb_name, sizeof(rb_name), "supernode_%d_input", node_id);
    global_supernode->input_rb = ring_buffer_create(RING_BUFFER_SIZE, rb_name);
    
    if (!global_supernode->input_rb) {
        supernode_shutdown();
        return C_ERR;
    }
    
    /* 初始化 Workers */
    global_supernode->workers = zcalloc(sizeof(sve_worker_context_t) * num_workers);
    
    for (int i = 0; i < num_workers; i++) {
        sve_worker_context_t *ctx = &global_supernode->workers[i];
        
        ctx->worker_id = i;
        ctx->running = 1;
        ctx->input_rb = global_supernode->input_rb;
        ctx->ub_mem = global_supernode->ub_mem;
        ctx->bitmap = global_supernode->global_bitmap;
        ctx->sve_vl = SVE_VECTOR_BITS / 8;
        
        atomic_init(&ctx->total_batches, 0);
        atomic_init(&ctx->total_requests, 0);
        atomic_init(&ctx->locked_skips, 0);
        atomic_init(&ctx->sve_operations, 0);
        atomic_init(&ctx->total_latency_us, 0);
        
        /* 启动 Worker 线程 */
        if (pthread_create(&ctx->thread, NULL, sve_worker_thread, ctx) != 0) {
            serverLog(LL_WARNING, "Failed to create SVE worker %d", i);
            supernode_shutdown();
            return C_ERR;
        }
    }
    
    global_supernode->running = 1;
    
    serverLog(LL_NOTICE, "SuperNode %d initialized with %d workers", node_id, num_workers);
    return C_OK;
}

/* 关闭超节点 */
void supernode_shutdown(void) {
    if (!global_supernode) return;
    
    global_supernode->running = 0;
    
    /* 停止所有 Workers */
    if (global_supernode->workers) {
        for (int i = 0; i < global_supernode->num_workers; i++) {
            sve_worker_context_t *ctx = &global_supernode->workers[i];
            
            if (ctx->running) {
                ctx->running = 0;
                pthread_join(ctx->thread, NULL);
            }
        }
        
        zfree(global_supernode->workers);
    }
    
    /* 清理 Ring Buffer */
    if (global_supernode->input_rb) {
        ring_buffer_destroy(global_supernode->input_rb);
    }
    
    /* 清理 Bitmap */
    if (global_supernode->global_bitmap) {
        bitmap_destroy(global_supernode->global_bitmap);
    }
    
    /* 清理 UB.mem */
    if (global_supernode->ub_mem) {
        ub_mem_cleanup(global_supernode->ub_mem);
    }
    
    zfree(global_supernode);
    global_supernode = NULL;
    
    serverLog(LL_NOTICE, "SuperNode shutdown");
}

/* 获取统计信息 */
sds supernode_get_stats(void) {
    sds stats = sdsempty();
    
    if (!global_supernode) {
        stats = sdscat(stats, "SuperNode: Not initialized\n");
        return stats;
    }
    
    stats = sdscatprintf(stats, "SuperNode Stats (Node %d):\n", global_supernode->node_id);
    stats = sdscatprintf(stats, "  Workers: %d\n", global_supernode->num_workers);
    stats = sdscatprintf(stats, "  UB.mem size: %zu GB\n",
                        global_supernode->ub_mem->size / (1024 * 1024 * 1024));
    
    /* 汇总所有 Worker 统计 */
    uint64_t total_batches = 0;
    uint64_t total_requests = 0;
    uint64_t total_locked_skips = 0;
    uint64_t total_sve_ops = 0;
    uint64_t total_latency = 0;
    
    for (int i = 0; i < global_supernode->num_workers; i++) {
        sve_worker_context_t *ctx = &global_supernode->workers[i];
        total_batches += atomic_load(&ctx->total_batches);
        total_requests += atomic_load(&ctx->total_requests);
        total_locked_skips += atomic_load(&ctx->locked_skips);
        total_sve_ops += atomic_load(&ctx->sve_operations);
        total_latency += atomic_load(&ctx->total_latency_us);
    }
    
    stats = sdscatprintf(stats, "  Total batches: %llu\n", (unsigned long long)total_batches);
    stats = sdscatprintf(stats, "  Total requests: %llu\n", (unsigned long long)total_requests);
    stats = sdscatprintf(stats, "  Locked skips: %llu\n", (unsigned long long)total_locked_skips);
    stats = sdscatprintf(stats, "  SVE operations: %llu\n", (unsigned long long)total_sve_ops);
    
    if (total_batches > 0) {
        double avg_latency = (double)total_latency / total_batches;
        double avg_batch_size = (double)total_requests / total_batches;
        stats = sdscatprintf(stats, "  Average batch latency: %.1f μs\n", avg_latency);
        stats = sdscatprintf(stats, "  Average batch size: %.1f\n", avg_batch_size);
    }
    
    return stats;
}

/* 获取单个 Worker 统计 */
sds sve_worker_get_stats(sve_worker_context_t *ctx) {
    sds stats = sdsempty();
    
    if (!ctx) {
        stats = sdscat(stats, "Worker: Invalid\n");
        return stats;
    }
    
    uint64_t batches = atomic_load(&ctx->total_batches);
    uint64_t requests = atomic_load(&ctx->total_requests);
    uint64_t locked = atomic_load(&ctx->locked_skips);
    uint64_t sve_ops = atomic_load(&ctx->sve_operations);
    uint64_t latency = atomic_load(&ctx->total_latency_us);
    
    stats = sdscatprintf(stats, "Worker %d Stats:\n", ctx->worker_id);
    stats = sdscatprintf(stats, "  Batches: %llu\n", (unsigned long long)batches);
    stats = sdscatprintf(stats, "  Requests: %llu\n", (unsigned long long)requests);
    stats = sdscatprintf(stats, "  Locked skips: %llu\n", (unsigned long long)locked);
    stats = sdscatprintf(stats, "  SVE operations: %llu\n", (unsigned long long)sve_ops);
    
    if (batches > 0) {
        double avg_latency = (double)latency / batches;
        stats = sdscatprintf(stats, "  Average latency: %.1f μs\n", avg_latency);
    }
    
    return stats;
}
