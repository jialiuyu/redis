/**
 * @file ub_ringbuffer_lockfree.hpp
 * @brief 针对鲲鹏 UB + SVE 场景优化的多生产者多消费者 (MPMC) 无锁环形队列
 * * 设计要点：
 * 1. 缓存行填充 (Padding)：防止伪共享 (False Sharing)，确保 head 和 tail 位于不同 Cache Line。
 * 2. 内存屏障 (Memory Barriers)：使用 C++11 std::atomic 的 Acquire/Release 语义，适配 ARM 弱内存模型。
 * 3. 幂次容量优化：容量必须为 2 的幂，利用 & (size - 1) 替代取模运算。
 */

#include <atomic>
#include <vector>
#include <optional>
#include <cstdint>
#include <stdexcept>

template <typename T>
class UBRingBuffer {
public:
    explicit UBRingBuffer(size_t capacity) : capacity_(next_pow2(capacity)), mask_(capacity_ - 1) {
        // 每个槽位包含数据和当前序列号（用于标记槽位状态）
        nodes_ = new Node[capacity_];
        for (size_t i = 0; i < capacity_; ++i) {
            nodes_[i].sequence.store(i, std::memory_order_relaxed);
        }
        enqueue_pos_.store(0, std::memory_order_relaxed);
        dequeue_pos_.store(0, std::memory_order_relaxed);
    }

    ~UBRingBuffer() {
        delete[] nodes_;
    }

    // 禁用拷贝
    UBRingBuffer(const UBRingBuffer&) = delete;
    UBRingBuffer& operator=(const UBRingBuffer&) = delete;

    /**
     * @brief 入队 (由 Batch Proxy 调用)
     * @param data 待入队的数据（如 Redis 请求指针或 ID）
     * @return bool 是否成功入队
     */
    bool enqueue(T const& data) {
        Node* node;
        size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
        
        while (true) {
            node = &nodes_[pos & mask_];
            size_t seq = node->sequence.load(std::memory_order_acquire);
            intptr_t diff = (intptr_t)seq - (intptr_t)pos;

            if (diff == 0) {
                // 槽位准备就绪，尝试原子占位
                if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                // 缓冲区已满
                return false;
            } else {
                // 当前位置已被抢占，重新加载
                pos = enqueue_pos_.load(std::memory_order_relaxed);
            }
        }

        node->data = data;
        // 更新序列号，通知消费者该槽位可读
        node->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    /**
     * @brief 出队 (由 SVE Worker 调用)
     * @param data 用于存储弹出数据的引用
     * @return bool 是否成功获取数据
     */
    bool dequeue(T& data) {
        Node* node;
        size_t pos = dequeue_pos_.load(std::memory_order_relaxed);

        while (true) {
            node = &nodes_[pos & mask_];
            size_t seq = node->sequence.load(std::memory_order_acquire);
            intptr_t diff = (intptr_t)seq - (intptr_t)(pos + 1);

            if (diff == 0) {
                // 槽位有数据，尝试原子占位
                if (dequeue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                // 缓冲区为空
                return false;
            } else {
                // 重新加载位置
                pos = dequeue_pos_.load(std::memory_order_relaxed);
            }
        }

        data = node->data;
        // 更新序列号，通知生产者该槽位已空（下个周期可用）
        node->sequence.store(pos + mask_ + 1, std::memory_order_release);
        return true;
    }

private:
    struct Node {
        std::atomic<size_t> sequence;
        T data;
    };

    // 针对现代 CPU 缓存行（通常 64 字节）进行对齐，防止伪共享
    static constexpr size_t kCacheLineSize = 64;

    alignas(kCacheLineSize) const size_t capacity_;
    alignas(kCacheLineSize) const size_t mask_;
    alignas(kCacheLineSize) Node* nodes_;
    
    // 写指针 (生产者位置)
    alignas(kCacheLineSize) std::atomic<size_t> enqueue_pos_;
    
    // 读指针 (消费者位置)
    alignas(kCacheLineSize) std::atomic<size_t> dequeue_pos_;

    // 辅助函数：计算最近的 2 幂次方
    size_t next_pow2(size_t n) {
        size_t res = 1;
        while (res < n) res <<= 1;
        return res;
    }
};

/**
 * @brief 在鲲鹏 UB + SVE 环境下的使用建议
 * * 1. 多线程模型：
 * Batch Proxy (多线程/协程) -> Enqueue -> RingBuffer -> Dequeue -> SVE Worker (单线程/单 Core)
 * 2. 批量处理：
 * SVE Worker 可以通过 dequeue 批量获取 16/32 个请求，装载到 SVE 向量寄存器中利用 Gather/Load 
 * 加速处理。
 */