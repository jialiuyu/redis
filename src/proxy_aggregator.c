/*
 * Proxy Aggregator Implementation
 * 智能批量聚合器 - 蓄水池策略
 */

#include "proxy_aggregator.h"
#include "server.h"
#include <sys/mman.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>

/* 全局实例 */
proxy_aggregator_t *global_proxy_aggregator = NULL;

typedef struct ring_buffer_registry_entry {
    char *name;
    ring_buffer_t *rb;
    struct ring_buffer_registry_entry *next;
} ring_buffer_registry_entry_t;

/* 原子请求 ID 生成器 */
static atomic_uint_fast64_t next_request_id = 1;
static atomic_uint_fast64_t next_batch_id = 1;
static ring_buffer_registry_entry_t *g_ring_buffer_registry = NULL;
static pthread_mutex_t g_ring_buffer_registry_lock = PTHREAD_MUTEX_INITIALIZER;

static inline size_t worker_queue_index(size_t workers_per_supernode,
                                        int supernode_id,
                                        int worker_id) {
    return (size_t)supernode_id * workers_per_supernode + (size_t)worker_id;
}

/* ========== 工具函数 ========== */

/* 获取当前时间（微秒） */
uint64_t get_time_us(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000 + (uint64_t)tv.tv_usec;
}

/* MurmurHash3 32-bit 实现 */
uint32_t murmur3_hash(const char *key, size_t len) {
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;
    const uint32_t seed = 0x5bd1e995;
    
    uint32_t h = seed;
    const uint8_t *data = (const uint8_t *)key;
    const int nblocks = len / 4;
    
    /* Body */
    const uint32_t *blocks = (const uint32_t *)(data + nblocks * 4);
    for (int i = -nblocks; i; i++) {
        uint32_t k = blocks[i];
        k *= c1;
        k = (k << 15) | (k >> (32 - 15));
        k *= c2;
        
        h ^= k;
        h = (h << 13) | (h >> (32 - 13));
        h = h * 5 + 0xe6546b64;
    }
    
    /* Tail */
    const uint8_t *tail = (const uint8_t *)(data + nblocks * 4);
    uint32_t k = 0;
    switch (len & 3) {
        case 3: k ^= tail[2] << 16; /* fall through */
        case 2: k ^= tail[1] << 8; /* fall through */
        case 1: k ^= tail[0];
                k *= c1;
                k = (k << 15) | (k >> (32 - 15));
                k *= c2;
                h ^= k;
    }
    
    /* Finalization */
    h ^= len;
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    
    return h;
}

/* ========== 一致性哈希实现 ========== */

/* 初始化一致性哈希环 */
int consistent_hash_init(consistent_hash_ring_t **ring, int num_supernodes) {
    *ring = zmalloc(sizeof(consistent_hash_ring_t));
    if (!*ring) return C_ERR;
    
    (*ring)->capacity = PROXY_HASH_RING_SIZE;
    (*ring)->num_nodes = 0;
    (*ring)->nodes = zmalloc(sizeof(hash_node_t) * (*ring)->capacity);
    
    if (!(*ring)->nodes) {
        zfree(*ring);
        return C_ERR;
    }
    
    pthread_rwlock_init(&(*ring)->lock, NULL);
    
    /* 为每个超节点添加虚拟节点（提高均衡性）*/
    for (int i = 0; i < num_supernodes; i++) {
        /* 每个物理节点创建 10 个虚拟节点 */
        for (int v = 0; v < 10; v++) {
            consistent_hash_add_node(*ring, i);
        }
    }
    
    serverLog(LL_NOTICE, "Consistent hash ring initialized with %d supernodes, %zu virtual nodes",
              num_supernodes, (*ring)->num_nodes);
    
    return C_OK;
}

/* 销毁一致性哈希环 */
void consistent_hash_destroy(consistent_hash_ring_t *ring) {
    if (!ring) return;
    
    pthread_rwlock_destroy(&ring->lock);
    zfree(ring->nodes);
    zfree(ring);
}

/* 添加节点到哈希环 */
int consistent_hash_add_node(consistent_hash_ring_t *ring, int supernode_id) {
    if (!ring || ring->num_nodes >= ring->capacity) return C_ERR;
    
    pthread_rwlock_wrlock(&ring->lock);
    
    /* 生成虚拟节点的哈希值 */
    char node_key[64];
    snprintf(node_key, sizeof(node_key), "supernode_%d_vnode_%zu", 
             supernode_id, ring->num_nodes);
    
    uint32_t hash = murmur3_hash(node_key, strlen(node_key));
    
    /* 插入到哈希环（保持有序）*/
    size_t insert_pos = ring->num_nodes;
    for (size_t i = 0; i < ring->num_nodes; i++) {
        if (hash < ring->nodes[i].hash_value) {
            insert_pos = i;
            break;
        }
    }
    
    /* 移动后续节点 */
    if (insert_pos < ring->num_nodes) {
        memmove(&ring->nodes[insert_pos + 1], &ring->nodes[insert_pos],
                (ring->num_nodes - insert_pos) * sizeof(hash_node_t));
    }
    
    ring->nodes[insert_pos].hash_value = hash;
    ring->nodes[insert_pos].supernode_id = supernode_id;
    ring->num_nodes++;
    
    pthread_rwlock_unlock(&ring->lock);
    return C_OK;
}

/* 从哈希环移除节点 */
int consistent_hash_remove_node(consistent_hash_ring_t *ring, int supernode_id) {
    if (!ring) return C_ERR;
    
    pthread_rwlock_wrlock(&ring->lock);
    
    /* 移除所有属于该超节点的虚拟节点 */
    size_t write_pos = 0;
    for (size_t i = 0; i < ring->num_nodes; i++) {
        if (ring->nodes[i].supernode_id != supernode_id) {
            if (write_pos != i) {
                ring->nodes[write_pos] = ring->nodes[i];
            }
            write_pos++;
        }
    }
    
    ring->num_nodes = write_pos;
    
    pthread_rwlock_unlock(&ring->lock);
    return C_OK;
}

/* 获取 key 对应的超节点 */
int consistent_hash_get_node(consistent_hash_ring_t *ring, const char *key) {
    if (!ring || ring->num_nodes == 0) return 0;
    
    pthread_rwlock_rdlock(&ring->lock);
    
    uint32_t hash = murmur3_hash(key, strlen(key));
    
    /* 二分查找第一个大于等于 hash 的节点 */
    size_t left = 0, right = ring->num_nodes;
    while (left < right) {
        size_t mid = left + (right - left) / 2;
        if (ring->nodes[mid].hash_value < hash) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    
    /* 如果超出范围，回到第一个节点（环形）*/
    if (left >= ring->num_nodes) left = 0;
    
    int supernode_id = ring->nodes[left].supernode_id;
    
    pthread_rwlock_unlock(&ring->lock);
    return supernode_id;
}

/* ========== Ring Buffer 实现 ========== */

/* 创建 Ring Buffer */
ring_buffer_t *ring_buffer_create(size_t size, const char *name) {
    ring_buffer_t *rb = zmalloc(sizeof(ring_buffer_t));
    if (!rb) return NULL;
    
    /* 使用共享内存（用于跨进程通信）*/
    char shm_name[256];
    snprintf(shm_name, sizeof(shm_name), "/redis_ub_rb_%s", name);

    pthread_mutex_lock(&g_ring_buffer_registry_lock);
    for (ring_buffer_registry_entry_t *entry = g_ring_buffer_registry; entry; entry = entry->next) {
        if (strcmp(entry->name, shm_name) == 0) {
            ring_buffer_retain(entry->rb);
            pthread_mutex_unlock(&g_ring_buffer_registry_lock);
            zfree(rb);
            return entry->rb;
        }
    }
    pthread_mutex_unlock(&g_ring_buffer_registry_lock);
    
    rb->fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
    if (rb->fd < 0) {
        serverLog(LL_WARNING, "Failed to create shared memory for ring buffer: %s", 
                  strerror(errno));
        zfree(rb);
        return NULL;
    }
    
    /* 设置共享内存大小 */
    if (ftruncate(rb->fd, size) < 0) {
        serverLog(LL_WARNING, "Failed to set shared memory size: %s", strerror(errno));
        close(rb->fd);
        shm_unlink(shm_name);
        zfree(rb);
        return NULL;
    }
    
    /* 映射共享内存 */
    rb->buffer = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, rb->fd, 0);
    if (rb->buffer == MAP_FAILED) {
        serverLog(LL_WARNING, "Failed to mmap ring buffer: %s", strerror(errno));
        close(rb->fd);
        shm_unlink(shm_name);
        zfree(rb);
        return NULL;
    }
    
    rb->size = size;
    atomic_init(&rb->head, 0);
    atomic_init(&rb->tail, 0);
    atomic_init(&rb->refcount, 1);
    rb->reserved_commit_tail = 0;
    rb->reserved_payload_len = 0;
    rb->reservation_active = 0;

    ring_buffer_registry_entry_t *entry = zmalloc(sizeof(*entry));
    if (!entry) {
        munmap(rb->buffer, rb->size);
        close(rb->fd);
        zfree(rb);
        return NULL;
    }
    entry->name = zstrdup(shm_name);
    if (!entry->name) {
        zfree(entry);
        munmap(rb->buffer, rb->size);
        close(rb->fd);
        zfree(rb);
        return NULL;
    }

    pthread_mutex_lock(&g_ring_buffer_registry_lock);
    entry->rb = rb;
    entry->next = g_ring_buffer_registry;
    g_ring_buffer_registry = entry;
    pthread_mutex_unlock(&g_ring_buffer_registry_lock);
    
    serverLog(LL_NOTICE, "Ring buffer created: %s, size: %zu bytes", name, size);
    return rb;
}

/* 销毁 Ring Buffer */
void ring_buffer_destroy(ring_buffer_t *rb) {
    if (!rb) return;

    if (atomic_fetch_sub_explicit(&rb->refcount, 1, memory_order_acq_rel) != 1) {
        return;
    }

    pthread_mutex_lock(&g_ring_buffer_registry_lock);
    ring_buffer_registry_entry_t **prev = &g_ring_buffer_registry;
    ring_buffer_registry_entry_t *entry = g_ring_buffer_registry;
    while (entry) {
        if (entry->rb == rb) {
            *prev = entry->next;
            zfree(entry->name);
            zfree(entry);
            break;
        }
        prev = &entry->next;
        entry = entry->next;
    }
    pthread_mutex_unlock(&g_ring_buffer_registry_lock);
    
    if (rb->buffer && rb->buffer != MAP_FAILED) {
        munmap(rb->buffer, rb->size);
    }
    
    if (rb->fd >= 0) {
        close(rb->fd);
    }
    
    zfree(rb);
}

void ring_buffer_retain(ring_buffer_t *rb) {
    if (!rb) return;
    atomic_fetch_add_explicit(&rb->refcount, 1, memory_order_relaxed);
}

/* 获取可用空间 */
size_t ring_buffer_available_space(ring_buffer_t *rb) {
    uint64_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    uint64_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    
    if (tail >= head) {
        return rb->size - (tail - head) - 1;
    } else {
        return head - tail - 1;
    }
}

/* 获取可用数据 */
size_t ring_buffer_available_data(ring_buffer_t *rb) {
    uint64_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
    
    if (tail >= head) {
        return tail - head;
    } else {
        return rb->size - (head - tail);
    }
}

/* 写入数据到 Ring Buffer（零拷贝）*/
int ring_buffer_push(ring_buffer_t *rb, const void *data, size_t len) {
    if (!rb || !data || len == 0) return C_ERR;
    void *payload = NULL;
    if (ring_buffer_reserve(rb, len, &payload) != C_OK) return C_ERR;
    memcpy(payload, data, len);
    return ring_buffer_commit_write(rb, len);
}

int ring_buffer_reserve(ring_buffer_t *rb, size_t payload_len, void **payload) {
    if (!rb || !payload || payload_len == 0) return C_ERR;
    if (rb->reservation_active) return C_ERR;

    uint64_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    uint64_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    size_t used = (tail >= head) ? (size_t)(tail - head) : (rb->size - (size_t)(head - tail));
    size_t total_len = sizeof(uint32_t) + payload_len;
    if (rb->size - used - 1 < total_len) {
        return C_ERR;
    }

    size_t pos = tail % rb->size;
    size_t contiguous = rb->size - pos;
    if (contiguous < total_len) {
        if (contiguous < sizeof(uint32_t)) {
            return C_ERR; /* 头部都写不下，暂不支持这种跨边界 */
        }

        uint32_t padding_len = 0;
        memcpy(rb->buffer + pos, &padding_len, sizeof(uint32_t));
        tail += contiguous;
        pos = 0;
        contiguous = rb->size;

        head = atomic_load_explicit(&rb->head, memory_order_acquire);
        used = (tail >= head) ? (size_t)(tail - head) : (rb->size - (size_t)(head - tail));
        if (rb->size - used - 1 < total_len || contiguous < total_len) {
            return C_ERR;
        }
    }

    uint32_t packet_len = (uint32_t)payload_len;
    memcpy(rb->buffer + pos, &packet_len, sizeof(uint32_t));
    *payload = rb->buffer + pos + sizeof(uint32_t);
    rb->reserved_commit_tail = tail + total_len;
    rb->reserved_payload_len = payload_len;
    rb->reservation_active = 1;
    return C_OK;
}

int ring_buffer_commit_write(ring_buffer_t *rb, size_t payload_len) {
    if (!rb || payload_len == 0) return C_ERR;
    if (!rb->reservation_active || rb->reserved_payload_len != payload_len) return C_ERR;

    atomic_store_explicit(&rb->tail, rb->reserved_commit_tail, memory_order_release);
    rb->reservation_active = 0;
    rb->reserved_payload_len = 0;
    rb->reserved_commit_tail = 0;
    return C_OK;
}

/* 从 Ring Buffer 读取数据 */
int ring_buffer_pop(ring_buffer_t *rb, void *data, size_t max_len, size_t *actual_len) {
    if (!rb || !data) return C_ERR;
    void *payload = NULL;
    size_t payload_len = 0;
    if (ring_buffer_peek(rb, &payload, &payload_len) != C_OK) {
        return C_ERR;
    }
    if (payload_len > max_len) {
        return C_ERR;
    }
    memcpy(data, payload, payload_len);
    if (ring_buffer_commit_read(rb, payload_len) != C_OK) {
        return C_ERR;
    }
    if (actual_len) *actual_len = payload_len;
    return C_OK;
}

int ring_buffer_peek(ring_buffer_t *rb, void **payload, size_t *payload_len) {
    if (!rb || !payload || !payload_len) return C_ERR;

    while (1) {
        uint64_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
        uint64_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
        size_t available = (tail >= head) ? (size_t)(tail - head) : (rb->size - (size_t)(head - tail));
        if (available < sizeof(uint32_t)) {
            return C_ERR;
        }

        size_t pos = head % rb->size;
        uint32_t packet_len;
        size_t remaining = rb->size - pos;
        if (remaining >= sizeof(uint32_t)) {
            memcpy(&packet_len, rb->buffer + pos, sizeof(uint32_t));
            pos += sizeof(uint32_t);
        } else {
            memcpy(&packet_len, rb->buffer + pos, remaining);
            memcpy(((uint8_t *)&packet_len) + remaining, rb->buffer,
                   sizeof(uint32_t) - remaining);
            pos = sizeof(uint32_t) - remaining;
        }

        if (packet_len == 0) {
            atomic_store_explicit(&rb->head, head + remaining, memory_order_release);
            continue;
        }

        if (available < sizeof(uint32_t) + packet_len) {
            return C_ERR;
        }

        if (rb->size - pos < packet_len) {
            return C_ERR;
        }

        *payload = rb->buffer + pos;
        *payload_len = packet_len;
        return C_OK;
    }
}

int ring_buffer_commit_read(ring_buffer_t *rb, size_t payload_len) {
    if (!rb || payload_len == 0) return C_ERR;

    uint64_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    atomic_store_explicit(&rb->head, head + sizeof(uint32_t) + payload_len, memory_order_release);
    return C_OK;
}

/* ========== 批量处理实现 ========== */

size_t batch_packet_wire_size(batch_bucket_t *bucket) {
    if (!bucket || bucket->count == 0) return 0;
    return sizeof(batch_packet_t) + bucket->count * sizeof(((batch_packet_t *)0)->requests[0]);
}

int fill_batch_packet(batch_bucket_t *bucket, batch_packet_t *packet, size_t packet_size) {
    if (!bucket || !packet || packet_size < batch_packet_wire_size(bucket)) return C_ERR;

    packet->magic = 0xCAC0BEEF;
    packet->packet_size = packet_size;
    packet->num_requests = bucket->count;
    packet->supernode_id = bucket->target_supernode_id;
    packet->worker_id = bucket->target_worker_id;
    packet->timestamp_us = get_time_us();
    packet->batch_id = atomic_fetch_add(&next_batch_id, 1);
    
    /* 填充请求数据 */
    for (size_t i = 0; i < bucket->count; i++) {
        proxy_request_t *req = bucket->requests[i];
        packet->requests[i].request_id = req->request_id;
        packet->requests[i].key_hash = req->key_hash;
    }

    return C_OK;
}

/* 刷新批量到 Ring Buffer */
int flush_batch(batch_bucket_t *bucket, ring_buffer_t *rb) {
    if (!bucket || !rb || bucket->count == 0) return C_ERR;

    size_t packet_size = batch_packet_wire_size(bucket);
    if (packet_size == 0) return C_ERR;

    batch_packet_t *packet = NULL;
    if (ring_buffer_reserve(rb, packet_size, (void **)&packet) != C_OK) {
        return C_ERR;
    }
    if (fill_batch_packet(bucket, packet, packet_size) != C_OK) {
        return C_ERR;
    }
    int ret = ring_buffer_commit_write(rb, packet_size);

    if (ret == C_OK) {
        serverLog(LL_DEBUG, "Flushed batch: %zu requests to supernode %d",
                  bucket->count, bucket->target_supernode_id);
    }
    
    return ret;
}

/* 刷新线程 - 定期检查超时的批量 */
void *flush_thread_func(void *arg) {
    proxy_aggregator_t *agg = (proxy_aggregator_t *)arg;
    
    serverLog(LL_NOTICE, "Proxy aggregator flush thread started");
    
    while (agg->running) {
        uint64_t current_time = get_time_us();
        
        /* 检查每个桶 */
        for (size_t i = 0; i < agg->num_buckets; i++) {
            batch_bucket_t *bucket = &agg->buckets[i];
            
            pthread_mutex_lock(&bucket->mutex);
            
            if (bucket->count > 0) {
                uint64_t age = current_time - bucket->last_flush_time_us;
                
                /* 批量满或超时 */
                if (bucket->count >= PROXY_BATCH_LIMIT || age >= PROXY_TIME_LIMIT_US) {
                    ring_buffer_t *rb = agg->ring_buffers[i];
                    
                    if (flush_batch(bucket, rb) == C_OK) {
                        atomic_fetch_add(&agg->total_flushes, 1);
                        
                        if (bucket->count >= PROXY_BATCH_LIMIT) {
                            atomic_fetch_add(&agg->batch_full_flushes, 1);
                        } else {
                            atomic_fetch_add(&agg->timeout_flushes, 1);
                        }
                        
                        /* 清空桶 */
                        for (size_t j = 0; j < bucket->count; j++) {
                            zfree(bucket->requests[j]);
                        }
                        bucket->count = 0;
                        bucket->last_flush_time_us = current_time;
                    }
                }
            }
            
            pthread_mutex_unlock(&bucket->mutex);
        }
        
        /* 微秒级睡眠 */
        usleep(50); /* 50 微秒 */
    }
    
    serverLog(LL_NOTICE, "Proxy aggregator flush thread stopped");
    return NULL;
}

/* ========== Proxy Aggregator API ========== */

/* 初始化 Proxy 聚合器 */
int proxy_aggregator_init(int num_supernodes) {
    if (global_proxy_aggregator) return C_OK;
    
    if (num_supernodes <= 0 || num_supernodes > PROXY_MAX_SUPERNODES) {
        serverLog(LL_WARNING, "Invalid number of supernodes: %d", num_supernodes);
        return C_ERR;
    }
    
    global_proxy_aggregator = zcalloc(sizeof(proxy_aggregator_t));
    if (!global_proxy_aggregator) return C_ERR;

    global_proxy_aggregator->num_supernodes = (size_t)num_supernodes;
    global_proxy_aggregator->workers_per_supernode =
        server.supernode_workers > 0 ? (size_t)server.supernode_workers :
        (size_t)max((int)sysconf(_SC_NPROCESSORS_ONLN), 1);
    
    /* 初始化批量桶 */
    global_proxy_aggregator->num_buckets =
        (size_t)num_supernodes * global_proxy_aggregator->workers_per_supernode;
    global_proxy_aggregator->buckets =
        zcalloc(sizeof(batch_bucket_t) * global_proxy_aggregator->num_buckets);
    
    for (int sn = 0; sn < num_supernodes; sn++) {
        for (size_t worker = 0; worker < global_proxy_aggregator->workers_per_supernode; worker++) {
            size_t idx = worker_queue_index(global_proxy_aggregator->workers_per_supernode, sn, (int)worker);
            batch_bucket_t *bucket = &global_proxy_aggregator->buckets[idx];
            bucket->capacity = PROXY_BATCH_LIMIT;
            bucket->requests = zcalloc(sizeof(proxy_request_t *) * bucket->capacity);
            bucket->count = 0;
            bucket->target_supernode_id = sn;
            bucket->target_worker_id = (int)worker;
            bucket->last_flush_time_us = get_time_us();
            pthread_mutex_init(&bucket->mutex, NULL);
        }
    }
    
    /* 初始化一致性哈希环 */
    if (consistent_hash_init(&global_proxy_aggregator->hash_ring, num_supernodes) != C_OK) {
        proxy_aggregator_shutdown();
        return C_ERR;
    }
    
    /* 创建 Ring Buffers */
    global_proxy_aggregator->num_ring_buffers = global_proxy_aggregator->num_buckets;
    global_proxy_aggregator->ring_buffers =
        zcalloc(sizeof(ring_buffer_t *) * global_proxy_aggregator->num_ring_buffers);
    
    for (int sn = 0; sn < num_supernodes; sn++) {
        for (size_t worker = 0; worker < global_proxy_aggregator->workers_per_supernode; worker++) {
            size_t idx = worker_queue_index(global_proxy_aggregator->workers_per_supernode, sn, (int)worker);
            char rb_name[64];
            snprintf(rb_name, sizeof(rb_name), "supernode_%d_worker_%zu", sn, worker);
            global_proxy_aggregator->ring_buffers[idx] = ring_buffer_create(RING_BUFFER_SIZE, rb_name);
            
            if (!global_proxy_aggregator->ring_buffers[idx]) {
                serverLog(LL_WARNING,
                          "Failed to create ring buffer for supernode %d worker %zu",
                          sn, worker);
                proxy_aggregator_shutdown();
                return C_ERR;
            }
        }
    }
    
    /* 初始化统计 */
    atomic_init(&global_proxy_aggregator->total_requests, 0);
    atomic_init(&global_proxy_aggregator->total_batches, 0);
    atomic_init(&global_proxy_aggregator->total_flushes, 0);
    atomic_init(&global_proxy_aggregator->batch_full_flushes, 0);
    atomic_init(&global_proxy_aggregator->timeout_flushes, 0);
    
    /* 启动刷新线程 */
    global_proxy_aggregator->running = 1;
    if (pthread_create(&global_proxy_aggregator->flush_thread, NULL,
                       flush_thread_func, global_proxy_aggregator) != 0) {
        serverLog(LL_WARNING, "Failed to create flush thread");
        proxy_aggregator_shutdown();
        return C_ERR;
    }
    
    serverLog(LL_NOTICE, "Proxy aggregator initialized with %d supernodes x %zu workers",
              num_supernodes, global_proxy_aggregator->workers_per_supernode);
    return C_OK;
}

/* 关闭 Proxy 聚合器 */
void proxy_aggregator_shutdown(void) {
    if (!global_proxy_aggregator) return;
    
    /* 停止刷新线程 */
    if (global_proxy_aggregator->running) {
        global_proxy_aggregator->running = 0;
        pthread_join(global_proxy_aggregator->flush_thread, NULL);
    }
    
    /* 清理批量桶 */
    if (global_proxy_aggregator->buckets) {
        for (size_t i = 0; i < global_proxy_aggregator->num_buckets; i++) {
            batch_bucket_t *bucket = &global_proxy_aggregator->buckets[i];
            
            for (size_t j = 0; j < bucket->count; j++) {
                if (bucket->requests[j]) {
                    zfree(bucket->requests[j]);
                }
            }
            
            zfree(bucket->requests);
            pthread_mutex_destroy(&bucket->mutex);
        }
        zfree(global_proxy_aggregator->buckets);
    }
    
    /* 清理一致性哈希环 */
    if (global_proxy_aggregator->hash_ring) {
        consistent_hash_destroy(global_proxy_aggregator->hash_ring);
    }
    
    /* 清理 Ring Buffers */
    if (global_proxy_aggregator->ring_buffers) {
        for (size_t i = 0; i < global_proxy_aggregator->num_ring_buffers; i++) {
            if (global_proxy_aggregator->ring_buffers[i]) {
                ring_buffer_destroy(global_proxy_aggregator->ring_buffers[i]);
            }
        }
        zfree(global_proxy_aggregator->ring_buffers);
    }
    
    zfree(global_proxy_aggregator);
    global_proxy_aggregator = NULL;
    
    serverLog(LL_NOTICE, "Proxy aggregator shutdown");
}

/* 提交请求到聚合器 */
int proxy_enqueue_request(const char *key, void *client_ctx,
                         float *result_buffer, size_t vector_dim) {
    if (!global_proxy_aggregator || !key) return C_ERR;
    
    /* 通过一致性哈希确定目标超节点 */
    int supernode_id = consistent_hash_get_node(global_proxy_aggregator->hash_ring, key);
    uint32_t key_hash = murmur3_hash(key, strlen(key));
    int worker_id = (int)(key_hash % global_proxy_aggregator->workers_per_supernode);
    size_t bucket_index = worker_queue_index(global_proxy_aggregator->workers_per_supernode,
                                             supernode_id, worker_id);
    batch_bucket_t *bucket = &global_proxy_aggregator->buckets[bucket_index];
    
    /* 创建请求 */
    proxy_request_t *req = zmalloc(sizeof(proxy_request_t));
    if (!req) return C_ERR;
    
    req->request_id = atomic_fetch_add(&next_request_id, 1);
    req->key_hash = key_hash;
    req->target_supernode_id = supernode_id;
    req->target_worker_id = worker_id;
    req->submit_time_us = get_time_us();
    req->client_context = client_ctx;
    req->completed = 0;
    req->error_code = 0;
    req->result_vector = result_buffer;
    req->vector_dim = vector_dim;
    
    /* 加入桶 */
    pthread_mutex_lock(&bucket->mutex);
    
    if (bucket->count >= bucket->capacity) {
        pthread_mutex_unlock(&bucket->mutex);
        zfree(req);
        return C_ERR; /* 桶已满 */
    }
    
    bucket->requests[bucket->count++] = req;
    
    if (bucket->count == 1) {
        bucket->last_flush_time_us = req->submit_time_us;
    }
    
    pthread_mutex_unlock(&bucket->mutex);
    
    atomic_fetch_add(&global_proxy_aggregator->total_requests, 1);
    
    return C_OK;
}

/* 获取统计信息 */
sds proxy_aggregator_get_stats(void) {
    sds stats = sdsempty();
    
    if (!global_proxy_aggregator) {
        stats = sdscat(stats, "Proxy Aggregator: Not initialized\n");
        return stats;
    }
    
    uint64_t total_req = atomic_load(&global_proxy_aggregator->total_requests);
    uint64_t total_flush = atomic_load(&global_proxy_aggregator->total_flushes);
    uint64_t batch_full = atomic_load(&global_proxy_aggregator->batch_full_flushes);
    uint64_t timeout = atomic_load(&global_proxy_aggregator->timeout_flushes);
    
    stats = sdscatprintf(stats, "Proxy Aggregator Stats:\n");
    stats = sdscatprintf(stats, "  Supernodes: %zu\n", global_proxy_aggregator->num_supernodes);
    stats = sdscatprintf(stats, "  Workers per supernode: %zu\n",
                         global_proxy_aggregator->workers_per_supernode);
    stats = sdscatprintf(stats, "  Total requests: %llu\n", (unsigned long long)total_req);
    stats = sdscatprintf(stats, "  Total flushes: %llu\n", (unsigned long long)total_flush);
    stats = sdscatprintf(stats, "  Batch full flushes: %llu\n", (unsigned long long)batch_full);
    stats = sdscatprintf(stats, "  Timeout flushes: %llu\n", (unsigned long long)timeout);
    
    if (total_flush > 0) {
        double avg_batch_size = (double)total_req / total_flush;
        stats = sdscatprintf(stats, "  Average batch size: %.1f\n", avg_batch_size);
    }
    
    return stats;
}

ring_buffer_t *proxy_aggregator_get_ring_buffer(int supernode_id, int worker_id) {
    if (!global_proxy_aggregator) return NULL;
    if (supernode_id < 0 || worker_id < 0) return NULL;
    if ((size_t)supernode_id >= global_proxy_aggregator->num_supernodes) return NULL;
    if ((size_t)worker_id >= global_proxy_aggregator->workers_per_supernode) return NULL;

    size_t idx = worker_queue_index(global_proxy_aggregator->workers_per_supernode,
                                    supernode_id, worker_id);
    return global_proxy_aggregator->ring_buffers[idx];
}

int proxy_aggregator_get_workers_per_supernode(void) {
    if (!global_proxy_aggregator) return 0;
    return (int)global_proxy_aggregator->workers_per_supernode;
}
