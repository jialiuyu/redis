#ifndef __RING_BUFFER_H
#define __RING_BUFFER_H

#include "server.h"
#include <stdint.h>
#include <stdatomic.h>

#define RING_BUFFER_SIZE (16 * 1024 * 1024)   /* 16MB Ring Buffer */
#define RING_BUFFER_BATCH_SIZE (256 * 1024)   /* 256KB per batch packet */

typedef struct ring_buffer {
    _Alignas(64) atomic_uint_fast64_t head; /* 仅 consumer 更新 */
    char head_pad[64 - sizeof(atomic_uint_fast64_t)];
    _Alignas(64) atomic_uint_fast64_t tail; /* 仅 producer 更新 */
    char tail_pad[64 - sizeof(atomic_uint_fast64_t)];
    atomic_uint_fast32_t refcount;      /* 进程内共享引用计数 */
    uint8_t *buffer;                    /* 缓冲区大小为 size 的共享内存映射 */
    size_t size;                        /* 缓冲区大小 */
    int fd;                             /* 共享内存 fd */
    uint64_t reserved_commit_tail;      /* producer 待发布 tail */
    size_t reserved_payload_len;        /* 本次预留的 payload 长度 */
    int reservation_active;             /* producer 是否持有预留 */
} ring_buffer_t;

ring_buffer_t *ring_buffer_create(size_t size, const char *name);
void ring_buffer_destroy(ring_buffer_t *rb);
void ring_buffer_retain(ring_buffer_t *rb);
int ring_buffer_push(ring_buffer_t *rb, const void *data, size_t len);
int ring_buffer_pop(ring_buffer_t *rb, void *data, size_t max_len, size_t *actual_len);
int ring_buffer_reserve(ring_buffer_t *rb, size_t payload_len, void **payload);
int ring_buffer_cancel_write(ring_buffer_t *rb);
int ring_buffer_commit_write(ring_buffer_t *rb, size_t payload_len);
int ring_buffer_peek(ring_buffer_t *rb, void **payload, size_t *payload_len);
int ring_buffer_commit_read(ring_buffer_t *rb, size_t payload_len);
size_t ring_buffer_available_space(ring_buffer_t *rb);
size_t ring_buffer_available_data(ring_buffer_t *rb);

#endif /* __RING_BUFFER_H */
