/*
 * io_uring Lite — Raw syscall interface (no liburing dependency)
 *
 * Implements io_uring with SQPOLL mode:
 *   - Kernel polls the submission queue (zero syscalls for submit)
 *   - Application just writes to mmap'd SQ ring
 *   - Completion read from mmap'd CQ ring (no syscall)
 *
 * Combined with UDS: io_uring handles the read/write async,
 * UDS bypasses TCP/IP stack. Together = zero-syscall network I/O.
 */
#ifndef __IOURING_LITE_H
#define __IOURING_LITE_H

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <linux/io_uring.h>
#include <unistd.h>
#include <errno.h>

/* io_uring syscalls */
static inline int io_uring_setup(unsigned entries, struct io_uring_params *p) {
    return syscall(__NR_io_uring_setup, entries, p);
}
static inline int io_uring_enter(int fd, unsigned to_submit, unsigned min_complete,
                                  unsigned flags, void *sig) {
    return syscall(__NR_io_uring_enter, fd, to_submit, min_complete, flags, sig, 0);
}

/* io_uring context */
typedef struct {
    int ring_fd;

    /* Submission queue */
    void *sq_mmap;
    size_t sq_mmap_sz;
    uint32_t *sq_head;
    uint32_t *sq_tail;
    uint32_t *sq_mask;
    uint32_t *sq_array;
    struct io_uring_sqe *sqes;
    size_t sqe_mmap_sz;

    /* Completion queue */
    void *cq_mmap;
    size_t cq_mmap_sz;
    uint32_t *cq_head;
    uint32_t *cq_tail;
    uint32_t *cq_mask;
    struct io_uring_cqe *cqes;

    uint32_t sq_entries;
    uint32_t cq_entries;
    int sqpoll_enabled;
} iouring_ctx_t;

/* Initialize io_uring with optional SQPOLL */
static inline int iouring_init(iouring_ctx_t *ctx, unsigned entries, int sqpoll) {
    memset(ctx, 0, sizeof(*ctx));

    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    if (sqpoll) {
        params.flags = IORING_SETUP_SQPOLL;
        params.sq_thread_idle = 2000; /* 2ms idle before sleeping */
    }

    ctx->ring_fd = io_uring_setup(entries, &params);
    if (ctx->ring_fd < 0) return -1;

    ctx->sq_entries = params.sq_entries;
    ctx->cq_entries = params.cq_entries;
    ctx->sqpoll_enabled = sqpoll;

    /* Map SQ ring */
    ctx->sq_mmap_sz = params.sq_off.array + params.sq_entries * sizeof(uint32_t);
    ctx->sq_mmap = mmap(NULL, ctx->sq_mmap_sz, PROT_READ | PROT_WRITE,
                        MAP_SHARED | MAP_POPULATE, ctx->ring_fd, IORING_OFF_SQ_RING);
    if (ctx->sq_mmap == MAP_FAILED) { close(ctx->ring_fd); return -2; }

    ctx->sq_head = ctx->sq_mmap + params.sq_off.head;
    ctx->sq_tail = ctx->sq_mmap + params.sq_off.tail;
    ctx->sq_mask = ctx->sq_mmap + params.sq_off.ring_mask;
    ctx->sq_array = ctx->sq_mmap + params.sq_off.array;

    /* Map SQEs */
    ctx->sqe_mmap_sz = params.sq_entries * sizeof(struct io_uring_sqe);
    ctx->sqes = mmap(NULL, ctx->sqe_mmap_sz, PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_POPULATE, ctx->ring_fd, IORING_OFF_SQES);
    if (ctx->sqes == MAP_FAILED) { munmap(ctx->sq_mmap, ctx->sq_mmap_sz); close(ctx->ring_fd); return -3; }

    /* Map CQ ring */
    ctx->cq_mmap_sz = params.cq_off.cqes + params.cq_entries * sizeof(struct io_uring_cqe);
    ctx->cq_mmap = mmap(NULL, ctx->cq_mmap_sz, PROT_READ | PROT_WRITE,
                        MAP_SHARED | MAP_POPULATE, ctx->ring_fd, IORING_OFF_CQ_RING);
    if (ctx->cq_mmap == MAP_FAILED) {
        munmap(ctx->sqes, ctx->sqe_mmap_sz);
        munmap(ctx->sq_mmap, ctx->sq_mmap_sz);
        close(ctx->ring_fd);
        return -4;
    }

    ctx->cq_head = ctx->cq_mmap + params.cq_off.head;
    ctx->cq_tail = ctx->cq_mmap + params.cq_off.tail;
    ctx->cq_mask = ctx->cq_mmap + params.cq_off.ring_mask;
    ctx->cqes = ctx->cq_mmap + params.cq_off.cqes;

    return 0;
}

/* Get next SQE (zero-syscall with SQPOLL) */
static inline struct io_uring_sqe *iouring_get_sqe(iouring_ctx_t *ctx) {
    uint32_t tail = *ctx->sq_tail;
    uint32_t head = __atomic_load_n(ctx->sq_head, __ATOMIC_ACQUIRE);
    if (tail - head >= ctx->sq_entries) return NULL; /* Full */

    struct io_uring_sqe *sqe = &ctx->sqes[tail & *ctx->sq_mask];
    ctx->sq_array[tail & *ctx->sq_mask] = tail & *ctx->sq_mask;
    return sqe;
}

/* Submit SQE (zero-syscall with SQPOLL — just advance tail) */
static inline void iouring_submit(iouring_ctx_t *ctx) {
    __atomic_store_n(ctx->sq_tail, *ctx->sq_tail + 1, __ATOMIC_RELEASE);
    /* With SQPOLL, kernel thread picks it up automatically.
     * Without SQPOLL, need io_uring_enter(). */
    if (!ctx->sqpoll_enabled) {
        io_uring_enter(ctx->ring_fd, 1, 0, 0, NULL);
    }
}

/* Poll CQE (zero-syscall) */
static inline struct io_uring_cqe *iouring_peek_cqe(iouring_ctx_t *ctx) {
    uint32_t head = *ctx->cq_head;
    uint32_t tail = __atomic_load_n(ctx->cq_tail, __ATOMIC_ACQUIRE);
    if (head >= tail) return NULL;
    return &ctx->cqes[head & *ctx->cq_mask];
}

/* Consume CQE */
static inline void iouring_cqe_seen(iouring_ctx_t *ctx) {
    __atomic_store_n(ctx->cq_head, *ctx->cq_head + 1, __ATOMIC_RELEASE);
}

/* Cleanup */
static inline void iouring_destroy(iouring_ctx_t *ctx) {
    if (ctx->cq_mmap) munmap(ctx->cq_mmap, ctx->cq_mmap_sz);
    if (ctx->sqes) munmap(ctx->sqes, ctx->sqe_mmap_sz);
    if (ctx->sq_mmap) munmap(ctx->sq_mmap, ctx->sq_mmap_sz);
    if (ctx->ring_fd >= 0) close(ctx->ring_fd);
}

#endif /* __IOURING_LITE_H */
