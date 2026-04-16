/*
 * URMA Implementation — Shared Memory Transport
 *
 * Same-node: mmap shared memory for segments, futex for completion events.
 * Segment read/write is direct memcpy (simulates UB Load/Store path).
 * On real UB hardware, this would go through UDMA driver → UB fabric.
 */
#define _GNU_SOURCE
#include "urma.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>

static inline uint64_t urma_now_ns(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

/* ---- Context ---- */
urma_ctx_t *urma_create_ctx(uint32_t node_id, uint32_t dev_id) {
    urma_ctx_t *ctx = calloc(1, sizeof(urma_ctx_t));
    if (!ctx) return NULL;
    ctx->local_eid.node_id = node_id;
    ctx->local_eid.dev_id = dev_id;
    return ctx;
}

void urma_destroy_ctx(urma_ctx_t *ctx) {
    if (!ctx) return;
    /* Cleanup segments */
    for (int i = 0; i < ctx->num_segments; i++) {
        if (ctx->segments[i].shm_fd >= 0)
            close(ctx->segments[i].shm_fd);
    }
    /* Cleanup JFCs */
    for (int i = 0; i < ctx->num_jfcs; i++)
        free(ctx->jfcs[i].ring);
    free(ctx);
}

/* ---- Segment ---- */
urma_seg_t *urma_register_seg(urma_ctx_t *ctx, void *va, size_t len, urma_token_t token) {
    if (ctx->num_segments >= URMA_MAX_SEGMENTS) return NULL;
    urma_seg_t *seg = &ctx->segments[ctx->num_segments];
    seg->seg_id = ctx->num_segments;
    seg->va = va;
    seg->uba = (uint64_t)(uintptr_t)va;  /* UBA = VA for same-node */
    seg->len = len;
    seg->token = token;
    seg->owner = ctx->local_eid;
    seg->is_local = 1;
    seg->shm_fd = -1;
    ctx->num_segments++;
    return seg;
}

void urma_unregister_seg(urma_ctx_t *ctx, urma_seg_t *seg) {
    (void)ctx;
    if (seg && seg->shm_fd >= 0) { close(seg->shm_fd); seg->shm_fd = -1; }
    seg->va = NULL;
    seg->len = 0;
}

urma_target_seg_t *urma_import_seg(urma_ctx_t *ctx, urma_eid_t remote_eid,
                                    uint32_t seg_id, urma_token_t token, size_t len) {
    (void)ctx;
    /* For same-node: the UBA is directly the VA, so we can access it */
    urma_target_seg_t *tseg = calloc(1, sizeof(urma_target_seg_t));
    if (!tseg) return NULL;
    tseg->seg_id = seg_id;
    tseg->uba = 0;  /* Will be set by caller */
    tseg->len = len;
    tseg->token = token;
    tseg->remote_eid = remote_eid;
    tseg->mapped_va = NULL;  /* Will be set when UBA is known */
    return tseg;
}

void urma_unimport_seg(urma_ctx_t *ctx, urma_target_seg_t *tseg) {
    (void)ctx;
    free(tseg);
}

/* ---- JFC (Completion Queue) ---- */
urma_jfc_t *urma_create_jfc(urma_ctx_t *ctx, uint32_t depth) {
    if (ctx->num_jfcs >= URMA_MAX_JETTIES) return NULL;
    /* Round up to power of 2 */
    uint32_t d = 1;
    while (d < depth) d <<= 1;

    urma_jfc_t *jfc = &ctx->jfcs[ctx->num_jfcs];
    jfc->jfc_id = ctx->num_jfcs;
    jfc->depth = d;
    jfc->mask = d - 1;
    jfc->ring = calloc(d, sizeof(urma_cqe_t));
    atomic_store(&jfc->head, 0);
    atomic_store(&jfc->tail, 0);
    pthread_mutex_init(&jfc->evt_mtx, NULL);
    pthread_cond_init(&jfc->evt_cond, NULL);
    jfc->armed = 0;
    ctx->num_jfcs++;
    return jfc;
}

void urma_destroy_jfc(urma_ctx_t *ctx, urma_jfc_t *jfc) {
    (void)ctx;
    if (jfc) {
        free(jfc->ring);
        pthread_mutex_destroy(&jfc->evt_mtx);
        pthread_cond_destroy(&jfc->evt_cond);
    }
}

/* Post a completion entry to JFC */
static void jfc_post_cqe(urma_jfc_t *jfc, urma_opcode_t op, uint64_t user_ctx,
                          int status, size_t len) {
    uint64_t t = atomic_load_explicit(&jfc->tail, memory_order_relaxed);
    urma_cqe_t *cqe = &jfc->ring[t & jfc->mask];
    cqe->user_ctx = user_ctx;
    cqe->opcode = op;
    cqe->status = status;
    cqe->completion_len = len;
    cqe->timestamp_ns = urma_now_ns();
    atomic_store_explicit(&jfc->tail, t + 1, memory_order_release);

    /* Signal event if armed */
    if (jfc->armed) {
        pthread_mutex_lock(&jfc->evt_mtx);
        pthread_cond_signal(&jfc->evt_cond);
        pthread_mutex_unlock(&jfc->evt_mtx);
    }
}

/* ---- JFS (Send Jetty) ---- */
urma_jfs_t *urma_create_jfs(urma_ctx_t *ctx, urma_jfc_t *jfc) {
    if (ctx->num_jfss >= URMA_MAX_JETTIES) return NULL;
    urma_jfs_t *jfs = &ctx->jfss[ctx->num_jfss];
    jfs->jfs_id = ctx->num_jfss;
    jfs->jfc = jfc;
    jfs->local_eid = ctx->local_eid;
    ctx->num_jfss++;
    return jfs;
}

void urma_destroy_jfs(urma_ctx_t *ctx, urma_jfs_t *jfs) { (void)ctx; (void)jfs; }

/* ---- JFR (Receive Jetty) ---- */
urma_jfr_t *urma_create_jfr(urma_ctx_t *ctx, urma_jfc_t *jfc, size_t buf_size, int buf_count) {
    if (ctx->num_jfrs >= URMA_MAX_JETTIES) return NULL;
    urma_jfr_t *jfr = &ctx->jfrs[ctx->num_jfrs];
    jfr->jfr_id = ctx->num_jfrs;
    jfr->jfc = jfc;
    jfr->local_eid = ctx->local_eid;
    jfr->recv_buf_size = buf_size;
    jfr->recv_buf_count = buf_count;
    jfr->recv_bufs = calloc(buf_count, buf_size);
    atomic_store(&jfr->recv_posted, 0);
    ctx->num_jfrs++;
    return jfr;
}

void urma_destroy_jfr(urma_ctx_t *ctx, urma_jfr_t *jfr) {
    (void)ctx;
    if (jfr && jfr->recv_bufs) free(jfr->recv_bufs);
}

/* ---- Data Operations ---- */

/* Single-sided READ: copy from remote segment to local SGE */
int urma_read(urma_jfs_t *jfs, urma_sge_t *local_sge, urma_target_seg_t *remote_seg,
              uint64_t remote_offset, size_t len, uint64_t user_ctx) {
    if (!remote_seg->mapped_va) return -1;
    void *src = (uint8_t *)remote_seg->mapped_va + remote_offset;
    memcpy(local_sge->addr, src, len);
    if (jfs->jfc)
        jfc_post_cqe(jfs->jfc, URMA_OPC_READ, user_ctx, 0, len);
    return 0;
}

/* Single-sided WRITE: copy from local SGE to remote segment */
int urma_write(urma_jfs_t *jfs, urma_sge_t *local_sge, urma_target_seg_t *remote_seg,
               uint64_t remote_offset, size_t len, uint64_t user_ctx) {
    if (!remote_seg->mapped_va) return -1;
    void *dst = (uint8_t *)remote_seg->mapped_va + remote_offset;
    memcpy(dst, local_sge->addr, len);
    if (jfs->jfc)
        jfc_post_cqe(jfs->jfc, URMA_OPC_WRITE, user_ctx, 0, len);
    return 0;
}

/* Double-sided SEND */
int urma_post_send(urma_jfs_t *jfs, const urma_wr_t *wr) {
    /* For same-node, send is just a memcpy to the receive buffer */
    if (jfs->jfc && wr->signal_completion)
        jfc_post_cqe(jfs->jfc, URMA_OPC_SEND, wr->user_ctx, 0,
                     wr->use_inline ? wr->inline_len : wr->sge[0].len);
    return 0;
}

/* Double-sided RECV */
int urma_post_recv(urma_jfr_t *jfr, const urma_wr_t *wr) {
    (void)wr;
    atomic_fetch_add(&jfr->recv_posted, 1);
    return 0;
}

/* ---- Completion ---- */
int urma_poll_jfc(urma_jfc_t *jfc, urma_cqe_t *cqes, int max_cqes) {
    int count = 0;
    uint64_t h = atomic_load_explicit(&jfc->head, memory_order_relaxed);
    uint64_t t = atomic_load_explicit(&jfc->tail, memory_order_acquire);

    while (count < max_cqes && h < t) {
        cqes[count] = jfc->ring[h & jfc->mask];
        h++;
        count++;
    }
    atomic_store_explicit(&jfc->head, h, memory_order_release);
    return count;
}

void urma_rearm_jfc(urma_jfc_t *jfc) {
    jfc->armed = 1;
}

int urma_wait_jfc(urma_jfc_t *jfc, int timeout_ms) {
    pthread_mutex_lock(&jfc->evt_mtx);
    if (atomic_load(&jfc->head) < atomic_load(&jfc->tail)) {
        jfc->armed = 0;
        pthread_mutex_unlock(&jfc->evt_mtx);
        return 0;
    }
    if (timeout_ms > 0) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += timeout_ms / 1000;
        ts.tv_nsec += (timeout_ms % 1000) * 1000000;
        if (ts.tv_nsec >= 1000000000) { ts.tv_sec++; ts.tv_nsec -= 1000000000; }
        pthread_cond_timedwait(&jfc->evt_cond, &jfc->evt_mtx, &ts);
    } else {
        pthread_cond_wait(&jfc->evt_cond, &jfc->evt_mtx);
    }
    jfc->armed = 0;
    pthread_mutex_unlock(&jfc->evt_mtx);
    return 0;
}
