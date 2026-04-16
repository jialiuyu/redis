/*
 * URMA — Simplified User-space Remote Memory Access
 *
 * Implements the core URMA abstractions from UB-Software-Reference-Design-2.0:
 *   - Jetty: communication endpoint (JFS/JFR/JFC)
 *   - Segment: registered memory region with token protection
 *   - Single-sided: read/write remote segment (no remote CPU)
 *   - Double-sided: send/recv messages
 *   - Completion: poll-based (low latency) or event-based
 *
 * Transport: shared memory (same-node) or TCP (cross-node fallback).
 * On real UB hardware, this maps to UDMA driver + ubcore + liburma.
 * Here we implement the userspace semantics over mmap + futex.
 */
#ifndef __URMA_H
#define __URMA_H

#include <stdint.h>
#include <stddef.h>
#include <stdatomic.h>
#include <pthread.h>

/* ---- Constants ---- */
#define URMA_MAX_SEGMENTS    256
#define URMA_MAX_JETTIES     64
#define URMA_JFC_DEPTH       4096
#define URMA_TOKEN_SIZE      8
#define URMA_MAX_SGE         8       /* Max scatter-gather entries */
#define URMA_MAX_INLINE      1024    /* Max inline data */

/* ---- Entity ID (EID) ---- */
typedef struct {
    uint32_t node_id;    /* Compute node */
    uint32_t dev_id;     /* Device within node */
} urma_eid_t;

/* ---- Token for access control ---- */
typedef struct {
    uint64_t value;
} urma_token_t;

/* ---- Segment: registered memory region ---- */
typedef struct {
    uint32_t    seg_id;
    void       *va;           /* Virtual address */
    uint64_t    uba;          /* UB Address (for remote access) */
    size_t      len;
    urma_token_t token;
    urma_eid_t   owner;       /* Home side EID */
    int          is_local;
    /* Internal */
    int          shm_fd;      /* Shared memory fd (for cross-process) */
} urma_seg_t;

/* ---- Target Segment (imported remote segment) ---- */
typedef struct {
    uint32_t    seg_id;
    void       *mapped_va;    /* Local mapping of remote memory */
    uint64_t    uba;
    size_t      len;
    urma_token_t token;
    urma_eid_t   remote_eid;
} urma_target_seg_t;

/* ---- Opcodes ---- */
typedef enum {
    URMA_OPC_SEND = 0,
    URMA_OPC_RECV,
    URMA_OPC_READ,
    URMA_OPC_WRITE,
    URMA_OPC_CAS,       /* Compare-and-swap */
    URMA_OPC_FAA,       /* Fetch-and-add */
} urma_opcode_t;

/* ---- Scatter-Gather Entry ---- */
typedef struct {
    void    *addr;
    size_t   len;
    uint32_t seg_id;     /* Local segment */
} urma_sge_t;

/* ---- Work Request ---- */
typedef struct {
    urma_opcode_t opcode;
    uint64_t      user_ctx;     /* User context (returned in completion) */
    /* For send/recv */
    urma_sge_t    sge[URMA_MAX_SGE];
    int           num_sge;
    /* For read/write (single-sided) */
    struct {
        uint64_t  remote_uba;
        size_t    remote_len;
        uint32_t  remote_seg_id;
        urma_token_t remote_token;
    } rdma;
    /* Inline data (small messages) */
    uint8_t       inline_data[URMA_MAX_INLINE];
    size_t        inline_len;
    /* Flags */
    int           signal_completion;
    int           use_inline;
} urma_wr_t;

/* ---- Completion Queue Entry ---- */
typedef struct {
    uint64_t      user_ctx;
    urma_opcode_t opcode;
    int           status;       /* 0 = success */
    size_t        completion_len;
    uint64_t      timestamp_ns;
} urma_cqe_t;

/* ---- Jetty For Completion (JFC) ---- */
typedef struct {
    uint32_t    jfc_id;
    urma_cqe_t *ring;
    uint32_t    depth;
    atomic_uint_fast64_t head;  /* Consumer reads here */
    atomic_uint_fast64_t tail;  /* Producer writes here */
    uint32_t    mask;
    /* Event notification */
    pthread_mutex_t evt_mtx;
    pthread_cond_t  evt_cond;
    volatile int    armed;
} urma_jfc_t;

/* ---- Jetty For Sending (JFS) ---- */
typedef struct {
    uint32_t    jfs_id;
    urma_jfc_t *jfc;           /* Associated completion queue */
    urma_eid_t  local_eid;
} urma_jfs_t;

/* ---- Jetty For Receiving (JFR) ---- */
typedef struct {
    uint32_t    jfr_id;
    urma_jfc_t *jfc;
    urma_eid_t  local_eid;
    /* Receive buffer pool */
    void       *recv_bufs;
    size_t      recv_buf_size;
    int         recv_buf_count;
    atomic_int  recv_posted;
} urma_jfr_t;

/* ---- URMA Context (per-device) ---- */
typedef struct {
    urma_eid_t  local_eid;
    urma_seg_t  segments[URMA_MAX_SEGMENTS];
    int         num_segments;
    urma_jfc_t  jfcs[URMA_MAX_JETTIES];
    int         num_jfcs;
    urma_jfs_t  jfss[URMA_MAX_JETTIES];
    int         num_jfss;
    urma_jfr_t  jfrs[URMA_MAX_JETTIES];
    int         num_jfrs;
    /* Stats */
    atomic_uint_fast64_t total_reads;
    atomic_uint_fast64_t total_writes;
    atomic_uint_fast64_t total_sends;
    atomic_uint_fast64_t total_recvs;
    atomic_uint_fast64_t total_bytes;
    atomic_uint_fast64_t total_completions;
} urma_ctx_t;

/* ============================================================
 * URMA API
 * ============================================================ */

/* Context management */
urma_ctx_t *urma_create_ctx(uint32_t node_id, uint32_t dev_id);
void        urma_destroy_ctx(urma_ctx_t *ctx);

/* Segment management */
urma_seg_t *urma_register_seg(urma_ctx_t *ctx, void *va, size_t len, urma_token_t token);
void        urma_unregister_seg(urma_ctx_t *ctx, urma_seg_t *seg);
urma_target_seg_t *urma_import_seg(urma_ctx_t *ctx, urma_eid_t remote_eid,
                                    uint32_t seg_id, urma_token_t token, size_t len);
void        urma_unimport_seg(urma_ctx_t *ctx, urma_target_seg_t *tseg);

/* Jetty management */
urma_jfc_t *urma_create_jfc(urma_ctx_t *ctx, uint32_t depth);
void        urma_destroy_jfc(urma_ctx_t *ctx, urma_jfc_t *jfc);
urma_jfs_t *urma_create_jfs(urma_ctx_t *ctx, urma_jfc_t *jfc);
void        urma_destroy_jfs(urma_ctx_t *ctx, urma_jfs_t *jfs);
urma_jfr_t *urma_create_jfr(urma_ctx_t *ctx, urma_jfc_t *jfc, size_t buf_size, int buf_count);
void        urma_destroy_jfr(urma_ctx_t *ctx, urma_jfr_t *jfr);

/* Data operations */
int urma_post_send(urma_jfs_t *jfs, const urma_wr_t *wr);
int urma_post_recv(urma_jfr_t *jfr, const urma_wr_t *wr);
int urma_read(urma_jfs_t *jfs, urma_sge_t *local_sge, urma_target_seg_t *remote_seg,
              uint64_t remote_offset, size_t len, uint64_t user_ctx);
int urma_write(urma_jfs_t *jfs, urma_sge_t *local_sge, urma_target_seg_t *remote_seg,
               uint64_t remote_offset, size_t len, uint64_t user_ctx);

/* Completion */
int  urma_poll_jfc(urma_jfc_t *jfc, urma_cqe_t *cqes, int max_cqes);
void urma_rearm_jfc(urma_jfc_t *jfc);
int  urma_wait_jfc(urma_jfc_t *jfc, int timeout_ms);

#endif /* __URMA_H */
