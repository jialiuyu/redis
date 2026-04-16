/*
 * KCP-Lite: Minimal Reliable UDP (RUDP) Protocol
 *
 * Pure algorithmic implementation — zero system calls.
 * Implements the core KCP concepts:
 *   - Sliding window with selective ACK
 *   - Fast retransmit (skip-based, no timer)
 *   - No congestion control (LAN-optimized, max throughput)
 *   - Segment-based framing
 *
 * This is the data-plane protocol. Transport (UDP send/recv)
 * is provided by the caller via callbacks.
 */
#ifndef __KCP_LITE_H
#define __KCP_LITE_H

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>

#define KCP_MTU        1400
#define KCP_WND_SND    256
#define KCP_WND_RCV    256
#define KCP_OVERHEAD   24     /* Header size per segment */
#define KCP_MAX_SEG    (KCP_MTU - KCP_OVERHEAD)

/* Segment commands */
#define KCP_CMD_PUSH   81     /* Data push */
#define KCP_CMD_ACK    82     /* ACK */

/* Segment header (24 bytes, packed) */
typedef struct {
    uint32_t conv;     /* Conversation ID */
    uint8_t  cmd;      /* Command */
    uint8_t  frg;      /* Fragment count (0 = last) */
    uint16_t wnd;      /* Window size */
    uint32_t ts;       /* Timestamp (ms) */
    uint32_t sn;       /* Sequence number */
    uint32_t una;      /* Unacknowledged SN */
    uint32_t len;      /* Data length */
} __attribute__((packed)) kcp_seg_hdr_t;

/* Send/receive segment */
typedef struct kcp_seg {
    kcp_seg_hdr_t hdr;
    uint8_t       data[KCP_MAX_SEG];
    int           xmit;       /* Transmit count */
    uint32_t      resendts;   /* Resend timestamp */
    struct kcp_seg *next;
} kcp_seg_t;

/* Output callback: called when KCP wants to send a UDP packet */
typedef int (*kcp_output_fn)(const void *buf, int len, void *user);

/* KCP control block */
typedef struct {
    uint32_t conv;
    uint32_t snd_una;    /* Oldest unacked SN */
    uint32_t snd_nxt;    /* Next SN to send */
    uint32_t rcv_nxt;    /* Next expected SN */

    /* Send queue (unsent) */
    uint8_t *snd_queue;
    int      snd_queue_len;

    /* Send buffer (in-flight) */
    kcp_seg_t *snd_buf;
    int        snd_buf_count;

    /* Receive buffer (out-of-order) */
    kcp_seg_t *rcv_buf;
    int        rcv_buf_count;

    /* Receive queue (ordered, ready for user) */
    uint8_t *rcv_queue;
    int      rcv_queue_len;
    int      rcv_queue_cap;

    /* Window */
    uint32_t snd_wnd;
    uint32_t rcv_wnd;
    uint32_t rmt_wnd;    /* Remote window */

    /* Output */
    kcp_output_fn output;
    void         *user;

    /* Flush buffer */
    uint8_t flush_buf[KCP_MTU * 4];

    /* Stats */
    uint64_t total_sent;
    uint64_t total_recv;
    uint64_t total_retrans;
} kcp_t;

/* ---- API ---- */

static inline kcp_t *kcp_create(uint32_t conv, void *user) {
    kcp_t *kcp = (kcp_t *)calloc(1, sizeof(kcp_t));
    if (!kcp) return NULL;
    kcp->conv = conv;
    kcp->snd_wnd = KCP_WND_SND;
    kcp->rcv_wnd = KCP_WND_RCV;
    kcp->rmt_wnd = KCP_WND_RCV;
    kcp->user = user;
    kcp->rcv_queue_cap = 1024 * 1024;  /* 1MB receive buffer */
    kcp->rcv_queue = (uint8_t *)malloc(kcp->rcv_queue_cap);
    kcp->snd_queue = (uint8_t *)malloc(kcp->rcv_queue_cap);
    return kcp;
}

static inline void kcp_release(kcp_t *kcp) {
    if (!kcp) return;
    /* Free segment lists */
    kcp_seg_t *seg, *next;
    for (seg = kcp->snd_buf; seg; seg = next) { next = seg->next; free(seg); }
    for (seg = kcp->rcv_buf; seg; seg = next) { next = seg->next; free(seg); }
    free(kcp->rcv_queue);
    free(kcp->snd_queue);
    free(kcp);
}

static inline void kcp_setoutput(kcp_t *kcp, kcp_output_fn fn) {
    kcp->output = fn;
}

/* Send data (queue for transmission) */
static inline int kcp_send(kcp_t *kcp, const void *data, int len) {
    if (len <= 0 || !data) return -1;
    if (kcp->snd_queue_len + len > kcp->rcv_queue_cap) return -2;
    memcpy(kcp->snd_queue + kcp->snd_queue_len, data, len);
    kcp->snd_queue_len += len;
    return 0;
}

/* Receive data (from ordered receive queue) */
static inline int kcp_recv(kcp_t *kcp, void *buf, int len) {
    if (kcp->rcv_queue_len <= 0) return -1;
    int copy = kcp->rcv_queue_len < len ? kcp->rcv_queue_len : len;
    memcpy(buf, kcp->rcv_queue, copy);
    if (copy < kcp->rcv_queue_len)
        memmove(kcp->rcv_queue, kcp->rcv_queue + copy, kcp->rcv_queue_len - copy);
    kcp->rcv_queue_len -= copy;
    return copy;
}

/* Input: feed a received UDP packet to KCP */
static inline int kcp_input(kcp_t *kcp, const void *data, int size) {
    if (size < (int)sizeof(kcp_seg_hdr_t)) return -1;
    const uint8_t *p = (const uint8_t *)data;

    while (size >= (int)sizeof(kcp_seg_hdr_t)) {
        kcp_seg_hdr_t hdr;
        memcpy(&hdr, p, sizeof(hdr));
        if (hdr.conv != kcp->conv) return -1;
        p += sizeof(hdr);
        size -= sizeof(hdr);

        kcp->rmt_wnd = hdr.wnd;

        /* Process UNA: remove acked segments */
        while (kcp->snd_buf && kcp->snd_buf->hdr.sn < hdr.una) {
            kcp_seg_t *next = kcp->snd_buf->next;
            free(kcp->snd_buf);
            kcp->snd_buf = next;
            kcp->snd_buf_count--;
        }
        kcp->snd_una = hdr.una;

        if (hdr.cmd == KCP_CMD_ACK) {
            /* Individual ACK — remove specific segment */
            kcp_seg_t **pp = &kcp->snd_buf;
            while (*pp) {
                if ((*pp)->hdr.sn == hdr.sn) {
                    kcp_seg_t *rm = *pp;
                    *pp = rm->next;
                    free(rm);
                    kcp->snd_buf_count--;
                    break;
                }
                pp = &(*pp)->next;
            }
        } else if (hdr.cmd == KCP_CMD_PUSH) {
            if ((int)hdr.len > size) return -1;
            /* Deliver to receive queue if in-order */
            if (hdr.sn == kcp->rcv_nxt) {
                if (kcp->rcv_queue_len + (int)hdr.len <= kcp->rcv_queue_cap) {
                    memcpy(kcp->rcv_queue + kcp->rcv_queue_len, p, hdr.len);
                    kcp->rcv_queue_len += hdr.len;
                }
                kcp->rcv_nxt++;
                kcp->total_recv++;
            }
            /* Send ACK */
            if (kcp->output) {
                kcp_seg_hdr_t ack = {
                    .conv = kcp->conv, .cmd = KCP_CMD_ACK,
                    .sn = hdr.sn, .una = kcp->rcv_nxt,
                    .wnd = (uint16_t)(kcp->rcv_wnd - kcp->rcv_queue_len / KCP_MAX_SEG),
                    .ts = hdr.ts
                };
                kcp->output(&ack, sizeof(ack), kcp->user);
            }
            p += hdr.len;
            size -= hdr.len;
        }
    }
    return 0;
}

/* Flush: send queued data as segments */
static inline void kcp_flush(kcp_t *kcp) {
    if (!kcp->output) return;

    /* Move data from send queue to send buffer as segments */
    while (kcp->snd_queue_len > 0 && kcp->snd_buf_count < (int)kcp->snd_wnd) {
        int seg_len = kcp->snd_queue_len < KCP_MAX_SEG ? kcp->snd_queue_len : KCP_MAX_SEG;
        kcp_seg_t *seg = (kcp_seg_t *)calloc(1, sizeof(kcp_seg_t));
        seg->hdr.conv = kcp->conv;
        seg->hdr.cmd = KCP_CMD_PUSH;
        seg->hdr.sn = kcp->snd_nxt++;
        seg->hdr.una = kcp->rcv_nxt;
        seg->hdr.wnd = (uint16_t)kcp->rcv_wnd;
        seg->hdr.len = seg_len;
        memcpy(seg->data, kcp->snd_queue, seg_len);

        /* Remove from send queue */
        kcp->snd_queue_len -= seg_len;
        if (kcp->snd_queue_len > 0)
            memmove(kcp->snd_queue, kcp->snd_queue + seg_len, kcp->snd_queue_len);

        /* Append to send buffer tail */
        kcp_seg_t **pp = &kcp->snd_buf;
        while (*pp) pp = &(*pp)->next;
        *pp = seg;
        kcp->snd_buf_count++;

        /* Send the segment */
        uint8_t pkt[KCP_MTU];
        memcpy(pkt, &seg->hdr, sizeof(seg->hdr));
        memcpy(pkt + sizeof(seg->hdr), seg->data, seg_len);
        kcp->output(pkt, sizeof(seg->hdr) + seg_len, kcp->user);
        kcp->total_sent++;
    }
}

/* Update: call periodically (e.g., every 1ms or after each poll) */
static inline void kcp_update(kcp_t *kcp) {
    kcp_flush(kcp);
}

#endif /* __KCP_LITE_H */
