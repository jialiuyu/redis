/*
 * WeChat 2x3 HA — High Availability with 4-Layer Heartbeat
 *
 * Architecture:
 *   2 full replicas (IDC-A, IDC-B), each with 3 shards = 6 partitions
 *   Assumption: two IDCs cannot fail simultaneously
 *
 * Heartbeat layers:
 *   L1: Process-level (pthread health check, 10ms)
 *   L2: Node-level (cross-shard ping, 100ms)
 *   L3: IDC-level (cross-IDC heartbeat, 500ms)
 *   L4: External arbiter (optional, 1s)
 *
 * Failover:
 *   When node detects peer heartbeat failure:
 *   1. Self-promote to leader
 *   2. Send last_success_id to failed peer
 *   3. Failed peer replays from last_success_id on recovery
 *
 * Split-brain prevention:
 *   Based on assumption: P(both IDCs fail) ≈ 0
 *   If A detects B down → A becomes leader
 *   If B detects A down → B becomes leader
 *   Both detect each other down → impossible (assumption)
 */
#ifndef __WECHAT_HA_H
#define __WECHAT_HA_H

#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <time.h>
#include <string.h>

#define HA_NUM_REPLICAS  2
#define HA_NUM_SHARDS    3
#define HA_TOTAL_PARTS   (HA_NUM_REPLICAS * HA_NUM_SHARDS)
#define HA_HB_L1_MS      10    /* Process heartbeat */
#define HA_HB_L2_MS      100   /* Node heartbeat */
#define HA_HB_L3_MS      500   /* IDC heartbeat */
#define HA_HB_TIMEOUT_MS 2000  /* Failure detection threshold */
#define HA_REPLAY_LOG_SIZE 65536

typedef struct {
    int idc_id;       /* 0 = IDC-A, 1 = IDC-B */
    int shard_id;     /* 0, 1, 2 */
    int is_leader;
    int is_alive;
    atomic_uint_fast64_t last_success_id;
    atomic_uint_fast64_t last_heartbeat_ms;
} wc_ha_partition_t;

/* Replay log entry */
typedef struct {
    uint64_t op_id;
    uint64_t key;
    uint8_t  op;      /* 1=PUT, 2=DEL */
    uint8_t  value[1200];  /* For PUT */
} __attribute__((packed)) wc_replay_entry_t;

/* Replay log (circular buffer) */
typedef struct {
    wc_replay_entry_t entries[HA_REPLAY_LOG_SIZE];
    atomic_uint_fast64_t head;
    atomic_uint_fast64_t tail;
    atomic_uint_fast64_t next_op_id;
} wc_replay_log_t;

typedef struct {
    int my_idc;
    int my_shard;
    wc_ha_partition_t parts[HA_TOTAL_PARTS];
    wc_replay_log_t replay_log;
    atomic_int failed_idc;
    atomic_uint_fast64_t failover_count;
    atomic_uint_fast64_t recovery_count;
    atomic_uint_fast64_t replayed_ops;
    pthread_t hb_thread;
    volatile int running;
} wechat_ha_t;

static inline uint64_t ha_now_ms(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + ts.tv_nsec / 1000000;
}

static inline void ha_init(wechat_ha_t *ha, int idc, int shard) {
    memset(ha, 0, sizeof(*ha));
    ha->my_idc = idc;
    ha->my_shard = shard;
    atomic_store(&ha->failed_idc, -1);
    atomic_store(&ha->replay_log.next_op_id, 1);

    for (int r = 0; r < HA_NUM_REPLICAS; r++) {
        for (int s = 0; s < HA_NUM_SHARDS; s++) {
            int p = r * HA_NUM_SHARDS + s;
            ha->parts[p].idc_id = r;
            ha->parts[p].shard_id = s;
            ha->parts[p].is_leader = (r == 0);  /* IDC-A is default leader */
            ha->parts[p].is_alive = 1;
            atomic_store(&ha->parts[p].last_success_id, 0);
            atomic_store(&ha->parts[p].last_heartbeat_ms, ha_now_ms());
        }
    }
    ha->running = 1;
}

/* Record an operation to the replay log */
static inline uint64_t ha_log_op(wechat_ha_t *ha, uint64_t key, uint8_t op,
                                  const void *value, size_t vlen) {
    uint64_t op_id = atomic_fetch_add(&ha->replay_log.next_op_id, 1);
    uint64_t tail = atomic_fetch_add(&ha->replay_log.tail, 1);
    wc_replay_entry_t *e = &ha->replay_log.entries[tail % HA_REPLAY_LOG_SIZE];
    e->op_id = op_id;
    e->key = key;
    e->op = op;
    if (value && vlen > 0) {
        size_t copy = vlen < sizeof(e->value) ? vlen : sizeof(e->value);
        memcpy(e->value, value, copy);
    }
    return op_id;
}

/* Simulate heartbeat update (called by peer) */
static inline void ha_heartbeat(wechat_ha_t *ha, int from_idc) {
    for (int s = 0; s < HA_NUM_SHARDS; s++) {
        int p = from_idc * HA_NUM_SHARDS + s;
        atomic_store(&ha->parts[p].last_heartbeat_ms, ha_now_ms());
    }
}

/* Check for failures and auto-failover */
static inline int ha_check_failover(wechat_ha_t *ha) {
    uint64_t now = ha_now_ms();
    int peer_idc = 1 - ha->my_idc;

    /* Check peer IDC heartbeat */
    int peer_alive = 0;
    for (int s = 0; s < HA_NUM_SHARDS; s++) {
        int p = peer_idc * HA_NUM_SHARDS + s;
        uint64_t last_hb = atomic_load(&ha->parts[p].last_heartbeat_ms);
        if (now - last_hb < HA_HB_TIMEOUT_MS) {
            peer_alive = 1;
            break;
        }
    }

    if (!peer_alive && atomic_load(&ha->failed_idc) != peer_idc) {
        /* Peer IDC failed — self-promote to leader */
        atomic_store(&ha->failed_idc, peer_idc);
        for (int s = 0; s < HA_NUM_SHARDS; s++) {
            ha->parts[peer_idc * HA_NUM_SHARDS + s].is_alive = 0;
            ha->parts[ha->my_idc * HA_NUM_SHARDS + s].is_leader = 1;
        }
        atomic_fetch_add(&ha->failover_count, 1);
        return 1;  /* Failover occurred */
    }
    return 0;
}

/* Recover peer IDC */
static inline int ha_recover(wechat_ha_t *ha, int recovered_idc, uint64_t from_op_id) {
    atomic_store(&ha->failed_idc, -1);
    for (int s = 0; s < HA_NUM_SHARDS; s++) {
        ha->parts[recovered_idc * HA_NUM_SHARDS + s].is_alive = 1;
        atomic_store(&ha->parts[recovered_idc * HA_NUM_SHARDS + s].last_heartbeat_ms, ha_now_ms());
    }

    /* Replay operations from from_op_id */
    uint64_t head = atomic_load(&ha->replay_log.head);
    uint64_t tail = atomic_load(&ha->replay_log.tail);
    int replayed = 0;
    for (uint64_t i = head; i < tail; i++) {
        wc_replay_entry_t *e = &ha->replay_log.entries[i % HA_REPLAY_LOG_SIZE];
        if (e->op_id >= from_op_id) {
            replayed++;
            /* In real implementation: send e to recovered peer for replay */
        }
    }
    atomic_fetch_add(&ha->recovery_count, 1);
    atomic_fetch_add(&ha->replayed_ops, replayed);
    return replayed;
}

/* Get partition for a key (routes to alive shard) */
static inline int ha_get_partition(wechat_ha_t *ha, uint64_t key) {
    int shard = (int)(key % HA_NUM_SHARDS);
    int failed = atomic_load(&ha->failed_idc);
    int idc = (failed == 0) ? 1 : 0;  /* Use non-failed IDC */
    return idc * HA_NUM_SHARDS + shard;
}

#endif /* __WECHAT_HA_H */
