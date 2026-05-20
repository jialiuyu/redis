/*
 * tlc_hash_fc_server.c — Hash Strategy Bench with Flat Combining
 *
 * Architecture: EXACTLY matches tlc_v16_server.c
 *   - Flat Combining with TTAS combiner election
 *   - direct_lookup() for HOT→WARM→COLD
 *   - Per-channel threads with CPU pinning
 *   - SVE2 prefetch gather in combiner path
 *   - Aeron IPC shared memory rings
 *
 * Difference from v16: HOT layer uses hash_strategy.h + eviction_strategy.h
 *   instead of hardcoded hot_hash_v2_*
 *
 * Compile (on ARM):
 *   cc -O3 -std=c11 -pthread -I. -DUSE_TLC \
 *      -DHASH_STRATEGY=1 -DEVICTION_STRATEGY=0 \
 *      [-DENABLE_COLLISION_STATS] \
 *      -o tlc_hash_fc_server_v1_blind \
 *      tlc_hash_fc_server.c tlc_hash_fc_cache.c -lm -lpthread
 */
#define _GNU_SOURCE
#include "aeron_ipc.h"
#include "three_layer_cache_ub.h"
#include "hash_strategy.h"
#include "eviction_strategy.h"
/* === BASELINE placement (inline, was placement_strategy.h) === */
static inline const char *placement_strategy_name(void) { return "BASELINE"; }

static inline int placement_insert_hot(hot_index_t *table, uint64_t key, int32_t warm_idx, uint32_t mask) {
    uint32_t slots[HASH_MAX_PROBES];
    for (uint32_t i = 0; i < HASH_MAX_PROBES; i++)
        slots[i] = hash_probe(key, mask, (int)i);
    for (int i = 0; i < HASH_MAX_PROBES; i++) {
        hot_index_t cur = table[slots[i]];
        if (cur.warm_idx < 0 || cur.key == key) {
            if (cur.key == key) eviction_on_put_hit(slots[i]);
            table[slots[i]] = (hot_index_t){key, warm_idx, 0};
            return i;
        }
    }
    uint32_t victim = eviction_select_victim_slots(slots, HASH_MAX_PROBES);
    table[victim] = (hot_index_t){key, warm_idx, 0};
    return HASH_MAX_PROBES;
}

static inline int32_t placement_lookup_hot(hot_index_t *table, uint64_t key, uint32_t mask,
                                           int *out_probe_bucket, int *out_collided) {
    int32_t warm_idx = -1;
    int collided = 0;
    for (int j = 0; j < HASH_MAX_PROBES; j++) {
        uint32_t s = hash_probe(key, mask, j);
        hot_index_t e = table[s];
        if (e.warm_idx >= 0 && e.key == key) {
            warm_idx = e.warm_idx;
            eviction_on_get(s);
            if (out_probe_bucket) *out_probe_bucket = j;
            if (out_collided) *out_collided = collided;
            return warm_idx;
        }
        if (e.warm_idx >= 0) collided = 1;
        if (e.warm_idx < 0) break;
    }
    if (out_probe_bucket) *out_probe_bucket = HASH_MAX_PROBES;
    if (out_collided) *out_collided = collided;
    return -1;
}

/* Cache module — standalone, no SVE2/HA deps */
extern int  fc_cache_init(three_layer_cache_t *c);
extern void fc_cache_destroy(three_layer_cache_t *c);
extern int  fc_cache_warm_put(three_layer_cache_t *c, uint64_t key, const void *val, int32_t *out_idx);
extern int  fc_cache_cold_append(cold_layer_t *c, uint64_t key, const void *val);
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sched.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/un.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <stdatomic.h>

/* ---- L0 Register-grade Signature Cache ----
 * Per-thread tiny cache: 8 entries of (16-bit sig + warm_idx).
 * Lookup uses NEON (ARM) or SSE2 (x86) parallel compare for zero-memory
 * hit/miss determination.
 */
#if defined(__aarch64__)
#include <arm_neon.h>
#elif defined(__x86_64__)
#include <emmintrin.h>
#endif

#ifndef L0_CACHE_SIZE
#define L0_CACHE_SIZE 64
#endif

static inline uint16_t hash_sig16(uint64_t key) {
    /* Re-use murmur3 finalizer, keep high 16 bits */
    uint32_t h = _hs_murmur3_mix64(key);
    return (uint16_t)(h >> 16);
}

/* ---- Frozen L0 cache: read-only snapshot after warmup ---- */
typedef struct {
    uint16_t sig[L0_CACHE_SIZE];
    uint64_t key[L0_CACHE_SIZE];
    int32_t  widx[L0_CACHE_SIZE];
    uint64_t valid;
} l0_frozen_cache_t;

static inline int32_t l0_lookup_frozen(l0_frozen_cache_t *l0, uint64_t key) {
    uint16_t sig = hash_sig16(key);
    uint64_t valid = l0->valid;
    if (!valid) return -1;

#if defined(__aarch64__)
    uint16x8_t v_sig = vdupq_n_u16(sig);
    for (int base = 0; base < L0_CACHE_SIZE; base += 8) {
        uint16x8_t v_cache = vld1q_u16(l0->sig + base);
        uint16x8_t v_eq = vceqq_u16(v_sig, v_cache);
        uint8x8_t  v_narrow = vmovn_u16(v_eq);
        uint64_t mask = vget_lane_u64(vreinterpret_u64_u8(v_narrow), 0);
        while (mask) {
            int lane = __builtin_ctzll(mask) >> 3; /* 8 bits per lane -> /8 */
            int i = base + lane;
            if ((valid & (1ULL << i)) && l0->key[i] == key) {
                return l0->widx[i];
            }
            mask &= ~(0xFFULL << (lane * 8));
        }
    }
#elif defined(__x86_64__)
    __m128i v_sig = _mm_set1_epi16((short)sig);
    for (int base = 0; base < L0_CACHE_SIZE; base += 8) {
        __m128i v_cache = _mm_loadu_si128((__m128i const*)(l0->sig + base));
        __m128i v_eq = _mm_cmpeq_epi16(v_sig, v_cache);
        int mask = _mm_movemask_epi8(v_eq);
        while (mask) {
            int lane = __builtin_ctz(mask) >> 1; /* 2 bytes per lane -> /2 */
            int i = base + lane;
            if ((valid & (1ULL << i)) && l0->key[i] == key) {
                return l0->widx[i];
            }
            mask &= ~(3 << (lane * 2));
        }
    }
#else
    for (int i = 0; i < L0_CACHE_SIZE; i++) {
        if ((valid & (1ULL << i)) && l0->sig[i] == sig && l0->key[i] == key) {
            return l0->widx[i];
        }
    }
#endif
    return -1;
}

/* ---- L0 Warmup Cache: large-window LFU, linear scan (only during warmup) ---- */
#ifndef L0_WARMUP_SIZE
#define L0_WARMUP_SIZE 256
#endif

typedef struct {
    uint16_t sig[L0_WARMUP_SIZE];
    uint64_t key[L0_WARMUP_SIZE];
    int32_t  widx[L0_WARMUP_SIZE];
    uint32_t freq[L0_WARMUP_SIZE];
    int      count;
} l0_warmup_cache_t;

static inline int32_t l0_lookup_warmup(l0_warmup_cache_t *l0, uint64_t key) {
    uint16_t sig = hash_sig16(key);
    for (int i = 0; i < l0->count; i++) {
        if (l0->sig[i] == sig && l0->key[i] == key) {
            l0->freq[i]++;
            return l0->widx[i];
        }
    }
    return -1;
}

static inline void l0_warmup_promote(l0_warmup_cache_t *l0, uint64_t key, int32_t widx) {
    if (l0->count < L0_WARMUP_SIZE) {
        int idx = l0->count++;
        l0->sig[idx] = hash_sig16(key);
        l0->key[idx] = key;
        l0->widx[idx] = widx;
        l0->freq[idx] = 1;
    } else {
        int idx = 0;
        uint32_t min_freq = l0->freq[0];
        for (int i = 1; i < L0_WARMUP_SIZE; i++) {
            if (l0->freq[i] < min_freq) {
                min_freq = l0->freq[i];
                idx = i;
            }
        }
        l0->sig[idx] = hash_sig16(key);
        l0->key[idx] = key;
        l0->widx[idx] = widx;
        l0->freq[idx] = 1;
    }
}

static inline void l0_freeze_from_warmup(l0_warmup_cache_t *src, l0_frozen_cache_t *dst) {
    int indices[L0_WARMUP_SIZE];
    for (int i = 0; i < src->count; i++) indices[i] = i;

    /* selection sort by freq descending */
    for (int i = 0; i < src->count - 1; i++) {
        for (int j = i + 1; j < src->count; j++) {
            if (src->freq[indices[j]] > src->freq[indices[i]]) {
                int tmp = indices[i];
                indices[i] = indices[j];
                indices[j] = tmp;
            }
        }
    }

    int n = (src->count < L0_CACHE_SIZE) ? src->count : L0_CACHE_SIZE;
    dst->valid = 0;
    for (int i = 0; i < n; i++) {
        int src_idx = indices[i];
        dst->sig[i] = src->sig[src_idx];
        dst->key[i] = src->key[src_idx];
        dst->widx[i] = src->widx[src_idx];
        dst->valid |= (1ULL << i);
    }
}

#ifndef L0_WARMUP_OPS
#define L0_WARMUP_OPS 500000
#endif

#ifdef ENABLE_COLLISION_STATS
static volatile uint64_t g_l0_hits;
static volatile uint64_t g_l0_misses;
#define L0_HIT_INC()  __sync_fetch_and_add(&g_l0_hits, (uint64_t)1)
#define L0_MISS_INC() __sync_fetch_and_add(&g_l0_misses, (uint64_t)1)
#else
#define L0_HIT_INC()  ((void)0)
#define L0_MISS_INC() ((void)0)
#endif /* ENABLE_COLLISION_STATS */

#define UDS_PATH       "/tmp/tlc_hash_bench.sock"
#define MAX_SLOTS      64

#define OP_GET             0x01
#define OP_PUT             0x02
#define OP_STATS           0x04
#define OP_FILL            0x05
#define OP_PING            0x06
#define OP_CLEAR_COUNTERS  0x07
#define OP_DUMP_COLLISION  0x08
#define OP_ALLOC_CHANNEL   0x20

#define SLOT_EMPTY   0
#define SLOT_PENDING 1
#define SLOT_DONE    2

/* ---- Collision Stats (optional) ---- */
#ifdef ENABLE_COLLISION_STATS
static atomic_uint_fast64_t g_hot_get_probe[HASH_MAX_PROBES + 1];
static atomic_uint_fast64_t g_hot_put_probe[HASH_MAX_PROBES + 1];
static atomic_uint_fast64_t g_warm_get_probe[7];
static atomic_uint_fast64_t g_total_hot_get;
static atomic_uint_fast64_t g_total_warm_get;
#define COLL_INC(c) atomic_fetch_add_explicit(&(c), 1, memory_order_relaxed)
#else
#define COLL_INC(c) ((void)0)
#endif

/* ---- Publication Board Slot (cache-line aligned) — same as v16 ---- */
typedef struct __attribute__((aligned(64))) {
    uint64_t key;
    int32_t  warm_idx;
    atomic_int state;
} fc_slot_t;

/* ---- Global State ---- */
static fc_slot_t g_slots[MAX_SLOTS];
static atomic_int g_combiner_lock = 0;
static three_layer_cache_t g_cache;
static volatile int g_running = 1;

static atomic_uint_fast64_t g_total_ops = 0;
static atomic_uint_fast64_t g_combine_rounds = 0;
static atomic_uint_fast64_t g_combine_total_keys = 0;
static atomic_uint_fast64_t g_direct_ops = 0;
static atomic_uint_fast64_t g_batch_ops = 0;

/* hash_fast for WARM layer — same as v16 */
static inline uint32_t hash_pf(uint64_t k, uint32_t mask) {
    k ^= k >> 33; k *= 0xff51afd7ed558ccdULL; k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL; k ^= k >> 33; return (uint32_t)(k & mask);
}

/* ---- Channel state — same as v16 ---- */
typedef struct {
    int id;
    aeron_ring_t *req, *resp;
    pthread_t thread;
    int active;
    atomic_uint_fast64_t ops;
} channel_t;

#define MAX_CHANNELS 64
static channel_t g_channels[MAX_CHANNELS];
static atomic_int g_num_channels_fc = 0;

/* ---- HOT PUT with hash_strategy.h + eviction_strategy.h ---- */
static inline void hot_put_fc(uint64_t key, int32_t warm_idx) {
    uint32_t mask = g_cache.hot.mask;
    int probe_bucket = placement_insert_hot(g_cache.hot.table,
                                            key, warm_idx, mask);
    if (probe_bucket >= 0 && probe_bucket < HASH_MAX_PROBES)
        COLL_INC(g_hot_put_probe[probe_bucket]);
    else
        COLL_INC(g_hot_put_probe[HASH_MAX_PROBES]);
}

/* ---- Direct Lookup — matches v16 structure, uses hash_strategy.h ---- */
static inline int32_t direct_lookup(uint64_t key) {
    uint32_t hot_mask = g_cache.hot.mask;
    int32_t warm_idx = -1;
    int hot_collided = 0;
    int probe_bucket = HASH_MAX_PROBES;

    warm_idx = placement_lookup_hot(g_cache.hot.table,
                                    key, hot_mask, &probe_bucket, &hot_collided);
    if (warm_idx >= 0) {
        if (probe_bucket >= 0 && probe_bucket < HASH_MAX_PROBES)
            COLL_INC(g_hot_get_probe[probe_bucket]);
        return warm_idx;
    }

    if (warm_idx < 0) {
        if (hot_collided) COLL_INC(g_hot_get_probe[HASH_MAX_PROBES]);
        COLL_INC(g_total_hot_get);

        /* WARM fallback — same as v16 */
        uint32_t wslot = hash_pf(key, g_cache.warm.mask);
        uint32_t lock_id = wslot % (uint32_t)g_cache.warm.bmp.num_words;
        bmp_lock_acquire(&g_cache.warm.bmp, lock_id);
        int warm_collided = 0;
        for (uint32_t j = 0; j < 6; j++) {
            int32_t idx = g_cache.warm.hash_table[(wslot + j) & g_cache.warm.mask];
            if (idx < 0) break;
            if (g_cache.warm.entries[idx].key == key &&
                g_cache.warm.entries[idx].state != ENTRY_EMPTY) {
                warm_idx = idx;
                COLL_INC(g_warm_get_probe[j]);
                break;
            }
            if (idx >= 0) warm_collided = 1;
        }
        bmp_lock_release(&g_cache.warm.bmp, lock_id);
        if (warm_idx < 0 && warm_collided) COLL_INC(g_warm_get_probe[6]);
        COLL_INC(g_total_warm_get);

        /* Promote to HOT — hash_strategy.h + eviction_strategy.h */
        if (warm_idx >= 0) {
            hot_put_fc(key, warm_idx);
        }
    }
    return warm_idx;
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t va = *(const uint64_t*)a, vb = *(const uint64_t*)b;
    return (va > vb) - (va < vb);
}

/* ---- Flat Combining GET — TTAS, EXACTLY v16 ---- */
static int32_t fc_get(int slot_id, uint64_t key) {
    g_slots[slot_id].key = key;
    g_slots[slot_id].warm_idx = -1;
    atomic_store_explicit(&g_slots[slot_id].state, SLOT_PENDING, memory_order_release);

    int became_combiner = 0;
    while (!became_combiner && atomic_load_explicit(&g_slots[slot_id].state, memory_order_acquire) == SLOT_PENDING) {
        if (atomic_load_explicit(&g_combiner_lock, memory_order_relaxed) == 0) {
            int expected = 0;
            became_combiner = atomic_compare_exchange_strong_explicit(
                &g_combiner_lock, &expected, 1,
                memory_order_acq_rel, memory_order_relaxed);
        }
        if (!became_combiner) {
#if defined(__aarch64__)
            __asm__ volatile("yield" ::: "memory");
#elif defined(__x86_64__)
            __asm__ volatile("pause" ::: "memory");
#else
            __asm__ volatile("" ::: "memory");
#endif
        }
    }

    if (became_combiner) {
        /* Ensure all PENDING stores from other threads are visible before scanning.
         * On ARM, store buffers can delay visibility of SLOT_PENDING writes;
         * without this barrier, the combiner may miss pending requests. */
        __sync_synchronize();

        int pending_ids[MAX_SLOTS];
        uint64_t pending_keys[MAX_SLOTS];
        int pending_count = 0;
        int num_channels = atomic_load_explicit(&g_num_channels_fc, memory_order_relaxed);

        for (int i = 0; i < num_channels && i < MAX_SLOTS; i++) {
            if (atomic_load_explicit(&g_slots[i].state, memory_order_acquire) == SLOT_PENDING) {
                pending_ids[pending_count] = i;
                pending_keys[pending_count] = g_slots[i].key;
                pending_count++;
            }
        }

        // TODO SVE2 batch gather with prefetch — same as v16
        if (pending_count > 0) {
            uint32_t hot_mask = g_cache.hot.mask;
            for (int p = 0; p < pending_count && p < 8; p++)
                __builtin_prefetch(&g_cache.hot.table[hash_primary(pending_keys[p], hot_mask)], 0, 3);

            for (int i = 0; i < pending_count; i++) {
                if (i + 8 < pending_count)
                    __builtin_prefetch(&g_cache.hot.table[hash_primary(pending_keys[i+8], hot_mask)], 0, 3);
                g_slots[pending_ids[i]].warm_idx = direct_lookup(pending_keys[i]);
            }
        }

        for (int i = 0; i < pending_count; i++)
            atomic_store_explicit(&g_slots[pending_ids[i]].state, SLOT_DONE, memory_order_release);

        atomic_store_explicit(&g_combiner_lock, 0, memory_order_release);
    }

    while (atomic_load_explicit(&g_slots[slot_id].state, memory_order_acquire) != SLOT_DONE)
        __asm__ volatile("" ::: "memory");

    int32_t result = g_slots[slot_id].warm_idx;
    atomic_store_explicit(&g_slots[slot_id].state, SLOT_EMPTY, memory_order_relaxed);
    return result;
}

/* ---- Channel Thread — same as v16 ---- */
static void *channel_poll(void *arg) {
    channel_t *ch = (channel_t *)arg;

    /* Pin to core — same as v16 */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(ch->id % 40, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    uint8_t req_buf[AERON_MSG_SIZE];
    uint8_t resp_buf[8];

    fprintf(stderr, "[ch%d] L0 cache init\n", ch->id);
    l0_warmup_cache_t warmup_cache;
    memset(&warmup_cache, 0, sizeof(warmup_cache));
    l0_frozen_cache_t frozen_cache;
    memset(&frozen_cache, 0, sizeof(frozen_cache));
    bool l0_frozen = false;
    uint64_t local_get_count = 0;
    uint64_t local_l0_hits = 0, local_l0_misses = 0;

    while (g_running && ch->active) {
        int rlen = aeron_poll(ch->req, req_buf, sizeof(req_buf));
        if (rlen <= 0) { __asm__ volatile("" ::: "memory"); continue; }

        uint8_t op = req_buf[0];

        if (op == OP_GET && rlen >= 9) {
            uint64_t key; memcpy(&key, req_buf + 1, 8);
            int32_t warm_idx = -1;

            if (l0_frozen) {
                warm_idx = l0_lookup_frozen(&frozen_cache, key);
            } else if (local_get_count < L0_WARMUP_OPS) {
                warm_idx = l0_lookup_warmup(&warmup_cache, key);
                local_get_count++;
            } else {
                l0_freeze_from_warmup(&warmup_cache, &frozen_cache);
                l0_frozen = true;
                warm_idx = l0_lookup_frozen(&frozen_cache, key);
            }
            if (warm_idx >= 0) {
                L0_HIT_INC();
                local_l0_hits++;
                resp_buf[0] = 0x00;
                memcpy(resp_buf + 1, &warm_idx, 4);
                while (aeron_publish(ch->resp, resp_buf, 5) != 0)
                    __asm__ volatile("" ::: "memory");
                continue;
            }
            L0_MISS_INC();
            local_l0_misses++;
            warm_idx = fc_get(ch->id, key);
            if (!l0_frozen && warm_idx >= 0) {
                l0_warmup_promote(&warmup_cache, key, warm_idx);
            }
            if (warm_idx >= 0) {
                resp_buf[0] = 0x00;
                memcpy(resp_buf + 1, &warm_idx, 4);
                while (aeron_publish(ch->resp, resp_buf, 5) != 0)
                    __asm__ volatile("" ::: "memory");
            } else {
                resp_buf[0] = 0x01;
                while (aeron_publish(ch->resp, resp_buf, 1) != 0)
                    __asm__ volatile("" ::: "memory");
            }
        } else if (op == OP_PUT && rlen >= 9 + TLC_VALUE_SIZE) {
            uint64_t key; memcpy(&key, req_buf + 1, 8);
            int32_t widx = -1;
            if (fc_cache_warm_put(&g_cache, key, req_buf + 9, &widx) != 0)
                fc_cache_cold_append(&g_cache.cold, key, req_buf + 9);
            if (widx >= 0) hot_put_fc(key, widx);

            resp_buf[0] = 0x00;
            while (aeron_publish(ch->resp, resp_buf, 1) != 0)
                __asm__ volatile("" ::: "memory");
        } else if (op == OP_PING) {
            resp_buf[0] = 0x00;
            while (aeron_publish(ch->resp, resp_buf, 1) != 0)
                __asm__ volatile("" ::: "memory");
        }
    }
    fprintf(stderr, "[ch%d] L0 local hits=%lu misses=%lu frozen_valid=%lu warmup_valid=%lu\n",
            ch->id, (unsigned long)local_l0_hits, (unsigned long)local_l0_misses,
            (unsigned long)frozen_cache.valid, (unsigned long)warmup_cache.count);
    return NULL;
}

/* ---- Channel Management — same as v16 ---- */
static int create_channel(void) {
    int id = atomic_fetch_add(&g_num_channels_fc, 1);
    if (id >= MAX_CHANNELS) return -1;
    channel_t *ch = &g_channels[id];
    ch->id = id;
    char name[64];
    snprintf(name, sizeof(name), "/tlc_hb_req_%d", id);
    int fd = shm_open(name, O_CREAT|O_RDWR, 0666); ftruncate(fd, sizeof(aeron_ring_t));
    ch->req = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd); memset(ch->req, 0, sizeof(aeron_ring_t));
    snprintf(name, sizeof(name), "/tlc_hb_resp_%d", id);
    fd = shm_open(name, O_CREAT|O_RDWR, 0666); ftruncate(fd, sizeof(aeron_ring_t));
    ch->resp = mmap(NULL, sizeof(aeron_ring_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd); memset(ch->resp, 0, sizeof(aeron_ring_t));
    atomic_store(&ch->ops, 0); ch->active = 1;
    atomic_store_explicit(&g_slots[id].state, SLOT_EMPTY, memory_order_relaxed);
    pthread_create(&ch->thread, NULL, channel_poll, ch);
    return id;
}

/* ---- UDS control plane ---- */
static int read_full(int fd, void *buf, size_t n) {
    size_t d=0; while(d<n){ssize_t r=read(fd,(char*)buf+d,n-d);if(r<=0)return -1;d+=r;} return 0;
}
static int write_full(int fd, const void *buf, size_t n) {
    size_t d=0; while(d<n){ssize_t w=write(fd,(const char*)buf+d,n-d);if(w<=0)return -1;d+=w;} return 0;
}

typedef struct { int tid; int epfd; } io_ctx_t;

static void *io_thread(void *arg) {
    io_ctx_t *ctx = arg;
    struct epoll_event events[256];
    while (g_running) {
        int n = epoll_wait(ctx->epfd, events, 256, 10);
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            uint8_t op;
            if (read(fd, &op, 1) != 1) goto cfd;
            switch (op) {
            case OP_ALLOC_CHANNEL: { int ch = create_channel(); write_full(fd, &ch, 4); break; }
            case OP_FILL: {
                uint64_t count; read_full(fd, &count, 8);
                uint8_t fbuf[TLC_VALUE_SIZE]; unsigned seed = 12345;
                for (uint64_t j = 0; j < count; j++) {
                    for (int k = 0; k < (int)(TLC_VALUE_SIZE/4); k++) ((uint32_t*)fbuf)[k] = rand_r(&seed);
                    int32_t widx = -1;
                    if (fc_cache_warm_put(&g_cache, j, fbuf, &widx) != 0)
                        fc_cache_cold_append(&g_cache.cold, j, fbuf);
                    if (widx >= 0) hot_put_fc(j, widx);
                }
                uint8_t ok = 0; write_full(fd, &ok, 1); write_full(fd, &count, 8); break;
            }
            case OP_CLEAR_COUNTERS: {
#ifdef ENABLE_COLLISION_STATS
                for (int i = 0; i <= HASH_MAX_PROBES; i++) {
                    atomic_store(&g_hot_get_probe[i], 0);
                    atomic_store(&g_hot_put_probe[i], 0);
                }
                for (int i = 0; i < 7; i++) atomic_store(&g_warm_get_probe[i], 0);
                atomic_store(&g_total_hot_get, 0);
                atomic_store(&g_total_warm_get, 0);
#endif
#ifdef ENABLE_COLLISION_STATS
                g_l0_hits = 0;
                g_l0_misses = 0;
#endif
                uint8_t ok = 0; write_full(fd, &ok, 1); break;
            }
            case OP_DUMP_COLLISION: {
#ifdef ENABLE_COLLISION_STATS
                uint32_t sid = HASH_STRATEGY;
                write_full(fd, &sid, 4);
                char sname[32];
                snprintf(sname, sizeof(sname), "%s+%s",
                         hash_strategy_name(), placement_strategy_name());
                write_full(fd, sname, 32);
                for (int i = 0; i <= HASH_MAX_PROBES; i++) {
                    uint64_t v = atomic_load(&g_hot_get_probe[i]);
                    write_full(fd, &v, 8);
                }
                for (int i = 0; i <= HASH_MAX_PROBES; i++) {
                    uint64_t v = atomic_load(&g_hot_put_probe[i]);
                    write_full(fd, &v, 8);
                }
                for (int i = 0; i < 7; i++) {
                    uint64_t v = atomic_load(&g_warm_get_probe[i]);
                    write_full(fd, &v, 8);
                }
                uint64_t th = atomic_load(&g_total_hot_get);
                uint64_t tw = atomic_load(&g_total_warm_get);
                write_full(fd, &th, 8);
                write_full(fd, &tw, 8);
                /* Utilization: HOT unique keys, WARM occupied entries */
                uint64_t hot_unique = 0;
                size_t hot_cap = g_cache.hot.capacity;
                uint64_t *keys = (uint64_t*)malloc(hot_cap * sizeof(uint64_t));
                if (keys) {
                    size_t n = 0;
                    for (size_t i = 0; i < hot_cap; i++)
                        if (g_cache.hot.table[i].warm_idx >= 0)
                            keys[n++] = g_cache.hot.table[i].key;
                    /* sort + dedup */
                    qsort(keys, n, sizeof(uint64_t), cmp_u64);
                    hot_unique = n > 0 ? 1 : 0;
                    for (size_t i = 1; i < n; i++)
                        if (keys[i] != keys[i-1]) hot_unique++;
                    free(keys);
                }
                write_full(fd, &hot_unique, 8);
                uint64_t warm_occ = (uint64_t)atomic_load(&g_cache.warm.count);
                if (warm_occ > g_cache.warm.capacity) warm_occ = g_cache.warm.capacity;
                write_full(fd, &warm_occ, 8);
                uint64_t l0_h = g_l0_hits;
                uint64_t l0_m = g_l0_misses;
                fprintf(stderr, "[DUMP] L0 hits=%lu misses=%lu\n", (unsigned long)l0_h, (unsigned long)l0_m);
                write_full(fd, &l0_h, 8);
                write_full(fd, &l0_m, 8);
#else
                uint32_t sid = HASH_STRATEGY;
                write_full(fd, &sid, 4);
                char sname[32];
                snprintf(sname, sizeof(sname), "%s+%s",
                         hash_strategy_name(), placement_strategy_name());
                write_full(fd, sname, 32);
                uint64_t zero = 0;
                for (int i = 0; i < (5 + 5 + 7 + 2 + 2 + 2); i++) write_full(fd, &zero, 8);
#endif
                break;
            }
            case OP_STATS: {
                uint64_t stats[6] = {
                    atomic_load(&g_total_ops), atomic_load(&g_combine_rounds),
                    atomic_load(&g_combine_total_keys), atomic_load(&g_direct_ops),
                    atomic_load(&g_batch_ops), atomic_load(&g_num_channels_fc)
                };
                uint8_t ok = 0; write_full(fd, &ok, 1); write_full(fd, stats, sizeof(stats)); break;
            }
            case OP_PING: { uint8_t ok = 0; write_full(fd, &ok, 1); break; }
            default: goto cfd;
            }
            continue;
cfd:        epoll_ctl(ctx->epfd, EPOLL_CTL_DEL, fd, NULL); close(fd);
        }
    }
    return NULL;
}

static void sig_handler(int sig) { (void)sig; g_running = 0; }

int main(void) {
    signal(SIGINT, sig_handler); signal(SIGTERM, sig_handler); signal(SIGPIPE, SIG_IGN);

    printf("=== TLC Hash FC Bench Server ===\n");
    printf("Strategy: %s (id=%d)  Probes: %d  Placement: %s  Eviction: %s\n",
        hash_strategy_name(), HASH_STRATEGY, HASH_MAX_PROBES,
        placement_strategy_name(), eviction_strategy_name());
    printf("L0 Cache: ENABLED  size=%d\n", L0_CACHE_SIZE);
#ifdef ENABLE_COLLISION_STATS
    printf("Collision stats: ON\n");
#else
    printf("Collision stats: OFF\n");
#endif
    printf("UDS: %s  SHM: tlc_hb_*\n\n", UDS_PATH);

    /* Init cache — standalone module, no SVE2/HA deps */
    if (fc_cache_init(&g_cache) != 0) { fprintf(stderr, "Cache init failed\n"); return 1; }

    printf("Cache initialized. HOT: %zu  WARM: %zu\n",
        g_cache.hot.capacity, g_cache.warm.capacity);

    for (int i = 0; i < MAX_SLOTS; i++)
        atomic_store_explicit(&g_slots[i].state, SLOT_EMPTY, memory_order_relaxed);

    /* UDS listener — same as v16 */
    unlink(UDS_PATH);
    int uds_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un ua = {.sun_family = AF_UNIX};
    strncpy(ua.sun_path, UDS_PATH, sizeof(ua.sun_path)-1);
    bind(uds_fd, (struct sockaddr*)&ua, sizeof(ua));
    listen(uds_fd, 4096);
    fcntl(uds_fd, F_SETFL, fcntl(uds_fd, F_GETFL, 0) | O_NONBLOCK);

    io_ctx_t ctx = {0, epoll_create1(0)};
    pthread_t io_pt;
    pthread_create(&io_pt, NULL, io_thread, &ctx);

    int aepfd = epoll_create1(0);
    struct epoll_event ev = {.events = EPOLLIN, .data.fd = uds_fd};
    epoll_ctl(aepfd, EPOLL_CTL_ADD, uds_fd, &ev);

    printf("Server ready.\n\n");

    while (g_running) {
        struct epoll_event aev[64];
        int n = epoll_wait(aepfd, aev, 64, 100);
        for (int i = 0; i < n; i++) {
            struct sockaddr_un ca; socklen_t cl = sizeof(ca);
            int cfd;
            while ((cfd = accept(uds_fd, (struct sockaddr*)&ca, &cl)) >= 0) {
                struct epoll_event cev = {.events=EPOLLIN, .data.fd=cfd};
                epoll_ctl(ctx.epfd, EPOLL_CTL_ADD, cfd, &cev);
            }
        }
    }

    printf("\n=== Flat Combining Stats ===\n");
    printf("  Total ops:      %lu\n", (unsigned long)atomic_load(&g_total_ops));
    printf("  Strategy:       %s\n", hash_strategy_name());
    printf("  Eviction:       %s\n", eviction_strategy_name());

    unlink(UDS_PATH);
    fc_cache_destroy(&g_cache);
    return 0;
}
