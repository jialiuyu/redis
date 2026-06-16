#include "tlc_core.h"
#include "vemb_v16_tlc.h"
#include "vemb_v16_remote_meta.h"
#include "cpu_relax.h"
#include "vemb_v16_log.h"
#include "zmalloc.h"
#include "macro.h"

#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define VEMB_V16_REMOTE_META_PUBLISH_QUEUE_CAP 1024u
#define VEMB_V16_TLC_LOG_LIMIT 32u

typedef struct vemb_v16_remote_meta_publish_event {
    vemb_v16_remote_meta_view_t *target_view;
    uint64_t key_hash;
    uint32_t key_len;
    uint32_t is_repair;
    char key[VEMB_V16_MAX_KEY_LEN];
    vemb_v16_vector_handle_t handle;
} vemb_v16_remote_meta_publish_event_t;

typedef struct vemb_v16_remote_meta_publish_slot {
    _Alignas(64) atomic_uint_fast64_t sequence;
    vemb_v16_remote_meta_publish_event_t event;
} vemb_v16_remote_meta_publish_slot_t;

struct vemb_v16_tlc_remote_meta_publisher {
    pthread_t thread;
    int thread_started;
    atomic_int stop;
    _Alignas(64) atomic_uint_fast64_t head;
    _Alignas(64) atomic_uint_fast64_t tail;
    atomic_uint_fast32_t active_consumers;
    uint32_t capacity;
    uint32_t mask;
    vemb_v16_remote_meta_publish_slot_t *slots;
    vemb_v16_tlc_t *tlc;
};

static atomic_uint_fast32_t remote_meta_stale_logs = ATOMIC_VAR_INIT(0);
static atomic_uint_fast32_t remote_meta_async_drop_logs = ATOMIC_VAR_INIT(0);
static atomic_uint_fast32_t remote_meta_publish_busy_logs = ATOMIC_VAR_INIT(0);
static atomic_uint_fast32_t remote_meta_publish_evict_logs = ATOMIC_VAR_INIT(0);
static atomic_uint_fast32_t remote_meta_set_conflict_logs = ATOMIC_VAR_INIT(0);

static int tlc_log_should(atomic_uint_fast32_t *counter) {
    uint32_t n = atomic_fetch_add_explicit(counter, 1,
                                          memory_order_relaxed);
    return n < VEMB_V16_TLC_LOG_LIMIT;
}

static uint64_t monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void tlc_queue_pause(void) {
    for (uint32_t i = 0; i < 64; i++)
        cpu_relax();
    sched_yield();
}

static uint32_t default_remote_meta_owner_resolver(uint64_t key_hash,
                                                   const char *key,
                                                   uint32_t key_len,
                                                   void *arg) {
    (void)key_hash;
    (void)key;
    (void)key_len;
    vemb_v16_remote_meta_view_t *view = arg;
    return view->header->owner_supernode_id;
}

static void make_handle(vemb_v16_tlc_t *tlc,
                        uint64_t key_hash,
                        const tlc_warm_location_t *location,
                        vemb_v16_vector_handle_t *handle) {
    (void)tlc;
    handle->region_id = location->region_id;
    handle->bytes = location->bytes;
    handle->local_slot = location->local_slot;
    handle->reserved0 = 0;
    handle->offset = location->offset;
    handle->key_hash = key_hash;
    handle->owner_generation = location->owner_generation;
}

static void tlc_counter_add(atomic_uint_fast64_t *counter, uint64_t value) {
    atomic_fetch_add_explicit(counter, value, memory_order_relaxed);
}

static uint64_t tlc_counter_load(atomic_uint_fast64_t *counter) {
    return atomic_load_explicit(counter, memory_order_relaxed);
}

static void tlc_init_counters(vemb_v16_tlc_t *tlc) {
    atomic_init(&tlc->ub_lookup_rpc_next_request_id, 1);
    atomic_init(&tlc->remote_meta_lookup_hit, 0);
    atomic_init(&tlc->remote_meta_lookup_miss, 0);
    atomic_init(&tlc->remote_meta_lookup_busy, 0);
    atomic_init(&tlc->remote_meta_lookup_way_probe, 0);
    atomic_init(&tlc->remote_meta_lookup_set_conflict, 0);
    atomic_init(&tlc->remote_meta_publish_async_enqueue, 0);
    atomic_init(&tlc->remote_meta_publish_async_drop, 0);
    atomic_init(&tlc->remote_meta_publish_async_coalesce, 0);
    atomic_init(&tlc->remote_meta_publish_ok, 0);
    atomic_init(&tlc->remote_meta_publish_busy, 0);
    atomic_init(&tlc->remote_meta_publish_insert, 0);
    atomic_init(&tlc->remote_meta_publish_update, 0);
    atomic_init(&tlc->remote_meta_publish_evict, 0);
    atomic_init(&tlc->remote_meta_publish_ns, 0);
    atomic_init(&tlc->ub_lookup_rpc_count, 0);
    atomic_init(&tlc->ub_lookup_rpc_ok, 0);
    atomic_init(&tlc->ub_lookup_rpc_not_found, 0);
    atomic_init(&tlc->ub_lookup_rpc_busy, 0);
    atomic_init(&tlc->ub_lookup_rpc_timeout, 0);
    atomic_init(&tlc->ub_lookup_rpc_error, 0);
    atomic_init(&tlc->ub_lookup_rpc_handle, 0);
    atomic_init(&tlc->ub_lookup_rpc_snapshot, 0);
    atomic_init(&tlc->ub_lookup_rpc_ns, 0);
    atomic_init(&tlc->remote_meta_repair_enqueue, 0);
    atomic_init(&tlc->remote_meta_repair_ok, 0);
    atomic_init(&tlc->remote_meta_repair_drop, 0);
}

static int publish_remote_meta_to_view(
    vemb_v16_tlc_t *tlc,
    vemb_v16_remote_meta_view_t *view,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    const vemb_v16_vector_handle_t *handle,
    int is_repair) {
    RETURN_IF(!tlc || !view || !key || !handle || key_len == 0 ||
              handle->bytes == 0,
              -1);
    vemb_v16_remote_meta_handle_t remote_handle = {
        .region_id = handle->region_id,
        .bytes = handle->bytes,
        .local_slot = handle->local_slot,
        .offset = handle->offset,
        .key_hash = key_hash,
        .owner_generation = handle->owner_generation,
    };
    vemb_v16_remote_meta_publish_result_t result = {0};
    uint64_t start = monotonic_ns();
    int rc = vemb_v16_remote_meta_publish_with_result(view,
                                                      key,
                                                      key_len,
                                                      key_hash,
                                                      &remote_handle,
                                                      &result);
    tlc_counter_add(&tlc->remote_meta_publish_ns, monotonic_ns() - start);
    if (rc == VEMB_V16_REMOTE_META_OK) {
        tlc_counter_add(&tlc->remote_meta_publish_ok, 1);
        if (is_repair)
            tlc_counter_add(&tlc->remote_meta_repair_ok, 1);
        if (result.action == VEMB_V16_REMOTE_META_PUBLISH_INSERT)
            tlc_counter_add(&tlc->remote_meta_publish_insert, 1);
        else if (result.action == VEMB_V16_REMOTE_META_PUBLISH_UPDATE)
            tlc_counter_add(&tlc->remote_meta_publish_update, 1);
        else if (result.action == VEMB_V16_REMOTE_META_PUBLISH_EVICT) {
            tlc_counter_add(&tlc->remote_meta_publish_evict, 1);
            if (tlc_log_should(&remote_meta_publish_evict_logs)) {
                serverLog(LL_NOTICE,
                          "vemb_v16 remote_meta publish evict: owner=%u key_hash=%llu set=%u way=%u repair=%d",
                          view->header->owner_supernode_id,
                          (unsigned long long)key_hash,
                          result.set_id,
                          result.way,
                          is_repair);
            }
        }
        return 0;
    }
    if (rc == VEMB_V16_REMOTE_META_BUSY) {
        tlc_counter_add(&tlc->remote_meta_publish_busy, 1);
        if (tlc_log_should(&remote_meta_publish_busy_logs)) {
            serverLog(LL_NOTICE,
                      "vemb_v16 remote_meta publish busy: owner=%u key_hash=%llu repair=%d",
                      view->header->owner_supernode_id,
                      (unsigned long long)key_hash,
                      is_repair);
        }
    }
    return -1;
}

static int remote_meta_publish_ring_init(
    vemb_v16_tlc_remote_meta_publisher_t *publisher,
    uint32_t capacity) {
    RETURN_IF(!publisher || capacity == 0 ||
              (capacity & (capacity - 1u)) != 0,
              -1);
    publisher->capacity = capacity;
    publisher->mask = capacity - 1u;
    atomic_init(&publisher->head, 0);
    atomic_init(&publisher->tail, 0);
    atomic_init(&publisher->active_consumers, 0);
    atomic_init(&publisher->stop, 0);
    if (posix_memalign((void **)&publisher->slots,
                       64,
                       sizeof(*publisher->slots) * capacity) != 0) {
        publisher->slots = NULL;
        return -1;
    }
    memset(publisher->slots, 0, sizeof(*publisher->slots) * capacity);
    for (uint32_t i = 0; i < capacity; i++)
        atomic_init(&publisher->slots[i].sequence, i);
    return 0;
}

static void remote_meta_publish_ring_destroy(
    vemb_v16_tlc_remote_meta_publisher_t *publisher) {
    if (!publisher)
        return;
    free(publisher->slots);
    publisher->slots = NULL;
}

static int remote_meta_publish_ring_offer(
    vemb_v16_tlc_remote_meta_publisher_t *publisher,
    const vemb_v16_remote_meta_publish_event_t *event) {
    uint64_t pos =
        atomic_load_explicit(&publisher->tail, memory_order_relaxed);
    for (;;) {
        vemb_v16_remote_meta_publish_slot_t *slot =
            &publisher->slots[pos & publisher->mask];
        uint64_t seq = atomic_load_explicit(&slot->sequence,
                                            memory_order_acquire);
        int64_t diff = (int64_t)seq - (int64_t)pos;
        if (diff == 0) {
            uint64_t desired = pos + 1;
            if (atomic_compare_exchange_weak_explicit(
                    &publisher->tail,
                    &pos,
                    desired,
                    memory_order_relaxed,
                    memory_order_relaxed)) {
                slot->event = *event;
                atomic_store_explicit(&slot->sequence,
                                      pos + 1,
                                      memory_order_release);
                return 0;
            }
        } else if (diff < 0) {
            return -1;
        } else {
            pos = atomic_load_explicit(&publisher->tail,
                                       memory_order_relaxed);
        }
    }
}

static int remote_meta_publish_ring_poll(
    vemb_v16_tlc_remote_meta_publisher_t *publisher,
    vemb_v16_remote_meta_publish_event_t *event) {
    uint64_t pos =
        atomic_load_explicit(&publisher->head, memory_order_relaxed);
    for (;;) {
        vemb_v16_remote_meta_publish_slot_t *slot =
            &publisher->slots[pos & publisher->mask];
        uint64_t seq = atomic_load_explicit(&slot->sequence,
                                            memory_order_acquire);
        int64_t diff = (int64_t)seq - (int64_t)(pos + 1);
        if (diff == 0) {
            uint64_t desired = pos + 1;
            if (atomic_compare_exchange_weak_explicit(
                    &publisher->head,
                    &pos,
                    desired,
                    memory_order_relaxed,
                    memory_order_relaxed)) {
                *event = slot->event;
                atomic_store_explicit(&slot->sequence,
                                      pos + publisher->capacity,
                                      memory_order_release);
                return 1;
            }
        } else if (diff < 0) {
            return 0;
        } else {
            pos = atomic_load_explicit(&publisher->head,
                                       memory_order_relaxed);
        }
    }
}

static uint64_t remote_meta_publish_ring_available(
    vemb_v16_tlc_remote_meta_publisher_t *publisher) {
    uint64_t head = atomic_load_explicit(&publisher->head,
                                         memory_order_acquire);
    uint64_t tail = atomic_load_explicit(&publisher->tail,
                                         memory_order_acquire);
    return tail - head;
}

static int remote_meta_publisher_drain_one(
    vemb_v16_tlc_remote_meta_publisher_t *publisher,
    vemb_v16_tlc_t *tlc) {
    vemb_v16_remote_meta_publish_event_t event;
    atomic_fetch_add_explicit(&publisher->active_consumers,
                              1,
                              memory_order_acq_rel);
    int got = remote_meta_publish_ring_poll(publisher, &event);
    if (got <= 0) {
        atomic_fetch_sub_explicit(&publisher->active_consumers,
                                  1,
                                  memory_order_acq_rel);
        return got;
    }

    (void)publish_remote_meta_to_view(tlc,
                                      event.target_view,
                                      event.key,
                                      event.key_len,
                                      event.key_hash,
                                      &event.handle,
                                      event.is_repair);
    atomic_fetch_sub_explicit(&publisher->active_consumers,
                              1,
                              memory_order_acq_rel);
    return 1;
}

static void *remote_meta_publisher_main(void *arg) {
    vemb_v16_tlc_remote_meta_publisher_t *publisher = arg;
    vemb_v16_tlc_t *tlc = publisher->tlc;
    for (;;) {
        int got = remote_meta_publisher_drain_one(publisher, tlc);
        if (got > 0)
            continue;
        if (atomic_load_explicit(&publisher->stop, memory_order_acquire))
            break;
        tlc_queue_pause();
    }
    while (remote_meta_publisher_drain_one(publisher, tlc) > 0) {
    }
    return NULL;
}

static int remote_meta_publisher_start(vemb_v16_tlc_t *tlc) {
    if (atomic_load_explicit(&tlc->remote_meta_publisher,
                             memory_order_acquire))
        return 0;
    vemb_v16_tlc_remote_meta_publisher_t *publisher =
        zcalloc(sizeof(*publisher));
    RETURN_IF(!publisher, -1);
    publisher->tlc = tlc;
    if (remote_meta_publish_ring_init(
            publisher,
            VEMB_V16_REMOTE_META_PUBLISH_QUEUE_CAP) != 0) {
        zfree(publisher);
        return -1;
    }
    if (pthread_create(&publisher->thread,
                       NULL,
                       remote_meta_publisher_main,
                       publisher) != 0) {
        remote_meta_publish_ring_destroy(publisher);
        zfree(publisher);
        return -1;
    }
    publisher->thread_started = 1;

    vemb_v16_tlc_remote_meta_publisher_t *expected = NULL;
    if (atomic_compare_exchange_strong_explicit(
            &tlc->remote_meta_publisher,
            &expected,
            publisher,
            memory_order_release,
            memory_order_acquire)) {
        return 0;
    }

    atomic_store_explicit(&publisher->stop, 1, memory_order_release);
    pthread_join(publisher->thread, NULL);
    remote_meta_publish_ring_destroy(publisher);
    zfree(publisher);
    return 0;
}

static void remote_meta_publisher_stop(vemb_v16_tlc_t *tlc) {
    vemb_v16_tlc_remote_meta_publisher_t *publisher =
        atomic_exchange_explicit(&tlc->remote_meta_publisher,
                                 NULL,
                                 memory_order_acq_rel);
    if (!publisher)
        return;
    atomic_store_explicit(&publisher->stop, 1, memory_order_release);
    if (publisher->thread_started)
        pthread_join(publisher->thread, NULL);
    remote_meta_publish_ring_destroy(publisher);
    zfree(publisher);
}

static int enqueue_remote_meta_publish(
    vemb_v16_tlc_t *tlc,
    vemb_v16_remote_meta_view_t *target_view,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    const vemb_v16_vector_handle_t *handle,
    int is_repair) {
    if (!tlc || !target_view || !key || !handle ||
        key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN ||
        handle->bytes == 0 ||
        remote_meta_publisher_start(tlc) != 0) {
        if (is_repair)
            tlc_counter_add(&tlc->remote_meta_repair_drop, 1);
        else
            tlc_counter_add(&tlc->remote_meta_publish_async_drop, 1);
        if (tlc_log_should(&remote_meta_async_drop_logs)) {
            serverLog(LL_NOTICE,
                      "vemb_v16 remote_meta async publish drop: key_hash=%llu repair=%d reason=invalid_or_start_failed",
                      (unsigned long long)key_hash,
                      is_repair);
        }
        return -1;
    }

    vemb_v16_tlc_remote_meta_publisher_t *publisher =
        atomic_load_explicit(&tlc->remote_meta_publisher,
                             memory_order_acquire);
    if (!publisher) {
        if (is_repair)
            tlc_counter_add(&tlc->remote_meta_repair_drop, 1);
        else
            tlc_counter_add(&tlc->remote_meta_publish_async_drop, 1);
        if (tlc_log_should(&remote_meta_async_drop_logs)) {
            serverLog(LL_NOTICE,
                      "vemb_v16 remote_meta async publish drop: key_hash=%llu repair=%d reason=publisher_missing",
                      (unsigned long long)key_hash,
                      is_repair);
        }
        return -1;
    }

    vemb_v16_remote_meta_publish_event_t event;
    memset(&event, 0, sizeof(event));
    event.target_view = target_view;
    event.key_hash = key_hash;
    event.key_len = key_len;
    event.is_repair = is_repair;
    memcpy(event.key, key, key_len);
    event.handle = *handle;
    if (remote_meta_publish_ring_offer(publisher, &event) != 0) {
        if (is_repair)
            tlc_counter_add(&tlc->remote_meta_repair_drop, 1);
        else
            tlc_counter_add(&tlc->remote_meta_publish_async_drop, 1);
        if (tlc_log_should(&remote_meta_async_drop_logs)) {
            serverLog(LL_NOTICE,
                      "vemb_v16 remote_meta async publish drop: key_hash=%llu repair=%d reason=queue_full",
                      (unsigned long long)key_hash,
                      is_repair);
        }
        return -1;
    }

    if (is_repair)
        tlc_counter_add(&tlc->remote_meta_repair_enqueue, 1);
    else
        tlc_counter_add(&tlc->remote_meta_publish_async_enqueue, 1);
    return 0;
}

int vemb_v16_tlc_create(vemb_v16_tlc_t **out,
                        uint32_t vector_dim,
                        uint32_t max_vectors,
                        const vemb_v16_tlc_warm_region_t *warm_regions,
                        uint32_t warm_region_count,
                        uint32_t local_region_weight) {
    RETURN_IF(!out || warm_region_count == 0 ||
        warm_region_count > TLC_CORE_MAX_WARM_REGIONS ||
        vector_dim == 0 || max_vectors == 0, -1);
    /* Region ids must stay unique because vector handles address data by region_id. */
    for (uint32_t i = 0; i < warm_region_count; i++) {
        for (uint32_t j = 0; j < i; j++) {
            if (warm_regions[j].region_id == warm_regions[i].region_id)
                return -1;
        }
    }

    vemb_v16_tlc_t *tlc = zcalloc(sizeof(vemb_v16_tlc_t));
    RETURN_IF(!tlc, -1);
    tlc->vector_dim = vector_dim;
    tlc->value_size = vector_dim * sizeof(float);
    tlc->max_vectors = max_vectors;
    tlc->region_id = warm_regions[0].region_id;
    tlc->backend_type = warm_regions[0].backend_type;
    tlc->warm_region_count = warm_region_count;
    tlc_init_counters(tlc);
    atomic_init(&tlc->remote_meta_publisher, NULL);
    tlc->warm_regions = zcalloc(sizeof(vemb_v16_tlc_warm_region_t) * warm_region_count);
    if (!tlc->warm_regions) {
        vemb_v16_tlc_destroy(tlc);
        return -1;
    }
    memcpy(tlc->warm_regions, warm_regions,
           sizeof(vemb_v16_tlc_warm_region_t) * warm_region_count);
    atomic_init(&tlc->sve_stats.lock_success, 0);
    atomic_init(&tlc->sve_stats.lock_failure, 0);

    tlc_core_warm_region_config_t core_regions[TLC_CORE_MAX_WARM_REGIONS];
    memset(core_regions, 0, sizeof(core_regions));
    for (uint32_t i = 0; i < warm_region_count; i++) {
        core_regions[i] = (tlc_core_warm_region_config_t){
            .region_id = warm_regions[i].region_id,
            .backend_type = warm_regions[i].backend_type,
            .is_local = warm_regions[i].is_local,
            .weight = warm_regions[i].weight,
            .value_size = warm_regions[i].value_size,
            .region_bytes = warm_regions[i].region_bytes,
            .mapped_addr = warm_regions[i].mapped_addr,
            .shared_allocator = warm_regions[i].shared_allocator,
        };
    }
    tlc_core_config_t core_config = {
        .value_size = tlc->value_size,
        .warm_capacity = max_vectors,
        .hot_capacity = TLC_CORE_DEFAULT_HOT_CAPACITY,
        .cold_max_segments = TLC_CORE_DEFAULT_COLD_MAX_SEGMENTS,
        .cold_segment_records = TLC_CORE_DEFAULT_COLD_SEGMENT_RECORDS,
        .warm_regions = core_regions,
        .warm_region_count = warm_region_count,
        .local_region_weight = local_region_weight,
    };
    if (bitmap_init(&tlc->bitmap, max_vectors) != 0 ||
        tlc_core_create(&tlc->core, &core_config) != 0) {
        vemb_v16_tlc_destroy(tlc);
        return -1;
    }

    *out = tlc;
    return 0;
}

void vemb_v16_tlc_destroy(vemb_v16_tlc_t *tlc) {
    if (!tlc)
        return;
    remote_meta_publisher_stop(tlc);
    bitmap_destroy(&tlc->bitmap);
    tlc_core_destroy(tlc->core);
    if (tlc->warm_regions) zfree(tlc->warm_regions);
    zfree(tlc);
}

int vemb_v16_tlc_get_handle(vemb_v16_tlc_t *tlc,
                            const char *key,
                            uint32_t key_len,
                            uint64_t key_hash,
                            vemb_v16_vector_handle_t *handle,
                            uint32_t *warm_slot) {
    tlc_warm_location_t location = {0};
    if (tlc_core_get_warm_location(tlc->core, key, key_len,
                                   key_hash, &location) != 0) {
        return -1;
    }
    if (warm_slot) *warm_slot = location.local_slot;
    make_handle(tlc, key_hash, &location, handle);
    return 0;
}

void vemb_v16_tlc_set_remote_meta_view(vemb_v16_tlc_t *tlc,
                                       vemb_v16_remote_meta_view_t *view,
                                       uint32_t retry_budget) {
    tlc->remote_meta_view = view;
    tlc->remote_meta_retry_budget = retry_budget;
    if (!tlc->owner_resolver) {
        tlc->owner_resolver = default_remote_meta_owner_resolver;
        tlc->owner_resolver_arg = view;
    }
    (void)vemb_v16_tlc_set_remote_meta_owner_view(
        tlc,
        view->header->owner_supernode_id,
        view);
}

int vemb_v16_tlc_set_remote_meta_owner_view(vemb_v16_tlc_t *tlc,
                                            uint32_t owner_id,
                                            vemb_v16_remote_meta_view_t *view) {
    for (uint32_t i = 0; i < tlc->remote_meta_view_count; i++) {
        if (tlc->remote_meta_views[i].owner_id == owner_id) {
            tlc->remote_meta_views[i].view = view;
            return 0;
        }
    }
    RETURN_IF(tlc->remote_meta_view_count >= VEMB_V16_TLC_MAX_REMOTE_META_VIEWS, -1);
    tlc->remote_meta_views[tlc->remote_meta_view_count++] =
        (vemb_v16_tlc_remote_meta_owner_view_t){
            .owner_id = owner_id,
            .view = view,
        };
    return 0;
}

void vemb_v16_tlc_set_owner_resolver(vemb_v16_tlc_t *tlc,
                                     vemb_v16_tlc_owner_resolver_fn resolver,
                                     void *arg) {
    tlc->owner_resolver = resolver;
    tlc->owner_resolver_arg = arg;
}

static vemb_v16_remote_meta_view_t *remote_meta_view_for_key(vemb_v16_tlc_t *tlc,
                                                             const char *key,
                                                             uint32_t key_len,
                                                             uint64_t key_hash) {
    RETURN_IF(tlc->remote_meta_view_count == 0, NULL);
    uint32_t owner_id = tlc->owner_resolver(key_hash,
                                            key,
                                            key_len,
                                            tlc->owner_resolver_arg);
    for (uint32_t i = 0; i < tlc->remote_meta_view_count; i++) {
        if (tlc->remote_meta_views[i].owner_id == owner_id)
            return tlc->remote_meta_views[i].view;
    }
    return NULL;
}

static vemb_v16_remote_meta_view_t *remote_meta_view_for_owner(vemb_v16_tlc_t *tlc,
                                                               uint32_t owner_id) {
    for (uint32_t i = 0; i < tlc->remote_meta_view_count; i++) {
        if (tlc->remote_meta_views[i].owner_id == owner_id)
            return tlc->remote_meta_views[i].view;
    }
    return NULL;
}

int vemb_v16_tlc_publish_remote_meta(vemb_v16_tlc_t *tlc,
                                     const char *key,
                                     uint32_t key_len,
                                     uint64_t key_hash,
                                     const vemb_v16_vector_handle_t *handle) {
    return publish_remote_meta_to_view(tlc,
                                       tlc->remote_meta_view,
                                       key,
                                       key_len,
                                       key_hash,
                                       handle,
                                       0);
}

int vemb_v16_tlc_publish_remote_meta_async(
    vemb_v16_tlc_t *tlc,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    const vemb_v16_vector_handle_t *handle) {
    return enqueue_remote_meta_publish(tlc,
                                       tlc ? tlc->remote_meta_view : NULL,
                                       key,
                                       key_len,
                                       key_hash,
                                       handle,
                                       0);
}

uint32_t vemb_v16_tlc_flush_remote_meta_publishes(vemb_v16_tlc_t *tlc,
                                                  uint32_t budget) {
    if (!tlc)
        return 0;
    vemb_v16_tlc_remote_meta_publisher_t *publisher =
        atomic_load_explicit(&tlc->remote_meta_publisher,
                             memory_order_acquire);
    if (!publisher)
        return 0;
    uint32_t done = 0;
    for (;;) {
        if (budget && done >= budget)
            break;
        int got = remote_meta_publisher_drain_one(publisher, tlc);
        if (got > 0) {
            done++;
            continue;
        }
        if (budget)
            break;
        if (remote_meta_publish_ring_available(publisher) == 0 &&
            atomic_load_explicit(&publisher->active_consumers,
                                 memory_order_acquire) == 0) {
            break;
        }
        tlc_queue_pause();
    }
    return done;
}

void vemb_v16_tlc_set_lookup_rpc(vemb_v16_tlc_t *tlc,
                                 vemb_v16_tlc_lookup_rpc_fn fn,
                                 void *arg) {
    tlc->lookup_rpc = fn;
    tlc->lookup_rpc_arg = arg;
}

int vemb_v16_tlc_lookup_rpc_local_handler(
    void *arg,
    const vemb_v16_ub_lookup_rpc_req_t *req,
    vemb_v16_ub_lookup_rpc_resp_t *resp) {
    vemb_v16_tlc_t *owner = arg;
    RETURN_IF(!owner || !req || !resp ||
              req->op != VEMB_V16_UB_LOOKUP_RPC_LOOKUP_HANDLE ||
              req->key_len == 0 ||
              req->key_len > VEMB_V16_MAX_KEY_LEN,
              -1);
    memset(resp, 0, sizeof(*resp));
    resp->request_id = req->request_id;
    resp->key_hash = req->key_hash;

    vemb_v16_vector_handle_t handle = {0};
    if (vemb_v16_tlc_get_handle(owner,
                                req->key,
                                req->key_len,
                                req->key_hash,
                                &handle,
                                NULL) != 0) {
        resp->status = VEMB_V16_UB_LOOKUP_RPC_NOT_FOUND;
        resp->kind = VEMB_V16_UB_LOOKUP_RPC_KIND_NONE;
        return 0;
    }
    resp->status = VEMB_V16_UB_LOOKUP_RPC_OK;
    resp->kind = VEMB_V16_UB_LOOKUP_RPC_KIND_HANDLE;
    resp->region_id = handle.region_id;
    resp->local_slot = handle.local_slot;
    resp->offset = handle.offset;
    resp->bytes = handle.bytes;
    resp->owner_generation = handle.owner_generation;
    return 0;
}

static int validate_remote_handle(vemb_v16_tlc_t *tlc,
                                  uint64_t key_hash,
                                  const vemb_v16_vector_handle_t *candidate) {
    tlc_warm_location_t location = {
        .region_id = candidate->region_id,
        .region_index = UINT32_MAX,
        .local_slot = candidate->local_slot,
        .bytes = candidate->bytes,
        .offset = candidate->offset,
        .owner_generation = candidate->owner_generation,
    };
    return tlc_core_validate_warm_location(tlc->core, key_hash, &location);
}

static int repair_remote_meta_async(vemb_v16_tlc_t *tlc,
                                    uint32_t owner_id,
                                    const char *key,
                                    uint32_t key_len,
                                    uint64_t key_hash,
                                    const vemb_v16_vector_handle_t *handle) {
    vemb_v16_remote_meta_view_t *view =
        remote_meta_view_for_owner(tlc, owner_id);
    return enqueue_remote_meta_publish(tlc,
                                       view,
                                       key,
                                       key_len,
                                       key_hash,
                                       handle,
                                       1);
}

static int lookup_vsim_key2_via_rpc(vemb_v16_tlc_t *tlc,
                                    uint32_t local_owner_id,
                                    uint32_t owner_id,
                                    const char *key2,
                                    uint32_t key2_len,
                                    uint64_t key2_hash,
                                    vemb_v16_vector_handle_t *handle,
                                    vemb_v16_tlc_lookup_source_t *source) {
    RETURN_IF(!tlc->lookup_rpc || owner_id == local_owner_id ||
              !key2 || key2_len == 0 || key2_len > VEMB_V16_MAX_KEY_LEN,
              -1);
    vemb_v16_ub_lookup_rpc_req_t req = {
        .request_id = atomic_fetch_add_explicit(
            &tlc->ub_lookup_rpc_next_request_id,
            1,
            memory_order_relaxed),
        .src_owner_id = local_owner_id,
        .dst_owner_id = owner_id,
        .op = VEMB_V16_UB_LOOKUP_RPC_LOOKUP_HANDLE,
        .flags = 0,
        .key_hash = key2_hash,
        .key_len = key2_len,
        .timeout_ns = 0,
    };
    memcpy(req.key, key2, key2_len);
    vemb_v16_ub_lookup_rpc_resp_t resp = {0};
    uint64_t rpc_start = monotonic_ns();
    tlc_counter_add(&tlc->ub_lookup_rpc_count, 1);
    int rc = tlc->lookup_rpc(tlc->lookup_rpc_arg, &req, &resp);
    tlc_counter_add(&tlc->ub_lookup_rpc_ns, monotonic_ns() - rpc_start);
    if (rc != 0) {
        tlc_counter_add(&tlc->ub_lookup_rpc_error, 1);
        return -1;
    }
    if (resp.status == VEMB_V16_UB_LOOKUP_RPC_NOT_FOUND) {
        tlc_counter_add(&tlc->ub_lookup_rpc_not_found, 1);
        return -1;
    }
    if (resp.status == VEMB_V16_UB_LOOKUP_RPC_BUSY) {
        tlc_counter_add(&tlc->ub_lookup_rpc_busy, 1);
        return -1;
    }
    if (resp.status == VEMB_V16_UB_LOOKUP_RPC_TIMEOUT) {
        tlc_counter_add(&tlc->ub_lookup_rpc_timeout, 1);
        return -1;
    }
    if (resp.status != VEMB_V16_UB_LOOKUP_RPC_OK ||
        resp.key_hash != key2_hash) {
        tlc_counter_add(&tlc->ub_lookup_rpc_error, 1);
        return -1;
    }
    tlc_counter_add(&tlc->ub_lookup_rpc_ok, 1);

    if (resp.kind == VEMB_V16_UB_LOOKUP_RPC_KIND_HANDLE) {
        tlc_counter_add(&tlc->ub_lookup_rpc_handle, 1);
        vemb_v16_vector_handle_t candidate = {
            .region_id = resp.region_id,
            .bytes = resp.bytes,
            .local_slot = resp.local_slot,
            .offset = resp.offset,
            .key_hash = key2_hash,
            .owner_generation = resp.owner_generation,
        };
        if (validate_remote_handle(tlc, key2_hash, &candidate) != 0) {
            tlc_core_note_remote_meta_stale(tlc->core);
            if (tlc_log_should(&remote_meta_stale_logs)) {
                serverLog(LL_NOTICE,
                          "vemb_v16 ub rpc handle stale: owner=%u key_hash=%llu region=%u slot=%u generation=%llu",
                          owner_id,
                          (unsigned long long)key2_hash,
                          candidate.region_id,
                          candidate.local_slot,
                          (unsigned long long)candidate.owner_generation);
            }
            return -1;
        }
        *handle = candidate;
        *source = VEMB_V16_TLC_LOOKUP_SOURCE_UB_RPC;
        (void)repair_remote_meta_async(tlc,
                                       owner_id,
                                       key2,
                                       key2_len,
                                       key2_hash,
                                       &candidate);
        return 0;
    }
    if (resp.kind == VEMB_V16_UB_LOOKUP_RPC_KIND_SNAPSHOT)
        tlc_counter_add(&tlc->ub_lookup_rpc_snapshot, 1);
    tlc_counter_add(&tlc->ub_lookup_rpc_error, 1);
    return -1;
}

int vemb_v16_tlc_lookup_vsim_key2(vemb_v16_tlc_t *tlc,
                                  const char *key2,
                                  uint32_t key2_len,
                                  uint64_t key2_hash,
                                  vemb_v16_vector_handle_t *handle,
                                  vemb_v16_tlc_lookup_source_t *source,
                                  vemb_v16_tlc_lookup_timing_t *timing) {
    *source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    memset(timing, 0, sizeof(*timing));

    int try_local = 1;
    uint32_t owner_id = UINT32_MAX;
    uint32_t local_owner_id = UINT32_MAX;
    vemb_v16_remote_meta_view_t *remote_meta = NULL;
    if (tlc->remote_meta_view_count > 0 &&
        tlc->owner_resolver &&
        tlc->remote_meta_view) {
        owner_id = tlc->owner_resolver(key2_hash,
                                       key2,
                                       key2_len,
                                       tlc->owner_resolver_arg);
        local_owner_id = tlc->remote_meta_view->header->owner_supernode_id;
        remote_meta = remote_meta_view_for_owner(tlc, owner_id);
        try_local = owner_id == local_owner_id;
    }

    uint64_t stage_start = monotonic_ns();
    if (try_local &&
        vemb_v16_tlc_get_handle(tlc,
                                key2,
                                key2_len,
                                key2_hash,
                                handle,
                                NULL) == 0) {
        timing->local_lookup_count = 1;
        timing->local_lookup_ns = monotonic_ns() - stage_start;
        *source = VEMB_V16_TLC_LOOKUP_SOURCE_LOCAL;
        return 0;
    }
    timing->local_lookup_count = 1;
    timing->local_lookup_ns = monotonic_ns() - stage_start;

    if (!remote_meta)
        remote_meta = remote_meta_view_for_key(tlc, key2, key2_len, key2_hash);
    if (remote_meta) {
        vemb_v16_remote_meta_handle_t remote_handle = {0};
        vemb_v16_remote_meta_lookup_result_t lookup_result = {0};
        stage_start = monotonic_ns();
        int remote_rc = vemb_v16_remote_meta_lookup_with_result(
            remote_meta,
            key2,
            key2_len,
            key2_hash,
            tlc->remote_meta_retry_budget,
            &remote_handle,
            &lookup_result);
        timing->remote_meta_lookup_count = 1;
        timing->remote_meta_lookup_ns = monotonic_ns() - stage_start;
        tlc_counter_add(&tlc->remote_meta_lookup_way_probe,
                        lookup_result.probes);
        if (remote_rc == VEMB_V16_REMOTE_META_OK) {
            tlc_counter_add(&tlc->remote_meta_lookup_hit, 1);
            vemb_v16_vector_handle_t candidate = {
                .region_id = remote_handle.region_id,
                .bytes = remote_handle.bytes,
                .local_slot = remote_handle.local_slot,
                .offset = remote_handle.offset,
                .key_hash = key2_hash,
                .owner_generation = remote_handle.owner_generation,
            };
            if (validate_remote_handle(tlc, key2_hash, &candidate) != 0) {
                tlc_core_note_remote_meta_stale(tlc->core);
                if (tlc_log_should(&remote_meta_stale_logs)) {
                    serverLog(LL_NOTICE,
                              "vemb_v16 remote_meta stale: owner=%u key_hash=%llu region=%u slot=%u generation=%llu",
                              owner_id,
                              (unsigned long long)key2_hash,
                              candidate.region_id,
                              candidate.local_slot,
                              (unsigned long long)candidate.owner_generation);
                }
                if (lookup_vsim_key2_via_rpc(tlc,
                                             local_owner_id,
                                             owner_id,
                                             key2,
                                             key2_len,
                                             key2_hash,
                                             handle,
                                             source) == 0) {
                    return 0;
                }
                return -1;
            }
            *handle = candidate;
            *source = VEMB_V16_TLC_LOOKUP_SOURCE_REMOTE;
            return 0;
        } else if (remote_rc == VEMB_V16_REMOTE_META_BUSY) {
            tlc_counter_add(&tlc->remote_meta_lookup_busy, 1);
        } else {
            tlc_counter_add(&tlc->remote_meta_lookup_miss, 1);
            if (remote_meta->header->ways &&
                lookup_result.probes >= remote_meta->header->ways) {
                tlc_counter_add(&tlc->remote_meta_lookup_set_conflict, 1);
                if (tlc_log_should(&remote_meta_set_conflict_logs)) {
                    serverLog(LL_NOTICE,
                              "vemb_v16 remote_meta set conflict miss: owner=%u key_hash=%llu set=%u probes=%u ways=%u",
                              remote_meta->header->owner_supernode_id,
                              (unsigned long long)key2_hash,
                              lookup_result.set_id,
                              lookup_result.probes,
                              remote_meta->header->ways);
                }
            }
        }
    }

    if (lookup_vsim_key2_via_rpc(tlc,
                                 local_owner_id,
                                 owner_id,
                                 key2,
                                 key2_len,
                                 key2_hash,
                                 handle,
                                 source) == 0) {
        return 0;
    }
    return -1;
}

int vemb_v16_tlc_put(vemb_v16_tlc_t *tlc,
                     const char *key,
                     uint32_t key_len,
                     uint64_t key_hash,
                     const float *vector,
                     uint32_t vector_bytes,
                     vemb_v16_vector_handle_t *handle,
                     uint32_t *warm_slot) {
    tlc_warm_location_t location = {0};
    if (tlc_core_put_location(tlc->core, key, key_len, key_hash,
                              vector, vector_bytes, &location) != 0) {
        return -1;
    }
    *warm_slot = location.local_slot;
    if (location.local_slot == TLC_CORE_INVALID_SLOT ||
        location.region_id == TLC_CORE_INVALID_REGION_ID) {
        memset(handle, 0, sizeof(*handle));
        return 0;
    }
    make_handle(tlc, key_hash, &location, handle);
    return 0;
}

int vemb_v16_tlc_cold_append(vemb_v16_tlc_t *tlc,
                             const char *key,
                             uint32_t key_len,
                             uint64_t key_hash,
                             const float *vector,
                             uint32_t vector_bytes) {
    return tlc_core_cold_append(tlc->core, key, key_len, key_hash,
                                vector, vector_bytes);
}

const vemb_v16_tlc_warm_region_t *vemb_v16_tlc_find_region(
    const vemb_v16_tlc_t *tlc, uint32_t region_id) {
    for (uint32_t i = 0; i < tlc->warm_region_count; i++) {
        if (tlc->warm_regions[i].region_id == region_id)
            return &tlc->warm_regions[i];
    }
    return NULL;
}

int vemb_v16_tlc_vector_slice(const vemb_v16_tlc_t *tlc,
                              const vemb_v16_vector_handle_t *handle,
                              const uint8_t **vector,
                              uint32_t *vector_bytes) {
    *vector = NULL;
    *vector_bytes = 0;
    const vemb_v16_tlc_warm_region_t *region =
        vemb_v16_tlc_find_region(tlc, handle->region_id);
    if (!region || !region->mapped_addr ||
        handle->local_slot == TLC_CORE_INVALID_SLOT ||
        handle->offset > region->region_bytes ||
        handle->bytes > region->region_bytes - handle->offset) {
        return -1;
    }
    tlc_warm_location_t location = {
        .region_id = handle->region_id,
        .region_index = UINT32_MAX,
        .local_slot = handle->local_slot,
        .bytes = handle->bytes,
        .offset = handle->offset,
        .owner_generation = handle->owner_generation,
    };
    if (tlc_core_validate_warm_location(tlc->core,
                                        handle->key_hash,
                                        &location) != 0) {
        return -1;
    }
    *vector = (const uint8_t *)region->mapped_addr + handle->offset;
    *vector_bytes = handle->bytes;
    return 0;
}

void vemb_v16_tlc_get_core_stats(vemb_v16_tlc_t *tlc,
                                 tlc_core_stats_t *stats) {
    tlc_core_get_stats(tlc->core, stats);
}

void vemb_v16_tlc_get_runtime_stats(vemb_v16_tlc_t *tlc,
                                    vemb_v16_stats_t *stats) {
    stats->remote_meta_lookup_hit =
        tlc_counter_load(&tlc->remote_meta_lookup_hit);
    stats->remote_meta_lookup_miss =
        tlc_counter_load(&tlc->remote_meta_lookup_miss);
    stats->remote_meta_lookup_busy =
        tlc_counter_load(&tlc->remote_meta_lookup_busy);
    stats->remote_meta_lookup_way_probe =
        tlc_counter_load(&tlc->remote_meta_lookup_way_probe);
    stats->remote_meta_lookup_set_conflict =
        tlc_counter_load(&tlc->remote_meta_lookup_set_conflict);
    stats->remote_meta_publish_async_enqueue =
        tlc_counter_load(&tlc->remote_meta_publish_async_enqueue);
    stats->remote_meta_publish_async_drop =
        tlc_counter_load(&tlc->remote_meta_publish_async_drop);
    stats->remote_meta_publish_async_coalesce =
        tlc_counter_load(&tlc->remote_meta_publish_async_coalesce);
    stats->remote_meta_publish_ok =
        tlc_counter_load(&tlc->remote_meta_publish_ok);
    stats->remote_meta_publish_busy =
        tlc_counter_load(&tlc->remote_meta_publish_busy);
    stats->remote_meta_publish_insert =
        tlc_counter_load(&tlc->remote_meta_publish_insert);
    stats->remote_meta_publish_update =
        tlc_counter_load(&tlc->remote_meta_publish_update);
    stats->remote_meta_publish_evict =
        tlc_counter_load(&tlc->remote_meta_publish_evict);
    stats->remote_meta_publish_ns =
        tlc_counter_load(&tlc->remote_meta_publish_ns);
    stats->ub_lookup_rpc_count =
        tlc_counter_load(&tlc->ub_lookup_rpc_count);
    stats->ub_lookup_rpc_ok =
        tlc_counter_load(&tlc->ub_lookup_rpc_ok);
    stats->ub_lookup_rpc_not_found =
        tlc_counter_load(&tlc->ub_lookup_rpc_not_found);
    stats->ub_lookup_rpc_busy =
        tlc_counter_load(&tlc->ub_lookup_rpc_busy);
    stats->ub_lookup_rpc_timeout =
        tlc_counter_load(&tlc->ub_lookup_rpc_timeout);
    stats->ub_lookup_rpc_error =
        tlc_counter_load(&tlc->ub_lookup_rpc_error);
    stats->ub_lookup_rpc_handle =
        tlc_counter_load(&tlc->ub_lookup_rpc_handle);
    stats->ub_lookup_rpc_snapshot =
        tlc_counter_load(&tlc->ub_lookup_rpc_snapshot);
    stats->ub_lookup_rpc_ns =
        tlc_counter_load(&tlc->ub_lookup_rpc_ns);
    stats->remote_meta_repair_enqueue =
        tlc_counter_load(&tlc->remote_meta_repair_enqueue);
    stats->remote_meta_repair_ok =
        tlc_counter_load(&tlc->remote_meta_repair_ok);
    stats->remote_meta_repair_drop =
        tlc_counter_load(&tlc->remote_meta_repair_drop);
}

uint32_t vemb_v16_tlc_get_region_stats(vemb_v16_tlc_t *tlc,
                                       tlc_core_region_stats_t *regions,
                                       uint32_t max_regions) {
    return tlc_core_get_region_stats(tlc->core, regions, max_regions);
}
