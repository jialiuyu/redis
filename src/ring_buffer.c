#include "ring_buffer.h"
#include "macro.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

typedef struct ring_buffer_registry_entry {
    char *shm_name;
    char *logical_name;
    ring_buffer_t *rb;
    struct ring_buffer_registry_entry *next;
} ring_buffer_registry_entry_t;

static ring_buffer_registry_entry_t *g_ring_buffer_registry = NULL;
static pthread_mutex_t g_ring_buffer_registry_lock = PTHREAD_MUTEX_INITIALIZER;

static uint64_t ring_buffer_name_hash(const char *name) {
    uint64_t hash = 1469598103934665603ull;
    for (const unsigned char *p = (const unsigned char *)name; p && *p; ++p) {
        hash ^= *p;
        hash *= 1099511628211ull;
    }
    return hash;
}

ring_buffer_t *ring_buffer_create(size_t size, const char *name) {
    ring_buffer_t *rb = zmalloc(sizeof(ring_buffer_t));
    RETURN_IF(!rb, NULL);

    char shm_name[256];
    snprintf(shm_name, sizeof(shm_name), "/rub_%016llx",
             (unsigned long long)ring_buffer_name_hash(name));

    pthread_mutex_lock(&g_ring_buffer_registry_lock);
    for (ring_buffer_registry_entry_t *entry = g_ring_buffer_registry; entry; entry = entry->next) {
        if (strcmp(entry->shm_name, shm_name) == 0) {
            if (strcmp(entry->logical_name, name) != 0) {
                pthread_mutex_unlock(&g_ring_buffer_registry_lock);
                serverLog(LL_WARNING,
                          "Ring buffer name collision: logical names '%s' and '%s' map to the same shm name %s",
                          entry->logical_name, name, shm_name);
                zfree(rb);
                return NULL;
            }
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

    if (ftruncate(rb->fd, size) < 0) {
        serverLog(LL_WARNING, "Failed to set shared memory size: %s", strerror(errno));
        close(rb->fd);
        shm_unlink(shm_name);
        zfree(rb);
        return NULL;
    }

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
    entry->shm_name = zstrdup(shm_name);
    if (!entry->shm_name) {
        zfree(entry);
        munmap(rb->buffer, rb->size);
        close(rb->fd);
        zfree(rb);
        return NULL;
    }
    entry->logical_name = zstrdup(name);
    if (!entry->logical_name) {
        zfree(entry->shm_name);
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

void ring_buffer_destroy(ring_buffer_t *rb) {
    RETURN_IF(!rb);

    if (atomic_fetch_sub_explicit(&rb->refcount, 1, memory_order_acq_rel) != 1) {
        return;
    }

    pthread_mutex_lock(&g_ring_buffer_registry_lock);
    ring_buffer_registry_entry_t **prev = &g_ring_buffer_registry;
    ring_buffer_registry_entry_t *entry = g_ring_buffer_registry;
    while (entry) {
        if (entry->rb == rb) {
            *prev = entry->next;
            zfree(entry->shm_name);
            zfree(entry->logical_name);
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
    RETURN_IF(!rb);
    atomic_fetch_add_explicit(&rb->refcount, 1, memory_order_relaxed);
}

size_t ring_buffer_available_space(ring_buffer_t *rb) {
    uint64_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    uint64_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);

    if (tail >= head) {
        return rb->size - (tail - head) - 1;
    } else {
        return head - tail - 1;
    }
}

size_t ring_buffer_available_data(ring_buffer_t *rb) {
    uint64_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);

    if (tail >= head) {
        return tail - head;
    } else {
        return rb->size - (head - tail);
    }
}

int ring_buffer_push(ring_buffer_t *rb, const void *data, size_t len) {
    RETURN_IF(!rb || !data || len == 0, C_ERR);
    void *payload = NULL;
    RETURN_IF(ring_buffer_reserve(rb, len, &payload) != C_OK, C_ERR);
    memcpy(payload, data, len);
    return ring_buffer_commit_write(rb, len);
}

int ring_buffer_reserve(ring_buffer_t *rb, size_t payload_len, void **payload) {
    RETURN_IF(!rb || !payload || payload_len == 0, C_ERR);
    RETURN_IF(rb->reservation_active, C_ERR);

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
            return C_ERR;
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
    RETURN_IF(!rb || payload_len == 0, C_ERR);
    RETURN_IF(!rb->reservation_active || rb->reserved_payload_len != payload_len, C_ERR);

    atomic_store_explicit(&rb->tail, rb->reserved_commit_tail, memory_order_release);
    rb->reservation_active = 0;
    rb->reserved_payload_len = 0;
    rb->reserved_commit_tail = 0;
    return C_OK;
}

int ring_buffer_cancel_write(ring_buffer_t *rb) {
    RETURN_IF(!rb || !rb->reservation_active, C_ERR);

    rb->reservation_active = 0;
    rb->reserved_payload_len = 0;
    rb->reserved_commit_tail = 0;
    return C_OK;
}

int ring_buffer_pop(ring_buffer_t *rb, void *data, size_t max_len, size_t *actual_len) {
    RETURN_IF(!rb || !data, C_ERR);
    void *payload = NULL;
    size_t payload_len = 0;
    RETURN_IF(ring_buffer_peek(rb, &payload, &payload_len) != C_OK, C_ERR);
    RETURN_IF(payload_len > max_len, C_ERR);
    memcpy(data, payload, payload_len);
    RETURN_IF(ring_buffer_commit_read(rb, payload_len) != C_OK, C_ERR);
    if (actual_len) *actual_len = payload_len;
    return C_OK;
}

int ring_buffer_peek(ring_buffer_t *rb, void **payload, size_t *payload_len) {
    RETURN_IF(!rb || !payload || !payload_len, C_ERR);

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
    RETURN_IF(!rb || payload_len == 0, C_ERR);

    uint64_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    atomic_store_explicit(&rb->head, head + sizeof(uint32_t) + payload_len, memory_order_release);
    return C_OK;
}
