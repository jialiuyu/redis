#include "../src/vemb_v16_shared_allocator.h"

#include <assert.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

typedef struct alloc_thread_arg {
    vemb_v16_shared_region_allocator_t *allocator;
    uint8_t *seen;
    pthread_mutex_t *lock;
    uint32_t iterations;
} alloc_thread_arg_t;

static void *alloc_worker(void *arg) {
    alloc_thread_arg_t *a = arg;
    for (uint32_t i = 0; i < a->iterations; i++) {
        uint32_t slot = UINT32_MAX;
        assert(vemb_v16_shared_allocator_alloc(a->allocator, &slot) ==
               VEMB_V16_SHARED_ALLOCATOR_OK);
        pthread_mutex_lock(a->lock);
        assert(a->seen[slot] == 0);
        a->seen[slot] = 1;
        pthread_mutex_unlock(a->lock);
    }
    return NULL;
}

static void test_multithread_unique_slots(void) {
    enum { threads = 8, per_thread = 64, capacity = threads * per_thread };
    char name[128];
    snprintf(name, sizeof(name), "/vemb_v16_alloc_ut_%ld_unique",
             (long)getpid());
    vemb_v16_shared_allocator_unlink(name);

    vemb_v16_shared_allocator_mapping_t mapping;
    assert(vemb_v16_shared_allocator_open(&mapping,
                                          VEMB_V16_REGION_LOCAL_SHM,
                                          name,
                                          0,
                                          7,
                                          capacity,
                                          1) == 0);

    uint8_t *seen = calloc(capacity, sizeof(*seen));
    pthread_mutex_t lock;
    pthread_mutex_init(&lock, NULL);
    pthread_t tids[threads];
    alloc_thread_arg_t args[threads];
    assert(seen != NULL);
    for (uint32_t i = 0; i < threads; i++) {
        args[i] = (alloc_thread_arg_t){
            .allocator = mapping.allocator,
            .seen = seen,
            .lock = &lock,
            .iterations = per_thread,
        };
        assert(pthread_create(&tids[i], NULL, alloc_worker, &args[i]) == 0);
    }
    for (uint32_t i = 0; i < threads; i++)
        assert(pthread_join(tids[i], NULL) == 0);
    for (uint32_t i = 0; i < capacity; i++)
        assert(seen[i] == 1);
    assert(vemb_v16_shared_allocator_used_slots(mapping.allocator) == capacity);
    uint32_t slot = UINT32_MAX;
    assert(vemb_v16_shared_allocator_alloc(mapping.allocator, &slot) ==
           VEMB_V16_SHARED_ALLOCATOR_FULL);
    assert(vemb_v16_shared_allocator_full(mapping.allocator) == 1);

    pthread_mutex_destroy(&lock);
    free(seen);
    vemb_v16_shared_allocator_close(&mapping);
    vemb_v16_shared_allocator_unlink(name);
}

static void test_reopen_does_not_reset_next_slot(void) {
    char name[128];
    snprintf(name, sizeof(name), "/vemb_v16_alloc_ut_%ld_reopen",
             (long)getpid());
    vemb_v16_shared_allocator_unlink(name);

    vemb_v16_shared_allocator_mapping_t first;
    assert(vemb_v16_shared_allocator_open(&first,
                                          VEMB_V16_REGION_LOCAL_SHM,
                                          name,
                                          0,
                                          9,
                                          4,
                                          1) == 0);
    uint32_t slot = UINT32_MAX;
    assert(vemb_v16_shared_allocator_alloc(first.allocator, &slot) ==
           VEMB_V16_SHARED_ALLOCATOR_OK);
    assert(slot == 0);

    vemb_v16_shared_allocator_mapping_t second;
    assert(vemb_v16_shared_allocator_open(&second,
                                          VEMB_V16_REGION_LOCAL_SHM,
                                          name,
                                          0,
                                          9,
                                          4,
                                          1) == 0);
    slot = UINT32_MAX;
    assert(vemb_v16_shared_allocator_alloc(second.allocator, &slot) ==
           VEMB_V16_SHARED_ALLOCATOR_OK);
    assert(slot == 1);
    assert(vemb_v16_shared_allocator_used_slots(second.allocator) == 2);

    vemb_v16_shared_allocator_close(&second);
    vemb_v16_shared_allocator_close(&first);
    vemb_v16_shared_allocator_unlink(name);
}

static int read_full(int fd, void *buf, size_t len) {
    uint8_t *p = buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n <= 0)
            return -1;
        p += (size_t)n;
        len -= (size_t)n;
    }
    return 0;
}

static int write_full(int fd, const void *buf, size_t len) {
    const uint8_t *p = buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n <= 0)
            return -1;
        p += (size_t)n;
        len -= (size_t)n;
    }
    return 0;
}

static uint32_t parse_env_u32(const char *name, uint32_t fallback) {
    const char *value = getenv(name);
    if (!value || !value[0])
        return fallback;
    char *end = NULL;
    unsigned long v = strtoul(value, &end, 10);
    assert(end != value && *end == '\0' && v <= UINT32_MAX);
    return (uint32_t)v;
}

static uint64_t parse_env_u64(const char *name, uint64_t fallback) {
    const char *value = getenv(name);
    if (!value || !value[0])
        return fallback;
    char *end = NULL;
    unsigned long long v = strtoull(value, &end, 10);
    assert(end != value && *end == '\0');
    return (uint64_t)v;
}

static void run_multiprocess_unique_slots(uint32_t backend_type,
                                          const char *path,
                                          uint64_t mmap_offset,
                                          uint32_t region_id,
                                          uint32_t children,
                                          uint32_t per_child,
                                          int reset_first,
                                          int unlink_after) {
    assert(children > 0 && per_child > 0);
    assert(children <= 64 && per_child <= 4096);
    assert(children <= UINT32_MAX / per_child);
    uint32_t capacity = children * per_child;
    int (*pipes)[2] = calloc(children, sizeof(*pipes));
    pid_t *pids = calloc(children, sizeof(*pids));
    assert(pipes != NULL && pids != NULL);

    if (reset_first) {
        if (backend_type == VEMB_V16_REGION_LOCAL_SHM) {
            vemb_v16_shared_allocator_unlink(path);
            vemb_v16_shared_allocator_mapping_t initial;
            assert(vemb_v16_shared_allocator_open(&initial,
                                                  backend_type,
                                                  path,
                                                  mmap_offset,
                                                  region_id,
                                                  capacity,
                                                  1) == 0);
            vemb_v16_shared_allocator_close(&initial);
        } else {
            assert(vemb_v16_shared_allocator_reset(backend_type,
                                                   path,
                                                   mmap_offset,
                                                   region_id,
                                                   capacity,
                                                   1) == 0);
        }
    }

    for (uint32_t i = 0; i < children; i++) {
        assert(pipe(pipes[i]) == 0);
        pids[i] = fork();
        assert(pids[i] >= 0);
        if (pids[i] == 0) {
            close(pipes[i][0]);
            vemb_v16_shared_allocator_mapping_t mapping;
            if (vemb_v16_shared_allocator_open(&mapping,
                                               backend_type,
                                               path,
                                               mmap_offset,
                                               region_id,
                                               capacity,
                                               1) != 0) {
                _exit(2);
            }
            uint32_t *slots = calloc(per_child, sizeof(*slots));
            if (!slots)
                _exit(5);
            for (uint32_t j = 0; j < per_child; j++) {
                if (vemb_v16_shared_allocator_alloc(mapping.allocator,
                                                    &slots[j]) !=
                    VEMB_V16_SHARED_ALLOCATOR_OK) {
                    vemb_v16_shared_allocator_close(&mapping);
                    _exit(3);
                }
            }
            vemb_v16_shared_allocator_close(&mapping);
            if (write_full(pipes[i][1],
                           slots,
                           (size_t)per_child * sizeof(*slots)) != 0)
                _exit(4);
            close(pipes[i][1]);
            _exit(0);
        }
        close(pipes[i][1]);
    }

    uint8_t *seen = calloc(capacity, sizeof(*seen));
    assert(seen != NULL);
    for (uint32_t i = 0; i < children; i++) {
        uint32_t slots[per_child];
        assert(read_full(pipes[i][0], slots, sizeof(slots)) == 0);
        close(pipes[i][0]);
        for (uint32_t j = 0; j < per_child; j++) {
            assert(slots[j] < capacity);
            assert(seen[slots[j]] == 0);
            seen[slots[j]] = 1;
        }
    }
    for (uint32_t i = 0; i < children; i++) {
        int status = 0;
        assert(waitpid(pids[i], &status, 0) == pids[i]);
        assert(WIFEXITED(status));
        assert(WEXITSTATUS(status) == 0);
    }
    for (uint32_t i = 0; i < capacity; i++)
        assert(seen[i] == 1);

    vemb_v16_shared_allocator_mapping_t final;
    assert(vemb_v16_shared_allocator_open(&final,
                                          backend_type,
                                          path,
                                          mmap_offset,
                                          region_id,
                                          capacity,
                                          1) == 0);
    assert(vemb_v16_shared_allocator_used_slots(final.allocator) == capacity);
    uint32_t slot = UINT32_MAX;
    assert(vemb_v16_shared_allocator_alloc(final.allocator, &slot) ==
           VEMB_V16_SHARED_ALLOCATOR_FULL);
    vemb_v16_shared_allocator_close(&final);
    free(seen);
    if (unlink_after && backend_type == VEMB_V16_REGION_LOCAL_SHM)
        vemb_v16_shared_allocator_unlink(path);
    free(pids);
    free(pipes);
}

static void test_multiprocess_shm_unique_slots(void) {
    char name[128];
    snprintf(name, sizeof(name), "/vemb_v16_alloc_ut_%ld_proc",
             (long)getpid());
    run_multiprocess_unique_slots(VEMB_V16_REGION_LOCAL_SHM,
                                  name,
                                  0,
                                  13,
                                  8,
                                  128,
                                  1,
                                  1);
}

static void test_real_ub_multiprocess_unique_slots_if_configured(void) {
    const char *path = getenv("VEMB_V16_UB_ALLOCATOR_PATH");
    if (!path || !path[0]) {
        printf("vemb_v16 real UB allocator UT skipped: set VEMB_V16_UB_ALLOCATOR_PATH to enable\n");
        return;
    }
    uint64_t mmap_offset =
        parse_env_u64("VEMB_V16_UB_ALLOCATOR_OFFSET", 0);
    uint32_t children =
        parse_env_u32("VEMB_V16_UB_ALLOCATOR_CHILDREN", 8);
    uint32_t per_child =
        parse_env_u32("VEMB_V16_UB_ALLOCATOR_PER_CHILD", 128);
    uint32_t region_id =
        parse_env_u32("VEMB_V16_UB_ALLOCATOR_REGION_ID", 9001);

    printf("vemb_v16 real UB allocator UT: path=%s offset=%llu region_id=%u children=%u per_child=%u\n",
           path,
           (unsigned long long)mmap_offset,
           region_id,
           children,
           per_child);
    run_multiprocess_unique_slots(VEMB_V16_REGION_UB,
                                  path,
                                  mmap_offset,
                                  region_id,
                                  children,
                                  per_child,
                                  1,
                                  0);
}

static void test_ub_provider_reopen_and_reset(void) {
    char path[128];
    snprintf(path, sizeof(path), "/tmp/vemb_v16_alloc_ut_%ld_ub",
             (long)getpid());
    unlink(path);
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0666);
    assert(fd >= 0);
    assert(ftruncate(fd, 4096) == 0);
    close(fd);

    vemb_v16_shared_allocator_mapping_t misaligned;
    assert(vemb_v16_shared_allocator_open(&misaligned,
                                          VEMB_V16_REGION_UB,
                                          path,
                                          1,
                                          11,
                                          3,
                                          1) != 0);

    vemb_v16_shared_allocator_mapping_t first;
    assert(vemb_v16_shared_allocator_open(&first,
                                          VEMB_V16_REGION_UB,
                                          path,
                                          128,
                                          11,
                                          3,
                                          1) == 0);
    uint32_t slot = UINT32_MAX;
    assert(vemb_v16_shared_allocator_alloc(first.allocator, &slot) ==
           VEMB_V16_SHARED_ALLOCATOR_OK);
    assert(slot == 0);
    vemb_v16_shared_allocator_close(&first);

    vemb_v16_shared_allocator_mapping_t second;
    assert(vemb_v16_shared_allocator_open(&second,
                                          VEMB_V16_REGION_UB,
                                          path,
                                          128,
                                          11,
                                          3,
                                          1) == 0);
    slot = UINT32_MAX;
    assert(vemb_v16_shared_allocator_alloc(second.allocator, &slot) ==
           VEMB_V16_SHARED_ALLOCATOR_OK);
    assert(slot == 1);
    vemb_v16_shared_allocator_close(&second);

    assert(vemb_v16_shared_allocator_reset(VEMB_V16_REGION_UB,
                                           path,
                                           128,
                                           11,
                                           3,
                                           1) == 0);
    assert(vemb_v16_shared_allocator_open(&second,
                                          VEMB_V16_REGION_UB,
                                          path,
                                          128,
                                          11,
                                          3,
                                          1) == 0);
    slot = UINT32_MAX;
    assert(vemb_v16_shared_allocator_alloc(second.allocator, &slot) ==
           VEMB_V16_SHARED_ALLOCATOR_OK);
    assert(slot == 0);
    vemb_v16_shared_allocator_close(&second);
    unlink(path);
}

static void test_name_derivation(void) {
    char name[128];
    assert(vemb_v16_shared_allocator_name_from_region_path(
               "/vemb_v16_vectors_0", 1, name, sizeof(name)) == 0);
    assert(!strcmp(name, "/vemb_v16_vectors_0.alloc"));
    assert(vemb_v16_shared_allocator_name_from_region_path(
               "/tmp/vemb/vectors", 2, name, sizeof(name)) == 0);
    assert(name[0] == '/');
    assert(strstr(name, "/v16a_") == name);
}

int main(void) {
    test_name_derivation();
    test_multithread_unique_slots();
    test_reopen_does_not_reset_next_slot();
    test_multiprocess_shm_unique_slots();
    test_ub_provider_reopen_and_reset();
    test_real_ub_multiprocess_unique_slots_if_configured();
    printf("vemb_v16_shared_allocator_ut: all tests passed\n");
    return 0;
}
