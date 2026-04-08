/*
 * UB Client Implementation
 * User-space zero-copy communication with SVE acceleration
 */

#include "ub_client.h"
#include "server.h"
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <dlfcn.h>

/* Global UB client instance */
ub_client_t *global_ub_client = NULL;

/* SVE instruction detection and availability */
static int sve_supported = 0;
static size_t sve_vector_length = 0;

/* UB Firmware Library Handles */
static void *ubios_handle = NULL;
static void *sve_handle = NULL;

/* Function pointers for UB APIs */
typedef int (*ubios_call_func)(uint32_t call_id, uint32_t receiver_id,
                              void *input, size_t input_size,
                              void *output, size_t output_size);

typedef void* (*ubios_mmap_remote_func)(uint64_t ubas_addr, size_t size,
                                       uint32_t token_id);

typedef int (*sve_init_func)(void **context);
typedef void (*sve_cleanup_func)(void *context);
typedef int (*sve_gather_func)(void *context, const float *base,
                              const uint64_t *indices, size_t count,
                              float *results);

static ubios_call_func ubios_call_ptr = NULL;
static ubios_mmap_remote_func ubios_mmap_remote_ptr = NULL;
static sve_init_func sve_init_ptr = NULL;
static sve_cleanup_func sve_cleanup_ptr = NULL;
static sve_gather_func sve_gather_ptr = NULL;

/* Check SVE support */
static int check_sve_support(void) {
    /* Check if running on ARM64 with SVE support */
    /* This is a simplified check - in real implementation */
    /* we'd use getauxval() or CPUID equivalents */

#ifdef __aarch64__
    /* Try to detect SVE availability */
    /* For now, assume SVE is available on aarch64 */
    sve_supported = 1;
    sve_vector_length = 256; /* Assume 256-bit vectors */
    return 1;
#else
    serverLog(LL_WARNING, "SVE not supported on this architecture");
    return 0;
#endif
}

/* Load UB libraries */
static int load_ub_libraries(void) {
    /* Load UB firmware library */
    ubios_handle = dlopen("libubios.so", RTLD_LAZY);
    if (!ubios_handle) {
        serverLog(LL_WARNING, "Failed to load UB firmware library: %s", dlerror());
        return 0;
    }

    /* Load function pointers */
    ubios_call_ptr = dlsym(ubios_handle, "ubios_call");
    ubios_mmap_remote_ptr = dlsym(ubios_handle, "ubios_mmap_remote");

    if (!ubios_call_ptr || !ubios_mmap_remote_ptr) {
        serverLog(LL_WARNING, "Failed to load UB firmware functions");
        dlclose(ubios_handle);
        ubios_handle = NULL;
        return 0;
    }

    /* Load SVE library */
    sve_handle = dlopen("libsve.so", RTLD_LAZY);
    if (!sve_handle) {
        serverLog(LL_WARNING, "Failed to load SVE library: %s", dlerror());
        /* SVE is optional - continue without it */
    } else {
        sve_init_ptr = dlsym(sve_handle, "sve_init_context");
        sve_cleanup_ptr = dlsym(sve_handle, "sve_cleanup_context");
        sve_gather_ptr = dlsym(sve_handle, "sve_gather_load_f32");

        if (!sve_init_ptr || !sve_cleanup_ptr || !sve_gather_ptr) {
            serverLog(LL_WARNING, "Failed to load SVE functions");
            dlclose(sve_handle);
            sve_handle = NULL;
        }
    }

    return 1;
}

/* Initialize ring buffer */
static ub_ring_buffer_t *ring_buffer_create(void) {
    ub_ring_buffer_t *rb = zmalloc(sizeof(ub_ring_buffer_t));
    if (!rb) return NULL;

    rb->head = 0;
    rb->tail = 0;
    memset(rb->buffer, 0, UB_RING_BUFFER_SIZE);

    pthread_mutex_init(&rb->mutex, NULL);
    pthread_cond_init(&rb->cond, NULL);

    return rb;
}

static void ring_buffer_destroy(ub_ring_buffer_t *rb) {
    if (rb) {
        pthread_mutex_destroy(&rb->mutex);
        pthread_cond_destroy(&rb->cond);
        zfree(rb);
    }
}

/* Event handling thread */
static void *ub_event_thread_func(void *arg) {
    ub_client_t *client = arg;

    serverLog(LL_NOTICE, "UB event thread started");

    while (client->event_thread_running) {
        /* Poll completion queue and ring buffer */
        /* Process incoming messages and events */

        /* Check for new completions */
        ub_message_t *msg = NULL;
        if (ub_client_receive_message(&msg, 100) == C_OK && msg) {
            /* Process message */
            client->total_responses++;

            /* Free message */
            zfree(msg);
        }

        usleep(1000); /* 1ms sleep to avoid busy loop */
    }

    serverLog(LL_NOTICE, "UB event thread stopped");
    return NULL;
}

/* Initialize UB client */
int ub_client_init(void) {
    if (global_ub_client) {
        return C_OK; /* Already initialized */
    }

    /* Check SVE support */
    if (!check_sve_support()) {
        serverLog(LL_WARNING, "SVE not supported, falling back to scalar operations");
    }

    /* Load UB libraries */
    if (!load_ub_libraries()) {
        serverLog(LL_WARNING, "Failed to load UB libraries");
        return C_ERR;
    }

    /* Allocate client structure */
    global_ub_client = zcalloc(sizeof(ub_client_t));
    if (!global_ub_client) {
        return C_ERR;
    }

    /* Initialize client */
    global_ub_client->initialized = 0;
    global_ub_client->num_entities = 0;

    /* Create communication queues */
    global_ub_client->sq = zcalloc(sizeof(ub_submission_queue_t));
    global_ub_client->cq = zcalloc(sizeof(ub_completion_queue_t));
    global_ub_client->event_rb = ring_buffer_create();

    if (!global_ub_client->sq || !global_ub_client->cq || !global_ub_client->event_rb) {
        ub_client_cleanup();
        return C_ERR;
    }

    /* Initialize SVE context if available */
    if (sve_supported && sve_init_ptr) {
        if (sve_init_ptr(&global_ub_client->sve_context) != C_OK) {
            serverLog(LL_WARNING, "Failed to initialize SVE context");
            global_ub_client->sve_context = NULL;
        } else {
            global_ub_client->sve_vl = sve_vector_length / 8; /* bytes per vector */
        }
    }

    /* Connect to fabric manager */
    if (ub_client_connect_fabric_manager() != C_OK) {
        serverLog(LL_WARNING, "Failed to connect to UB fabric manager");
        ub_client_cleanup();
        return C_ERR;
    }

    /* Enumerate entities */
    if (ub_client_enumerate_entities() != C_OK) {
        serverLog(LL_WARNING, "Failed to enumerate UB entities");
        ub_client_cleanup();
        return C_ERR;
    }

    /* Start event thread */
    global_ub_client->event_thread_running = 1;
    if (pthread_create(&global_ub_client->event_thread, NULL,
                       ub_event_thread_func, global_ub_client) != 0) {
        serverLog(LL_WARNING, "Failed to create UB event thread");
        ub_client_cleanup();
        return C_ERR;
    }

    global_ub_client->initialized = 1;
    serverLog(LL_NOTICE, "UB client initialized successfully");
    return C_OK;
}

/* Cleanup UB client */
void ub_client_cleanup(void) {
    if (!global_ub_client) return;

    /* Stop event thread */
    if (global_ub_client->event_thread_running) {
        global_ub_client->event_thread_running = 0;
        pthread_join(global_ub_client->event_thread, NULL);
    }

    /* Cleanup SVE context */
    if (global_ub_client->sve_context && sve_cleanup_ptr) {
        sve_cleanup_ptr(global_ub_client->sve_context);
    }

    /* Free communication queues */
    if (global_ub_client->sq) zfree(global_ub_client->sq);
    if (global_ub_client->cq) zfree(global_ub_client->cq);
    if (global_ub_client->event_rb) ring_buffer_destroy(global_ub_client->event_rb);

    /* Unmap global address space */
    if (global_ub_client->global_ubas && global_ub_client->global_ubas->mapped_addr) {
        ub_unmap_memory(global_ub_client->global_ubas->mapped_addr,
                       global_ub_client->global_ubas->size);
        zfree(global_ub_client->global_ubas);
    }

    /* Free entities */
    for (size_t i = 0; i < global_ub_client->num_entities; i++) {
        if (global_ub_client->entities[i].addr_space) {
            zfree(global_ub_client->entities[i].addr_space);
        }
    }

    /* Close libraries */
    if (ubios_handle) dlclose(ubios_handle);
    if (sve_handle) dlclose(sve_handle);

    zfree(global_ub_client);
    global_ub_client = NULL;

    serverLog(LL_NOTICE, "UB client cleaned up");
}

/* Connect to UB fabric manager */
int ub_client_connect_fabric_manager(void) {
    /* This would implement the actual connection to UB fabric manager */
    /* For now, simulate connection */

    serverLog(LL_NOTICE, "Connecting to UB fabric manager...");

    /* Get local EID and CNA from system registers/hardware */
    /* In simulation, use dummy values */
    global_ub_client->local_eid = 0x1001;
    global_ub_client->local_cna = 0x2001;

    serverLog(LL_NOTICE, "Connected to UB fabric manager (EID: %x, CNA: %x)",
              global_ub_client->local_eid, global_ub_client->local_cna);

    return C_OK;
}

/* Enumerate UB entities */
int ub_client_enumerate_entities(void) {
    /* Enumerate compute nodes, memory tiles, etc. */
    /* This would query the fabric manager for available entities */

    serverLog(LL_NOTICE, "Enumerating UB entities...");

    /* Add fabric manager */
    global_ub_client->entities[0].eid = 0x0001; /* UBFM EID */
    global_ub_client->entities[0].cna = 0x0001;
    global_ub_client->entities[0].type = UB_ENTITY_FABRIC_MANAGER;
    global_ub_client->num_entities = 1;

    /* Add memory tile */
    global_ub_client->entities[1].eid = 0x2001;
    global_ub_client->entities[1].cna = 0x2001;
    global_ub_client->entities[1].type = UB_ENTITY_MEMORY_TILE;

    /* Allocate address space for memory tile */
    global_ub_client->entities[1].addr_space = zcalloc(sizeof(ub_address_space_t));
    if (global_ub_client->entities[1].addr_space) {
        global_ub_client->entities[1].addr_space->base_addr = 0x100000000ULL; /* 4GB */
        global_ub_client->entities[1].addr_space->size = 600ULL * 1024 * 1024 * 1024; /* 600GB */
        global_ub_client->entities[1].addr_space->token_id = 0x12345678;
    }

    global_ub_client->num_entities = 2;

    serverLog(LL_NOTICE, "Enumerated %zu UB entities", global_ub_client->num_entities);
    return C_OK;
}

/* Load embedding table */
int ub_client_load_embedding_table(const char *resource_name,
                                 ub_address_space_t **addr_space) {
    if (!global_ub_client || !global_ub_client->initialized) {
        return C_ERR;
    }

    serverLog(LL_NOTICE, "Loading embedding table: %s", resource_name);

    /* Find memory tile entity */
    ub_entity_t *mem_tile = NULL;
    for (size_t i = 0; i < global_ub_client->num_entities; i++) {
        if (global_ub_client->entities[i].type == UB_ENTITY_MEMORY_TILE) {
            mem_tile = &global_ub_client->entities[i];
            break;
        }
    }

    if (!mem_tile || !mem_tile->addr_space) {
        serverLog(LL_WARNING, "No memory tile available");
        return C_ERR;
    }

    /* Send image service request */
    ub_image_request_t request = {
        .ubfm_eid = 0x0001, /* Fabric manager EID */
        .client_cna = global_ub_client->local_cna,
        .client_eid = global_ub_client->local_eid,
    };
    strncpy(request.resource_name, resource_name, sizeof(request.resource_name));

    if (ub_client_send_message(UB_MSG_MEMORY_LOAD, &request,
                             sizeof(request), 0x0001) != C_OK) {
        return C_ERR;
    }

    /* Wait for response */
    ub_message_t *response = NULL;
    if (ub_client_receive_message(&response, 5000) != C_OK || !response) {
        serverLog(LL_WARNING, "Timeout waiting for embedding table load response");
        return C_ERR;
    }

    /* Process response and map memory */
    *addr_space = mem_tile->addr_space;

    /* Map the remote memory to local address space */
    if (ub_mmap_remote_memory((*addr_space)->base_addr, (*addr_space)->size,
                            (*addr_space)->token_id,
                            &(*addr_space)->mapped_addr) != C_OK) {
        serverLog(LL_WARNING, "Failed to map remote UB memory");
        zfree(response);
        return C_ERR;
    }

    serverLog(LL_NOTICE, "Successfully loaded embedding table: %s", resource_name);
    zfree(response);
    return C_OK;
}

/* Perform SVE gather load */
int ub_client_perform_gather_load(ub_address_space_t *addr_space,
                                uint64_t *indices, size_t num_indices,
                                float *results, size_t vector_dim) {
    if (!global_ub_client || !addr_space || !addr_space->mapped_addr) {
        return C_ERR;
    }

    global_ub_client->total_requests++;

    /* Check if SVE is available */
    if (global_ub_client->sve_context && sve_gather_ptr) {
        /* Use SVE accelerated gather */
        return sve_gather_load_f32(global_ub_client->sve_context,
                                 addr_space->mapped_addr,
                                 indices, num_indices, results);
    } else {
        /* Fallback to scalar gather */
        const float *base = addr_space->mapped_addr;

        for (size_t i = 0; i < num_indices; i++) {
            uint64_t idx = indices[i];
            if (idx * vector_dim >= addr_space->size / sizeof(float)) {
                serverLog(LL_WARNING, "Index out of bounds: %llu", (unsigned long long)idx);
                return C_ERR;
            }

            memcpy(&results[i * vector_dim], &base[idx * vector_dim],
                   vector_dim * sizeof(float));
        }

        return C_OK;
    }
}

/* Send UB message */
int ub_client_send_message(ub_message_type_t type, void *payload,
                          size_t payload_size, uint32_t target_eid) {
    if (!global_ub_client || !ubios_call_ptr) {
        return C_ERR;
    }

    /* Use UB firmware call */
    int result = ubios_call_ptr((uint32_t)type, target_eid,
                               payload, payload_size, NULL, 0);

    return result == 0 ? C_OK : C_ERR;
}

/* Receive UB message */
int ub_client_receive_message(ub_message_t **msg, int timeout_ms) {
    /* This would implement message reception from completion queue */
    /* For now, return timeout */
    *msg = NULL;
    return C_ERR; /* Timeout */
}

/* Memory mapping functions */
int ub_mmap_remote_memory(uint64_t ubas_addr, size_t size,
                         uint32_t token_id, void **local_addr) {
    if (!ubios_mmap_remote_ptr) {
        /* Fallback to regular mmap for simulation */
        *local_addr = mmap(NULL, size, PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        return (*local_addr != MAP_FAILED) ? C_OK : C_ERR;
    }

    *local_addr = ubios_mmap_remote_ptr(ubas_addr, size, token_id);
    return (*local_addr != NULL) ? C_OK : C_ERR;
}

void ub_unmap_memory(void *local_addr, size_t size) {
    if (local_addr) {
        munmap(local_addr, size);
    }
}

/* SVE functions */
int sve_init_context(void *context) {
    if (sve_init_ptr) {
        return sve_init_ptr(&context);
    }
    return C_ERR;
}

void sve_cleanup_context(void *context) {
    if (sve_cleanup_ptr) {
        sve_cleanup_ptr(context);
    }
}

int sve_gather_load_f32(void *sve_ctx, const float *base_addr,
                       const uint64_t *indices, size_t num_indices,
                       float *results) {
    if (sve_gather_ptr) {
        return sve_gather_ptr(sve_ctx, base_addr, indices, num_indices, results);
    }

    /* Fallback implementation */
    for (size_t i = 0; i < num_indices; i++) {
        results[i] = base_addr[indices[i]];
    }
    return C_OK;
}

int sve_scatter_store_f32(void *sve_ctx, float *base_addr,
                         const uint64_t *indices, const float *values,
                         size_t num_indices) {
    /* SVE scatter store implementation */
    for (size_t i = 0; i < num_indices; i++) {
        base_addr[indices[i]] = values[i];
    }
    return C_OK;
}

/* Configuration */
int ub_client_set_config(const char *key, const char *value) {
    /* Store configuration - implementation needed */
    return C_OK;
}

sds ub_client_get_config(const char *key) {
    /* Retrieve configuration - implementation needed */
    return NULL;
}

/* Statistics */
sds ub_client_get_stats(void) {
    sds stats = sdsempty();

    if (!global_ub_client) {
        stats = sdscat(stats, "UB Client: Not initialized");
        return stats;
    }

    stats = sdscatprintf(stats, "UB Client Stats:\n");
    stats = sdscatprintf(stats, "  Initialized: %s\n",
                        global_ub_client->initialized ? "Yes" : "No");
    stats = sdscatprintf(stats, "  Local EID: 0x%x\n", global_ub_client->local_eid);
    stats = sdscatprintf(stats, "  Local CNA: 0x%x\n", global_ub_client->local_cna);
    stats = sdscatprintf(stats, "  Entities: %zu\n", global_ub_client->num_entities);
    stats = sdscatprintf(stats, "  SVE Supported: %s\n", sve_supported ? "Yes" : "No");
    stats = sdscatprintf(stats, "  SVE Vector Length: %zu bits\n", sve_vector_length);
    stats = sdscatprintf(stats, "  Total Requests: %llu\n",
                        (unsigned long long)global_ub_client->total_requests);
    stats = sdscatprintf(stats, "  Total Responses: %llu\n",
                        (unsigned long long)global_ub_client->total_responses);

    return stats;
}