/*
 * UB (Unified Bus) Client for High-Performance Feature Queries
 * Implements user-space zero-copy communication with SVE acceleration
 */

#ifndef __UB_CLIENT_H
#define __UB_CLIENT_H

#include "server.h"
#include "vector_engine.h"
#include <stdint.h>
#include <pthread.h>

/* UB Constants */
#define UB_MAX_EIDS 1024
#define UB_MAX_CNAs 256
#define UB_RING_BUFFER_SIZE (1024 * 1024)  /* 1MB ring buffer */
#define UB_SVE_VECTOR_SIZE 256             /* SVE vector size in bits */
#define UB_MEM_PAGE_SIZE (4 * 1024 * 1024) /* 4MB huge pages */

/* UB Entity Types */
typedef enum {
    UB_ENTITY_COMPUTE_NODE = 0,
    UB_ENTITY_MEMORY_TILE = 1,
    UB_ENTITY_FABRIC_MANAGER = 2,
    UB_ENTITY_STORAGE_NODE = 3
} ub_entity_type_t;

/* UB Message Types */
typedef enum {
    UB_MSG_MEMORY_LOAD = 0xC00B0020,    /* Image service load */
    UB_MSG_MEMORY_QUERY = 0xC00B0021,   /* Capability query */
    UB_MSG_MEMORY_INFO = 0xC00B0022,    /* File info request */
    UB_MSG_VECTOR_GATHER = 0xC00B0100,  /* SVE gather load */
    UB_MSG_VECTOR_SCATTER = 0xC00B0101  /* SVE scatter store */
} ub_message_type_t;

/* UB Address Space */
typedef struct {
    uint64_t base_addr;     /* Base physical address */
    size_t size;           /* Address space size */
    uint32_t token_id;     /* Access token */
    void *mapped_addr;     /* Local virtual mapping */
} ub_address_space_t;

/* UB Entity Information */
typedef struct {
    uint32_t eid;          /* Entity ID */
    uint32_t cna;          /* Compact Network Address */
    ub_entity_type_t type; /* Entity type */
    ub_address_space_t *addr_space; /* Associated address space */
} ub_entity_t;

/* UB Ring Buffer for Event Notifications */
typedef struct {
    volatile uint64_t head;           /* Read pointer */
    volatile uint64_t tail;           /* Write pointer */
    uint8_t buffer[UB_RING_BUFFER_SIZE]; /* Ring buffer data */
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} ub_ring_buffer_t;

/* UB Submission Queue */
typedef struct {
    volatile uint64_t head;
    volatile uint64_t tail;
    uint8_t *entries;
    size_t entry_size;
    size_t queue_size;
    pthread_mutex_t mutex;
} ub_submission_queue_t;

/* UB Completion Queue */
typedef struct {
    volatile uint64_t head;
    volatile uint64_t tail;
    uint8_t *entries;
    size_t entry_size;
    size_t queue_size;
    pthread_mutex_t mutex;
} ub_completion_queue_t;

/* UB Client Context */
typedef struct {
    /* Connection state */
    int initialized;
    uint32_t local_eid;
    uint32_t local_cna;

    /* UB entities */
    ub_entity_t entities[UB_MAX_EIDS];
    size_t num_entities;

    /* Communication queues */
    ub_submission_queue_t *sq;
    ub_completion_queue_t *cq;
    ub_ring_buffer_t *event_rb;

    /* Memory mappings */
    ub_address_space_t *global_ubas;  /* Global UB Address Space */

    /* SVE context */
    void *sve_context;
    size_t sve_vl;  /* SVE vector length */

    /* Threading */
    pthread_t event_thread;
    int event_thread_running;

    /* Statistics */
    uint64_t total_requests;
    uint64_t total_responses;
    uint64_t cache_hits;
    uint64_t cache_misses;

} ub_client_t;

/* UB Message Structures */
typedef struct {
    uint32_t call_id;
    uint32_t sender_eid;
    uint32_t receiver_eid;
    uint32_t payload_size;
    uint8_t payload[];
} __attribute__((packed)) ub_message_t;

/* Image Service Request (for embedding loading) */
typedef struct {
    uint32_t ubfm_eid;
    uint32_t client_cna;
    uint32_t client_eid;
    char resource_name[256];
} __attribute__((packed)) ub_image_request_t;

/* Vector Gather Request */
typedef struct {
    uint64_t base_addr;     /* Base address in UBAS */
    uint32_t token_id;      /* Access token */
    uint32_t vector_dim;    /* Vector dimension */
    uint32_t num_vectors;   /* Number of vectors to gather */
    uint64_t indices[];     /* Vector indices */
} __attribute__((packed)) ub_gather_request_t;

/* Global UB Client */
extern ub_client_t *global_ub_client;

/* UB Client API */
int ub_client_init(void);
void ub_client_cleanup(void);

int ub_client_connect_fabric_manager(void);
int ub_client_enumerate_entities(void);

int ub_client_load_embedding_table(const char *resource_name,
                                 ub_address_space_t **addr_space);

int ub_client_perform_gather_load(ub_address_space_t *addr_space,
                                uint64_t *indices, size_t num_indices,
                                float *results, size_t vector_dim);

int ub_client_send_message(ub_message_type_t type, void *payload,
                          size_t payload_size, uint32_t target_eid);

int ub_client_receive_message(ub_message_t **msg, int timeout_ms);

/* SVE Accelerated Operations */
int sve_init_context(void *context);
void sve_cleanup_context(void *context);

int sve_gather_load_f32(void *sve_ctx, const float *base_addr,
                       const uint64_t *indices, size_t num_indices,
                       float *results);

int sve_scatter_store_f32(void *sve_ctx, float *base_addr,
                         const uint64_t *indices, const float *values,
                         size_t num_indices);

/* Memory Management */
int ub_mmap_remote_memory(uint64_t ubas_addr, size_t size,
                         uint32_t token_id, void **local_addr);

void ub_unmap_memory(void *local_addr, size_t size);

/* Configuration */
int ub_client_set_config(const char *key, const char *value);
sds ub_client_get_config(const char *key);

/* Statistics */
sds ub_client_get_stats(void);

#endif /* __UB_CLIENT_H */