/*
 * Batch Processor for UB Vector Engine
 * High-throughput batch processing of embedding requests
 */

#ifndef __BATCH_PROCESSOR_H
#define __BATCH_PROCESSOR_H

#include "vector_engine.h"
#include <pthread.h>
#include <stdatomic.h>

// Batch processing configuration
#define MAX_BATCH_SIZE 1024
#define BATCH_TIMEOUT_US 50000    // 50μs batch timeout
#define MAX_CONCURRENT_BATCHES 8

// Batch request structure
typedef struct {
    uint64_t request_id;
    char *key_name;
    char *element_name;
    vector_data_t *result;
    long submit_time_us;
    int completed;
    int error_code;
} batch_request_t;

// Batch structure
typedef struct {
    batch_request_t requests[MAX_BATCH_SIZE];
    size_t num_requests;
    long batch_start_time;
    int processing;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} embedding_batch_t;

// Batch processor context
typedef struct {
    embedding_batch_t batches[MAX_CONCURRENT_BATCHES];
    atomic_int next_batch_id;
    pthread_t processor_thread;
    int running;

    // Statistics
    atomic_long total_batches_processed;
    atomic_long total_requests_processed;
    atomic_long total_batch_latency_us;
    atomic_long min_batch_latency_us;
    atomic_long max_batch_latency_us;

} batch_processor_t;

// Global batch processor
extern batch_processor_t *global_batch_processor;

// Batch processor API
int batch_processor_init(void);
void batch_processor_shutdown(void);

// Batch submission API
int batch_submit_vemb_request(const char *key, const char *element,
                            vector_data_t *result, long timeout_us);

int batch_wait_for_completion(uint64_t request_id, long timeout_us);

// Batch processing thread
void *batch_processor_thread(void *arg);

// Statistics
sds batch_processor_get_stats(void);

// Low-latency batch processing
int batch_process_ub_embeddings(embedding_batch_t *batch);

#endif /* __BATCH_PROCESSOR_H */