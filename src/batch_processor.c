/*
 * Batch Processor Implementation
 * Microsecond-level batch processing for UB vector engine
 */

#include "batch_processor.h"
#include "ub_client.h"
#include "vector_engine.h"
#include "sve_compute.h"
#include <sys/time.h>
#include <unistd.h>
#include <time.h>

// Global batch processor instance
batch_processor_t *global_batch_processor = NULL;

// Utility function for current time in microseconds
static long get_time_us(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long)tv.tv_sec * 1000000 + (long)tv.tv_usec;
}

// Atomic request ID generator
static atomic_long next_request_id = 1;

// Initialize batch processor
int batch_processor_init(void) {
    if (global_batch_processor) return C_OK;

    global_batch_processor = zcalloc(sizeof(batch_processor_t));
    if (!global_batch_processor) return C_ERR;

    // Initialize batches
    for (int i = 0; i < MAX_CONCURRENT_BATCHES; i++) {
        embedding_batch_t *batch = &global_batch_processor->batches[i];
        batch->num_requests = 0;
        batch->batch_start_time = 0;
        batch->processing = 0;
        pthread_mutex_init(&batch->mutex, NULL);
        pthread_cond_init(&batch->cond, NULL);
    }

    // Initialize statistics
    atomic_init(&global_batch_processor->total_batches_processed, 0);
    atomic_init(&global_batch_processor->total_requests_processed, 0);
    atomic_init(&global_batch_processor->total_batch_latency_us, 0);
    atomic_init(&global_batch_processor->min_batch_latency_us, LONG_MAX);
    atomic_init(&global_batch_processor->max_batch_latency_us, 0);

    // Start processor thread
    global_batch_processor->running = 1;
    if (pthread_create(&global_batch_processor->processor_thread, NULL,
                       batch_processor_thread, NULL) != 0) {
        zfree(global_batch_processor);
        global_batch_processor = NULL;
        return C_ERR;
    }

    serverLog(LL_NOTICE, "Batch processor initialized with %d concurrent batches", MAX_CONCURRENT_BATCHES);
    return C_OK;
}

// Shutdown batch processor
void batch_processor_shutdown(void) {
    if (!global_batch_processor) return;

    global_batch_processor->running = 0;
    pthread_join(global_batch_processor->processor_thread, NULL);

    // Cleanup batches
    for (int i = 0; i < MAX_CONCURRENT_BATCHES; i++) {
        embedding_batch_t *batch = &global_batch_processor->batches[i];
        pthread_mutex_destroy(&batch->mutex);
        pthread_cond_destroy(&batch->cond);
    }

    zfree(global_batch_processor);
    global_batch_processor = NULL;

    serverLog(LL_NOTICE, "Batch processor shutdown");
}

// Submit batch VEMB request
int batch_submit_vemb_request(const char *key, const char *element,
                            vector_data_t *result, long timeout_us) {
    (void)timeout_us;
    if (!global_batch_processor || !key || !element || !result) return C_ERR;

    uint64_t request_id = atomic_fetch_add(&next_request_id, 1);

    // Find available batch
    for (int i = 0; i < MAX_CONCURRENT_BATCHES; i++) {
        embedding_batch_t *batch = &global_batch_processor->batches[i];

        pthread_mutex_lock(&batch->mutex);

        if (batch->num_requests < MAX_BATCH_SIZE && !batch->processing) {
            // Add request to batch
            batch_request_t *req = &batch->requests[batch->num_requests++];
            req->request_id = request_id;
            //TODO: 放到锁外面
            req->key_name = zstrdup(key);
            req->element_name = zstrdup(element);
            req->result = result;
            req->submit_time_us = get_time_us();
            req->completed = 0;
            req->error_code = 0;

            if (batch->num_requests == 1) {
                batch->batch_start_time = req->submit_time_us;
            }

            pthread_mutex_unlock(&batch->mutex);
            return request_id;
        }

        pthread_mutex_unlock(&batch->mutex);
    }

    return C_ERR; // No available batch slots
}

// Wait for batch completion
int batch_wait_for_completion(uint64_t request_id, long timeout_us) {
    if (!global_batch_processor) return C_ERR;

    long start_time = get_time_us();
    long end_time = start_time + timeout_us;

    // Find the request across all batches
    while (get_time_us() < end_time) {
        for (int i = 0; i < MAX_CONCURRENT_BATCHES; i++) {
            embedding_batch_t *batch = &global_batch_processor->batches[i];

            pthread_mutex_lock(&batch->mutex);

            for (size_t j = 0; j < batch->num_requests; j++) {
                batch_request_t *req = &batch->requests[j];
                if (req->request_id == request_id) {
                    if (req->completed) {
                        int error_code = req->error_code;
                        pthread_mutex_unlock(&batch->mutex);
                        return error_code;
                    }
                    break;
                }
            }

            pthread_mutex_unlock(&batch->mutex);
        }

        // TODO: yield
        // Small sleep to avoid busy waiting
        struct timespec sleep_time = {0, 1000}; // 1 microsecond
        nanosleep(&sleep_time, NULL);
    }

    return C_ERR; // Timeout
}

// Batch processor thread
void *batch_processor_thread(void *arg) {
    (void)arg;
    serverLog(LL_NOTICE, "Batch processor thread started");

    while (global_batch_processor && global_batch_processor->running) {
        int processed_any = 0;

        for (int i = 0; i < MAX_CONCURRENT_BATCHES; i++) {
            embedding_batch_t *batch = &global_batch_processor->batches[i];

            pthread_mutex_lock(&batch->mutex);

            // Check if batch is ready for processing
            if (batch->num_requests > 0 && !batch->processing) {
                long current_time = get_time_us();
                long batch_age = current_time - batch->batch_start_time;

                // Process batch if full or timed out
                if (batch->num_requests >= MAX_BATCH_SIZE ||
                    batch_age >= BATCH_TIMEOUT_US) {

                    batch->processing = 1;
                    pthread_mutex_unlock(&batch->mutex);

                    // Process the batch
                    batch_process_ub_embeddings(batch);

                    // Update statistics
                    long batch_latency = current_time - batch->batch_start_time;
                    atomic_fetch_add(&global_batch_processor->total_batches_processed, 1);
                    atomic_fetch_add(&global_batch_processor->total_requests_processed, batch->num_requests);
                    atomic_fetch_add(&global_batch_processor->total_batch_latency_us, batch_latency);

                    long min_lat = atomic_load(&global_batch_processor->min_batch_latency_us);
                    while (batch_latency < min_lat &&
                           !atomic_compare_exchange_weak(&global_batch_processor->min_batch_latency_us, &min_lat, batch_latency)) {
                        min_lat = atomic_load(&global_batch_processor->min_batch_latency_us);
                    }

                    long max_lat = atomic_load(&global_batch_processor->max_batch_latency_us);
                    while (batch_latency > max_lat &&
                           !atomic_compare_exchange_weak(&global_batch_processor->max_batch_latency_us, &max_lat, batch_latency)) {
                        max_lat = atomic_load(&global_batch_processor->max_batch_latency_us);
                    }

                    // Reset batch for next use
                    for (size_t j = 0; j < batch->num_requests; j++) {
                        batch_request_t *req = &batch->requests[j];
                        zfree(req->key_name);
                        zfree(req->element_name);
                    }
                    batch->num_requests = 0;
                    batch->batch_start_time = 0;
                    batch->processing = 0;

                    processed_any = 1;
                } else {
                    pthread_mutex_unlock(&batch->mutex);
                }
            } else {
                pthread_mutex_unlock(&batch->mutex);
            }
        }

        // If no batches were processed, sleep briefly
        if (!processed_any) {
            struct timespec sleep_time = {0, 10000}; // 10 microseconds
            nanosleep(&sleep_time, NULL);
        }
    }

    serverLog(LL_NOTICE, "Batch processor thread stopped");
    return NULL;
}

// Process UB embeddings batch
int batch_process_ub_embeddings(embedding_batch_t *batch) {
    if (!batch || batch->num_requests == 0) return C_ERR;

    // Simulate UB batch processing with SVE acceleration
    // In real implementation, this would:
    // 1. Gather all element IDs
    // 2. Perform SVE batch gather from UB memory
    // 3. Parallel dequantization and normalization

    struct timespec process_time = {0, batch->num_requests * 1000}; // 1μs per request
    nanosleep(&process_time, NULL);

    // Process each request in the batch
    for (size_t i = 0; i < batch->num_requests; i++) {
        batch_request_t *req = &batch->requests[i];

        // Simulate UB VEMB processing
        int ret = ub_engine_vemb(NULL, (void*)req->key_name,
                               (void*)req->element_name, req->result);

        req->completed = 1;
        req->error_code = ret;
    }

    return C_OK;
}

// Get batch processor statistics
sds batch_processor_get_stats(void) {
    sds stats = sdsempty();

    if (!global_batch_processor) {
        stats = sdscat(stats, "Batch Processor: Not initialized");
        return stats;
    }

    long total_batches = atomic_load(&global_batch_processor->total_batches_processed);
    long total_requests = atomic_load(&global_batch_processor->total_requests_processed);
    long total_latency = atomic_load(&global_batch_processor->total_batch_latency_us);
    long min_latency = atomic_load(&global_batch_processor->min_batch_latency_us);
    long max_latency = atomic_load(&global_batch_processor->max_batch_latency_us);

    double avg_latency = total_batches > 0 ? (double)total_latency / total_batches : 0;
    double avg_requests_per_batch = total_batches > 0 ? (double)total_requests / total_batches : 0;

    stats = sdscatprintf(stats, "Batch Processor Stats:\n");
    stats = sdscatprintf(stats, "  Total batches processed: %ld\n", total_batches);
    stats = sdscatprintf(stats, "  Total requests processed: %ld\n", total_requests);
    stats = sdscatprintf(stats, "  Average requests per batch: %.1f\n", avg_requests_per_batch);
    stats = sdscatprintf(stats, "  Average batch latency: %.1f μs\n", avg_latency);
    stats = sdscatprintf(stats, "  Min batch latency: %ld μs\n", min_latency == LONG_MAX ? 0 : min_latency);
    stats = sdscatprintf(stats, "  Max batch latency: %ld μs\n", max_latency);

    return stats;
}