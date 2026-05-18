#include "test_runtime_shim.h"

#include "../src/vector_proxy_completion.h"
#include "../src/vector_proxy_request.h"
#include "../src/zmalloc.h"

#include <pthread.h>
#include <stdio.h>
#include <string.h>

#ifndef C_OK
#define C_OK 0
#endif
#ifndef C_ERR
#define C_ERR 1
#endif

#define ASSERT_TRUE(expr)                                                     \
    do {                                                                      \
        if (!(expr)) {                                                        \
            fprintf(stderr, "assertion failed at %s:%d: %s\n", __FILE__,     \
                    __LINE__, #expr);                                         \
            return 1;                                                         \
        }                                                                     \
    } while (0)

#define ASSERT_OK(expr) ASSERT_TRUE((expr) == C_OK)
#define ASSERT_ERR(expr) ASSERT_TRUE((expr) == C_ERR)

static int test_vemb_completion_lifecycle(void) {
    ASSERT_OK(vector_proxy_completion_init());

    proxy_vector_request_t *req = proxy_vector_request_create_vemb(101, 7, 0, NULL, 0);
    proxy_vector_request_t *dup = proxy_vector_request_create_vemb(101, 8, 0, NULL, 0);
    ASSERT_TRUE(req != NULL);
    ASSERT_TRUE(dup != NULL);

    ASSERT_OK(vector_proxy_completion_register(req));
    ASSERT_ERR(vector_proxy_completion_register(dup));
    proxy_vector_request_free(dup);

    float vector[] = {1.0f, 2.0f, 3.0f};
    ASSERT_OK(vector_proxy_completion_complete_vemb(req->request_id, vector, 3, C_OK));

    proxy_vector_request_t *found = vector_proxy_completion_lookup(req->request_id);
    ASSERT_TRUE(found == req);
    ASSERT_TRUE(found->error_code == C_OK);
    ASSERT_TRUE(found->result_dim == 3);
    ASSERT_TRUE(found->result_vector != NULL);
    ASSERT_TRUE(found->result_vector != vector);
    ASSERT_TRUE(memcmp(found->result_vector, vector, sizeof(vector)) == 0);
    ASSERT_TRUE(found->completion_time_us > 0);

    proxy_vector_request_t *taken = NULL;
    ASSERT_OK(vector_proxy_completion_take(req->request_id, &taken));
    ASSERT_TRUE(taken == req);
    ASSERT_TRUE(vector_proxy_completion_lookup(req->request_id) == NULL);
    ASSERT_ERR(vector_proxy_completion_take(req->request_id, &taken));
    proxy_vector_request_free(req);
    return 0;
}

static int test_vemb_error_completion(void) {
    proxy_vector_request_t *req = proxy_vector_request_create_vemb(102, 9, 0, NULL, 0);
    ASSERT_TRUE(req != NULL);
    ASSERT_OK(vector_proxy_completion_register(req));

    float vector[] = {4.0f, 5.0f};
    ASSERT_ERR(vector_proxy_completion_complete_vemb(req->request_id, vector, 2, C_ERR));

    proxy_vector_request_t *found = vector_proxy_completion_lookup(req->request_id);
    ASSERT_TRUE(found == req);
    ASSERT_TRUE(found->error_code == C_ERR);
    ASSERT_TRUE(found->result_vector == NULL);
    ASSERT_TRUE(found->result_dim == 0);

    proxy_vector_request_t *taken = NULL;
    ASSERT_OK(vector_proxy_completion_take(req->request_id, &taken));
    ASSERT_TRUE(taken == req);
    proxy_vector_request_free(req);
    return 0;
}

static int test_vsim_completion_lifecycle(void) {
    float *query = zmalloc(sizeof(float) * 2);
    ASSERT_TRUE(query != NULL);
    query[0] = 0.25f;
    query[1] = 0.75f;

    proxy_vector_request_t *req =
        proxy_vector_request_create_vsim(201, query, 2, NULL, NULL, 0, 2, 1, NULL);
    ASSERT_TRUE(req != NULL);
    ASSERT_OK(vector_proxy_completion_register(req));

    uint64_t rows[] = {11, 22};
    float scores[] = {0.9f, 0.8f};
    ASSERT_OK(vector_proxy_completion_complete_vsim(req->request_id, rows, scores, 2, C_OK));

    proxy_vector_request_t *found = vector_proxy_completion_lookup(req->request_id);
    ASSERT_TRUE(found == req);
    ASSERT_TRUE(found->error_code == C_OK);
    ASSERT_TRUE(found->result_count == 2);
    ASSERT_TRUE(found->result_rows != NULL);
    ASSERT_TRUE(found->result_scores != NULL);
    ASSERT_TRUE(found->result_rows != rows);
    ASSERT_TRUE(found->result_scores != scores);
    ASSERT_TRUE(memcmp(found->result_rows, rows, sizeof(rows)) == 0);
    ASSERT_TRUE(memcmp(found->result_scores, scores, sizeof(scores)) == 0);

    proxy_vector_request_t *taken = NULL;
    ASSERT_OK(vector_proxy_completion_take(req->request_id, &taken));
    ASSERT_TRUE(taken == req);
    proxy_vector_request_free(req);
    return 0;
}

typedef struct completion_thread_arg {
    uint64_t base_id;
    size_t iterations;
    int failed;
} completion_thread_arg_t;

static void *completion_thread_main(void *arg) {
    completion_thread_arg_t *thread_arg = arg;

    for (size_t i = 0; i < thread_arg->iterations; i++) {
        uint64_t request_id = thread_arg->base_id + i;
        proxy_vector_request_t *req =
            proxy_vector_request_create_vemb(request_id, request_id + 100, 0, NULL, 0);
        if (!req) {
            thread_arg->failed = 1;
            return NULL;
        }
        if (vector_proxy_completion_register(req) != C_OK) {
            proxy_vector_request_free(req);
            thread_arg->failed = 1;
            return NULL;
        }

        float vector[] = {(float)request_id, (float)i};
        if (vector_proxy_completion_complete_vemb(request_id, vector, 2, C_OK) != C_OK) {
            proxy_vector_request_free(req);
            thread_arg->failed = 1;
            return NULL;
        }

        proxy_vector_request_t *found = vector_proxy_completion_lookup(request_id);
        if (found != req || found->result_dim != 2 || !found->result_vector ||
            found->result_vector[0] != vector[0] || found->result_vector[1] != vector[1]) {
            proxy_vector_request_free(req);
            thread_arg->failed = 1;
            return NULL;
        }

        proxy_vector_request_t *taken = NULL;
        if (vector_proxy_completion_take(request_id, &taken) != C_OK || taken != req) {
            proxy_vector_request_free(req);
            thread_arg->failed = 1;
            return NULL;
        }
        proxy_vector_request_free(req);
    }
    return NULL;
}

static int test_concurrent_register_complete_take(void) {
    enum { THREADS = 8, ITERATIONS = 512 };
    pthread_t threads[THREADS];
    completion_thread_arg_t args[THREADS];

    for (size_t i = 0; i < THREADS; i++) {
        args[i].base_id = 1000000 + i * 100000;
        args[i].iterations = ITERATIONS;
        args[i].failed = 0;
        ASSERT_TRUE(pthread_create(&threads[i], NULL, completion_thread_main, &args[i]) == 0);
    }

    for (size_t i = 0; i < THREADS; i++) {
        ASSERT_TRUE(pthread_join(threads[i], NULL) == 0);
        ASSERT_TRUE(args[i].failed == 0);
    }
    return 0;
}

int main(void) {
    test_runtime_reset_server();
    vector_proxy_completion_cleanup();

    if (test_vemb_completion_lifecycle() != 0) return 1;
    if (test_vemb_error_completion() != 0) return 1;
    if (test_vsim_completion_lifecycle() != 0) return 1;
    if (test_concurrent_register_complete_take() != 0) return 1;

    vector_proxy_completion_cleanup();
    printf("vector_proxy_completion_ut: all tests passed\n");
    return 0;
}
