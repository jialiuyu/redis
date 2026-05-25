#ifndef __VEMB_V16_PROXY_THREAD_H
#define __VEMB_V16_PROXY_THREAD_H

#include "vemb_v16_proxy.h"

typedef struct {
    vemb_v16_proxy_t *proxy;
    int exit_code;
} vemb_v16_proxy_thread_result_t;

/* Launch vemb_v16_proxy_run() in a detached thread, return immediately */
int vemb_v16_proxy_run_in_thread(vemb_v16_proxy_t *proxy,
                                 vemb_v16_proxy_thread_result_t **out_result);

/* Free the result structure */
void vemb_v16_proxy_thread_wait(vemb_v16_proxy_thread_result_t *result);

#endif
