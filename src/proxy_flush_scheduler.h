#ifndef __PROXY_FLUSH_SCHEDULER_H
#define __PROXY_FLUSH_SCHEDULER_H

#include <stddef.h>
#include <stdint.h>

typedef struct flush_scheduler {
    size_t batch_limit;
    uint64_t time_limit_us;
} flush_scheduler_t;

typedef struct flush_decision {
    int should_flush;
    int flush_reason_full;
} flush_decision_t;

void flush_scheduler_init(flush_scheduler_t *scheduler, size_t batch_limit, uint64_t time_limit_us);
flush_decision_t flush_scheduler_on_append(flush_scheduler_t *scheduler, size_t count);
flush_decision_t flush_scheduler_on_poll(flush_scheduler_t *scheduler, size_t count, uint64_t age_us);
int flush_scheduler_idle_sleep_us(int processed_any);

#endif /* __PROXY_FLUSH_SCHEDULER_H */
