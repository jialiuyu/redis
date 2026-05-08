#include "proxy_flush_scheduler.h"
#include "macro.h"

void flush_scheduler_init(flush_scheduler_t *scheduler, size_t batch_limit, uint64_t time_limit_us) {
    RETURN_IF(!scheduler);

    scheduler->batch_limit = batch_limit;
    scheduler->time_limit_us = time_limit_us;
}

flush_decision_t flush_scheduler_on_append(flush_scheduler_t *scheduler, size_t count) {
    flush_decision_t decision = {0, 0};
    RETURN_IF(!scheduler || count == 0, decision);

    if (count >= scheduler->batch_limit) {
        decision.should_flush = 1;
        decision.flush_reason_full = 1;
    }

    return decision;
}

flush_decision_t flush_scheduler_on_poll(flush_scheduler_t *scheduler, size_t count, uint64_t age_us) {
    flush_decision_t decision = {0, 0};
    RETURN_IF(!scheduler || count == 0, decision);

    if (count >= scheduler->batch_limit) {
        decision.should_flush = 1;
        decision.flush_reason_full = 1;
    } else if (age_us >= scheduler->time_limit_us) {
        decision.should_flush = 1;
        decision.flush_reason_full = 0;
    }

    return decision;
}

int flush_scheduler_idle_sleep_us(int processed_any) {
    return processed_any ? 1 : 10;
}
