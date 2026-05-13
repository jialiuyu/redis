#ifndef TEST_RUNTIME_SHIM_H
#define TEST_RUNTIME_SHIM_H

typedef long long mstime_t;

struct redisServer;
extern struct redisServer server;

void test_runtime_reset_server(void);

#endif
