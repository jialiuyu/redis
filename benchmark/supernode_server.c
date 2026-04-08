/*
 * SuperNode Server - 简化的SuperNode服务器实现
 * 
 * 功能：
 * - 监听端口 6388
 * - 接收批量请求（3000-6000个）
 * - 模拟 Proxy Aggregator + SuperNode Worker + SVE2 Gather Load
 * - 返回批量响应
 */

#include "benchmark_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* 协议定义 */
#define PROTO_MAGIC 0xCAC0BEEF
#define PROTO_CMD_BATCH_GET 0x01
#define PROTO_CMD_BATCH_RESPONSE 0x02

/* 服务器配置 */
#define SERVER_PORT 6388
#define SERVER_BACKLOG 128
#define MAX_WORKERS 16
#define MAX_EMBEDDINGS (100 * 1000 * 1000)  /* 1亿 embeddings */

/* 批量请求协议 */
typedef struct {
    uint32_t magic;
    uint32_t command;
    uint32_t num_requests;
    uint32_t embedding_dim;
    uint64_t batch_id;
    uint64_t timestamp_us;
    
    struct {
        uint64_t key_hash;
        char key[64];
    } requests[];
} __attribute__((packed)) batch_request_t;

/* 批量响应协议 */
typedef struct {
    uint32_t magic;
    uint32_t command;
    uint32_t num_responses;
    uint32_t embedding_dim;
    uint64_t batch_id;
    uint64_t process_time_us;
} __attribute__((packed)) batch_response_t;

/* Embedding 存储（模拟 UB.mem）*/
typedef struct {
    float *data;                        /* Embedding 数据 */
    size_t capacity;                    /* 容量 */
    size_t embedding_dim;               /* 维度 */
} embedding_store_t;

/* 服务器上下文 */
typedef struct {
    int listen_sock;
    int running;
    embedding_store_t *store;
    
    /* 统计 */
    atomic_uint_fast64_t total_requests;
    atomic_uint_fast64_t total_batches;
    atomic_uint_fast64_t total_bytes_sent;
    atomic_uint_fast64_t total_bytes_received;
    
} server_context_t;

static server_context_t *global_server = NULL;

/* 信号处理 */
static void signal_handler(int sig) {
    if (global_server) {
        printf("\nReceived signal %d, shutting down...\n", sig);
        global_server->running = 0;
    }
}

/* 初始化 Embedding 存储 */
embedding_store_t *embedding_store_init(size_t capacity, size_t embedding_dim) {
    embedding_store_t *store = malloc(sizeof(embedding_store_t));
    if (!store) return NULL;
    
    store->capacity = capacity;
    store->embedding_dim = embedding_dim;
    
    /* 分配大内存（模拟 UB.mem）*/
    size_t total_size = capacity * embedding_dim * sizeof(float);
    
    printf("Allocating embedding store: %.2f GB...\n", total_size / (1024.0 * 1024.0 * 1024.0));
    
    /* 使用 mmap 分配大页内存 */
    store->data = mmap(NULL, total_size, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | 0x20 /* MAP_ANONYMOUS */, -1, 0);
    
    if (store->data == MAP_FAILED) {
        fprintf(stderr, "Failed to allocate embedding store: %s\n", strerror(errno));
        free(store);
        return NULL;
    }
    
    /* 初始化随机数据 */
    printf("Initializing embeddings...\n");
    
    for (size_t i = 0; i < capacity; i++) {
        float *emb = &store->data[i * embedding_dim];
        generate_random_embedding(i, emb);
        
        if ((i + 1) % 10000000 == 0) {
            printf("  Initialized %zu / %zu embeddings\n", i + 1, capacity);
        }
    }
    
    printf("✅ Embedding store initialized\n\n");
    return store;
}

/* 清理 Embedding 存储 */
void embedding_store_cleanup(embedding_store_t *store) {
    if (!store) return;
    
    if (store->data && store->data != MAP_FAILED) {
        size_t total_size = store->capacity * store->embedding_dim * sizeof(float);
        munmap(store->data, total_size);
    }
    
    free(store);
}

/* 获取 Embedding */
float *embedding_store_get(embedding_store_t *store, uint64_t emb_id) {
    if (!store || emb_id >= store->capacity) return NULL;
    
    return &store->data[emb_id * store->embedding_dim];
}

/* 模拟 SVE2 Gather Load 批量读取 */
static void sve2_batch_gather_load_sim(embedding_store_t *store,
                                       uint64_t *emb_ids,
                                       size_t num_ids,
                                       float *results) {
    /* 模拟 SVE2 批量并行读取 */
    for (size_t i = 0; i < num_ids; i++) {
        uint64_t emb_id = emb_ids[i] % store->capacity;
        float *emb = embedding_store_get(store, emb_id);
        
        if (emb) {
            memcpy(&results[i * store->embedding_dim], emb,
                   store->embedding_dim * sizeof(float));
        } else {
            /* 填充零 */
            memset(&results[i * store->embedding_dim], 0,
                   store->embedding_dim * sizeof(float));
        }
    }
}

/* 处理批量请求 */
static int handle_batch_request(server_context_t *ctx, int client_sock,
                                batch_request_t *request, size_t request_size) {
    uint64_t start_time = get_time_us();
    
    /* 验证请求 */
    if (request->magic != PROTO_MAGIC || request->command != PROTO_CMD_BATCH_GET) {
        fprintf(stderr, "Invalid request\n");
        return -1;
    }
    
    printf("Processing batch: %u requests, batch_id=%llu\n",
           request->num_requests, (unsigned long long)request->batch_id);
    
    /* 提取 Embedding IDs */
    uint64_t *emb_ids = malloc(request->num_requests * sizeof(uint64_t));
    if (!emb_ids) return -1;
    
    for (uint32_t i = 0; i < request->num_requests; i++) {
        emb_ids[i] = request->requests[i].key_hash;
    }
    
    /* 分配结果缓冲区 */
    size_t embedding_data_size = request->num_requests * request->embedding_dim * sizeof(float);
    float *embeddings = malloc(embedding_data_size);
    if (!embeddings) {
        free(emb_ids);
        return -1;
    }
    
    /* SVE2 批量 Gather Load（模拟）*/
    sve2_batch_gather_load_sim(ctx->store, emb_ids, request->num_requests, embeddings);
    
    free(emb_ids);
    
    uint64_t end_time = get_time_us();
    uint64_t process_time = end_time - start_time;
    
    /* 构造响应 */
    batch_response_t response;
    response.magic = PROTO_MAGIC;
    response.command = PROTO_CMD_BATCH_RESPONSE;
    response.num_responses = request->num_requests;
    response.embedding_dim = request->embedding_dim;
    response.batch_id = request->batch_id;
    response.process_time_us = process_time;
    
    /* 发送响应头 */
    ssize_t sent = send(client_sock, &response, sizeof(response), 0);
    if (sent != sizeof(response)) {
        fprintf(stderr, "Failed to send response header: %s\n", strerror(errno));
        free(embeddings);
        return -1;
    }
    
    /* 发送 Embedding 数据 */
    sent = send(client_sock, embeddings, embedding_data_size, 0);
    if (sent != (ssize_t)embedding_data_size) {
        fprintf(stderr, "Failed to send embedding data: %s\n", strerror(errno));
        free(embeddings);
        return -1;
    }
    
    free(embeddings);
    
    /* 更新统计 */
    atomic_fetch_add(&ctx->total_requests, request->num_requests);
    atomic_fetch_add(&ctx->total_batches, 1);
    atomic_fetch_add(&ctx->total_bytes_received, request_size);
    atomic_fetch_add(&ctx->total_bytes_sent, sizeof(response) + embedding_data_size);
    
    printf("  Completed in %llu μs, sent %.2f MB\n",
           (unsigned long long)process_time,
           embedding_data_size / (1024.0 * 1024.0));
    
    return 0;
}

/* 客户端处理线程 */
void *client_handler_thread(void *arg) {
    int client_sock = *(int *)arg;
    free(arg);
    
    server_context_t *ctx = global_server;
    
    printf("Client connected: socket %d\n", client_sock);
    
    /* 接收和处理请求 */
    while (ctx->running) {
        /* 接收请求头 */
        batch_request_t header;
        ssize_t received = recv(client_sock, &header, sizeof(batch_request_t), MSG_PEEK);
        
        if (received <= 0) {
            if (received < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                fprintf(stderr, "Client disconnected: %s\n", strerror(errno));
            }
            break;
        }
        
        /* 计算完整请求大小 */
        size_t request_size = sizeof(batch_request_t) + 
                             header.num_requests * sizeof(header.requests[0]);
        
        /* 分配缓冲区 */
        batch_request_t *request = malloc(request_size);
        if (!request) break;
        
        /* 接收完整请求 */
        received = recv(client_sock, request, request_size, MSG_WAITALL);
        if (received != (ssize_t)request_size) {
            fprintf(stderr, "Failed to receive full request: %s\n", strerror(errno));
            free(request);
            break;
        }
        
        /* 处理请求 */
        if (handle_batch_request(ctx, client_sock, request, request_size) < 0) {
            free(request);
            break;
        }
        
        free(request);
    }
    
    close(client_sock);
    printf("Client disconnected: socket %d\n", client_sock);
    
    return NULL;
}

/* 启动服务器 */
int start_server(int port) {
    global_server = calloc(1, sizeof(server_context_t));
    if (!global_server) {
        fprintf(stderr, "Failed to allocate server context\n");
        return -1;
    }
    
    /* 初始化 Embedding 存储 */
    global_server->store = embedding_store_init(MAX_EMBEDDINGS, EMBEDDING_DIM);
    if (!global_server->store) {
        free(global_server);
        return -1;
    }
    
    /* 创建监听 socket */
    global_server->listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (global_server->listen_sock < 0) {
        fprintf(stderr, "Failed to create socket: %s\n", strerror(errno));
        embedding_store_cleanup(global_server->store);
        free(global_server);
        return -1;
    }
    
    /* 设置 socket 选项 */
    int opt = 1;
    if (setsockopt(global_server->listen_sock, SOL_SOCKET, SO_REUSEADDR,
                   &opt, sizeof(opt)) < 0) {
        fprintf(stderr, "Failed to set SO_REUSEADDR: %s\n", strerror(errno));
    }
    
    /* 绑定地址 */
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);
    
    if (bind(global_server->listen_sock, (struct sockaddr *)&server_addr,
             sizeof(server_addr)) < 0) {
        fprintf(stderr, "Failed to bind to port %d: %s\n", port, strerror(errno));
        close(global_server->listen_sock);
        embedding_store_cleanup(global_server->store);
        free(global_server);
        return -1;
    }
    
    /* 开始监听 */
    if (listen(global_server->listen_sock, SERVER_BACKLOG) < 0) {
        fprintf(stderr, "Failed to listen: %s\n", strerror(errno));
        close(global_server->listen_sock);
        embedding_store_cleanup(global_server->store);
        free(global_server);
        return -1;
    }
    
    global_server->running = 1;
    
    /* 初始化统计 */
    atomic_init(&global_server->total_requests, 0);
    atomic_init(&global_server->total_batches, 0);
    atomic_init(&global_server->total_bytes_sent, 0);
    atomic_init(&global_server->total_bytes_received, 0);
    
    printf("\n========================================\n");
    printf("SuperNode Server Started\n");
    printf("========================================\n");
    printf("Listening on port: %d\n", port);
    printf("Max embeddings: %d\n", MAX_EMBEDDINGS);
    printf("Embedding dimension: %d\n", EMBEDDING_DIM);
    printf("========================================\n\n");
    
    /* 接受连接 */
    while (global_server->running) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        int client_sock = accept(global_server->listen_sock,
                                (struct sockaddr *)&client_addr, &client_len);
        
        if (client_sock < 0) {
            if (errno == EINTR) continue;
            fprintf(stderr, "Failed to accept connection: %s\n", strerror(errno));
            continue;
        }
        
        /* 创建客户端处理线程 */
        int *sock_ptr = malloc(sizeof(int));
        *sock_ptr = client_sock;
        
        pthread_t thread;
        if (pthread_create(&thread, NULL, client_handler_thread, sock_ptr) != 0) {
            fprintf(stderr, "Failed to create client thread: %s\n", strerror(errno));
            close(client_sock);
            free(sock_ptr);
            continue;
        }
        
        pthread_detach(thread);
    }
    
    /* 清理 */
    close(global_server->listen_sock);
    embedding_store_cleanup(global_server->store);
    
    /* 打印统计 */
    printf("\n========================================\n");
    printf("Server Statistics\n");
    printf("========================================\n");
    printf("Total requests: %llu\n",
           (unsigned long long)atomic_load(&global_server->total_requests));
    printf("Total batches: %llu\n",
           (unsigned long long)atomic_load(&global_server->total_batches));
    printf("Total bytes received: %.2f MB\n",
           atomic_load(&global_server->total_bytes_received) / (1024.0 * 1024.0));
    printf("Total bytes sent: %.2f MB\n",
           atomic_load(&global_server->total_bytes_sent) / (1024.0 * 1024.0));
    printf("========================================\n\n");
    
    free(global_server);
    global_server = NULL;
    
    return 0;
}

int main(int argc, char *argv[]) {
    int port = SERVER_PORT;
    
    /* 解析参数 */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0) {
            printf("Usage: %s [options]\n", argv[0]);
            printf("Options:\n");
            printf("  --port N    Server port (default: %d)\n", SERVER_PORT);
            printf("  --help      Show this help\n");
            return 0;
        }
    }
    
    /* 设置信号处理 */
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    /* 启动服务器 */
    return start_server(port);
}
