/*
 * VEMB V16 Client SDK Example
 *
 * Build:
 *   gcc -I../src vemb_v16_sdk_example.c ../src/vemb_v16_client_sdk.c ../src/vemb_v16_net.c -o vemb_v16_sdk_example
 *
 * Run (requires proxy listening on 127.0.0.1:6391):
 *   ./vemb_v16_sdk_example
 */

#include "vemb_v16_client_sdk.h"
#include <stdio.h>
#include <string.h>

#define DIM 10

int main(int argc, char **argv)
{
    (void)argc; (void)argv;

    const char *host = "127.0.0.1";
    uint16_t port = 6391;

    /* 1. create client */
    vemb_v16_client_t *client = vemb_v16_client_create(host, port, DIM);
    if (!client) {
        fprintf(stderr, "failed to connect to %s:%u\n", host, port);
        return 1;
    }
    printf("connected to %s:%u, dim=%u\n", host, port, DIM);

    /* 2. VADD — write a vector */
    float vec[DIM];
    for (uint32_t i = 0; i < DIM; i++) vec[i] = (float)(i + 1);

    printf("\n--- VADD ---\n");
    if (vemb_v16_client_vadd(client, "myset", "elem1", vec, DIM) == 0) {
        printf("VADD myset elem1 -> OK\n");
    } else {
        printf("VADD failed\n");
    }

    /* 3. VEMB handle only (zero-copy path) */
    printf("\n--- VEMB handle ---\n");
    uint64_t offset;
    uint32_t bytes, dim_out, region_id;
    int rc = vemb_v16_client_vemb_handle(client, "myset", "elem1",
                                         &offset, &bytes, &dim_out, &region_id);
    if (rc == 0) {
        printf("handle: offset=%llu bytes=%u dim=%u region=%u\n",
               (unsigned long long)offset, bytes, dim_out, region_id);

        /* read vector from mmap'd warm region manually */
        float buf[DIM];
        if (vemb_v16_client_read_vector(client, offset, bytes,
                                        buf, DIM) == 0) {
            printf("read_vector: ");
            for (uint32_t i = 0; i < dim_out; i++) printf("%.1f ", buf[i]);
            printf("\n");
        }
    } else if (rc == 1) {
        printf("VEMB: not found\n");
    } else {
        printf("VEMB handle query failed\n");
    }

    /* 4. VEMB full vector (convenience path) */
    printf("\n--- VEMB vector ---\n");
    float out[DIM];
    uint32_t out_dim = 0;
    rc = vemb_v16_client_vemb_vector(client, "myset", "elem1",
                                     out, DIM, &out_dim);
    if (rc == 0) {
        printf("vector (%u dims): ", out_dim);
        for (uint32_t i = 0; i < out_dim; i++) printf("%.1f ", out[i]);
        printf("\n");
    } else if (rc == 1) {
        printf("VEMB: not found\n");
    } else {
        printf("VEMB vector query failed\n");
    }

    /* 5. miss case */
    printf("\n--- VEMB miss ---\n");
    rc = vemb_v16_client_vemb_vector(client, "myset", "nonexist",
                                     out, DIM, &out_dim);
    if (rc == 1) {
        printf("VEMB myset nonexist -> (nil)\n");
    } else {
        printf("unexpected result: rc=%d\n", rc);
    }

    /* 6. PING — heartbeat */
    printf("\n--- PING ---\n");
    if (vemb_v16_client_ping(client) == 0) {
        printf("PING -> OK\n");
    } else {
        printf("PING failed\n");
    }

    /* 7. STATS — proxy diagnostics */
    printf("\n--- STATS ---\n");
    vemb_v16_stats_t stats;
    memset(&stats, 0, sizeof(stats));
    if (vemb_v16_client_stats(client, &stats) == 0) {
        printf("total_requests=%lu vadd=%lu vemb=%lu active_channels=%lu\n",
               (unsigned long)stats.total_requests,
               (unsigned long)stats.vadd_requests,
               (unsigned long)stats.vemb_requests,
               (unsigned long)stats.active_channels);
    } else {
        printf("STATS failed\n");
    }

    /* 8. Pipeline — batch query */
    printf("\n--- Pipeline ---\n");
    const char *sets[3]  = {"myset", "myset", "myset"};
    const char *elems[3] = {"elem1", "elem1", "elem1"};
    vemb_v16_pipeline_resp_t resps[3];
    memset(resps, 0, sizeof(resps));
    if (vemb_v16_client_vemb_pipeline(client, sets, elems, 3, resps, 16) == 0) {
        for (int i = 0; i < 3; i++) {
            printf("resp[%d]: status=%d offset=%llu bytes=%u\n",
                   i, resps[i].status,
                   (unsigned long long)resps[i].offset,
                   resps[i].bytes);
        }
    } else {
        printf("Pipeline failed\n");
    }

    /* 9. destroy */
    vemb_v16_client_destroy(client);
    printf("\ndisconnected.\n");
    return 0;
}
