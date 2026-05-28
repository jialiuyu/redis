#include "vemb_v16_cli_shm.h"
#include "vemb_v16_protocol.h"
#include "../clients/c/vemb_v16_client_sdk.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static vemb_v16_client_shm_t *g_client = NULL;

int vemb_v16_cli_shm_init(const char *socket_path, uint32_t vector_dim)
{
    if (g_client) return 0;
    g_client = vemb_v16_client_shm_create(socket_path, vector_dim);
    return g_client ? 0 : -1;
}

void vemb_v16_cli_shm_cleanup(void)
{
    if (g_client) {
        vemb_v16_client_shm_destroy(g_client);
        g_client = NULL;
    }
}

int vemb_v16_cli_shm_vemb(int argc, char **argv, int raw_output)
{
    if (!g_client || argc < 3) return -1;

    float *vec = malloc(4096 * sizeof(float));
    if (!vec) return -1;

    uint32_t out_dim = 0;
    int rc = vemb_v16_client_shm_vemb(g_client, argv[1], argv[2],
                                       vec, 4096, &out_dim);

    if (rc == 1) {
        if (!raw_output) printf("(nil)\n");
        free(vec);
        return 0;
    }
    if (rc != 0) {
        if (!raw_output) printf("(error) VEMB failed\n");
        free(vec);
        return -1;
    }

    if (!raw_output) {
        printf("OK dim=%u\n", out_dim);
        for (uint32_t i = 0; i < out_dim; i++) {
            printf("%f\n", vec[i]);
        }
    }
    free(vec);
    return 0;
}

int vemb_v16_cli_shm_vadd(int argc, char **argv)
{
    if (!g_client || argc < 4) return -1;

    uint32_t dim = vemb_v16_client_shm_dim(g_client);
    float *vec = vemb_v16_parse_vector_csv(argv[3], dim);
    if (!vec) {
        fprintf(stderr, "vemb_v16_cli_shm: vector parse failed\n");
        return -1;
    }

    int rc = vemb_v16_client_shm_vadd(g_client, argv[1], argv[2],
                                       vec, dim);
    free(vec);

    if (rc == 0) {
        printf("OK\n");
        return 0;
    } else {
        printf("(error) VADD failed\n");
        return -1;
    }
}

int vemb_v16_cli_shm_vsim(int argc, char **argv)
{
    if (!g_client || argc < 4) return -1;

    uint32_t dim = vemb_v16_client_shm_dim(g_client);
    float *vec = vemb_v16_parse_vector_csv(argv[3], dim);
    if (!vec) {
        fprintf(stderr, "vemb_v16_cli_shm: VSIM vector parse failed\n");
        return -1;
    }

    float score = 0.0f;
    int rc = vemb_v16_client_shm_vsim(g_client, argv[1], argv[2],
                                       vec, dim, &score);
    free(vec);

    if (rc == 1) {
        printf("(nil)\n");
        return 0;
    }
    if (rc != 0) {
        printf("(error) VSIM failed\n");
        return -1;
    }

    printf("%.6f\n", score);
    return 0;
}
