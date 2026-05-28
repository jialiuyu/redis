#ifndef __VEMB_V16_CLI_SHM_H
#define __VEMB_V16_CLI_SHM_H

#include <stdint.h>

int vemb_v16_cli_shm_init(const char *socket_path, uint32_t vector_dim);
void vemb_v16_cli_shm_cleanup(void);
int vemb_v16_cli_shm_vemb(int argc, char **argv, int raw_output);
int vemb_v16_cli_shm_vadd(int argc, char **argv);
int vemb_v16_cli_shm_vsim(int argc, char **argv);

#endif
