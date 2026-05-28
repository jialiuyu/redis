#ifndef __VEMB_V16_CLI_TCP_H
#define __VEMB_V16_CLI_TCP_H

#include <stdint.h>

int vemb_v16_cli_tcp_init(const char *host, uint16_t port, uint32_t dim);
int vemb_v16_cli_tcp_vadd(int argc, char **argv);
int vemb_v16_cli_tcp_vemb(int argc, char **argv, int raw_output);
int vemb_v16_cli_tcp_vemb_pipeline(int argc, char **argv, int raw_output, int repeat);
int vemb_v16_cli_tcp_vsim(int argc, char **argv, int raw_output);
int vemb_v16_cli_tcp_vsim_pipeline(int argc, char **argv, int raw_output, int repeat);
void vemb_v16_cli_tcp_cleanup(void);

#endif
