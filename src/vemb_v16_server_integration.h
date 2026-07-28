#ifndef __VEMB_V16_SERVER_INTEGRATION_H
#define __VEMB_V16_SERVER_INTEGRATION_H

struct connection;  /* forward declaration — avoids pulling connection.h here */

int vemb_v16_server_integration_init(void);
void vemb_v16_server_integration_shutdown(void);
int vemb_v16_sniff_and_handoff(struct connection *conn);

/* Reflects --vemb-v16-cross-node-aeron config. Other translation units
 * (proxy.c, supernode.c) cannot include server.h directly. */
int vemb_v16_cross_node_aeron_enabled(void);

#endif
