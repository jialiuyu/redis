#ifndef __RING_BUFFER_MGR_H
#define __RING_BUFFER_MGR_H

#include "ring_buffer.h"

#include <stddef.h>

int ring_buffer_mgr_init(size_t workers_per_node, size_t ring_buffer_size);
void ring_buffer_mgr_shutdown(void);

int ring_buffer_mgr_ensure_supernodes(size_t num_supernodes);
ring_buffer_t *ring_buffer_mgr_get(int supernode_id, int worker_id);

#endif /* __RING_BUFFER_MGR_H */
