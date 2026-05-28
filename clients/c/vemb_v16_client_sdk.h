#ifndef __VEMB_V16_CLIENT_SDK_H
#define __VEMB_V16_CLIENT_SDK_H

#include <stdint.h>
#include <unistd.h>
#include <stddef.h>

/* Include shared wire-protocol definitions */
#include "../../src/vemb_v16_protocol.h"

#ifdef __cplusplus
extern "C" {
#endif

/* opaque handle */
typedef struct vemb_v16_client vemb_v16_client_t;

/* =====================================================================
 *  Synchronous Blocking API (high-level, existing)
 * ===================================================================== */

vemb_v16_client_t *vemb_v16_client_create(const char *host,
                                          uint16_t port,
                                          uint32_t dim);
void vemb_v16_client_destroy(vemb_v16_client_t *client);

int vemb_v16_client_vadd(vemb_v16_client_t *client,
                         const char *set_name,
                         const char *elem_name,
                         const float *vector,
                         uint32_t dim);

int vemb_v16_client_vemb_handle(vemb_v16_client_t *client,
                                const char *set_name,
                                const char *elem_name,
                                uint64_t *out_offset,
                                uint32_t *out_bytes,
                                uint32_t *out_dim,
                                uint32_t *out_region_id);

int vemb_v16_client_vemb_vector(vemb_v16_client_t *client,
                                const char *set_name,
                                const char *elem_name,
                                float *out_vector,
                                uint32_t out_cap,
                                uint32_t *out_dim);

/*
 * VSIM_INLINE — compute cosine similarity between the stored vector
 * for (set_name, elem_name) and the provided query_vector.
 * On success, *out_score receives the similarity score.
 * Returns 0 on success, 1 if key not found, -1 on error.
 */
int vemb_v16_client_vsim(vemb_v16_client_t *client,
                         const char *set_name,
                         const char *elem_name,
                         const float *query_vector,
                         uint32_t dim,
                         float *out_score);

int vemb_v16_client_read_vector(vemb_v16_client_t *client,
                                uint64_t offset,
                                uint32_t bytes,
                                float *out_vector,
                                uint32_t out_cap);

/*
 * Pipeline — batch send / batch recv, blocking.
 * Returns 0 on success, -1 on error (caller cannot tell which one failed).
 */
int vemb_v16_client_vadd_pipeline(vemb_v16_client_t *c,
                                  const char **set_names,
                                  const char **elem_names,
                                  const float **vectors,
                                  uint32_t count,
                                  uint32_t max_inflight);

/* Per-response status for vemb pipeline */
typedef struct vemb_v16_pipeline_resp {
    int      status;      /* 0=OK, 1=NOT_FOUND, -1=error */
    uint64_t offset;
    uint32_t bytes;
    uint32_t dim;
    uint32_t region_id;
} vemb_v16_pipeline_resp_t;

int vemb_v16_client_vemb_pipeline(vemb_v16_client_t *c,
                                  const char **set_names,
                                  const char **elem_names,
                                  uint32_t count,
                                  vemb_v16_pipeline_resp_t *out_resps,
                                  uint32_t max_inflight);

/* VSIM_INLINE pipeline — cosine similarity for many (set,elem) pairs.
 * query_vector must be valid for the duration of the call (read-only).
 * out_scores is filled with similarity scores for OK responses.
 * Returns 0 on success, -1 on network/protocol error. */
int vemb_v16_client_vsim_pipeline(vemb_v16_client_t *c,
                                  const char **set_names,
                                  const char **elem_names,
                                  const float *query_vector,
                                  uint32_t count,
                                  float *out_scores,
                                  uint32_t max_inflight);

/*
 * PING — data-plane heartbeat.
 * Returns 0 if server responds OK, -1 on error or timeout.
 */
int vemb_v16_client_ping(vemb_v16_client_t *client);

/*
 * STATS — fetch proxy runtime statistics.
 * Returns 0 on success, -1 on error.
 */
int vemb_v16_client_stats(vemb_v16_client_t *client,
                          vemb_v16_stats_t *out_stats);

/* =====================================================================
 *  Convenience helpers (caller-allocates or standalone)
 * ===================================================================== */

/* Parse a comma-separated vector string: "0.1,0.2,0.3"
 * Returns malloc'd float array on success, NULL on error.
 */
float *vemb_v16_parse_vector_csv(const char *str, uint32_t expected_dim);

/* Parse vector from argv array starting at start_idx.
 * Supports comma-separated single token or individual float tokens.
 * Returns malloc'd float array on success, NULL on error.
 * out_consumed receives the number of argv tokens consumed.
 */
float *vemb_v16_parse_vector_argv(char **argv, int argc, int start_idx,
                                   uint32_t expected_dim, int *out_consumed);

/* Repeat VSIM on the same (set_name, elem_name) pair 'repeat' times.
 * Returns 0 on success, -1 on error.
 * out_score receives the last response's score.
 * out_found receives 1 if last response was OK, 0 if NOT_FOUND.
 */
int vemb_v16_client_vsim_repeat(vemb_v16_client_t *c,
                                 const char *set_name, const char *elem_name,
                                 const float *query_vector, uint32_t repeat,
                                 float *out_score, int *out_found,
                                 uint32_t max_inflight);

/* Repeat VEMB_HANDLE on the same (set_name, elem_name) pair 'repeat' times.
 * Returns 0 on success, -1 on error.
 */
int vemb_v16_client_vemb_repeat(vemb_v16_client_t *c,
                                 const char *set_name, const char *elem_name,
                                 uint32_t repeat, uint32_t max_inflight);

/* Repeat VADD_INLINE on the same (set_name, elem_name) pair 'repeat' times
 * with the same vector.
 * Returns 0 on success, -1 on error.
 */
int vemb_v16_client_vadd_repeat(vemb_v16_client_t *c,
                                 const char *set_name, const char *elem_name,
                                 const float *vector, uint32_t repeat,
                                 uint32_t max_inflight);

/* Internal accessors for thin wrappers (e.g. redis-cli pipeline) */
int vemb_v16_client_fd(const vemb_v16_client_t *client);
uint64_t vemb_v16_client_channel_id(const vemb_v16_client_t *client);

/* =====================================================================
 *  SHM Client API (UDS control + shared-memory ring buffer)
 * ===================================================================== */

typedef struct vemb_v16_client_shm vemb_v16_client_shm_t;

vemb_v16_client_shm_t *vemb_v16_client_shm_create(const char *socket_path,
                                                   uint32_t vector_dim);
void vemb_v16_client_shm_destroy(vemb_v16_client_shm_t *client);

int vemb_v16_client_shm_vadd(vemb_v16_client_shm_t *client,
                              const char *set_name, const char *elem_name,
                              const float *vector, uint32_t dim);

int vemb_v16_client_shm_vemb(vemb_v16_client_shm_t *client,
                              const char *set_name, const char *elem_name,
                              float *out_vector, uint32_t out_cap,
                              uint32_t *out_dim);

int vemb_v16_client_shm_vsim(vemb_v16_client_shm_t *client,
                              const char *set_name, const char *elem_name,
                              const float *query_vector, uint32_t dim,
                              float *out_score);

/* Accessor for vector dimension configured at create time */
uint32_t vemb_v16_client_shm_dim(const vemb_v16_client_shm_t *client);

/* =====================================================================
 *  Async / Buffer-based API (low-level, for event-loop callers)
 * =====================================================================
 *
 * These functions serialize / deserialize VEMB V16 frames into
 * caller-provided buffers.  The caller is responsible for transport
 * (e.g. libevent evbuffer_add, sendmsg, etc.).
 */

/*
 * Build combined key: set_name + '\0' + elem_name
 * Used by the RESP path; native-protocol callers may skip this.
 * Returns 0 on success, -1 on error.
 */
int vemb_v16_build_combined_key(char *out, size_t out_cap,
                                const char *set_name, const char *elem_name,
                                uint32_t *out_len);

/*
 * Serialize a complete HELLO frame into a user-provided buffer.
 * Returns bytes written (>0), or -1 if buf_cap too small.
 */
ssize_t vemb_v16_serialize_hello(void *buf, size_t buf_cap,
                                 uint32_t vector_dim, uint32_t flags);

/*
 * Serialize a complete VADD_INLINE frame into a user-provided buffer.
 * 'key' is the raw key as sent on the wire (already combined if needed).
 * Returns bytes written (>0), or -1 if buf_cap too small.
 */
ssize_t vemb_v16_serialize_vadd(void *buf, size_t buf_cap,
                                uint64_t channel_id, uint32_t req_id,
                                const char *key, uint32_t key_len,
                                const float *vector, uint32_t dim);

/*
 * Serialize a complete VEMB_HANDLE frame into a user-provided buffer.
 * Returns bytes written (>0), or -1 if buf_cap too small.
 */
ssize_t vemb_v16_serialize_vemb(void *buf, size_t buf_cap,
                                uint64_t channel_id, uint32_t req_id,
                                const char *key, uint32_t key_len,
                                uint32_t dim);

/*
 * Serialize a complete VSIM_INLINE frame into a user-provided buffer.
 * 'key' is the raw key as sent on the wire (already combined if needed).
 * Returns bytes written (>0), or -1 if buf_cap too small.
 */
ssize_t vemb_v16_serialize_vsim_inline(void *buf, size_t buf_cap,
                                       uint64_t channel_id, uint32_t req_id,
                                       const char *key, uint32_t key_len,
                                       const float *query_vector, uint32_t dim);

/*
 * Warm-region mmap helpers.
 * Standalone — no client handle required.
 */
int vemb_v16_open_warm_region(const vemb_v16_channel_desc_t *desc,
                              void **out_mapping_addr,
                              size_t *out_mapping_bytes,
                              void **out_mapped_addr,
                              uint64_t *out_region_bytes);
void vemb_v16_close_warm_region(void *mapping_addr, size_t mapping_bytes);

#ifdef __cplusplus
}
#endif

#endif
