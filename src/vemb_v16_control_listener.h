#ifndef VEMB_V16_CONTROL_LISTENER_H
#define VEMB_V16_CONTROL_LISTENER_H

/*
 * Control-plane fd dispatcher for redis-server sniff integration.
 *
 * When the sniff hijack in vemb_v16_server_integration.c detects a VEMB
 * frame whose type is NOT HELLO (i.e. it's a control frame such as
 * TOPOLOGY_SET/GET, RANGE_*, SCALEOUT_ACK, STATS, etc.), it hands the fd
 * to this module.  We spawn a detached pthread that calls
 * vemb_v16_tcp_handle_fd() once (single-shot: read frame, dispatch, write
 * reply, close fd) and exits.
 *
 * Why one thread per fd:
 *   - top_ctl opens a fresh TCP connection per control operation (no
 *     keep-alive), so the live control-fd count is bounded by the number
 *     of concurrent invocations (typically <10 for human-driven ops).
 *   - vemb_v16_tcp_handle_fd forces the fd to blocking mode and uses
 *     blocking writev with a 10s timeout; isolating each call in its own
 *     thread prevents any single slow client from blocking others.
 *   - The redis main thread and proxy IO threads are not affected.
 *
 * VEMB_V16_CONTROL_LISTENER_MAX_CONCURRENT caps the number of in-flight
 * control threads to prevent accidental thread explosion.
 */

int vemb_v16_control_inject_fd(int fd);

#endif /* VEMB_V16_CONTROL_LISTENER_H */
