/*
 * Copyright (C) 2026 Redis Labs Ltd.
 *
 * This file is part of memtier_benchmark.
 *
 * memtier_benchmark is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2.
 *
 * memtier_benchmark is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "vemb_v16_aeron_runner.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/time.h>
#include <atomic>
#include <vector>

#include "vemb_v16_protocol.h"

/* Transport mode: "aeron" (UDS+SHM, local) or "aeron-cross-node" (TCP attach + shmdev).
 * Set by main() from --vemb-v16-transport via vemb_v16_aeron_set_transport().
 * Default = "aeron" preserves existing loopback behavior. */
static std::string g_aeron_transport_mode = "aeron";
static std::string g_aeron_remote_endpoint;  /* "host:port" for cross-node */

void vemb_v16_aeron_set_transport(const std::string &mode,
                                  const std::string &endpoint) {
    g_aeron_transport_mode = mode;
    g_aeron_remote_endpoint = endpoint;
}

extern "C" {
/* SDK ships the aeron (UDS + SHM SPSC ring) transport as opaque handles.
 * No C11 <stdatomic.h> dependency leaks into this C++ translation unit. */
#include "vemb_v16_client_sdk.h"
}

/* ------------------------------------------------------------------ */
/* Worker state                                                       */
/* ------------------------------------------------------------------ */

struct pending_op {
    uint32_t     req_id;
    int          is_set;     /* 1=set/vadd/vrem, 0=get/vemb/vsim */
    struct timeval sent_time;
    uint32_t     bytes_tx;
};

struct worker_arg {
    benchmark_config    *cfg;
    object_generator    *obj_gen;
    uint32_t             worker_id;
    /* channels — SDK owns the handles; we hold pointers */
    std::vector<vemb_v16_aeron_channel_t *> channels;
    /* ratio bookkeeping for mixed workloads */
    unsigned long set_ratio_count;
    unsigned long get_ratio_count;
    /* pending slots — pipeline depth per channel */
    std::vector<std::vector<pending_op>> pending;  /* [ch][slot] */
    std::vector<uint32_t>                pending_head;
    std::vector<uint32_t>                pending_tail;
    std::vector<uint32_t>                pending_count;
    /* stats — pointer because run_stats has no default ctor */
    run_stats    *stats;
    /* coordination */
    std::atomic<bool> *stop;
    std::atomic<bool>  done;
    std::atomic<unsigned long long> ops_done;
    std::atomic<unsigned long long> ops_fail;
    /* budget (-n requests total / N workers) */
    unsigned long long budget;
};

static uint64_t now_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* Map cfg->key_pattern[idx] to object_generator iterator type.
 * Mirrors client.h:151 obj_iter_type — sequential patterns use SET/GET
 * iterators (stateful counters), R/G/Z are stateless distributions. */
#define AERON_SET_CMD_IDX 0
#define AERON_GET_CMD_IDX 2
static int obj_iter_type(benchmark_config *cfg, unsigned char index) {
    if (cfg->key_pattern[index] == 'R') return OBJECT_GENERATOR_KEY_RANDOM;
    if (cfg->key_pattern[index] == 'G') return OBJECT_GENERATOR_KEY_GAUSSIAN;
    if (cfg->key_pattern[index] == 'Z') return OBJECT_GENERATOR_KEY_ZIPFIAN;
    if (index == key_pattern_set) return OBJECT_GENERATOR_KEY_SET_ITER;
    return OBJECT_GENERATOR_KEY_GET_ITER;
}

/* Fill a vector deterministically — same intent as vemb_v16_bench.c fill_vector.
 * We use a simple LCG per index to avoid runtime randomness cost on hot path. */
static void fill_vector_for_index(float *vec, uint32_t dim, uint32_t global_id) {
    uint32_t s = global_id * 2654435761u + 12345u;
    for (uint32_t i = 0; i < dim; i++) {
        s = s * 1103515245u + 12345u;
        uint32_t bits = (s >> 9) | 0x40000000u;  /* finite non-zero */
        float f;
        memcpy(&f, &bits, sizeof(f));
        vec[i] = f * 0.001f;
    }
}

/* Build a vemb_v16_req_t for VADD/VREM/VEMB/VSIM. Mirrors vemb_v16_bench.c:759
 * prepare_req + protocol.cpp:1470 write_command_set/get. */
static size_t build_req(worker_arg *w, unsigned long long global_op_idx,
                        uint8_t op, const char *key, int key_len,
                        vemb_v16_req_t *req, uint32_t *out_actual_key_len) {
    uint32_t actual_key_len = (key_len < (int)VEMB_V16_MAX_KEY_LEN)
        ? (uint32_t)key_len : VEMB_V16_MAX_KEY_LEN - 1;
    memset(req, 0, sizeof(*req));
    req->op = op;
    req->req_id = (uint32_t)(global_op_idx + 1);
    req->key_hash = vemb_v16_xxh3_64_str(key, actual_key_len);
    req->key_len = actual_key_len;
    req->dim = w->cfg->vemb_v16_dim;
    memcpy(req->key, key, actual_key_len);

    if (op == VEMB_V16_OP_VADD) {
        req->vector_bytes = w->cfg->vemb_v16_dim * sizeof(float);
        fill_vector_for_index(req->vector, w->cfg->vemb_v16_dim, (uint32_t)global_op_idx);
        *out_actual_key_len = actual_key_len;
        return vemb_v16_req_inline_len(req->vector_bytes);
    }
    if (op == VEMB_V16_OP_VREM) {
        req->dim = 0;
        req->vector_bytes = 0;
        *out_actual_key_len = actual_key_len;
        return vemb_v16_req_handle_len();
    }
    if (op == VEMB_V16_OP_VSIM_INLINE) {
        req->vector_bytes = w->cfg->vemb_v16_dim * sizeof(float);
        fill_vector_for_index(req->vector, w->cfg->vemb_v16_dim,
                              (uint32_t)global_op_idx + 0x9e3779b9u);
        *out_actual_key_len = actual_key_len;
        return vemb_v16_req_inline_len(req->vector_bytes);
    }
    /* VEMB_INLINE / VEMB_HANDLE: key-only request on the wire (handle_len),
     * but the server still validates vector_bytes == dim*sizeof(float) for
     * shape consistency (else returns shape_mismatch). Match vemb_v16_bench's
     * prepare_req convention by setting vector_bytes even though we don't
     * send the payload. */
    req->dim = w->cfg->vemb_v16_dim;
    req->vector_bytes = w->cfg->vemb_v16_dim * sizeof(float);
    *out_actual_key_len = actual_key_len;
    return vemb_v16_req_handle_len();
}

/* Decide next op for this worker. Mirrors client.cpp:365 create_request. */
static uint8_t pick_next_op(worker_arg *w) {
    /* vsim/vrem override */
    if (w->cfg->vemb_v16_vsim) return VEMB_V16_OP_VSIM_INLINE;
    if (w->cfg->vemb_v16_vrem) return VEMB_V16_OP_VREM;

    /* pure set (--ratio=1:0) */
    if (w->cfg->ratio.a > 0 && w->cfg->ratio.b == 0) return VEMB_V16_OP_VADD;
    /* pure get (--ratio=0:1) — default to VEMB_HANDLE per memtier help text */
    if (w->cfg->ratio.b > 0 && w->cfg->ratio.a == 0) return VEMB_V16_OP_VEMB_HANDLE;

    /* mixed — rotate by ratio */
    if (w->set_ratio_count < w->cfg->ratio.a) {
        w->set_ratio_count++;
        if (w->set_ratio_count >= w->cfg->ratio.a &&
            w->get_ratio_count >= w->cfg->ratio.b) {
            w->set_ratio_count = 0;
            w->get_ratio_count = 0;
        }
        return VEMB_V16_OP_VADD;
    }
    if (w->get_ratio_count < w->cfg->ratio.b) {
        w->get_ratio_count++;
        if (w->set_ratio_count >= w->cfg->ratio.a &&
            w->get_ratio_count >= w->cfg->ratio.b) {
            w->set_ratio_count = 0;
            w->get_ratio_count = 0;
        }
        return VEMB_V16_OP_VEMB_HANDLE;
    }
    /* fallthrough — shouldn't happen */
    w->set_ratio_count = 0;
    w->get_ratio_count = 0;
    return VEMB_V16_OP_VEMB_HANDLE;
}

static int is_set_op(uint8_t op) {
    return op == VEMB_V16_OP_VADD || op == VEMB_V16_OP_VREM;
}

static void *worker_main(void *arg) {
    worker_arg *w = (worker_arg *)arg;
    uint32_t n_ch = (uint32_t)w->channels.size();
    uint32_t pipeline = w->cfg->pipeline > 0 ? w->cfg->pipeline : 1;

    /* struct timeval timezone-aware for run_stats::update_*_op. */
    struct timeval start_tv;
    gettimeofday(&start_tv, NULL);
    w->stats->set_start_time(&start_tv);

    unsigned long long op_idx = 0;
    unsigned long long budget = w->budget;  /* 0 = unlimited (--test-time) */
    uint32_t next_ch = 0;
    vemb_v16_req_t req_buf;
    vemb_v16_resp_t resp_buf;
    /* Scratch buffer for VEMB_HANDLE dereference — large enough for any
     * dim up to VEMB_V16_MAX_DIM. Lives on the worker stack. */
    float vec_scratch[VEMB_V16_MAX_DIM];
    uint64_t debug_publish_ok = 0, debug_publish_fail = 0;
    uint64_t debug_poll_zero = 0, debug_poll_got = 0;
    uint64_t debug_loop_count = 0;
    uint64_t debug_status_ok = 0, debug_status_notfound = 0, debug_status_err = 0, debug_status_other = 0;
    uint64_t debug_handle_deref_ok = 0, debug_handle_deref_fail = 0;

    while (!w->stop->load(std::memory_order_acquire)) {
        if (budget > 0 && op_idx >= budget) {
            /* drain then exit */
            break;
        }
        debug_loop_count++;

        /* ---- publish phase: round-robin channels, fill each to pipeline.
         * Keep pipeline full so the server-side proxy never sees an empty
         * req ring (which triggers its 10ms epoll_wait sleep). ---- */
        int published_any = 0;
        for (uint32_t k = 0; k < n_ch; k++) {
            uint32_t ch = (next_ch + k) % n_ch;
            if (w->pending_count[ch] >= pipeline) continue;
            if (budget > 0 && op_idx >= budget) break;
            if (w->stop->load(std::memory_order_acquire)) break;

            /* pick op + key */
            uint8_t op = pick_next_op(w);
            int iter = is_set_op(op)
                ? obj_iter_type(w->cfg, AERON_SET_CMD_IDX)
                : obj_iter_type(w->cfg, AERON_GET_CMD_IDX);
            unsigned long long key_index = w->obj_gen->get_key_index(iter);
            w->obj_gen->generate_key(key_index);

            uint32_t actual_key_len = 0;
            size_t req_len = build_req(w, op_idx, op,
                                       w->obj_gen->get_key(),
                                       w->obj_gen->get_key_len(),
                                       &req_buf, &actual_key_len);
            req_buf.channel_id = vemb_v16_aeron_channel_id(w->channels[ch]);

            int pub_rc = vemb_v16_aeron_publish_request(w->channels[ch],
                                                        &req_buf, (uint32_t)req_len);
            if (pub_rc != 0) {
                debug_publish_fail++;
                continue;
            }
            debug_publish_ok++;
            /* record pending */
            uint32_t slot = w->pending_tail[ch];
            struct timeval now;
            gettimeofday(&now, NULL);
            w->pending[ch][slot].req_id = req_buf.req_id;
            w->pending[ch][slot].is_set = is_set_op(op);
            w->pending[ch][slot].sent_time = now;
            w->pending[ch][slot].bytes_tx = (uint32_t)req_len;
            w->pending_tail[ch] = (slot + 1) % pipeline;
            w->pending_count[ch]++;
            op_idx++;
            published_any = 1;
            next_ch = (ch + 1) % n_ch;
        }

        /* ---- poll phase: take AT MOST ONE response per channel per iter.
         * Draining all responses in one pass lets the proxy's req ring run
         * empty between batches and the proxy enters its 10ms sleep — which
         * becomes the per-op latency. Mirrors vemb_v16_bench's
         * recv_channel_resp-one-then-publish-one pattern. ---- */
        int polled_any = 0;
        for (uint32_t ch = 0; ch < n_ch; ch++) {
            if (w->pending_count[ch] == 0) continue;
            int got = vemb_v16_aeron_poll_response(w->channels[ch],
                                                    &resp_buf, sizeof(resp_buf));
            if (got <= 0) {
                if (got == 0) debug_poll_zero++;
                continue;
            }
            debug_poll_got++;
            if (got != (int)sizeof(resp_buf)) continue;
            if (w->pending_count[ch] == 0) continue;

            uint32_t slot = w->pending_head[ch];
            pending_op *pe = &w->pending[ch][slot];
            if (pe->req_id != resp_buf.req_id) {
                int found = -1;
                for (uint32_t j = 0; j < w->pending_count[ch]; j++) {
                    uint32_t s2 = (w->pending_head[ch] + j) % pipeline;
                    if (w->pending[ch][s2].req_id == resp_buf.req_id) {
                        found = (int)s2;
                        break;
                    }
                }
                if (found < 0) continue;
                if (found != (int)slot) {
                    pending_op tmp = w->pending[ch][slot];
                    w->pending[ch][slot] = w->pending[ch][found];
                    w->pending[ch][found] = tmp;
                    pe = &w->pending[ch][slot];
                }
            }
            w->pending_head[ch] = (slot + 1) % pipeline;
            w->pending_count[ch]--;

            struct timeval now;
            gettimeofday(&now, NULL);
            unsigned int latency_usec =
                (unsigned int)ts_diff(pe->sent_time, now);
            unsigned int bytes_rx = (unsigned int)sizeof(resp_buf);
            unsigned int bytes_tx = pe->bytes_tx;

            if (pe->is_set) {
                w->stats->update_set_op(&now, bytes_rx, bytes_tx, latency_usec);
            } else {
                switch (resp_buf.status) {
                    case VEMB_V16_STATUS_OK: debug_status_ok++; break;
                    case VEMB_V16_STATUS_NOT_FOUND: debug_status_notfound++; break;
                    case VEMB_V16_STATUS_ERR: debug_status_err++; break;
                    default: debug_status_other++; break;
                }
                /* VEMB_HANDLE: server returned (offset, bytes); dereference
                 * via the channel's warm-region mapping to read the actual
                 * vector. Count as a hit only if the dereference succeeds. */
                unsigned int hits = 0, misses = 0;
                if (resp_buf.status == VEMB_V16_STATUS_OK) {
                    int n = vemb_v16_aeron_read_vector(w->channels[ch],
                                                       resp_buf.region_id,
                                                       resp_buf.vector_offset,
                                                       resp_buf.vector_bytes,
                                                       vec_scratch,
                                                       sizeof(vec_scratch));
                    if (n > 0) { hits = 1; debug_handle_deref_ok++; }
                    else       { debug_handle_deref_fail++; }
                } else if (resp_buf.status == VEMB_V16_STATUS_NOT_FOUND) {
                    misses = 1;
                }
                w->stats->update_get_op(&now, bytes_rx, bytes_tx,
                                        latency_usec, hits, misses);
            }
            w->ops_done.fetch_add(1, std::memory_order_relaxed);
            polled_any = 1;
        }

        /* if nothing published and nothing polled, busy-spin with a CPU yield
         * hint. Do NOT use nanosleep — even 200ns requests round up to ~1-10ms
         * kernel tick latency on Linux, which becomes the bottleneck when
         * the server-side proxy goes to sleep between batches. */
        if (!published_any && !polled_any) {
#if defined(__x86_64__) || defined(__i386__)
            __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__) || defined(__arm__)
            __asm__ volatile("yield" ::: "memory");
#else
            __asm__ volatile("" ::: "memory");
#endif
        }
    }

    /* ---- drain phase: up to ~1s to collect outstanding responses ---- */
    {
        uint64_t drain_start = now_ns();
        while (1) {
            int any_pending = 0;
            for (uint32_t ch = 0; ch < n_ch; ch++) {
                if (w->pending_count[ch] > 0) { any_pending = 1; break; }
            }
            if (!any_pending) break;
            if (now_ns() - drain_start > 1000000000ull) break;

            for (uint32_t ch = 0; ch < n_ch; ch++) {
                if (w->pending_count[ch] == 0) continue;
                int got;
                while ((got = vemb_v16_aeron_poll_response(w->channels[ch],
                                                            &resp_buf, sizeof(resp_buf))) > 0) {
                    if (got != (int)sizeof(resp_buf)) continue;
                    if (w->pending_count[ch] == 0) break;
                    uint32_t slot = w->pending_head[ch];
                    pending_op *pe = &w->pending[ch][slot];
                    if (pe->req_id != resp_buf.req_id) {
                        int found = -1;
                        for (uint32_t j = 0; j < w->pending_count[ch]; j++) {
                            uint32_t s2 = (w->pending_head[ch] + j) % pipeline;
                            if (w->pending[ch][s2].req_id == resp_buf.req_id) {
                                found = (int)s2; break;
                            }
                        }
                        if (found < 0) continue;
                        if (found != (int)slot) {
                            pending_op tmp = w->pending[ch][slot];
                            w->pending[ch][slot] = w->pending[ch][found];
                            w->pending[ch][found] = tmp;
                            pe = &w->pending[ch][slot];
                        }
                    }
                    w->pending_head[ch] = (slot + 1) % pipeline;
                    w->pending_count[ch]--;

                    struct timeval now;
                    gettimeofday(&now, NULL);
                    unsigned int latency_usec =
                        (unsigned int)ts_diff(pe->sent_time, now);
                    unsigned int bytes_rx = (unsigned int)sizeof(resp_buf);
                    unsigned int bytes_tx = pe->bytes_tx;
                    if (pe->is_set) {
                        w->stats->update_set_op(&now, bytes_rx, bytes_tx, latency_usec);
                    } else {
                        unsigned int hits = 0, misses = 0;
                        if (resp_buf.status == VEMB_V16_STATUS_OK) {
                            int n = vemb_v16_aeron_read_vector(w->channels[ch],
                                                               resp_buf.region_id,
                                                               resp_buf.vector_offset,
                                                               resp_buf.vector_bytes,
                                                               vec_scratch,
                                                               sizeof(vec_scratch));
                            if (n > 0) { hits = 1; debug_handle_deref_ok++; }
                            else       { debug_handle_deref_fail++; }
                        } else if (resp_buf.status == VEMB_V16_STATUS_NOT_FOUND) {
                            misses = 1;
                        }
                        w->stats->update_get_op(&now, bytes_rx, bytes_tx,
                                                latency_usec, hits, misses);
                    }
                    w->ops_done.fetch_add(1, std::memory_order_relaxed);
                }
            }
            struct timespec ts = {0, 500};
            nanosleep(&ts, NULL);
        }
    }

    struct timeval end_tv;
    gettimeofday(&end_tv, NULL);
    w->stats->set_end_time(&end_tv);
    fprintf(stderr, "[aeron] w%u done: loops=%llu publish_ok=%llu publish_fail=%llu poll_zero=%llu poll_got=%llu ops_done=%llu status[ok=%llu nf=%llu err=%llu other=%llu] handle_deref[ok=%llu fail=%llu]\n",
            w->worker_id,
            (unsigned long long)debug_loop_count,
            (unsigned long long)debug_publish_ok,
            (unsigned long long)debug_publish_fail,
            (unsigned long long)debug_poll_zero,
            (unsigned long long)debug_poll_got,
            (unsigned long long)w->ops_done.load(),
            (unsigned long long)debug_status_ok,
            (unsigned long long)debug_status_notfound,
            (unsigned long long)debug_status_err,
            (unsigned long long)debug_status_other,
            (unsigned long long)debug_handle_deref_ok,
            (unsigned long long)debug_handle_deref_fail);
    w->done.store(true, std::memory_order_release);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Public entry                                                       */
/* ------------------------------------------------------------------ */

run_stats vemb_v16_aeron_run(benchmark_config* cfg, object_generator* obj_gen) {
    fprintf(stderr, "[aeron] side-channel runner start (t=%u c=%u pipeline=%u dim=%u)\n",
            cfg->threads, cfg->clients, cfg->pipeline, cfg->vemb_v16_dim);

    /* Validate config */
    if (cfg->vemb_v16_dim == 0) {
        benchmark_error_log("[aeron] --vemb-v16-dim required\n");
        exit(1);
    }
    if (cfg->vemb_v16_endpoints && g_aeron_transport_mode != "aeron-cross-node") {
        benchmark_error_log("[aeron] --vemb-v16-endpoints not supported in aeron mode (single-node UDS only)\n");
        exit(1);
    }
    uint32_t total_channels = cfg->threads * cfg->clients;
    if (total_channels == 0) {
        benchmark_error_log("[aeron] threads*clients must be > 0\n");
        exit(1);
    }
    if (total_channels > VEMB_V16_MAX_CHANNELS) {
        benchmark_error_log("[aeron] t*c=%u exceeds VEMB_V16_MAX_CHANNELS=%u\n",
                            total_channels, VEMB_V16_MAX_CHANNELS);
        exit(1);
    }

    char control_endpoint[320];
    const char *uds_path = VEMB_V16_UDS_PATH;
    if (cfg->unix_socket && cfg->unix_socket[0]) {
        uds_path = cfg->unix_socket;
    } else if (cfg->server && cfg->server[0] && cfg->port != 0) {
        snprintf(control_endpoint,
                 sizeof(control_endpoint),
                 "tcp://%s:%u",
                 cfg->server,
                 (unsigned)cfg->port);
        uds_path = control_endpoint;
    }

    /* Best-effort cleanup of any stale channels from a previous crashed run.
     * Cross-node mode bypasses UDS entirely (TCP ATTACH + shmdev mmap). */
    bool cross_node = (g_aeron_transport_mode == "aeron-cross-node");
    std::string cn_host;
    uint16_t    cn_port = 0;
    if (cross_node) {
        auto colon = g_aeron_remote_endpoint.find(':');
        if (colon == std::string::npos) {
            benchmark_error_log("[aeron] cross-node endpoint must be HOST:PORT, got '%s'\n",
                                g_aeron_remote_endpoint.c_str());
            exit(1);
        }
        cn_host = g_aeron_remote_endpoint.substr(0, colon);
        cn_port = (uint16_t)atoi(g_aeron_remote_endpoint.c_str() + colon + 1);
        if (cn_host.empty() || cn_port == 0) {
            benchmark_error_log("[aeron] cross-node endpoint parse failed\n");
            exit(1);
        }
        fprintf(stderr, "[aeron] cross-node mode: %s:%u\n", cn_host.c_str(), cn_port);
    } else {
        vemb_v16_aeron_close_all(uds_path);
        fprintf(stderr, "[aeron] local mode: uds=%s\n", uds_path);
    }

    /* Allocate channels up-front (main thread). Each worker will own
     * cfg->clients of them. */
    fprintf(stderr, "[aeron] allocating %u channels%s\n",
            total_channels, cross_node ? " (cross-node)" : "");
    std::vector<vemb_v16_aeron_channel_t *> all_channels(total_channels, nullptr);
    for (uint32_t i = 0; i < total_channels; i++) {
        if (cross_node) {
            all_channels[i] = vemb_v16_aeron_open_remote(cn_host.c_str(), cn_port,
                                                         cfg->vemb_v16_dim);
        } else {
            all_channels[i] = vemb_v16_aeron_open(uds_path, cfg->vemb_v16_dim);
        }
        if (!all_channels[i]) {
            benchmark_error_log("[aeron] open channel %u failed: %s\n",
                                i, strerror(errno));
            for (uint32_t j = 0; j < i; j++) {
                vemb_v16_aeron_close(all_channels[j]);
                all_channels[j] = nullptr;
            }
            exit(1);
        }
        /* Map the warm region so worker_main can dereference VEMB_HANDLE
         * offsets and read the actual vector bytes — without this the test
         * would only validate that the server returns a handle, not that
         * the handle points to real data.
         *
         * Cross-node ATTACH resp advertises warm region paths via the
         * client_path field in the manifest; SDK parses them into
         * ch->desc.warm_regions[] which this call consumes. */
        if (vemb_v16_aeron_open_warm_region(all_channels[i]) != 0) {
            benchmark_error_log("[aeron] open warm region %u failed: %s\n",
                                i, strerror(errno));
            for (uint32_t j = 0; j <= i; j++) {
                vemb_v16_aeron_close(all_channels[j]);
                all_channels[j] = nullptr;
            }
            exit(1);
        }
    }
    fprintf(stderr, "[aeron] all %u channels ready%s\n",
            total_channels, cross_node ? " (cross-node, no warm region)" : " (warm region mapped)");

    /* Setup workers */
    std::vector<worker_arg> workers(cfg->threads);
    std::vector<pthread_t>  tids(cfg->threads);
    std::atomic<bool> stop(false);

    /* Budget: if --requests (-n) is set, divide across workers.
     * If only --test-time, budget=0 (unlimited, stop flag ends loop). */
    unsigned long long per_worker_budget = 0;
    if (cfg->requests > 0) {
        per_worker_budget = cfg->requests / cfg->threads;
        if (per_worker_budget == 0) per_worker_budget = 1;
    }

    uint32_t pipeline = cfg->pipeline > 0 ? cfg->pipeline : 1;

    for (unsigned int i = 0; i < cfg->threads; i++) {
        workers[i].stats = new run_stats(cfg);
        workers[i].cfg = cfg;
        workers[i].obj_gen = obj_gen->clone();
        /* Give each worker a distinct random seed so that RANDOM/ZIPFIAN
         * key patterns don't collide across workers (mirrors client.cpp:73
         * distinct_client_seed path). */
        workers[i].obj_gen->set_random_seed((int)i + 1);
        workers[i].worker_id = i;
        workers[i].stop = &stop;
        workers[i].done.store(false, std::memory_order_relaxed);
        workers[i].ops_done.store(0, std::memory_order_relaxed);
        workers[i].ops_fail.store(0, std::memory_order_relaxed);
        workers[i].set_ratio_count = 0;
        workers[i].get_ratio_count = 0;
        workers[i].budget = per_worker_budget;

        /* assign cfg->clients channels to this worker */
        workers[i].channels.reserve(cfg->clients);
        for (unsigned int j = 0; j < cfg->clients; j++) {
            uint32_t idx = i * cfg->clients + j;
            workers[i].channels.push_back(all_channels[idx]);
        }
        workers[i].pending.resize(cfg->clients);
        workers[i].pending_head.assign(cfg->clients, 0);
        workers[i].pending_tail.assign(cfg->clients, 0);
        workers[i].pending_count.assign(cfg->clients, 0);
        for (unsigned int j = 0; j < cfg->clients; j++) {
            workers[i].pending[j].assign(pipeline, pending_op());
        }
    }

    /* Launch */
    fprintf(stderr, "[aeron] launching %u worker threads\n", cfg->threads);
    for (unsigned int i = 0; i < cfg->threads; i++) {
        int rc = pthread_create(&tids[i], NULL, worker_main, &workers[i]);
        if (rc != 0) {
            benchmark_error_log("[aeron] pthread_create %u failed: %s\n",
                                i, strerror(rc));
            exit(1);
        }
    }

    /* Test-time stop arm */
    if (cfg->test_time > 0) {
        sleep(cfg->test_time);
        stop.store(true, std::memory_order_release);
    }

    /* Wait for all workers to report done */
    for (unsigned int i = 0; i < cfg->threads; i++) {
        pthread_join(tids[i], NULL);
    }
    fprintf(stderr, "[aeron] all workers joined\n");

    /* Merge per-worker stats into one run_stats — reuse run_stats::merge
     * with iteration counter (matches client_group::merge_run_stats). */
    run_stats merged(cfg);
    int iteration = 0;
    for (unsigned int i = 0; i < cfg->threads; i++) {
        merged.merge(*workers[i].stats, iteration++);
    }

    /* Teardown channels — SDK handles ring unmap + UDS notify. */
    for (uint32_t i = 0; i < total_channels; i++) {
        vemb_v16_aeron_close(all_channels[i]);
        all_channels[i] = nullptr;
    }

    /* Destroy cloned obj_gens + per-worker stats */
    for (unsigned int i = 0; i < cfg->threads; i++) {
        delete workers[i].obj_gen;
        delete workers[i].stats;
    }

    return merged;
}
