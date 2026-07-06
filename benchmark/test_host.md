$ ssh -p 22 root@192.168.90.111
$ cd /root/szz/codespace/hpc-redis
$ manifest: /tmp/v16_node0.yaml
  $ --ub-path :
    - /dev/obmm_shmdev1 local region: cc mode, size: 8G
    - /dev/obmm_shmdev5 remote region: nc mode, size: 8G
$ run
  ```
  make -C src USE_SVE=yes vemb_v16_server
  make -B -C benchmark USE_SVE=yes vemb_v16_bench

  ./src/vemb_v16_server     --transport tcp     --tcp-host 0.0.0.0     --tcp-port 6391     --proxy-io-threads 16     --supernode-workers 32     --warm-regions-manifest /tmp/warm-regions-manifest.yaml     --reset-warm-regions     --dim 300     --max-vectors 131072     --loglevel notice     > /tmp/vemb_v16_server_tcp_mixed.log 2>&1


  ./benchmark/vemb_v16_bench     --transport tcp     --host 127.0.0.1     --port 6391     --mode mixed-80r20w     --dim 300     --prefill 65536     --keyspace 65536     --ops 200000     --threads 128     --pipeline 32     --timeout-ms 30000     --no-pin     2>&1 | tee /tmp/vemb_v16_bench_tcp_mixed.log
  ```

$ note
  TCP transport now uses the encoded request/response protocol unconditionally.
  Do not pass `--tcp-encoded-payloads`; the old struct + memcpy TCP payload mode has been removed.

$ benchmarking
  Run the benchmark 3 times and compare the average QPS instead of
  judging by a single run. The first run after a server restart may be slightly
  lower because of warmup effects such as CPU scheduling, page faults, and cache
  population.

$ latest result
  Latest `mixed-80r20w` TCP rerun on `2026-07-03`:
    - runs: `3`
    - qps avg: `2843824.98`
    - qps min: `2831729.75`
    - qps max: `2853016.37`
    - fail: `0`

$ bandwidth conclusion
  New TCP protocol is encode/decode only and reduces wire bytes mainly by making
  request size proportional to the actual op instead of always sending the old
  fixed-width request struct.

  Old fixed TCP payload sizes from the legacy struct layout:
    - request payload: `sizeof(vemb_v16_req_t) = 16704B`
    - response metadata: `sizeof(vemb_v16_resp_t) = 64B`

  New compact TCP payload sizes for `dim=300`, `key_len=10`:
    - `VEMB_INLINE` request: `34B` payload, `66B` total frame with net header
    - `VADD` request: `1238B` payload, `1270B` total frame with net header
    - `VADD` response: `6B` metadata, `38B` total frame with net header
    - `VEMB_INLINE` response: `34B` metadata + `1200B` vector payload,
      `1266B` total frame with net header

  Weighted by the `mixed-80r20w` workload:
    - average request frame: `16736B -> 306.8B`, save `98.17%`
    - average response frame: `1056.0B -> 1020.4B`, save `3.37%`
    - average round-trip bytes/op: `17792.0B -> 1327.2B`, save `92.54%`

  For the `25,600,000` requests in this benchmark, the estimated total wire
  traffic changes from about `455.48 GB` to `33.98 GB`. Most of the saving comes
  from removing the old fixed-width request struct on the TCP write path. The
  response-side saving is smaller in this workload because `VEMB_INLINE`
  responses are dominated by the `1200B` vector payload itself.
