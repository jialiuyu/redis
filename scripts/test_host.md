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

$ benchmarking
  Run the benchmark 3 times and compare the average QPS instead of judging by a single run. 