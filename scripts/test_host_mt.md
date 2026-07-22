$ ssh -p 22 root@192.168.90.111
$ directory:
$ - /root/szz/codespace/hpc-redis
$ - /root/FlameGraph/flamegraph.pl: only for redis-server
$ run
```
  make -C src redis-server USE_UB=yes
  NUM_KEYS=100000 WORKERS='21:21' TS='64' CS='4' TEST_TIME=30 bash hpc_redis_max_tput.sh
```
## Note:
- 仅同步代码，不其他文档或者二进制，覆盖远程同位置代码
- 有问题及时反馈不要自己猜测
- 结果需要: `ops/sec / p50 / p99 / cpu_cores` + 火焰图
- 远端的火焰图 svg 拉取到本地的 perf 目录下
- 热点key: NUM_KEYS 可以降低

## memtier_benchmar
- 如果需要编译 memtier_benchmark, 流程:
```
- 先编译 client sdk : cd clients/c/ && make -j
- 再编译 memtier_benchmark:  autoreconf -ivf && ./configure && make -j
```
