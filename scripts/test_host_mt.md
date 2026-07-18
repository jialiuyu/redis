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
- 仅同步代码，不同步文件
- 有问题及时反馈不要自己猜测
