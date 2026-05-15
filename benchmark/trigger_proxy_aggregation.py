#!/usr/bin/env python3
"""
Trigger proxy/supernode aggregation for VEMB / VSIM using many concurrent
redis-cli processes against the same key.

Why this exists:
- single interactive redis-cli request/response is too serial to trigger proxy batching
- this script fans out many concurrent requests so proxy can accumulate them

No third-party dependencies are required.
"""

import argparse
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def run_cmd(cmd):
    start = time.time()
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    end = time.time()
    return {
        "cmd": cmd,
        "rc": p.returncode,
        "stdout": p.stdout.strip(),
        "stderr": p.stderr.strip(),
        "latency_ms": (end - start) * 1000.0,
    }


def build_vemb_cmd(redis_cli, port, key, element, raw):
    cmd = [redis_cli, "-p", str(port), "VEMB", key, element]
    if raw:
        cmd.append("RAW")
    return cmd


def build_vsim_cmd(redis_cli, port, key, query):
    dim = len(query)
    cmd = [redis_cli, "-p", str(port), "VSIM", key, "VALUES", str(dim)]
    cmd.extend(str(x) for x in query)
    cmd.append("WITHSCORES")
    return cmd


def prefill_vectors(redis_cli, port, key, dim, count):
    print(f"[setup] FLUSHALL")
    print(run_cmd([redis_cli, "-p", str(port), "FLUSHALL"])["stdout"])

    print(f"[setup] inserting {count} vectors into key={key}")
    basis = []
    for i in range(dim):
        vec = [0.0] * dim
        vec[i] = 1.0
        basis.append(vec)

    for i in range(count):
        vec = basis[i % len(basis)]
        cmd = [redis_cli, "-p", str(port), "VADD", key, "VALUES", str(dim)]
        cmd.extend(str(x) for x in vec)
        cmd.append(f"item:{i}")
        res = run_cmd(cmd)
        if res["rc"] != 0:
            print(f"[setup] VADD failed: {' '.join(cmd)}", file=sys.stderr)
            print(res["stderr"], file=sys.stderr)
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Concurrent redis-cli fanout to trigger proxy aggregation")
    parser.add_argument("--redis-cli", default="./src/redis-cli", help="Path to redis-cli")
    parser.add_argument("--port", type=int, default=6391, help="Redis port")
    parser.add_argument("--key", default="myvectors", help="Target vector key")
    parser.add_argument("--mode", choices=["vemb", "vsim"], required=True, help="Benchmark mode")
    parser.add_argument("--concurrency", type=int, default=32, help="Concurrent redis-cli processes")
    parser.add_argument("--requests", type=int, default=128, help="Total requests")
    parser.add_argument("--raw", action="store_true", help="Use RAW for VEMB")
    parser.add_argument("--dim", type=int, default=4, help="Vector dimension for setup/query")
    parser.add_argument("--prefill-count", type=int, default=64, help="How many vectors to insert before running")
    args = parser.parse_args()

    redis_cli = args.redis_cli
    if not os.path.exists(redis_cli):
        print(f"redis-cli not found: {redis_cli}", file=sys.stderr)
        sys.exit(1)

    prefill_vectors(redis_cli, args.port, args.key, args.dim, args.prefill_count)

    jobs = []
    if args.mode == "vemb":
        for i in range(args.requests):
            element = f"item:{i % args.prefill_count}"
            jobs.append(build_vemb_cmd(redis_cli, args.port, args.key, element, args.raw))
    else:
        query = [1.0] + [0.0] * (args.dim - 1)
        for _ in range(args.requests):
            jobs.append(build_vsim_cmd(redis_cli, args.port, args.key, query))

    print(f"[run] mode={args.mode} requests={args.requests} concurrency={args.concurrency} key={args.key}")

    results = []
    wall_start = time.time()
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futs = [ex.submit(run_cmd, job) for job in jobs]
        for fut in as_completed(futs):
            results.append(fut.result())
    wall_end = time.time()

    ok = sum(1 for r in results if r["rc"] == 0)
    failed = len(results) - ok
    avg_ms = sum(r["latency_ms"] for r in results) / len(results) if results else 0.0
    qps = len(results) / max(wall_end - wall_start, 1e-9)

    print("")
    print("=== Aggregation Trigger Summary ===")
    print(f"successful: {ok}")
    print(f"failed:     {failed}")
    print(f"avg ms/op:  {avg_ms:.2f}")
    print(f"wall qps:   {qps:.2f}")

    if failed:
        print("")
        print("=== Sample Failures ===")
        shown = 0
        for r in results:
            if r["rc"] != 0 or r["stderr"]:
                print("cmd:", " ".join(r["cmd"]))
                if r["stdout"]:
                    print("stdout:", r["stdout"])
                if r["stderr"]:
                    print("stderr:", r["stderr"])
                print("---")
                shown += 1
                if shown >= 5:
                    break

    print("")
    print("Suggested log checks:")
    print("  tail -n 100 /tmp/redis-vemb-test/redis.log")
    print("  tail -n 100 /tmp/redis-vsim-test/redis.log")
    print("Look for batch sizes > 1 in proxy/supernode logs.")


if __name__ == "__main__":
    main()
