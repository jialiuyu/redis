#!/usr/bin/env python3
"""
TLC Benchmark Log Parser
解析 tlc_bench 日志文件，提取性能数据并生成对比报告
"""

import re
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class BenchmarkResult:
    name: str
    qps: float
    latency_us: float
    puts: int
    gets: int
    misses: int
    ops: int
    threads: int
    pipeline: int
    write_pct: int

def parse_log_file(filepath: str) -> Dict:
    """解析完整日志文件"""
    with open(filepath, 'r') as f:
        lines = f.readlines()
    
    results = {
        'baseline': [],
        'tlc': [],
        'optimized': [],
        'cache_stats': {},
        'memory_6379': {},
        'memory_6380': {}
    }
    
    current_section = None
    current_test_name = ""
    last_ops_config = None
    in_cache_stats = False
    last_cache_key = None
    in_mem_section = None
    
    for i, line in enumerate(lines):
        line = line.rstrip()
        
        # 检测 section
        if 'Baseline Redis (port 6379)' in line:
            current_section = 'baseline'
            in_cache_stats = False
            in_mem_section = None
        elif 'TLC Module (port 6380' in line:
            current_section = 'tlc'
            in_cache_stats = False
            in_mem_section = None
        elif 'Optimized Redis SET/GET' in line:
            current_section = 'optimized'
            in_cache_stats = False
            in_mem_section = None
        elif '=== TLC Cache Statistics ===' in line:
            in_cache_stats = True
            in_mem_section = None
            continue
        elif '=== Redis Info (6379) ===' in line:
            in_cache_stats = False
            in_mem_section = '6379'
            continue
        elif '=== Redis Info (6380) ===' in line:
            in_cache_stats = False
            in_mem_section = '6380'
            continue
        
        # 解析缓存统计 (key-value 格式)
        if in_cache_stats:
            stripped = line.strip()
            if re.match(r'^[a-z_]+$', stripped):
                last_cache_key = stripped
            elif re.match(r'^\d+$', stripped) and last_cache_key:
                results['cache_stats'][last_cache_key] = int(stripped)
        
        # 解析内存信息
        if in_mem_section and ':' in line:
            key, value = line.split(':', 1)
            key = key.replace('# ', '').strip()
            results[f'memory_{in_mem_section}'][key] = value.strip()
        
        # 获取测试名称
        # 格式: "  Baseline Pre-fill (100%% write)" 或 "  TLC.PUT/GET 80R/20W (no pipeline)"
        test_patterns = [
            (r'^\s+(Baseline|TLC|Optimized)\s+Pre-fill', 'Pre-fill'),
            (r'^\s+Baseline\s+SET/GET\s+80R/20W\s+\(no pipeline\)', 'SET/GET 80R/20W (P=1)'),
            (r'^\s+Baseline\s+SET/GET\s+80R/20W\s+\(P=16\)', 'SET/GET 80R/20W (P=16)'),
            (r'^\s+Baseline\s+100%%\s+GET\s+\(P=16\)', '100% GET (P=16)'),
            (r'^\s+TLC\.PUT/GET\s+80R/20W\s+\(no pipeline\)', 'TLC.PUT/GET 80R/20W (P=1)'),
            (r'^\s+TLC\.PUT/GET\s+80R/20W\s+\(P=16\)', 'TLC.PUT/GET 80R/20W (P=16)'),
            (r'^\s+TLC\s+100%%\s+GET\s+\(P=16\)', 'TLC 100% GET (P=16)'),
            (r'^\s+Optimized\s+Pre-fill', 'Pre-fill'),
            (r'^\s+Optimized\s+SET/GET\s+80R/20W\s+\(no pipeline\)', 'SET/GET 80R/20W (P=1)'),
            (r'^\s+Optimized\s+SET/GET\s+80R/20W\s+\(P=16\)', 'SET/GET 80R/20W (P=16)'),
            (r'^\s+Optimized\s+100%%\s+GET\s+\(P=16\)', '100% GET (P=16)'),
        ]
        
        for pattern, name in test_patterns:
            if re.search(pattern, line):
                current_test_name = name
                break
        
        # 解析 Ops 配置行 (单独一行)
        ops_match = re.search(r'Ops:(\d+)\s+Thr:(\d+)\s+Pipeline:(\d+)\s+W%:(\d+)', line)
        if ops_match:
            last_ops_config = {
                'ops': int(ops_match.group(1)),
                'threads': int(ops_match.group(2)),
                'pipeline': int(ops_match.group(3)),
                'write_pct': int(ops_match.group(4))
            }
        
        # 解析性能结果行 (单独一行，使用上一次的 ops_config)
        result_match = re.search(
            r'→\s+([\d.]+)\s+QPS\s+\([^)]+\)\s+Lat:\s+([\d.]+)\s+μs\s+PUT:(\d+)\s+GET:(\d+)\s+MISS:(\d+)',
            line
        )
        
        if result_match and last_ops_config and current_section:
            r = BenchmarkResult(
                name=current_test_name,
                qps=float(result_match.group(1)),
                latency_us=float(result_match.group(2)),
                puts=int(result_match.group(3)),
                gets=int(result_match.group(4)),
                misses=int(result_match.group(5)),
                ops=last_ops_config['ops'],
                threads=last_ops_config['threads'],
                pipeline=last_ops_config['pipeline'],
                write_pct=last_ops_config['write_pct']
            )
            results[current_section].append(r)
            last_ops_config = None
    
    return results

def generate_report(results: Dict) -> str:
    """生成对比报告"""
    lines = []
    lines.append("=" * 80)
    lines.append("TLC BENCHMARK ANALYSIS REPORT")
    lines.append("=" * 80)
    lines.append("")
    
    # 1. 性能对比表
    lines.append("PERFORMANCE COMPARISON (QPS)")
    lines.append("-" * 80)
    
    # 按 pipeline 分组
    p1_tests = {'baseline': {}, 'tlc': {}, 'optimized': {}}
    p16_tests = {'baseline': {}, 'tlc': {}, 'optimized': {}}
    
    for section in ['baseline', 'tlc', 'optimized']:
        for r in results[section]:
            key = r.name
            if r.pipeline == 1:
                p1_tests[section][key] = r
            else:
                p16_tests[section][key] = r
    
    # Pipeline=1 对比
    lines.append("")
    lines.append("Pipeline = 1 (no pipelining)")
    lines.append(f"{'Test':<40} {'Baseline':>12} {'TLC':>12} {'Optimized':>12} {'Winner':>10}")
    lines.append("-" * 80)
    
    all_p1_names = sorted(set(
        list(p1_tests['baseline'].keys()) + 
        list(p1_tests['tlc'].keys()) + 
        list(p1_tests['optimized'].keys())
    ))
    
    for test_name in all_p1_names:
        b = p1_tests['baseline'].get(test_name)
        t = p1_tests['tlc'].get(test_name)
        o = p1_tests['optimized'].get(test_name)
        
        b_qps = b.qps if b else 0
        t_qps = t.qps if t else 0
        o_qps = o.qps if o else 0
        
        winner = 'baseline' if b_qps >= max(t_qps, o_qps) else ('tlc' if t_qps >= o_qps else 'optimized')
        
        lines.append(f"{test_name:<40} {b_qps:>12.0f} {t_qps:>12.0f} {o_qps:>12.0f} {winner:>10}")
    
    # Pipeline=16 对比
    lines.append("")
    lines.append("Pipeline = 16 (high throughput)")
    lines.append(f"{'Test':<40} {'Baseline':>12} {'TLC':>12} {'Optimized':>12} {'Winner':>10} {'Improve':>10}")
    lines.append("-" * 80)
    
    all_p16_names = sorted(set(
        list(p16_tests['baseline'].keys()) + 
        list(p16_tests['tlc'].keys()) + 
        list(p16_tests['optimized'].keys())
    ))
    
    for test_name in all_p16_names:
        b = p16_tests['baseline'].get(test_name)
        t = p16_tests['tlc'].get(test_name)
        o = p16_tests['optimized'].get(test_name)
        
        b_qps = b.qps if b else 0
        t_qps = t.qps if t else 0
        o_qps = o.qps if o else 0
        
        winner = 'baseline' if b_qps >= max(t_qps, o_qps) else ('tlc' if t_qps >= o_qps else 'optimized')
        best_qps = max(b_qps, t_qps, o_qps)
        
        improve = ""
        if b_qps > 0 and best_qps > b_qps:
            pct = (best_qps / b_qps - 1) * 100
            improve = f"+{pct:.1f}%"
        
        lines.append(f"{test_name:<40} {b_qps:>12.0f} {t_qps:>12.0f} {o_qps:>12.0f} {winner:>10} {improve:>10}")
    
    # 2. 延迟对比
    lines.append("")
    lines.append("=" * 80)
    lines.append("LATENCY COMPARISON (μs)")
    lines.append("-" * 80)
    
    lines.append(f"{'Test':<40} {'Baseline':>10} {'TLC':>10} {'Optimized':>10}")
    lines.append("-" * 80)
    
    # 按测试名称分组
    all_tests = {}
    for section in ['baseline', 'tlc', 'optimized']:
        for r in results[section]:
            key = f"{r.name} (P={r.pipeline})"
            if key not in all_tests:
                all_tests[key] = {'baseline': None, 'tlc': None, 'optimized': None}
            all_tests[key][section] = r
    
    for test_name in sorted(all_tests.keys()):
        data = all_tests[test_name]
        b_lat = data['baseline'].latency_us if data['baseline'] else 0
        t_lat = data['tlc'].latency_us if data['tlc'] else 0
        o_lat = data['optimized'].latency_us if data['optimized'] else 0
        
        lines.append(f"{test_name:<40} {b_lat:>10.1f} {t_lat:>10.1f} {o_lat:>10.1f}")
    
    # 3. 缓存命中率
    if results['cache_stats']:
        stats = results['cache_stats']
        lines.append("")
        lines.append("=" * 80)
        lines.append("TLC CACHE HIT ANALYSIS")
        lines.append("-" * 80)
        
        total_reads = stats.get('total_reads', 0)
        if total_reads > 0:
            hot_hits = stats.get('hot_hits', 0)
            warm_hits = stats.get('warm_hits', 0)
            cold_hits = stats.get('cold_hits', 0)
            total_misses = stats.get('hot_misses', 0) + stats.get('warm_misses', 0) + stats.get('cold_misses', 0)
            
            hot_rate = hot_hits / total_reads * 100
            warm_rate = warm_hits / total_reads * 100
            cold_rate = cold_hits / total_reads * 100
            miss_rate = total_misses / total_reads * 100
            
            lines.append(f"Total reads:      {total_reads:,}")
            lines.append(f"Total writes:     {stats.get('total_writes', 0):,}")
            lines.append("")
            lines.append(f"HOT hit rate:     {hot_rate:.2f}% ({hot_hits:,} hits)")
            lines.append(f"WARM hit rate:    {warm_rate:.2f}% ({warm_hits:,} hits)")
            lines.append(f"COLD hit rate:    {cold_rate:.2f}% ({cold_hits:,} hits)")
            lines.append(f"Miss rate:        {miss_rate:.2f}% ({total_misses:,} misses)")
            lines.append("")
            lines.append(f"WARM entries:     {stats.get('warm_count', 0):,}")
            lines.append(f"UB local access:  {stats.get('ub_local_pct', 0)}%")
            lines.append(f"UB nodes:         {stats.get('ub_nodes', 4)}")
    
    # 4. 内存使用
    mem_6379 = results['memory_6379']
    mem_6380 = results['memory_6380']
    
    if mem_6379 or mem_6380:
        lines.append("")
        lines.append("=" * 80)
        lines.append("MEMORY USAGE")
        lines.append("-" * 80)
        
        lines.append(f"{'Metric':<30} {'Port 6379':>20} {'Port 6380':>20}")
        lines.append("-" * 80)
        
        for key in ['used_memory_human', 'used_memory_rss_human', 'used_memory_peak_human', 'maxmemory_human']:
            lines.append(f"{key:<30} {mem_6379.get(key, 'N/A'):>20} {mem_6380.get(key, 'N/A'):>20}")
        
        rss_ratio = mem_6380.get('rss_overhead_ratio', 'N/A')
        lines.append("")
        lines.append(f"Port 6380 RSS overhead ratio: {rss_ratio} (UB memory mapping)")
    
    # 5. 总结
    lines.append("")
    lines.append("=" * 80)
    lines.append("SUMMARY")
    lines.append("=" * 80)
    
    # 找最佳 100% GET P=16 结果
    best_get = None
    for section in ['baseline', 'tlc', 'optimized']:
        for r in results[section]:
            if r.pipeline == 16 and r.write_pct == 0 and 'GET' in r.name:
                if best_get is None or r.qps > best_get[1].qps:
                    best_get = (section, r)
    
    if best_get:
        section, r = best_get
        lines.append(f"Best 100% GET (P=16): {r.qps:.0f} QPS, {r.latency_us:.1f} μs latency ({section})")
    
    # 计算平均提升
    baseline_p16 = [r.qps for r in results['baseline'] if r.pipeline == 16 and r.write_pct < 100]
    tlc_p16 = [r.qps for r in results['tlc'] if r.pipeline == 16 and r.write_pct < 100]
    optimized_p16 = [r.qps for r in results['optimized'] if r.pipeline == 16 and r.write_pct < 100]
    
    if baseline_p16 and optimized_p16:
        avg_baseline = sum(baseline_p16) / len(baseline_p16)
        avg_optimized = sum(optimized_p16) / len(optimized_p16)
        avg_tlc = sum(tlc_p16) / len(tlc_p16) if tlc_p16 else 0
        
        if avg_baseline > 0:
            opt_improve = (avg_optimized / avg_baseline - 1) * 100
            tlc_improve = (avg_tlc / avg_baseline - 1) * 100 if avg_tlc > 0 else 0
            
            lines.append(f"Optimized avg improvement: +{opt_improve:.1f}% vs baseline (P=16)")
            lines.append(f"TLC avg improvement:       +{tlc_improve:.1f}% vs baseline (P=16)")
    
    lines.append("")
    lines.append("=" * 80)
    
    return "\n".join(lines)

def main():
    if len(sys.argv) < 2:
        default_path = "/home/xuwei/code/hpc-redis/benchmark/results/tlc_bench_20260421_150201.log"
        if Path(default_path).exists():
            filepath = default_path
        else:
            print("Usage: python parse_tlc_bench.py <log_file>")
            sys.exit(1)
    else:
        filepath = sys.argv[1]
    
    print(f"Parsing: {filepath}\n")
    
    results = parse_log_file(filepath)
    report = generate_report(results)
    print(report)
    
    output_path = Path(filepath).parent / f"{Path(filepath).stem}_analysis.txt"
    with open(output_path, 'w') as f:
        f.write(report)
    print(f"\nSaved to: {output_path}")

if __name__ == "__main__":
    main()