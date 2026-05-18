// vemb_fc_server_bench.go
//
// FC server-side benchmark driver. Start Redis with
// benchmark/redis-vemb-fc-bench.conf, then run:
//
//	go run ./benchmark/vemb_fc_server_bench.go
//
// The program talks RESP directly to Redis: it can prefill vectors, sweep VEMB
// RAW query concurrency, and save VENGINE STATS snapshots into a result dir.
package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type redisValue struct {
	kind  byte
	str   string
	array []redisValue
}

type benchResult struct {
	name         string
	concurrency  int
	requests     int
	ok           int64
	fail         int64
	avgLatency   time.Duration
	p50Latency   time.Duration
	p95Latency   time.Duration
	p99Latency   time.Duration
	maxLatency   time.Duration
	wallDuration time.Duration
	qps          float64
	sampleErrors []string
}

type redisConn struct {
	conn net.Conn
	br   *bufio.Reader
}

func buildRESP(args []string) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b.Bytes()
}

func readRESP(br *bufio.Reader) (redisValue, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return redisValue{}, err
	}
	if len(line) == 0 {
		return redisValue{}, fmt.Errorf("empty RESP line")
	}

	prefix := line[0]
	payload := strings.TrimRight(line[1:], "\r\n")
	switch prefix {
	case '+', '-', ':':
		return redisValue{kind: prefix, str: payload}, nil
	case ',':
		return redisValue{kind: prefix, str: payload}, nil
	case '$':
		n, err := strconv.Atoi(payload)
		if err != nil {
			return redisValue{}, err
		}
		if n < 0 {
			return redisValue{kind: prefix, str: ""}, nil
		}
		buf := make([]byte, n+2)
		if _, err := br.Read(buf); err != nil {
			return redisValue{}, err
		}
		return redisValue{kind: prefix, str: string(buf[:n])}, nil
	case '*':
		n, err := strconv.Atoi(payload)
		if err != nil {
			return redisValue{}, err
		}
		if n < 0 {
			return redisValue{kind: prefix}, nil
		}
		items := make([]redisValue, 0, n)
		for i := 0; i < n; i++ {
			v, err := readRESP(br)
			if err != nil {
				return redisValue{}, err
			}
			items = append(items, v)
		}
		return redisValue{kind: prefix, array: items}, nil
	default:
		return redisValue{}, fmt.Errorf("unknown RESP prefix %q in %q", prefix, strings.TrimSpace(line))
	}
}

func (v redisValue) String() string {
	switch v.kind {
	case '*':
		parts := make([]string, 0, len(v.array))
		for _, item := range v.array {
			parts = append(parts, item.String())
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case '-':
		return "ERR " + v.str
	default:
		return v.str
	}
}

func newRedisConn(addr string, timeout time.Duration) (*redisConn, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return &redisConn{conn: conn, br: bufio.NewReader(conn)}, nil
}

func (rc *redisConn) close() {
	if rc != nil && rc.conn != nil {
		_ = rc.conn.Close()
	}
}

func (rc *redisConn) call(timeout time.Duration, args ...string) (redisValue, error) {
	if err := rc.conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return redisValue{}, err
	}
	if _, err := rc.conn.Write(buildRESP(args)); err != nil {
		return redisValue{}, err
	}
	resp, err := readRESP(rc.br)
	if err != nil {
		return redisValue{}, err
	}
	if resp.kind == '-' {
		return resp, errors.New(resp.str)
	}
	return resp, nil
}

func redisCall(addr string, timeout time.Duration, args ...string) (redisValue, error) {
	rc, err := newRedisConn(addr, timeout)
	if err != nil {
		return redisValue{}, err
	}
	defer rc.close()
	return rc.call(timeout, args...)
}

func ensureOK(addr string, timeout time.Duration) error {
	if _, err := redisCall(addr, timeout, "PING"); err != nil {
		return err
	}
	resp, err := redisCall(addr, timeout, "VENGINE", "GET")
	if err != nil {
		return err
	}
	if !strings.Contains(resp.String(), "UB") {
		return fmt.Errorf("expected VENGINE GET to include UB, got %q", resp.String())
	}
	return nil
}

func writeFile(path, content string) {
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s failed: %v\n", path, err)
	}
}

func captureStats(addr string, timeout time.Duration, outDir, tag string) {
	resp, err := redisCall(addr, timeout, "VENGINE", "STATS")
	if err != nil {
		writeFile(filepath.Join(outDir, tag+".vengine_stats.txt"), "ERR "+err.Error()+"\n")
		return
	}

	stats := resp.String()
	writeFile(filepath.Join(outDir, tag+".vengine_stats.txt"), stats)

	var filtered []string
	for _, line := range strings.Split(stats, "\n") {
		if keepStatsLine(line) {
			filtered = append(filtered, line)
		}
	}
	writeFile(filepath.Join(outDir, tag+".summary.txt"), strings.Join(filtered, "\n")+"\n")
}

func keepStatsLine(line string) bool {
	patterns := []string{
		"Proxy Aggregator",
		"SuperNode",
		"Total requests",
		"Active buckets",
		"Active bucket peak",
		"Total flushes",
		"Batch full flushes",
		"Timeout flushes",
		"Batch flush requests",
		"Batch size histogram",
		"Immediate flush",
		"Enqueue rejections",
		"VEMB direct",
		"VSIM direct",
		"VEMB batch",
		"VEMB adaptive",
		"Direct ring full",
		"VEMB FC",
		"Average submit size",
		"Average batch",
		"Average VEMB FC",
		"Worker ",
		"Batches:",
		"Requests:",
		"Avg queue",
		"Max queue",
		"Avg gather",
		"Avg compute",
		"Avg response",
		"VEMB scratch",
	}
	for _, p := range patterns {
		if strings.Contains(line, p) {
			return true
		}
	}
	return false
}

func prefill(addr, key string, dim, count int, timeout time.Duration, flush bool) error {
	if flush {
		fmt.Println("[setup] FLUSHALL")
		if _, err := redisCall(addr, timeout, "FLUSHALL"); err != nil {
			return err
		}
	}

	fmt.Printf("[setup] inserting %d vectors into key=%s dim=%d\n", count, key, dim)
	basis := make([][]string, dim)
	for i := 0; i < dim; i++ {
		vec := make([]string, dim)
		for j := 0; j < dim; j++ {
			if i == j {
				vec[j] = "1"
			} else {
				vec[j] = "0"
			}
		}
		basis[i] = vec
	}

	rc, err := newRedisConn(addr, timeout)
	if err != nil {
		return err
	}
	defer rc.close()

	start := time.Now()
	for i := 0; i < count; i++ {
		args := []string{"VADD", key, "VALUES", strconv.Itoa(dim)}
		args = append(args, basis[i%dim]...)
		args = append(args, fmt.Sprintf("item:%d", i))
		if _, err := rc.call(timeout, args...); err != nil {
			return fmt.Errorf("VADD item:%d failed: %w", i, err)
		}
		if (i+1)%10000 == 0 {
			fmt.Printf("[setup] inserted=%d elapsed=%s\n", i+1, time.Since(start).Round(time.Millisecond))
		}
	}
	fmt.Printf("[setup] inserted=%d elapsed=%s\n", count, time.Since(start).Round(time.Millisecond))
	return nil
}

func runVemb(addr, key string, prefillCount, concurrency, requests int, timeout time.Duration) benchResult {
	name := fmt.Sprintf("fc_c%d_n%d", concurrency, requests)
	latencies := make([]time.Duration, requests)
	var latIndex atomic.Int64
	var ok atomic.Int64
	var fail atomic.Int64
	var errMu sync.Mutex
	var sampleErrors []string
	var wg sync.WaitGroup

	startWall := time.Now()
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rc, err := newRedisConn(addr, timeout)
			if err != nil {
				localFail := int64(0)
				for i := workerID; i < requests; i += concurrency {
					localFail++
				}
				fail.Add(localFail)
				errMu.Lock()
				if len(sampleErrors) < 5 {
					sampleErrors = append(sampleErrors, err.Error())
				}
				errMu.Unlock()
				return
			}
			defer rc.close()

			for i := workerID; i < requests; i += concurrency {
				element := fmt.Sprintf("item:%d", i%prefillCount)
				start := time.Now()
				_, err := rc.call(timeout, "VEMB", key, element, "RAW")
				lat := time.Since(start)
				idx := latIndex.Add(1) - 1
				if int(idx) < len(latencies) {
					latencies[idx] = lat
				}
				if err != nil {
					fail.Add(1)
					errMu.Lock()
					if len(sampleErrors) < 5 {
						sampleErrors = append(sampleErrors, fmt.Sprintf("VEMB %s RAW: %v", element, err))
					}
					errMu.Unlock()
					continue
				}
				ok.Add(1)
			}
		}(worker)
	}
	wg.Wait()
	wall := time.Since(startWall)

	used := int(latIndex.Load())
	if used > len(latencies) {
		used = len(latencies)
	}
	latencies = latencies[:used]
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	total := time.Duration(0)
	for _, lat := range latencies {
		total += lat
	}
	avg := time.Duration(0)
	if len(latencies) > 0 {
		avg = total / time.Duration(len(latencies))
	}

	qps := 0.0
	if wall > 0 {
		qps = float64(requests) / wall.Seconds()
	}

	return benchResult{
		name:         name,
		concurrency:  concurrency,
		requests:     requests,
		ok:           ok.Load(),
		fail:         fail.Load(),
		avgLatency:   avg,
		p50Latency:   percentile(latencies, 50),
		p95Latency:   percentile(latencies, 95),
		p99Latency:   percentile(latencies, 99),
		maxLatency:   maxLatency(latencies),
		wallDuration: wall,
		qps:          qps,
		sampleErrors: sampleErrors,
	}
}

func percentile(values []time.Duration, pct float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	rank := int(math.Ceil((pct / 100.0) * float64(len(values))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(values) {
		rank = len(values)
	}
	return values[rank-1]
}

func maxLatency(values []time.Duration) time.Duration {
	if len(values) == 0 {
		return 0
	}
	return values[len(values)-1]
}

func parseConcurrencies(s string) ([]int, error) {
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	})
	var out []int
	for _, f := range fields {
		if f == "" {
			continue
		}
		v, err := strconv.Atoi(f)
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("invalid concurrency %q", f)
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty concurrency list")
	}
	return out, nil
}

func formatDuration(d time.Duration) string {
	if d == 0 {
		return "0"
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.3fus", float64(d.Nanoseconds())/1000.0)
	}
	return fmt.Sprintf("%.3fms", float64(d.Microseconds())/1000.0)
}

func writeRunLog(outDir string, r benchResult) {
	var b strings.Builder
	fmt.Fprintf(&b, "name:       %s\n", r.name)
	fmt.Fprintf(&b, "concurrency:%d\n", r.concurrency)
	fmt.Fprintf(&b, "requests:   %d\n", r.requests)
	fmt.Fprintf(&b, "successful: %d\n", r.ok)
	fmt.Fprintf(&b, "failed:     %d\n", r.fail)
	fmt.Fprintf(&b, "avg:        %s\n", formatDuration(r.avgLatency))
	fmt.Fprintf(&b, "p50:        %s\n", formatDuration(r.p50Latency))
	fmt.Fprintf(&b, "p95:        %s\n", formatDuration(r.p95Latency))
	fmt.Fprintf(&b, "p99:        %s\n", formatDuration(r.p99Latency))
	fmt.Fprintf(&b, "max:        %s\n", formatDuration(r.maxLatency))
	fmt.Fprintf(&b, "wall:       %s\n", r.wallDuration.Round(time.Millisecond))
	fmt.Fprintf(&b, "wall qps:   %.2f\n", r.qps)
	if len(r.sampleErrors) > 0 {
		fmt.Fprintf(&b, "\nerrors:\n")
		for _, e := range r.sampleErrors {
			fmt.Fprintf(&b, "- %s\n", e)
		}
	}
	writeFile(filepath.Join(outDir, r.name+".log"), b.String())
}

func main() {
	var host string
	var port int
	var key string
	var dim int
	var prefillCount int
	var requests int
	var concurrencyList string
	var outDir string
	var skipPrefill bool
	var flush bool
	var timeout time.Duration

	flag.StringVar(&host, "host", "127.0.0.1", "Redis host")
	flag.IntVar(&port, "port", 6391, "Redis port")
	flag.StringVar(&key, "key", "myvectors", "Vector key")
	flag.IntVar(&dim, "dim", 4, "Vector dimension")
	flag.IntVar(&dim, "d", 4, "Vector dimension (short)")
	flag.IntVar(&prefillCount, "prefill", 65536, "Number of vectors to prefill")
	flag.IntVar(&prefillCount, "p", 65536, "Number of vectors to prefill (short)")
	flag.IntVar(&requests, "requests", 200000, "Requests per concurrency point")
	flag.IntVar(&requests, "n", 200000, "Requests per concurrency point (short)")
	flag.StringVar(&concurrencyList, "concurrency", "16,32,64,128", "Comma or space separated concurrency list")
	flag.StringVar(&concurrencyList, "c", "16,32,64,128", "Concurrency list (short)")
	flag.StringVar(&outDir, "out", "", "Output directory")
	flag.BoolVar(&skipPrefill, "skip-prefill", false, "Do not prefill; assume data already exists")
	flag.BoolVar(&flush, "flush", true, "FLUSHALL before prefill")
	flag.DurationVar(&timeout, "timeout", 10*time.Second, "Per-command timeout")
	flag.Parse()

	addr := fmt.Sprintf("%s:%d", host, port)
	if outDir == "" {
		outDir = filepath.Join("benchmark", "results", "vemb_fc_"+time.Now().Format("20060102_150405"))
	}
	if err := os.MkdirAll(outDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir %s failed: %v\n", outDir, err)
		os.Exit(1)
	}

	concurrencies, err := parseConcurrencies(concurrencyList)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if prefillCount <= 0 || requests <= 0 || dim <= 0 {
		fmt.Fprintln(os.Stderr, "dim, prefill, and requests must be positive")
		os.Exit(1)
	}

	if err := ensureOK(addr, timeout); err != nil {
		fmt.Fprintf(os.Stderr, "Redis FC server is not ready at %s: %v\n", addr, err)
		fmt.Fprintln(os.Stderr, "Start it with: ./src/redis-server benchmark/redis-vemb-fc-bench.conf")
		os.Exit(1)
	}

	config := fmt.Sprintf(
		"addr=%s\nkey=%s\ndim=%d\nprefill=%d\nrequests=%d\nconcurrency=%s\nskip_prefill=%v\nflush=%v\ntimeout=%s\n",
		addr, key, dim, prefillCount, requests, concurrencyList, skipPrefill, flush, timeout)
	writeFile(filepath.Join(outDir, "config.txt"), config)

	if !skipPrefill {
		if err := prefill(addr, key, dim, prefillCount, timeout, flush); err != nil {
			fmt.Fprintf(os.Stderr, "prefill failed: %v\n", err)
			os.Exit(1)
		}
		captureStats(addr, timeout, outDir, "after_prefill")
	}

	var summary strings.Builder
	fmt.Fprintf(&summary, "name,concurrency,requests,successful,failed,avg,p50,p95,p99,max,wall,qps\n")

	for _, c := range concurrencies {
		fmt.Printf("[run] VEMB RAW concurrency=%d requests=%d\n", c, requests)
		r := runVemb(addr, key, prefillCount, c, requests, timeout)
		writeRunLog(outDir, r)
		captureStats(addr, timeout, outDir, r.name)

		fmt.Printf("[done] c=%d ok=%d fail=%d avg=%s p95=%s p99=%s qps=%.2f\n",
			c, r.ok, r.fail, formatDuration(r.avgLatency), formatDuration(r.p95Latency),
			formatDuration(r.p99Latency), r.qps)
		fmt.Fprintf(&summary, "%s,%d,%d,%d,%d,%s,%s,%s,%s,%s,%s,%.2f\n",
			r.name, r.concurrency, r.requests, r.ok, r.fail,
			formatDuration(r.avgLatency), formatDuration(r.p50Latency),
			formatDuration(r.p95Latency), formatDuration(r.p99Latency),
			formatDuration(r.maxLatency), r.wallDuration.Round(time.Millisecond), r.qps)
	}

	writeFile(filepath.Join(outDir, "summary.csv"), summary.String())
	fmt.Printf("results: %s\n", outDir)
}
