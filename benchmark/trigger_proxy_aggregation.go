// trigger_proxy_aggregation.go
//
// Concurrent request generator for exercising proxy/supernode batching with
// the UB vector engine.
//
// Typical usage:
//
//   go run ./benchmark/trigger_proxy_aggregation.go \
//     --redis-cli ./src/redis-cli \
//     --port 6391 \
//     --key myvectors \
//     --phase prefill \
//     --mode vemb \
//     -d 300 \
//     -p 100000
//
//   go run ./benchmark/trigger_proxy_aggregation.go \
//     --redis-cli ./src/redis-cli \
//     --port 6391 \
//     --key myvectors \
//     --phase query \
//     --mode vemb \
//     --raw \
//     -c 32 \
//     -n 128 \
//     -d 4 \
//     -p 64
//
//   go run ./benchmark/trigger_proxy_aggregation.go \
//     --redis-cli ./src/redis-cli \
//     --port 6391 \
//     --key myvectors \
//     --mode vsim \
//     -c 32 \
//     -n 128 \
//     -d 4 \
//     -p 64
//
// Notes:
// - In --phase all or --phase prefill, the script starts by FLUSHALL and
//   re-populates the target key.
// - --prefill-count controls how many vectors are inserted before issuing
//   concurrent VEMB/VSIM requests.
// - VSIM requests are sent with WITHSCORES.
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

type result struct {
	cmd       []string
	ok        bool
	latency   time.Duration
	response  string
	errString string
}

func buildRESP(args []string) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b.Bytes()
}

func readRESP(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) == 0 {
		return "", fmt.Errorf("empty RESP line")
	}
	prefix := line[0]
	switch prefix {
	case '+', '-', ':':
		return strings.TrimSpace(line[1:]), nil
	case '$':
		n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return "", err
		}
		if n < 0 {
			return "(nil)", nil
		}
		buf := make([]byte, n+2)
		if _, err := br.Read(buf); err != nil {
			return "", err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return "", err
		}
		parts := make([]string, 0, n)
		for i := 0; i < n; i++ {
			p, err := readRESP(br)
			if err != nil {
				return "", err
			}
			parts = append(parts, p)
		}
		return "[" + strings.Join(parts, ", ") + "]", nil
	default:
		return strings.TrimSpace(line), nil
	}
}

func redisCall(addr string, args []string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return "", err
	}
	if _, err := conn.Write(buildRESP(args)); err != nil {
		return "", err
	}
	return readRESP(bufio.NewReader(conn))
}

func redisCallCLI(redisCLI string, port int, args []string) (string, string, error) {
	cmd := append([]string{"-p", strconv.Itoa(port)}, args...)
	proc := execCommand(redisCLI, cmd)
	return proc.stdout, proc.stderr, proc.err
}

type cliResult struct {
	stdout string
	stderr string
	err    error
}

func execCommand(bin string, args []string) cliResult {
	cmd := exec.Command(bin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return cliResult{
		stdout: strings.TrimSpace(stdout.String()),
		stderr: strings.TrimSpace(stderr.String()),
		err:    err,
	}
}

func prefill(addr, redisCLI string, port int, key string, dim, count int) error {
	fmt.Printf("[setup] FLUSHALL\n")
	if redisCLI != "" {
		stdout, stderr, err := redisCallCLI(redisCLI, port, []string{"FLUSHALL"})
		if stdout != "" {
			fmt.Println(stdout)
		}
		if stderr != "" {
			fmt.Fprintln(os.Stderr, stderr)
		}
		if err != nil {
			return err
		}
	} else if _, err := redisCall(addr, []string{"FLUSHALL"}); err != nil {
		return err
	}
	fmt.Printf("[setup] inserting %d vectors into key=%s\n", count, key)
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

	for i := 0; i < count; i++ {
		args := []string{"VADD", key, "VALUES", strconv.Itoa(dim)}
		args = append(args, basis[i%dim]...)
		args = append(args, fmt.Sprintf("item:%d", i))
		if redisCLI != "" {
			stdout, stderr, err := redisCallCLI(redisCLI, port, args)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[setup] VADD failed: %s %s\n", redisCLI, strings.Join(append([]string{"-p", strconv.Itoa(port)}, args...), " "))
				if stdout != "" {
					fmt.Fprintln(os.Stderr, stdout)
				}
				if stderr != "" {
					fmt.Fprintln(os.Stderr, stderr)
				}
				return err
			}
			continue
		}
		if _, err := redisCall(addr, args); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	var redisCLI string
	var host string
	var port int
	var key string
	var phase string
	var mode string
	var concurrency int
	var requests int
	var dim int
	var prefillCount int

	flag.StringVar(&redisCLI, "redis-cli", "./src/redis-cli", "Path to redis-cli")
	flag.StringVar(&host, "host", "127.0.0.1", "Redis host")
	flag.IntVar(&port, "port", 6391, "Redis port")
	flag.StringVar(&key, "key", "myvectors", "Vector key")
	flag.StringVar(&phase, "phase", "all", "Phase: prefill|query|all")
	flag.StringVar(&mode, "mode", "vemb", "Mode: vemb|vsim")
	flag.IntVar(&concurrency, "concurrency", 32, "Concurrent connections")
	flag.IntVar(&concurrency, "c", 32, "Concurrent connections (short form)")
	flag.IntVar(&requests, "requests", 128, "Total requests")
	flag.IntVar(&requests, "n", 128, "Total requests (short form)")
	flag.IntVar(&dim, "dim", 4, "Vector dimension")
	flag.IntVar(&dim, "d", 4, "Vector dimension (short form)")
	flag.IntVar(&prefillCount, "prefill-count", 64, "Prefill vector count")
	flag.IntVar(&prefillCount, "p", 64, "Prefill vector count (short form)")
	raw := flag.Bool("raw", false, "Use RAW for VEMB")
	flag.Parse()

	addr := fmt.Sprintf("%s:%d", host, port)

	if redisCLI != "" {
		if _, err := os.Stat(redisCLI); err != nil {
			fmt.Fprintf(os.Stderr, "redis-cli not found: %s\n", redisCLI)
			os.Exit(1)
		}
	}

	switch phase {
	case "prefill", "all":
		fmt.Printf("[setup] prefill key=%s dim=%d count=%d\n", key, dim, prefillCount)
		if err := prefill(addr, redisCLI, port, key, dim, prefillCount); err != nil {
			fmt.Fprintf(os.Stderr, "prefill failed: %v\n", err)
			os.Exit(1)
		}
		if phase == "prefill" {
			fmt.Println("")
			fmt.Println("=== Prefill Summary ===")
			fmt.Printf("key:        %s\n", key)
			fmt.Printf("dim:        %d\n", dim)
			fmt.Printf("inserted:   %d\n", prefillCount)
			return
		}
	case "query":
		// Query-only mode assumes data is already present.
	default:
		fmt.Fprintf(os.Stderr, "unknown phase: %s\n", phase)
		os.Exit(1)
	}

	var jobs [][]string
	switch mode {
	case "vemb":
		for i := 0; i < requests; i++ {
			args := []string{"VEMB", key, fmt.Sprintf("item:%d", i%prefillCount)}
			if *raw {
				args = append(args, "RAW")
			}
			jobs = append(jobs, args)
		}
	case "vsim":
		query := []string{"1"}
		for i := 1; i < dim; i++ {
			query = append(query, "0")
		}
		for i := 0; i < requests; i++ {
			args := []string{"VSIM", key, "VALUES", strconv.Itoa(dim)}
			args = append(args, query...)
			args = append(args, "WITHSCORES")
			jobs = append(jobs, args)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", mode)
		os.Exit(1)
	}

	fmt.Printf("[run] mode=%s requests=%d concurrency=%d dim=%d key=%s addr=%s\n",
		mode, requests, concurrency, dim, key, addr)

	sem := make(chan struct{}, concurrency)
	results := make(chan result, len(jobs))
	var wg sync.WaitGroup

	startWall := time.Now()
	for _, args := range jobs {
		wg.Add(1)
		go func(a []string) {
			defer wg.Done()
			sem <- struct{}{}
			start := time.Now()
			resp, err := redisCall(addr, a)
			lat := time.Since(start)
			<-sem
			if err != nil {
				results <- result{cmd: a, ok: false, latency: lat, errString: err.Error()}
				return
			}
			results <- result{cmd: a, ok: true, latency: lat, response: resp}
		}(args)
	}

	wg.Wait()
	close(results)
	totalWall := time.Since(startWall)

	var ok, fail int
	var totalLat time.Duration
	samples := 0
	var firstFailures []string

	for r := range results {
		totalLat += r.latency
		samples++
		if r.ok {
			ok++
		} else {
			fail++
			if len(firstFailures) < 5 {
				msg := fmt.Sprintf("cmd: %s\nstderr: %s", strings.Join(r.cmd, " "), r.errString)
				firstFailures = append(firstFailures, msg)
			}
		}
	}

	avgMs := 0.0
	if samples > 0 {
		avgMs = float64(totalLat.Microseconds()) / 1000.0 / float64(samples)
	}
	qps := 0.0
	if totalWall > 0 {
		qps = float64(samples) / totalWall.Seconds()
	}

	fmt.Println("")
	fmt.Println("=== Aggregation Trigger Summary ===")
	fmt.Printf("dim:        %d\n", dim)
	fmt.Printf("successful: %d\n", ok)
	fmt.Printf("failed:     %d\n", fail)
	fmt.Printf("avg ms/op:  %.2f\n", avgMs)
	fmt.Printf("wall qps:   %.2f\n", qps)
	if len(firstFailures) > 0 {
		fmt.Println("")
		fmt.Println("=== Sample Failures ===")
		for _, f := range firstFailures {
			fmt.Println(f)
		}
	}

	fmt.Println("")
	fmt.Println("Suggested checks:")
	fmt.Println("  tail -n 100 /tmp/redis-vemb-test/redis.log")
	fmt.Println("  tail -n 100 /tmp/redis-vsim-test/redis.log")
	fmt.Println("Look for worker/proxy batch sizes > 1.")
}
