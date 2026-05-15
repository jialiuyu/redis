package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type result struct {
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

func prefill(addr, key string, dim, count int) error {
	if _, err := redisCall(addr, []string{"FLUSHALL"}); err != nil {
		return err
	}
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
		if _, err := redisCall(addr, args); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	host := flag.String("host", "127.0.0.1", "Redis host")
	port := flag.Int("port", 6391, "Redis port")
	key := flag.String("key", "myvectors", "Vector key")
	mode := flag.String("mode", "vemb", "Mode: vemb|vemb-raw|vsim|vsim-withscores")
	concurrency := flag.Int("concurrency", 32, "Concurrent connections")
	requests := flag.Int("requests", 128, "Total requests")
	dim := flag.Int("dim", 4, "Vector dimension")
	prefillCount := flag.Int("prefill-count", 64, "Prefill vector count")
	flag.Parse()

	addr := fmt.Sprintf("%s:%d", *host, *port)

	fmt.Printf("[setup] prefill key=%s dim=%d count=%d\n", *key, *dim, *prefillCount)
	if err := prefill(addr, *key, *dim, *prefillCount); err != nil {
		fmt.Fprintf(os.Stderr, "prefill failed: %v\n", err)
		os.Exit(1)
	}

	var jobs [][]string
	switch *mode {
	case "vemb":
		for i := 0; i < *requests; i++ {
			jobs = append(jobs, []string{"VEMB", *key, fmt.Sprintf("item:%d", i%*prefillCount)})
		}
	case "vemb-raw":
		for i := 0; i < *requests; i++ {
			jobs = append(jobs, []string{"VEMB", *key, fmt.Sprintf("item:%d", i%*prefillCount), "RAW"})
		}
	case "vsim":
		query := []string{"1"}
		for i := 1; i < *dim; i++ {
			query = append(query, "0")
		}
		for i := 0; i < *requests; i++ {
			args := []string{"VSIM", *key, "VALUES", strconv.Itoa(*dim)}
			args = append(args, query...)
			jobs = append(jobs, args)
		}
	case "vsim-withscores":
		query := []string{"1"}
		for i := 1; i < *dim; i++ {
			query = append(query, "0")
		}
		for i := 0; i < *requests; i++ {
			args := []string{"VSIM", *key, "VALUES", strconv.Itoa(*dim)}
			args = append(args, query...)
			args = append(args, "WITHSCORES")
			jobs = append(jobs, args)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", *mode)
		os.Exit(1)
	}

	fmt.Printf("[run] mode=%s requests=%d concurrency=%d key=%s addr=%s\n",
		*mode, *requests, *concurrency, *key, addr)

	sem := make(chan struct{}, *concurrency)
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
				results <- result{ok: false, latency: lat, errString: err.Error()}
				return
			}
			results <- result{ok: true, latency: lat, response: resp}
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
				firstFailures = append(firstFailures, r.errString)
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
