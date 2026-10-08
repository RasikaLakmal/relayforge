// Command loadgen drives concurrent HTTP load against a target address
// for a fixed duration and reports throughput, latency percentiles, and
// an outcome breakdown (success, 503 rejection, other failure). Each
// worker opens its own fresh connection per request, deliberately
// mirroring many independent clients rather than a handful of persistent
// ones, since that is what "N concurrent connections" means in the
// benchmark this exists for. If -metrics is set, it also scrapes that
// address's /metrics once after the run, for the goroutine/memory/
// connection numbers a load test cannot see from the outside.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	target := flag.String("target", "127.0.0.1:8080", "address to send requests to")
	concurrency := flag.Int("concurrency", 100, "number of concurrent workers, each its own connection per request")
	duration := flag.Duration("duration", 5*time.Second, "how long to generate load")
	path := flag.String("path", "/", "request path")
	connectTimeout := flag.Duration("connect-timeout", 5*time.Second, "how long a single connect attempt may take before counting as failed")
	metricsAddr := flag.String("metrics", "", "optional metrics address to scrape once after the run")
	flag.Parse()

	var total, ok, rejected, failed int64
	var latMu sync.Mutex
	var latencies []time.Duration

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: bench\r\n\r\n", *path)
			for {
				select {
				case <-stop:
					return
				default:
				}

				start := time.Now()
				conn, err := net.DialTimeout("tcp", *target, *connectTimeout)
				if err != nil {
					atomic.AddInt64(&failed, 1)
					atomic.AddInt64(&total, 1)
					continue
				}
				conn.SetDeadline(time.Now().Add(10 * time.Second))

				if _, err := conn.Write([]byte(req)); err != nil {
					conn.Close()
					atomic.AddInt64(&failed, 1)
					atomic.AddInt64(&total, 1)
					continue
				}
				resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "GET"})
				if err != nil {
					conn.Close()
					atomic.AddInt64(&failed, 1)
					atomic.AddInt64(&total, 1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				conn.Close()
				elapsed := time.Since(start)

				atomic.AddInt64(&total, 1)
				switch {
				case resp.StatusCode == http.StatusServiceUnavailable:
					atomic.AddInt64(&rejected, 1)
				case resp.StatusCode >= 200 && resp.StatusCode < 300:
					atomic.AddInt64(&ok, 1)
					latMu.Lock()
					latencies = append(latencies, elapsed)
					latMu.Unlock()
				default:
					atomic.AddInt64(&failed, 1)
				}
			}
		}()
	}

	benchStart := time.Now()
	time.Sleep(*duration)
	close(stop)
	wg.Wait()
	elapsedTotal := time.Since(benchStart)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		idx := int(p * float64(len(latencies)-1))
		return latencies[idx]
	}

	fmt.Printf("target=%s concurrency=%d duration=%s\n", *target, *concurrency, *duration)
	fmt.Printf("total=%d ok=%d rejected_503=%d failed=%d\n", total, ok, rejected, failed)
	if elapsedTotal.Seconds() > 0 {
		fmt.Printf("throughput=%.1f req/s\n", float64(total)/elapsedTotal.Seconds())
	}
	fmt.Printf("latency(ok only) p50=%v p95=%v p99=%v\n", pct(0.50), pct(0.95), pct(0.99))

	if *metricsAddr != "" {
		resp, err := http.Get("http://" + *metricsAddr + "/metrics")
		if err != nil {
			fmt.Printf("metrics scrape failed: %v\n", err)
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("metrics scrape read failed: %v\n", err)
			return
		}
		fmt.Println("--- metrics snapshot ---")
		fmt.Print(string(body))
	}
}
