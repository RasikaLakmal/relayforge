// Command naive-proxy is a deliberately simple reverse proxy, used only
// as a baseline for relayforge's benchmarks. It is HTTP-aware and
// round-robins across backends (the same as relayforge), but has none of
// the optimizations or resilience relayforge adds afterward: no
// connection pooling (a fresh backend dial on every single request), no
// health checks (a dead backend is tried like any other), and no
// retries (a failed request just fails). It exists specifically to show
// what milestones 3-5 actually bought, not to be a usable proxy on its
// own.
package main

import (
	"bufio"
	"flag"
	"log"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
)

func main() {
	listenAddr := flag.String("listen", ":8081", "address to listen on")
	backendsFlag := flag.String("backends", "", "comma-separated backend addresses")
	flag.Parse()
	if *backendsFlag == "" {
		log.Fatal("naive-proxy: -backends is required")
	}
	backends := strings.Split(*backendsFlag, ",")

	ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("naive-proxy: %v", err)
	}
	log.Printf("naive-proxy: listening on %s, backends=%v (round robin, no pooling, no health checks, no retries)", *listenAddr, backends)

	var next uint64
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalf("naive-proxy: accept: %v", err)
		}
		go handleConn(conn, backends, &next)
	}
}

func handleConn(client net.Conn, backends []string, next *uint64) {
	defer client.Close()
	clientReader := bufio.NewReader(client)
	for {
		req, err := http.ReadRequest(clientReader)
		if err != nil {
			return
		}

		idx := atomic.AddUint64(next, 1) - 1
		backendAddr := backends[idx%uint64(len(backends))]

		// Fresh dial every request: no pooling.
		backend, err := net.Dial("tcp", backendAddr)
		if err != nil {
			// No retry against another backend. Deliberately not logged
			// here: under real load this fails often enough (ephemeral
			// port exhaustion from the dial-per-request churn) that
			// per-failure logging would itself become a throughput-
			// skewing confound in a benchmark. The failure already shows
			// up in loadgen's own failure count.
			return
		}

		if err := req.Write(backend); err != nil {
			backend.Close()
			return
		}
		resp, err := http.ReadResponse(bufio.NewReader(backend), req)
		if err != nil {
			backend.Close()
			return
		}
		writeErr := resp.Write(client)
		resp.Body.Close()
		backend.Close() // always closed: no pooling
		if writeErr != nil {
			return
		}

		if req.Close || resp.Close {
			return
		}
	}
}
