// Command relayforge is an HTTP/1.1 reverse proxy and load balancer.
package main

import (
	"flag"
	"log"
	"strings"
	"time"

	"github.com/RasikaLakmal/relayforge/internal/proxy"
)

func main() {
	listenAddr := flag.String("listen", ":8080", "address to listen on")
	backendsFlag := flag.String("backends", "", "comma-separated list of backend addresses to load-balance across")
	strategy := flag.String("strategy", proxy.StrategyRoundRobin, "load-balancing strategy: round-robin or least-connections")
	healthInterval := flag.Duration("health-interval", 5*time.Second, "how often to probe each backend")
	healthTimeout := flag.Duration("health-timeout", 2*time.Second, "how long a single health probe waits to connect")
	connectTimeout := flag.Duration("connect-timeout", 3*time.Second, "how long dialing a backend may take")
	headerTimeout := flag.Duration("header-timeout", 10*time.Second, "how long a connection may wait for its next request, and how long that request has to finish once it starts arriving")
	responseTimeout := flag.Duration("response-timeout", 30*time.Second, "how long the whole exchange with a backend (request write plus response read) may take")
	flag.Parse()

	if *backendsFlag == "" {
		log.Fatal("relayforge: -backends is required")
	}

	srv := &proxy.Server{
		ListenAddr:          *listenAddr,
		Backends:            strings.Split(*backendsFlag, ","),
		Strategy:            *strategy,
		HealthCheckInterval: *healthInterval,
		HealthCheckTimeout:  *healthTimeout,
		ConnectTimeout:      *connectTimeout,
		HeaderTimeout:       *headerTimeout,
		ResponseTimeout:     *responseTimeout,
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("relayforge: %v", err)
	}
}
