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
	backendsFlag := flag.String("backends", "", "comma-separated list of backend addresses to round-robin across")
	healthInterval := flag.Duration("health-interval", 5*time.Second, "how often to probe each backend")
	healthTimeout := flag.Duration("health-timeout", 2*time.Second, "how long a single health probe waits to connect")
	flag.Parse()

	if *backendsFlag == "" {
		log.Fatal("relayforge: -backends is required")
	}

	srv := &proxy.Server{
		ListenAddr:          *listenAddr,
		Backends:            strings.Split(*backendsFlag, ","),
		HealthCheckInterval: *healthInterval,
		HealthCheckTimeout:  *healthTimeout,
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("relayforge: %v", err)
	}
}
