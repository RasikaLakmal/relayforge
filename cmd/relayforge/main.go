// Command relayforge is an HTTP/1.1 reverse proxy and load balancer.
package main

import (
	"flag"
	"log"
	"strings"

	"github.com/RasikaLakmal/relayforge/internal/proxy"
)

func main() {
	listenAddr := flag.String("listen", ":8080", "address to listen on")
	backendsFlag := flag.String("backends", "", "comma-separated list of backend addresses to round-robin across")
	flag.Parse()

	if *backendsFlag == "" {
		log.Fatal("relayforge: -backends is required")
	}

	srv := &proxy.Server{
		ListenAddr: *listenAddr,
		Backends:   strings.Split(*backendsFlag, ","),
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("relayforge: %v", err)
	}
}
