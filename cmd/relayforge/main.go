// Command relayforge is a TCP reverse proxy. This is the milestone-0
// skeleton: one listen address, one fixed backend, no HTTP awareness, no
// routing. Everything else in the project builds on top of it.
package main

import (
	"flag"
	"log"

	"github.com/RasikaLakmal/relayforge/internal/proxy"
)

func main() {
	listenAddr := flag.String("listen", ":8080", "address to listen on")
	backendAddr := flag.String("backend", "", "backend address to forward every connection to")
	flag.Parse()

	if *backendAddr == "" {
		log.Fatal("relayforge: -backend is required")
	}

	srv := &proxy.Server{
		ListenAddr: *listenAddr,
		Backend:    *backendAddr,
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("relayforge: %v", err)
	}
}
