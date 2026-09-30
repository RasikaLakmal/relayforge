// Package proxy implements request/response level forwarding: read one
// HTTP/1.1 request off the client connection at a time, round-robin to
// pick a healthy backend, forward the request to it as its own message,
// and write back the matching response. A TCP connection can carry many
// requests; this is what gives each of them its own boundary, which
// per-request routing, retries, and metrics need and a raw byte pipe
// cannot provide.
package proxy

import (
	"bufio"
	"errors"
	"log"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// defaultHealthCheckInterval and defaultHealthCheckTimeout apply whenever
// Server's corresponding fields are left at zero.
const (
	defaultHealthCheckInterval = 5 * time.Second
	defaultHealthCheckTimeout  = 2 * time.Second
)

// Server forwards every request to one of Backends, chosen in round-robin
// order among whichever of them the periodic health check currently
// considers reachable. A backend is assumed healthy until the first check
// says otherwise, so nothing is taken out of rotation before it has
// actually been probed. There is no connection pooling yet either: every
// request dials its own fresh backend connection rather than reusing one.
type Server struct {
	ListenAddr string
	Backends   []string

	// HealthCheckInterval is how often each backend is probed. Zero uses
	// defaultHealthCheckInterval.
	HealthCheckInterval time.Duration
	// HealthCheckTimeout bounds how long a single probe waits to connect.
	// Zero uses defaultHealthCheckTimeout.
	HealthCheckTimeout time.Duration

	next    uint64
	healthy []atomic.Bool
}

// ListenAndServe opens ListenAddr and serves it until Accept fails.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.ListenAddr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Serve accepts connections on an already-open listener. Split out from
// ListenAndServe so tests can bind an ephemeral port and hand the listener
// in directly.
func (s *Server) Serve(ln net.Listener) error {
	defer ln.Close()

	if len(s.Backends) == 0 {
		return errors.New("relayforge: at least one backend is required")
	}

	s.healthy = make([]atomic.Bool, len(s.Backends))
	for i := range s.healthy {
		s.healthy[i].Store(true)
	}
	go s.probeAllBackends()
	go s.runHealthChecks()

	log.Printf("relayforge: listening on %s, backends=%v (round robin)", ln.Addr(), s.Backends)

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

// runHealthChecks probes every backend on a fixed interval for as long as
// the server runs. There is no shutdown signal for this loop yet, it ends
// only when the process does, that is a known limitation until graceful
// shutdown exists.
func (s *Server) runHealthChecks() {
	interval := s.HealthCheckInterval
	if interval <= 0 {
		interval = defaultHealthCheckInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		s.probeAllBackends()
	}
}

func (s *Server) probeAllBackends() {
	for i, addr := range s.Backends {
		go s.probeBackend(i, addr)
	}
}

// probeBackend checks one backend by attempting a plain TCP connect, the
// simplest available signal that something is listening. It does not
// speak HTTP to the backend, an application-level health endpoint is a
// choice for a config-driven future milestone, not this one.
func (s *Server) probeBackend(idx int, addr string) {
	timeout := s.HealthCheckTimeout
	if timeout <= 0 {
		timeout = defaultHealthCheckTimeout
	}

	conn, err := net.DialTimeout("tcp", addr, timeout)
	isHealthy := err == nil
	if conn != nil {
		conn.Close()
	}

	wasHealthy := s.healthy[idx].Swap(isHealthy)
	if wasHealthy == isHealthy {
		return
	}
	if isHealthy {
		log.Printf("relayforge: backend %s passed its health check, back in rotation", addr)
	} else {
		log.Printf("relayforge: backend %s failed its health check, taking it out of rotation", addr)
	}
}

// nextBackend picks the next healthy backend address in round-robin
// order. It reports false if every backend currently looks unhealthy, in
// which case there is nothing to route to.
func (s *Server) nextBackend() (string, bool) {
	n := len(s.Backends)
	for i := 0; i < n; i++ {
		idx := (atomic.AddUint64(&s.next, 1) - 1) % uint64(n)
		if s.healthy[idx].Load() {
			return s.Backends[idx], true
		}
	}
	return "", false
}

func (s *Server) handleConn(client net.Conn) {
	defer client.Close()

	clientReader := bufio.NewReader(client)
	for s.forwardOneRequest(client, clientReader) {
	}
}

// forwardOneRequest reads one HTTP request off the client, round-robins to
// pick a healthy backend, dials a fresh connection to it for this request
// alone, and writes back the response. It reports whether the client
// connection should be used for another request: request/response close
// framing, no healthy backend being available, a failed backend dial, and
// a malformed or partial request all end it.
func (s *Server) forwardOneRequest(client net.Conn, clientReader *bufio.Reader) bool {
	req, err := http.ReadRequest(clientReader)
	if err != nil {
		return false
	}

	backendAddr, ok := s.nextBackend()
	if !ok {
		log.Printf("relayforge: no healthy backends available")
		return false
	}

	backend, err := net.Dial("tcp", backendAddr)
	if err != nil {
		log.Printf("relayforge: dial backend %s failed: %v", backendAddr, err)
		return false
	}
	defer backend.Close()

	if err := req.Write(backend); err != nil {
		log.Printf("relayforge: write to backend %s failed: %v", backendAddr, err)
		return false
	}

	resp, err := http.ReadResponse(bufio.NewReader(backend), req)
	if err != nil {
		log.Printf("relayforge: read from backend %s failed: %v", backendAddr, err)
		return false
	}
	defer resp.Body.Close()

	if err := resp.Write(client); err != nil {
		log.Printf("relayforge: write to client failed: %v", err)
		return false
	}

	return !req.Close && !resp.Close
}
