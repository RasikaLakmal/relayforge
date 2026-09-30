// Package proxy implements request/response level forwarding: read one
// HTTP/1.1 request off the client connection at a time, round-robin to
// pick a healthy backend, forward the request to it over a pooled or
// freshly dialed connection, and write back the matching response. A TCP
// connection can carry many requests; this is what gives each of them its
// own boundary, which per-request routing, retries, metrics, and backend
// connection reuse all need and a raw byte pipe cannot provide.
package proxy

import (
	"bufio"
	"errors"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// defaultHealthCheckInterval and defaultHealthCheckTimeout apply whenever
// Server's corresponding fields are left at zero. maxIdleConnsPerBackend
// bounds how many idle backend connections are kept around per backend;
// it is a fixed constant rather than a flag for now, there is no
// benchmark yet that would justify exposing it as one.
const (
	defaultHealthCheckInterval = 5 * time.Second
	defaultHealthCheckTimeout  = 2 * time.Second
	maxIdleConnsPerBackend     = 8
)

// Server forwards every request to one of Backends, chosen in round-robin
// order among whichever of them the periodic health check currently
// considers reachable. A backend is assumed healthy until the first check
// says otherwise, so nothing is taken out of rotation before it has
// actually been probed. A backend connection that finishes a request
// cleanly (no error, no Connection: close) is kept idle and reused by a
// later request to the same backend rather than being dialed again from
// scratch, regardless of which client connection that later request
// arrives on.
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

	idleMu sync.Mutex
	idle   map[string][]net.Conn
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
	s.idle = make(map[string][]net.Conn)
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

// takeIdleConn returns a pooled, idle connection to addr if one is
// available, or nil if the caller should dial a fresh one.
func (s *Server) takeIdleConn(addr string) net.Conn {
	s.idleMu.Lock()
	defer s.idleMu.Unlock()
	conns := s.idle[addr]
	if len(conns) == 0 {
		return nil
	}
	conn := conns[len(conns)-1]
	s.idle[addr] = conns[:len(conns)-1]
	return conn
}

// putIdleConn returns conn to addr's idle pool for reuse by a later
// request, unless the pool for that backend is already at capacity, in
// which case conn is simply closed instead of accumulating unboundedly.
func (s *Server) putIdleConn(addr string, conn net.Conn) {
	s.idleMu.Lock()
	full := len(s.idle[addr]) >= maxIdleConnsPerBackend
	if !full {
		s.idle[addr] = append(s.idle[addr], conn)
	}
	s.idleMu.Unlock()

	if full {
		conn.Close()
	}
}

func (s *Server) handleConn(client net.Conn) {
	defer client.Close()

	clientReader := bufio.NewReader(client)
	for s.forwardOneRequest(client, clientReader) {
	}
}

// forwardOneRequest reads one HTTP request off the client, round-robins to
// pick a healthy backend, gets a connection to it (reused from the idle
// pool if one is available, freshly dialed otherwise), and writes back the
// response. It reports whether the client connection should be used for
// another request: request/response close framing, no healthy backend
// being available, a failed backend dial, and a malformed or partial
// request all end it.
//
// A pooled connection can go stale between being returned and being
// picked up again, the backend is free to close an idle connection on its
// own schedule, and the pool has no way to know until it actually tries to
// use it. That failure is not retried against a fresh connection yet:
// retry eligibility is deliberately its own later concern, not folded in
// here.
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

	backend := s.takeIdleConn(backendAddr)
	if backend == nil {
		backend, err = net.Dial("tcp", backendAddr)
		if err != nil {
			log.Printf("relayforge: dial backend %s failed: %v", backendAddr, err)
			return false
		}
	}

	if err := req.Write(backend); err != nil {
		backend.Close()
		log.Printf("relayforge: write to backend %s failed: %v", backendAddr, err)
		return false
	}

	resp, err := http.ReadResponse(bufio.NewReader(backend), req)
	if err != nil {
		backend.Close()
		log.Printf("relayforge: read from backend %s failed: %v", backendAddr, err)
		return false
	}
	defer resp.Body.Close()

	if err := resp.Write(client); err != nil {
		backend.Close()
		log.Printf("relayforge: write to client failed: %v", err)
		return false
	}

	if resp.Close {
		backend.Close()
	} else {
		s.putIdleConn(backendAddr, backend)
	}

	return !req.Close && !resp.Close
}
