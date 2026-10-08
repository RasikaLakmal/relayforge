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
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Defaults applied whenever the corresponding config value is left at
// zero. maxIdleConnsPerBackend bounds how many idle backend connections
// are kept around per backend; it is a fixed constant rather than a flag
// for now, there is no benchmark yet that would justify exposing it as
// one.
const (
	defaultHealthCheckInterval = 5 * time.Second
	defaultHealthCheckTimeout  = 2 * time.Second
	defaultConnectTimeout      = 3 * time.Second
	defaultHeaderTimeout       = 10 * time.Second
	defaultResponseTimeout     = 30 * time.Second
	maxIdleConnsPerBackend     = 8
)

var errNoHealthyBackends = errors.New("no healthy backends available")

// ErrServerClosed is returned by Serve/ListenAndServe after a graceful
// Shutdown, distinguishing an intentional stop from a real Accept
// failure.
var ErrServerClosed = errors.New("relayforge: server closed")

// Load-balancing strategies recognized by Server.Strategy. The empty
// string is treated as StrategyRoundRobin.
const (
	StrategyRoundRobin       = "round-robin"
	StrategyLeastConnections = "least-connections"
)

// Server forwards every request to one of Backends, chosen by Strategy
// among whichever of them the periodic health check currently considers
// reachable. A backend is assumed healthy until the first check says
// otherwise, so nothing is taken out of rotation before it has actually
// been probed. A backend connection that finishes a request cleanly (no
// error, no Connection: close) is kept idle and reused by a later request
// to the same backend rather than being dialed again from scratch,
// regardless of which client connection that later request arrives on.
//
// Backends, Strategy, the health-check settings, the timeouts, and
// MaxInFlight can all be changed on a running server with Reload, without
// dropping any in-flight request: each already holds its own reference to
// the backend it is talking to, independent of whatever the current
// configuration looks like by the time it finishes. ListenAddr,
// MetricsListenAddr, and MaxConnections cannot: changing where or how
// many connections are accepted needs a new listener, which is a
// restart, not a reload.
type Server struct {
	ListenAddr string
	Backends   []string

	// Strategy picks how a backend is selected for each request.
	// StrategyRoundRobin (the default, used if left empty) cycles
	// through backends in order. StrategyLeastConnections picks
	// whichever healthy backend currently has the fewest in-flight
	// requests, kept as a second, comparable strategy specifically so
	// the two can be benchmarked against each other under uneven request
	// costs, not because either is assumed better. Any other value is
	// rejected at Serve time.
	Strategy string

	// HealthCheckInterval is how often each backend is probed. Zero uses
	// defaultHealthCheckInterval.
	HealthCheckInterval time.Duration
	// HealthCheckTimeout bounds how long a single probe waits to connect.
	// Zero uses defaultHealthCheckTimeout.
	HealthCheckTimeout time.Duration
	// ConnectTimeout bounds dialing a backend. Zero uses
	// defaultConnectTimeout.
	ConnectTimeout time.Duration
	// HeaderTimeout bounds both how long a connection may sit idle
	// waiting for its next request and how long, once bytes start
	// arriving, that request has to finish. One value covers both cases
	// deliberately, splitting them into separate knobs isn't justified
	// yet. Zero uses defaultHeaderTimeout.
	HeaderTimeout time.Duration
	// ResponseTimeout bounds the entire exchange with a backend for one
	// request: writing the request and reading the response together.
	// Zero uses defaultResponseTimeout.
	ResponseTimeout time.Duration

	// MaxConnections bounds how many client connections may be open at
	// once. Zero means unbounded. Once at capacity, Serve simply stops
	// calling Accept until a connection closes and frees a slot, letting
	// further connection attempts queue in the OS's own listen backlog
	// and, once that backlog itself fills, get refused by the kernel,
	// rather than this process accepting an unbounded number of sockets
	// it has nowhere to put.
	MaxConnections int
	// MaxInFlight bounds how many requests may be concurrently in flight
	// to backends at once, summed across every connection. Zero means
	// unbounded. A request that arrives once this bound is already
	// reached is rejected immediately with a 503 Service Unavailable
	// response and the connection closed, rather than queued: a sized
	// queue trades that predictability for burst tolerance at the cost
	// of unbounded added latency if sized wrong, not a trade worth making
	// without a real benchmark motivating it (milestone 11).
	MaxInFlight int

	// MetricsListenAddr, if set, serves Prometheus-style text metrics at
	// /metrics on this separate address for as long as the proxy runs.
	// Empty disables it. A separate address rather than a path on the
	// main listener, so a backend's own routes can never collide with it
	// and metrics scraping is never mixed in with proxied traffic.
	MetricsListenAddr string

	next uint64

	// backends is the live, swappable set of backends. Serve populates it
	// from the Backends field above; Reload atomically replaces it.
	// Everything after startup reads through this, never through the
	// Backends field directly, so a reload can never desync a backend's
	// address from its health/active/stats state the way parallel slices
	// indexed by position would.
	backends atomic.Pointer[[]*backendState]
	// live holds the hot-reloadable scalar settings (Strategy, the
	// timeouts, MaxInFlight). Reads go through this, not the struct
	// fields directly, so Reload can replace them with a single atomic
	// store instead of mutating fields concurrent readers might be
	// examining mid-request.
	live atomic.Pointer[liveConfig]

	inFlight      int64 // in-flight request count across all backends, for MaxInFlight
	totalRequests int64
	totalRetries  int64
	totalRejected int64

	idleMu sync.Mutex
	idle   map[string][]net.Conn

	closing    atomic.Bool
	shutdownCh chan struct{}
	connWG     sync.WaitGroup
	connSem    chan struct{} // nil if MaxConnections <= 0
	metricsSrv *http.Server

	connsMu sync.Mutex
	conns   map[net.Conn]struct{}

	mu       sync.Mutex
	listener net.Listener
}

// backendState holds everything specific to one backend: its address,
// current health, in-flight count, and accumulated stats. Referenced by
// pointer from the slice Server.backends points to, so a Reload that
// keeps an address can hand the new slice the very same *backendState,
// carrying its health/active/stats forward untouched, while a brand new
// address starts a fresh one (assumed healthy, zero stats, exactly like
// startup).
type backendState struct {
	addr    string
	healthy atomic.Bool
	active  int64 // in-flight request count for this backend, for least-connections
	stats   backendStats
}

// backendStats accumulates per-backend counters, all updated with atomics
// so they can be read concurrently with ongoing requests without a lock.
// durationNanosSum/durationCount together give an average latency, not a
// real histogram with percentiles, a deliberate simplification: bucketed
// histograms add real design complexity (choosing boundaries) that isn't
// justified without a benchmark need yet.
type backendStats struct {
	requests         int64
	errors           int64
	durationNanosSum int64
	durationCount    int64
}

// liveConfig is the hot-reloadable subset of Server's configuration,
// published as a single immutable snapshot so readers never see a
// half-updated mix of old and new values.
type liveConfig struct {
	strategy            string
	healthCheckInterval time.Duration
	healthCheckTimeout  time.Duration
	connectTimeout      time.Duration
	headerTimeout       time.Duration
	responseTimeout     time.Duration
	maxInFlight         int
}

// ReloadableConfig holds the subset of Server's configuration that can be
// changed on a running server via Reload without dropping traffic.
// ListenAddr, MetricsListenAddr, and MaxConnections are deliberately not
// included: changing where or how many connections are accepted needs a
// new listener, which is a restart, not a reload.
type ReloadableConfig struct {
	Backends            []string
	Strategy            string
	HealthCheckInterval time.Duration
	HealthCheckTimeout  time.Duration
	ConnectTimeout      time.Duration
	HeaderTimeout       time.Duration
	ResponseTimeout     time.Duration
	MaxInFlight         int
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
	switch s.Strategy {
	case "", StrategyRoundRobin, StrategyLeastConnections:
	default:
		return fmt.Errorf("relayforge: unrecognized strategy %q", s.Strategy)
	}

	initial := make([]*backendState, len(s.Backends))
	for i, addr := range s.Backends {
		bs := &backendState{addr: addr}
		bs.healthy.Store(true)
		initial[i] = bs
	}
	s.backends.Store(&initial)

	s.live.Store(&liveConfig{
		strategy:            s.Strategy,
		healthCheckInterval: s.HealthCheckInterval,
		healthCheckTimeout:  s.HealthCheckTimeout,
		connectTimeout:      s.ConnectTimeout,
		headerTimeout:       s.HeaderTimeout,
		responseTimeout:     s.ResponseTimeout,
		maxInFlight:         s.MaxInFlight,
	})

	s.idle = make(map[string][]net.Conn)
	s.conns = make(map[net.Conn]struct{})
	s.shutdownCh = make(chan struct{})
	if s.MaxConnections > 0 {
		s.connSem = make(chan struct{}, s.MaxConnections)
	}

	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	go s.probeAllBackends()
	go s.runHealthChecks()
	s.startMetricsServer()

	strategy := s.Strategy
	if strategy == "" {
		strategy = StrategyRoundRobin
	}
	log.Printf("relayforge: listening on %s, backends=%v (%s)", ln.Addr(), s.Backends, strategy)

	for {
		if s.connSem != nil {
			select {
			case s.connSem <- struct{}{}:
			case <-s.shutdownCh:
				return ErrServerClosed
			}
		}

		conn, err := ln.Accept()
		if err != nil {
			if s.connSem != nil {
				<-s.connSem
			}
			if s.closing.Load() {
				return ErrServerClosed
			}
			return err
		}
		s.connWG.Add(1)
		go func() {
			defer s.connWG.Done()
			if s.connSem != nil {
				defer func() { <-s.connSem }()
			}
			s.handleConn(conn)
		}()
	}
}

// Reload atomically swaps in new configuration for a running server,
// without dropping any in-flight request. A backend address that was
// already present keeps its existing health state, in-flight count, and
// accumulated metrics; a reload does not reset backend state wholesale
// just because it touched something else. A brand new address starts out
// assumed healthy with zero stats, exactly like at startup. An address
// that is no longer listed is simply dropped from future selection, any
// request already in flight against it keeps its own reference to that
// backend's state regardless, so it finishes exactly as if nothing had
// changed.
func (s *Server) Reload(cfg ReloadableConfig) error {
	switch cfg.Strategy {
	case "", StrategyRoundRobin, StrategyLeastConnections:
	default:
		return fmt.Errorf("relayforge: unrecognized strategy %q", cfg.Strategy)
	}
	if len(cfg.Backends) == 0 {
		return errors.New("relayforge: at least one backend is required")
	}
	seen := make(map[string]bool, len(cfg.Backends))
	for _, addr := range cfg.Backends {
		if seen[addr] {
			return fmt.Errorf("relayforge: duplicate backend %q", addr)
		}
		seen[addr] = true
	}

	oldByAddr := make(map[string]*backendState)
	if old := s.backends.Load(); old != nil {
		for _, bs := range *old {
			oldByAddr[bs.addr] = bs
		}
	}

	newList := make([]*backendState, len(cfg.Backends))
	for i, addr := range cfg.Backends {
		if existing, ok := oldByAddr[addr]; ok {
			newList[i] = existing
			continue
		}
		bs := &backendState{addr: addr}
		bs.healthy.Store(true)
		newList[i] = bs
	}

	s.backends.Store(&newList)
	s.live.Store(&liveConfig{
		strategy:            cfg.Strategy,
		healthCheckInterval: cfg.HealthCheckInterval,
		healthCheckTimeout:  cfg.HealthCheckTimeout,
		connectTimeout:      cfg.ConnectTimeout,
		headerTimeout:       cfg.HeaderTimeout,
		responseTimeout:     cfg.ResponseTimeout,
		maxInFlight:         cfg.MaxInFlight,
	})
	s.Backends = cfg.Backends
	s.Strategy = cfg.Strategy

	strategy := cfg.Strategy
	if strategy == "" {
		strategy = StrategyRoundRobin
	}
	log.Printf("relayforge: reloaded configuration, backends=%v (%s)", cfg.Backends, strategy)
	return nil
}

// Shutdown stops accepting new connections and waits for in-flight
// requests to finish before returning, instead of cutting every open
// connection. Existing persistent client connections finish whatever
// request is currently in flight, if any, and then close rather than
// waiting around for a further request that may never come. If ctx is
// done first, Shutdown force-closes every connection still open (which,
// via the same disconnect watcher that cancels backend work for a client
// that left on its own, also tears down whatever backend work was still
// in flight for them) and returns ctx.Err(); the caller decides what, if
// anything, to do after that.
func (s *Server) Shutdown(ctx context.Context) error {
	s.closing.Store(true)
	close(s.shutdownCh)

	s.mu.Lock()
	ln := s.listener
	s.mu.Unlock()
	if ln != nil {
		ln.Close()
	}
	s.closeIdleConns()
	if s.metricsSrv != nil {
		s.metricsSrv.Close()
	}

	done := make(chan struct{})
	go func() {
		s.connWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.forceCloseConns()
		return ctx.Err()
	}
}

func (s *Server) forceCloseConns() {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	for conn := range s.conns {
		conn.Close()
	}
}

func (s *Server) closeIdleConns() {
	s.idleMu.Lock()
	defer s.idleMu.Unlock()
	for addr, conns := range s.idle {
		for _, c := range conns {
			c.Close()
		}
		delete(s.idle, addr)
	}
}

// startMetricsServer starts the optional /metrics endpoint in the
// background if MetricsListenAddr is set. A failure here (e.g. the
// address is already in use) is logged, not fatal: metrics are a
// secondary concern, not worth taking down request handling over.
func (s *Server) startMetricsServer() {
	if s.MetricsListenAddr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.serveMetrics)
	s.metricsSrv = &http.Server{Addr: s.MetricsListenAddr, Handler: mux}
	go func() {
		log.Printf("relayforge: metrics listening on %s/metrics", s.MetricsListenAddr)
		if err := s.metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("relayforge: metrics server error: %v", err)
		}
	}()
}

// serveMetrics renders current counters and gauges in Prometheus text
// exposition format, by hand rather than via a client library: the
// format itself is simple line-based text, and the point of this project
// is understanding what is actually being exposed, not wiring up a
// registry.
func (s *Server) serveMetrics(w http.ResponseWriter, r *http.Request) {
	var b strings.Builder

	b.WriteString("# HELP relayforge_requests_total Total requests received by the proxy.\n")
	b.WriteString("# TYPE relayforge_requests_total counter\n")
	fmt.Fprintf(&b, "relayforge_requests_total %d\n", atomic.LoadInt64(&s.totalRequests))

	b.WriteString("# HELP relayforge_rejected_total Requests rejected for exceeding MaxInFlight.\n")
	b.WriteString("# TYPE relayforge_rejected_total counter\n")
	fmt.Fprintf(&b, "relayforge_rejected_total %d\n", atomic.LoadInt64(&s.totalRejected))

	b.WriteString("# HELP relayforge_retries_total Requests retried against a different backend.\n")
	b.WriteString("# TYPE relayforge_retries_total counter\n")
	fmt.Fprintf(&b, "relayforge_retries_total %d\n", atomic.LoadInt64(&s.totalRetries))

	b.WriteString("# HELP relayforge_active_connections Currently open client connections.\n")
	b.WriteString("# TYPE relayforge_active_connections gauge\n")
	s.connsMu.Lock()
	activeConns := len(s.conns)
	s.connsMu.Unlock()
	fmt.Fprintf(&b, "relayforge_active_connections %d\n", activeConns)

	b.WriteString("# HELP relayforge_in_flight_requests Requests currently being forwarded to a backend.\n")
	b.WriteString("# TYPE relayforge_in_flight_requests gauge\n")
	fmt.Fprintf(&b, "relayforge_in_flight_requests %d\n", atomic.LoadInt64(&s.inFlight))

	b.WriteString("# HELP relayforge_goroutines Current goroutine count for the whole process, a direct signal of per-connection/per-request leaks.\n")
	b.WriteString("# TYPE relayforge_goroutines gauge\n")
	fmt.Fprintf(&b, "relayforge_goroutines %d\n", runtime.NumGoroutine())

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	b.WriteString("# HELP relayforge_memory_bytes Current heap memory in use (runtime.MemStats.Alloc) for the whole process.\n")
	b.WriteString("# TYPE relayforge_memory_bytes gauge\n")
	fmt.Fprintf(&b, "relayforge_memory_bytes %d\n", mem.Alloc)

	b.WriteString("# HELP relayforge_backend_up Whether the backend passed its most recent health check.\n")
	b.WriteString("# TYPE relayforge_backend_up gauge\n")
	b.WriteString("# HELP relayforge_backend_requests_total Requests routed to this backend.\n")
	b.WriteString("# TYPE relayforge_backend_requests_total counter\n")
	b.WriteString("# HELP relayforge_backend_errors_total Requests to this backend that failed.\n")
	b.WriteString("# TYPE relayforge_backend_errors_total counter\n")
	b.WriteString("# HELP relayforge_backend_active_requests Requests currently in flight to this backend.\n")
	b.WriteString("# TYPE relayforge_backend_active_requests gauge\n")
	b.WriteString("# HELP relayforge_backend_request_duration_seconds_sum Total time spent on requests to this backend. Sum/count gives an average, there are no percentile buckets yet.\n")
	b.WriteString("# TYPE relayforge_backend_request_duration_seconds_sum counter\n")
	b.WriteString("# HELP relayforge_backend_request_duration_seconds_count Requests to this backend with a recorded duration.\n")
	b.WriteString("# TYPE relayforge_backend_request_duration_seconds_count counter\n")
	for _, bs := range s.loadBackends() {
		up := 0
		if bs.healthy.Load() {
			up = 1
		}
		fmt.Fprintf(&b, "relayforge_backend_up{backend=%q} %d\n", bs.addr, up)
		fmt.Fprintf(&b, "relayforge_backend_requests_total{backend=%q} %d\n", bs.addr, atomic.LoadInt64(&bs.stats.requests))
		fmt.Fprintf(&b, "relayforge_backend_errors_total{backend=%q} %d\n", bs.addr, atomic.LoadInt64(&bs.stats.errors))
		fmt.Fprintf(&b, "relayforge_backend_active_requests{backend=%q} %d\n", bs.addr, atomic.LoadInt64(&bs.active))
		seconds := float64(atomic.LoadInt64(&bs.stats.durationNanosSum)) / 1e9
		fmt.Fprintf(&b, "relayforge_backend_request_duration_seconds_sum{backend=%q} %g\n", bs.addr, seconds)
		fmt.Fprintf(&b, "relayforge_backend_request_duration_seconds_count{backend=%q} %d\n", bs.addr, atomic.LoadInt64(&bs.stats.durationCount))
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.Write([]byte(b.String()))
}

// loadBackends returns the current backend list. Safe to call concurrently
// with Reload: the returned slice is a stable snapshot that Reload never
// mutates in place, it only ever publishes a new one.
func (s *Server) loadBackends() []*backendState {
	p := s.backends.Load()
	if p == nil {
		return nil
	}
	return *p
}

func (s *Server) liveCfg() *liveConfig {
	return s.live.Load()
}

// runHealthChecks probes every backend on an interval until Shutdown
// closes shutdownCh. The interval is re-read after every tick so a Reload
// that changes HealthCheckInterval takes effect on the next tick rather
// than only after a restart.
func (s *Server) runHealthChecks() {
	interval := s.liveCfg().healthCheckInterval
	if interval <= 0 {
		interval = defaultHealthCheckInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.probeAllBackends()
			if next := s.liveCfg().healthCheckInterval; next <= 0 {
				next = defaultHealthCheckInterval
				if next != interval {
					interval = next
					ticker.Reset(interval)
				}
			} else if next != interval {
				interval = next
				ticker.Reset(interval)
			}
		case <-s.shutdownCh:
			return
		}
	}
}

func (s *Server) probeAllBackends() {
	for _, bs := range s.loadBackends() {
		go s.probeBackend(bs)
	}
}

// probeBackend checks one backend by attempting a plain TCP connect, the
// simplest available signal that something is listening. It does not
// speak HTTP to the backend, an application-level health endpoint is a
// choice for a config-driven future milestone, not this one.
func (s *Server) probeBackend(bs *backendState) {
	timeout := s.liveCfg().healthCheckTimeout
	if timeout <= 0 {
		timeout = defaultHealthCheckTimeout
	}

	conn, err := net.DialTimeout("tcp", bs.addr, timeout)
	isHealthy := err == nil
	if conn != nil {
		conn.Close()
	}

	wasHealthy := bs.healthy.Swap(isHealthy)
	if wasHealthy == isHealthy {
		return
	}
	if isHealthy {
		log.Printf("relayforge: backend %s passed its health check, back in rotation", bs.addr)
	} else {
		log.Printf("relayforge: backend %s failed its health check, taking it out of rotation", bs.addr)
	}
}

// pickBackend selects a backend according to the current strategy. It
// reports false if every backend currently looks unhealthy, in which case
// there is nothing to route to.
func (s *Server) pickBackend() (*backendState, bool) {
	list := s.loadBackends()
	if len(list) == 0 {
		return nil, false
	}
	if s.liveCfg().strategy == StrategyLeastConnections {
		return s.pickLeastConnections(list)
	}
	return s.pickRoundRobin(list)
}

// pickRoundRobin cycles through backends in order, skipping unhealthy
// ones.
func (s *Server) pickRoundRobin(list []*backendState) (*backendState, bool) {
	n := len(list)
	for i := 0; i < n; i++ {
		idx := int((atomic.AddUint64(&s.next, 1) - 1) % uint64(n))
		if list[idx].healthy.Load() {
			return list[idx], true
		}
	}
	return nil, false
}

// pickLeastConnections picks whichever healthy backend currently has the
// fewest in-flight requests. The scan starts from a rotating offset (the
// same counter round robin uses) rather than always from index 0, so that
// ties, which are common when load is light or every backend is equally
// fast, are broken fairly across backends instead of always favoring
// whichever comes first in the list.
func (s *Server) pickLeastConnections(list []*backendState) (*backendState, bool) {
	n := len(list)
	start := int((atomic.AddUint64(&s.next, 1) - 1) % uint64(n))
	var best *backendState
	var bestCount int64
	for i := 0; i < n; i++ {
		bs := list[(start+i)%n]
		if !bs.healthy.Load() {
			continue
		}
		count := atomic.LoadInt64(&bs.active)
		if best == nil || count < bestCount {
			best, bestCount = bs, count
		}
	}
	if best == nil {
		return nil, false
	}
	return best, true
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

func (s *Server) connectTimeout() time.Duration {
	if t := s.liveCfg().connectTimeout; t > 0 {
		return t
	}
	return defaultConnectTimeout
}

func (s *Server) headerTimeout() time.Duration {
	if t := s.liveCfg().headerTimeout; t > 0 {
		return t
	}
	return defaultHeaderTimeout
}

func (s *Server) responseTimeout() time.Duration {
	if t := s.liveCfg().responseTimeout; t > 0 {
		return t
	}
	return defaultResponseTimeout
}

// handleConn forwards requests off client until the connection ends or a
// shutdown is in progress. It is tracked in s.conns for the duration so
// Shutdown can force-close it if its deadline passes before the
// connection finishes on its own.
func (s *Server) handleConn(client net.Conn) {
	s.connsMu.Lock()
	s.conns[client] = struct{}{}
	s.connsMu.Unlock()

	defer func() {
		s.connsMu.Lock()
		delete(s.conns, client)
		s.connsMu.Unlock()
		client.Close()
	}()

	clientReader := bufio.NewReader(client)
	// Checked before waiting for another request, not mid-exchange: a
	// request already in flight when shutdown starts is left to finish
	// normally, only the next one is refused.
	for !s.closing.Load() && s.forwardOneRequest(client, clientReader) {
	}
}

// writeOverloadedResponse tells the client to back off: capacity is
// explicitly full, not accidentally degraded, so this is a real 503 with
// a hint of when to retry, not a silently dropped connection. The
// connection is always closed afterward (Connection: close) rather than
// kept alive for more requests that would just get rejected the same
// way, and the request's own body, if any, is not drained first, a
// deliberate simplification: this path exists for when the proxy is
// already under more load than it can handle, reading more from an
// already-rejected request isn't worth the complexity here.
func writeOverloadedResponse(client net.Conn) {
	body := "503 Service Unavailable: at capacity, try again shortly\n"
	resp := fmt.Sprintf("HTTP/1.1 503 Service Unavailable\r\nContent-Length: %d\r\nRetry-After: 1\r\nConnection: close\r\n\r\n%s", len(body), body)
	client.Write([]byte(resp))
}

// forwardOneRequest reads one HTTP request off the client and forwards it,
// with up to one retry against a different backend for failures known not
// to risk a duplicate side effect (see forwardWithRetry). It reports
// whether the client connection should be used for another request.
func (s *Server) forwardOneRequest(client net.Conn, clientReader *bufio.Reader) bool {
	client.SetReadDeadline(time.Now().Add(s.headerTimeout()))
	req, err := http.ReadRequest(clientReader)
	if err != nil {
		return false
	}
	client.SetReadDeadline(time.Time{})
	atomic.AddInt64(&s.totalRequests, 1)

	if maxInFlight := s.liveCfg().maxInFlight; maxInFlight > 0 {
		current := atomic.AddInt64(&s.inFlight, 1)
		if current > int64(maxInFlight) {
			atomic.AddInt64(&s.inFlight, -1)
			atomic.AddInt64(&s.totalRejected, 1)
			log.Printf("relayforge: rejecting request, at capacity (%d in flight)", maxInFlight)
			writeOverloadedResponse(client)
			return false
		}
		defer atomic.AddInt64(&s.inFlight, -1)
	}

	a := s.forwardWithRetry(client, clientReader, req)
	if a.err != nil {
		if errors.Is(a.err, errNoHealthyBackends) {
			log.Printf("relayforge: %v", a.err)
		} else {
			log.Printf("relayforge: request to backend %s failed: %v", a.addr, a.err)
		}
		return false
	}
	resp := a.resp
	backend := a.backend
	defer resp.Body.Close()

	if err := resp.Write(client); err != nil {
		backend.Close()
		log.Printf("relayforge: write to client failed: %v", err)
		return false
	}

	if resp.Close {
		backend.Close()
	} else {
		s.putIdleConn(a.addr, backend)
	}

	return !req.Close && !resp.Close
}

// isIdempotentMethod reports whether method is safe to send more than
// once against a backend, per ordinary HTTP semantics. POST and PATCH are
// deliberately excluded: they are the methods most likely to have a
// side effect that must not happen twice.
func isIdempotentMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete, http.MethodOptions, http.MethodTrace:
		return true
	default:
		return false
	}
}

// attempt is the outcome of trying to forward a request to one backend.
type attempt struct {
	resp    *http.Response
	backend net.Conn
	addr    string
	// dialed reports whether a backend connection was actually obtained
	// (pooled or freshly dialed). If false, the request never reached
	// any backend at all.
	dialed bool
	// reused reports whether backend came from the idle pool rather than
	// being freshly dialed.
	reused bool
	// wroteFully reports whether the entire request was written to the
	// backend before any failure. If false, whatever the backend saw (if
	// anything) was incomplete.
	wroteFully bool
	// clientDisconnected reports whether this attempt's failure was
	// caused by the client itself going away mid-flight, as opposed to a
	// backend problem.
	clientDisconnected bool
	err                error
}

// forwardWithRetry sends req to a backend and returns the result,
// including the backend connection that produced it so the caller can
// decide whether to pool or close it. It retries exactly once, against a
// different backend chosen by the normal round-robin order, but only for
// failures that could not have caused the backend to apply a request
// twice:
//
//   - the backend was never actually reached (dial failed, or there was
//     no healthy backend to try), so nothing could have been applied
//   - a write failed against a connection taken from the idle pool,
//     which is the signature of a backend that closed an idle connection
//     on its own schedule rather than of it having received anything
//   - the request method is idempotent (GET, HEAD, PUT, DELETE, OPTIONS,
//     TRACE), so sending it again cannot itself cause new harm
//
// A request with a body is never retried, regardless of the above: doing
// so safely would mean buffering the body so it can be replayed, which
// isn't implemented yet. A non-idempotent request (POST, PATCH) whose
// write may have reached a freshly dialed backend connection before
// failing is also never retried, that is the deliberately unresolved
// case, the backend might already have applied it, and retrying blind
// risks doing it twice.
func (s *Server) forwardWithRetry(client net.Conn, clientReader *bufio.Reader, req *http.Request) attempt {
	first := s.attemptOnce(client, clientReader, req)
	if first.err == nil {
		return first
	}
	if !retryEligible(req, first) {
		return first
	}
	atomic.AddInt64(&s.totalRetries, 1)
	log.Printf("relayforge: retrying request after backend %s failed: %v", first.addr, first.err)
	return s.attemptOnce(client, clientReader, req)
}

func retryEligible(req *http.Request, a attempt) bool {
	if a.clientDisconnected {
		// The client is already gone; there is no one to hand a
		// response to even if a retry succeeded, so retrying would only
		// be extra work spent on a request nobody is waiting for.
		return false
	}
	if !a.dialed {
		// Never reached any backend: the body, if any, was never
		// touched, so retrying is always safe regardless of method.
		return true
	}
	if req.ContentLength != 0 {
		// A body may already have been partially written; replaying it
		// safely would need buffering it first, which isn't implemented
		// yet.
		return false
	}
	if !a.wroteFully && a.reused {
		return true
	}
	return isIdempotentMethod(req.Method)
}

// attemptOnce picks a backend, obtains a connection to it (pooled or
// freshly dialed), and runs the full request/response exchange once. The
// chosen backend's active count is incremented for the duration of the
// exchange (not including writing the response back to the client, which
// is no longer backend-side work), so StrategyLeastConnections sees a
// request as "in flight" for exactly as long as a backend is actually
// doing something on its behalf.
func (s *Server) attemptOnce(client net.Conn, clientReader *bufio.Reader, req *http.Request) attempt {
	bs, ok := s.pickBackend()
	if !ok {
		return attempt{err: errNoHealthyBackends}
	}
	atomic.AddInt64(&bs.active, 1)
	defer atomic.AddInt64(&bs.active, -1)

	atomic.AddInt64(&bs.stats.requests, 1)
	start := time.Now()
	recordDuration := func() {
		atomic.AddInt64(&bs.stats.durationNanosSum, int64(time.Since(start)))
		atomic.AddInt64(&bs.stats.durationCount, 1)
	}

	backend := s.takeIdleConn(bs.addr)
	reused := backend != nil
	if backend == nil {
		var err error
		backend, err = net.DialTimeout("tcp", bs.addr, s.connectTimeout())
		if err != nil {
			recordDuration()
			atomic.AddInt64(&bs.stats.errors, 1)
			return attempt{addr: bs.addr, dialed: false, err: err}
		}
	}

	resp, wroteFully, clientDisconnected, err := s.exchangeWithBackend(client, clientReader, backend, req)
	recordDuration()
	if err != nil {
		atomic.AddInt64(&bs.stats.errors, 1)
		backend.Close()
		return attempt{addr: bs.addr, dialed: true, reused: reused, wroteFully: wroteFully, clientDisconnected: clientDisconnected, err: err}
	}
	return attempt{resp: resp, backend: backend, addr: bs.addr, dialed: true, reused: reused, wroteFully: true}
}

// exchangeWithBackend writes req to backend and reads the matching
// response, bounded by the response timeout. While that is in progress, a
// side watcher peeks at clientReader without consuming anything: if the
// client disconnects before the backend has answered, the watcher closes
// backend to unblock whichever of the write or read was in progress,
// instead of letting backend work run to completion for a client that is
// no longer there. Peek is used specifically because it does not consume
// bytes, an early byte of the client's next pipelined request (if any) is
// left untouched for the real read that follows.
//
// The watcher's own Peek call is itself force-unblocked (via a deadline
// in the past) once this function is done waiting on the backend, so it
// doesn't leak past this call. That forced unblock surfaces as a timeout
// error, which is how the watcher tells a genuine client disconnect
// (returned as clientDisconnected) apart from its own cancellation.
func (s *Server) exchangeWithBackend(client net.Conn, clientReader *bufio.Reader, backend net.Conn, req *http.Request) (resp *http.Response, wroteFully bool, clientDisconnected bool, err error) {
	backend.SetDeadline(time.Now().Add(s.responseTimeout()))

	watcherDone := make(chan bool, 1)
	go func() {
		_, peekErr := clientReader.Peek(1)
		genuine := true
		if netErr, ok := peekErr.(net.Error); ok && netErr.Timeout() {
			genuine = false
		}
		if genuine {
			backend.Close()
		}
		watcherDone <- genuine
	}()
	defer func() {
		client.SetReadDeadline(time.Now())
		clientDisconnected = <-watcherDone
		client.SetReadDeadline(time.Time{})
	}()

	if werr := req.Write(backend); werr != nil {
		err = werr
		return
	}
	wroteFully = true
	resp, err = http.ReadResponse(bufio.NewReader(backend), req)
	return
}
