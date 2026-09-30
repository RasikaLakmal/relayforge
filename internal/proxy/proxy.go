// Package proxy implements request/response level forwarding: read one
// HTTP/1.1 request off the client connection at a time, round-robin to
// pick a backend, forward the request to it as its own message, and write
// back the matching response. A TCP connection can carry many requests;
// this is what gives each of them its own boundary, which per-request
// routing, retries, and metrics need and a raw byte pipe cannot provide.
package proxy

import (
	"bufio"
	"errors"
	"log"
	"net"
	"net/http"
	"sync/atomic"
)

// Server forwards every request to one of Backends, chosen in round-robin
// order. There is no health awareness yet: a backend that is down gets
// picked like any other, and the request routed to it simply fails. There
// is no connection pooling yet either: every request dials its own fresh
// backend connection rather than reusing one, which is also what makes
// picking a different backend for each request on the same client
// connection possible in the first place.
type Server struct {
	ListenAddr string
	Backends   []string

	next uint64
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

	log.Printf("relayforge: listening on %s, backends=%v (round robin)", ln.Addr(), s.Backends)

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

// nextBackend picks the next backend address in round-robin order.
func (s *Server) nextBackend() string {
	n := atomic.AddUint64(&s.next, 1)
	return s.Backends[(n-1)%uint64(len(s.Backends))]
}

func (s *Server) handleConn(client net.Conn) {
	defer client.Close()

	clientReader := bufio.NewReader(client)
	for s.forwardOneRequest(client, clientReader) {
	}
}

// forwardOneRequest reads one HTTP request off the client, round-robins to
// pick a backend, dials a fresh connection to it for this request alone,
// and writes back the response. It reports whether the client connection
// should be used for another request: request/response close framing
// (Connection: close, HTTP/1.0 with no keep-alive), a failed backend dial,
// and a malformed or partial request all end it.
func (s *Server) forwardOneRequest(client net.Conn, clientReader *bufio.Reader) bool {
	req, err := http.ReadRequest(clientReader)
	if err != nil {
		return false
	}

	backendAddr := s.nextBackend()
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
