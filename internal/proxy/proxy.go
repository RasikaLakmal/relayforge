// Package proxy implements request/response level forwarding: read one
// HTTP/1.1 request off the client connection at a time, forward it to a
// backend as its own message, and write back the matching response. A TCP
// connection can carry many requests; this is what gives each of them its
// own boundary, which per-request routing, retries, and metrics need in
// later milestones and a raw byte pipe cannot provide.
package proxy

import (
	"bufio"
	"log"
	"net"
	"net/http"
)

// Server forwards every accepted connection to a single fixed backend
// address. It has no routing or health checks yet, and no connection
// pooling: each client connection gets its own backend connection, and if
// the backend closes it, the client connection closes too rather than
// being kept alive against a fresh backend dial. Those come in later
// milestones.
type Server struct {
	ListenAddr string
	Backend    string
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

	log.Printf("relayforge: listening on %s, forwarding to %s", ln.Addr(), s.Backend)

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

// handleConn dials the backend once for this client connection, then keeps
// forwarding requests off it until either side ends the connection.
func (s *Server) handleConn(client net.Conn) {
	defer client.Close()

	backend, err := net.Dial("tcp", s.Backend)
	if err != nil {
		log.Printf("relayforge: dial backend %s failed: %v", s.Backend, err)
		return
	}
	defer backend.Close()

	clientReader := bufio.NewReader(client)
	backendReader := bufio.NewReader(backend)

	for s.forwardOneRequest(client, clientReader, backend, backendReader) {
	}
}

// forwardOneRequest reads one HTTP request off the client, forwards it to
// the backend, and writes back the matching response. It reports whether
// the same connection should be used for another request: both request and
// response framing (Connection: close, HTTP/1.0 with no keep-alive) can end
// the connection, and a malformed or partial request is rejected outright
// rather than left hanging.
func (s *Server) forwardOneRequest(client net.Conn, clientReader *bufio.Reader, backend net.Conn, backendReader *bufio.Reader) bool {
	req, err := http.ReadRequest(clientReader)
	if err != nil {
		return false
	}

	if err := req.Write(backend); err != nil {
		log.Printf("relayforge: write to backend failed: %v", err)
		return false
	}

	resp, err := http.ReadResponse(backendReader, req)
	if err != nil {
		log.Printf("relayforge: read from backend failed: %v", err)
		return false
	}
	defer resp.Body.Close()

	if err := resp.Write(client); err != nil {
		log.Printf("relayforge: write to client failed: %v", err)
		return false
	}

	// If the backend closed on us, the connection this client has can't
	// be reused for its next request either: there is no pooling yet to
	// dial a fresh backend connection mid-stream, so the whole thing ends
	// here even if the client itself asked to keep it alive.
	return !req.Close && !resp.Close
}
