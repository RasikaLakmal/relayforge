// Package proxy implements the core TCP forwarding loop that everything
// else in relayforge builds on: accept a client connection, dial a backend,
// copy bytes in both directions until either side hangs up.
package proxy

import (
	"io"
	"log"
	"net"
)

// Server forwards every accepted connection to a single fixed backend
// address. It has no HTTP awareness, no routing, and no health checks,
// those come in later milestones once there's a request boundary to hang
// them on.
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

// handleConn dials the backend and copies bytes bidirectionally until both
// directions have drained. A half-close (CloseWrite) is used instead of a
// full close when one side finishes first, so the still-open direction can
// keep flowing instead of getting torn down along with it.
func (s *Server) handleConn(client net.Conn) {
	defer client.Close()

	backend, err := net.Dial("tcp", s.Backend)
	if err != nil {
		log.Printf("relayforge: dial backend %s failed: %v", s.Backend, err)
		return
	}
	defer backend.Close()

	done := make(chan struct{}, 2)

	go func() {
		io.Copy(backend, client)
		closeWrite(backend)
		done <- struct{}{}
	}()

	go func() {
		io.Copy(client, backend)
		closeWrite(client)
		done <- struct{}{}
	}()

	<-done
	<-done
}

// closeWrite half-closes the write side of conn, if it supports doing so,
// so the peer observes EOF without the read side being torn down too. Most
// callers here are *net.TCPConn, which does; the interface check is only a
// safety net for the connection types that don't.
func closeWrite(conn net.Conn) {
	type writeCloser interface {
		CloseWrite() error
	}
	if wc, ok := conn.(writeCloser); ok {
		wc.CloseWrite()
		return
	}
	conn.Close()
}
