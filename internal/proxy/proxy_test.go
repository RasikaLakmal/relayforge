package proxy

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// listenLoopback binds an ephemeral port on the loopback interface and
// returns the listener along with its address for a test client to dial.
func listenLoopback(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return ln
}

// startProxy serves a proxy.Server on an ephemeral port pointed at the
// given backend address, and returns the address clients should dial.
func startProxy(t *testing.T, backend string) string {
	t.Helper()
	ln := listenLoopback(t)
	srv := &Server{Backend: backend}
	go srv.Serve(ln)
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

// rawHTTPBackend accepts a single connection and, for each entry in
// responses, reads one HTTP request off it and writes back the given raw
// response bytes verbatim. This is deliberately hand-written rather than
// built on net/http.Server, so the tests can construct exact framing
// (Content-Length, chunked, Connection: close) instead of trusting the
// standard server to always produce it the same way the proxy needs to
// prove it understands.
func rawHTTPBackend(t *testing.T, ln net.Listener, responses []string) {
	t.Helper()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		for _, resp := range responses {
			req, err := http.ReadRequest(r)
			if err != nil {
				return
			}
			io.Copy(io.Discard, req.Body)
			if _, err := conn.Write([]byte(resp)); err != nil {
				return
			}
		}
	}()
}

func TestForwardsSingleRequestResponse(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()
	rawHTTPBackend(t, backendLn, []string{
		"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
	})

	proxyAddr := startProxy(t, backendLn.Addr().String())

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()

	if _, err := client.Write([]byte("GET /hello HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(client), &http.Request{Method: "GET"})
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if got, want := string(body), "hello"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestChunkedResponseBodyForwarded(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()
	rawHTTPBackend(t, backendLn, []string{
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n",
	})

	proxyAddr := startProxy(t, backendLn.Addr().String())

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()

	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(client), &http.Request{Method: "GET"})
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if got, want := string(body), "hello world"; got != want {
		t.Fatalf("body = %q, want %q (chunked reassembly failed)", got, want)
	}
}

func TestPersistentConnectionCarriesMultipleRequests(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()
	rawHTTPBackend(t, backendLn, []string{
		"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\none",
		"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\ntwo",
	})

	proxyAddr := startProxy(t, backendLn.Addr().String())

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	clientReader := bufio.NewReader(client)

	for _, want := range []string{"one", "two"} {
		if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
			t.Fatalf("write request: %v", err)
		}
		resp, err := http.ReadResponse(clientReader, &http.Request{Method: "GET"})
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if got := string(body); got != want {
			t.Fatalf("body = %q, want %q", got, want)
		}
	}
}

func TestConnectionCloseHeaderEndsConnection(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()
	rawHTTPBackend(t, backendLn, []string{
		"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
	})

	proxyAddr := startProxy(t, backendLn.Addr().String())

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	clientReader := bufio.NewReader(client)

	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}
	resp, err := http.ReadResponse(clientReader, &http.Request{Method: "GET"})
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()
	if !resp.Close {
		t.Fatal("response should have parsed Close=true from Connection: close")
	}

	// The proxy should have torn the connection down after that response.
	// A further read should see EOF (or the write itself may fail),
	// rather than the proxy waiting around for a request that will never
	// get a reply.
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	_, err = clientReader.Read(buf)
	if err != io.EOF {
		t.Fatalf("got err %v, want io.EOF after Connection: close", err)
	}
}

func TestMalformedRequestClosesConnectionCleanly(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()
	// No responses queued: a well-behaved proxy rejects the malformed
	// request before ever forwarding anything to the backend.
	rawHTTPBackend(t, backendLn, nil)

	proxyAddr := startProxy(t, backendLn.Addr().String())

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()

	if _, err := client.Write([]byte("this is not an HTTP request\r\n\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	_, err = client.Read(buf)
	if err != io.EOF {
		t.Fatalf("got err %v, want io.EOF (proxy should close, not hang, on a malformed request)", err)
	}
}

func TestClientClosedWhenBackendDialFails(t *testing.T) {
	// Bind a listener just to get a guaranteed-free port, then close it
	// immediately so nothing is listening there when the proxy dials it.
	deadLn := listenLoopback(t)
	deadAddr := deadLn.Addr().String()
	deadLn.Close()

	proxyAddr := startProxy(t, deadAddr)

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	_, err = client.Read(buf)
	if err != io.EOF {
		t.Fatalf("got err %v, want io.EOF (client should be closed when backend dial fails)", err)
	}
}
