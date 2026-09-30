package proxy

import (
	"bufio"
	"fmt"
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

// startProxy serves a proxy.Server on an ephemeral port round-robining
// across the given backend addresses, and returns the address clients
// should dial.
func startProxy(t *testing.T, backends ...string) string {
	t.Helper()
	ln := listenLoopback(t)
	srv := &Server{Backends: backends}
	go srv.Serve(ln)
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

// rawHTTPBackend accepts one connection per entry in responses, reads
// exactly one HTTP request off each, and writes back the corresponding
// raw response bytes verbatim before closing that connection. The proxy
// dials a fresh backend connection per request rather than reusing one
// (pooling comes later), so each response here gets its own accept
// instead of being multiplexed over one long-lived connection.
func rawHTTPBackend(t *testing.T, ln net.Listener, responses []string) {
	t.Helper()
	go func() {
		for _, resp := range responses {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			req, err := http.ReadRequest(bufio.NewReader(conn))
			if err == nil {
				io.Copy(io.Discard, req.Body)
				conn.Write([]byte(resp))
			}
			conn.Close()
		}
	}()
}

// readOneResponse reads one HTTP response off r and returns its body as a
// string.
func readOneResponse(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	resp, err := http.ReadResponse(r, &http.Request{Method: "GET"})
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(body)
}

func canned200(body string) string {
	return fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
}

func TestForwardsSingleRequestResponse(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()
	rawHTTPBackend(t, backendLn, []string{canned200("hello")})

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
	if got, want := readOneResponse(t, bufio.NewReader(client)), "hello"; got != want {
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
	if got, want := readOneResponse(t, bufio.NewReader(client)), "hello world"; got != want {
		t.Fatalf("body = %q, want %q (chunked reassembly failed)", got, want)
	}
}

func TestPersistentConnectionCarriesMultipleRequests(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()
	rawHTTPBackend(t, backendLn, []string{canned200("one"), canned200("two")})

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
		if got := readOneResponse(t, clientReader); got != want {
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
	// A further read should see EOF, rather than the proxy waiting around
	// for a request that will never get a reply.
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
	// request before ever dialing the backend.
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

	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	_, err = client.Read(buf)
	if err != io.EOF {
		t.Fatalf("got err %v, want io.EOF (client should be closed when backend dial fails)", err)
	}
}

func TestRoundRobinDistributesAcrossBackends(t *testing.T) {
	var addrs []string
	for i := 0; i < 3; i++ {
		ln := listenLoopback(t)
		defer ln.Close()
		addrs = append(addrs, ln.Addr().String())
		body := fmt.Sprintf("backend-%d", i)
		// Two responses queued each, enough for two full trips around
		// all three backends.
		rawHTTPBackend(t, ln, []string{canned200(body), canned200(body)})
	}

	proxyAddr := startProxy(t, addrs...)

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	clientReader := bufio.NewReader(client)

	// All six requests go out over the same persistent client
	// connection. If backend selection were pinned per connection instead
	// of per request, every one of them would land on backend-0.
	want := []string{"backend-0", "backend-1", "backend-2", "backend-0", "backend-1", "backend-2"}
	for i, wantBody := range want {
		if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
			t.Fatalf("write request %d: %v", i, err)
		}
		if got := readOneResponse(t, clientReader); got != wantBody {
			t.Fatalf("request %d: body = %q, want %q", i, got, wantBody)
		}
	}
}

func TestDeadBackendFailsOnlyRequestsRoutedToIt(t *testing.T) {
	aliveLn := listenLoopback(t)
	defer aliveLn.Close()
	rawHTTPBackend(t, aliveLn, []string{canned200("alive")})

	deadLn := listenLoopback(t)
	deadAddr := deadLn.Addr().String()
	deadLn.Close()

	// Round-robin order across these two backends: alive, dead, alive,
	// dead, ...
	proxyAddr := startProxy(t, aliveLn.Addr().String(), deadAddr)

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	clientReader := bufio.NewReader(client)

	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write first request: %v", err)
	}
	if got, want := readOneResponse(t, clientReader), "alive"; got != want {
		t.Fatalf("first request body = %q, want %q", got, want)
	}

	// The second request round-robins to the dead backend. There are no
	// health checks yet to route around it: the dial fails and the whole
	// client connection ends, not just this one request.
	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write second request: %v", err)
	}
	buf := make([]byte, 16)
	_, err = clientReader.Read(buf)
	if err != io.EOF {
		t.Fatalf("got err %v, want io.EOF (no health checks yet, so a dead backend ends the connection)", err)
	}
}
