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

// startHealthCheckedProxy is startProxy but with an explicit, short
// health-check interval and timeout, for tests that need to see health
// checks actually converge within the test's own deadline rather than
// relying on the multi-second production defaults.
func startHealthCheckedProxy(t *testing.T, interval, timeout time.Duration, backends ...string) string {
	t.Helper()
	ln := listenLoopback(t)
	srv := &Server{
		Backends:            backends,
		HealthCheckInterval: interval,
		HealthCheckTimeout:  timeout,
	}
	go srv.Serve(ln)
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

// rawHTTPBackend answers up to len(responses) real HTTP requests with the
// corresponding raw response bytes verbatim, one request per connection,
// matching how the proxy dials a fresh backend connection per request
// rather than reusing one. It accepts connections in an unbounded loop
// rather than exactly len(responses) times, and only consumes a queued
// response once it has actually parsed a request off a connection: a
// health-check probe (connect, then close without sending anything) is a
// connection too, and a real backend would not be disturbed by one, so
// this fake should not treat it as consuming a response slot either.
func rawHTTPBackend(t *testing.T, ln net.Listener, responses []string) {
	t.Helper()
	go func() {
		i := 0
		for i < len(responses) {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			req, err := http.ReadRequest(bufio.NewReader(conn))
			if err != nil {
				conn.Close()
				continue
			}
			io.Copy(io.Discard, req.Body)
			conn.Write([]byte(responses[i]))
			conn.Close()
			i++
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

func TestHealthCheckIsPeriodicNotPerRequest(t *testing.T) {
	backendLn := listenLoopback(t)
	backendAddr := backendLn.Addr().String()
	rawHTTPBackend(t, backendLn, []string{canned200("first")})

	// A long interval that will not fire again for the life of this
	// test: the only check that matters here is the immediate one Serve
	// runs at startup, which should find this backend healthy.
	proxyAddr := startHealthCheckedProxy(t, 10*time.Second, 2*time.Second, backendAddr)

	deadline := time.Now().Add(2 * time.Second)
	for {
		if body, ok := requestSucceeds(proxyAddr); ok {
			if body != "first" {
				t.Fatalf("body = %q, want %q", body, "first")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the startup health check to mark the backend healthy")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Take the backend down. The next periodic probe is 10s away, well
	// outside this test, so the proxy has no way to know yet: health
	// checks catch a dead backend at the next probe, not the instant it
	// actually goes down.
	backendLn.Close()

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()
	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}
	client.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 16)
	_, err = client.Read(buf)
	if err != io.EOF {
		t.Fatalf("got err %v, want io.EOF (health checks are periodic, so a backend that just died should still be picked and fail until the next probe)", err)
	}
}

// requestSucceeds opens a fresh connection to proxyAddr, sends one GET,
// and reports the response body and whether it was read successfully at
// all. Used by the health-check tests below to poll for convergence
// instead of sleeping a fixed amount and hoping.
func requestSucceeds(proxyAddr string) (body string, ok bool) {
	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		return "", false
	}
	defer client.Close()
	client.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		return "", false
	}
	resp, err := http.ReadResponse(bufio.NewReader(client), &http.Request{Method: "GET"})
	if err != nil {
		return "", false
	}
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return "", false
	}
	return string(b), true
}

func TestHealthCheckRoutesAroundDeadBackend(t *testing.T) {
	aliveLn := listenLoopback(t)
	defer aliveLn.Close()
	responses := make([]string, 50)
	for i := range responses {
		responses[i] = canned200("alive")
	}
	rawHTTPBackend(t, aliveLn, responses)

	deadLn := listenLoopback(t)
	deadAddr := deadLn.Addr().String()
	deadLn.Close()

	proxyAddr := startHealthCheckedProxy(t, 10*time.Millisecond, 50*time.Millisecond, aliveLn.Addr().String(), deadAddr)

	// Poll until the health checker has converged on "alive is up, dead
	// is down" and every request lands on alive, rather than sleeping a
	// fixed guess. A few consecutive successes rules out a lucky
	// round-robin draw that just happened to skip the dead one this once.
	deadline := time.Now().Add(3 * time.Second)
	consecutive := 0
	for consecutive < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for health checks to converge, got %d consecutive successes", consecutive)
		}
		body, ok := requestSucceeds(proxyAddr)
		if !ok {
			consecutive = 0
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if body != "alive" {
			t.Fatalf("body = %q, want %q", body, "alive")
		}
		consecutive++
	}
}

func TestBackendRecoversAfterHealthCheckPasses(t *testing.T) {
	aliveLn := listenLoopback(t)
	defer aliveLn.Close()
	aliveResponses := make([]string, 50)
	for i := range aliveResponses {
		aliveResponses[i] = canned200("alive")
	}
	rawHTTPBackend(t, aliveLn, aliveResponses)

	// Reserve an address and close it immediately: this backend starts
	// out down.
	recoveringAddr := func() string {
		ln := listenLoopback(t)
		defer ln.Close()
		return ln.Addr().String()
	}()

	proxyAddr := startHealthCheckedProxy(t, 10*time.Millisecond, 50*time.Millisecond, aliveLn.Addr().String(), recoveringAddr)

	// Give the health checker a moment to actually mark it down first,
	// so this test exercises recovery rather than the backend never
	// having been probed as unhealthy in the first place.
	time.Sleep(100 * time.Millisecond)

	recoveredLn, err := net.Listen("tcp", recoveringAddr)
	if err != nil {
		t.Fatalf("re-listen on recovered address: %v", err)
	}
	defer recoveredLn.Close()
	recoveredResponses := make([]string, 50)
	for i := range recoveredResponses {
		recoveredResponses[i] = canned200("recovered")
	}
	rawHTTPBackend(t, recoveredLn, recoveredResponses)

	deadline := time.Now().Add(3 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("backend never came back into rotation after its health check started passing")
		}
		body, ok := requestSucceeds(proxyAddr)
		if ok && body == "recovered" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAllBackendsUnhealthyFailsCleanly(t *testing.T) {
	deadLn1 := listenLoopback(t)
	deadAddr1 := deadLn1.Addr().String()
	deadLn1.Close()
	deadLn2 := listenLoopback(t)
	deadAddr2 := deadLn2.Addr().String()
	deadLn2.Close()

	proxyAddr := startHealthCheckedProxy(t, 10*time.Millisecond, 50*time.Millisecond, deadAddr1, deadAddr2)

	// Give health checks a generous margin to converge on "both
	// unhealthy" before asserting on it.
	time.Sleep(300 * time.Millisecond)

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()
	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}

	client.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 16)
	_, err = client.Read(buf)
	if err != io.EOF {
		t.Fatalf("got err %v, want io.EOF (no healthy backends, request should fail cleanly)", err)
	}
}
