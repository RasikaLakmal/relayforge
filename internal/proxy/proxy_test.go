package proxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
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
// corresponding raw response bytes verbatim, behaving like an ordinary
// HTTP/1.1 keep-alive server: it keeps serving requests off the same
// accepted connection until that response says Connection: close, the
// peer stops sending requests on it (the proxy dialed a different
// connection instead, or a health-check probe connected and closed
// without ever sending a request), or the responses are exhausted. A
// probe connection does not consume a queued response slot, only an
// actual parsed request does.
func rawHTTPBackend(t *testing.T, ln net.Listener, responses []string) {
	t.Helper()
	go func() {
		i := 0
		for i < len(responses) {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			r := bufio.NewReader(conn)
			for i < len(responses) {
				req, err := http.ReadRequest(r)
				if err != nil {
					break
				}
				io.Copy(io.Discard, req.Body)
				resp := responses[i]
				i++
				if _, err := conn.Write([]byte(resp)); err != nil {
					break
				}
				if strings.Contains(resp, "Connection: close") {
					break
				}
			}
			conn.Close()
		}
	}()
}

// singleConnBackend accepts connections until one of them actually sends a
// real request (skipping over anything else, such as a stray
// health-check probe), then commits to serving every remaining queued
// response off that one connection only, never accepting again. Because
// of that, it structurally proves whether the proxy reused this backend
// connection across requests: if the proxy instead dialed a fresh one for
// a later request, nothing would ever accept it, and that request would
// hang waiting for a response that never arrives.
func singleConnBackend(t *testing.T, ln net.Listener, responses []string) {
	t.Helper()
	go func() {
		var conn net.Conn
		var r *bufio.Reader
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			br := bufio.NewReader(c)
			req, err := http.ReadRequest(br)
			if err != nil {
				c.Close()
				continue
			}
			io.Copy(io.Discard, req.Body)
			conn, r = c, br
			if _, err := conn.Write([]byte(responses[0])); err != nil {
				conn.Close()
				return
			}
			break
		}
		defer conn.Close()
		for _, resp := range responses[1:] {
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

// acceptRealRequest accepts connections on ln until one of them actually
// sends a parseable HTTP request, skipping and closing anything else
// (such as a stray health-check probe that just connects and closes), and
// returns that connection with the request already parsed off it.
func acceptRealRequest(ln net.Listener) (net.Conn, *http.Request, error) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return nil, nil, err
		}
		req, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			conn.Close()
			continue
		}
		return conn, req, nil
	}
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

func TestConnectionPoolingReusesBackendConnection(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()
	singleConnBackend(t, backendLn, []string{canned200("one"), canned200("two"), canned200("three")})

	proxyAddr := startProxy(t, backendLn.Addr().String())

	// Three separate client connections, each making one request.
	// singleConnBackend only ever accepts once, so this only works at all
	// if the proxy pools and reuses that one backend connection across
	// all three, an unpooled proxy dialing fresh each time would leave
	// requests two and three hanging against a backend that never
	// accepts them.
	for _, want := range []string{"one", "two", "three"} {
		client, err := net.Dial("tcp", proxyAddr)
		if err != nil {
			t.Fatalf("dial proxy: %v", err)
		}
		client.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
			t.Fatalf("write request: %v", err)
		}
		got := readOneResponse(t, bufio.NewReader(client))
		client.Close()
		if got != want {
			t.Fatalf("body = %q, want %q", got, want)
		}
	}
}

func TestConnectionCloseResponseIsNotPooled(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()

	go func() {
		// First connection: exactly one response, explicitly
		// Connection: close, then the backend itself hangs up, exactly
		// like a real server honoring the header it just sent.
		conn1, req, err := acceptRealRequest(backendLn)
		if err != nil {
			return
		}
		io.Copy(io.Discard, req.Body)
		conn1.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nfirst"))
		conn1.Close()

		// A second, separate connection is expected for the next
		// request: the first one was not poolable, so if the proxy
		// tried to reuse it anyway, this Accept would never be needed
		// and the next request would just fail against the dead
		// connection instead.
		conn2, req2, err := acceptRealRequest(backendLn)
		if err != nil {
			return
		}
		defer conn2.Close()
		io.Copy(io.Discard, req2.Body)
		conn2.Write([]byte(canned200("second-conn")))
	}()

	proxyAddr := startProxy(t, backendLn.Addr().String())

	client1, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	client1.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := client1.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request 1: %v", err)
	}
	if got, want := readOneResponse(t, bufio.NewReader(client1)), "first"; got != want {
		t.Fatalf("first body = %q, want %q", got, want)
	}
	client1.Close()

	client2, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client2.Close()
	client2.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := client2.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request 2: %v", err)
	}
	if got, want := readOneResponse(t, bufio.NewReader(client2)), "second-conn"; got != want {
		t.Fatalf("second body = %q, want %q (a Connection: close response should not have been pooled)", got, want)
	}
}

// TestStalePooledConnectionIsRetriedTransparently exercises the exact
// scenario milestone 4 left undefended (a pooled connection the backend
// quietly closed while it sat idle), now that milestone 5 adds retry
// eligibility for it: a write failing on a connection taken from the pool
// is treated as a dead-connection signal, not as evidence the backend saw
// anything, so it is retried once against a fresh connection rather than
// failing the request outright.
func TestStalePooledConnectionIsRetriedTransparently(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()

	backendSideConn := make(chan net.Conn, 1)
	go func() {
		conn, req, err := acceptRealRequest(backendLn)
		if err != nil {
			return
		}
		backendSideConn <- conn
		io.Copy(io.Discard, req.Body)
		conn.Write([]byte(canned200("first")))
		// Deliberately left open (ordinary keep-alive, no Connection:
		// close), so the proxy pools it. The test will then kill it out
		// from under the pool, simulating the backend dropping an idle
		// connection on its own schedule.

		// A second, fresh connection for the retry the stale pooled one
		// should trigger.
		conn2, req2, err := acceptRealRequest(backendLn)
		if err != nil {
			return
		}
		defer conn2.Close()
		io.Copy(io.Discard, req2.Body)
		conn2.Write([]byte(canned200("second")))
	}()

	proxyAddr := startProxy(t, backendLn.Addr().String())

	client1, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	client1.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := client1.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request 1: %v", err)
	}
	if got, want := readOneResponse(t, bufio.NewReader(client1)), "first"; got != want {
		t.Fatalf("first body = %q, want %q", got, want)
	}
	client1.Close()

	(<-backendSideConn).Close()
	// Give the close a moment to actually land before the pool entry
	// gets reused, otherwise this races the closure itself.
	time.Sleep(50 * time.Millisecond)

	client2, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client2.Close()
	client2.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := client2.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request 2: %v", err)
	}
	if got, want := readOneResponse(t, bufio.NewReader(client2)), "second"; got != want {
		t.Fatalf("second body = %q, want %q (a stale pooled connection should be retried transparently)", got, want)
	}
}

// TestRequestWithBodyNotRetriedOnStalePooledConnection proves the safety
// gate that stops the transparent retry above from ever resending a body:
// replaying a request whose body may already have been partially written
// isn't safe without buffering it, which isn't implemented, so a request
// with one is never retried even in the otherwise-eligible stale-pooled
// -connection case, it just fails.
func TestRequestWithBodyNotRetriedOnStalePooledConnection(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()

	backendSideConn := make(chan net.Conn, 1)
	go func() {
		conn, req, err := acceptRealRequest(backendLn)
		if err != nil {
			return
		}
		backendSideConn <- conn
		io.Copy(io.Discard, req.Body)
		conn.Write([]byte(canned200("first")))
	}()

	proxyAddr := startProxy(t, backendLn.Addr().String())

	client1, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	client1.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := client1.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request 1: %v", err)
	}
	if got, want := readOneResponse(t, bufio.NewReader(client1)), "first"; got != want {
		t.Fatalf("first body = %q, want %q", got, want)
	}
	client1.Close()

	(<-backendSideConn).Close()
	time.Sleep(50 * time.Millisecond)

	client2, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client2.Close()
	body := "hello"
	req := fmt.Sprintf("POST / HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
	if _, err := client2.Write([]byte(req)); err != nil {
		t.Fatalf("write request 2: %v", err)
	}
	client2.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 16)
	_, err = client2.Read(buf)
	if err != io.EOF {
		t.Fatalf("got err %v, want io.EOF (a request with a body must not be retried, even on an otherwise-eligible stale pooled connection)", err)
	}
}

func TestIdempotentRequestRetriedAfterBackendFailsToRespond(t *testing.T) {
	backend1Ln := listenLoopback(t)
	defer backend1Ln.Close()
	go func() {
		conn, req, err := acceptRealRequest(backend1Ln)
		if err != nil {
			return
		}
		defer conn.Close()
		io.Copy(io.Discard, req.Body)
		// Accept the request, then just hang up without responding: the
		// write to this backend succeeds, but the read never gets an
		// answer.
	}()

	backend2Ln := listenLoopback(t)
	defer backend2Ln.Close()
	rawHTTPBackend(t, backend2Ln, []string{canned200("from-second-backend")})

	proxyAddr := startProxy(t, backend1Ln.Addr().String(), backend2Ln.Addr().String())

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if got, want := readOneResponse(t, bufio.NewReader(client)), "from-second-backend"; got != want {
		t.Fatalf("body = %q, want %q (an idempotent request should be retried against the other backend)", got, want)
	}
}

func TestNonIdempotentRequestNotRetriedAfterBackendFailsToRespond(t *testing.T) {
	backend1Ln := listenLoopback(t)
	defer backend1Ln.Close()
	go func() {
		conn, req, err := acceptRealRequest(backend1Ln)
		if err != nil {
			return
		}
		defer conn.Close()
		io.Copy(io.Discard, req.Body)
		// Accept the request, then just hang up without responding.
	}()

	backend2Ln := listenLoopback(t)
	defer backend2Ln.Close()
	// Queue a response on the second backend so that if the proxy
	// incorrectly retried against it, the test would observe a real
	// response instead of a hang, making the bug obvious rather than
	// just slow.
	rawHTTPBackend(t, backend2Ln, []string{canned200("should-not-be-used")})

	proxyAddr := startProxy(t, backend1Ln.Addr().String(), backend2Ln.Addr().String())

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()
	if _, err := client.Write([]byte("POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 0\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}
	client.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 16)
	_, err = client.Read(buf)
	if err != io.EOF {
		t.Fatalf("got err %v, want io.EOF (a non-idempotent request must not be retried once the backend may have already seen it)", err)
	}
}

func TestConnectTimeoutAppliesToBackendDial(t *testing.T) {
	ln := listenLoopback(t)
	srv := &Server{
		// A private, non-routable address: connect() neither succeeds
		// nor is refused, it just hangs, which is exactly what exercises
		// a connect timeout rather than an immediate "connection
		// refused" that would pass for the wrong reason.
		Backends:       []string{"10.255.255.1:19999"},
		ConnectTimeout: 200 * time.Millisecond,
	}
	go srv.Serve(ln)
	t.Cleanup(func() { ln.Close() })
	proxyAddr := ln.Addr().String()

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()
	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}

	start := time.Now()
	client.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 16)
	_, err = client.Read(buf)
	elapsed := time.Since(start)

	if err != io.EOF {
		t.Fatalf("got err %v, want io.EOF", err)
	}
	if elapsed > 1*time.Second {
		t.Fatalf("connect timeout was not applied: took %v to fail, want well under 1s", elapsed)
	}
}

func TestHeaderTimeoutClosesIdleConnection(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()

	ln := listenLoopback(t)
	srv := &Server{
		Backends:      []string{backendLn.Addr().String()},
		HeaderTimeout: 100 * time.Millisecond,
	}
	go srv.Serve(ln)
	t.Cleanup(func() { ln.Close() })

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()

	// Send nothing at all, not even a partial request line, and confirm
	// the proxy gives up rather than holding this connection open
	// forever.
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	_, err = client.Read(buf)
	if err != io.EOF {
		t.Fatalf("got err %v, want io.EOF (an idle connection past the header timeout should be closed)", err)
	}
}

func TestResponseTimeoutFailsSlowBackend(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()
	go func() {
		conn, req, err := acceptRealRequest(backendLn)
		if err != nil {
			return
		}
		defer conn.Close()
		io.Copy(io.Discard, req.Body)
		// Never respond at all: the response timeout, not a hang, should
		// be what ends this.
	}()

	ln := listenLoopback(t)
	srv := &Server{
		Backends:        []string{backendLn.Addr().String()},
		ResponseTimeout: 150 * time.Millisecond,
	}
	go srv.Serve(ln)
	t.Cleanup(func() { ln.Close() })

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()
	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}

	start := time.Now()
	client.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 16)
	_, err = client.Read(buf)
	elapsed := time.Since(start)

	if err != io.EOF {
		t.Fatalf("got err %v, want io.EOF", err)
	}
	if elapsed > 1*time.Second {
		t.Fatalf("response timeout was not applied: took %v to fail, want well under 1s", elapsed)
	}
}

// TestClientDisconnectCancelsSlowBackendWork is the failure experiment
// listed in relayforge.md for this milestone: disconnect a client while
// the proxy is waiting on a slow backend, and confirm the backend
// work/connection tied to that request is cancelled and cleaned up rather
// than left running to completion for no one.
func TestClientDisconnectCancelsSlowBackendWork(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()

	backendSawCancellation := make(chan bool, 1)
	go func() {
		conn, req, err := acceptRealRequest(backendLn)
		if err != nil {
			return
		}
		defer conn.Close()
		io.Copy(io.Discard, req.Body)

		// A generous deadline that only matters if cancellation is
		// broken: if the proxy tears this connection down early after
		// the client disconnects, this read returns well before it.
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		buf := make([]byte, 1)
		_, err = conn.Read(buf)
		backendSawCancellation <- err != nil
	}()

	proxyAddr := startProxy(t, backendLn.Addr().String())

	// Snapshot the goroutine count after the server and backend fake are
	// already running but before this specific request starts, so the
	// comparison below isolates goroutines this one request spins up
	// (handleConn, the disconnect watcher) rather than being thrown off
	// by the server's own permanent per-instance goroutines (the health
	// checker has no shutdown yet, a separate, already-documented gap).
	runtime.Gosched()
	time.Sleep(20 * time.Millisecond)
	before := runtime.NumGoroutine()

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}

	// Give the proxy a moment to have forwarded the request and be
	// genuinely blocked waiting on the backend's response, then
	// disconnect as the client.
	time.Sleep(100 * time.Millisecond)
	client.Close()

	select {
	case sawCancellation := <-backendSawCancellation:
		if !sawCancellation {
			t.Fatal("backend connection was not torn down early after the client disconnected")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting to observe whether the backend connection was cancelled")
	}

	// This request's own goroutines (handleConn, the disconnect watcher)
	// should have fully exited by now rather than leaking, give them a
	// moment to actually finish scheduling out, then confirm it.
	deadline := time.Now().Add(1 * time.Second)
	for {
		runtime.Gosched()
		current := runtime.NumGoroutine()
		if current <= before {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine count did not return to baseline after cancellation: before=%d, still=%d", before, current)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// delayedBackend accepts connections concurrently (unlike rawHTTPBackend,
// which serves one request at a time), and for each one waits delay
// before answering with a body of label. hits counts how many requests
// actually reached it, used to measure how a load-balancing strategy
// distributed work across backends of different speed. Each response
// says Connection: close, matching what this fake actually does (closes
// the connection right after responding, it isn't a keep-alive server),
// so the proxy doesn't pool a connection this fake has already hung up
// on.
func delayedBackend(t *testing.T, ln net.Listener, delay time.Duration, label string, hits *int64) {
	t.Helper()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				req, err := http.ReadRequest(bufio.NewReader(conn))
				if err != nil {
					return
				}
				io.Copy(io.Discard, req.Body)
				atomic.AddInt64(hits, 1)
				time.Sleep(delay)
				resp := fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(label), label)
				conn.Write([]byte(resp))
			}(conn)
		}
	}()
}

// TestLeastConnectionsFavorsFasterBackend is the reason this strategy
// exists as an alternative to round robin: under uneven backend cost,
// round robin would split load exactly 50/50 regardless of how long each
// backend takes, but least-connections should notice the slow backend is
// still busy and route more of a concurrent burst to the faster one.
func TestLeastConnectionsFavorsFasterBackend(t *testing.T) {
	slowLn := listenLoopback(t)
	defer slowLn.Close()
	var slowHits int64
	delayedBackend(t, slowLn, 150*time.Millisecond, "slow", &slowHits)

	fastLn := listenLoopback(t)
	defer fastLn.Close()
	var fastHits int64
	delayedBackend(t, fastLn, 5*time.Millisecond, "fast", &fastHits)

	ln := listenLoopback(t)
	srv := &Server{
		Backends: []string{slowLn.Addr().String(), fastLn.Addr().String()},
		Strategy: StrategyLeastConnections,
	}
	go srv.Serve(ln)
	t.Cleanup(func() { ln.Close() })
	proxyAddr := ln.Addr().String()

	var wg sync.WaitGroup
	const n = 20
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := net.Dial("tcp", proxyAddr)
			if err != nil {
				return
			}
			defer client.Close()
			client.SetReadDeadline(time.Now().Add(3 * time.Second))
			if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
				return
			}
			resp, err := http.ReadResponse(bufio.NewReader(client), &http.Request{Method: "GET"})
			if err != nil {
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()
		time.Sleep(2 * time.Millisecond)
	}
	wg.Wait()

	if fastHits <= slowHits {
		t.Fatalf("expected least-connections to favor the faster backend under concurrent load, got slow=%d fast=%d", slowHits, fastHits)
	}
}

func TestLeastConnectionsSkipsUnhealthyBackend(t *testing.T) {
	aliveLn := listenLoopback(t)
	defer aliveLn.Close()
	responses := make([]string, 10)
	for i := range responses {
		responses[i] = canned200("alive")
	}
	rawHTTPBackend(t, aliveLn, responses)

	deadLn := listenLoopback(t)
	deadAddr := deadLn.Addr().String()
	deadLn.Close()

	ln := listenLoopback(t)
	srv := &Server{
		Backends:            []string{aliveLn.Addr().String(), deadAddr},
		Strategy:            StrategyLeastConnections,
		HealthCheckInterval: 10 * time.Millisecond,
		HealthCheckTimeout:  50 * time.Millisecond,
	}
	go srv.Serve(ln)
	t.Cleanup(func() { ln.Close() })
	proxyAddr := ln.Addr().String()

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

func TestInvalidStrategyRejectedAtStartup(t *testing.T) {
	ln := listenLoopback(t)
	defer ln.Close()
	srv := &Server{
		Backends: []string{"127.0.0.1:1"},
		Strategy: "some-made-up-strategy",
	}
	if err := srv.Serve(ln); err == nil {
		t.Fatal("expected Serve to reject an unrecognized strategy, got nil error")
	}
}

func TestShutdownStopsAcceptingNewConnections(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()
	rawHTTPBackend(t, backendLn, []string{canned200("ok")})

	ln := listenLoopback(t)
	srv := &Server{Backends: []string{backendLn.Addr().String()}}
	go srv.Serve(ln)
	proxyAddr := ln.Addr().String()

	// Give Serve a moment to actually be listening before shutting down
	// immediately.
	time.Sleep(20 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if _, err := net.DialTimeout("tcp", proxyAddr, 1*time.Second); err == nil {
		t.Fatal("expected dialing the proxy after Shutdown to fail, it succeeded")
	}
}

// TestShutdownLetsInFlightRequestFinishThenClosesConnection is the
// failure experiment this milestone is actually about: a request already
// in flight against a slow backend should complete normally even though
// shutdown starts while it's still running, and only once that finishes
// does the (now idle) persistent connection get closed rather than kept
// around for a request that may never come.
func TestShutdownLetsInFlightRequestFinishThenClosesConnection(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()
	go func() {
		conn, req, err := acceptRealRequest(backendLn)
		if err != nil {
			return
		}
		defer conn.Close()
		io.Copy(io.Discard, req.Body)
		time.Sleep(300 * time.Millisecond)
		conn.Write([]byte(canned200("finished")))
	}()

	ln := listenLoopback(t)
	srv := &Server{Backends: []string{backendLn.Addr().String()}}
	go srv.Serve(ln)
	proxyAddr := ln.Addr().String()

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()
	client.SetReadDeadline(time.Now().Add(3 * time.Second))
	clientReader := bufio.NewReader(client)

	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request 1: %v", err)
	}

	// Give the proxy a moment to actually be mid-exchange with the slow
	// backend before shutdown starts.
	time.Sleep(50 * time.Millisecond)

	shutdownErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		shutdownErr <- srv.Shutdown(ctx)
	}()

	if got, want := readOneResponse(t, clientReader), "finished"; got != want {
		t.Fatalf("body = %q, want %q (in-flight request should finish during shutdown)", got, want)
	}
	if err := <-shutdownErr; err != nil {
		t.Fatalf("Shutdown returned %v, want nil (should have waited for the in-flight request)", err)
	}

	// Shutdown having returned nil means every tracked connection,
	// including this one, has already finished and closed. A bare read
	// (no second write, writing onto a socket at the exact instant its
	// peer closes is its own Windows-specific race that reports
	// ECONNABORTED instead of a clean EOF) should find it already gone.
	buf := make([]byte, 16)
	_, err = clientReader.Read(buf)
	if err != io.EOF {
		t.Fatalf("got err %v, want io.EOF (connection should be closed once shutdown has completed)", err)
	}
}

func TestShutdownForceClosesAfterDeadline(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()
	go func() {
		conn, req, err := acceptRealRequest(backendLn)
		if err != nil {
			return
		}
		defer conn.Close()
		io.Copy(io.Discard, req.Body)
		// Never respond: this request is still in flight when
		// Shutdown's deadline passes.
		select {}
	}()

	ln := listenLoopback(t)
	srv := &Server{
		Backends:        []string{backendLn.Addr().String()},
		ResponseTimeout: 10 * time.Second,
	}
	go srv.Serve(ln)
	proxyAddr := ln.Addr().String()

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()
	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	shutdownErr := srv.Shutdown(ctx)
	elapsed := time.Since(start)

	if !errors.Is(shutdownErr, context.DeadlineExceeded) {
		t.Fatalf("Shutdown returned %v, want context.DeadlineExceeded", shutdownErr)
	}
	if elapsed > 1*time.Second {
		t.Fatalf("Shutdown took %v to return after its deadline, want close to 200ms", elapsed)
	}

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	_, err = client.Read(buf)
	if err != io.EOF {
		t.Fatalf("got err %v, want io.EOF (stuck connection should be force-closed once Shutdown's deadline passes)", err)
	}
}

func TestShutdownStopsHealthCheckGoroutine(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()

	ln := listenLoopback(t)
	srv := &Server{
		Backends:            []string{backendLn.Addr().String()},
		HealthCheckInterval: 10 * time.Millisecond,
		HealthCheckTimeout:  50 * time.Millisecond,
	}

	runtime.Gosched()
	time.Sleep(20 * time.Millisecond)
	before := runtime.NumGoroutine()

	go srv.Serve(ln)
	time.Sleep(50 * time.Millisecond) // let it probe at least once

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	deadline := time.Now().Add(1 * time.Second)
	for {
		runtime.Gosched()
		current := runtime.NumGoroutine()
		if current <= before {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine count did not return to baseline after Shutdown: before=%d, still=%d", before, current)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestMaxInFlightRejectsOverCapacity is the failure experiment this
// milestone is about: push more concurrent requests than the configured
// limit against a backend too slow to drain them, and confirm the excess
// gets a prompt, explicit 503 rather than piling up unboundedly or
// hanging.
func TestMaxInFlightRejectsOverCapacity(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()
	var hits int64
	delayedBackend(t, backendLn, 300*time.Millisecond, "ok", &hits)

	ln := listenLoopback(t)
	srv := &Server{
		Backends:    []string{backendLn.Addr().String()},
		MaxInFlight: 2,
	}
	go srv.Serve(ln)
	t.Cleanup(func() { ln.Close() })
	proxyAddr := ln.Addr().String()

	const n = 6
	statusCodes := make([]int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			client, err := net.Dial("tcp", proxyAddr)
			if err != nil {
				return
			}
			defer client.Close()
			client.SetReadDeadline(time.Now().Add(3 * time.Second))
			if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
				return
			}
			resp, err := http.ReadResponse(bufio.NewReader(client), &http.Request{Method: "GET"})
			if err != nil {
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			statusCodes[i] = resp.StatusCode
		}(i)
	}
	wg.Wait()

	var ok, rejected int
	for _, code := range statusCodes {
		switch code {
		case http.StatusOK:
			ok++
		case http.StatusServiceUnavailable:
			rejected++
		}
	}
	if ok > 2 {
		t.Fatalf("got %d successful requests, want at most MaxInFlight=2", ok)
	}
	if rejected == 0 {
		t.Fatal("expected at least one request to be rejected with 503, got none")
	}
	if ok+rejected != n {
		t.Fatalf("got %d ok + %d rejected = %d responses, want all %d accounted for", ok, rejected, ok+rejected, n)
	}
}

// TestMaxConnectionsLimitsAcceptedConnections confirms the connection
// cap gates actual request processing, not just raw TCP handshakes: a
// connection attempted while already at capacity can still complete its
// handshake (the kernel's own listen backlog doesn't know about our
// limit), but gets no response until an existing connection closes and
// frees a slot.
func TestMaxConnectionsLimitsAcceptedConnections(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()
	rawHTTPBackend(t, backendLn, []string{canned200("one"), canned200("two"), canned200("three")})

	ln := listenLoopback(t)
	srv := &Server{
		Backends:       []string{backendLn.Addr().String()},
		MaxConnections: 2,
	}
	go srv.Serve(ln)
	t.Cleanup(func() { ln.Close() })
	proxyAddr := ln.Addr().String()

	conn1, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial connection 1: %v", err)
	}
	defer conn1.Close()
	conn2, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial connection 2: %v", err)
	}
	defer conn2.Close()

	// Give the accept loop a moment to actually accept both and fill its
	// two connSem slots.
	time.Sleep(50 * time.Millisecond)

	conn3, err := net.DialTimeout("tcp", proxyAddr, 1*time.Second)
	if err != nil {
		t.Fatalf("dial connection 3: %v", err)
	}
	defer conn3.Close()
	if _, err := conn3.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write on connection 3: %v", err)
	}

	conn3.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	buf := make([]byte, 16)
	_, err = conn3.Read(buf)
	netErr, isNetErr := err.(net.Error)
	if !isNetErr || !netErr.Timeout() {
		t.Fatalf("connection 3 got %v before any slot freed, want a read timeout (it should not have been accepted yet)", err)
	}

	// Free a slot; connection 3 should now get served.
	conn1.Close()

	conn3.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn3), &http.Request{Method: "GET"})
	if err != nil {
		t.Fatalf("read response on connection 3 after a slot freed: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if got := string(body); got != "one" && got != "two" && got != "three" {
		t.Fatalf("unexpected body %q", got)
	}
}

// fetchMetrics fetches and returns the raw Prometheus-text body served at
// /metrics on addr.
func fetchMetrics(t *testing.T, addr string) string {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("fetch metrics: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read metrics body: %v", err)
	}
	return string(body)
}

// reserveAddr binds an ephemeral port, closes it immediately, and returns
// the address so a caller can pass it to something else that will bind
// its own listener there a moment later. Used here to hand MetricsListenAddr
// a free port before the metrics http.Server binds it for real.
func reserveAddr(t *testing.T) string {
	t.Helper()
	ln := listenLoopback(t)
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func TestMetricsReportsRequestAndBackendCounts(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()
	responses := make([]string, 3)
	for i := range responses {
		responses[i] = canned200("ok")
	}
	rawHTTPBackend(t, backendLn, responses)

	metricsAddr := reserveAddr(t)

	ln := listenLoopback(t)
	srv := &Server{
		Backends:          []string{backendLn.Addr().String()},
		MetricsListenAddr: metricsAddr,
	}
	go srv.Serve(ln)
	t.Cleanup(func() { ln.Close() })
	proxyAddr := ln.Addr().String()
	time.Sleep(50 * time.Millisecond) // let the metrics server actually bind

	for i := 0; i < 3; i++ {
		client, err := net.Dial("tcp", proxyAddr)
		if err != nil {
			t.Fatalf("dial proxy: %v", err)
		}
		client.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
			t.Fatalf("write request: %v", err)
		}
		readOneResponse(t, bufio.NewReader(client))
		client.Close()
	}

	metrics := fetchMetrics(t, metricsAddr)
	if !strings.Contains(metrics, "relayforge_requests_total 3\n") {
		t.Fatalf("metrics missing relayforge_requests_total 3:\n%s", metrics)
	}
	wantRequests := fmt.Sprintf("relayforge_backend_requests_total{backend=%q} 3\n", backendLn.Addr().String())
	if !strings.Contains(metrics, wantRequests) {
		t.Fatalf("metrics missing %q:\n%s", wantRequests, metrics)
	}
	wantUp := fmt.Sprintf("relayforge_backend_up{backend=%q} 1\n", backendLn.Addr().String())
	if !strings.Contains(metrics, wantUp) {
		t.Fatalf("metrics missing %q:\n%s", wantUp, metrics)
	}
}

func TestMetricsReportsRetries(t *testing.T) {
	backend1Ln := listenLoopback(t)
	defer backend1Ln.Close()
	go func() {
		conn, req, err := acceptRealRequest(backend1Ln)
		if err != nil {
			return
		}
		defer conn.Close()
		io.Copy(io.Discard, req.Body)
		// Accept the request, then hang up without responding: the
		// write succeeds, but the read never gets an answer, which
		// makes this retry-eligible (idempotent GET).
	}()

	backend2Ln := listenLoopback(t)
	defer backend2Ln.Close()
	rawHTTPBackend(t, backend2Ln, []string{canned200("ok")})

	metricsAddr := reserveAddr(t)

	ln := listenLoopback(t)
	srv := &Server{
		Backends:          []string{backend1Ln.Addr().String(), backend2Ln.Addr().String()},
		MetricsListenAddr: metricsAddr,
	}
	go srv.Serve(ln)
	t.Cleanup(func() { ln.Close() })
	proxyAddr := ln.Addr().String()
	time.Sleep(50 * time.Millisecond)

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if got, want := readOneResponse(t, bufio.NewReader(client)), "ok"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}

	metrics := fetchMetrics(t, metricsAddr)
	if !strings.Contains(metrics, "relayforge_retries_total 1\n") {
		t.Fatalf("metrics missing relayforge_retries_total 1:\n%s", metrics)
	}
}

func TestMetricsReportsRejections(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()
	go func() {
		conn, req, err := acceptRealRequest(backendLn)
		if err != nil {
			return
		}
		defer conn.Close()
		io.Copy(io.Discard, req.Body)
		time.Sleep(300 * time.Millisecond)
		conn.Write([]byte(canned200("ok")))
	}()

	metricsAddr := reserveAddr(t)

	ln := listenLoopback(t)
	srv := &Server{
		Backends:          []string{backendLn.Addr().String()},
		MetricsListenAddr: metricsAddr,
		MaxInFlight:       1,
	}
	go srv.Serve(ln)
	t.Cleanup(func() { ln.Close() })
	proxyAddr := ln.Addr().String()
	time.Sleep(50 * time.Millisecond)

	client1, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client1.Close()
	if _, err := client1.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request 1: %v", err)
	}

	// Give request 1 a moment to actually occupy the single MaxInFlight
	// slot before request 2 arrives.
	time.Sleep(50 * time.Millisecond)

	client2, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client2.Close()
	if _, err := client2.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request 2: %v", err)
	}
	client2.SetReadDeadline(time.Now().Add(1 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(client2), &http.Request{Method: "GET"})
	if err != nil {
		t.Fatalf("read response 2: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("request 2 status = %d, want 503", resp.StatusCode)
	}

	metrics := fetchMetrics(t, metricsAddr)
	if !strings.Contains(metrics, "relayforge_rejected_total 1\n") {
		t.Fatalf("metrics missing relayforge_rejected_total 1:\n%s", metrics)
	}
}
