package proxy

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
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

func TestPooledConnectionGoneStaleFailsCleanly(t *testing.T) {
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
		// close), so the proxy pools it. The test itself will then kill
		// it out from under the pool, simulating the backend dropping an
		// idle connection on its own schedule.
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
	if _, err := client2.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request 2: %v", err)
	}
	client2.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 16)
	_, err = client2.Read(buf)
	if err != io.EOF {
		t.Fatalf("got err %v, want io.EOF (a stale pooled connection should fail the request cleanly, no retry yet)", err)
	}
}
