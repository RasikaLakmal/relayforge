package proxy

import (
	"io"
	"net"
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

func TestForwardsBytesBothDirections(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()

	go func() {
		conn, err := backendLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Echo back everything the client sends, uppercased, so the test
		// can tell the bytes actually round-tripped through the backend
		// rather than a client just reading back its own write.
		buf := make([]byte, 4096)
		n, _ := conn.Read(buf)
		conn.Write([]byte("echo:" + string(buf[:n])))
	}()

	proxyAddr := startProxy(t, backendLn.Addr().String())

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()

	if _, err := client.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4096)
	n, err := client.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	got := string(buf[:n])
	want := "echo:hello"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestClientSeesEOFWhenBackendCloses(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()

	go func() {
		conn, err := backendLn.Accept()
		if err != nil {
			return
		}
		// Hang up immediately without writing anything.
		conn.Close()
	}()

	proxyAddr := startProxy(t, backendLn.Addr().String())

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	_, err = client.Read(buf)
	if err != io.EOF {
		t.Fatalf("got err %v, want io.EOF", err)
	}
}

func TestBackendSeesEOFWhenClientCloses(t *testing.T) {
	backendLn := listenLoopback(t)
	defer backendLn.Close()

	backendSawEOF := make(chan bool, 1)
	go func() {
		conn, err := backendLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 16)
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, err = conn.Read(buf)
		backendSawEOF <- err == io.EOF
	}()

	proxyAddr := startProxy(t, backendLn.Addr().String())

	client, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	// Close immediately without writing anything.
	client.Close()

	select {
	case sawEOF := <-backendSawEOF:
		if !sawEOF {
			t.Fatal("backend did not see EOF after client closed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for backend to observe client close")
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
