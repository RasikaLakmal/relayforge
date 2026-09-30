# RelayForge

TCP reverse proxy and load balancer in Go, built from scratch to actually understand connection handling, HTTP framing, load balancing, connection pooling, and backpressure rather than configuring nginx/HAProxy/Traefik and trusting the magic.

Part of a personal [engineering lab](../../checklist.md) of systems-depth projects, following [VellumDB](../vellumdb).

## What's here

- **Request/response forwarding**: each HTTP/1.1 request is read off the client connection, forwarded to a backend as its own message, and matched with its response, rather than the connection being treated as a blind byte stream.
- **Framing-aware**: `Content-Length`, chunked encoding, and `Connection: close` are parsed with `net/http`'s `http.ReadRequest`/`http.ReadResponse`, not reimplemented by hand.
- **Persistent connections**: a single TCP connection can carry many HTTP requests in sequence, gated by the connection's own close framing rather than the proxy assuming one request per connection.
- **Multiple backends, round robin**: backend selection happens per request, not per connection, so a client's persistent connection is not pinned to one backend, its requests still get distributed round robin across all configured backends.

## Usage

```
go build -o relayforge ./cmd/relayforge
./relayforge -listen 127.0.0.1:8080 -backends 127.0.0.1:9090,127.0.0.1:9091
```

Every request is forwarded to one of `-backends`, chosen round robin, one at a time regardless of how many backends are listed.

## Known limitations

- No config file yet, backends are a flag-supplied comma-separated list.
- No health checks yet: a dead backend is picked in its turn like any other, and the request routed to it simply fails, taking the whole client connection down with it since there is no fallback to another backend mid-request.
- No connection pooling yet: every request dials its own fresh backend connection rather than reusing one.
- Hop-by-hop headers (`Connection`, `Keep-Alive`, `TE`, `Trailer`, etc.) are forwarded to the backend as-is rather than being stripped per RFC 7230 6.1. Harmless for the backends tested against so far, but not strictly correct proxy behavior.
- `Request.Write` always emits the request line as HTTP/1.1 regardless of what the client actually sent, so an HTTP/1.0 request is silently upgraded on the wire to the backend.
- `go test -race` could not be run in this environment: no C compiler is installed, and the race detector requires cgo. Tests were run and pass under plain `go test ./...`; race detection should be run wherever a C toolchain is available before trusting concurrency-sensitive work (connection pooling, health state, retry counters) at face value.

## Testing

```
go test ./...
```

Covers: a single request/response round trip through the proxy, a chunked response body reassembled correctly, two requests carried over one persistent connection, a connection torn down after a `Connection: close` response, a malformed request rejected cleanly instead of hanging the proxy, the client connection closed cleanly when a backend dial fails, requests round-robining across backends in the correct order over a single persistent connection, and a dead backend failing only the requests routed to it.
