# RelayForge

TCP reverse proxy and load balancer in Go, built from scratch to actually understand connection handling, HTTP framing, load balancing, connection pooling, and backpressure rather than configuring nginx/HAProxy/Traefik and trusting the magic.

Part of a personal [engineering lab](../../checklist.md) of systems-depth projects, following [VellumDB](../vellumdb).

## What's here

- **Request/response forwarding**: each client connection is dialed through to a single fixed backend, then read and forwarded one HTTP/1.1 request at a time rather than as a blind byte copy, so there is a clear boundary around each request instead of just a stream of bytes.
- **Framing-aware**: `Content-Length`, chunked encoding, and `Connection: close` are parsed with `net/http`'s `http.ReadRequest`/`http.ReadResponse`, not reimplemented by hand.
- **Persistent connections**: a single TCP connection can carry many HTTP requests in sequence, gated by the connection's own close framing rather than the proxy assuming one request per connection.

## Usage

```
go build -o relayforge ./cmd/relayforge
./relayforge -listen 127.0.0.1:8080 -backend 127.0.0.1:9090
```

Every connection to `-listen` is forwarded to `-backend`. There is no routing or load balancing yet, one proxy instance talks to exactly one backend.

## Known limitations

- Single fixed backend, no config file, no multiple backends, no load balancing yet.
- No connection pooling yet: each client connection dials its own backend connection, and if the backend closes it, the client connection is closed too even if the client asked to keep it alive.
- Hop-by-hop headers (`Connection`, `Keep-Alive`, `TE`, `Trailer`, etc.) are forwarded to the backend as-is rather than being stripped per RFC 7230 6.1. Harmless for the backends tested against so far, but not strictly correct proxy behavior.
- `Request.Write` always emits the request line as HTTP/1.1 regardless of what the client actually sent, so an HTTP/1.0 request is silently upgraded on the wire to the backend.
- `go test -race` could not be run in this environment: no C compiler is installed, and the race detector requires cgo. Tests were run and pass under plain `go test ./...`; race detection should be run wherever a C toolchain is available before trusting concurrency-sensitive work (connection pooling, health state, retry counters) at face value.

## Testing

```
go test ./...
```

Covers: a single request/response round trip through the proxy, a chunked response body reassembled correctly, two requests carried over one persistent connection, a connection torn down after a `Connection: close` response, a malformed request rejected cleanly instead of hanging the proxy, and the client connection closed cleanly when the backend dial fails.
