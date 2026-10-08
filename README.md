# RelayForge

TCP reverse proxy and load balancer in Go, built from scratch to actually understand connection handling, HTTP framing, load balancing, connection pooling, and backpressure rather than configuring nginx/HAProxy/Traefik and trusting the magic.

Part of a personal [engineering lab](../../checklist.md) of systems-depth projects, following [VellumDB](../vellumdb).

## What's here

- **Request/response forwarding**: each HTTP/1.1 request is read off the client connection, forwarded to a backend as its own message, and matched with its response, rather than the connection being treated as a blind byte stream.
- **Framing-aware**: `Content-Length`, chunked encoding, and `Connection: close` are parsed with `net/http`'s `http.ReadRequest`/`http.ReadResponse`, not reimplemented by hand.
- **Persistent connections**: a single TCP connection can carry many HTTP requests in sequence, gated by the connection's own close framing rather than the proxy assuming one request per connection.
- **Multiple backends, two load-balancing strategies**: backend selection happens per request, not per connection, so a client's persistent connection is not pinned to one backend. `round-robin` (the default) cycles through backends in order regardless of how busy each one is; `least-connections` picks whichever healthy backend currently has the fewest in-flight requests, which actually adapts to backends of different speed instead of splitting load blindly evenly. Kept as two comparable strategies specifically so they can be benchmarked against each other, not because one is assumed better.
- **Health checks**: each backend is probed on an interval with a plain TCP connect, and one that fails is taken out of rotation until a later probe succeeds again. A backend is assumed healthy until the first check says otherwise, so nothing is excluded before it has actually been probed.
- **Connection pooling**: a backend connection that finishes a request cleanly is kept idle and reused by a later request to that backend, regardless of which client connection that later request arrives on, instead of every request paying for its own TCP handshake.
- **Timeouts**: a connect timeout bounds dialing a backend, a header timeout bounds both how long a connection may sit idle waiting for its next request and how long that request has to finish once it starts arriving, and a response timeout bounds the whole write-request-then-read-response exchange with a backend.
- **Client disconnect cancellation**: if the client goes away while the proxy is still waiting on a backend, the backend connection is torn down immediately instead of the backend work running to completion for a client that is no longer there.
- **Retry eligibility**: a request is retried once, against a different backend, only when doing so cannot cause a duplicate side effect, the backend was never reached at all, a pooled connection turned out to be dead, or the method is idempotent (GET, HEAD, PUT, DELETE, OPTIONS, TRACE). A request with a body, or a non-idempotent request whose write may have already reached a backend, is never retried, that is the deliberately unresolved case where the backend might already have applied it.
- **Graceful shutdown**: on `SIGINT`/`SIGTERM`, the proxy stops accepting new connections immediately but lets whatever request is currently in flight on each existing connection finish normally before closing it, rather than cutting every open connection on the spot. A persistent connection that's already idle, waiting for its next request, is not forcibly interrupted, but will not be served again once it finishes whatever it's doing; a bounded grace period (`-shutdown-timeout`) forces anything still open closed if draining takes too long, so the process always actually exits.
- **Backpressure**: `-max-connections` bounds how many client connections may be open at once, accepting no further ones (letting them queue in the OS's own listen backlog instead) once at capacity. `-max-in-flight` separately bounds how many requests may be concurrently in flight to backends across all connections combined; a request past that limit gets an immediate, explicit 503 rather than being queued or silently left to pile up. Both are opt-in (zero means unbounded) since the right numbers depend on real capacity testing, not a guess.

## Usage

```
go build -o relayforge ./cmd/relayforge
./relayforge -listen 127.0.0.1:8080 -backends 127.0.0.1:9090,127.0.0.1:9091 -strategy least-connections \
  -health-interval 5s -health-timeout 2s \
  -connect-timeout 3s -header-timeout 10s -response-timeout 30s \
  -shutdown-timeout 10s \
  -max-connections 1000 -max-in-flight 200
```

Send `SIGINT` (Ctrl+C) or `SIGTERM` to shut down gracefully: in-flight requests get to finish, new connections are refused immediately, and anything still open past `-shutdown-timeout` is forced closed so the process exits either way.

Every request is forwarded to one of `-backends`, chosen by `-strategy` (`round-robin` or `least-connections`) among whichever of them the health checker currently considers reachable.

## Known limitations

- No config file yet, backends are a flag-supplied comma-separated list.
- Health checks are a plain TCP connect, not an HTTP-level check against a real health endpoint, and they run on a fixed interval rather than reacting to a request that actually failed. A backend that dies between two probes still gets picked and fails whatever request lands on it until the next probe catches it.
- If every backend is currently unhealthy, a request simply fails, there is nothing to fall back to.
- The idle pool per backend is capped at a fixed 8 connections, not configurable yet, there is no benchmark yet that would justify exposing it as a flag.
- With only one backend configured, a retry-eligible failure retries against that same backend. This is still bounded, exactly one retry, never a cascade, but it means a genuinely slow single backend gets waited on twice (roughly double the response timeout) before the request fails, rather than failing fast.
- A request with a body is never retried under any circumstances, even in the otherwise-safe case where the backend was never reached at all. Replaying a body safely needs buffering it first, which isn't implemented yet, so this is more conservative than it strictly needs to be.
- The single HeaderTimeout value governs both a persistent connection's idle wait for its next request and how long that request has once it starts arriving; production proxies often split these into two separate knobs, this project doesn't yet.
- Health checks, retries, and timeouts are three independent mechanisms that don't yet coordinate: a backend mid-retry isn't marked unhealthy any faster because of it, and a health check failing doesn't cancel a request already in flight against that backend.
- Least-connections measures "in flight" purely as time spent actually talking to a backend (the write-then-read exchange), not overall client-perceived latency, and it has no memory of a backend's recent performance beyond its current in-flight count. Two backends serving requests at genuinely different rates but with the same momentary in-flight count look identical to it.
- A persistent connection that is already idle (waiting for its next request) when shutdown starts is not proactively closed, it is simply never served again once whatever it does next finishes; if a client just never sends another request and never disconnects, that one connection lingers until the shutdown deadline forces it. Only a connection with a request already in flight is guaranteed to wrap up and close promptly.
- A request rejected for being over `-max-in-flight` has its body left unread and its connection always closed afterward, even if the request itself would otherwise have been a harmless, poolable keep-alive one. Simpler than trying to decide per-rejection whether continuing the connection is worth it while already under more load than the configured limit allows.
- When a backend request ultimately fails, even after the one eligible retry, the client gets nothing, the connection is just closed, with no HTTP error response written at all. A client sees this as a bare connection reset rather than any status code, which is indistinguishable from a network-level problem. This is different from, and easy to confuse with, the `-max-in-flight` 503, which is a real written response; a proper error response here (502/504) is unaddressed scope, not something this milestone covers.
- `-max-connections` and `-max-in-flight` are independent knobs that don't coordinate: a proxy at its connection cap but with in-flight headroom still accepts no new connections, and vice versa. Picking sensible values for both requires the real benchmark numbers milestone 11 is for, there's no principled default yet, which is why both are opt-in (zero, unbounded) rather than defaulting to some guessed-at number.
- Hop-by-hop headers (`Connection`, `Keep-Alive`, `TE`, `Trailer`, etc.) are forwarded to the backend as-is rather than being stripped per RFC 7230 6.1. Harmless for the backends tested against so far, but not strictly correct proxy behavior.
- `Request.Write` always emits the request line as HTTP/1.1 regardless of what the client actually sent, so an HTTP/1.0 request is silently upgraded on the wire to the backend.
- `go test -race` could not be run in this environment: no C compiler is installed, and the race detector requires cgo. Tests were run and pass under plain `go test ./...`; race detection should be run wherever a C toolchain is available before trusting concurrency-sensitive work (connection pooling, health state, retry counters) at face value.

## Testing

```
go test ./...
```

Covers: a single request/response round trip through the proxy, a chunked response body reassembled correctly, two requests carried over one persistent connection, a connection torn down after a `Connection: close` response, a malformed request rejected cleanly instead of hanging the proxy, the client connection closed cleanly when a backend dial fails, requests round-robining across backends in the correct order over a single persistent connection, a dead backend getting routed around once health checks converge, a backend rejoining rotation after it recovers, every backend being unhealthy failing requests cleanly, health checks being periodic rather than catching a backend the instant it dies, a backend connection actually being reused across three separate client connections, a `Connection: close` response never being pooled, a stale pooled connection being retried transparently, a request with a body never being retried even on an otherwise-eligible stale connection, an idempotent request being retried against a different backend after one fails to respond, a non-idempotent request never being retried in that same situation, the connect timeout actually applying to a hanging dial, an idle connection past the header timeout being closed, a hanging backend failing after the response timeout, a client disconnecting mid-request cancelling the backend work with no goroutine leak, least-connections favoring a faster backend under a concurrent burst against a slower one, least-connections skipping an unhealthy backend, an unrecognized strategy being rejected at startup, shutdown refusing new connections immediately, an in-flight request finishing successfully during shutdown before its now-idle connection closes, a stuck request being force-closed once the shutdown deadline passes, the health-check goroutine actually stopping after shutdown instead of leaking, a burst of requests past `-max-in-flight` getting 503s for the excess while the rest succeed, and a connection attempted past `-max-connections` getting no response until an existing one closes and frees a slot.

A separate benchmark comparing round robin against least-connections under uneven backend cost, with real numbers instead of the qualitative demonstration above, belongs in milestone 11.
