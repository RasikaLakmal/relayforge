# RelayForge

TCP reverse proxy and load balancer in Go, built from scratch to actually understand connection handling, HTTP framing, load balancing, connection pooling, and backpressure rather than configuring nginx/HAProxy/Traefik and trusting the magic.

Part of a personal engineering lab of systems-depth projects.

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
- **Metrics**: `-metrics-listen` serves hand-written Prometheus text format at `/metrics` on its own address, separate from proxied traffic so a backend's own routes can never collide with it. Covers aggregate request/retry/rejection counters, active connection and in-flight gauges, and per-backend request/error counts, health state, and average latency (a sum and count, not a real histogram with percentiles, that's a deliberate simplification until there's a reason for real buckets).
- **Config file and reload**: `-config` points at a JSON file instead of individual flags, validated up front (malformed backend addresses, duplicate backends, an unrecognized strategy, a non-positive duration, a negative count all fail startup immediately with a clear error rather than starting up broken). On `SIGHUP`, the file is re-read, re-validated, and swapped into the running server without dropping any in-flight request: `Backends`, `Strategy`, the health-check settings, the timeouts, and `MaxInFlight` all take effect immediately; a backend address that is still present afterward keeps its existing health state and accumulated metrics rather than resetting. `ListenAddr`, `MetricsListenAddr`, and `MaxConnections` are not reloadable, changing where or how many connections are accepted needs a new listener, which is a restart, not a reload.

## Usage

```
go build -o relayforge ./cmd/relayforge
./relayforge -listen 127.0.0.1:8080 -backends 127.0.0.1:9090,127.0.0.1:9091 -strategy least-connections \
  -health-interval 5s -health-timeout 2s \
  -connect-timeout 3s -header-timeout 10s -response-timeout 30s \
  -shutdown-timeout 10s \
  -max-connections 1000 -max-in-flight 200 \
  -metrics-listen 127.0.0.1:9100
```

With `-metrics-listen` set, `curl http://127.0.0.1:9100/metrics` returns the current counters and gauges.

Send `SIGINT` (Ctrl+C) or `SIGTERM` to shut down gracefully: in-flight requests get to finish, new connections are refused immediately, and anything still open past `-shutdown-timeout` is forced closed so the process exits either way.

Every request is forwarded to one of `-backends`, chosen by `-strategy` (`round-robin` or `least-connections`) among whichever of them the health checker currently considers reachable.

### Config file

```
go build -o relayforge ./cmd/relayforge
./relayforge -config relayforge.json
```

```json
{
  "listen": "127.0.0.1:8080",
  "backends": ["127.0.0.1:9090", "127.0.0.1:9091"],
  "strategy": "least-connections",
  "health_interval": "5s",
  "health_timeout": "2s",
  "connect_timeout": "3s",
  "header_timeout": "10s",
  "response_timeout": "30s",
  "shutdown_timeout": "10s",
  "max_connections": 1000,
  "max_in_flight": 200,
  "metrics_listen": "127.0.0.1:9100"
}
```

If `-config` is set, every other flag is ignored, settings come entirely from the file. Every field is optional except `listen` and `backends`; anything else left out uses the same built-in default as its flag would. Sending `SIGHUP` re-reads and re-validates the file and reloads `backends`, `strategy`, the health-check settings, the timeouts, and `max_in_flight` into the running server without dropping any in-flight request; an invalid file at reload time is rejected and the server keeps running on its existing configuration rather than being torn down by a typo.

## Known limitations

- `SIGHUP` is a POSIX signal with no real Windows OS equivalent; Go only wires actual console events (Ctrl+C-style) to `os.Interrupt` on Windows, not `SIGHUP`. The reload trigger is effectively untestable, and likely unusable, on Windows even though the code compiles there; it was verified directly via `Server.Reload` in Go tests instead of via a real delivered signal.
- `Reload` assumes the server has already finished starting. Calling it concurrently with `Serve`'s own startup (before the first backend/config snapshot is published) could have its changes clobbered by `Serve`'s belated initialization. Not a realistic concern in practice, a reload presupposes an already-running server, but it is not actively guarded against either.
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
- A backend error counted in the metrics includes a request cancelled because the client itself disconnected mid-exchange, not only a genuine backend-side failure. The two aren't distinguished in `relayforge_backend_errors_total`, so a backend can look like it is erroring when clients are actually just leaving early.
- Per-backend latency is an average (duration sum divided by count) with no percentiles and no decay over time: one very slow request early in the process's life permanently drags the average up for as long as it keeps running, a few hours later the number is still partly describing a request that happened once.
- The metrics server has no authentication and isn't rate-limited; exposing `-metrics-listen` on anything other than a private/internal address would let anyone scrape internal request patterns and backend addresses.
- Hop-by-hop headers (`Connection`, `Keep-Alive`, `TE`, `Trailer`, etc.) are forwarded to the backend as-is rather than being stripped per RFC 7230 6.1. Harmless for the backends tested against so far, but not strictly correct proxy behavior.
- `Request.Write` always emits the request line as HTTP/1.1 regardless of what the client actually sent, so an HTTP/1.0 request is silently upgraded on the wire to the backend.
- `go test -race` could not be run in this environment: no C compiler is installed, and the race detector requires cgo. Tests were run and pass under plain `go test ./...`; race detection should be run wherever a C toolchain is available before trusting concurrency-sensitive work (connection pooling, health state, retry counters) at face value.

## Testing

```
go test ./...
```

Covers: a single request/response round trip through the proxy, a chunked response body reassembled correctly, two requests carried over one persistent connection, a connection torn down after a `Connection: close` response, a malformed request rejected cleanly instead of hanging the proxy, the client connection closed cleanly when a backend dial fails, requests round-robining across backends in the correct order over a single persistent connection, a dead backend getting routed around once health checks converge, a backend rejoining rotation after it recovers, every backend being unhealthy failing requests cleanly, health checks being periodic rather than catching a backend the instant it dies, a backend connection actually being reused across three separate client connections, a `Connection: close` response never being pooled, a stale pooled connection being retried transparently, a request with a body never being retried even on an otherwise-eligible stale connection, an idempotent request being retried against a different backend after one fails to respond, a non-idempotent request never being retried in that same situation, the connect timeout actually applying to a hanging dial, an idle connection past the header timeout being closed, a hanging backend failing after the response timeout, a client disconnecting mid-request cancelling the backend work with no goroutine leak, least-connections favoring a faster backend under a concurrent burst against a slower one, least-connections skipping an unhealthy backend, an unrecognized strategy being rejected at startup, shutdown refusing new connections immediately, an in-flight request finishing successfully during shutdown before its now-idle connection closes, a stuck request being force-closed once the shutdown deadline passes, the health-check goroutine actually stopping after shutdown instead of leaking, a burst of requests past `-max-in-flight` getting 503s for the excess while the rest succeed, a connection attempted past `-max-connections` getting no response until an existing one closes and frees a slot, the metrics endpoint reporting accurate aggregate and per-backend request counts, a retry showing up in `relayforge_retries_total`, a rejection showing up in `relayforge_rejected_total`, a reload not disturbing a request already in flight, a reload preserving an unchanged backend's accumulated state, invalid reload configuration being rejected, `max_in_flight` taking effect immediately after a reload, and a shortened health-check interval taking effect immediately after a reload rather than waiting for the old interval's own schedule. `cmd/relayforge` has its own tests for config file loading and validation: a valid file resolves correctly, a missing file and malformed JSON are both rejected, and each individual validation rule (bad backend address, duplicate backend, bad strategy, bad or non-positive duration, negative counts) is rejected on its own.

A separate benchmark comparing round robin against least-connections under uneven backend cost, with real numbers instead of the qualitative demonstration above, belongs in milestone 11.
