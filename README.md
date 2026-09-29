# RelayForge

TCP reverse proxy and load balancer in Go, built from scratch to actually understand connection handling, HTTP framing, load balancing, connection pooling, and backpressure rather than configuring nginx/HAProxy/Traefik and trusting the magic.

Part of a personal [engineering lab](../../checklist.md) of systems-depth projects, following [VellumDB](../vellumdb).

## What's here

- **Milestone 0: raw TCP forwarding**: accept a client connection, dial one fixed backend, copy bytes bidirectionally until either side hangs up. No HTTP awareness yet, this just proves the base plumbing (accept loop, bidirectional copy, half-close propagation) works before anything smarter is layered on.

## Usage

```
go build -o relayforge ./cmd/relayforge
./relayforge -listen 127.0.0.1:8080 -backend 127.0.0.1:9090
```

Every connection to `-listen` is forwarded to `-backend`. There is no routing or load balancing yet, one proxy instance talks to exactly one backend.

## Known limitations

- Single fixed backend, no config file, no multiple backends, no load balancing yet (milestones 2 onward).
- No HTTP awareness: this forwards raw bytes, so persistent HTTP/1.1 connections work but there is no request boundary to hang retries, per-request routing, or metrics on (milestone 1).
- `go test -race` could not be run in this environment: no C compiler is installed, and the race detector requires cgo. Tests were run and pass under plain `go test ./...`; race detection should be run wherever a C toolchain is available before trusting concurrency-sensitive milestones (connection pooling, health state, retry counters) at face value.

## Testing

```
go test ./...
```

Covers: data flowing correctly in both directions, the client observing EOF when the backend closes first, the backend observing EOF when the client closes first, and the client connection being closed cleanly when the backend dial fails.
