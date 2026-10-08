# Failure experiments

A proxy's real job isn't forwarding traffic when everything behind it is healthy, it's behaving predictably when a backend dies, a client vanishes, or load exceeds capacity. This document is the evidence for that: six deliberate ways relayforge was broken on purpose, run against real processes, not mocks, with the actual numbers from those runs.

## 1. Kill a backend process mid-request

**Setup.** Two backends configured, one deliberately held unresponsive (never answers), the other healthy. A GET request round-robins to the unresponsive one first.

**Result.** The request waited out the configured response timeout (2s), was transparently retried against the healthy backend, and succeeded in about 2.06s total, invisible to the client as anything other than a slow response:

```
curl against [slow(never responds), healthy], response-timeout=2s
http_code=200 time=2.058392s
```

Separately, pointing both backends at a live process and killing one (`taskkill`) while health checks ran every 500ms: round-robin converged to routing 100% of traffic to the surviving backend within about 2 seconds, with zero failed client requests observed during the transition.

**Why this matters.** Two independent mechanisms cover this, at different layers. Retry (milestone 5) catches it on the very next request after a backend dies, before any health check has even run, as long as the failed request is retry-eligible. Health checks (milestone 3) catch it on their own schedule and stop sending traffic there at all, so later requests don't pay the cost of discovering it's dead each time. Neither one alone is the whole story; the benchmarks section below has a precise, request-by-request breakdown of exactly how small the retry-covered gap actually is.

## 2. Kill the proxy process itself while requests are in flight

**Setup.** A request in flight against a 3-second-slow backend, timed to kill the proxy process exactly 0.5 seconds in, using a real forceful kill (`taskkill /F`, no graceful path possible), all within one atomic script so the timing is real rather than an artifact of separate tool calls racing each other.

**Result.**

```
killing relayforge now, 0.5s into a 3s-slow request
http_code=000 exit=56 time=0.843961s
```

`exit=56` is curl's code for a clean receive error. No hang, no truncated data, no silently-returned partial response, just a clean failure well under a second after the kill.

**Why this matters.** A forceful kill gives the process no chance to do anything, the client's failure here comes entirely from the OS tearing down the TCP connection once the process is gone, not from anything relayforge itself does. That's the right outcome: a client should never be left waiting indefinitely or receiving corrupted data just because the thing on the other end disappeared. (Contrast this with a *graceful* stop of the same process: an in-flight request against the same kind of slow backend completed correctly and the process exited cleanly afterward, see milestone 7's notes for that comparison.)

## 3. Make a backend artificially slow and confirm retries don't cascade

**Setup.** A single backend configured with a 2-second response timeout, made to hang forever. Since there's only one backend, a retry-eligible failure has nowhere to go but back to the same backend.

**Result.**

```
retrying request after backend 127.0.0.1:19101 failed: i/o timeout
request to backend 127.0.0.1:19101 failed: i/o timeout
```

Total time before giving up: about 4 seconds, exactly two response-timeout windows (the original attempt plus the one retry), not an unbounded retry storm. With a second, healthy backend configured instead, the same scenario resolves in one response-timeout window (about 2s) since the retry actually has somewhere useful to go.

**Why this matters.** The retry budget is exactly one attempt, by design (see milestone 5). A single slow backend costs a client double the configured timeout in the worst case, not an ever-growing pile of retries against something already known to be struggling. The project's Known Limitations call this out explicitly: it is a real cost of having only one configured backend, and it is a bounded one.

## 4. Send malformed or partial HTTP directly at the proxy

**Setup.** A raw TCP connection to the proxy, followed by bytes that are not a valid HTTP request line at all (`this is not an HTTP request\r\n\r\n`), no backend ever involved.

**Result.** The connection closes cleanly (`io.EOF` on the client's next read), before any backend is ever dialed. Covered by an automated test (`TestMalformedRequestClosesConnectionCleanly`) that runs on every `go test ./...`, not just a one-off manual check.

**Why this matters.** `http.ReadRequest` returning an error is exactly the signal the proxy needs to reject the connection outright rather than hang, retry, or panic trying to make sense of garbage. This is the proxy's first line of defense against anything that isn't actually HTTP arriving on the wire.

## 5. Push more concurrent connections than the configured backpressure limit

**Setup.** `-max-in-flight 3` against a 500ms-slow backend, 10 concurrent requests fired at once.

**Result.**

```
200 200 200 503 503 503 503 503 503 503
```

Exactly 3 succeeded (matching the configured limit), the other 7 got an immediate, explicit `503 Service Unavailable` rather than queueing or hanging. Separately, `-max-connections 5` against the same slow backend with 30 concurrent requests: all 30 eventually completed successfully in about 3 seconds (30 ÷ 5 × 500ms, exactly as expected), the process stayed healthy throughout, no crash, no resource exhaustion, they simply queued through the connection cap rather than overwhelming anything.

**Why this matters.** This is the difference between "fails predictably" and "falls over." A proxy that's past capacity should say so immediately and explicitly (the 503s), or queue bounded work safely (the connection cap backed by the OS's own listen backlog), not accept unbounded work it cannot actually do and let memory or file descriptors be the thing that eventually gives out.

## 6. Disconnect a client while the proxy is waiting on a slow backend

**Setup.** A client sends a request, the proxy forwards it to a backend that never responds, then the client disconnects mid-wait. An automated test (`TestClientDisconnectCancelsSlowBackendWork`) captures the goroutine count immediately before the request and again after the disconnect is handled.

**Result.** The backend-side connection is torn down immediately on disconnect (confirmed by the backend's own read returning an error well before its own 3-second deadline), and the goroutine count returns to its pre-request baseline, run 10 consecutive times with no leak.

**Why this matters.** This is the one that actually proves cancellation propagation works, not just that it compiles. Without it, a client that gives up mid-request would leave its backend-side work (and the goroutine and connection tied to it) running to completion for absolutely no one, for as long as that backend took to respond, which in the worst case is however long `ResponseTimeout` allows. `context.Context`-style cancellation earning its keep here is the actual point of milestone 5's design, not a side effect of it.

## What this demonstrates

Every result above came from a real process, a real backend, and a real client, not a mock standing in for one. "The proxy handles failures" is a claim. "I killed a backend and watched the retry mask it in one request while the health check caught up in two seconds; I forcefully killed the proxy itself mid-request and got a clean error in under a second; I pushed 10 requests through a 3-slot limit and got exactly 7 explicit rejections; I disconnected mid-wait and watched the goroutine count return to baseline ten times in a row" is evidence.
