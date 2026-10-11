# wsload

High-concurrency WebSocket load tester and broadcast fan-out latency benchmark. Built on Tokio's asynchronous epoll runtime to maintain tens of thousands of concurrent connections while capturing high-resolution latency distributions (min, p50, p90, p95, p99, p99.9, max).

## Why

Existing load testing tools are designed primarily for request-response HTTP traffic:
- **`httpload` / `wrk` / `autocannon`**: HTTP-only; they cannot maintain long-lived duplex WebSocket connections.
- **`k6`**: Requires a heavy JavaScript runtime per virtual user (high memory and CPU overhead at 10k+ connections).
- **`websocat` / `wscli`**: Excellent for single manual connections, but lack concurrent connection pools, connection ramp-up scheduling, and percentile latency distributions.
- **The Broadcast Fan-Out Gap**: In chat rooms, live auctions, or market data feeds, one publisher broadcasts to $N - 1$ subscribers. Existing tools cannot measure how long it takes for a message to fan out across all subscribers.

`wsload` provides a zero-overhead, compiled Rust CLI for all three real-time load testing scenarios.

---

## Installation

```sh
cargo install --path .
```

---

## Usage

### 1. Basic Echo Throughput & RTT Benchmark

Spawn 100 concurrent connections sending an echo message every 200ms for 10 seconds:

```sh
wsload ws://localhost:8080/ws -c 100 -d 10 -i 200
```

Sample output:
```text
==================== Benchmark Summary ====================
Target URL:             ws://localhost:8080/ws
Benchmark Mode:         Echo
Target Connections:     100
Test Duration:          10.02s
Handshake Success Rate: 100/100 (100.0%)
Messages Sent / Recv:   4850 / 4850
Message Throughput:     967.8 msgs/sec (0.42 MB/s)
Total Errors / Drops:   0

--- Connection Handshake Latency ---
  Min:        0.82 ms
  p50:        1.45 ms
  p90:        2.80 ms
  p99:        5.10 ms
  Max:        7.24 ms
  Mean:       1.62 ms (± 0.85 ms)

--- Message Round-Trip Latency (RTT) ---
  Count:  4850
  Min:        0.12 ms
  p50:        0.35 ms
  p75:        0.48 ms
  p90:        0.72 ms
  p95:        1.05 ms
  p99:        1.85 ms
  p99.9:      3.20 ms
  Max:        4.50 ms
  Mean:       0.41 ms (± 0.28 ms)
===========================================================
```

---

### 2. Protocol-Level Ping/Pong Heartbeat Benchmark

Benchmark pure protocol-level RFC 6455 Ping/Pong frame RTT with 500 concurrent connections:

```sh
wsload ws://localhost:8080/ws -m ping -c 500 -d 15
```

---

### 3. Broadcast Fan-Out Latency Benchmark

In fan-out mode, connection #0 acts as the **publisher**, broadcasting timestamped messages at `--interval-ms`. The remaining $N - 1$ connections act as **subscribers**, computing the microsecond delivery latency histogram:

```sh
# 1 publisher, 99 subscribers
wsload ws://localhost:8080/chat -m fanout -c 100 -i 500 -d 15
```

Reports delivery latency across all subscribers:
```text
--- Broadcast Fan-Out Delivery Latency ---
  Deliveries: 2970
  Min:        0.42 ms
  p50:        1.10 ms
  p90:        2.45 ms
  p99:        4.80 ms
  Max:        6.15 ms
  Mean:       1.28 ms (± 0.65 ms)
```

---

### 4. Controlled Ramp-Up

Gradually ramp up 1,000 connections over 5 seconds (200 connections/sec) to avoid overwhelming reverse proxies during connection storms:

```sh
wsload ws://localhost:8080/ws -c 1000 --ramp-ms 5000 -d 30
```

---

### 5. Automated CI Benchmarking (JSON Output)

Output structured JSON for regression tracking in CI:

```sh
wsload ws://localhost:8080/ws -c 50 -d 5 --json
```

---

## Status: built and verified against real WebSocket servers

- **4 unit tests** (`cargo test --lib`):
  - Exact percentile interpolation (min, p50, p75, p90, p95, p99, p99.9, max, mean, stddev) on empty, single, and 100-sample uniform distributions.
  - Microsecond timestamp extraction for fan-out calculation.

- **4 live integration tests** (`cargo test --test integration_test`):
  - **Echo mode**: Verified 8 concurrent clients against a real multi-connection WebSocket echo server; confirmed message counts, byte counters, and RTT calculations.
  - **Ping mode**: Verified RFC 6455 Ping/Pong round-trip matching by payload under concurrent load.
  - **Fan-out mode**: Verified 1 publisher and 5 subscribers against a real broadcasting pub/sub server; confirmed end-to-end fan-out delivery latency tracking.
  - **Connection refused handling**: Verified clean failure accounting without process panic when targeting unreachable endpoints.

---

## Deliberately Deferred

- **Distributed multi-node clustering**: Operates from a single host process (handles up to OS file descriptor limit `ulimit -n`).
- **Custom authentication challenge scripts**: OAuth/Bearer token query strings can be provided in the target URL directly; dynamic multi-step auth handshakes deferred.
