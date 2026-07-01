# eden_logger benchmark report

Comparative throughput data for `eden_logger` against three other Rust loggers across four scenarios.

## Setup

- **Loggers**: `eden_logger` (this crate), `spdlog-rs` 0.4, `env_logger`, `fast_log` 1.7
- **Sink**: `io::sink()` for all loggers (no disk I/O)
- **eden_logger config**: `LogTarget::Sink`, `LogFormat::Display`, `R = ()`
- **Build**: release, `--features production`, criterion 0.5
- **Sample size**: 20, measurement time 4s/bench
- **Iterations per sample**: 1000 log calls (scenarios 1, 3, 4) or 100 × N threads (scenario 2)

All numbers below are total wall-clock time for the full inner loop; the per-call number divides that by the call count. eden_logger is configured to write to the in-memory sink; the same is true for env_logger and spdlog-rs. fast_log is configured with a custom `LogAppender` that discards records, so we measure dispatch cost without I/O.

### About `fast_log`

`fast_log` is the only async logger in the comparison. `log::info!` enqueues a record to a crossbeam channel and a worker thread does the formatting + sink write. Two variants are reported where relevant:

- **enqueue only**: caller-thread cost. The worker thread's work is not in the measurement. If your code doesn't need logs to be durable before continuing, this is the latency you pay.
- **end-to-end via flush**: emit 1000 records, then `log::logger().flush()` and block until the worker drains. Comparable to the synchronous loggers because the bench thread waits for completion.

---

## Scenario 1: single thread, static message

1000 calls of `log_info!(ctx, "test log", audience = LogAudience::Internal)` (or each logger's equivalent).

| Logger | Total (1000 calls) | Per call | vs eden_logger |
|---|---|---|---|
| **eden_logger** | **60 µs** | **60 ns** | — |
| spdlog-rs | 82 µs | 82 ns | 1.37× slower |
| env_logger | 126 µs | 126 ns | 2.10× slower |
| fast_log (enqueue) | 127 µs | 127 ns | 2.13× slower |
| fast_log (flush) | 125 µs | 125 ns | 2.09× slower |

**Observation.** The simplest possible case. eden_logger's display formatter and direct sink write are the fastest path. fast_log's channel-send cost matches env_logger's full sync path — its async design doesn't help when the sink is a no-op.

---

## Scenario 2: multi-thread contention

N threads each emit 100 log lines to the shared sink. The sink is `io::sink()`, so we're measuring caller-side locking + dispatch, not I/O.

| Threads | eden_logger | env_logger | spdlog-rs | fast_log (async) |
|---|---|---|---|---|
| **2** | **22 µs** | 31 µs | 31 µs | 31 µs |
| **4** | **48 µs** | 58 µs | 93 µs | 58 µs |
| **8** | **109 µs** | 114 µs | 219 µs | 117 µs |

Per-log (total / total calls):

| Threads | eden_logger | env_logger | spdlog-rs | fast_log |
|---|---|---|---|---|
| 2 (200 logs)  | **110 ns** | 156 ns | 155 ns | 156 ns |
| 4 (400 logs)  | **120 ns** | 145 ns | 232 ns | 145 ns |
| 8 (800 logs)  | **136 ns** | 143 ns | 274 ns | 146 ns |

**Observations.**

- eden_logger stays fastest across the range. The cost grows from 60 ns/call (1 thread) to 136 ns/call (8 threads) — that's `stderr().lock()` contention, since the sink is `Mutex<dyn Write>` underneath.
- env_logger and fast_log scale similarly to each other: both are ~150 ns/call regardless of thread count.
- **spdlog-rs scales the worst.** From 82 ns (1 thread) to 274 ns (8 threads). The deeper formatting + filtering pipeline doesn't tolerate contention as well.
- The gap between eden_logger and fast_log narrows from 2.1× (1 thread) to 1.07× (8 threads). At 16+ threads we'd expect them to converge or invert.

---

## Scenario 3: caller cost with dynamic formatting

Each call formats three runtime-computed arguments (`request_id`, `latency_ns`, `path`).

| Logger | Total | Per call | vs eden_logger |
|---|---|---|---|
| **eden_logger** | **92 µs** | **92 ns** | — |
| spdlog-rs | 112 µs | 112 ns | 1.22× slower |
| env_logger | 164 µs | 164 ns | 1.78× slower |
| fast_log (enqueue) | 166 µs | 166 ns | 1.80× slower |
| fast_log (flush) | 165 µs | 165 ns | 1.79× slower |

**Observation.** This scenario was supposed to favor fast_log — it can defer the `Display::fmt` work to the worker thread. In practice, the cost of capturing the arguments + sending through the channel exceeds eden_logger's full inline format. eden_logger uses `itoa`-based integer formatting and a thread-local reusable buffer, both of which contribute.

---

## Scenario 4: varying message length

Each row is 1000 calls of `log::info!("{}", msg)` (or equivalent), where `msg` is a static `&str` literal.

### Short message (~10 bytes: `"request ok"`)

| Logger | Total | Per call |
|---|---|---|
| **eden_logger** | **52 µs** | **52 ns** |
| spdlog-rs | 93 µs | 93 ns |
| env_logger | 127 µs | 127 ns |
| fast_log (async) | 125 µs | 125 ns |
| fast_log (flush) | 126 µs | 126 ns |

### Medium message (~120 bytes: structured log line)

| Logger | Total | Per call |
|---|---|---|
| **eden_logger** | **56 µs** | **56 ns** |
| spdlog-rs | 122 µs | 122 ns |
| env_logger | 140 µs | 140 ns |
| fast_log (async) | 139 µs | 139 ns |
| fast_log (flush) | 142 µs | 142 ns |

### Long message (~1.2 KB: SQL query + plan)

| Logger | Total | Per call |
|---|---|---|
| **eden_logger** | **60 µs** | **60 ns** |
| spdlog-rs | 160 µs | 160 ns |
| env_logger | 258 µs | 258 ns |
| fast_log (async) | 256 µs | 256 ns |
| fast_log (flush) | 257 µs | 257 ns |

**Observation.** eden_logger's cost is **almost flat** across the 100× size range — 52 ns → 60 ns going from 10 bytes to 1.2 KB. The display writer just copies bytes into the thread-local buffer with `push_str`, and the sink write is a no-op, so message length costs near-zero memcpy.

env_logger and fast_log both scale **linearly** with message size — they call into formatting machinery (`format!`, channel capture) that has per-byte cost.

spdlog-rs sits in the middle — its formatter is heavier than eden_logger's but lighter than env_logger's.

---

## Summary across scenarios

| Scenario | eden_logger | next-fastest | gap |
|---|---|---|---|
| Single-thread, simple message | **60 ns** | spdlog-rs 82 ns | 1.37× |
| 2-thread contention | **110 ns/log** | env_logger / spdlog / fast_log ~155 ns | 1.4× |
| 4-thread contention | **120 ns/log** | env_logger / fast_log 145 ns | 1.2× |
| 8-thread contention | **136 ns/log** | env_logger 143 ns | 1.05× |
| Dynamic formatting (3 args) | **92 ns** | spdlog-rs 112 ns | 1.22× |
| Short message (10 B) | **52 ns** | spdlog-rs 93 ns | 1.79× |
| Medium message (120 B) | **56 ns** | spdlog-rs 122 ns | 2.18× |
| Long message (1.2 KB) | **60 ns** | spdlog-rs 160 ns | 2.67× |

**eden_logger is fastest in every scenario we measured.** The gap is largest for long messages (2.7×) and smallest under heavy thread contention (1.05×, where `stderr().lock()` overhead dominates the call cost).

---

## What we have NOT measured

These benchmarks deliberately use a no-op sink to isolate dispatch cost. Several real-world axes are not covered:

1. **Slow sinks (file / network).** Async loggers (fast_log) are designed for cases where the sink is the bottleneck. With `io::sink()` the worker thread has nothing slow to hide. If your sink takes 5 µs per write, fast_log's caller still pays ~127 ns; eden_logger's caller would pay 5 µs. **fast_log is expected to win here, and we have not benchmarked it.**

2. **Bursty workloads.** Async logging absorbs bursts into the channel. Our benches are steady-state.

3. **Many fields (>3 dynamic args).** Scenario 3 measures only three args. eden_logger's per-arg cost is constant (push to vec); other loggers may scale differently.

4. **>8 threads.** The trend at 2/4/8 suggests the gap continues to narrow. At 16+ threads with a contended sink, eden_logger may converge with or lose to async loggers.

5. **Cold-start / first-call latency.** All numbers above are steady-state warmed-up criterion measurements.

---

## Reproducing

```bash
cargo bench -p eden_logger --bench logging_comparison --features production
```

To run a specific scenario:

```bash
# Single-thread
cargo bench -p eden_logger --bench logging_comparison --features production -- integrated_comparison

# Message length only
cargo bench -p eden_logger --bench logging_comparison --features production -- msg_length

# Multi-thread only
cargo bench -p eden_logger --bench logging_comparison --features production -- multi_thread

# Dynamic formatting only
cargo bench -p eden_logger --bench logging_comparison --features production -- caller_cost
```

Source: [`benches/logging_comparison.rs`](logging_comparison.rs).
