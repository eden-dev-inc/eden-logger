# eden_logger_export

`eden_logger_export` installs a bounded, non-blocking `eden_logger` sink and
exports accepted records to any OTLP/HTTP protobuf endpoint, including the
OpenTelemetry Collector.

```toml
[dependencies]
eden_logger = { version = "0.1.2", features = ["sink", "log-info", "log-warn", "log-error", "log-internal"] }
eden_logger_export = "0.1.0"
```

```rust,no_run
use std::time::Duration;

use eden_logger::{EdenLog, LogAudience, LogContext, LogLevel};
use eden_logger_export::{ExporterConfig, install};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let exporter = install::<()>(
        ExporterConfig::new("http://localhost:4318", "payments")
            .with_resource_attribute("service.instance.id", "payments-1")
            .with_header("Authorization", "Bearer token"),
    )?;

    EdenLog::new(
        LogLevel::Info,
        "service started",
        &LogContext::empty(),
        LogAudience::Internal,
    )
    .emit();

    let report = exporter.shutdown(Duration::from_secs(5)).await;
    assert!(!report.timed_out);
    Ok(())
}
```

Each application thread resolves its typed Eden sink and producer lane once.
The steady-state callback performs filtering, bounded admission, and a push
into that thread's lane. One shared Tokio worker drains all registered lanes;
protobuf mapping, batching, compression, retries, TLS, and network I/O remain
off the application threads.

Use `LogTarget::StructuredSink` when OTLP is the only output. It skips creation
of a duplicate display/JSON line while retaining structured sink dispatch.

## Mapping

- Eden message → OTLP body.
- Eden timestamp → `time_unix_nano`; worker mapping time →
  `observed_time_unix_nano`.
- Trace/debug/info/warn/error → OTLP severity numbers 1/5/9/13/17.
- Valid hexadecimal trace/span IDs → native OTLP IDs; malformed values remain
  visible as Eden attributes.
- Audience, feature, function, source location, errors, request fields, and
  additional fields → OTLP attributes.
- `service.name` and configured identity fields → OTLP resource attributes.
- Scope name defaults to `eden_logger`.

Intrinsic Eden attributes win key collisions, followed by typed request fields,
then additional string fields. `EdenLogOtlpMapper::map_with_attributes` lets an
ordered intermediary add stable event, stream, epoch, or sequence attributes.

## Configuration and security

`ExporterConfig` exposes severity/audience filtering, priority threshold,
normal and reserved record capacities and retained-byte budgets, batch
count/bytes/delay, request timeout, retry bounds, resource identity, scope
name, and diagnostic interval. Its embedded
`OtlpHttpConfig` also supports a gzip threshold, custom headers, additional CA
bundles, and a PEM client certificate/private-key identity for mTLS.

Invalid endpoints, headers, CA bundles, identities, and batch/queue/retry
settings fail during installation before a sink is registered.

The direct exporter is intentionally best-effort:

- Normal capacity is 61,440 records, with 4,096 reserved slots available to
  warn/error records when the normal queue is full.
- Normal and reserved queues have independent 60 MiB and 4 MiB retained-byte
  budgets. Heap-owning `RequestFields` implementations should override
  `estimated_size_bytes` for accurate admission accounting.
- Batches flush at 512 records, 1 MiB encoded, or 200 ms.
- Transient failures retry with jittered exponential backoff capped at 30
  seconds.
- A lost response can cause duplicate delivery.
- Queue overflow, process termination, and shutdown deadlines can drop records.

Use shard-stream when logs require HA replication, bounded disk spooling, or
per-stream ordering before OTLP delivery.

## Health and metrics

`ExporterHandle::status` reports running, backing off, terminal failure,
shutting down, or stopped. `last_error` provides the latest diagnostic without
recursively entering Eden logging. `metrics_snapshot` exposes queue, drop,
attempt, retry, rejection, batch, inflight, lane, and lifecycle values.
`visit_metrics` emits the same snapshot through
`fast_telemetry::MetricVisitor`.

Authentication failures and permanent configuration/status failures stop
progress and set terminal health. Transport failures and HTTP 429/502/503/504
retry with jitter and honor `Retry-After`; other HTTP failures are terminal
except 400/413, which are bisected until invalid individual records are
isolated. OTLP partial-success responses are accounted once and never retried.

`force_flush(deadline)` covers every record accepted before the call without
stopping future logs. Acceptance tickets and a contiguous completion watermark
keep this exact even when reserved-priority records overtake normal records.

`reconfigure(config, deadline)` prepares a new client and worker, atomically
routes new records to it, and boundedly drains the prior generation. Use it to
rotate endpoints, authorization headers, CA bundles, or mTLS identities without
a logging outage. `shutdown(deadline)` stops acceptance, releases the typed
sink for later reinstallation, and attempts a bounded drain. Dropping the
handle also disables the sink and aborts the worker.

The eight-thread contention benchmark is in `benches/sink_enqueue.rs`; its
recorded before/after results are in `benches/THREAD_LOCAL_RESULTS.md`. Its
15-round direct-path comparison improved from 1.709 µs to 625 ns median p99 and
from 9.05M to 23.62M records/s while preserving the lifecycle and memory-safety
contract.
