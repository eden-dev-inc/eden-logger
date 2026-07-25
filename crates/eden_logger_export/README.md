# eden_logger_export

`eden_logger_export` installs a bounded, non-blocking `eden_logger` sink and
exports accepted records to any OTLP/HTTP protobuf endpoint, including the
OpenTelemetry Collector.

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

The direct exporter is intentionally best-effort:

- Normal capacity is 61,440 records, with 4,096 reserved slots available to
  warn/error records when the normal queue is full.
- Batches flush at 512 records, 1 MiB encoded, or 200 ms.
- Transient failures retry with jittered exponential backoff capped at 30
  seconds.
- A lost response can cause duplicate delivery.
- Queue overflow, process termination, and shutdown deadlines can drop records.

Use shard-stream when logs require HA replication, bounded disk spooling, or
per-stream ordering before OTLP delivery.

The eight-thread contention benchmark is in `benches/sink_enqueue.rs`; its
recorded before/after results are in `benches/THREAD_LOCAL_RESULTS.md`.
