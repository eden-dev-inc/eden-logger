# eden_logger

High-performance structured logging with compile-time tier gating and pluggable trace-context extraction.

`eden_logger` writes structured logs directly to stderr/stdout (or a custom sink) via a zero-allocation hot path. Trace and span IDs from `fast-telemetry` or `opentelemetry::Context` are picked up automatically when configured. The schema of "request" context (tenant, user, request id, whatever your application cares about) is **user-defined**: implement [`RequestFields`] on a struct of your own, and the logger threads it through every log without copies.

## Quick start

```rust
use eden_logger::{LogAudience, LogContext, log_info};

fn main() {
    let ctx = LogContext::<()>::new().with_feature("auth");
    log_info!(ctx, "Server started", audience = LogAudience::Internal);
}
```

`LogContext::<()>` says "no application-specific request fields." For most production setups you'll want to define your own; see [Application schema](#application-schema) below.

## Output format

Logs render in a colored, key-value display by default:

```
[2026-05-26T18:04:21.130Z] [INFO] [INTERNAL] trace_id=4bf92f35... feature=auth fn=login Server started
```

Switch to single-line JSON via [`WriterConfig::format`]:

```rust
use eden_logger::{LogFormat, LogTarget, WriterConfig, init};

init(WriterConfig {
    target: LogTarget::Stderr,
    format: LogFormat::Json,
    ..Default::default()
});
```

```json
{"ts":"2026-05-26T18:04:21.130Z","level":"INFO","audience":"INTERNAL","feature":"auth","fn":"login","msg":"Server started"}
```

## Compile-time tier gating

Every log has both a **level** (`trace`/`debug`/`info`/`warn`/`error`) and an **audience**: Internal, Client, or Both. Audiences let you separate logs that are safe to surface to API consumers from operator-only diagnostics. Both axes are gated by Cargo features:

| Feature           | Effect                                         |
|-------------------|------------------------------------------------|
| `log-trace`       | Compile `log_trace!` calls                     |
| `log-debug`       | Compile `log_debug!` calls                     |
| `log-info`        | Compile `log_info!` calls                      |
| `log-warn`        | Compile `log_warn!` calls                      |
| `log-error`       | Compile `log_error!` calls                     |
| `log-internal`    | Compile logs with `audience = Internal`        |
| `log-client`      | Compile logs with `audience = Client`          |
| `log-both`        | Compile logs with `audience = Both`            |
| `source-location` | Include `file!()` / `line!()` in output        |
| `function-name`   | `ctx_with_trace!()` captures the enclosing function name (requires `#[function_name::named]` on the calling function; see [Trace context](#trace-context)) |
| `serde`           | Enable serde derives on the log types, `EdenLog::to_json`, and the optional `install_sink` registry. Off by default. When off, `RequestFields` impls don't need `Serialize`/`Deserialize` bounds. |

A log macro call requires **both** the matching level feature and the matching audience feature. If either is off, the macro expands to `{}`: no code, no string literal in the binary. Build a release that physically cannot emit client-facing logs by leaving `log-client` off.

Two convenience profiles ship in `Cargo.toml`:

```toml
# All levels + both audiences + source location + function-name
eden_logger = { version = "0.1", features = ["full"] }

# info/warn/error, both audiences, fast-telemetry context + function-name
eden_logger = { version = "0.1", features = ["production"] }
```

## Runtime level filter

Independent of compile-time stripping, `EDEN_LOG_LEVEL` controls which compiled levels actually emit at runtime:

```bash
EDEN_LOG_LEVEL=info;warn ./my_app    # only info and warn
EDEN_LOG_LEVEL=error ./my_app         # only error
EDEN_LOG_LEVEL=none ./my_app          # silence everything
./my_app                              # unset = all compiled levels
```

The filter check is a single atomic-relaxed load (~1 ns).

## Application schema

A useful logger captures more than message and level: it needs to thread your application's identity model (tenant ID, user ID, request ID, endpoint…) through every log. `eden_logger` doesn't ship a fixed schema for this. Instead, [`LogContext<R>`] is generic over a [`RequestFields`] type that **you** define:

```rust
use eden_logger::{FieldWriter, LogAudience, LogContext, RequestFields, log_info};
use smol_str::SmolStr;

#[derive(Clone, Default)]
struct AppRequest {
    tenant_id: Option<SmolStr>,
    user_id: Option<SmolStr>,
}

impl RequestFields for AppRequest {
    fn write_display(&self, w: &mut dyn FieldWriter) {
        if let Some(v) = &self.tenant_id { w.write_str("tenant", v); }
        if let Some(v) = &self.user_id   { w.write_str("user",   v); }
    }
    fn write_json(&self, w: &mut dyn FieldWriter) {
        if let Some(v) = &self.tenant_id { w.write_str("tenant_id", v); }
        if let Some(v) = &self.user_id   { w.write_str("user_id",   v); }
    }
    fn merge(&mut self, other: Self) {
        if other.tenant_id.is_some() { self.tenant_id = other.tenant_id; }
        if other.user_id.is_some()   { self.user_id   = other.user_id; }
    }
}

fn handle(tenant: &str, user: &str) {
    let ctx = LogContext::<AppRequest>::new()
        .with_feature("api")
        .with_request(AppRequest {
            tenant_id: Some(tenant.into()),
            user_id:   Some(user.into()),
        });
    log_info!(ctx, "Request received", audience = LogAudience::Internal);
}
```

The display writer emits `tenant=...`; the JSON writer emits `"tenant_id":"..."`. Field keys and ordering are entirely under your control. Fields that are `None` are skipped at write time: no allocations, no empty strings in the output.

If you don't need this, `R = ()` is the default and writes nothing extra. `LogContext::<()>::new()` and `LogContext::new()` (when the context is unambiguous) both work.

## Trace context

When a `fast-telemetry` or OpenTelemetry span is active, `trace_id` and `span_id` are injected into every log emitted on that thread. Enable one (or both) sources via features:

```toml
eden_logger = { version = "0.1", features = ["fast-telemetry-context"] }
# or
eden_logger = { version = "0.1", features = ["otel-context"] }
```

Select the active source at startup:

```rust
use eden_logger::{TraceSource, set_trace_source};

set_trace_source(TraceSource::Otel);   // default is FastTelemetry
```

Either source can also be selected through [`WriterConfig::trace_source`] when calling [`init`].

Both are off by default; with neither enabled, `trace_id`/`span_id` simply stay `None`.

### `ctx_with_trace!()` and the `function-name` feature

`ctx_with_trace!()` is a convenience macro that builds a `LogContext` populated with the current trace/span IDs. When the `function-name` feature is enabled (it's on in both the `production` and `full` profiles), the macro **also** captures the enclosing function's name via the [`function_name`](https://crates.io/crates/function_name) crate.

Using it then requires the calling function to be annotated with `#[function_name::named]`:

```rust
use eden_logger::{ctx_with_trace, log_info, LogAudience};
use function_name::named;

#[named]                                // required when `function-name` is enabled
fn handle_request() {
    let ctx = ctx_with_trace!();        // captures trace_id, span_id, fn=handle_request
    log_info!(ctx, "Request received", audience = LogAudience::Internal);
}
```

If you'd rather not annotate every function, turn the feature off:

```toml
eden_logger = { version = "0.1", default-features = false, features = ["log-info", "log-internal"] }
```

With `function-name` disabled, `ctx_with_trace!()` still captures trace/span IDs but skips the function-name field, with no `#[named]` required.

## Macros

```rust
use eden_logger::{LogAudience, LogContext, log_error, log_info, log_warn};

let ctx = LogContext::<()>::new().with_feature("checkout");

// Bare log
log_info!(ctx.clone(), "Order placed", audience = LogAudience::Internal);

// With ad-hoc fields
log_warn!(
    ctx.clone(),
    "Slow query",
    audience = LogAudience::Internal,
    table = "orders",
    rows  = 142_000,
    ms    = 1834,
);

// Client-visible error
log_error!(ctx, "Payment declined", audience = LogAudience::Client);
```

The trailing `key = value` pairs are formatted via `Display`. They never allocate when the log is stripped at compile time.

## Custom sinks

Beyond stderr/stdout, you can register a non-blocking sink to ship logs to a collector, file, or analytics pipeline:

```rust
use eden_logger::{EdenLog, install_sink};

install_sink::<(), _>(|log: EdenLog<()>| {
    // forward to your transport
}).expect("sink already installed");
```

Sinks are keyed by the `RequestFields` type, so different `R` instantiations have independent slots.

## Performance

`eden_logger` is the **fastest Rust logger we measured** across every scenario in our benchmark suite. Disabled logs are stripped at compile time (0 ns, 0 bytes in the binary); enabled logs go through a hand-written, zero-allocation formatter and a single `write()` per line. No buffering layer, no async worker thread, no `format!` allocations.

The headline number: a `log_info!` call into a null sink, single thread, takes **~60 ns** end-to-end including timestamp generation and `Display` formatting.

### Against other Rust loggers

1000 sync `info!` calls into a null sink, single thread, release build:

| Logger        | Time (1000 calls) | Per call    | vs `eden_logger` |
|---------------|-------------------|-------------|------------------|
| **eden_logger**   | **60 µs**     | **60 ns**   | baseline      |
| `spdlog-rs`       | 82 µs         | 82 ns       | 1.4× slower   |
| `env_logger`      | 126 µs        | 126 ns      | 2.1× slower   |
| `fast_log`*       | 127 µs        | 127 ns      | 2.1× slower   |

\* `fast_log` is async: it sends each record through a crossbeam channel to a worker thread. The number above is the caller-thread cost (enqueue only); the worker's formatting + sink-write work is not included. We also measured the end-to-end cost via `flush()` and got the same number (~125 ns); for our no-op sink the channel-send dominates.

### Message length: nearly constant cost

eden_logger's display formatter just appends bytes to a thread-local buffer, so per-call cost is almost independent of the message itself. Other loggers go through `format!` and pay linearly per byte:

| Message size      | eden_logger   | spdlog-rs   | env_logger   | fast_log   |
|-------------------|---------------|-------------|--------------|------------|
| **10 B** (short)  | **52 ns**     | 93 ns       | 127 ns       | 125 ns     |
| **120 B** (medium)| **56 ns**     | 122 ns      | 140 ns       | 139 ns     |
| **1.2 KB** (long) | **60 ns**     | 160 ns      | 258 ns       | 256 ns     |

At 1.2 KB, eden_logger is **2.7× faster than spdlog-rs and 4.3× faster than env_logger / fast_log.**

### Dynamic formatting (3 runtime args per call)

The case where the macro has to format multiple values per log line:

| Logger        | Per call    | vs `eden_logger` |
|---------------|-------------|------------------|
| **eden_logger**   | **92 ns**   | baseline      |
| `spdlog-rs`       | 112 ns      | 1.2× slower   |
| `env_logger`      | 164 ns      | 1.8× slower   |
| `fast_log`        | 166 ns      | 1.8× slower   |

eden_logger uses `itoa` for integer formatting and a thread-local reusable buffer, which together beat `format!`-based pipelines and beat the channel-send cost of fast_log's "defer formatting to worker" design.

### Multi-thread contention

N threads each emit 100 calls into the same sink. Per-call cost rises with contention (the `stderr().lock()` cost grows), but eden_logger stays the fastest through 8 threads:

| Threads | eden_logger    | env_logger   | spdlog-rs   | fast_log   |
|---------|----------------|--------------|-------------|------------|
| 2       | **110 ns/log** | 156 ns       | 155 ns      | 156 ns     |
| 4       | **120 ns/log** | 145 ns       | 232 ns      | 145 ns     |
| 8       | **136 ns/log** | 143 ns       | 274 ns      | 146 ns     |

At 8 threads the gap to env_logger / fast_log closes to ~5%. spdlog-rs scales the worst of the four: its filtering + sink pipeline doesn't tolerate contention as well.

### Internal hot paths

For callers who want to see where the time is spent:

| Path                                                | Time     |
|-----------------------------------------------------|----------|
| `emit_direct` (zero-copy hot path, display format)  | 45 ns    |
| `emit_direct` + 2 k=v pairs + source location       | 64 ns    |
| `ctx_with_trace!()` (trace context extraction)      | 69 ns    |
| `write_display_direct` (format-only, no I/O)        | 29 ns    |
| `write_json_direct` (format-only, no I/O)           | 55 ns    |

Both display and JSON outputs use hand-written zero-allocation formatters. `serde_json::to_string` on the same record takes ~226 ns (**~4× slower**) and is never used on the hot path; it's only reachable via `EdenLog::to_json()` for callers that explicitly want it.

### Notes

- Disabled-by-feature logs: **0 ns**. They don't exist in the binary.
- The dyn-dispatch into `RequestFields::write_display` / `write_json` is inlined and erased by LLVM when `R = ()`. With a non-empty `R`, expect ~5–15 ns of additional cost per set field.
- All numbers are release-build criterion measurements with a 4 s sample window. Reproduce with `cargo bench -p eden_logger --bench logging_comparison --features production`.
- We have **not** benchmarked real I/O sinks (file, network). Async loggers like `fast_log` are designed to hide slow sink writes behind a worker thread; in that scenario eden_logger's caller pays the I/O cost directly and `fast_log` is likely to win. See [`benches/REPORT.md`](benches/REPORT.md) for the full methodology and what we didn't test.

There is no buffering layer between the formatter and the sink; bytes go in a single `write` per log line.

## Crate layout

| Module      | Purpose                                                            |
|-------------|--------------------------------------------------------------------|
| `context`   | `LogContext<R>`, `LogAudience`                                     |
| `fields`    | `RequestFields`, `FieldWriter`                                     |
| `schema`    | `EdenLog<R>`, `emit_direct`, display/JSON writers                  |
| `filter`    | Runtime level filter (`EDEN_LOG_LEVEL`)                            |
| `trace`     | Pluggable `TraceSource` (fast-telemetry / OpenTelemetry)           |
| `writer`    | Output target (stderr/stdout/sink), format selection               |
| `sink`      | Optional secondary sink for telemetry export                       |

## License

See repository root.
