use criterion::{Criterion, black_box, criterion_group, criterion_main};
use eden_logger::{LogAudience, LogContext, LogFormat, LogTarget, WriterConfig, ctx_with_trace, extract_trace_context, init};
use function_name::named;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;

static INIT_LOGGER_DISPLAY: Once = Once::new();
static INIT_LOGGER_JSON: Once = Once::new();

fn init_logger() {
    INIT_LOGGER_DISPLAY.call_once(|| {
        init(WriterConfig {
            target: LogTarget::Sink,
            format: LogFormat::Display,
            ..Default::default()
        });
    });
}

fn init_logger_json() {
    INIT_LOGGER_JSON.call_once(|| {
        init(WriterConfig {
            target: LogTarget::Sink,
            format: LogFormat::Json,
            ..Default::default()
        });
    });
}

/// Benchmark: Creating LogContext without trace extraction
fn bench_context_creation_basic(c: &mut Criterion) {
    c.bench_function("context_creation_basic", |b| {
        b.iter(|| {
            let ctx = LogContext::<()>::new().with_feature("test");
            black_box(ctx);
        });
    });
}

/// Benchmark: Creating LogContext with full data
fn bench_context_creation_full(c: &mut Criterion) {
    c.bench_function("context_creation_full", |b| {
        b.iter(|| {
            let ctx = LogContext::<()>::new().with_feature("test").with_additional("key1", "value1").with_additional("key2", "value2");
            black_box(ctx);
        });
    });
}

/// Benchmark: Trace context extraction (critical path)
#[allow(unused_macros)]
#[named]
fn bench_trace_extraction(c: &mut Criterion) {
    init_logger();
    let collector = Arc::new(fast_telemetry::SpanCollector::new(1, 1024));

    c.bench_function("trace_extraction", |b| {
        let mut span = collector.start_span("test_span", fast_telemetry::SpanKind::Internal);
        span.enter();

        b.iter(|| {
            let result = extract_trace_context();
            black_box(result);
        });
    });
}

/// Benchmark: ctx_with_trace! macro
#[allow(unused_macros)]
#[named]
fn bench_ctx_with_trace_macro(c: &mut Criterion) {
    init_logger();
    let collector = Arc::new(fast_telemetry::SpanCollector::new(1, 1024));

    c.bench_function("ctx_with_trace_macro", |b| {
        let mut span = collector.start_span("test_span", fast_telemetry::SpanKind::Internal);
        span.enter();

        b.iter(|| {
            let ctx = ctx_with_trace!();
            black_box(ctx);
        });
    });
}

/// Benchmark: Context cloning (happens frequently)
fn bench_context_clone(c: &mut Criterion) {
    let ctx = LogContext::<()>::new().with_feature("test");

    c.bench_function("context_clone", |b| {
        b.iter(|| {
            let cloned = ctx.clone();
            black_box(cloned);
        });
    });
}

/// Benchmark: Log creation (no emission)
fn bench_log_creation(c: &mut Criterion) {
    let ctx = LogContext::<()>::new().with_feature("test").with_trace_id("abc123def456").with_span_id("def456abc123");

    c.bench_function("log_creation", |b| {
        b.iter(|| {
            let log = eden_logger::EdenLog::<()>::new(eden_logger::LogLevel::Info, "Test message", &ctx, LogAudience::Internal);
            black_box(log);
        });
    });
}

/// Benchmark: Log creation with additional fields
fn bench_log_creation_with_additional(c: &mut Criterion) {
    let ctx = LogContext::<()>::new().with_feature("test").with_trace_id("abc123def456");

    c.bench_function("log_creation_with_additional", |b| {
        b.iter(|| {
            let log = eden_logger::EdenLog::<()>::new(eden_logger::LogLevel::Info, "Test message", &ctx, LogAudience::Internal)
                .with_additional("key1", "value1")
                .with_additional("key2", "value2")
                .with_additional("key3", "value3");
            black_box(log);
        });
    });
}

/// Benchmark: JSON serialization
fn bench_log_json_serialization(c: &mut Criterion) {
    let ctx = LogContext::<()>::new().with_feature("test").with_trace_id("abc123def456");

    let log = eden_logger::EdenLog::<()>::new(eden_logger::LogLevel::Info, "Test message", &ctx, LogAudience::Client);

    c.bench_function("log_json_serialization", |b| {
        b.iter(|| {
            let json = log.to_json();
            black_box(json);
        });
    });
}

/// Benchmark: Display formatting
fn bench_log_display_formatting(c: &mut Criterion) {
    let ctx = LogContext::<()>::new().with_feature("test").with_trace_id("abc123def456");

    let log = eden_logger::EdenLog::<()>::new(eden_logger::LogLevel::Info, "Test message", &ctx, LogAudience::Internal);

    c.bench_function("log_display_formatting", |b| {
        b.iter(|| {
            let display = log.to_display();
            black_box(display);
        });
    });
}

/// Benchmark: Complete log flow (create + format + emit)
fn bench_complete_log_flow(c: &mut Criterion) {
    init_logger();

    c.bench_function("complete_log_flow", |b| {
        b.iter(|| {
            let ctx = LogContext::<()>::new().with_feature("test");

            let log = eden_logger::EdenLog::<()>::new(eden_logger::LogLevel::Info, "Test message", &ctx, LogAudience::Internal);

            log.emit();
        });
    });
}

/// Benchmark: Zero-copy emit_direct (no EdenLog construction)
fn bench_emit_direct(c: &mut Criterion) {
    init_logger();
    let ctx = LogContext::<()>::new().with_feature("test");

    c.bench_function("emit_direct", |b| {
        b.iter(|| {
            eden_logger::emit_direct(eden_logger::LogLevel::Info, "Test message", black_box(&ctx), LogAudience::Internal, &[], None, None);
        });
    });
}

/// Benchmark: emit_direct with additional k-v pairs
fn bench_emit_direct_with_additional(c: &mut Criterion) {
    init_logger();
    let ctx = LogContext::<()>::new().with_feature("test").with_trace_id("abc123def456");

    c.bench_function("emit_direct_with_additional", |b| {
        b.iter(|| {
            eden_logger::emit_direct(
                eden_logger::LogLevel::Info,
                "Test message",
                black_box(&ctx),
                LogAudience::Internal,
                &[
                    ("key1", &"value1" as &dyn std::fmt::Display),
                    ("key2", &"value2" as &dyn std::fmt::Display),
                ],
                Some("src/services/auth.rs"),
                Some(42),
            );
        });
    });
}

/// Benchmark: Complete JSON log flow (create + format + emit)
fn bench_complete_json_log_flow(c: &mut Criterion) {
    init_logger_json();

    c.bench_function("complete_json_log_flow", |b| {
        b.iter(|| {
            let ctx = LogContext::<()>::new()
                .with_feature("test")
                .with_trace_id("4bf92f3577b34da6a3ce929d0e0e4736")
                .with_span_id("00f067aa0ba902b7");

            let log = eden_logger::EdenLog::<()>::new(eden_logger::LogLevel::Info, "Processing request", &ctx, LogAudience::Internal);

            log.emit();
        });
    });
}

/// Benchmark: JSON emit_direct path with additional k-v pairs
fn bench_emit_direct_json_with_additional(c: &mut Criterion) {
    init_logger_json();
    let ctx = LogContext::<()>::new()
        .with_feature("proxy")
        .with_trace_id("4bf92f3577b34da6a3ce929d0e0e4736")
        .with_span_id("00f067aa0ba902b7");

    c.bench_function("emit_direct_json_with_additional", |b| {
        b.iter(|| {
            eden_logger::emit_direct(
                eden_logger::LogLevel::Info,
                "Request processed",
                black_box(&ctx),
                LogAudience::Internal,
                &[
                    ("status", &200 as &dyn std::fmt::Display),
                    ("path", &"/api/v1/users" as &dyn std::fmt::Display),
                ],
                Some("src/api/handler.rs"),
                Some(42),
            );
        });
    });
}

/// Benchmark: Manual JSON formatting via write_json_direct
fn bench_json_format_direct(c: &mut Criterion) {
    let ctx = LogContext::<()>::new()
        .with_feature("test")
        .with_trace_id("4bf92f3577b34da6a3ce929d0e0e4736")
        .with_span_id("00f067aa0ba902b7");

    c.bench_function("json_format_direct", |b| {
        let mut buf = String::with_capacity(1024);
        b.iter(|| {
            buf.clear();
            eden_logger::schema::write_json_direct(
                &mut buf,
                eden_logger::LogLevel::Info,
                "Processing request for user authentication",
                black_box(&ctx),
                LogAudience::Internal,
                &[],
                None,
                None,
            );
            black_box(&buf);
        });
    });
}

/// Benchmark: Serde JSON serialization (for comparison)
fn bench_json_format_serde(c: &mut Criterion) {
    let ctx = LogContext::<()>::new()
        .with_feature("test")
        .with_trace_id("4bf92f3577b34da6a3ce929d0e0e4736")
        .with_span_id("00f067aa0ba902b7");

    let log = eden_logger::EdenLog::<()>::new(
        eden_logger::LogLevel::Info,
        "Processing request for user authentication",
        &ctx,
        LogAudience::Internal,
    );

    c.bench_function("json_format_serde", |b| {
        b.iter(|| {
            let json = log.to_json();
            black_box(json);
        });
    });
}

/// Benchmark: JSON format with additional fields
fn bench_json_format_with_additional(c: &mut Criterion) {
    let ctx = LogContext::<()>::new()
        .with_feature("proxy")
        .with_trace_id("4bf92f3577b34da6a3ce929d0e0e4736")
        .with_span_id("00f067aa0ba902b7")
        .with_additional("request_id", "req-12345")
        .with_additional("method", "POST");

    c.bench_function("json_format_with_additional", |b| {
        let mut buf = String::with_capacity(1024);
        b.iter(|| {
            buf.clear();
            eden_logger::schema::write_json_direct(
                &mut buf,
                eden_logger::LogLevel::Info,
                "Request processed",
                black_box(&ctx),
                LogAudience::Internal,
                &[
                    ("status", &200 as &dyn std::fmt::Display),
                    ("path", &"/api/v1/users" as &dyn std::fmt::Display),
                ],
                Some("src/api/handler.rs"),
                Some(42),
            );
            black_box(&buf);
        });
    });
}

/// Benchmark: JSON format with message requiring escaping
fn bench_json_format_with_escaping(c: &mut Criterion) {
    let ctx = LogContext::<()>::new().with_feature("test").with_trace_id("4bf92f3577b34da6a3ce929d0e0e4736");

    c.bench_function("json_format_with_escaping", |b| {
        let mut buf = String::with_capacity(1024);
        b.iter(|| {
            buf.clear();
            eden_logger::schema::write_json_direct(
                &mut buf,
                eden_logger::LogLevel::Error,
                "Error: \"file not found\"\nPath: C:\\Users\\test\\file.txt",
                black_box(&ctx),
                LogAudience::Internal,
                &[],
                None,
                None,
            );
            black_box(&buf);
        });
    });
}

/// Benchmark: Display format (for comparison with JSON)
fn bench_display_format_direct(c: &mut Criterion) {
    let ctx = LogContext::<()>::new()
        .with_feature("test")
        .with_trace_id("4bf92f3577b34da6a3ce929d0e0e4736")
        .with_span_id("00f067aa0ba902b7");

    let log = eden_logger::EdenLog::<()>::new(
        eden_logger::LogLevel::Info,
        "Processing request for user authentication",
        &ctx,
        LogAudience::Internal,
    );

    c.bench_function("display_format_direct", |b| {
        let mut buf = String::with_capacity(1024);
        b.iter(|| {
            buf.clear();
            log.write_display(&mut buf);
            black_box(&buf);
        });
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(1000);
    targets =
        bench_context_creation_basic,
        bench_context_creation_full,
        bench_trace_extraction,
        bench_ctx_with_trace_macro,
        bench_context_clone,
        bench_log_creation,
        bench_log_creation_with_additional,
        bench_log_json_serialization,
        bench_log_display_formatting,
        bench_complete_log_flow,
        bench_emit_direct,
        bench_emit_direct_with_additional,
        bench_complete_json_log_flow,
        bench_emit_direct_json_with_additional,
        // JSON vs Display comparison
        bench_json_format_direct,
        bench_json_format_serde,
        bench_json_format_with_additional,
        bench_json_format_with_escaping,
        bench_display_format_direct,
);

criterion_main!(benches);
