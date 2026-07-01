use criterion::{Criterion, black_box, criterion_group, criterion_main};
use eden_logger::{EdenLog, LogAudience, LogContext, LogLevel};

fn create_test_log() -> EdenLog<()> {
    let ctx = LogContext::<()>::new().with_feature("test").with_trace_id("abc123def456").with_function("my_function");

    EdenLog::<()>::new(LogLevel::Info, "This is a test message", &ctx, LogAudience::Internal).with_location("src/services/auth.rs", 42)
}

fn to_display_production(log: &EdenLog<()>) -> String {
    log.to_display()
}

fn to_display_std_fmt(log: &EdenLog<()>) -> String {
    use std::fmt::Write;

    let mut out = String::with_capacity(512);

    // Timestamp
    let ts = log.timestamp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

    let _ = write!(out, "[{}] [{}] [{}]", ts, log.level.as_colored_str(), log.audience.as_str());

    // Optional fields
    macro_rules! write_opt {
        ($label:literal, $opt:expr) => {
            if let Some(v) = &$opt {
                let _ = write!(out, " {}={}", $label, v);
            }
        };
    }

    write_opt!("trace_id", log.trace_id);
    write_opt!("feature", log.feature);
    write_opt!("fn", log.function);
    write_opt!("error", log.error_code);

    let _ = write!(out, " {}", log.message);

    // file:line on separate line (rustc style)
    if let Some(file) = &log.file {
        match log.line {
            Some(line) => {
                let _ = write!(out, "\n   --> {}:{}", file, line);
            }
            None => {
                let _ = write!(out, "\n   --> {}", file);
            }
        }
    }

    out
}

fn bench_display_formats(c: &mut Criterion) {
    let log = create_test_log();

    let mut group = c.benchmark_group("to_display");

    group.bench_function("production", |b| b.iter(|| to_display_production(black_box(&log))));

    group.bench_function("std_fmt_write", |b| b.iter(|| to_display_std_fmt(black_box(&log))));

    group.finish();
}

criterion_group!(benches, bench_display_formats);
criterion_main!(benches);
