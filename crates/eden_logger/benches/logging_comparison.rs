//! Throughput comparison: eden_logger vs other Rust loggers across several
//! scenarios. Three groups of benches:
//!
//! 1. `integrated_comparison` — single thread, 1000 sync calls, null sink.
//!    Measures formatting + dispatch overhead in isolation.
//! 2. `multi_thread_contention` — N threads each emit logs to the same sink.
//!    Surfaces caller-side locking cost as thread count grows.
//! 3. `caller_cost_with_formatting` — heavy `format_args!`-style work per
//!    call. Surfaces the cost of doing formatting on the caller thread
//!    (eden_logger, spdlog-rs, env_logger) vs deferring to a worker (fast_log).
//!
//! Important caveat: `fast_log` is async — `log::info!` enqueues to a
//! crossbeam channel and a worker thread does the actual formatting + write.
//! Two fast_log variants are reported where relevant:
//!   - "(enqueue only)" — caller-thread cost only; the worker's work is not
//!     included. This is what users see if they only care about call-site
//!     latency and don't need logs to be durable before continuing.
//!   - "(end-to-end via flush)" — emits 1000 records then `log::logger().flush()`,
//!     blocking until the worker drains. Comparable to the synchronous loggers.

use criterion::{Criterion, criterion_group, criterion_main};
use eden_logger::{LogAudience, LogTarget, WriterConfig, ctx_with_trace, init, log_info};
use function_name::named;
use std::sync::{Arc, Barrier, Once};

static INIT_EDEN_LOGGER: Once = Once::new();
static INIT_ENV_LOGGER: Once = Once::new();
static INIT_SPDLOG: Once = Once::new();
static INIT_FAST_LOG: Once = Once::new();

fn init_eden_logger() {
    INIT_EDEN_LOGGER.call_once(|| {
        init(WriterConfig { target: LogTarget::Sink, ..Default::default() });
    });
}

fn init_env_logger() {
    INIT_ENV_LOGGER.call_once(|| {
        use std::io;
        let _ = env_logger::Builder::from_default_env().target(env_logger::Target::Pipe(Box::new(io::sink()))).try_init();
    });
}

fn init_spdlog() {
    INIT_SPDLOG.call_once(|| {
        use spdlog::Logger;
        use spdlog::sink::WriteSink;
        use std::io;
        use std::sync::Arc;

        let sink = Arc::new(WriteSink::builder().target(io::sink()).build().expect("spdlog WriteSink"));
        let logger = Arc::new(Logger::builder().sink(sink).build().expect("spdlog Logger"));
        spdlog::set_default_logger(logger);
    });
}

fn init_fast_log() {
    INIT_FAST_LOG.call_once(|| {
        use fast_log::Config;
        use fast_log::appender::{FastLogRecord, LogAppender};

        struct NullAppender;
        impl LogAppender for NullAppender {
            fn do_logs(&mut self, _records: &[FastLogRecord]) {
                // discard
            }
        }

        let _ = fast_log::init(Config::new().level(log::LevelFilter::Info).custom(NullAppender));
    });
}

// ---------------------------------------------------------------------------
// Scenario 1: single thread, 1000 calls (the original bench)
// ---------------------------------------------------------------------------

#[named]
fn eden_logger_integrated() {
    init_eden_logger();
    let ctx = ctx_with_trace!();
    for _i in 0..1000 {
        log_info!(ctx.clone(), "test log", audience = LogAudience::Internal);
    }
}

fn env_logger_bench() {
    init_env_logger();
    for _i in 0..1000 {
        log::info!("[INTERNAL] fn=env_logger test log");
    }
}

fn spdlog_bench() {
    init_spdlog();
    for _i in 0..1000 {
        spdlog::info!("[INTERNAL] fn=spdlog test log");
    }
}

fn fast_log_bench() {
    init_fast_log();
    for _i in 0..1000 {
        log::info!("[INTERNAL] fn=fast_log test log");
    }
}

/// fast_log measured end-to-end: emit 1000 records, then `flush()` and wait
/// for the worker thread to drain the channel. Comparable to sync loggers.
fn fast_log_bench_sync() {
    init_fast_log();
    for _i in 0..1000 {
        log::info!("[INTERNAL] fn=fast_log test log");
    }
    log::logger().flush();
}

// ---------------------------------------------------------------------------
// Scenario 2: multi-thread contention
//
// N threads each emit `iters` log lines into the same sink. Caller-side
// locking dominates the cost. Reported as "wall-clock time to complete all
// threads."
//
// Note for fast_log: the per-thread cost is just enqueue, but the channel
// itself is contended. We measure enqueue-only here because measuring
// end-to-end through `flush()` mid-bench creates noisy results when many
// threads call flush concurrently.
// ---------------------------------------------------------------------------

fn run_threaded<F>(threads: usize, iters: usize, work: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let work = Arc::new(work);
    let barrier = Arc::new(Barrier::new(threads));
    let mut handles = Vec::with_capacity(threads);
    for _ in 0..threads {
        let work = Arc::clone(&work);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            for _ in 0..iters {
                work();
            }
        }));
    }
    for h in handles {
        h.join().expect("worker panic");
    }
}

fn eden_logger_threaded(threads: usize) {
    init_eden_logger();
    run_threaded(threads, 100, || {
        let ctx = eden_logger::LogContext::<()>::new().with_feature("bench");
        log_info!(ctx, "test log", audience = LogAudience::Internal);
    });
}

fn env_logger_threaded(threads: usize) {
    init_env_logger();
    run_threaded(threads, 100, || {
        log::info!("[INTERNAL] fn=env_logger test log");
    });
}

fn spdlog_threaded(threads: usize) {
    init_spdlog();
    run_threaded(threads, 100, || {
        spdlog::info!("[INTERNAL] fn=spdlog test log");
    });
}

fn fast_log_threaded(threads: usize) {
    init_fast_log();
    run_threaded(threads, 100, || {
        log::info!("[INTERNAL] fn=fast_log test log");
    });
}

// ---------------------------------------------------------------------------
// Scenario 3: caller-side cost with heavy formatting
//
// Each call formats several arguments via the standard `format_args!` path.
// eden_logger, spdlog-rs, and env_logger format on the caller thread.
// fast_log defers the actual `Display::fmt` work to the worker — the caller
// only pays for argument capture + channel enqueue.
// ---------------------------------------------------------------------------

#[inline(never)]
fn dynamic_args(i: u64) -> (u64, u64, &'static str) {
    (i, i.wrapping_mul(1_000_003), "/api/v1/orders")
}

#[named]
fn eden_logger_with_formatting() {
    init_eden_logger();
    let ctx = ctx_with_trace!();
    for i in 0..1000 {
        let (req_id, latency_ns, path) = dynamic_args(i);
        log_info!(
            ctx.clone(),
            "request handled",
            audience = LogAudience::Internal,
            request_id = req_id,
            latency_ns = latency_ns,
            path = path
        );
    }
}

fn env_logger_with_formatting() {
    init_env_logger();
    for i in 0..1000 {
        let (req_id, latency_ns, path) = dynamic_args(i);
        log::info!("[INTERNAL] request handled request_id={} latency_ns={} path={}", req_id, latency_ns, path);
    }
}

fn spdlog_with_formatting() {
    init_spdlog();
    for i in 0..1000 {
        let (req_id, latency_ns, path) = dynamic_args(i);
        spdlog::info!("[INTERNAL] request handled request_id={} latency_ns={} path={}", req_id, latency_ns, path);
    }
}

fn fast_log_with_formatting() {
    init_fast_log();
    for i in 0..1000 {
        let (req_id, latency_ns, path) = dynamic_args(i);
        log::info!("[INTERNAL] request handled request_id={} latency_ns={} path={}", req_id, latency_ns, path);
    }
}

fn fast_log_with_formatting_sync() {
    init_fast_log();
    for i in 0..1000 {
        let (req_id, latency_ns, path) = dynamic_args(i);
        log::info!("[INTERNAL] request handled request_id={} latency_ns={} path={}", req_id, latency_ns, path);
    }
    log::logger().flush();
}

// ---------------------------------------------------------------------------
// Scenario 4: varying message length
//
// Tests how each logger's cost scales with message size. Short = ~12 bytes,
// medium = ~120 bytes, long = ~1.2 KB. All static literals — no formatting
// cost on top.
// ---------------------------------------------------------------------------

const MSG_SHORT: &str = "request ok";
const MSG_MEDIUM: &str = "GET /api/v1/users/abc-123/profile -> 200 OK in 4.2ms (cache=miss, db=hit, region=us-east-1, replica=primary)";
// ~1.2 KB
const MSG_LONG: &str = "SELECT u.id, u.email, u.created_at, u.organization_uuid, o.name, o.tier, COUNT(s.id) AS session_count, MAX(s.last_active_at) AS last_seen FROM users u INNER JOIN organizations o ON u.organization_uuid = o.uuid LEFT JOIN sessions s ON s.user_id = u.id WHERE u.created_at > '2025-01-01' AND o.tier IN ('pro', 'enterprise') AND u.deleted_at IS NULL GROUP BY u.id, u.email, u.created_at, u.organization_uuid, o.name, o.tier HAVING COUNT(s.id) > 3 ORDER BY last_seen DESC LIMIT 100 -- executed in 12.7ms, rows returned: 87, plan: index_scan on users_org_created_idx, cost estimate: 4521.34, buffer cache hit ratio: 0.94, parallel workers: 2";

#[named]
fn eden_logger_with_msg(msg: &str) {
    init_eden_logger();
    let ctx = ctx_with_trace!();
    for _i in 0..1000 {
        log_info!(ctx.clone(), msg, audience = LogAudience::Internal);
    }
}

fn env_logger_with_msg(msg: &str) {
    init_env_logger();
    for _i in 0..1000 {
        log::info!("{}", msg);
    }
}

fn spdlog_with_msg(msg: &str) {
    init_spdlog();
    for _i in 0..1000 {
        spdlog::info!("{}", msg);
    }
}

fn fast_log_with_msg(msg: &str) {
    init_fast_log();
    for _i in 0..1000 {
        log::info!("{}", msg);
    }
}

fn fast_log_with_msg_sync(msg: &str) {
    init_fast_log();
    for _i in 0..1000 {
        log::info!("{}", msg);
    }
    log::logger().flush();
}

fn criterion_benchmark(c: &mut Criterion) {
    // Scenario 1: single-thread, 1000 calls
    {
        let mut group = c.benchmark_group("integrated_comparison");
        group.bench_function("eden_logger", |b| b.iter(eden_logger_integrated));
        group.bench_function("env_logger", |b| b.iter(env_logger_bench));
        group.bench_function("spdlog_rs", |b| b.iter(spdlog_bench));
        group.bench_function("fast_log (async, enqueue only)", |b| b.iter(fast_log_bench));
        group.bench_function("fast_log (end-to-end via flush)", |b| b.iter(fast_log_bench_sync));
        group.finish();
    }

    // Scenario 2: multi-thread contention. 100 calls × N threads.
    for &threads in &[2usize, 4, 8] {
        let mut group = c.benchmark_group(format!("multi_thread_{threads}_threads"));
        group.bench_function("eden_logger", |b| b.iter(|| eden_logger_threaded(threads)));
        group.bench_function("env_logger", |b| b.iter(|| env_logger_threaded(threads)));
        group.bench_function("spdlog_rs", |b| b.iter(|| spdlog_threaded(threads)));
        group.bench_function("fast_log (async)", |b| b.iter(|| fast_log_threaded(threads)));
        group.finish();
    }

    // Scenario 3: dynamic-formatting caller cost
    {
        let mut group = c.benchmark_group("caller_cost_with_formatting");
        group.bench_function("eden_logger", |b| b.iter(eden_logger_with_formatting));
        group.bench_function("env_logger", |b| b.iter(env_logger_with_formatting));
        group.bench_function("spdlog_rs", |b| b.iter(spdlog_with_formatting));
        group.bench_function("fast_log (async, enqueue only)", |b| b.iter(fast_log_with_formatting));
        group.bench_function("fast_log (end-to-end via flush)", |b| b.iter(fast_log_with_formatting_sync));
        group.finish();
    }

    // Scenario 4: varying message length
    for (label, msg) in &[("short_10b", MSG_SHORT), ("medium_120b", MSG_MEDIUM), ("long_1.2kb", MSG_LONG)] {
        let mut group = c.benchmark_group(format!("msg_length_{label}"));
        group.bench_function("eden_logger", |b| b.iter(|| eden_logger_with_msg(msg)));
        group.bench_function("env_logger", |b| b.iter(|| env_logger_with_msg(msg)));
        group.bench_function("spdlog_rs", |b| b.iter(|| spdlog_with_msg(msg)));
        group.bench_function("fast_log (async)", |b| b.iter(|| fast_log_with_msg(msg)));
        group.bench_function("fast_log (flush)", |b| b.iter(|| fast_log_with_msg_sync(msg)));
        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
