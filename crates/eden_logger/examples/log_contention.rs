//! Compare contention behavior of eden_logger vs env_logger under multi-thread load.
//!
//! Each worker thread emits log lines in a tight loop. Measures throughput under
//! varying thread counts and log complexity scenarios.
//!
//! Build and run directly:
//!   cargo run --release -p eden_logger --features full --example log_contention -- --mode eden --threads 8 --iters 1000000
//!   cargo run --release -p eden_logger --features full --example log_contention -- --mode env  --threads 8 --iters 1000000

use eden_logger::{LogAudience, LogContext, LogFormat, LogLevel, LogTarget, WriterConfig, emit_direct, init};
use std::sync::{Arc, Barrier};
use std::time::Instant;

// ============================================================================
// Config
// ============================================================================

#[derive(Copy, Clone)]
enum Mode {
    Eden,
    EdenJson,
    Env,
}

#[derive(Copy, Clone)]
enum Scenario {
    /// Minimal context: feature only.
    Minimal,
    /// Rich context: trace_id, span_id, feature, function, org, user, endpoint.
    Rich,
    /// Rich context + 3 additional key-value pairs.
    Additional,
}

struct Config {
    mode: Mode,
    scenario: Scenario,
    threads: usize,
    iters: usize,
}

// ============================================================================
// Args
// ============================================================================

fn parse_args() -> Config {
    let mut mode = Mode::Eden;
    let mut scenario = Scenario::Minimal;
    let mut threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    let mut iters = 1_000_000usize;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--mode" if i + 1 < args.len() => {
                mode = match args[i + 1].as_str() {
                    "eden" => Mode::Eden,
                    "eden-json" => Mode::EdenJson,
                    "env" => Mode::Env,
                    value => panic!("invalid --mode: {value} (expected eden|eden-json|env)"),
                };
                i += 2;
            }
            "--scenario" if i + 1 < args.len() => {
                scenario = match args[i + 1].as_str() {
                    "minimal" => Scenario::Minimal,
                    "rich" => Scenario::Rich,
                    "additional" => Scenario::Additional,
                    value => panic!("invalid --scenario: {value} (expected minimal|rich|additional)"),
                };
                i += 2;
            }
            "--threads" if i + 1 < args.len() => {
                threads = args[i + 1].parse().expect("--threads must be an integer");
                i += 2;
            }
            "--iters" if i + 1 < args.len() => {
                iters = args[i + 1].parse().expect("--iters must be an integer");
                i += 2;
            }
            "--help" => {
                println!(
                    "Usage: log_contention --mode <eden|eden-json|env> [--scenario <minimal|rich|additional>] --threads <n> --iters <n>"
                );
                std::process::exit(0);
            }
            arg => panic!("unknown arg: {arg}"),
        }
    }

    Config { mode, scenario, threads, iters }
}

// ============================================================================
// Threading harness (simplified — no exporter thread for logging)
// ============================================================================

fn run_with_threads<W>(threads: usize, iters: usize, worker: W) -> f64
where
    W: Fn(usize, usize) + Send + Sync + 'static,
{
    let barrier = Arc::new(Barrier::new(threads + 1));
    let worker = Arc::new(worker);
    let mut workers = Vec::with_capacity(threads);

    for t in 0..threads {
        let worker_fn = Arc::clone(&worker);
        let worker_barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            worker_barrier.wait();
            worker_fn(t, iters);
        }));
    }

    barrier.wait();
    let start = Instant::now();
    for w in workers {
        w.join().expect("worker thread panicked");
    }
    start.elapsed().as_secs_f64()
}

// ============================================================================
// eden_logger: emit_direct (zero-copy hot path)
// ============================================================================

fn run_eden(scenario: Scenario, threads: usize, iters: usize, format: LogFormat) -> f64 {
    init(WriterConfig { target: LogTarget::Sink, format, ..Default::default() });

    let ctx = Arc::new(match scenario {
        Scenario::Minimal => LogContext::<()>::new().with_feature("bench"),
        Scenario::Rich | Scenario::Additional => LogContext::<()>::new()
            .with_trace_id("4bf92f3577b34da6a3ce929d0e0e4736")
            .with_span_id("00f067aa0ba902b7")
            .with_feature("proxy")
            .with_function("handle_request"),
    });

    let use_additional = matches!(scenario, Scenario::Additional);

    let worker_ctx = Arc::clone(&ctx);

    run_with_threads(threads, iters, move |_t, n| {
        let ctx = &*worker_ctx;
        let additional: &[(&str, &dyn std::fmt::Display)] = if use_additional {
            &[("request_id", &"req-12345"), ("method", &"POST"), ("path", &"/api/v1/users")]
        } else {
            &[]
        };
        for _ in 0..n {
            emit_direct(
                LogLevel::Info,
                "Processing request successfully completed",
                ctx,
                LogAudience::Internal,
                additional,
                None,
                None,
            );
        }
    })
}

// ============================================================================
// env_logger: log::info! (baseline comparison)
// ============================================================================

fn run_env(scenario: Scenario, threads: usize, iters: usize) -> f64 {
    use std::io;
    let _ = env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .target(env_logger::Target::Pipe(Box::new(io::sink())))
        .try_init();

    // Pre-format the message to match what eden_logger would produce,
    // so the comparison measures formatting + write overhead fairly.
    let message: &'static str = match scenario {
        Scenario::Minimal => "[INTERNAL] feature=bench Processing request successfully completed",
        Scenario::Rich => {
            "[INTERNAL] trace_id=4bf92f3577b34da6a3ce929d0e0e4736 span_id=00f067aa0ba902b7 feature=proxy fn=handle_request org=org-660e8400-e29b-41d4-a716-446655440001 user=user-770e8400-e29b-41d4-a716-446655440002 endpoint=ep-880e8400-e29b-41d4-a716-446655440003 Processing request successfully completed"
        }
        Scenario::Additional => {
            "[INTERNAL] trace_id=4bf92f3577b34da6a3ce929d0e0e4736 span_id=00f067aa0ba902b7 feature=proxy fn=handle_request org=org-660e8400-e29b-41d4-a716-446655440001 user=user-770e8400-e29b-41d4-a716-446655440002 endpoint=ep-880e8400-e29b-41d4-a716-446655440003 request_id=req-12345 method=POST path=/api/v1/users Processing request successfully completed"
        }
    };

    run_with_threads(threads, iters, move |_t, n| {
        for _ in 0..n {
            log::info!("{}", message);
        }
    })
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    let cfg = parse_args();
    let total_ops = cfg.threads * cfg.iters;

    let elapsed = match cfg.mode {
        Mode::Eden => run_eden(cfg.scenario, cfg.threads, cfg.iters, LogFormat::Display),
        Mode::EdenJson => run_eden(cfg.scenario, cfg.threads, cfg.iters, LogFormat::Json),
        Mode::Env => run_env(cfg.scenario, cfg.threads, cfg.iters),
    };

    let record_ops_per_sec = (total_ops as f64) / elapsed;
    let mode = match cfg.mode {
        Mode::Eden => "eden",
        Mode::EdenJson => "eden-json",
        Mode::Env => "env",
    };
    let scenario = match cfg.scenario {
        Scenario::Minimal => "minimal",
        Scenario::Rich => "rich",
        Scenario::Additional => "additional",
    };

    println!("mode={mode}");
    println!("scenario={scenario}");
    println!("threads={}", cfg.threads);
    println!("iters_per_thread={}", cfg.iters);
    println!("total_ops={total_ops}");
    println!("record_seconds={elapsed:.6}");
    println!("total_seconds={elapsed:.6}");
    println!("record_ops_per_sec={record_ops_per_sec:.2}");
    println!("total_ops_per_sec={record_ops_per_sec:.2}");
    println!("ops_per_sec={record_ops_per_sec:.2}");
    // Compatibility with summarize_bench.py (no exporter for logging)
    println!("export_count=0");
    println!("export_seconds=0.000000");
    println!("export_avg_ms=0.000000");
}
