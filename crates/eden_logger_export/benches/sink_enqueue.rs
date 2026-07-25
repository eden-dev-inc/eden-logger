use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use eden_logger::{
    EdenLog, FieldWriter, LogAudience, LogContext, LogFormat, LogLevel, LogTarget, RequestFields, TraceSource, WriterConfig, emit_direct,
};
use eden_logger_export::{ExporterConfig, install_on};

const THREADS: usize = 8;
const RECORDS_PER_THREAD: usize = 20_000;
const P99_GATE: Duration = Duration::from_micros(10);

struct Measurement {
    p50: Duration,
    p99: Duration,
    elapsed: Duration,
}

#[derive(Clone, Default)]
struct BenchFields;

impl RequestFields for BenchFields {
    fn write_display(&self, _: &mut dyn FieldWriter) {}
    fn write_json(&self, _: &mut dyn FieldWriter) {}
    fn merge(&mut self, _: Self) {}
}

fn main() {
    let rounds = std::env::var("EDEN_EXPORT_BENCH_ROUNDS").ok().and_then(|value| value.parse().ok()).unwrap_or(1);
    let mode = std::env::var("EDEN_EXPORT_BENCH_MODE").unwrap_or_else(|_| "all".to_string());
    let profile = std::env::var_os("EDEN_EXPORT_BENCH_PROFILE").is_some();
    let (endpoint, stop_collector, collector) = start_collector();
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().expect("build benchmark runtime");
    let mut config = ExporterConfig::new(endpoint, "eden-logger-export-bench")
        .with_queue_capacities(THREADS * RECORDS_PER_THREAD * 2, 4_096)
        .with_batch_limits(512, 1024 * 1024, Duration::from_millis(5));
    config.http.gzip_threshold = usize::MAX;
    let exporter = install_on::<BenchFields>(runtime.handle(), config).expect("install benchmark exporter");

    if mode == "all" || mode == "formatted" {
        run_mode("formatted + thread-local", LogTarget::Sink, false, rounds, profile, &exporter);
    }
    if mode == "all" || mode == "structured" {
        run_mode("structured-only + thread-local", LogTarget::StructuredSink, false, rounds, profile, &exporter);
    }
    if mode == "all" || mode == "direct" {
        run_mode(
            "emit_direct structured-only + thread-local",
            LogTarget::StructuredSink,
            true,
            rounds,
            profile,
            &exporter,
        );
    }
    assert!(
        matches!(mode.as_str(), "all" | "formatted" | "structured" | "direct"),
        "EDEN_EXPORT_BENCH_MODE must be all, formatted, structured, or direct"
    );

    let metrics = exporter.metrics_snapshot();
    assert_eq!(metrics.dropped_queue_full, 0, "benchmark queue filled before measuring accepted enqueue latency");

    let _ = runtime.block_on(exporter.shutdown(Duration::from_secs(5)));
    stop_collector.store(true, Ordering::Release);
    let _ = TcpStream::connect(collector.0);
    collector.1.join().expect("collector thread");
}

fn run_mode(name: &str, target: LogTarget, direct: bool, rounds: usize, profile: bool, exporter: &eden_logger_export::ExporterHandle) {
    assert!(rounds > 0, "EDEN_EXPORT_BENCH_ROUNDS must be greater than zero");
    let mut measurement = None;
    for _ in 0..rounds {
        measurement = Some(measure(target, direct, profile));
        wait_for_drain(exporter);
    }
    let measurement = measurement.expect("at least one benchmark round");
    if profile {
        println!(
            "eden_logger_export {name}: profile throughput={:.0} records/s ({rounds} round(s))",
            throughput(&measurement)
        );
        return;
    }
    println!(
        "eden_logger_export {name}: p50={} ns p99={} ns throughput={:.0} records/s ({rounds} round(s))",
        measurement.p50.as_nanos(),
        measurement.p99.as_nanos(),
        throughput(&measurement)
    );
    assert!(
        measurement.p99 < P99_GATE,
        "{name} p99 {} ns exceeded {} ns",
        measurement.p99.as_nanos(),
        P99_GATE.as_nanos()
    );
}

fn measure(target: LogTarget, direct: bool, profile: bool) -> Measurement {
    eden_logger::init(WriterConfig {
        target,
        format: LogFormat::Display,
        trace_source: TraceSource::FastTelemetry,
    });

    let barrier = Arc::new(Barrier::new(THREADS + 1));
    let mut workers = Vec::with_capacity(THREADS);
    for index in 0..THREADS {
        let barrier = Arc::clone(&barrier);
        workers.push(
            thread::Builder::new()
                .name(format!("eden-log-producer-{index}"))
                .spawn(move || {
                    let context = LogContext::<BenchFields>::new();
                    let mut samples = (!profile).then(|| Vec::with_capacity(RECORDS_PER_THREAD));
                    barrier.wait();
                    for _ in 0..RECORDS_PER_THREAD {
                        let started = (!profile).then(Instant::now);
                        if direct {
                            emit_direct(LogLevel::Info, "benchmark record", &context, LogAudience::Internal, &[], None, None);
                        } else {
                            EdenLog::new(LogLevel::Info, "benchmark record", &context, LogAudience::Internal).emit();
                        }
                        if let (Some(samples), Some(started)) = (&mut samples, started) {
                            samples.push(started.elapsed());
                        }
                    }
                    samples.unwrap_or_default()
                })
                .expect("spawn benchmark producer"),
        );
    }

    let started = Instant::now();
    barrier.wait();
    let mut samples = Vec::with_capacity(THREADS * RECORDS_PER_THREAD);
    for worker in workers {
        samples.extend(worker.join().expect("benchmark worker"));
    }
    let elapsed = started.elapsed();
    let (p50, p99) = if profile {
        (Duration::ZERO, Duration::ZERO)
    } else {
        samples.sort_unstable();
        (samples[samples.len() / 2], samples[samples.len() * 99 / 100])
    };
    Measurement { p50, p99, elapsed }
}

fn throughput(measurement: &Measurement) -> f64 {
    (THREADS * RECORDS_PER_THREAD) as f64 / measurement.elapsed.as_secs_f64()
}

fn wait_for_drain(exporter: &eden_logger_export::ExporterHandle) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let metrics = exporter.metrics_snapshot();
        if metrics.normal_queue_depth == 0 && metrics.reserved_queue_depth == 0 && metrics.inflight == 0 && metrics.producer_lanes == 0 {
            return;
        }
        assert!(Instant::now() < deadline, "exporter did not drain benchmark records");
        thread::yield_now();
    }
}

fn start_collector() -> (String, Arc<AtomicBool>, (std::net::SocketAddr, thread::JoinHandle<()>)) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind benchmark collector");
    let address = listener.local_addr().expect("collector address");
    let stop = Arc::new(AtomicBool::new(false));
    let worker_stop = Arc::clone(&stop);
    let worker = thread::spawn(move || {
        while !worker_stop.load(Ordering::Acquire) {
            let Ok((mut stream, _)) = listener.accept() else {
                continue;
            };
            if worker_stop.load(Ordering::Acquire) {
                break;
            }
            read_request(&mut stream);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .expect("write collector response");
        }
    });
    (format!("http://{address}"), stop, (address, worker))
}

fn read_request(stream: &mut TcpStream) {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut buffer).expect("read request");
        if read == 0 {
            return;
        }
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let head = String::from_utf8_lossy(&bytes[..header_end]);
    let content_length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length").then(|| value.trim().parse::<usize>().ok()).flatten()
        })
        .unwrap_or_default();
    while bytes.len() < header_end + content_length {
        let read = stream.read(&mut buffer).expect("read request body");
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
}
