use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc as std_mpsc;
use std::thread;
use std::time::Duration;

use eden_logger::{EdenLog, FieldWriter, LogAudience, LogContext, LogLevel, RequestFields};
use eden_logger_export::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse, ExporterConfig, ExporterStatus, install,
};
use flate2::read::GzDecoder;
use prost::Message;

#[derive(Clone)]
struct Reply {
    status: u16,
    headers: Vec<(&'static str, &'static str)>,
    body: Vec<u8>,
    delay: Duration,
}

impl Reply {
    fn success() -> Self {
        Self {
            status: 200,
            headers: Vec::new(),
            body: ExportLogsServiceResponse::default().encode_to_vec(),
            delay: Duration::ZERO,
        }
    }

    fn partial(rejected: i64, message: &str) -> Self {
        Self {
            status: 200,
            headers: Vec::new(),
            body: ExportLogsServiceResponse {
                partial_success: Some(ExportLogsPartialSuccess {
                    rejected_log_records: rejected,
                    error_message: message.to_string(),
                }),
            }
            .encode_to_vec(),
            delay: Duration::ZERO,
        }
    }
}

struct CapturedRequest {
    head: String,
    body: Vec<u8>,
}

fn collector(replies: Vec<Reply>) -> (String, std_mpsc::Receiver<CapturedRequest>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test collector");
    let address = listener.local_addr().expect("collector address");
    let (captured_tx, captured_rx) = std_mpsc::channel();
    let task = thread::spawn(move || {
        for reply in replies {
            let (mut stream, _) = listener.accept().expect("accept OTLP request");
            let request = read_request(&mut stream);
            captured_tx.send(request).expect("capture request");
            write_reply(&mut stream, &reply);
        }
    });
    (format!("http://{address}"), captured_rx, task)
}

fn read_request(stream: &mut TcpStream) -> CapturedRequest {
    stream.set_read_timeout(Some(Duration::from_secs(5))).expect("set read timeout");
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut buffer).expect("read request");
        assert!(read > 0, "request closed before headers");
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let head = String::from_utf8(bytes[..header_end].to_vec()).expect("ASCII request head");
    let content_length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length").then(|| value.trim().parse::<usize>().expect("content length"))
        })
        .expect("content-length header");
    while bytes.len() < header_end + content_length {
        let read = stream.read(&mut buffer).expect("read request body");
        assert!(read > 0, "request closed before body");
        bytes.extend_from_slice(&buffer[..read]);
    }
    CapturedRequest {
        head,
        body: bytes[header_end..header_end + content_length].to_vec(),
    }
}

fn write_reply(stream: &mut TcpStream, reply: &Reply) {
    thread::sleep(reply.delay);
    let reason = match reply.status {
        200 => "OK",
        429 => "Too Many Requests",
        _ => "Error",
    };
    let mut head = format!(
        "HTTP/1.1 {} {reason}\r\nContent-Type: application/x-protobuf\r\nContent-Length: {}\r\nConnection: close\r\n",
        reply.status,
        reply.body.len()
    );
    for (name, value) in &reply.headers {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).expect("write response head");
    stream.write_all(&reply.body).expect("write response body");
}

fn decode_request_body(request: &CapturedRequest) -> Vec<u8> {
    if !request.head.to_ascii_lowercase().contains("content-encoding: gzip") {
        return request.body.clone();
    }
    let mut decoder = GzDecoder::new(request.body.as_slice());
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded).expect("decode gzip request");
    decoded
}

fn captured_message(request: &CapturedRequest) -> String {
    let body = decode_request_body(request);
    let decoded = ExportLogsServiceRequest::decode(body.as_slice()).expect("decode captured OTLP request");
    decoded.resource_logs[0].scope_logs[0].log_records[0]
        .body
        .as_ref()
        .and_then(|body| body.value.as_ref())
        .and_then(|value| match value {
            eden_logger_export::any_value::Value::StringValue(message) => Some(message.clone()),
            _ => None,
        })
        .expect("captured string log body")
}

async fn wait_for(mut predicate: impl FnMut() -> bool, message: &'static str) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect(message);
}

async fn wait_capture(receiver: &std_mpsc::Receiver<CapturedRequest>) -> CapturedRequest {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match receiver.try_recv() {
                Ok(request) => return request,
                Err(std_mpsc::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(std_mpsc::TryRecvError::Disconnected) => {
                    panic!("collector disconnected before capturing a request");
                }
            }
        }
    })
    .await
    .expect("request did not reach collector")
}

#[derive(Clone, Default)]
struct BasicFields;

impl RequestFields for BasicFields {
    fn write_display(&self, _: &mut dyn FieldWriter) {}
    fn write_json(&self, _: &mut dyn FieldWriter) {}
    fn merge(&mut self, _: Self) {}
}

#[tokio::test(flavor = "current_thread")]
async fn exports_an_acknowledged_otlp_request() {
    let (endpoint, captured, server) = collector(vec![Reply::success()]);
    let mut config = ExporterConfig::new(endpoint, "checkout")
        .with_resource_attribute("service.instance.id", "checkout-1")
        .with_header("x-tenant", "tenant-a")
        .with_batch_limits(1, 1024 * 1024, Duration::from_millis(10));
    config.http.gzip_threshold = 0;
    let exporter = install::<BasicFields>(config).expect("install exporter");

    EdenLog::new(LogLevel::Info, "ready", &LogContext::<BasicFields>::new(), LogAudience::Internal).emit();

    wait_for(|| exporter.metrics_snapshot().exported == 1, "record was not exported").await;
    let request = captured.recv_timeout(Duration::from_secs(1)).expect("captured request");
    assert!(request.head.starts_with("POST /v1/logs HTTP/1.1"));
    let request_head = request.head.to_ascii_lowercase();
    assert!(request_head.contains("x-tenant: tenant-a"));
    assert!(request_head.contains("content-encoding: gzip"));
    let request_body = decode_request_body(&request);
    let decoded = ExportLogsServiceRequest::decode(request_body.as_slice()).expect("decode OTLP request");
    let resource = decoded.resource_logs[0].resource.as_ref().expect("resource");
    assert!(resource.attributes.iter().any(|attribute| {
        attribute.key == "service.name"
            && attribute.value.as_ref().and_then(|value| value.value.as_ref()).is_some_and(|value| {
                matches!(
                    value,
                    eden_logger_export::any_value::Value::StringValue(name) if name == "checkout"
                )
            })
    }));
    assert_eq!(decoded.resource_logs[0].scope_logs[0].scope.as_ref().expect("scope").name, "eden_logger");
    assert_eq!(
        decoded.resource_logs[0].scope_logs[0].log_records[0].body.as_ref().and_then(|body| body.value.as_ref()),
        Some(&eden_logger_export::any_value::Value::StringValue("ready".to_string()))
    );

    let report = exporter.shutdown(Duration::from_secs(1)).await;
    assert!(!report.timed_out);
    server.join().expect("collector thread");
}

#[derive(Clone, Default)]
struct OversizeFields;

impl RequestFields for OversizeFields {
    fn write_display(&self, _: &mut dyn FieldWriter) {}
    fn write_json(&self, _: &mut dyn FieldWriter) {}
    fn merge(&mut self, _: Self) {}
}

#[tokio::test(flavor = "current_thread")]
async fn rejects_an_individually_oversized_encoded_request() {
    let config = ExporterConfig::new("http://127.0.0.1:9", "checkout").with_batch_limits(512, 128, Duration::from_millis(10));
    let exporter = install::<OversizeFields>(config).expect("install exporter");

    EdenLog::new(LogLevel::Info, "x".repeat(256), &LogContext::<OversizeFields>::new(), LogAudience::Internal).emit();

    wait_for(|| exporter.metrics_snapshot().dropped_oversize == 1, "oversized record was not rejected").await;
    assert_eq!(exporter.metrics_snapshot().export_attempts, 0);

    let report = exporter.shutdown(Duration::from_secs(1)).await;
    assert!(!report.timed_out);
}

#[derive(Clone, Default)]
struct PartialFields;

impl RequestFields for PartialFields {
    fn write_display(&self, _: &mut dyn FieldWriter) {}
    fn write_json(&self, _: &mut dyn FieldWriter) {}
    fn merge(&mut self, _: Self) {}
}

#[tokio::test(flavor = "current_thread")]
async fn bisects_partial_success_until_a_poison_record_is_isolated() {
    let (endpoint, _captured, server) = collector(vec![
        Reply::partial(1, "one invalid record"),
        Reply::success(),
        Reply::partial(1, "invalid record"),
    ]);
    let mut config = ExporterConfig::new(endpoint, "checkout").with_batch_limits(2, 1024 * 1024, Duration::from_millis(50));
    config.http.gzip_threshold = usize::MAX;
    let exporter = install::<PartialFields>(config).expect("install exporter");
    let context = LogContext::<PartialFields>::new();

    EdenLog::new(LogLevel::Info, "valid", &context, LogAudience::Internal).emit();
    EdenLog::new(LogLevel::Info, "invalid", &context, LogAudience::Internal).emit();

    wait_for(
        || {
            let metrics = exporter.metrics_snapshot();
            metrics.exported == 1 && metrics.rejected == 1
        },
        "partial rejection was not isolated",
    )
    .await;
    let metrics = exporter.metrics_snapshot();
    assert_eq!(metrics.partial_rejections, 2);
    assert_eq!(metrics.export_attempts, 3);

    let report = exporter.shutdown(Duration::from_secs(1)).await;
    assert!(!report.timed_out);
    server.join().expect("collector thread");
}

#[derive(Clone, Default)]
struct RetryFields;

impl RequestFields for RetryFields {
    fn write_display(&self, _: &mut dyn FieldWriter) {}
    fn write_json(&self, _: &mut dyn FieldWriter) {}
    fn merge(&mut self, _: Self) {}
}

#[tokio::test(flavor = "current_thread")]
async fn retries_throttling_and_honors_retry_after() {
    let (endpoint, _captured, server) = collector(vec![
        Reply {
            status: 429,
            headers: vec![("Retry-After", "0")],
            body: Vec::new(),
            delay: Duration::ZERO,
        },
        Reply::success(),
    ]);
    let mut config = ExporterConfig::new(endpoint, "checkout")
        .with_batch_limits(1, 1024 * 1024, Duration::from_millis(10))
        .with_retry(Duration::from_millis(5), Duration::from_millis(20));
    config.http.gzip_threshold = usize::MAX;
    let exporter = install::<RetryFields>(config).expect("install exporter");

    EdenLog::new(LogLevel::Warn, "throttled", &LogContext::<RetryFields>::new(), LogAudience::Internal).emit();

    wait_for(|| exporter.metrics_snapshot().exported == 1, "retry did not recover").await;
    assert_eq!(exporter.metrics_snapshot().retries, 1);

    let report = exporter.shutdown(Duration::from_secs(1)).await;
    assert!(!report.timed_out);
    server.join().expect("collector thread");
}

#[derive(Clone, Default)]
struct TerminalFields;

impl RequestFields for TerminalFields {
    fn write_display(&self, _: &mut dyn FieldWriter) {}
    fn write_json(&self, _: &mut dyn FieldWriter) {}
    fn merge(&mut self, _: Self) {}
}

#[tokio::test(flavor = "current_thread")]
async fn authentication_failure_sets_terminal_health() {
    let (endpoint, _captured, server) = collector(vec![Reply {
        status: 401,
        headers: Vec::new(),
        body: b"unauthorized".to_vec(),
        delay: Duration::ZERO,
    }]);
    let mut config = ExporterConfig::new(endpoint, "checkout").with_batch_limits(1, 1024 * 1024, Duration::from_millis(10));
    config.http.gzip_threshold = usize::MAX;
    let exporter = install::<TerminalFields>(config).expect("install exporter");

    EdenLog::new(LogLevel::Error, "authentication test", &LogContext::<TerminalFields>::new(), LogAudience::Internal).emit();

    wait_for(|| exporter.status() == ExporterStatus::TerminalFailure, "terminal state was not reported").await;
    assert_eq!(exporter.metrics_snapshot().status, ExporterStatus::TerminalFailure);
    assert!(exporter.last_error().is_some_and(|error| error.contains("401")));

    let report = exporter.shutdown(Duration::from_secs(1)).await;
    assert_eq!(report.remaining_records, 1);
    assert_eq!(report.metrics.dropped_shutdown, 1);
    server.join().expect("collector thread");
}

#[derive(Clone, Default)]
struct PressureFields;

impl RequestFields for PressureFields {
    fn write_display(&self, _: &mut dyn FieldWriter) {}
    fn write_json(&self, _: &mut dyn FieldWriter) {}
    fn merge(&mut self, _: Self) {}
}

#[tokio::test(flavor = "current_thread")]
async fn queue_pressure_preserves_reserved_warn_capacity() {
    let mut delayed = Reply::success();
    delayed.delay = Duration::from_millis(150);
    let (endpoint, captured, server) = collector(vec![delayed, Reply::success(), Reply::success()]);
    let mut config =
        ExporterConfig::new(endpoint, "checkout")
            .with_queue_capacities(1, 1)
            .with_batch_limits(1, 1024 * 1024, Duration::from_millis(10));
    config.http.gzip_threshold = usize::MAX;
    let exporter = install::<PressureFields>(config).expect("install exporter");
    let context = LogContext::<PressureFields>::new();

    EdenLog::new(LogLevel::Info, "in flight", &context, LogAudience::Internal).emit();
    let first = wait_capture(&captured).await;
    assert_eq!(captured_message(&first), "in flight");

    EdenLog::new(LogLevel::Info, "normal queue", &context, LogAudience::Internal).emit();
    EdenLog::new(LogLevel::Info, "dropped info", &context, LogAudience::Internal).emit();
    EdenLog::new(LogLevel::Warn, "reserved warning", &context, LogAudience::Internal).emit();

    let pressured = exporter.metrics_snapshot();
    assert_eq!(pressured.accepted, 3);
    assert_eq!(pressured.dropped_queue_full, 1);
    assert_eq!(pressured.normal_queue_depth, 1);
    assert_eq!(pressured.reserved_queue_depth, 1);

    wait_for(|| exporter.metrics_snapshot().exported == 3, "queued records did not drain").await;
    let second = captured.recv_timeout(Duration::from_secs(1)).expect("second captured request");
    let third = captured.recv_timeout(Duration::from_secs(1)).expect("third captured request");
    assert_eq!(captured_message(&second), "reserved warning");
    assert_eq!(captured_message(&third), "normal queue");
    let report = exporter.shutdown(Duration::from_secs(1)).await;
    assert!(!report.timed_out);
    server.join().expect("collector thread");
}
