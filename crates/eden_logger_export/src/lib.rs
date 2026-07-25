#![cfg_attr(test, allow(clippy::unwrap_used))]
//! Bounded, best-effort OTLP log export for [`eden_logger`].
//!
//! The installed sink resolves a producer lane once per thread, then performs
//! filtering, bounded admission, and an uncontended lane push. Mapping,
//! protobuf encoding, retries, and network I/O run on one shared Tokio worker.
//! Applications that require crash durability or strict ordering should put
//! shard-stream between the sink and the collector.

mod collector;
mod mapper;
mod metrics;
mod worker;

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use eden_logger::{EdenLog, LogAudience, LogLevel, RequestFields};
use fast_telemetry::otlp::build_resource;
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use collector::{CollectorControl, LogCollector, SubmitResult};
pub use fast_telemetry::otlp::pb::{
    AnyValue, ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse, KeyValue, LogRecord, ResourceLogs, ScopeLogs,
    SeverityNumber, any_value,
};
pub use fast_telemetry_export::otlp::{OtlpExportOutcome, OtlpHttpClient, OtlpHttpConfig, OtlpHttpError, OtlpHttpErrorKind, OtlpTlsConfig};
pub use mapper::EdenLogOtlpMapper;
use metrics::ExporterMetrics;
pub use metrics::{ExporterMetricsSnapshot, visit_exporter_metrics};
pub use worker::ShutdownReport;
use worker::run_worker;

const DEFAULT_NORMAL_CAPACITY: usize = 61_440;
const DEFAULT_RESERVED_CAPACITY: usize = 4_096;

/// Audience selection applied independently from eden_logger's runtime filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AudienceFilter {
    /// Accept records whose audience is internal.
    pub internal: bool,
    /// Accept records whose audience is client-facing.
    pub client: bool,
    /// Accept records explicitly marked for both audiences.
    pub both: bool,
}

impl AudienceFilter {
    pub const ALL: Self = Self { internal: true, client: true, both: true };

    pub const INTERNAL_ONLY: Self = Self { internal: true, client: false, both: false };

    const fn allows(self, audience: LogAudience) -> bool {
        match audience {
            LogAudience::Internal => self.internal,
            LogAudience::Client => self.client,
            LogAudience::Both => self.both,
        }
    }
}

impl Default for AudienceFilter {
    fn default() -> Self {
        Self::ALL
    }
}

/// Direct exporter configuration.
#[derive(Clone)]
pub struct ExporterConfig {
    /// Shared OTLP/HTTP endpoint, timeout, headers, compression, and TLS options.
    pub http: OtlpHttpConfig,
    /// OTLP `service.name` resource attribute.
    pub service_name: String,
    /// OTLP instrumentation scope name.
    pub scope_name: String,
    /// Additional OTLP resource attributes.
    pub resource_attributes: Vec<(String, String)>,
    /// Lowest severity accepted by the exporter.
    pub min_level: LogLevel,
    /// Accepted Eden audiences.
    pub audiences: AudienceFilter,
    /// Severity that may fall back to reserved queue capacity.
    pub priority_threshold: LogLevel,
    /// Global capacity available to all accepted severities.
    pub normal_queue_capacity: usize,
    /// Additional global capacity available at or above `priority_threshold`.
    pub reserved_queue_capacity: usize,
    /// Maximum records in one OTLP request.
    pub max_batch_records: usize,
    /// Maximum encoded bytes in one OTLP request.
    pub max_batch_bytes: usize,
    /// Maximum time to wait before flushing a non-empty batch.
    pub max_batch_delay: Duration,
    /// Initial transient retry delay before jitter.
    pub retry_initial: Duration,
    /// Maximum transient retry delay before jitter.
    pub retry_max: Duration,
    /// Minimum interval between aggregate emergency stderr diagnostics.
    pub diagnostic_interval: Duration,
}

impl ExporterConfig {
    /// Create a configuration using production defaults.
    pub fn new(endpoint: impl Into<String>, service_name: impl Into<String>) -> Self {
        Self {
            http: OtlpHttpConfig::new(endpoint),
            service_name: service_name.into(),
            scope_name: "eden_logger".to_string(),
            resource_attributes: Vec::new(),
            min_level: LogLevel::Trace,
            audiences: AudienceFilter::ALL,
            priority_threshold: LogLevel::Warn,
            normal_queue_capacity: DEFAULT_NORMAL_CAPACITY,
            reserved_queue_capacity: DEFAULT_RESERVED_CAPACITY,
            max_batch_records: 512,
            max_batch_bytes: 1024 * 1024,
            max_batch_delay: Duration::from_millis(200),
            retry_initial: Duration::from_millis(250),
            retry_max: Duration::from_secs(30),
            diagnostic_interval: Duration::from_secs(60),
        }
    }

    /// Override the OTLP instrumentation scope name.
    pub fn with_scope_name(mut self, scope_name: impl Into<String>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    /// Add one OTLP resource attribute.
    pub fn with_resource_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.resource_attributes.push((key.into(), value.into()));
        self
    }

    /// Add one HTTP header to every request.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.http = self.http.with_header(name, value);
        self
    }

    /// Set the per-request timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.http = self.http.with_timeout(timeout);
        self
    }

    /// Add a PEM-encoded CA certificate bundle.
    pub fn with_ca_certificate_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.http = self.http.with_ca_certificate_pem(pem);
        self
    }

    /// Set a PEM client certificate chain and private key for mTLS.
    pub fn with_client_identity_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.http = self.http.with_client_identity_pem(pem);
        self
    }

    /// Set the lowest exported severity.
    pub fn with_min_level(mut self, min_level: LogLevel) -> Self {
        self.min_level = min_level;
        self
    }

    /// Select the Eden audiences accepted by this exporter.
    pub fn with_audiences(mut self, audiences: AudienceFilter) -> Self {
        self.audiences = audiences;
        self
    }

    /// Set normal and reserved global queue capacities.
    pub fn with_queue_capacities(mut self, normal: usize, reserved: usize) -> Self {
        self.normal_queue_capacity = normal;
        self.reserved_queue_capacity = reserved;
        self
    }

    /// Set batch record, encoded-byte, and maximum-delay limits.
    pub fn with_batch_limits(mut self, records: usize, bytes: usize, delay: Duration) -> Self {
        self.max_batch_records = records;
        self.max_batch_bytes = bytes;
        self.max_batch_delay = delay;
        self
    }

    /// Set initial and maximum transient retry delays.
    pub fn with_retry(mut self, initial: Duration, maximum: Duration) -> Self {
        self.retry_initial = initial;
        self.retry_max = maximum;
        self
    }

    fn validate(&self) -> Result<(), InstallError> {
        if self.service_name.trim().is_empty() {
            return Err(InstallError::InvalidConfig("service_name must not be empty".to_string()));
        }
        if self.scope_name.trim().is_empty() {
            return Err(InstallError::InvalidConfig("scope_name must not be empty".to_string()));
        }
        if self.normal_queue_capacity == 0 || self.reserved_queue_capacity == 0 {
            return Err(InstallError::InvalidConfig("queue capacities must be greater than zero".to_string()));
        }
        if self.max_batch_records == 0 || self.max_batch_bytes == 0 || self.max_batch_delay.is_zero() {
            return Err(InstallError::InvalidConfig("batch limits and delay must be greater than zero".to_string()));
        }
        if self.retry_initial.is_zero() || self.retry_max.is_zero() || self.retry_initial > self.retry_max {
            return Err(InstallError::InvalidConfig(
                "retry durations must be non-zero and initial must not exceed maximum".to_string(),
            ));
        }
        if self.diagnostic_interval.is_zero() {
            return Err(InstallError::InvalidConfig("diagnostic_interval must be greater than zero".to_string()));
        }
        Ok(())
    }
}

/// Current worker lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum ExporterStatus {
    #[default]
    /// Accepting and exporting records normally.
    Running = 0,
    /// Waiting before a transient retry.
    BackingOff = 1,
    /// Stopped on a permanent transport or configuration failure.
    TerminalFailure = 2,
    /// No longer accepting records and attempting a bounded drain.
    ShuttingDown = 3,
    /// Worker execution has ended.
    Stopped = 4,
}

impl ExporterStatus {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::BackingOff,
            2 => Self::TerminalFailure,
            3 => Self::ShuttingDown,
            4 => Self::Stopped,
            _ => Self::Running,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::BackingOff => "backing_off",
            Self::TerminalFailure => "terminal_failure",
            Self::ShuttingDown => "shutting_down",
            Self::Stopped => "stopped",
        }
    }
}

/// Installation failure before a sink is registered.
#[derive(Debug)]
pub enum InstallError {
    /// Installation was attempted outside a Tokio runtime.
    NoTokioRuntime,
    /// Queue, batch, identity, or retry configuration is invalid.
    InvalidConfig(String),
    /// The OTLP HTTP client rejected its endpoint, headers, or TLS material.
    Transport(OtlpHttpError),
    /// A typed Eden sink was already installed for this `RequestFields` type.
    SinkAlreadyInstalled,
}

impl fmt::Display for InstallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoTokioRuntime => formatter.write_str("no Tokio runtime is active"),
            Self::InvalidConfig(message) => write!(formatter, "invalid exporter config: {message}"),
            Self::Transport(error) => write!(formatter, "failed to create OTLP client: {error}"),
            Self::SinkAlreadyInstalled => formatter.write_str("an eden_logger sink is already installed for this RequestFields type"),
        }
    }
}

impl std::error::Error for InstallError {}

impl From<OtlpHttpError> for InstallError {
    fn from(error: OtlpHttpError) -> Self {
        Self::Transport(error)
    }
}

pub(crate) struct Shared {
    pub accepting: AtomicBool,
    pub metrics: ExporterMetrics,
    pub shutdown: Notify,
    pub records: Notify,
    pub diagnostic_interval_millis: u64,
    pub last_diagnostic_millis: AtomicU64,
    pub last_error: Mutex<Option<String>>,
}

impl Shared {
    fn set_status(&self, status: ExporterStatus) {
        self.metrics.status.store(status as u8, Ordering::Release);
    }

    fn emergency(&self, message: &str) {
        let now = unix_millis();
        let previous = self.last_diagnostic_millis.load(Ordering::Relaxed);
        if now.saturating_sub(previous) < self.diagnostic_interval_millis {
            return;
        }
        if self.last_diagnostic_millis.compare_exchange(previous, now, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
            eprintln!("[eden_logger_export emergency] {message}");
        }
    }
}

/// Handle for health, metrics, and bounded shutdown.
pub struct ExporterHandle {
    shared: Arc<Shared>,
    collector: Arc<dyn CollectorControl>,
    worker: Option<JoinHandle<ShutdownReport>>,
}

impl ExporterHandle {
    /// Return the current exporter lifecycle state.
    pub fn status(&self) -> ExporterStatus {
        ExporterStatus::from_u8(self.shared.metrics.status.load(Ordering::Acquire))
    }

    /// Return the latest worker or transport diagnostic.
    pub fn last_error(&self) -> Option<String> {
        self.shared.last_error.lock().ok().and_then(|error| error.clone())
    }

    /// Snapshot cumulative counters and current gauges.
    pub fn metrics_snapshot(&self) -> ExporterMetricsSnapshot {
        self.shared.metrics.snapshot()
    }

    /// Emit the current snapshot through fast-telemetry's native visitor API.
    pub fn visit_metrics<V: fast_telemetry::MetricVisitor + ?Sized>(&self, visitor: &mut V) {
        visit_exporter_metrics(&self.metrics_snapshot(), visitor);
    }

    /// Stop accepting records and drain until completion or `deadline`.
    pub async fn shutdown(mut self, deadline: Duration) -> ShutdownReport {
        self.shared.accepting.store(false, Ordering::Release);
        self.shared.set_status(ExporterStatus::ShuttingDown);
        self.shared.shutdown.notify_one();

        let Some(mut worker) = self.worker.take() else {
            return ShutdownReport {
                timed_out: false,
                remaining_records: 0,
                metrics: self.shared.metrics.snapshot(),
            };
        };

        match tokio::time::timeout(deadline, &mut worker).await {
            Ok(Ok(report)) => report,
            Ok(Err(error)) => {
                set_last_error(&self.shared, format!("exporter worker failed: {error}"));
                self.collector.clear();
                self.shared.set_status(ExporterStatus::Stopped);
                ShutdownReport {
                    timed_out: false,
                    remaining_records: remaining_records(&self.shared),
                    metrics: self.shared.metrics.snapshot(),
                }
            }
            Err(_) => {
                worker.abort();
                let _ = worker.await;
                let remaining = remaining_records(&self.shared);
                self.collector.clear();
                self.shared.metrics.dropped_shutdown.fetch_add(remaining, Ordering::Relaxed);
                self.shared.metrics.normal_queue_depth.store(0, Ordering::Relaxed);
                self.shared.metrics.reserved_queue_depth.store(0, Ordering::Relaxed);
                self.shared.metrics.inflight.store(0, Ordering::Relaxed);
                self.shared.set_status(ExporterStatus::Stopped);
                ShutdownReport {
                    timed_out: true,
                    remaining_records: remaining,
                    metrics: self.shared.metrics.snapshot(),
                }
            }
        }
    }
}

/// Install on the current Tokio runtime.
pub fn install<R>(config: ExporterConfig) -> Result<ExporterHandle, InstallError>
where
    R: RequestFields,
{
    let runtime = Handle::try_current().map_err(|_| InstallError::NoTokioRuntime)?;
    install_on::<R>(&runtime, config)
}

/// Install on an explicitly supplied Tokio runtime.
///
/// This is useful when setup occurs outside the runtime thread that will own
/// the exporter worker.
pub fn install_on<R>(runtime: &Handle, config: ExporterConfig) -> Result<ExporterHandle, InstallError>
where
    R: RequestFields,
{
    config.validate()?;
    let client = OtlpHttpClient::new(config.http.clone())?;
    let attribute_refs: Vec<_> = config.resource_attributes.iter().map(|(key, value)| (key.as_str(), value.as_str())).collect();
    let resource = build_resource(&config.service_name, &attribute_refs);

    let shared = Arc::new(Shared {
        accepting: AtomicBool::new(true),
        metrics: ExporterMetrics::default(),
        shutdown: Notify::new(),
        records: Notify::new(),
        diagnostic_interval_millis: duration_millis(config.diagnostic_interval),
        last_diagnostic_millis: AtomicU64::new(0),
        last_error: Mutex::new(None),
    });
    let collector = LogCollector::new(config.normal_queue_capacity, config.reserved_queue_capacity, Arc::clone(&shared));

    let sink_shared = Arc::clone(&shared);
    let sink_collector = Arc::clone(&collector);
    let handle_collector: Arc<dyn CollectorControl> = collector.clone();
    let min_level = config.min_level;
    let audiences = config.audiences;
    let priority_threshold = config.priority_threshold;
    eden_logger::install_sink::<R, _>(move |log: EdenLog<R>| {
        if !sink_shared.accepting.load(Ordering::Acquire) {
            sink_shared.metrics.dropped_stopped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if log.level < min_level || !audiences.allows(log.audience) {
            sink_shared.metrics.filtered.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let priority = log.level >= priority_threshold;
        match sink_collector.submit(log, priority) {
            SubmitResult::Accepted => {
                sink_shared.metrics.accepted.fetch_add(1, Ordering::Relaxed);
            }
            SubmitResult::Full => {
                sink_shared.metrics.dropped_queue_full.fetch_add(1, Ordering::Relaxed);
                sink_shared.emergency("bounded log queues are full; records are being dropped");
            }
        }
    })
    .map_err(|_| InstallError::SinkAlreadyInstalled)?;

    let worker_shared = Arc::clone(&shared);
    let worker = runtime.spawn(run_worker(config, client, resource, collector, worker_shared));
    Ok(ExporterHandle { shared, collector: handle_collector, worker: Some(worker) })
}

pub(crate) fn set_last_error(shared: &Shared, message: String) {
    if let Ok(mut error) = shared.last_error.lock() {
        *error = Some(message);
    }
}

pub(crate) fn decrement(value: &AtomicU64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| Some(current.saturating_sub(1)));
}

fn remaining_records(shared: &Shared) -> u64 {
    shared
        .metrics
        .normal_queue_depth
        .load(Ordering::Relaxed)
        .saturating_add(shared.metrics.reserved_queue_depth.load(Ordering::Relaxed))
        .saturating_add(shared.metrics.inflight.load(Ordering::Relaxed))
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_every_bounded_exporter_limit() {
        let valid = ExporterConfig::new("http://localhost:4318", "checkout");
        assert!(valid.validate().is_ok());

        let mut cases = Vec::new();

        let mut config = valid.clone();
        config.service_name.clear();
        cases.push(config);

        let mut config = valid.clone();
        config.scope_name.clear();
        cases.push(config);

        let mut config = valid.clone();
        config.normal_queue_capacity = 0;
        cases.push(config);

        let mut config = valid.clone();
        config.reserved_queue_capacity = 0;
        cases.push(config);

        let mut config = valid.clone();
        config.max_batch_records = 0;
        cases.push(config);

        let mut config = valid.clone();
        config.max_batch_bytes = 0;
        cases.push(config);

        let mut config = valid.clone();
        config.max_batch_delay = Duration::ZERO;
        cases.push(config);

        let mut config = valid.clone();
        config.retry_initial = Duration::ZERO;
        cases.push(config);

        let mut config = valid.clone();
        config.retry_initial = config.retry_max + Duration::from_millis(1);
        cases.push(config);

        let mut config = valid;
        config.diagnostic_interval = Duration::ZERO;
        cases.push(config);

        for config in cases {
            assert!(matches!(config.validate(), Err(InstallError::InvalidConfig(_))));
        }
    }

    #[test]
    fn audience_filter_keeps_each_audience_independent() {
        let filter = AudienceFilter { internal: true, client: false, both: true };

        assert!(filter.allows(LogAudience::Internal));
        assert!(!filter.allows(LogAudience::Client));
        assert!(filter.allows(LogAudience::Both));
    }
}
