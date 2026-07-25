use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use fast_telemetry::{MetricKind, MetricLabel, MetricLabels, MetricMeta, MetricVisitor};

use crate::ExporterStatus;

pub(crate) struct ExporterMetrics {
    pub filtered: AtomicU64,
    pub dropped_queue_full: AtomicU64,
    pub dropped_queue_bytes_full: AtomicU64,
    pub dropped_stopped: AtomicU64,
    pub dropped_oversize: AtomicU64,
    pub dropped_shutdown: AtomicU64,
    pub exported: AtomicU64,
    pub rejected: AtomicU64,
    pub export_attempts: AtomicU64,
    pub retries: AtomicU64,
    pub partial_rejections: AtomicU64,
    pub batches: AtomicU64,
    pub normal_queue_depth: AtomicU64,
    pub reserved_queue_depth: AtomicU64,
    pub normal_queue_bytes: AtomicU64,
    pub reserved_queue_bytes: AtomicU64,
    pub inflight: AtomicU64,
    pub producer_lanes: AtomicU64,
    pub status: AtomicU8,
}

impl Default for ExporterMetrics {
    fn default() -> Self {
        Self {
            filtered: AtomicU64::new(0),
            dropped_queue_full: AtomicU64::new(0),
            dropped_queue_bytes_full: AtomicU64::new(0),
            dropped_stopped: AtomicU64::new(0),
            dropped_oversize: AtomicU64::new(0),
            dropped_shutdown: AtomicU64::new(0),
            exported: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            export_attempts: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            partial_rejections: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            normal_queue_depth: AtomicU64::new(0),
            reserved_queue_depth: AtomicU64::new(0),
            normal_queue_bytes: AtomicU64::new(0),
            reserved_queue_bytes: AtomicU64::new(0),
            inflight: AtomicU64::new(0),
            producer_lanes: AtomicU64::new(0),
            status: AtomicU8::new(ExporterStatus::Running as u8),
        }
    }
}

impl ExporterMetrics {
    pub fn snapshot(&self, accepted: u64) -> ExporterMetricsSnapshot {
        ExporterMetricsSnapshot {
            accepted,
            filtered: load(&self.filtered),
            dropped_queue_full: load(&self.dropped_queue_full),
            dropped_queue_bytes_full: load(&self.dropped_queue_bytes_full),
            dropped_stopped: load(&self.dropped_stopped),
            dropped_oversize: load(&self.dropped_oversize),
            dropped_shutdown: load(&self.dropped_shutdown),
            exported: load(&self.exported),
            rejected: load(&self.rejected),
            export_attempts: load(&self.export_attempts),
            retries: load(&self.retries),
            partial_rejections: load(&self.partial_rejections),
            batches: load(&self.batches),
            normal_queue_depth: load(&self.normal_queue_depth),
            reserved_queue_depth: load(&self.reserved_queue_depth),
            normal_queue_bytes: load(&self.normal_queue_bytes),
            reserved_queue_bytes: load(&self.reserved_queue_bytes),
            inflight: load(&self.inflight),
            producer_lanes: load(&self.producer_lanes),
            status: ExporterStatus::from_u8(self.status.load(Ordering::Acquire)),
        }
    }
}

fn load(value: &AtomicU64) -> u64 {
    value.load(Ordering::Relaxed)
}

/// Cumulative counters and current gauges for one exporter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExporterMetricsSnapshot {
    /// Records accepted into normal or reserved memory queues.
    pub accepted: u64,
    /// Records excluded by severity or audience filters.
    pub filtered: u64,
    /// Records dropped because applicable queues were full.
    pub dropped_queue_full: u64,
    /// Records dropped because applicable queue byte budgets were exhausted.
    pub dropped_queue_bytes_full: u64,
    /// Records submitted after acceptance stopped.
    pub dropped_stopped: u64,
    /// Individual records larger than the configured encoded batch limit.
    pub dropped_oversize: u64,
    /// Records abandoned when bounded shutdown ended.
    pub dropped_shutdown: u64,
    /// Records acknowledged by the collector.
    pub exported: u64,
    /// Records rejected by partial success or isolated invalid-payload checks.
    pub rejected: u64,
    /// OTLP requests attempted.
    pub export_attempts: u64,
    /// Transient request retries.
    pub retries: u64,
    /// OTLP partial-success responses received.
    pub partial_rejections: u64,
    /// Batches acknowledged by the collector, including partial success.
    pub batches: u64,
    /// Records currently held in the normal queue.
    pub normal_queue_depth: u64,
    /// Records currently held in the reserved queue.
    pub reserved_queue_depth: u64,
    /// Estimated retained bytes currently held in the normal queue.
    pub normal_queue_bytes: u64,
    /// Estimated retained bytes currently held in the reserved queue.
    pub reserved_queue_bytes: u64,
    /// Records currently owned by the worker's active batch.
    pub inflight: u64,
    /// Registered thread-local producer lanes.
    pub producer_lanes: u64,
    /// Current exporter lifecycle state.
    pub status: ExporterStatus,
}

impl ExporterMetricsSnapshot {
    /// Sum every record-drop reason in this snapshot.
    pub const fn dropped_total(&self) -> u64 {
        self.dropped_queue_full
            .saturating_add(self.dropped_queue_bytes_full)
            .saturating_add(self.dropped_stopped)
            .saturating_add(self.dropped_oversize)
            .saturating_add(self.dropped_shutdown)
    }
}

const ACCEPTED: MetricMeta<'static> = MetricMeta {
    name: "eden_logger_export.logs_accepted_total",
    help: "Log records accepted into the in-memory exporter queues.",
    kind: MetricKind::Counter,
    unit: Some("logs"),
};
const FILTERED: MetricMeta<'static> = MetricMeta {
    name: "eden_logger_export.logs_filtered_total",
    help: "Log records excluded by exporter severity or audience filters.",
    kind: MetricKind::Counter,
    unit: Some("logs"),
};
const DROPPED: MetricMeta<'static> = MetricMeta {
    name: "eden_logger_export.logs_dropped_total",
    help: "Log records dropped by the bounded direct exporter.",
    kind: MetricKind::Counter,
    unit: Some("logs"),
};
const EXPORTED: MetricMeta<'static> = MetricMeta {
    name: "eden_logger_export.logs_exported_total",
    help: "Log records acknowledged by the OTLP collector.",
    kind: MetricKind::Counter,
    unit: Some("logs"),
};
const REJECTED: MetricMeta<'static> = MetricMeta {
    name: "eden_logger_export.logs_rejected_total",
    help: "Log records rejected by the collector or invalid-payload isolation.",
    kind: MetricKind::Counter,
    unit: Some("logs"),
};
const ATTEMPTS: MetricMeta<'static> = MetricMeta {
    name: "eden_logger_export.export_attempts_total",
    help: "OTLP export requests attempted.",
    kind: MetricKind::Counter,
    unit: Some("requests"),
};
const RETRIES: MetricMeta<'static> = MetricMeta {
    name: "eden_logger_export.export_retries_total",
    help: "OTLP export requests retried after transient failures.",
    kind: MetricKind::Counter,
    unit: Some("requests"),
};
const PARTIAL: MetricMeta<'static> = MetricMeta {
    name: "eden_logger_export.partial_rejections_total",
    help: "OTLP batches that received a partial-success response.",
    kind: MetricKind::Counter,
    unit: Some("batches"),
};
const BATCHES: MetricMeta<'static> = MetricMeta {
    name: "eden_logger_export.batches_exported_total",
    help: "OTLP log batches acknowledged, including partial success.",
    kind: MetricKind::Counter,
    unit: Some("batches"),
};
const QUEUE_DEPTH: MetricMeta<'static> = MetricMeta {
    name: "eden_logger_export.queue_depth",
    help: "Current records held in an exporter queue.",
    kind: MetricKind::Gauge,
    unit: Some("logs"),
};
const QUEUE_BYTES: MetricMeta<'static> = MetricMeta {
    name: "eden_logger_export.queue_bytes",
    help: "Estimated retained bytes held in an exporter queue.",
    kind: MetricKind::Gauge,
    unit: Some("By"),
};
const INFLIGHT: MetricMeta<'static> = MetricMeta {
    name: "eden_logger_export.inflight_records",
    help: "Current records in the batch being exported.",
    kind: MetricKind::Gauge,
    unit: Some("logs"),
};
const STATUS: MetricMeta<'static> = MetricMeta {
    name: "eden_logger_export.exporter_state",
    help: "Current OTLP log exporter lifecycle state.",
    kind: MetricKind::Gauge,
    unit: None,
};
const PRODUCER_LANES: MetricMeta<'static> = MetricMeta {
    name: "eden_logger_export.producer_lanes",
    help: "Current thread-local producer lanes registered with the exporter.",
    kind: MetricKind::Gauge,
    unit: Some("lanes"),
};

/// Emit an exporter snapshot through fast-telemetry's native visitor API.
pub fn visit_exporter_metrics<V: MetricVisitor + ?Sized>(snapshot: &ExporterMetricsSnapshot, visitor: &mut V) {
    visitor.counter(ACCEPTED, MetricLabels::none(), as_i64(snapshot.accepted));
    visitor.counter(FILTERED, MetricLabels::none(), as_i64(snapshot.filtered));
    visitor.counter(EXPORTED, MetricLabels::none(), as_i64(snapshot.exported));
    visitor.counter(REJECTED, MetricLabels::none(), as_i64(snapshot.rejected));
    visitor.counter(ATTEMPTS, MetricLabels::none(), as_i64(snapshot.export_attempts));
    visitor.counter(RETRIES, MetricLabels::none(), as_i64(snapshot.retries));
    visitor.counter(PARTIAL, MetricLabels::none(), as_i64(snapshot.partial_rejections));
    visitor.counter(BATCHES, MetricLabels::none(), as_i64(snapshot.batches));

    for (reason, value) in [
        ("queue_full", snapshot.dropped_queue_full),
        ("queue_bytes_full", snapshot.dropped_queue_bytes_full),
        ("stopped", snapshot.dropped_stopped),
        ("oversize", snapshot.dropped_oversize),
        ("shutdown", snapshot.dropped_shutdown),
    ] {
        let labels = [MetricLabel { name: "reason", value: reason }];
        visitor.counter(DROPPED, MetricLabels::slice(&labels), as_i64(value));
    }

    for (queue, value) in [("normal", snapshot.normal_queue_depth), ("reserved", snapshot.reserved_queue_depth)] {
        let labels = [MetricLabel { name: "queue", value: queue }];
        visitor.gauge_i64(QUEUE_DEPTH, MetricLabels::slice(&labels), as_i64(value));
    }
    for (queue, value) in [("normal", snapshot.normal_queue_bytes), ("reserved", snapshot.reserved_queue_bytes)] {
        let labels = [MetricLabel { name: "queue", value: queue }];
        visitor.gauge_i64(QUEUE_BYTES, MetricLabels::slice(&labels), as_i64(value));
    }
    visitor.gauge_i64(INFLIGHT, MetricLabels::none(), as_i64(snapshot.inflight));
    visitor.gauge_i64(PRODUCER_LANES, MetricLabels::none(), as_i64(snapshot.producer_lanes));
    let labels = [MetricLabel { name: "state", value: snapshot.status.as_str() }];
    visitor.gauge_i64(STATUS, MetricLabels::slice(&labels), 1);
}

const fn as_i64(value: u64) -> i64 {
    if value > i64::MAX as u64 { i64::MAX } else { value as i64 }
}
