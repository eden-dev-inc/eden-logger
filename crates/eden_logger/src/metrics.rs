//! fast-telemetry counters for logger activity.
//!
//! This module is compiled with the `fast-telemetry-context` feature because it
//! uses fast-telemetry's grouped counter primitives. The hot path updates one
//! [`fast_telemetry::CounterSet`] row for total, level, and audience counters.

use crate::context::LogAudience;
use crate::schema::LogLevel;
use fast_telemetry::{CounterSet, MetricKind, MetricLabel, MetricLabels, MetricMeta, MetricVisitor};
use std::sync::OnceLock;

const LOG_COUNTER_SHARDS: usize = 64;

const IDX_TOTAL: usize = 0;
const IDX_LEVEL_TRACE: usize = 1;
const IDX_LEVEL_DEBUG: usize = 2;
const IDX_LEVEL_INFO: usize = 3;
const IDX_LEVEL_WARN: usize = 4;
const IDX_LEVEL_ERROR: usize = 5;
const IDX_AUDIENCE_INTERNAL: usize = 6;
const IDX_AUDIENCE_CLIENT: usize = 7;
const IDX_AUDIENCE_BOTH: usize = 8;
const LOG_COUNTER_COUNT: usize = 9;

static LOG_COUNTERS: OnceLock<CounterSet> = OnceLock::new();

const TOTAL_META: MetricMeta<'static> = MetricMeta {
    name: "eden_logger.logs_emitted_total",
    help: "Total log records emitted after runtime filtering.",
    kind: MetricKind::Counter,
    unit: Some("logs"),
};

const LEVEL_META: MetricMeta<'static> = MetricMeta {
    name: "eden_logger.logs_emitted_by_level_total",
    help: "Log records emitted after runtime filtering, grouped by severity level.",
    kind: MetricKind::Counter,
    unit: Some("logs"),
};

const AUDIENCE_META: MetricMeta<'static> = MetricMeta {
    name: "eden_logger.logs_emitted_by_audience_total",
    help: "Log records emitted after runtime filtering, grouped by log audience.",
    kind: MetricKind::Counter,
    unit: Some("logs"),
};

/// Cumulative log counters grouped by severity level.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogLevelMetrics {
    pub trace: u64,
    pub debug: u64,
    pub info: u64,
    pub warn: u64,
    pub error: u64,
}

/// Cumulative log counters grouped by audience.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogAudienceMetrics {
    pub internal: u64,
    pub client: u64,
    pub both: u64,
}

/// Cumulative logger metrics snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogMetricsSnapshot {
    pub emitted_total: u64,
    pub by_level: LogLevelMetrics,
    pub by_audience: LogAudienceMetrics,
}

#[inline]
fn counters() -> &'static CounterSet {
    LOG_COUNTERS.get_or_init(|| CounterSet::new(LOG_COUNTER_SHARDS, LOG_COUNTER_COUNT))
}

#[inline(always)]
const fn level_counter_idx(level: LogLevel) -> usize {
    match level {
        LogLevel::Trace => IDX_LEVEL_TRACE,
        LogLevel::Debug => IDX_LEVEL_DEBUG,
        LogLevel::Info => IDX_LEVEL_INFO,
        LogLevel::Warn => IDX_LEVEL_WARN,
        LogLevel::Error => IDX_LEVEL_ERROR,
    }
}

#[inline(always)]
const fn audience_counter_idx(audience: LogAudience) -> usize {
    match audience {
        LogAudience::Internal => IDX_AUDIENCE_INTERNAL,
        LogAudience::Client => IDX_AUDIENCE_CLIENT,
        LogAudience::Both => IDX_AUDIENCE_BOTH,
    }
}

#[inline(always)]
fn counter_value(idx: usize) -> u64 {
    counters().sum(idx).max(0) as u64
}

#[inline(always)]
fn counter_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[inline(always)]
pub(crate) fn record_emitted(level: LogLevel, audience: LogAudience) {
    counters().add_indices(&[IDX_TOTAL, level_counter_idx(level), audience_counter_idx(audience)], 1);
}

/// Return cumulative logger metrics.
pub fn log_metrics_snapshot() -> LogMetricsSnapshot {
    LogMetricsSnapshot {
        emitted_total: counter_value(IDX_TOTAL),
        by_level: LogLevelMetrics {
            trace: counter_value(IDX_LEVEL_TRACE),
            debug: counter_value(IDX_LEVEL_DEBUG),
            info: counter_value(IDX_LEVEL_INFO),
            warn: counter_value(IDX_LEVEL_WARN),
            error: counter_value(IDX_LEVEL_ERROR),
        },
        by_audience: LogAudienceMetrics {
            internal: counter_value(IDX_AUDIENCE_INTERNAL),
            client: counter_value(IDX_AUDIENCE_CLIENT),
            both: counter_value(IDX_AUDIENCE_BOTH),
        },
    }
}

/// Visit cumulative logger metrics using fast-telemetry's structured visitor API.
pub fn visit_log_metrics<V: MetricVisitor + ?Sized>(visitor: &mut V) {
    let snapshot = log_metrics_snapshot();
    visitor.counter(TOTAL_META, MetricLabels::none(), counter_i64(snapshot.emitted_total));

    let level_values = [
        ("trace", snapshot.by_level.trace),
        ("debug", snapshot.by_level.debug),
        ("info", snapshot.by_level.info),
        ("warn", snapshot.by_level.warn),
        ("error", snapshot.by_level.error),
    ];
    for (level, value) in level_values {
        let labels = [MetricLabel { name: "level", value: level }];
        visitor.counter(LEVEL_META, MetricLabels::slice(&labels), counter_i64(value));
    }

    let audience_values = [
        ("internal", snapshot.by_audience.internal),
        ("client", snapshot.by_audience.client),
        ("both", snapshot.by_audience.both),
    ];
    for (audience, value) in audience_values {
        let labels = [MetricLabel { name: "audience", value: audience }];
        visitor.counter(AUDIENCE_META, MetricLabels::slice(&labels), counter_i64(value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fast_telemetry::{DistributionSnapshot, HistogramSnapshot};
    use std::sync::{Mutex, MutexGuard};

    static METRICS_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn metrics_guard() -> MutexGuard<'static, ()> {
        METRICS_TEST_LOCK.lock().expect("metrics test lock poisoned")
    }

    fn snapshot_delta(before: LogMetricsSnapshot, after: LogMetricsSnapshot) -> LogMetricsSnapshot {
        LogMetricsSnapshot {
            emitted_total: after.emitted_total.saturating_sub(before.emitted_total),
            by_level: LogLevelMetrics {
                trace: after.by_level.trace.saturating_sub(before.by_level.trace),
                debug: after.by_level.debug.saturating_sub(before.by_level.debug),
                info: after.by_level.info.saturating_sub(before.by_level.info),
                warn: after.by_level.warn.saturating_sub(before.by_level.warn),
                error: after.by_level.error.saturating_sub(before.by_level.error),
            },
            by_audience: LogAudienceMetrics {
                internal: after.by_audience.internal.saturating_sub(before.by_audience.internal),
                client: after.by_audience.client.saturating_sub(before.by_audience.client),
                both: after.by_audience.both.saturating_sub(before.by_audience.both),
            },
        }
    }

    #[derive(Debug)]
    struct SeenCounter {
        name: String,
        labels: Vec<(String, String)>,
        value: i64,
    }

    #[derive(Default)]
    struct CapturingVisitor {
        counters: Vec<SeenCounter>,
    }

    impl MetricVisitor for CapturingVisitor {
        fn counter(&mut self, meta: MetricMeta<'_>, labels: MetricLabels<'_>, value: i64) {
            self.counters.push(SeenCounter {
                name: meta.name.to_string(),
                labels: labels.iter().map(|label| (label.name.to_string(), label.value.to_string())).collect(),
                value,
            });
        }

        fn gauge_i64(&mut self, _meta: MetricMeta<'_>, _labels: MetricLabels<'_>, _value: i64) {}

        fn gauge_f64(&mut self, _meta: MetricMeta<'_>, _labels: MetricLabels<'_>, _value: f64) {}

        fn histogram(&mut self, _meta: MetricMeta<'_>, _labels: MetricLabels<'_>, _histogram: &dyn HistogramSnapshot) {}

        fn distribution(&mut self, _meta: MetricMeta<'_>, _labels: MetricLabels<'_>, _distribution: &dyn DistributionSnapshot) {}
    }

    #[test]
    fn grouped_counters_track_total_level_and_audience() {
        let _guard = metrics_guard();
        let before = log_metrics_snapshot();

        record_emitted(LogLevel::Info, LogAudience::Internal);
        record_emitted(LogLevel::Error, LogAudience::Both);

        let delta = snapshot_delta(before, log_metrics_snapshot());
        assert_eq!(delta.emitted_total, 2);
        assert_eq!(delta.by_level.info, 1);
        assert_eq!(delta.by_level.error, 1);
        assert_eq!(delta.by_audience.internal, 1);
        assert_eq!(delta.by_audience.both, 1);
    }

    #[test]
    fn visit_log_metrics_exports_grouped_counter_series() {
        let _guard = metrics_guard();
        record_emitted(LogLevel::Warn, LogAudience::Client);

        let mut visitor = CapturingVisitor::default();
        visit_log_metrics(&mut visitor);

        assert!(visitor.counters.iter().any(|counter| counter.name == TOTAL_META.name));
        assert!(visitor.counters.iter().any(|counter| {
            counter.name == LEVEL_META.name && counter.labels == vec![("level".to_string(), "warn".to_string())] && counter.value >= 1
        }));
        assert!(visitor.counters.iter().any(|counter| {
            counter.name == AUDIENCE_META.name
                && counter.labels == vec![("audience".to_string(), "client".to_string())]
                && counter.value >= 1
        }));
    }
}
