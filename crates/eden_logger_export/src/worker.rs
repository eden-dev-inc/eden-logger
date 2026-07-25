use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};

use eden_logger::{EdenLog, RequestFields};
use fast_telemetry::otlp::{build_log_export_request, pb};
use fast_telemetry_export::otlp::{OtlpHttpClient, OtlpHttpError};
use prost::Message;
use tokio::time::Instant;

use crate::collector::{CollectorControl, LogCollector, QueuedLog};
use crate::{EdenLogOtlpMapper, ExporterConfig, ExporterMetricsSnapshot, ExporterStatus, Shared, set_last_error};

/// Result of a bounded exporter shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownReport {
    /// Whether the shutdown deadline elapsed before the worker drained.
    pub timed_out: bool,
    /// Records that remained when shutdown ended.
    pub remaining_records: u64,
    /// Final exporter metric snapshot.
    pub metrics: ExporterMetricsSnapshot,
}

enum BatchResult {
    Complete,
    Terminal(Vec<Vec<pb::LogRecord>>),
}

struct WorkerInput<R: RequestFields> {
    reserved: VecDeque<QueuedLog<R>>,
    normal: VecDeque<QueuedLog<R>>,
}

impl<R: RequestFields> WorkerInput<R> {
    fn new() -> Self {
        Self { reserved: VecDeque::new(), normal: VecDeque::new() }
    }

    fn refresh(&mut self, collector: &LogCollector<R>) {
        collector.drain_into(&mut self.reserved, &mut self.normal);
    }

    fn pop_front(&mut self) -> Option<QueuedLog<R>> {
        self.reserved.pop_front().or_else(|| self.normal.pop_front())
    }

    fn is_empty(&self) -> bool {
        self.reserved.is_empty() && self.normal.is_empty()
    }

    fn clear(&mut self) {
        self.reserved.clear();
        self.normal.clear();
    }
}

/// Worker entry point, public for runtimes that want to own task placement.
#[doc(hidden)]
pub(crate) async fn run_worker<R>(
    config: ExporterConfig,
    client: OtlpHttpClient,
    resource: pb::Resource,
    collector: Arc<LogCollector<R>>,
    shared: Arc<Shared>,
) -> ShutdownReport
where
    R: RequestFields,
{
    let mapper = EdenLogOtlpMapper;
    let request_sizer = ExportRequestSizer::new(&resource, &config.scope_name);
    let mut shutting_down = false;
    let mut pending = VecDeque::<pb::LogRecord>::new();
    let mut input = WorkerInput::<R>::new();

    loop {
        // Refresh at every batch boundary so newly-arrived priority records can
        // preempt a normal backlog already owned by the worker.
        input.refresh(&collector);
        let first = if let Some(record) = pending.pop_front() {
            Some(record)
        } else if shutting_down {
            try_receive(&collector, &mut input).map(|log| map_log(&mapper, &log))
        } else {
            match receive(&collector, &mut input, &shared).await {
                Some(log) => Some(map_log(&mapper, &log)),
                None => {
                    shutting_down = true;
                    None
                }
            }
        };

        let Some(first) = first else {
            if shutting_down {
                if let Some(log) = try_receive(&collector, &mut input) {
                    pending.push_back(map_log(&mapper, &log));
                    continue;
                }
                shared.set_status(ExporterStatus::Stopped);
                return ShutdownReport {
                    timed_out: false,
                    remaining_records: 0,
                    metrics: shared.metrics.snapshot(),
                };
            }
            continue;
        };

        let first_field_len = length_delimited_field_len(first.encoded_len());
        if request_sizer.encoded_len(first_field_len) > config.max_batch_bytes {
            reject_oversize(&shared);
            continue;
        }

        let mut records_encoded_len = first_field_len;
        let mut batch = vec![first];
        let deadline = Instant::now() + config.max_batch_delay;

        while batch.len() < config.max_batch_records {
            let next = if let Some(record) = pending.pop_front() {
                Some(record)
            } else if shutting_down {
                try_receive(&collector, &mut input).map(|log| map_log(&mapper, &log))
            } else if let Some(log) = try_receive(&collector, &mut input) {
                Some(map_log(&mapper, &log))
            } else {
                tokio::select! {
                    biased;
                    _ = shared.shutdown.notified() => {
                        shutting_down = true;
                        None
                    }
                    _ = tokio::time::sleep_until(deadline) => None,
                    _ = shared.records.notified() => {
                        try_receive(&collector, &mut input).map(|log| map_log(&mapper, &log))
                    }
                }
            };

            let Some(record) = next else {
                break;
            };
            let record_field_len = length_delimited_field_len(record.encoded_len());
            if request_sizer.encoded_len(record_field_len) > config.max_batch_bytes {
                reject_oversize(&shared);
                continue;
            }
            let candidate_records_len = records_encoded_len.saturating_add(record_field_len);
            if request_sizer.encoded_len(candidate_records_len) > config.max_batch_bytes {
                pending.push_front(record);
                break;
            }
            records_encoded_len = candidate_records_len;
            batch.push(record);
        }

        shared.metrics.inflight.store(batch.len() as u64, Ordering::Relaxed);
        match export_batch(batch, &client, &resource, &config.scope_name, &config, &shared).await {
            BatchResult::Complete => {
                shared.metrics.inflight.store(0, Ordering::Relaxed);
                if shutting_down {
                    shared.set_status(ExporterStatus::ShuttingDown);
                } else {
                    shared.set_status(ExporterStatus::Running);
                }
            }
            BatchResult::Terminal(retained) => {
                let retained_count = retained.iter().map(Vec::len).sum::<usize>() as u64;
                shared.metrics.inflight.store(retained_count, Ordering::Relaxed);
                if shared.accepting.load(Ordering::Acquire) {
                    shared.shutdown.notified().await;
                }
                let queued = shared
                    .metrics
                    .normal_queue_depth
                    .load(Ordering::Relaxed)
                    .saturating_add(shared.metrics.reserved_queue_depth.load(Ordering::Relaxed));
                let remaining = retained_count.saturating_add(queued);
                shared.metrics.dropped_shutdown.fetch_add(remaining, Ordering::Relaxed);
                collector.clear();
                input.clear();
                shared.metrics.normal_queue_depth.store(0, Ordering::Relaxed);
                shared.metrics.reserved_queue_depth.store(0, Ordering::Relaxed);
                shared.metrics.inflight.store(0, Ordering::Relaxed);
                shared.set_status(ExporterStatus::Stopped);
                return ShutdownReport {
                    timed_out: false,
                    remaining_records: remaining,
                    metrics: shared.metrics.snapshot(),
                };
            }
        }
    }
}

fn map_log<R: RequestFields>(mapper: &EdenLogOtlpMapper, log: &EdenLog<R>) -> pb::LogRecord {
    mapper.map(log, unix_nanos())
}

struct ExportRequestSizer {
    resource_field_len: usize,
    scope_field_len: usize,
}

impl ExportRequestSizer {
    fn new(resource: &pb::Resource, scope_name: &str) -> Self {
        let scope = pb::InstrumentationScope { name: scope_name.to_string(), ..Default::default() };
        Self {
            resource_field_len: length_delimited_field_len(resource.encoded_len()),
            scope_field_len: length_delimited_field_len(scope.encoded_len()),
        }
    }

    fn encoded_len(&self, records_encoded_len: usize) -> usize {
        let scope_logs_len = self.scope_field_len.saturating_add(records_encoded_len);
        let resource_logs_len = self.resource_field_len.saturating_add(length_delimited_field_len(scope_logs_len));
        length_delimited_field_len(resource_logs_len)
    }
}

fn length_delimited_field_len(payload_len: usize) -> usize {
    1_usize.saturating_add(varint_len(payload_len)).saturating_add(payload_len)
}

fn varint_len(mut value: usize) -> usize {
    let mut encoded_len = 1;
    while value >= 128 {
        value >>= 7;
        encoded_len += 1;
    }
    encoded_len
}

async fn receive<R: RequestFields>(collector: &LogCollector<R>, input: &mut WorkerInput<R>, shared: &Shared) -> Option<EdenLog<R>> {
    loop {
        if let Some(log) = try_receive(collector, input) {
            return Some(log);
        }
        tokio::select! {
            biased;
            _ = shared.shutdown.notified() => return None,
            _ = shared.records.notified() => {}
        }
    }
}

fn try_receive<R: RequestFields>(collector: &LogCollector<R>, input: &mut WorkerInput<R>) -> Option<EdenLog<R>> {
    if input.is_empty() {
        input.refresh(collector);
    }
    let queued = input.pop_front()?;
    collector.release(queued.queue);
    Some(queued.log)
}

async fn export_batch(
    batch: Vec<pb::LogRecord>,
    client: &OtlpHttpClient,
    resource: &pb::Resource,
    scope_name: &str,
    config: &ExporterConfig,
    shared: &Shared,
) -> BatchResult {
    let mut pending = vec![batch];

    while let Some(records) = pending.pop() {
        let mut failures = 0_u32;
        loop {
            let request = build_log_export_request(resource, scope_name, records.clone());
            shared.metrics.export_attempts.fetch_add(1, Ordering::Relaxed);
            match client.export_logs(&request).await {
                Ok(outcome) if outcome.rejected == 0 => {
                    shared.metrics.exported.fetch_add(records.len() as u64, Ordering::Relaxed);
                    shared.metrics.batches.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                Ok(outcome) => {
                    shared.metrics.partial_rejections.fetch_add(1, Ordering::Relaxed);
                    if records.len() == 1 {
                        reject_record(shared, outcome.message.as_deref().unwrap_or("collector partially rejected an individual record"));
                    } else {
                        split_records(records, &mut pending);
                    }
                    break;
                }
                Err(error) if error.is_invalid_payload() => {
                    if records.len() == 1 {
                        reject_record(shared, &error.to_string());
                    } else {
                        split_records(records, &mut pending);
                    }
                    break;
                }
                Err(error) if error.is_retryable() => {
                    failures = failures.saturating_add(1);
                    shared.metrics.retries.fetch_add(1, Ordering::Relaxed);
                    shared.set_status(ExporterStatus::BackingOff);
                    set_last_error(shared, error.to_string());
                    let delay = error
                        .retry_after
                        .unwrap_or_else(|| retry_delay(failures, config.retry_initial, config.retry_max))
                        .min(config.retry_max);
                    tokio::time::sleep(delay).await;
                    if shared.accepting.load(Ordering::Acquire) {
                        shared.set_status(ExporterStatus::Running);
                    } else {
                        shared.set_status(ExporterStatus::ShuttingDown);
                    }
                }
                Err(error) => {
                    return terminal(records, pending, error, shared);
                }
            }
        }
    }

    BatchResult::Complete
}

fn split_records(mut records: Vec<pb::LogRecord>, pending: &mut Vec<Vec<pb::LogRecord>>) {
    let right = records.split_off(records.len() / 2);
    pending.push(right);
    pending.push(records);
}

fn terminal(records: Vec<pb::LogRecord>, mut pending: Vec<Vec<pb::LogRecord>>, error: OtlpHttpError, shared: &Shared) -> BatchResult {
    set_last_error(shared, error.to_string());
    shared.set_status(ExporterStatus::TerminalFailure);
    shared.emergency(&format!("OTLP exporter entered terminal failure: {error}"));
    pending.push(records);
    BatchResult::Terminal(pending)
}

fn reject_record(shared: &Shared, reason: &str) {
    shared.metrics.rejected.fetch_add(1, Ordering::Relaxed);
    shared.emergency(&format!("collector rejected an individual log record: {reason}"));
}

fn reject_oversize(shared: &Shared) {
    shared.metrics.dropped_oversize.fetch_add(1, Ordering::Relaxed);
    shared.metrics.rejected.fetch_add(1, Ordering::Relaxed);
    shared.emergency("an encoded log record exceeded max_batch_bytes and was dropped");
}

fn retry_delay(failures: u32, initial: Duration, maximum: Duration) -> Duration {
    let exponent = failures.saturating_sub(1).min(20);
    let base_millis = initial.as_millis().saturating_mul(1_u128 << exponent).min(maximum.as_millis());
    let nanos = SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().subsec_nanos() as u128;
    let percent = 75_u128.saturating_add(nanos % 51);
    let jittered = base_millis.saturating_mul(percent) / 100;
    Duration::from_millis(jittered.min(u128::from(u64::MAX)) as u64)
}

fn unix_nanos() -> u64 {
    SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fast_telemetry::otlp::build_resource;
    use hegel::TestCase;
    use hegel::generators as gs;

    #[test]
    fn retry_is_bounded_and_jittered() {
        let initial = Duration::from_millis(250);
        let maximum = Duration::from_secs(30);
        for failures in 1..30 {
            let delay = retry_delay(failures, initial, maximum);
            assert!(delay <= Duration::from_millis(37_500));
            assert!(!delay.is_zero());
        }
    }

    #[test]
    fn request_size_matches_protobuf_encoding() {
        let resource = build_resource("checkout", &[("service.instance.id", "checkout-1")]);
        let records = vec![
            pb::LogRecord {
                body: Some(pb::AnyValue {
                    value: Some(pb::any_value::Value::StringValue("short".to_string())),
                }),
                ..Default::default()
            },
            pb::LogRecord {
                body: Some(pb::AnyValue {
                    value: Some(pb::any_value::Value::StringValue("x".repeat(256))),
                }),
                ..Default::default()
            },
        ];
        let request = build_log_export_request(&resource, "eden_logger", records.clone());
        let records_encoded_len = records.iter().map(|record| length_delimited_field_len(record.encoded_len())).sum();

        assert_eq!(
            ExportRequestSizer::new(&resource, "eden_logger").encoded_len(records_encoded_len),
            request.encoded_len()
        );
    }

    #[hegel::test(test_cases = 300)]
    fn generated_requests_match_exact_protobuf_size(tc: TestCase) {
        let service_len = tc.draw(gs::integers::<u8>().min_value(1).max_value(64)) as usize;
        let scope_len = tc.draw(gs::integers::<u8>().max_value(64)) as usize;
        let body_lengths = tc.draw(gs::vecs(gs::integers::<u16>().max_value(2_048)).min_size(1).max_size(48));
        let service_name = "s".repeat(service_len);
        let scope_name = "o".repeat(scope_len);
        let resource = build_resource(&service_name, &[("service.instance.id", "instance-1")]);
        let records = body_lengths
            .into_iter()
            .map(|body_len| pb::LogRecord {
                body: Some(pb::AnyValue {
                    value: Some(pb::any_value::Value::StringValue("x".repeat(body_len as usize))),
                }),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let request = build_log_export_request(&resource, &scope_name, records.clone());
        let records_encoded_len = records.iter().map(|record| length_delimited_field_len(record.encoded_len())).sum();

        assert_eq!(
            ExportRequestSizer::new(&resource, &scope_name).encoded_len(records_encoded_len),
            request.encoded_len()
        );
    }

    #[hegel::test(test_cases = 300)]
    fn recursive_batch_bisection_preserves_order_and_identity(tc: TestCase) {
        let count = tc.draw(gs::integers::<u8>().min_value(1).max_value(128)) as usize;
        let records = (0..count).map(|index| pb::LogRecord { flags: index as u32, ..Default::default() }).collect::<Vec<_>>();
        let mut pending = vec![records];
        let mut recovered = Vec::with_capacity(count);

        while let Some(records) = pending.pop() {
            if records.len() == 1 {
                recovered.push(records[0].flags as usize);
            } else {
                split_records(records, &mut pending);
            }
        }

        assert_eq!(recovered, (0..count).collect::<Vec<_>>());
    }

    #[hegel::test(test_cases = 300)]
    fn generated_retry_delays_stay_inside_the_jitter_envelope(tc: TestCase) {
        let failures = tc.draw(gs::integers::<u8>().min_value(1).max_value(40)) as u32;
        let initial_millis = tc.draw(gs::integers::<u16>().min_value(4).max_value(2_000)) as u128;
        let maximum_millis = tc.draw(gs::integers::<u16>().min_value(4).max_value(30_000)) as u128;
        let initial = Duration::from_millis(initial_millis as u64);
        let maximum = Duration::from_millis(maximum_millis as u64);
        let exponent = failures.saturating_sub(1).min(20);
        let base_millis = initial_millis.saturating_mul(1_u128 << exponent).min(maximum_millis);
        let delay_millis = retry_delay(failures, initial, maximum).as_millis();

        assert!(delay_millis >= base_millis.saturating_mul(75) / 100);
        assert!(delay_millis <= base_millis.saturating_mul(125) / 100);
        assert!(delay_millis > 0);
    }
}
