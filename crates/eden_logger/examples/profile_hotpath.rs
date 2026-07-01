//! Quick profiling target for flamegraph analysis.
//! Run with: cargo flamegraph --example profile_hotpath --features full -p eden_logger

use eden_logger::{LogAudience, LogTarget, WriterConfig, ctx_with_trace, init, log_info};
use function_name::named;
use std::sync::Arc;

#[named]
fn main() {
    init(WriterConfig { target: LogTarget::Sink, ..Default::default() });

    let collector = Arc::new(fast_telemetry::SpanCollector::new(1, 1024));
    let mut span = collector.start_span("request", fast_telemetry::SpanKind::Server);
    span.enter();

    for _ in 0..5_000_000 {
        let ctx = ctx_with_trace!();
        log_info!(ctx, "handling request", audience = LogAudience::Internal, user_id = "u-123", endpoint = "pg-456");
    }
}
