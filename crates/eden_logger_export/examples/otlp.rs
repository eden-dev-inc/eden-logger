use std::time::Duration;

use eden_logger::{EdenLog, LogAudience, LogContext, LogLevel};
use eden_logger_export::{ExporterConfig, install};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let exporter = install::<()>(ExporterConfig::new("http://localhost:4318", "eden-logger-example"))?;

    EdenLog::new(LogLevel::Info, "hello from eden_logger", &LogContext::empty(), LogAudience::Internal).emit();

    let report = exporter.shutdown(Duration::from_secs(5)).await;
    if report.timed_out {
        eprintln!("exporter shutdown timed out with {} records remaining", report.remaining_records);
    }
    Ok(())
}
