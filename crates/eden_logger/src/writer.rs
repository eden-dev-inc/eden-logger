//! Direct logger - writes directly to stderr/stdout

use crate::trace::{TraceSource, set_trace_source};
use std::io::{self, Write};
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum LogTarget {
    /// Write to stderr (default, production use)
    #[default]
    Stderr = 0,
    /// Write to stdout
    Stdout = 1,
    /// Write to sink (for benchmarking, no actual I/O)
    Sink = 2,
}

impl LogTarget {
    #[inline(always)]
    fn from_u8(v: u8) -> Self {
        match v {
            1 => LogTarget::Stdout,
            2 => LogTarget::Sink,
            _ => LogTarget::Stderr,
        }
    }
}

/// Output format for log lines.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum LogFormat {
    #[default]
    Display = 0,
    Json = 1,
}

impl LogFormat {
    #[inline(always)]
    fn from_u8(v: u8) -> Self {
        match v {
            1 => LogFormat::Json,
            _ => LogFormat::Display,
        }
    }
}

#[derive(Clone, Copy)]
pub struct WriterConfig {
    /// Output target.
    pub target: LogTarget,
    /// Output format.
    pub format: LogFormat,
    /// Source used to extract trace/span IDs for log enrichment.
    pub trace_source: TraceSource,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            target: LogTarget::Stderr,
            format: LogFormat::Display,
            trace_source: TraceSource::FastTelemetry,
        }
    }
}

// Stored as atomics so reads from the hot path don't need `unsafe` and writes
// from `init` are race-free. `Relaxed` ordering is sufficient: there is no
// memory we care about synchronizing — once the bytes are stored, every
// thread will observe them eventually, and a brief window of stale reads
// after `init` is harmless (it just means a few logs go to the old target).
static TARGET: AtomicU8 = AtomicU8::new(LogTarget::Stderr as u8);
static FORMAT: AtomicU8 = AtomicU8::new(LogFormat::Display as u8);

/// Initialize the logger with configuration.
pub fn init(config: WriterConfig) {
    TARGET.store(config.target as u8, Ordering::Relaxed);
    FORMAT.store(config.format as u8, Ordering::Relaxed);
    set_trace_source(config.trace_source);
}

/// Returns the current log format.
#[inline(always)]
pub fn format() -> LogFormat {
    LogFormat::from_u8(FORMAT.load(Ordering::Relaxed))
}

/// Returns the current log target.
#[inline(always)]
pub fn target() -> LogTarget {
    LogTarget::from_u8(TARGET.load(Ordering::Relaxed))
}

/// Log a line directly to stderr/stdout
#[inline(always)]
pub fn log(line: &str) {
    match target() {
        LogTarget::Stderr => {
            let mut h = io::stderr().lock();
            let _ = h.write_all(line.as_bytes());
            let _ = h.write_all(b"\n");
        }
        LogTarget::Stdout => {
            let mut h = io::stdout().lock();
            let _ = h.write_all(line.as_bytes());
            let _ = h.write_all(b"\n");
        }
        LogTarget::Sink => {
            let mut sink = io::sink();
            let _ = sink.write_all(line.as_bytes());
            let _ = sink.write_all(b"\n");
        }
    }
}

/// Log pre-formatted bytes (newline already included) in a single write.
///
/// Avoids the double `write_all` (line + newline) and lets the caller
/// reuse a thread-local buffer.
#[inline(always)]
pub fn log_bytes(bytes: &[u8]) {
    match target() {
        LogTarget::Stderr => {
            let _ = io::stderr().lock().write_all(bytes);
        }
        LogTarget::Stdout => {
            let _ = io::stdout().lock().write_all(bytes);
        }
        LogTarget::Sink => {
            let _ = io::sink().write_all(bytes);
        }
    }
}
