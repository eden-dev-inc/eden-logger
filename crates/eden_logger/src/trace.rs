//! Extracts trace and span IDs from the current span context.
//!
//! Supports two sources, selectable at runtime via [`set_trace_source`]:
//! - [`TraceSource::FastTelemetry`] — reads the fast-telemetry thread-local
//!   (~1ns Cell read). Requires the `fast-telemetry-context` feature.
//! - [`TraceSource::Otel`] — reads `opentelemetry::Context::current()`.
//!   Requires the `otel-context` feature.
//!
//! Returns `(None, None)` when no span is active or the selected source's
//! feature is not compiled in.

use crate::context::LogContext;
use crate::fields::RequestFields;
use smol_str::SmolStr;
use std::sync::atomic::{AtomicU8, Ordering};

/// Source for trace/span ID extraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TraceSource {
    /// Read from the fast-telemetry thread-local span context.
    FastTelemetry = 0,
    /// Read from the current `opentelemetry::Context`.
    Otel = 1,
}

/// Active trace source. Default is `FastTelemetry` to preserve existing behavior.
static TRACE_SOURCE: AtomicU8 = AtomicU8::new(TraceSource::FastTelemetry as u8);

/// Set the active trace source.
///
/// Typically called once at startup from `eden_logger::init`. Safe to call
/// from multiple threads; the last write wins.
#[inline]
pub fn set_trace_source(source: TraceSource) {
    TRACE_SOURCE.store(source as u8, Ordering::Relaxed);
}

/// Read the active trace source.
#[inline(always)]
pub fn trace_source() -> TraceSource {
    match TRACE_SOURCE.load(Ordering::Relaxed) {
        1 => TraceSource::Otel,
        _ => TraceSource::FastTelemetry,
    }
}

/// Hex lookup table — maps a nibble (0–15) to its ASCII hex character.
#[cfg(any(feature = "fast-telemetry-context", feature = "otel-context"))]
const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

/// Encode 16 bytes as 32-char lowercase hex into a stack buffer.
/// Avoids the heap allocation of `format!("{:032x}", ...)`.
#[cfg(any(feature = "fast-telemetry-context", feature = "otel-context"))]
fn hex_encode_16(bytes: &[u8; 16]) -> [u8; 32] {
    let mut out = [0u8; 32];
    for (i, &b) in bytes.iter().enumerate() {
        out[i * 2] = HEX_CHARS[(b >> 4) as usize];
        out[i * 2 + 1] = HEX_CHARS[(b & 0x0f) as usize];
    }
    out
}

/// Encode 8 bytes as 16-char lowercase hex into a stack buffer.
#[cfg(any(feature = "fast-telemetry-context", feature = "otel-context"))]
fn hex_encode_8(bytes: &[u8; 8]) -> [u8; 16] {
    let mut out = [0u8; 16];
    for (i, &b) in bytes.iter().enumerate() {
        out[i * 2] = HEX_CHARS[(b >> 4) as usize];
        out[i * 2 + 1] = HEX_CHARS[(b & 0x0f) as usize];
    }
    out
}

/// Build `SmolStr` pair from 16-byte trace + 8-byte span id.
#[cfg(any(feature = "fast-telemetry-context", feature = "otel-context"))]
#[inline]
fn ids_from_bytes(trace: &[u8; 16], span: &[u8; 8]) -> (Option<SmolStr>, Option<SmolStr>) {
    let trace_hex = hex_encode_16(trace);
    let span_hex = hex_encode_8(span);
    // SAFETY: `hex_encode_16` and `hex_encode_8` write each output byte from
    // a lookup into `HEX_CHARS = b"0123456789abcdef"`. Every byte in
    // `HEX_CHARS` is in 0x30..=0x66 (ASCII '0'-'9', 'a'-'f'), so every byte
    // of `trace_hex` / `span_hex` is single-byte UTF-8. Therefore the
    // resulting buffers are valid UTF-8 and `from_utf8_unchecked` does not
    // produce a malformed `&str`. The `SmolStr::new` calls below then own
    // copies, so the unchecked references' lifetimes are scoped to this
    // function.
    let tid = unsafe { std::str::from_utf8_unchecked(&trace_hex) };
    let sid = unsafe { std::str::from_utf8_unchecked(&span_hex) };
    (Some(SmolStr::new(tid)), Some(SmolStr::new(sid)))
}

#[cfg(feature = "fast-telemetry-context")]
#[inline]
fn extract_from_fast_telemetry() -> (Option<SmolStr>, Option<SmolStr>) {
    if let (Some(trace_id), Some(span_id)) = (fast_telemetry::current_trace_id(), fast_telemetry::current_span_id()) {
        return ids_from_bytes(trace_id.as_bytes(), span_id.as_bytes());
    }
    (None, None)
}

#[cfg(feature = "otel-context")]
#[inline]
fn extract_from_otel() -> (Option<SmolStr>, Option<SmolStr>) {
    use opentelemetry::trace::TraceContextExt;
    let ctx = opentelemetry::Context::current();
    let span_ref = ctx.span();
    let sc = span_ref.span_context();
    if !sc.is_valid() {
        return (None, None);
    }
    ids_from_bytes(&sc.trace_id().to_bytes(), &sc.span_id().to_bytes())
}

/// Extracts trace ID and span ID from the currently selected trace source.
///
/// Returns `(None, None)` when no span is active or the selected source's
/// feature is not compiled in.
///
/// `span_id` (16 hex chars) is inlined in `SmolStr` (zero alloc); `trace_id`
/// (32 hex chars) requires one allocation.
pub fn extract_trace_context() -> (Option<SmolStr>, Option<SmolStr>) {
    match trace_source() {
        TraceSource::FastTelemetry => {
            #[cfg(feature = "fast-telemetry-context")]
            {
                extract_from_fast_telemetry()
            }
            #[cfg(not(feature = "fast-telemetry-context"))]
            {
                (None, None)
            }
        }
        TraceSource::Otel => {
            #[cfg(feature = "otel-context")]
            {
                extract_from_otel()
            }
            #[cfg(not(feature = "otel-context"))]
            {
                (None, None)
            }
        }
    }
}

/// Creates LogContext with current trace IDs. Convenience wrapper around extract_trace_context().
pub fn trace_context<R: RequestFields>() -> LogContext<R> {
    let (trace_id, span_id) = extract_trace_context();
    let mut ctx = LogContext::<R>::new();

    if let Some(tid) = trace_id {
        ctx = ctx.with_trace_id(tid);
    }

    if let Some(sid) = span_id {
        ctx = ctx.with_span_id(sid);
    }

    ctx
}

/// Extension trait for adding trace context to LogContext.
pub trait TraceContextExt {
    /// Adds current trace IDs to this context.
    fn with_trace_context(self) -> Self;
}

impl<R: RequestFields> TraceContextExt for LogContext<R> {
    fn with_trace_context(mut self) -> Self {
        let (trace_id, span_id) = extract_trace_context();

        if let Some(tid) = trace_id {
            self = self.with_trace_id(tid);
        }

        if let Some(sid) = span_id {
            self = self.with_span_id(sid);
        }

        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_trace_context_no_span() {
        // Without an active span, should return None
        let (trace_id, span_id) = extract_trace_context();
        assert!(trace_id.is_none());
        assert!(span_id.is_none());
    }

    #[test]
    fn test_trace_context_builder() {
        let ctx = trace_context::<()>().with_feature("test");
        assert_eq!(ctx.feature.as_deref(), Some("test"));
    }

    #[test]
    fn test_trace_context_ext() {
        let ctx = LogContext::<()>::new().with_feature("test").with_trace_context();

        assert_eq!(ctx.feature.as_deref(), Some("test"));
        // trace_id and span_id may or may not be present depending on context
    }

    #[test]
    fn test_trace_source_default_and_set() {
        // Default is FastTelemetry.
        let original = trace_source();
        set_trace_source(TraceSource::Otel);
        assert_eq!(trace_source(), TraceSource::Otel);
        set_trace_source(TraceSource::FastTelemetry);
        assert_eq!(trace_source(), TraceSource::FastTelemetry);
        // Restore.
        set_trace_source(original);
    }

    #[cfg(any(feature = "fast-telemetry-context", feature = "otel-context"))]
    #[test]
    fn test_hex_encode_roundtrip() {
        let bytes16: [u8; 16] = [
            0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36,
        ];
        let hex = hex_encode_16(&bytes16);
        assert_eq!(std::str::from_utf8(&hex).expect("valid utf8"), "4bf92f3577b34da6a3ce929d0e0e4736");

        let bytes8: [u8; 8] = [0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7];
        let hex = hex_encode_8(&bytes8);
        assert_eq!(std::str::from_utf8(&hex).expect("valid utf8"), "00f067aa0ba902b7");
    }
}
