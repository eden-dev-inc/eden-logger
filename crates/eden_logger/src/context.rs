//! Context builder for accumulating log metadata using builder pattern.

use crate::fields::RequestFields;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::collections::HashMap;

/// Distinguishes internal vs client-facing logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum LogAudience {
    /// Internal logs (not sent to clients).
    Internal,

    /// Client-facing logs (sent in API responses).
    Client,

    /// Logs for both operators and clients.
    Both,
}

impl LogAudience {
    #[inline(always)]
    pub const fn as_str(&self) -> &'static str {
        match self {
            LogAudience::Internal => "INTERNAL",
            LogAudience::Client => "CLIENT",
            LogAudience::Both => "BOTH",
        }
    }
}

impl std::fmt::Display for LogAudience {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogAudience::Internal => write!(f, "INTERNAL"),
            LogAudience::Client => write!(f, "CLIENT"),
            LogAudience::Both => write!(f, "BOTH"),
        }
    }
}

/// Accumulates contextual metadata for log records using builder pattern.
///
/// Uses `SmolStr` for efficient storage of small strings (≤23 bytes inline, no heap allocation).
/// Most logging strings (function names, feature names, UUIDs, error codes) fit inline.
///
/// `LogContext` is generic over `R: RequestFields` — application-specific
/// identity fields (tenant, user, endpoint, etc.) live in `R`. The default
/// `R = ()` carries no request schema; downstream crates supply their own.
#[derive(Debug, Clone, Default)]
pub struct LogContext<R: RequestFields = ()> {
    // Tracing Integration
    pub trace_id: Option<SmolStr>,
    pub span_id: Option<SmolStr>,

    // Application Context
    pub feature: Option<SmolStr>,
    pub function: Option<SmolStr>,

    // Error Information
    pub error_code: Option<SmolStr>,
    pub error_category: Option<SmolStr>,

    // Request Context (user-defined)
    pub request: R,

    // Additional Context
    pub additional: Option<Box<HashMap<SmolStr, SmolStr>>>,
}

/// Inherent constructor for the "no extra fields" case. Using a concrete
/// impl block lets `LogContext::new()` resolve without type annotation when
/// the surrounding code doesn't already constrain `R`.
impl LogContext<()> {
    /// Creates an empty context with no request-context schema (`R = ()`).
    ///
    /// Equivalent to `LogContext::<()>::default()`, but compiles even when
    /// type inference can't pick `R` from context.
    pub fn empty() -> Self {
        Self::default()
    }
}

impl<R: RequestFields> LogContext<R> {
    /// Creates empty context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the trace ID (from fast-telemetry span context).
    pub fn with_trace_id(mut self, trace_id: impl Into<SmolStr>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    /// Sets the span ID (from fast-telemetry span context).
    pub fn with_span_id(mut self, span_id: impl Into<SmolStr>) -> Self {
        self.span_id = Some(span_id.into());
        self
    }

    /// Sets feature name (e.g., "auth", "database").
    pub fn with_feature(mut self, feature: impl Into<SmolStr>) -> Self {
        self.feature = Some(feature.into());
        self
    }

    /// Sets function name.
    pub fn with_function(mut self, function: impl Into<SmolStr>) -> Self {
        self.function = Some(function.into());
        self
    }

    /// Sets error code (e.g., "E0A01").
    pub fn with_error_code(mut self, code: impl Into<SmolStr>) -> Self {
        self.error_code = Some(code.into());
        self
    }

    /// Sets error category (e.g., "Database", "Auth").
    pub fn with_error_category(mut self, category: impl Into<SmolStr>) -> Self {
        self.error_category = Some(category.into());
        self
    }

    /// Replaces the request-context block with a fresh value.
    pub fn with_request(mut self, request: R) -> Self {
        self.request = request;
        self
    }

    /// Mutate the request-context block in place via a closure.
    pub fn map_request(mut self, f: impl FnOnce(R) -> R) -> Self {
        self.request = f(self.request);
        self
    }

    /// Adds custom key-value field.
    pub fn with_additional(mut self, key: impl Into<SmolStr>, value: impl Into<SmolStr>) -> Self {
        self.additional.get_or_insert_with(|| Box::new(HashMap::new())).insert(key.into(), value.into());
        self
    }

    /// Clears the span_id to start a fresh span while keeping the same trace_id.
    ///
    /// Use this in long-running loops where you want each iteration to appear as
    /// a sibling span rather than deeply nested under the original parent span.
    /// This prevents trace trees from becoming excessively deep when processing
    /// many operations in a continuous loop.
    ///
    /// # Example
    /// ```ignore
    /// let ctx = ctx_with_trace!().with_feature("proxy");
    /// loop {
    ///     // Each iteration gets a fresh span under the same trace
    ///     let iter_ctx = ctx.clone().with_fresh_span();
    ///     // ... process command ...
    /// }
    /// ```
    pub fn with_fresh_span(mut self) -> Self {
        // Clear span_id so this operation appears as a new root-level span
        // under the same trace, rather than nested under the original parent
        self.span_id = None;
        self
    }

    /// Merges another context, overriding existing fields.
    pub fn merge(mut self, other: LogContext<R>) -> Self {
        if other.trace_id.is_some() {
            self.trace_id = other.trace_id;
        }
        if other.span_id.is_some() {
            self.span_id = other.span_id;
        }
        if other.feature.is_some() {
            self.feature = other.feature;
        }
        if other.function.is_some() {
            self.function = other.function;
        }
        if other.error_code.is_some() {
            self.error_code = other.error_code;
        }
        if other.error_category.is_some() {
            self.error_category = other.error_category;
        }
        self.request.merge(other.request);
        if let Some(other_map) = other.additional {
            self.additional.get_or_insert_with(|| Box::new(HashMap::new())).extend(*other_map);
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_builder() {
        let ctx = LogContext::<()>::new().with_feature("test").with_additional("key", "value");

        assert_eq!(ctx.feature.as_deref(), Some("test"));
        assert_eq!(ctx.additional.as_ref().and_then(|m| m.get("key")).map(|s| s.as_str()), Some("value"));
    }

    #[test]
    fn test_context_merge() {
        let ctx1 = LogContext::<()>::new().with_feature("feature1");
        let ctx2 = LogContext::<()>::new().with_error_code("E001");

        let merged = ctx1.merge(ctx2);

        assert_eq!(merged.feature.as_deref(), Some("feature1"));
        assert_eq!(merged.error_code.as_deref(), Some("E001"));
    }

    #[test]
    fn test_context_merge_override() {
        let ctx1 = LogContext::<()>::new().with_feature("feature1");
        let ctx2 = LogContext::<()>::new().with_feature("feature2");

        let merged = ctx1.merge(ctx2);

        assert_eq!(merged.feature.as_deref(), Some("feature2"));
    }

    #[test]
    fn test_audience_display() {
        assert_eq!(LogAudience::Internal.to_string(), "INTERNAL");
        assert_eq!(LogAudience::Client.to_string(), "CLIENT");
        assert_eq!(LogAudience::Both.to_string(), "BOTH");
    }

    #[test]
    fn test_with_fresh_span() {
        let ctx = LogContext::<()>::new().with_trace_id("trace-123").with_span_id("span-456").with_feature("test");

        let fresh = ctx.with_fresh_span();

        // trace_id should be preserved
        assert_eq!(fresh.trace_id.as_deref(), Some("trace-123"));
        // span_id should be cleared
        assert!(fresh.span_id.is_none());
        // other fields should be preserved
        assert_eq!(fresh.feature.as_deref(), Some("test"));
    }
}
