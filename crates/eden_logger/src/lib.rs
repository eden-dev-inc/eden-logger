#![cfg_attr(test, allow(clippy::unwrap_used))]
//! High-performance structured logging with fast-telemetry trace integration.
//!
//! This crate provides a fast, feature-gated logging system with compile-time log stripping,
//! colored output, and automatic trace context extraction. Logs are written directly to
//! stderr/stdout
//!
//! # Quick Start
//!
//! ```rust
//! use eden_logger::{log_info, ctx_with_trace, LogAudience, LogTarget};
//! use function_name::named;
//!
//! // Basic logging with automatic trace context
//! #[named]
//! fn my_function() {
//!     let ctx = ctx_with_trace!();
//!     log_info!(ctx, "Server started", audience = LogAudience::Internal);
//! }
//! ```
//!
//! # Feature Flags (Compile-Time Filtering)
//!
//! Logging can be controlled at compile-time using feature flags to strip unused logs
//! from the binary completely. This provides zero-cost abstraction for disabled logs.
//!
//! ## Log Levels
//! - `log-trace`: Enable trace-level logs
//! - `log-debug`: Enable debug-level logs
//! - `log-info`: Enable info-level logs
//! - `log-warn`: Enable warning-level logs
//! - `log-error`: Enable error-level logs
//!
//! ## Log Audiences
//! - `log-internal`: Enable internal logs
//! - `log-client`: Enable client-facing logs
//! - `log-both`: Enable logs for both audiences
//!
//! **Important**: A log requires BOTH a level feature AND an audience feature to be compiled.
//! For example, `log_info!(ctx, "msg", audience = LogAudience::Client)` requires both
//! `log-info` and `log-client` features enabled.
//!
//! # Runtime Filtering
//!
//! ## Environment Variable
//!
//! Use `EDEN_LOG_LEVEL` to enable specific log levels at runtime:
//!
//! ```bash
//! # Only show info and warn logs
//! EDEN_LOG_LEVEL=info;warn cargo run
//!
//! # Only show error logs
//! EDEN_LOG_LEVEL=error cargo run
//!
//! # Show all compiled logs (default - no env var set)
//! cargo run
//! ```
//!
//! ## Programmatic API
//!
//! You can also control filtering programmatically:
//!
//! ```rust
//! use eden_logger::{enable_levels, disable_levels, clear_filter, LogLevel};
//!
//! // Only emit info and warn logs
//! enable_levels(&[LogLevel::Info, LogLevel::Warn]);
//!
//! // Disable warn logs
//! disable_levels(&[LogLevel::Warn]);
//!
//! // Clear all filters (allow all levels)
//! clear_filter();
//! ```
//!
//! **How it works together**:
//! - Compile-time features: Remove code completely (zero cost)
//! - Runtime filter: Control what gets emitted from compiled code (~1ns overhead per log call)
//! - If `EDEN_LOG_LEVEL` is not set, all compiled logs are emitted
//! - If set, only the listed levels are emitted
//!
//! # Cargo.toml Configuration
//!
//! ```toml
//!
//! # Custom configuration - info/warn/error for both internal and client
//! eden_logger = {
//!     version = "0.1",
//!     default-features = false,
//!     features = ["log-info", "log-warn", "log-error", "log-internal", "log-client"]
//! }
//! ```
//!
//! # Basic Usage
//! ## Creating Log Context
//!
//! ```rust
//! use eden_logger::{ctx_with_trace, LogContext};
//! use function_name::named;
//!
//! // Automatic trace context with function name (recommended)
//! #[named]
//! fn my_function() {
//!     let ctx = ctx_with_trace!();  // Extracts trace_id, span_id, and function name
//!     // ... use ctx for logging
//! }
//!
//! // Manual context creation
//! fn manual_context() {
//!     let ctx = LogContext::<()>::default()
//!         .with_feature("auth")
//!         .with_function("login");
//! }
//! ```
//!
//! ## Logging Messages
//!
//! ```rust
//! use eden_logger::{log_info, log_warn, log_error, ctx_with_trace, LogAudience};
//! use function_name::named;
//!
//! #[named]
//! fn example() {
//!     let ctx = ctx_with_trace!();
//!
//!     // Basic log (internal audience)
//!     log_info!(ctx.clone(), "Server started", audience = LogAudience::Internal);
//!
//!     // Log with additional metadata
//!     log_info!(
//!         ctx.clone(),
//!         "User login",
//!         audience = LogAudience::Internal,
//!         user_id = "123",   // Additional key-value pairs
//!         ip_address = "192.168.1.1"  // Additional key-value pairs
//!     );
//!
//!     // Client-facing log (will be sent to API response)
//!     log_error!(ctx.clone(), "Invalid credentials", audience = LogAudience::Client);
//!
//!     // Log for both audiences
//!     log_warn!(ctx, "Rate limit approaching", audience = LogAudience::Both);
//! }
//! ```
//!
//! # Log Audiences
//!
//! The logging system supports three audience types to control who sees each log:
//!
//! - **Internal**: Logs for operators/developers only. Not sent to API clients.
//!   Use for debugging, performance metrics, internal state changes.
//!
//! - **Client**: Logs sent to API responses. Use for user-facing errors and warnings
//!   that help clients understand what went wrong.
//!
//! - **Both**: Critical events that both operators and clients need to see.
//!
//! ```rust
//! use eden_logger::{log_info, log_error, LogContext, LogAudience};
//!
//! let ctx = LogContext::empty();
//!
//! // Internal: debugging info
//! log_info!(ctx.clone(), "Cache hit", audience = LogAudience::Internal);
//!
//! // Client: user-facing error
//! log_error!(ctx.clone(), "Invalid email format", audience = LogAudience::Client);
//!
//! // Both: critical system event
//! log_error!(ctx, "Database connection lost", audience = LogAudience::Both);
//! ```
//!
//! # Output Format
//!
//! Logs are formatted with colored output for easy reading:
//!
//! ```text
//! [1735603200000] [INFO] [INTERNAL] trace_id=abc123 fn=my_function Server started
//! [1735603201000] [WARN] [CLIENT] Database slow query detected
//! [1735603202000] [ERROR] [BOTH] trace_id=def456 span_id=789 Connection timeout
//! ```
//!
//! Colors:
//! - TRACE: Magenta
//! - DEBUG: Cyan
//! - INFO: Green
//! - WARN: Yellow
//! - ERROR: Red
//!
//! # Performance
//!
//! The direct writer approach provides excellent performance:
//! - Single log call + context extraction: ~128ns (2.4x faster than env_logger)
//! - 1000 log calls: ~89µs (3.2x faster than env_logger)
//! - Zero allocation for disabled logs (compile-time elimination)
//! - No buffering overhead
//! - Direct syscalls to stderr/stdout
//!
//! # Advanced Usage
//!
//! ## Application-Specific Fields
//!
//! [`LogContext<R>`] is generic over a [`RequestFields`] type that the
//! consuming application defines. Use it to thread identity fields — tenant,
//! user, request id, endpoint — through every log without baking them into
//! this crate. `R` defaults to `()` (no extra fields).
//!
//! Implement [`RequestFields`] on a struct of your own, then pass it via
//! [`LogContext::with_request`]:
//!
//! ```rust
//! use eden_logger::{FieldWriter, LogAudience, LogContext, RequestFields, log_info};
//! use smol_str::SmolStr;
//!
//! #[derive(Clone, Default)]
//! struct AppRequest {
//!     tenant_id: Option<SmolStr>,
//!     user_id: Option<SmolStr>,
//! }
//!
//! impl RequestFields for AppRequest {
//!     fn write_display(&self, w: &mut dyn FieldWriter) {
//!         if let Some(v) = &self.tenant_id { w.write_str("tenant", v); }
//!         if let Some(v) = &self.user_id   { w.write_str("user",   v); }
//!     }
//!     fn write_json(&self, w: &mut dyn FieldWriter) {
//!         if let Some(v) = &self.tenant_id { w.write_str("tenant_id", v); }
//!         if let Some(v) = &self.user_id   { w.write_str("user_id",   v); }
//!     }
//!     fn merge(&mut self, other: Self) {
//!         if other.tenant_id.is_some() { self.tenant_id = other.tenant_id; }
//!         if other.user_id.is_some()   { self.user_id   = other.user_id; }
//!     }
//! }
//!
//! let ctx = LogContext::<AppRequest>::new()
//!     .with_feature("api")
//!     .with_request(AppRequest {
//!         tenant_id: Some("tenant-123".into()),
//!         user_id:   Some("user-456".into()),
//!     });
//! log_info!(ctx, "Request received", audience = LogAudience::Internal);
//! ```
//!
//! `write_display` emits `key=value` pairs into the display output;
//! `write_json` emits `,"key":"value"` pairs into the JSON output. Field
//! keys, types, and ordering are entirely under your control. Fields that
//! are `None` should be skipped — no allocations, no empty strings.
//!
//! For ad-hoc metadata that doesn't warrant a typed field, prefer the
//! built-in `additional` map ([`LogContext::with_additional`]) or the
//! trailing `key = value` pairs in the log macros (see next section).
//!
//! ## Additional Metadata
//!
//! ```rust
//! use eden_logger::{log_info, ctx_with_trace, LogAudience};
//! use function_name::named;
//!
//! #[named]
//! fn process_request(request_id: &str, user: &str) {
//!     let ctx = ctx_with_trace!();
//!
//!     log_info!(
//!         ctx,
//!         "Processing request",
//!         audience = LogAudience::Internal,
//!         request_id = request_id,
//!         user = user,
//!         method = "POST",
//!         path = "/api/users"
//!     );
//! }
//! ```

pub mod context;
pub mod fields;
pub mod filter;
#[cfg(feature = "fast-telemetry-context")]
pub mod metrics;
pub mod schema;
#[cfg(feature = "serde")]
pub mod sink;
pub mod trace;
pub mod writer;

// Re-export proc macros from eden_logger_macros
pub use eden_logger_macros::{ctx_with_trace, log_debug, log_error, log_info, log_trace, log_warn};

pub use context::{LogAudience, LogContext};
pub use fields::{FieldWriter, RequestFields};
pub use filter::{clear_filter, disable_levels, enable_levels, init_from_env, init_from_value, should_log};
#[cfg(feature = "fast-telemetry-context")]
pub use metrics::{LogAudienceMetrics, LogLevelMetrics, LogMetricsSnapshot, log_metrics_snapshot, visit_log_metrics};
pub use schema::{EdenLog, LogLevel, emit_direct, write_display_direct, write_json_direct};
#[cfg(feature = "serde")]
pub use sink::install_sink;
pub use trace::{TraceContextExt, TraceSource, extract_trace_context, set_trace_source, trace_context, trace_source};
pub use writer::{LogFormat, LogTarget, WriterConfig, init};
