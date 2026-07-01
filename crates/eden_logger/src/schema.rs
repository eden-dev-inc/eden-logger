//! Log record schema with standardized fields for structured logging.

use crate::context::{LogAudience, LogContext};
use crate::fields::{FieldWriter, RequestFields};
use crate::trace::extract_trace_context;
use chrono::{DateTime, Datelike, Timelike, Utc};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{self, Write};

/// Log severity levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(rename_all = "UPPERCASE"))]
pub enum LogLevel {
    /// Detailed execution traces.
    Trace,
    /// Debug information.
    Debug,
    /// Informational messages.
    Info,
    /// Warning conditions.
    Warn,
    /// Error conditions.
    Error,
}

impl LogLevel {
    #[inline(always)]
    pub const fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Trace => "TRACE",
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
        }
    }

    /// Returns the log level with ANSI color codes
    #[inline(always)]
    pub const fn as_colored_str(&self) -> &'static str {
        match self {
            LogLevel::Trace => "\x1b[35mTRACE\x1b[0m", // Magenta
            LogLevel::Debug => "\x1b[36mDEBUG\x1b[0m", // Cyan
            LogLevel::Info => "\x1b[32mINFO\x1b[0m",   // Green
            LogLevel::Warn => "\x1b[33mWARN\x1b[0m",   // Yellow
            LogLevel::Error => "\x1b[31mERROR\x1b[0m", // Red
        }
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Trace => write!(f, "TRACE"),
            LogLevel::Debug => write!(f, "DEBUG"),
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Warn => write!(f, "WARN"),
            LogLevel::Error => write!(f, "ERROR"),
        }
    }
}

#[inline(always)]
fn effective_trace_context<R: RequestFields>(context: &LogContext<R>) -> (Option<SmolStr>, Option<SmolStr>) {
    if context.trace_id.is_some() && context.span_id.is_some() {
        return (context.trace_id.clone(), context.span_id.clone());
    }

    let (current_trace_id, current_span_id) = extract_trace_context();
    (context.trace_id.clone().or(current_trace_id), context.span_id.clone().or(current_span_id))
}

/// Structured log record with trace IDs, context fields, and audience classification.
///
/// Uses `SmolStr` for metadata fields to reduce allocations. Log messages use `String`
/// as they can be arbitrarily long.
///
/// Generic over `R: RequestFields` — the application supplies its own
/// request-context schema (tenant, user, endpoint, etc.).
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize),
    serde(bound(serialize = "R: Serialize", deserialize = "R: for<'d> Deserialize<'d>"))
)]
pub struct EdenLog<R: RequestFields = ()> {
    // Temporal
    pub timestamp: DateTime<Utc>,

    // Identity & Classification
    pub level: LogLevel,
    pub audience: LogAudience,
    pub message: String,

    // Tracing Integration
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub trace_id: Option<SmolStr>,
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub span_id: Option<SmolStr>,

    // Application Context
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub feature: Option<SmolStr>,
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub function: Option<SmolStr>,
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub file: Option<SmolStr>,
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub line: Option<u32>,

    // Request Context (user-defined)
    #[cfg_attr(feature = "serde", serde(flatten))]
    pub request: R,

    // Error Information
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub error_code: Option<SmolStr>,
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub error_category: Option<SmolStr>,

    // Additional Context
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "HashMap::is_empty"))]
    pub additional: HashMap<SmolStr, SmolStr>,
}

impl<R: RequestFields> EdenLog<R> {
    /// Creates a new log record from context.
    pub fn new(level: LogLevel, message: impl Into<String>, context: &LogContext<R>, audience: LogAudience) -> Self {
        let (trace_id, span_id) = effective_trace_context(context);
        Self {
            timestamp: Utc::now(),
            level,
            audience,
            message: message.into(),
            trace_id,
            span_id,
            feature: context.feature.clone(),
            function: context.function.clone(),
            file: None,
            line: None,
            request: context.request.clone(),
            error_code: context.error_code.clone(),
            error_category: context.error_category.clone(),
            additional: context.additional.as_deref().cloned().unwrap_or_default(),
        }
    }

    /// Sets the file and line number where the log was called.
    pub fn with_location(mut self, file: &'static str, line: u32) -> Self {
        self.file = Some(SmolStr::new_static(file));
        self.line = Some(line);
        self
    }

    /// Adds additional context field.
    pub fn with_additional(mut self, key: impl Into<SmolStr>, value: impl Into<SmolStr>) -> Self {
        self.additional.insert(key.into(), value.into());
        self
    }

    pub fn from_direct(
        level: LogLevel,
        message: &str,
        context: &LogContext<R>,
        audience: LogAudience,
        additional: &[(&str, &dyn std::fmt::Display)],
        file: Option<&str>,
        line: Option<u32>,
    ) -> Self {
        let mut log = Self::new(level, message, context, audience);
        log.file = file.map(SmolStr::new);
        log.line = line;
        for (key, value) in additional {
            log.additional.insert(SmolStr::new(*key), SmolStr::new(value.to_string()));
        }
        log
    }

    /// Serializes log to JSON string.
    ///
    /// Requires the `serde` feature. The hot logging path does **not** use this
    /// — it formats JSON via the zero-allocation `write_json_direct` writer.
    /// This method exists for callers who hold an `EdenLog` value and want
    /// serde-compatible output (e.g. piping into another serializer).
    #[cfg(feature = "serde")]
    pub fn to_json(&self) -> String
    where
        R: Serialize,
    {
        serde_json::to_string(self).unwrap_or_else(|e| format!("{{\"error\":\"Failed to serialize log: {}\"}}", e))
    }

    #[inline(always)]
    pub fn to_display(&self) -> String {
        let mut s = String::with_capacity(512);
        self.write_display(&mut s);
        s
    }

    /// Write the formatted log line into the provided buffer.
    ///
    /// Uses `push_str` for field formatting and a fast manual timestamp
    /// formatter (no chrono `to_rfc3339_opts`).
    #[inline(always)]
    pub fn write_display(&self, s: &mut String) {
        // Helper to write [value]
        macro_rules! bracket {
            ($val:expr) => {{
                s.push('[');
                s.push_str($val);
                s.push(']');
            }};
        }

        // Helper to write key=value if present
        macro_rules! field {
            ($label:literal, $opt:expr) => {
                if let Some(v) = &$opt {
                    s.push_str(concat!(" ", $label, "="));
                    s.push_str(v);
                }
            };
        }

        // Header: [timestamp] [level] [audience]
        s.push('[');
        write_timestamp_rfc3339(s, &self.timestamp);
        s.push(']');
        s.push(' ');
        bracket!(self.level.as_colored_str());
        s.push(' ');
        bracket!(self.audience.as_str());

        // Optional context fields
        field!("trace_id", self.trace_id);
        field!("span_id", self.span_id);
        field!("feature", self.feature);
        field!("fn", self.function);
        // Request fields contributed by R.
        let mut dw = DisplayFieldWriter { buf: s };
        self.request.write_display(&mut dw);
        field!("error", self.error_code);

        // Additional metadata
        for (k, v) in &self.additional {
            s.push(' ');
            s.push_str(k.as_str());
            s.push('=');
            s.push_str(v.as_str());
        }

        // Message
        s.push(' ');
        s.push_str(&self.message);

        // Source location (rustc style)
        if let Some(file) = &self.file {
            s.push('\n');
            s.push_str("   --> ");
            s.push_str(file);
            if let Some(line) = self.line {
                s.push(':');
                let mut buf = itoa::Buffer::new();
                s.push_str(buf.format(line));
            }
        }
    }

    /// Returns true if log should be sent to API clients.
    pub fn should_send_to_client(&self) -> bool {
        matches!(self.audience, LogAudience::Client | LogAudience::Both)
    }

    /// Outputs log via direct writer.
    ///
    /// Uses a thread-local buffer to avoid per-log heap allocation and
    /// writes the complete line (including newline) in a single syscall.
    pub fn emit(&self) {
        // Check runtime filter
        if !crate::filter::should_log(self.level) {
            return;
        }

        #[cfg(feature = "fast-telemetry-context")]
        crate::metrics::record_emitted(self.level, self.audience);

        thread_local! {
            static FMT_BUF: RefCell<String> = RefCell::new(String::with_capacity(1024));
        }

        FMT_BUF.with(|buf| {
            let mut buf = buf.borrow_mut();
            buf.clear();
            self.write_display(&mut buf);
            buf.push('\n');
            crate::writer::log_bytes(buf.as_bytes());
        });

        #[cfg(feature = "serde")]
        {
            crate::sink::dispatch::<R>(|| self.clone());
        }
    }
}

/// Zero-copy emit: formats and writes a log line directly from references,
/// without constructing an intermediate `EdenLog` struct.
///
/// Eliminates ~50ns of overhead per log call by avoiding:
/// - `Option<SmolStr>` copies from `LogContext`
/// - `String` allocation for the message
/// - `HashMap` clone for additional fields
/// - `EdenLog` struct initialization on the stack
///
/// Automatically uses JSON or display format based on runtime configuration.
#[inline(always)]
pub fn emit_direct<R: RequestFields>(
    level: LogLevel,
    message: &str,
    context: &LogContext<R>,
    audience: LogAudience,
    additional: &[(&str, &dyn std::fmt::Display)],
    file: Option<&str>,
    line: Option<u32>,
) {
    if !crate::filter::should_log(level) {
        return;
    }

    #[cfg(feature = "fast-telemetry-context")]
    crate::metrics::record_emitted(level, audience);

    thread_local! {
        static FMT_BUF: RefCell<String> = RefCell::new(String::with_capacity(1024));
    }

    FMT_BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        buf.clear();

        match crate::writer::format() {
            crate::writer::LogFormat::Json => {
                write_json_direct(&mut buf, level, message, context, audience, additional, file, line);
            }
            crate::writer::LogFormat::Display => {
                write_display_direct(&mut buf, level, message, context, audience, additional, file, line);
            }
        }

        buf.push('\n');
        crate::writer::log_bytes(buf.as_bytes());
    });

    // Sink dispatch is gated on the `serde` feature: storing/forwarding an
    // `EdenLog<R>` value requires `R: Serialize` in practice (typical sinks
    // forward to a serializer), so the whole sink subsystem rides on it.
    #[cfg(feature = "serde")]
    {
        crate::sink::dispatch::<R>(|| EdenLog::from_direct(level, message, context, audience, additional, file, line));
    }
}

/// Format a log line directly from references into the provided buffer.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
pub fn write_display_direct<R: RequestFields>(
    s: &mut String,
    level: LogLevel,
    message: &str,
    context: &LogContext<R>,
    audience: LogAudience,
    additional: &[(&str, &dyn std::fmt::Display)],
    file: Option<&str>,
    line: Option<u32>,
) {
    let (trace_id, span_id) = effective_trace_context(context);
    macro_rules! bracket {
        ($val:expr) => {{
            s.push('[');
            s.push_str($val);
            s.push(']');
        }};
    }

    macro_rules! field {
        ($label:literal, $opt:expr) => {
            if let Some(v) = &$opt {
                s.push_str(concat!(" ", $label, "="));
                s.push_str(v);
            }
        };
    }

    // Header: [timestamp] [level] [audience]
    s.push('[');
    write_timestamp_rfc3339_now_cached(s);
    s.push(']');
    s.push(' ');
    bracket!(level.as_colored_str());
    s.push(' ');
    bracket!(audience.as_str());

    // Context fields — read directly from &LogContext, zero copies
    field!("trace_id", trace_id);
    field!("span_id", span_id);
    field!("feature", context.feature);
    field!("fn", context.function);
    // Request fields contributed by R.
    let mut dw = DisplayFieldWriter { buf: s };
    context.request.write_display(&mut dw);
    field!("error", context.error_code);

    // Additional metadata from LogContext
    if let Some(map) = &context.additional {
        for (k, v) in map.as_ref() {
            s.push(' ');
            s.push_str(k.as_str());
            s.push('=');
            s.push_str(v.as_str());
        }
    }

    // Additional metadata from macro k=v pairs
    for (k, v) in additional {
        s.push(' ');
        s.push_str(k);
        s.push('=');
        use std::fmt::Write;
        let _ = write!(s, "{v}");
    }

    // Message
    s.push(' ');
    s.push_str(message);

    // Source location (rustc style)
    if let Some(file) = file {
        s.push('\n');
        s.push_str("   --> ");
        s.push_str(file);
        if let Some(line) = line {
            s.push(':');
            let mut buf = itoa::Buffer::new();
            s.push_str(buf.format(line));
        }
    }
}

/// `FieldWriter` impl for display output. Each field is written as ` key=value`.
struct DisplayFieldWriter<'a> {
    buf: &'a mut String,
}

impl FieldWriter for DisplayFieldWriter<'_> {
    #[inline(always)]
    fn write_str(&mut self, key: &str, value: &str) {
        self.buf.push(' ');
        self.buf.push_str(key);
        self.buf.push('=');
        self.buf.push_str(value);
    }
    // u64/i64/bool/display use the default impls, which route through
    // write_str. For display output that's correct (everything renders as
    // text after `=`).
}

/// `FieldWriter` impl for JSON output. Strings are written as
/// `,"key":"value"` (escaped); numbers and booleans are written unquoted
/// so they survive as their natural JSON types in downstream parsers.
struct JsonFieldWriter<'a> {
    buf: &'a mut String,
}

impl FieldWriter for JsonFieldWriter<'_> {
    #[inline(always)]
    fn write_str(&mut self, key: &str, value: &str) {
        self.buf.push_str(",\"");
        write_json_escaped(self.buf, key);
        self.buf.push_str("\":\"");
        write_json_escaped(self.buf, value);
        self.buf.push('"');
    }

    #[inline(always)]
    fn write_u64(&mut self, key: &str, value: u64) {
        self.buf.push_str(",\"");
        write_json_escaped(self.buf, key);
        self.buf.push_str("\":");
        let mut tmp = itoa::Buffer::new();
        self.buf.push_str(tmp.format(value));
    }

    #[inline(always)]
    fn write_i64(&mut self, key: &str, value: i64) {
        self.buf.push_str(",\"");
        write_json_escaped(self.buf, key);
        self.buf.push_str("\":");
        let mut tmp = itoa::Buffer::new();
        self.buf.push_str(tmp.format(value));
    }

    #[inline(always)]
    fn write_bool(&mut self, key: &str, value: bool) {
        self.buf.push_str(",\"");
        write_json_escaped(self.buf, key);
        self.buf.push_str("\":");
        self.buf.push_str(if value { "true" } else { "false" });
    }

    #[inline(always)]
    fn write_display(&mut self, key: &str, value: &dyn core::fmt::Display) {
        use core::fmt::Write;
        self.buf.push_str(",\"");
        write_json_escaped(self.buf, key);
        self.buf.push_str("\":\"");
        let mut esc = JsonEscapingWriter { out: self.buf };
        let _ = write!(&mut esc, "{value}");
        self.buf.push('"');
    }
}

/// Write an RFC 3339 timestamp with millisecond precision directly into a String.
///
/// Produces `YYYY-MM-DDTHH:MM:SS.mmmZ` without allocating an intermediate String
/// (chrono's `to_rfc3339_opts` allocates internally). Uses `itoa` for fast integer
/// formatting and manual zero-padding for fixed-width fields.
#[inline(always)]
fn write_timestamp_rfc3339(buf: &mut String, ts: &DateTime<Utc>) {
    let mut itoa_buf = itoa::Buffer::new();

    // Year (4 digits)
    let year = ts.year();
    if year < 1000 {
        // Zero-pad years < 1000 (unlikely but correct)
        for _ in 0..(4 - digit_count(year as u64)) {
            buf.push('0');
        }
    }
    buf.push_str(itoa_buf.format(year));
    buf.push('-');

    // Month (2 digits, zero-padded)
    let month = ts.month();
    if month < 10 {
        buf.push('0');
    }
    buf.push_str(itoa_buf.format(month));
    buf.push('-');

    // Day (2 digits, zero-padded)
    let day = ts.day();
    if day < 10 {
        buf.push('0');
    }
    buf.push_str(itoa_buf.format(day));
    buf.push('T');

    // Hour (2 digits, zero-padded)
    let hour = ts.hour();
    if hour < 10 {
        buf.push('0');
    }
    buf.push_str(itoa_buf.format(hour));
    buf.push(':');

    // Minute (2 digits, zero-padded)
    let min = ts.minute();
    if min < 10 {
        buf.push('0');
    }
    buf.push_str(itoa_buf.format(min));
    buf.push(':');

    // Second (2 digits, zero-padded)
    let sec = ts.second();
    if sec < 10 {
        buf.push('0');
    }
    buf.push_str(itoa_buf.format(sec));
    buf.push('.');

    // Milliseconds (3 digits, zero-padded)
    let millis = ts.timestamp_subsec_millis();
    if millis < 100 {
        buf.push('0');
    }
    if millis < 10 {
        buf.push('0');
    }
    buf.push_str(itoa_buf.format(millis));
    buf.push('Z');
}

/// Thread-local timestamp cache to avoid rebuilding RFC3339 strings for
/// multiple logs emitted in the same millisecond.
struct TimestampCache {
    last_millis: i64,
    formatted: String,
}

impl TimestampCache {
    fn new() -> Self {
        Self {
            last_millis: i64::MIN,
            // RFC3339 with millis is fixed width: YYYY-MM-DDTHH:MM:SS.mmmZ
            formatted: String::with_capacity(24),
        }
    }
}

#[inline(always)]
fn write_timestamp_rfc3339_now_cached(buf: &mut String) {
    let now = Utc::now();
    let millis = now.timestamp_millis();

    thread_local! {
        static TS_CACHE: RefCell<TimestampCache> = RefCell::new(TimestampCache::new());
    }

    TS_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.last_millis != millis {
            cache.last_millis = millis;
            cache.formatted.clear();
            write_timestamp_rfc3339(&mut cache.formatted, &now);
        }
        buf.push_str(&cache.formatted);
    });
}

#[inline(always)]
const fn digit_count(n: u64) -> u32 {
    match n {
        0..=9 => 1,
        10..=99 => 2,
        100..=999 => 3,
        _ => 4,
    }
}

/// Writes a JSON-escaped string value into the buffer.
///
/// Escapes: `"`, `\`, and control characters (0x00-0x1F).
/// Uses a lookup table for fast escape detection.
#[inline(always)]
pub(crate) fn write_json_escaped(buf: &mut String, s: &str) {
    // Fast path: check if any escaping needed
    let bytes = s.as_bytes();
    let needs_escape = bytes.iter().any(|&b| JSON_ESCAPE_TABLE[b as usize]);

    if !needs_escape {
        buf.push_str(s);
        return;
    }

    // Slow path: escape character by character
    for &b in bytes {
        match b {
            b'"' => buf.push_str(r#"\""#),
            b'\\' => buf.push_str(r"\\"),
            b'\n' => buf.push_str(r"\n"),
            b'\r' => buf.push_str(r"\r"),
            b'\t' => buf.push_str(r"\t"),
            0x00..=0x1F => {
                // Other control characters: \u00XX
                buf.push_str(r"\u00");
                buf.push(HEX_CHARS[(b >> 4) as usize] as char);
                buf.push(HEX_CHARS[(b & 0x0F) as usize] as char);
            }
            _ => buf.push(b as char),
        }
    }
}

/// `fmt::Write` adapter that JSON-escapes text directly into `out`.
///
/// This avoids temporary `String` allocations when formatting `Display` values
/// for JSON output.
struct JsonEscapingWriter<'a> {
    out: &'a mut String,
}

impl fmt::Write for JsonEscapingWriter<'_> {
    #[inline(always)]
    fn write_str(&mut self, s: &str) -> fmt::Result {
        write_json_escaped(self.out, s);
        Ok(())
    }

    #[inline(always)]
    fn write_char(&mut self, c: char) -> fmt::Result {
        let mut tmp = [0u8; 4];
        let encoded = c.encode_utf8(&mut tmp);
        write_json_escaped(self.out, encoded);
        Ok(())
    }
}

/// Hex lookup for \u00XX escapes
const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

/// Lookup table: true if byte needs JSON escaping
const JSON_ESCAPE_TABLE: [bool; 256] = {
    let mut table = [false; 256];
    let mut i = 0;
    while i < 32 {
        table[i] = true; // Control characters 0x00-0x1F
        i += 1;
    }
    table[b'"' as usize] = true;
    table[b'\\' as usize] = true;
    table
};

/// Write a complete JSON log line directly into the buffer.
///
/// No serde, no allocations beyond the pre-allocated buffer.
/// Target performance: <100ns for typical log lines.
///
/// This function is public for benchmarking purposes.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
pub fn write_json_direct<R: RequestFields>(
    s: &mut String,
    level: LogLevel,
    message: &str,
    context: &LogContext<R>,
    audience: LogAudience,
    additional: &[(&str, &dyn std::fmt::Display)],
    file: Option<&str>,
    line: Option<u32>,
) {
    let (trace_id, span_id) = effective_trace_context(context);
    // Start JSON object with required fields
    s.push_str(r#"{"ts":""#);
    write_timestamp_rfc3339_now_cached(s);
    s.push_str(r#"","level":""#);
    s.push_str(level.as_str());
    s.push_str(r#"","audience":""#);
    s.push_str(audience.as_str());
    s.push('"');

    // Optional context fields - only include if present
    macro_rules! json_field {
        ($key:literal, $opt:expr) => {
            if let Some(v) = &$opt {
                s.push_str(concat!(r#",""#, $key, r#"":""#));
                write_json_escaped(s, v);
                s.push('"');
            }
        };
    }

    json_field!("trace_id", trace_id);
    json_field!("span_id", span_id);
    json_field!("feature", context.feature);
    json_field!("fn", context.function);
    // Request fields contributed by R.
    {
        let mut jw = JsonFieldWriter { buf: s };
        context.request.write_json(&mut jw);
    }
    json_field!("error_code", context.error_code);
    json_field!("error_category", context.error_category);

    // Additional metadata from LogContext
    if let Some(map) = &context.additional {
        for (k, v) in map.as_ref() {
            s.push_str(r#",""#);
            write_json_escaped(s, k.as_str());
            s.push_str(r#"":""#);
            write_json_escaped(s, v.as_str());
            s.push('"');
        }
    }

    // Additional metadata from macro k=v pairs
    for (k, v) in additional {
        s.push_str(r#",""#);
        write_json_escaped(s, k);
        s.push_str(r#"":""#);
        let mut escaping_writer = JsonEscapingWriter { out: s };
        let _ = write!(&mut escaping_writer, "{v}");
        s.push('"');
    }

    // Source location
    if let Some(f) = file {
        s.push_str(r#","file":""#);
        write_json_escaped(s, f);
        s.push('"');
        if let Some(l) = line {
            s.push_str(r#","line":"#);
            let mut buf = itoa::Buffer::new();
            s.push_str(buf.format(l));
        }
    }

    // Message (last, as it's most likely to need escaping)
    s.push_str(r#","msg":""#);
    write_json_escaped(s, message);
    s.push_str(r#""}"#);
}

impl<R: RequestFields> std::fmt::Display for EdenLog<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_display())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "fast-telemetry-context")]
    fn active_fast_trace_ids_for_test() -> (String, String) {
        crate::trace::set_trace_source(crate::trace::TraceSource::FastTelemetry);
        let (trace_id, span_id) = crate::trace::extract_trace_context();
        let Some(trace_id) = trace_id else {
            panic!("expected active fast-telemetry trace id");
        };
        let Some(span_id) = span_id else {
            panic!("expected active fast-telemetry span id");
        };
        (trace_id.to_string(), span_id.to_string())
    }

    #[test]
    fn test_log_level_display() {
        assert_eq!(LogLevel::Info.to_string(), "INFO");
        assert_eq!(LogLevel::Error.to_string(), "ERROR");
    }

    #[test]
    fn test_log_creation() {
        let ctx = LogContext::<()>::new().with_feature("test");

        let log = EdenLog::<()>::new(LogLevel::Info, "Test message", &ctx, LogAudience::Internal);

        assert_eq!(log.level, LogLevel::Info);
        assert_eq!(log.message, "Test message");
        assert_eq!(log.feature.as_deref(), Some("test"));
    }

    #[cfg(feature = "fast-telemetry-context")]
    #[test]
    fn test_log_creation_uses_active_trace_context_when_missing() {
        let collector = std::sync::Arc::new(fast_telemetry::SpanCollector::new(1, 1024));
        let mut span = collector.start_span("log-correlation", fast_telemetry::SpanKind::Server);
        span.enter();
        let (trace_id, span_id) = active_fast_trace_ids_for_test();

        let ctx = LogContext::<()>::new().with_feature("gateway");
        let log = EdenLog::<()>::new(LogLevel::Info, "request handled", &ctx, LogAudience::Internal);

        assert_eq!(log.trace_id.as_deref(), Some(trace_id.as_str()));
        assert_eq!(log.span_id.as_deref(), Some(span_id.as_str()));
    }

    #[cfg(feature = "fast-telemetry-context")]
    #[test]
    fn test_explicit_log_context_trace_ids_win_over_active_span() {
        let collector = std::sync::Arc::new(fast_telemetry::SpanCollector::new(1, 1024));
        let mut span = collector.start_span("log-correlation", fast_telemetry::SpanKind::Server);
        span.enter();
        let _ = active_fast_trace_ids_for_test();

        let ctx = LogContext::<()>::new().with_trace_id("upstream-trace-id").with_span_id("upstream-span-id");
        let log = EdenLog::<()>::new(LogLevel::Info, "request handled", &ctx, LogAudience::Internal);

        assert_eq!(log.trace_id.as_deref(), Some("upstream-trace-id"));
        assert_eq!(log.span_id.as_deref(), Some("upstream-span-id"));
    }

    #[test]
    fn test_log_with_additional() {
        let ctx = LogContext::<()>::new();
        let log = EdenLog::<()>::new(LogLevel::Info, "Test", &ctx, LogAudience::Internal)
            .with_additional("key1", "value1")
            .with_additional("key2", "value2");

        assert_eq!(log.additional.get("key1").map(|s| s.as_str()), Some("value1"));
        assert_eq!(log.additional.get("key2").map(|s| s.as_str()), Some("value2"));
    }

    #[test]
    fn test_should_send_to_client() {
        let ctx = LogContext::<()>::new();

        let internal_log = EdenLog::<()>::new(LogLevel::Info, "Internal", &ctx, LogAudience::Internal);
        assert!(!internal_log.should_send_to_client());

        let client_log = EdenLog::<()>::new(LogLevel::Info, "Client", &ctx, LogAudience::Client);
        assert!(client_log.should_send_to_client());

        let both_log = EdenLog::<()>::new(LogLevel::Info, "Both", &ctx, LogAudience::Both);
        assert!(both_log.should_send_to_client());
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_json_serialization() {
        let ctx = LogContext::<()>::new().with_feature("test").with_trace_id("trace123").with_span_id("span456");

        let log = EdenLog::<()>::new(LogLevel::Error, "Error occurred", &ctx, LogAudience::Client).with_additional("detail", "test detail");

        let json = log.to_json();
        assert!(json.contains("\"message\":\"Error occurred\""));
        assert!(json.contains("\"trace_id\":\"trace123\""));
        assert!(json.contains("\"span_id\":\"span456\""));
        assert!(json.contains("\"feature\":\"test\""));
    }

    // ========================================================================
    // JSON escape tests
    // ========================================================================

    #[test]
    fn test_json_escape_no_special_chars() {
        let mut buf = String::new();
        write_json_escaped(&mut buf, "hello world");
        assert_eq!(buf, "hello world");
    }

    #[test]
    fn test_json_escape_quotes() {
        let mut buf = String::new();
        write_json_escaped(&mut buf, r#"say "hello""#);
        assert_eq!(buf, r#"say \"hello\""#);
    }

    #[test]
    fn test_json_escape_backslash() {
        let mut buf = String::new();
        write_json_escaped(&mut buf, r"path\to\file");
        assert_eq!(buf, r"path\\to\\file");
    }

    #[test]
    fn test_json_escape_newlines() {
        let mut buf = String::new();
        write_json_escaped(&mut buf, "line1\nline2\r\nline3");
        assert_eq!(buf, r"line1\nline2\r\nline3");
    }

    #[test]
    fn test_json_escape_tabs() {
        let mut buf = String::new();
        write_json_escaped(&mut buf, "col1\tcol2");
        assert_eq!(buf, r"col1\tcol2");
    }

    #[test]
    fn test_json_escape_control_chars() {
        let mut buf = String::new();
        write_json_escaped(&mut buf, "null:\x00 bell:\x07");
        assert_eq!(buf, r"null:\u0000 bell:\u0007");
    }

    #[test]
    fn test_json_escape_mixed() {
        let mut buf = String::new();
        write_json_escaped(&mut buf, "He said \"hello\"\nand left\\departed");
        assert_eq!(buf, r#"He said \"hello\"\nand left\\departed"#);
    }

    // ========================================================================
    // write_json_direct tests - compare with serde
    //
    // These verify the manual JSON writer's output by parsing it through
    // serde_json. Gated on `serde` for the parser dependency only — the
    // production code being tested (`write_json_direct`) is always available.
    // ========================================================================

    #[cfg(feature = "fast-telemetry-context")]
    #[test]
    fn test_write_display_direct_uses_active_trace_context_when_missing() {
        let collector = std::sync::Arc::new(fast_telemetry::SpanCollector::new(1, 1024));
        let mut span = collector.start_span("display-log-correlation", fast_telemetry::SpanKind::Server);
        span.enter();
        let (trace_id, span_id) = active_fast_trace_ids_for_test();

        let ctx = LogContext::<()>::new().with_feature("gateway");
        let mut buf = String::new();

        write_display_direct(&mut buf, LogLevel::Info, "request handled", &ctx, LogAudience::Internal, &[], None, None);

        assert!(buf.contains(&format!("trace_id={trace_id}")));
        assert!(buf.contains(&format!("span_id={span_id}")));
    }

    /// Helper to parse JSON and compare field values
    #[cfg(feature = "serde")]
    fn parse_json_field(json: &str, field: &str) -> Option<String> {
        let parsed: serde_json::Value = serde_json::from_str(json).ok()?;
        parsed.get(field).and_then(|v| v.as_str()).map(|s| s.to_string())
    }

    #[cfg(feature = "serde")]
    fn parse_json_field_u64(json: &str, field: &str) -> Option<u64> {
        let parsed: serde_json::Value = serde_json::from_str(json).ok()?;
        parsed.get(field).and_then(|v| v.as_u64())
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_write_json_direct_basic() {
        let ctx = LogContext::<()>::new();
        let mut buf = String::new();

        write_json_direct(&mut buf, LogLevel::Info, "test message", &ctx, LogAudience::Internal, &[], None, None);

        // Verify it's valid JSON
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(&buf);
        assert!(parsed.is_ok(), "Invalid JSON: {}", buf);

        // Verify fields
        assert_eq!(parse_json_field(&buf, "level"), Some("INFO".to_string()));
        assert_eq!(parse_json_field(&buf, "audience"), Some("INTERNAL".to_string()));
        assert_eq!(parse_json_field(&buf, "msg"), Some("test message".to_string()));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_write_json_direct_with_context() {
        let ctx = LogContext::<()>::new()
            .with_trace_id("abc123def456")
            .with_span_id("span789")
            .with_feature("auth")
            .with_function("login");

        let mut buf = String::new();

        write_json_direct(&mut buf, LogLevel::Error, "Authentication failed", &ctx, LogAudience::Client, &[], None, None);

        // Verify valid JSON
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(&buf);
        assert!(parsed.is_ok(), "Invalid JSON: {}", buf);

        // Verify all context fields
        assert_eq!(parse_json_field(&buf, "trace_id"), Some("abc123def456".to_string()));
        assert_eq!(parse_json_field(&buf, "span_id"), Some("span789".to_string()));
        assert_eq!(parse_json_field(&buf, "feature"), Some("auth".to_string()));
        assert_eq!(parse_json_field(&buf, "fn"), Some("login".to_string()));
    }

    #[cfg(all(feature = "fast-telemetry-context", feature = "serde"))]
    #[test]
    fn test_write_json_direct_uses_active_trace_context_when_missing() {
        let collector = std::sync::Arc::new(fast_telemetry::SpanCollector::new(1, 1024));
        let mut span = collector.start_span("json-log-correlation", fast_telemetry::SpanKind::Server);
        span.enter();
        let (trace_id, span_id) = active_fast_trace_ids_for_test();

        let ctx = LogContext::<()>::new().with_feature("gateway");
        let mut buf = String::new();

        write_json_direct(&mut buf, LogLevel::Info, "request handled", &ctx, LogAudience::Internal, &[], None, None);

        assert_eq!(parse_json_field(&buf, "trace_id"), Some(trace_id));
        assert_eq!(parse_json_field(&buf, "span_id"), Some(span_id));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_write_json_direct_with_additional() {
        let ctx = LogContext::<()>::new().with_additional("request_id", "req-123").with_additional("method", "POST");

        let mut buf = String::new();

        write_json_direct(
            &mut buf,
            LogLevel::Info,
            "Request processed",
            &ctx,
            LogAudience::Internal,
            &[("status", &200), ("path", &"/api/users")],
            None,
            None,
        );

        let parsed: Result<serde_json::Value, _> = serde_json::from_str(&buf);
        assert!(parsed.is_ok(), "Invalid JSON: {}", buf);

        // Context additional fields
        assert_eq!(parse_json_field(&buf, "request_id"), Some("req-123".to_string()));
        assert_eq!(parse_json_field(&buf, "method"), Some("POST".to_string()));

        // Macro additional fields
        assert_eq!(parse_json_field(&buf, "status"), Some("200".to_string()));
        assert_eq!(parse_json_field(&buf, "path"), Some("/api/users".to_string()));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_write_json_direct_with_file_location() {
        let ctx = LogContext::<()>::new();
        let mut buf = String::new();

        write_json_direct(
            &mut buf,
            LogLevel::Warn,
            "Deprecation warning",
            &ctx,
            LogAudience::Internal,
            &[],
            Some("src/api/handler.rs"),
            Some(42),
        );

        let parsed: Result<serde_json::Value, _> = serde_json::from_str(&buf);
        assert!(parsed.is_ok(), "Invalid JSON: {}", buf);

        assert_eq!(parse_json_field(&buf, "file"), Some("src/api/handler.rs".to_string()));
        assert_eq!(parse_json_field_u64(&buf, "line"), Some(42));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_write_json_direct_escapes_message() {
        let ctx = LogContext::<()>::new();
        let mut buf = String::new();

        write_json_direct(
            &mut buf,
            LogLevel::Error,
            "Error: \"file not found\"\nPath: C:\\Users\\test",
            &ctx,
            LogAudience::Internal,
            &[],
            None,
            None,
        );

        // Must be valid JSON despite special characters
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(&buf);
        assert!(parsed.is_ok(), "Invalid JSON with special chars: {}", buf);

        // Verify the message is correctly escaped and parsed
        let msg = parse_json_field(&buf, "msg").unwrap();
        assert_eq!(msg, "Error: \"file not found\"\nPath: C:\\Users\\test");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_write_json_direct_escapes_context_fields() {
        let ctx = LogContext::<()>::new().with_feature("test\"feature").with_additional("path", "C:\\Program Files\\App");

        let mut buf = String::new();

        write_json_direct(&mut buf, LogLevel::Info, "test", &ctx, LogAudience::Internal, &[], None, None);

        let parsed: Result<serde_json::Value, _> = serde_json::from_str(&buf);
        assert!(parsed.is_ok(), "Invalid JSON with escaped context: {}", buf);

        assert_eq!(parse_json_field(&buf, "feature"), Some("test\"feature".to_string()));
        assert_eq!(parse_json_field(&buf, "path"), Some("C:\\Program Files\\App".to_string()));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_write_json_direct_timestamp_format() {
        let ctx = LogContext::<()>::new();
        let mut buf = String::new();

        write_json_direct(&mut buf, LogLevel::Info, "test", &ctx, LogAudience::Internal, &[], None, None);

        let ts = parse_json_field(&buf, "ts").unwrap();

        // Verify RFC3339 format: YYYY-MM-DDTHH:MM:SS.mmmZ
        assert!(ts.len() == 24, "Timestamp wrong length: {} ({})", ts, ts.len());
        assert!(ts.ends_with('Z'), "Timestamp should end with Z: {}", ts);
        assert!(ts.contains('T'), "Timestamp should contain T: {}", ts);

        // Verify it parses as a valid timestamp
        let parsed = chrono::DateTime::parse_from_rfc3339(&ts);
        assert!(parsed.is_ok(), "Invalid RFC3339 timestamp: {}", ts);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_write_json_omits_none_fields() {
        let ctx = LogContext::<()>::new(); // All fields None
        let mut buf = String::new();

        write_json_direct(&mut buf, LogLevel::Info, "test", &ctx, LogAudience::Internal, &[], None, None);

        // Should NOT contain optional fields when they're None
        assert!(!buf.contains("trace_id"), "Should not contain trace_id when None");
        assert!(!buf.contains("span_id"), "Should not contain span_id when None");
        assert!(!buf.contains("feature"), "Should not contain feature when None");
    }
}
