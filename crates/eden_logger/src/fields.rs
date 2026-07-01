//! User-extensible request-context fields for `LogContext`.
//!
//! Consumers define a struct (typically via `#[derive(RequestFields)]`)
//! holding application-specific identity fields — tenant, user, endpoint, etc.
//! The logger calls into the trait during the zero-allocation hot path to
//! emit these fields into the display and JSON outputs.
//!
//! The default impl for `()` writes nothing, so callers that don't need an
//! extension schema can use `LogContext` (which defaults `R = ()`) unchanged.

/// Sink that field implementors push key-value pairs into.
///
/// `eden_logger` provides two writers internally: a display writer that emits
/// `key=value` pairs prefixed with a space, and a JSON writer that emits
/// `,"key":"value"` (with the leading comma since required JSON fields are
/// always written before the request-context block).
///
/// The `write_*` methods cover the common scalar kinds. All numeric and
/// boolean writes go through stack-buffered formatters — they never
/// allocate. `write_display` is provided as a fallback for arbitrary
/// `Display` types and goes through a tiny per-call `fmt::Write` adapter
/// that the JSON variant uses to escape on the fly.
pub trait FieldWriter {
    /// Write a string-valued field.
    fn write_str(&mut self, key: &str, value: &str);

    /// Write an unsigned integer field.
    ///
    /// Default impl formats via `itoa` into a stack buffer and forwards to
    /// `write_str`. Implementations may override for a direct numeric
    /// representation (e.g. an unquoted number in JSON output).
    #[inline]
    fn write_u64(&mut self, key: &str, value: u64) {
        let mut buf = itoa::Buffer::new();
        self.write_str(key, buf.format(value));
    }

    /// Write a signed integer field.
    #[inline]
    fn write_i64(&mut self, key: &str, value: i64) {
        let mut buf = itoa::Buffer::new();
        self.write_str(key, buf.format(value));
    }

    /// Write a boolean field.
    #[inline]
    fn write_bool(&mut self, key: &str, value: bool) {
        self.write_str(key, if value { "true" } else { "false" });
    }

    /// Write a field whose value implements `Display`.
    ///
    /// Default impl allocates a `String` via `format!` and forwards to
    /// `write_str`. Override if you can stream the bytes directly.
    #[inline]
    fn write_display(&mut self, key: &str, value: &dyn core::fmt::Display) {
        self.write_str(key, &value.to_string());
    }
}

/// User-defined request-context schema for `LogContext`.
///
/// Implementors describe how to emit their fields into the display and JSON
/// writers. Fields that are absent should be skipped — do not write empty
/// strings.
pub trait RequestFields: Clone + Default + Send + Sync + 'static {
    /// Emit set fields into the display writer (`key=value`).
    fn write_display(&self, w: &mut dyn FieldWriter);

    /// Emit set fields into the JSON writer (`,"key":"value"`).
    fn write_json(&self, w: &mut dyn FieldWriter);

    /// Merge another instance into self. Set fields in `other` override self.
    fn merge(&mut self, other: Self);
}

impl RequestFields for () {
    #[inline(always)]
    fn write_display(&self, _: &mut dyn FieldWriter) {}
    #[inline(always)]
    fn write_json(&self, _: &mut dyn FieldWriter) {}
    #[inline(always)]
    fn merge(&mut self, _: ()) {}
}
