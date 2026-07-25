use std::collections::BTreeMap;

use eden_logger::{EdenLog, FieldWriter, LogLevel, RequestFields};

use crate::{AnyValue, KeyValue, LogRecord, any_value};

/// Converts typed Eden records into standard OTLP log records.
#[derive(Debug, Clone, Copy, Default)]
pub struct EdenLogOtlpMapper;

impl EdenLogOtlpMapper {
    /// Map an Eden record with no exporter-specific attributes.
    pub fn map<R: RequestFields>(&self, log: &EdenLog<R>, observed_time_unix_nano: u64) -> LogRecord {
        self.map_with_attributes(log, observed_time_unix_nano, std::iter::empty())
    }

    /// Map an Eden record and merge attributes supplied by an integration.
    ///
    /// Attribute precedence is: intrinsic Eden fields, supplied attributes,
    /// typed request fields, then ad-hoc additional fields.
    pub fn map_with_attributes<R, I>(&self, log: &EdenLog<R>, observed_time_unix_nano: u64, extra_attributes: I) -> LogRecord
    where
        R: RequestFields,
        I: IntoIterator<Item = KeyValue>,
    {
        let mut attributes = BTreeMap::<String, AnyValue>::new();

        for (key, value) in &log.additional {
            attributes.insert(key.to_string(), string_value(value.as_str()));
        }

        let mut request_writer = AttributeWriter { attributes: &mut attributes };
        log.request.write_json(&mut request_writer);

        for attribute in extra_attributes {
            if let Some(value) = attribute.value {
                attributes.insert(attribute.key, value);
            }
        }

        insert_string(&mut attributes, "eden.audience", log.audience.as_str());
        insert_optional_string(&mut attributes, "eden.feature", log.feature.as_deref());
        insert_optional_string(&mut attributes, "code.function.name", log.function.as_deref());
        insert_optional_string(&mut attributes, "code.file.path", log.file.as_deref());
        if let Some(line) = log.line {
            attributes.insert("code.line.number".to_string(), int_value(i64::from(line)));
        }
        insert_optional_string(&mut attributes, "error.code", log.error_code.as_deref());
        insert_optional_string(&mut attributes, "error.type", log.error_category.as_deref());

        let trace_id = decode_hex_id::<16>(log.trace_id.as_deref());
        let span_id = if trace_id.is_some() {
            decode_hex_id::<8>(log.span_id.as_deref())
        } else {
            None
        };
        if trace_id.is_none() {
            insert_optional_string(&mut attributes, "eden.trace_id", log.trace_id.as_deref());
        }
        if span_id.is_none() {
            insert_optional_string(&mut attributes, "eden.span_id", log.span_id.as_deref());
        }

        LogRecord {
            time_unix_nano: timestamp_nanos(log),
            observed_time_unix_nano,
            severity_number: severity_number(log.level),
            severity_text: log.level.as_str().to_string(),
            body: Some(string_value(&log.message)),
            attributes: attributes.into_iter().map(|(key, value)| KeyValue { key, value: Some(value) }).collect(),
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: trace_id.map_or_else(Vec::new, |id| id.to_vec()),
            span_id: span_id.map_or_else(Vec::new, |id| id.to_vec()),
            event_name: String::new(),
        }
    }
}

fn timestamp_nanos<R: RequestFields>(log: &EdenLog<R>) -> u64 {
    log.timestamp.timestamp_nanos_opt().and_then(|value| u64::try_from(value).ok()).unwrap_or_default()
}

const fn severity_number(level: LogLevel) -> i32 {
    match level {
        LogLevel::Trace => 1,
        LogLevel::Debug => 5,
        LogLevel::Info => 9,
        LogLevel::Warn => 13,
        LogLevel::Error => 17,
    }
}

fn decode_hex_id<const N: usize>(value: Option<&str>) -> Option<[u8; N]> {
    let value = value?;
    if value.len() != N * 2 {
        return None;
    }
    let mut output = [0_u8; N];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    if output.iter().all(|byte| *byte == 0) {
        return None;
    }
    Some(output)
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn insert_optional_string(attributes: &mut BTreeMap<String, AnyValue>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        insert_string(attributes, key, value);
    }
}

fn insert_string(attributes: &mut BTreeMap<String, AnyValue>, key: &str, value: &str) {
    attributes.insert(key.to_string(), string_value(value));
}

fn string_value(value: &str) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::StringValue(value.to_string())),
    }
}

fn int_value(value: i64) -> AnyValue {
    AnyValue { value: Some(any_value::Value::IntValue(value)) }
}

fn bool_value(value: bool) -> AnyValue {
    AnyValue { value: Some(any_value::Value::BoolValue(value)) }
}

struct AttributeWriter<'a> {
    attributes: &'a mut BTreeMap<String, AnyValue>,
}

impl FieldWriter for AttributeWriter<'_> {
    fn write_str(&mut self, key: &str, value: &str) {
        self.attributes.insert(key.to_string(), string_value(value));
    }

    fn write_u64(&mut self, key: &str, value: u64) {
        let value = i64::try_from(value).map(int_value).unwrap_or_else(|_| string_value(&value.to_string()));
        self.attributes.insert(key.to_string(), value);
    }

    fn write_i64(&mut self, key: &str, value: i64) {
        self.attributes.insert(key.to_string(), int_value(value));
    }

    fn write_bool(&mut self, key: &str, value: bool) {
        self.attributes.insert(key.to_string(), bool_value(value));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use eden_logger::{LogAudience, LogContext};
    use fast_telemetry::otlp::pb::any_value::Value;

    #[derive(Clone, Default)]
    struct TestRequest {
        tenant: String,
        attempt: u64,
        cached: bool,
    }

    impl RequestFields for TestRequest {
        fn write_display(&self, writer: &mut dyn FieldWriter) {
            self.write_json(writer);
        }

        fn write_json(&self, writer: &mut dyn FieldWriter) {
            writer.write_str("tenant.id", &self.tenant);
            writer.write_u64("request.attempt", self.attempt);
            writer.write_bool("cache.hit", self.cached);
        }

        fn merge(&mut self, other: Self) {
            *self = other;
        }
    }

    fn attributes(record: &LogRecord) -> HashMap<&str, &Value> {
        record
            .attributes
            .iter()
            .filter_map(|attribute| Some((attribute.key.as_str(), attribute.value.as_ref()?.value.as_ref()?)))
            .collect()
    }

    #[test]
    fn maps_all_fields_and_preserves_typed_request_values() {
        let context = LogContext::<TestRequest>::new()
            .with_feature("billing")
            .with_function("charge")
            .with_trace_id("4bf92f3577b34da6a3ce929d0e0e4736")
            .with_span_id("00f067aa0ba902b7")
            .with_error_code("declined")
            .with_error_category("payment")
            .with_request(TestRequest { tenant: "tenant-a".to_string(), attempt: 3, cached: true })
            .with_additional("region", "us-east-1");
        let log = EdenLog::new(LogLevel::Warn, "payment declined", &context, LogAudience::Both).with_location("src/billing.rs", 42);

        let record = EdenLogOtlpMapper.map(&log, 99);
        let attrs = attributes(&record);

        assert_eq!(record.severity_number, 13);
        assert_eq!(record.severity_text, "WARN");
        assert_eq!(record.observed_time_unix_nano, 99);
        assert_eq!(record.trace_id.len(), 16);
        assert_eq!(record.span_id.len(), 8);
        assert_eq!(
            record.body.as_ref().and_then(|body| body.value.as_ref()),
            Some(&Value::StringValue("payment declined".to_string()))
        );
        assert_eq!(attrs.get("tenant.id"), Some(&&Value::StringValue("tenant-a".to_string())));
        assert_eq!(attrs.get("request.attempt"), Some(&&Value::IntValue(3)));
        assert_eq!(attrs.get("cache.hit"), Some(&&Value::BoolValue(true)));
        assert_eq!(attrs.get("code.file.path"), Some(&&Value::StringValue("src/billing.rs".to_string())));
    }

    #[test]
    fn intrinsic_and_extra_attributes_win_collisions() {
        let context = LogContext::new()
            .with_request(TestRequest {
                tenant: "request-tenant".to_string(),
                ..TestRequest::default()
            })
            .with_additional("tenant.id", "additional-tenant")
            .with_additional("eden.audience", "forged");
        let log = EdenLog::new(LogLevel::Info, "hello", &context, LogAudience::Internal);
        let record = EdenLogOtlpMapper.map_with_attributes(
            &log,
            1,
            [KeyValue {
                key: "tenant.id".to_string(),
                value: Some(string_value("stream-tenant")),
            }],
        );
        let attrs = attributes(&record);

        assert_eq!(attrs.get("tenant.id"), Some(&&Value::StringValue("stream-tenant".to_string())));
        assert_eq!(attrs.get("eden.audience"), Some(&&Value::StringValue("INTERNAL".to_string())));
    }

    #[test]
    fn malformed_trace_context_is_preserved_as_attributes() {
        let context = LogContext::<()>::new().with_trace_id("bad-trace").with_span_id("bad-span");
        let log = EdenLog::new(LogLevel::Error, "failed", &context, LogAudience::Internal);

        let record = EdenLogOtlpMapper.map(&log, 1);
        let attrs = attributes(&record);

        assert!(record.trace_id.is_empty());
        assert!(record.span_id.is_empty());
        assert_eq!(attrs.get("eden.trace_id"), Some(&&Value::StringValue("bad-trace".to_string())));
        assert_eq!(attrs.get("eden.span_id"), Some(&&Value::StringValue("bad-span".to_string())));
    }
}
