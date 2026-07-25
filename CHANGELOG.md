# Changelog

All notable changes to this workspace are documented here.

## Unreleased

Planned releases: `eden_logger` 0.1.2 and `eden_logger_export` 0.1.0.

### Added

- Added `eden_logger_export`, a bounded Tokio OTLP/HTTP exporter with typed
  Eden-to-OTLP mapping, normal and reserved priority queues, count/byte/time
  batching, retained-byte admission budgets, OTLP-compliant retry and partial
  success handling, poison-record isolation, TLS/mTLS configuration, health
  snapshots, native `fast_telemetry::MetricVisitor` metrics, exact force flush,
  live endpoint/credential reconfiguration, and bounded shutdown.
- Added `EdenLogOtlpMapper::map_with_attributes` so shard-stream can attach
  stable stream identity and sequence attributes before acknowledged export.
- Added `LogTarget::StructuredSink` for deployments where the structured sink
  is the only destination and duplicate text formatting is unnecessary.
- Added contention benchmarks, a production-style profiling mode, mock
  Collector integration tests, and pinned Hegel properties.

### Changed

- Added a lightweight `sink` feature independent of Serde. The existing
  `serde` feature still implies `sink` for compatibility.
- Added lifecycle-managed `register_sink`/`SinkRegistration`; callbacks can be
  atomically replaced, disabled, dropped, and reinstalled for the same request
  type while thread-local slot caches remain valid. Replaced callbacks remain
  alive until dispatches that already loaded them return.
- Added `RequestFields::estimated_size_bytes` and
  `EdenLog::estimated_size_bytes` for retained-memory admission accounting.
- Removed serialization bounds from `install_sink`; exporters consume typed
  `RequestFields::write_json` output instead.
- Cached replaceable typed sink slots and weak producer lanes per thread,
  keeping global
  registry lookup, serialization, protobuf encoding, waiting, and network I/O
  off the steady-state logging path without retaining stopped exporter
  generations.
- Reused the callback lifetime as the exporter producer-quiescence fence and
  used payload-independent relaxed queue accounting. This removes per-record
  lifecycle atomics and wakeups without weakening bounded admission, exact
  flush, shutdown, or reconfiguration behavior.
- Updated the workspace MSRV to Rust 1.93 to match fast-telemetry 0.9 and its
  optional runtime dependencies.

### Delivery contract

- Direct export is best effort and memory bounded. Queue overflow, process
  termination, or an expired shutdown deadline can drop records.
- Delivery is at least once while records remain in memory; a lost response can
  cause duplicates.
- Ordering, HA replication, and durable retry remain shard-stream
  responsibilities.

### Release order

1. Publish `fast-telemetry` and `fast-telemetry-export` 0.9.
2. Publish `eden_logger` 0.1.2.
3. Publish `eden_logger_export` 0.1.0.

## 0.1.1 - 2026-07-01

- Extracted the public Eden logger workspace.
- Added fast-telemetry trace context and grouped log counters.
