# Changelog

All notable changes to this workspace are documented here.

## Unreleased

Planned releases: `eden_logger` 0.1.2 and `eden_logger_export` 0.1.0.

### Added

- Added `eden_logger_export`, a bounded Tokio OTLP/HTTP exporter with typed
  Eden-to-OTLP mapping, normal and reserved priority queues, count/byte/time
  batching, transient retry, partial-rejection bisection, poison-record
  isolation, TLS/mTLS configuration, health snapshots, native
  `fast_telemetry::MetricVisitor` metrics, and bounded shutdown.
- Added `EdenLogOtlpMapper::map_with_attributes` so shard-stream can attach
  stable stream identity and sequence attributes before acknowledged export.
- Added `LogTarget::StructuredSink` for deployments where the structured sink
  is the only destination and duplicate text formatting is unnecessary.
- Added contention benchmarks, a production-style profiling mode, mock
  Collector integration tests, and pinned Hegel properties.

### Changed

- Added a lightweight `sink` feature independent of Serde. The existing
  `serde` feature still implies `sink` for compatibility.
- Removed serialization bounds from `install_sink`; exporters consume typed
  `RequestFields::write_json` output instead.
- Cached immutable typed sinks and producer lanes per thread, keeping global
  registry lookup, serialization, protobuf encoding, waiting, and network I/O
  off the steady-state logging path.
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
