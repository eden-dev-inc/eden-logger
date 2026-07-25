# Thread-local ingestion benchmark

Measured on an Apple M5 Max with eight producer threads and 20,000 accepted
records per thread:

```sh
cargo bench -p eden_logger_export --bench sink_enqueue
```

| Path | p99 runs | Median p99 | Change from shared MPSC |
| --- | --- | ---: | ---: |
| Before: shared Tokio MPSC | 5,333 / 5,750 / 5,791 ns | 5,750 ns | baseline |
| After: thread-local lanes with text formatting | 1,084 / 1,166 / 1,166 ns | 1,166 ns | 79.7% lower |
| After: thread-local lanes, structured sink only | 875 / 1,000 / 1,083 ns | 1,000 ns | 82.6% lower |
| `emit_direct`, before queue-buffer reuse | 917 / 917 / 1,125 ns | 917 ns | 84.1% lower |
| `emit_direct`, after queue-buffer reuse | 750 / 791 / 792 / 958 / 1,417 ns | 792 ns | 86.2% lower |

The before measurements were captured immediately before replacing per-record
shared MPSC submission. The after benchmark retains the same Eden record
construction, exporter worker, mock OTLP Collector, queue capacity, record
count, and thread count. `StructuredSink` additionally skips creation of an
unused display/JSON line.

The benchmark asserts p99 below 10 microseconds and fails if any accepted-path
record is dropped because of queue pressure.

## Follow-up CPU profile

The production-style `emit_direct` path was sampled for 10 seconds with
per-record benchmark timers disabled:

```sh
EDEN_EXPORT_BENCH_MODE=direct \
EDEN_EXPORT_BENCH_ROUNDS=500 \
EDEN_EXPORT_BENCH_PROFILE=1 \
cargo bench -p eden_logger_export --bench sink_enqueue
```

| Measurement | Before buffer reuse | After buffer reuse | Change |
| --- | ---: | ---: | ---: |
| Producer `VecDeque` growth stacks | 140 | 31 | 77.9% lower |
| Timer-free workload throughput | 18.66M records/s | 19.08M records/s | 2.3% higher |
| Timed median p99 | 917 ns | 792 ns | 13.6% lower |

Each lane now ping-pongs emptied normal and reserved queue allocations through a
worker-only spare slot. The worker still acquires the producer's hot queue lock
only once per drain. The remaining growth stacks are first-use allocations from
the benchmark's newly created producer threads.

The largest remaining producer costs are capturing the original timestamp and
constructing the owned `EdenLog` payload. Moving either to the worker would
change event-time semantics or require a new borrowed sink interface, so they
were not changed by this optimization.
