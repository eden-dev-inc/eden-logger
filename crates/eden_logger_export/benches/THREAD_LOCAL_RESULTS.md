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

## Safety-hardening follow-up

The lifecycle and memory-safety pass added strict retained-byte admission,
acceptance tickets for exact force flush, replaceable sink callbacks, and
shutdown/reconfiguration fencing. Its first finalized implementation paid for
a process-wide active-submission counter and worker notification on every
record:

| Path | Pre-hardening p99 | First safe p99 | Pre-hardening throughput | First safe throughput |
| --- | ---: | ---: | ---: | ---: |
| Formatted + thread-local | 875 ns | 1,750 ns | 18.49M records/s | 8.35M records/s |
| Structured-only + thread-local | 709 ns | 1,667 ns | 22.29M records/s | 8.70M records/s |
| `emit_direct` structured-only | 584 ns | 1,667 ns | 24.95M records/s | 9.01M records/s |

## Safety-preserving hot-path recovery

A follow-up profile showed that the active-submission decrement and unconditional
Tokio notification dominated the producer path. The exporter now uses the
reference-counted sink callback itself as the producer-quiescence fence:
replacement or disable removes the callback from future dispatch, while the
callback remains alive until dispatches that already loaded it return. Its drop
then wakes the worker exactly once. Queue counters use relaxed atomic ordering
because they publish no payload data; the lane mutex remains the payload
publication boundary and atomic modification order still enforces the global
record and byte limits.

The benchmark now reports the median-p99 round instead of whichever round ran
last. Matching 15-round runs on the same Apple M5 Max produced:

| Path | First safe p99 | Optimized p99 | First safe throughput | Optimized throughput |
| --- | ---: | ---: | ---: | ---: |
| Formatted + thread-local | 1,833 ns | 875 ns | 8.52M records/s | 16.16M records/s |
| Structured-only + thread-local | 1,750 ns | 833 ns | 8.84M records/s | 18.33M records/s |
| `emit_direct` structured-only | 1,709 ns | 625 ns | 9.05M records/s | 23.62M records/s |

The direct path reduces hardened p99 latency by 63.4% and delivers 2.61× its
throughput. It is within 7.0% of the historical 584 ns pre-hardening p99 and
within 5.3% of the historical 24.95M records/s throughput while retaining the
strict 64 MiB default queue bound, exact force flush, and race-safe
shutdown/reconfiguration.

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
