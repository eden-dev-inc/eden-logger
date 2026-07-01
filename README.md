# eden-logger

Standalone workspace for Eden's public structured logging crates.

## Crates

- `eden_logger`: structured logging API, context model, writers, sinks, and log macros.
- `eden_logger_macros`: proc macros used by `eden_logger`.

Eden-specific wrappers, including `eden_logger_internal`, live in `eden-dev` and are not part of this public logger workspace.

The primary crate documentation lives in [`crates/eden_logger/README.md`](crates/eden_logger/README.md).

## Development

```bash
cargo fmt --check
cargo check --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```
