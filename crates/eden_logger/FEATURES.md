# Enabling Debug/Trace Logs and Source Location

By default, production builds exclude debug/trace logs and source location. To enable them:

```bash
# Enable source location (file:line in logs)
cargo build --features eden_logger/source-location

# Enable debug logs
cargo build --features eden_logger/log-debug

# Enable trace logs
cargo build --features eden_logger/log-trace

# Enable everything (debug, trace, source-location)
cargo build --features eden_logger/full
```

When `source-location` is enabled, logs include the file and line number in rustc style:

```
[2024-01-01T00:00:00.000Z] [INFO] [INTERNAL] fn=my_function Server started
   --> src/services/auth.rs:42
```
