//! Runtime log level filtering via environment variable.
//!
//! This module provides runtime control over which log levels are emitted,
//! working in conjunction with compile-time feature flags.
//!
//! # Environment Variable
//!
//! Set `EDEN_LOG_LEVEL` to control log levels:
//!
//! ```bash
//! # Only show info and warn logs
//! EDEN_LOG_LEVEL=info;warn cargo run
//!
//! # Only show error logs
//! EDEN_LOG_LEVEL=error cargo run
//!
//! # Disable all logs
//! EDEN_LOG_LEVEL="" cargo run
//! # Or
//! EDEN_LOG_LEVEL=none cargo run
//! # Or
//! EDEN_LOG_LEVEL=off cargo run
//!
//! # Show all compiled logs (default if env var not set)
//! cargo run
//! ```
//!
//! # How it works
//! - Compile-time features strip code that will never be logged
//! - Runtime filter controls what actually gets emitted from compiled code
//! - If EDEN_LOG_LEVEL is not set, all compiled logs are emitted
//! - If EDEN_LOG_LEVEL is set to empty/"none"/"off", all logs are disabled
//! - If EDEN_LOG_LEVEL is set to specific levels, only those are emitted

use crate::schema::LogLevel;
use std::sync::atomic::{AtomicU8, Ordering};

/// Bitmask for enabled log levels.
/// Each bit represents a log level (0=Trace, 1=Debug, 2=Info, 3=Warn, 4=Error).
/// If bit is set, that level is ENABLED.
/// Default 0 means "all levels enabled" (when env var is not set).
static ENABLED_LEVELS: AtomicU8 = AtomicU8::new(0);

const FILTER_STATE_UNINITIALIZED: u8 = 0;
const FILTER_STATE_INITIALIZING: u8 = 1;
const FILTER_STATE_INITIALIZED: u8 = 2;

/// Tracks whether the runtime filter has been loaded from the environment.
/// Once initialized we can keep the hot path to a single atomic load.
static FILTER_STATE: AtomicU8 = AtomicU8::new(FILTER_STATE_UNINITIALIZED);

/// Initialize the filter from EDEN_LOG_LEVEL environment variable.
///
/// Called automatically on first use. Can be called manually to reload config.
pub fn init_from_env() {
    if let Ok(env_value) = std::env::var("EDEN_LOG_LEVEL") {
        init_from_value(&env_value);
    }

    // If env var is not set, ENABLED_LEVELS remains unchanged but
    // downstream callers can still observe that initialization has happened.
    FILTER_STATE.store(FILTER_STATE_INITIALIZED, Ordering::Release);
}

/// Initialize the filter from a provided log-level string.
///
/// This allows callers with access to a configuration system (e.g. `eden_config`)
/// to feed the value directly without going through the environment variable.
///
/// The format is the same as `EDEN_LOG_LEVEL`: semicolon-separated level names
/// (e.g. `"info;warn"`), or `"none"` / `"off"` / `""` to disable all logs.
pub fn init_from_value(value: &str) {
    let trimmed = value.trim();

    // Check for explicit disable-all markers
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("none") || trimmed.eq_ignore_ascii_case("off") {
        // Explicitly disable all logs by setting a sentinel value
        // We use 0x80 (bit 7 set) as a flag that means "explicitly disabled all"
        ENABLED_LEVELS.store(0x80, Ordering::Relaxed);
    } else {
        let mut mask = 0u8;

        for level_str in value.split(';') {
            let level_str = level_str.trim().to_lowercase();
            let bit = match level_str.as_str() {
                "trace" => 1 << (LogLevel::Trace as u8),
                "debug" => 1 << (LogLevel::Debug as u8),
                "info" => 1 << (LogLevel::Info as u8),
                "warn" => 1 << (LogLevel::Warn as u8),
                "error" => 1 << (LogLevel::Error as u8),
                _ => {
                    continue;
                }
            };
            mask |= bit;
        }

        ENABLED_LEVELS.store(mask, Ordering::Relaxed);
    }

    FILTER_STATE.store(FILTER_STATE_INITIALIZED, Ordering::Release);
}

/// Enable specific log levels at runtime.
///
/// # Example
/// ```rust
/// use eden_logger::{enable_levels, LogLevel};
///
/// // Only emit info and warn logs
/// enable_levels(&[LogLevel::Info, LogLevel::Warn]);
/// ```
pub fn enable_levels(levels: &[LogLevel]) {
    let mut mask = ENABLED_LEVELS.load(Ordering::Relaxed);
    for level in levels {
        mask |= 1 << (*level as u8);
    }
    ENABLED_LEVELS.store(mask, Ordering::Relaxed);
    FILTER_STATE.store(FILTER_STATE_INITIALIZED, Ordering::Release);
}

/// Disable specific log levels at runtime (remove from enabled set).
///
/// # Example
/// ```rust
/// use eden_logger::{disable_levels, LogLevel};
///
/// // Disable debug logs
/// disable_levels(&[LogLevel::Debug]);
/// ```
pub fn disable_levels(levels: &[LogLevel]) {
    let mut mask = ENABLED_LEVELS.load(Ordering::Relaxed);
    for level in levels {
        mask &= !(1 << (*level as u8));
    }
    ENABLED_LEVELS.store(mask, Ordering::Relaxed);
    FILTER_STATE.store(FILTER_STATE_INITIALIZED, Ordering::Release);
}

/// Clear all runtime filters (allow all levels).
pub fn clear_filter() {
    ENABLED_LEVELS.store(0, Ordering::Relaxed);
    FILTER_STATE.store(FILTER_STATE_INITIALIZED, Ordering::Release);
}

#[inline(always)]
fn ensure_filter_initialized() {
    let mut state = FILTER_STATE.load(Ordering::Acquire);
    if state == FILTER_STATE_INITIALIZED {
        return;
    }

    if state == FILTER_STATE_UNINITIALIZED {
        match FILTER_STATE.compare_exchange(FILTER_STATE_UNINITIALIZED, FILTER_STATE_INITIALIZING, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => {
                init_from_env();
                return;
            }
            Err(current) => {
                state = current;
            }
        }
    }

    while state != FILTER_STATE_INITIALIZED {
        std::hint::spin_loop();
        state = FILTER_STATE.load(Ordering::Acquire);
    }
}

/// Check if a log level should be emitted based on runtime filter.
/// This is a fast check (~1ns) using atomic load and bitwise AND.
#[inline(always)]
pub fn should_log(level: LogLevel) -> bool {
    ensure_filter_initialized();

    let enabled = ENABLED_LEVELS.load(Ordering::Relaxed);

    // If bit 7 (0x80) is set, it means explicitly disabled all
    if enabled == 0x80 {
        return false;
    }

    // If enabled == 0, it means no filter is set (env var not present), so allow all levels
    if enabled == 0 {
        return true;
    }

    // Otherwise, check if this level's bit is set in the enabled mask
    let level_bit = 1 << (level as u8);
    (enabled & level_bit) != 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    static TEST_FILTER_LOCK: Mutex<()> = Mutex::new(());

    fn filter_test_guard() -> MutexGuard<'static, ()> {
        let guard = TEST_FILTER_LOCK.lock().expect("filter test lock poisoned");
        clear_filter();
        guard
    }

    #[test]
    fn test_default_allows_all() {
        let _guard = filter_test_guard();
        assert!(should_log(LogLevel::Trace));
        assert!(should_log(LogLevel::Debug));
        assert!(should_log(LogLevel::Info));
        assert!(should_log(LogLevel::Warn));
        assert!(should_log(LogLevel::Error));
    }

    #[test]
    fn test_enable_specific_levels() {
        let _guard = filter_test_guard();
        enable_levels(&[LogLevel::Info, LogLevel::Warn]);

        assert!(!should_log(LogLevel::Trace));
        assert!(!should_log(LogLevel::Debug));
        assert!(should_log(LogLevel::Info));
        assert!(should_log(LogLevel::Warn));
        assert!(!should_log(LogLevel::Error));

        clear_filter();
    }

    #[test]
    fn test_enable_then_disable() {
        let _guard = filter_test_guard();
        enable_levels(&[LogLevel::Info, LogLevel::Warn, LogLevel::Error]);
        disable_levels(&[LogLevel::Warn]);

        assert!(!should_log(LogLevel::Trace));
        assert!(!should_log(LogLevel::Debug));
        assert!(should_log(LogLevel::Info));
        assert!(!should_log(LogLevel::Warn)); // Disabled
        assert!(should_log(LogLevel::Error));

        clear_filter();
    }

    #[test]
    fn test_error_only() {
        let _guard = filter_test_guard();
        enable_levels(&[LogLevel::Error]);

        assert!(!should_log(LogLevel::Trace));
        assert!(!should_log(LogLevel::Debug));
        assert!(!should_log(LogLevel::Info));
        assert!(!should_log(LogLevel::Warn));
        assert!(should_log(LogLevel::Error));

        clear_filter();
    }

    #[test]
    fn test_explicit_disable_all() {
        let _guard = filter_test_guard();
        // Simulate setting EDEN_LOG_LEVEL="" or "none"
        ENABLED_LEVELS.store(0x80, Ordering::Relaxed);

        assert!(!should_log(LogLevel::Trace));
        assert!(!should_log(LogLevel::Debug));
        assert!(!should_log(LogLevel::Info));
        assert!(!should_log(LogLevel::Warn));
        assert!(!should_log(LogLevel::Error));

        clear_filter();
    }
}
