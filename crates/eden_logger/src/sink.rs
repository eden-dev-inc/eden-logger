//! Optional structured log sink for non-blocking telemetry export.
//!
//! Sinks are keyed by the concrete `RequestFields` type, so each application
//! that uses its own `R` gets its own sink slot. At most one sink may be
//! installed per `R`.
//!
//! This module is compiled by the lightweight `sink` feature and does not
//! impose a serialization format on consumers.

use crate::fields::RequestFields;
use crate::schema::EdenLog;
use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{OnceLock, RwLock};

type SinkAny = dyn Any + Send + Sync + 'static;

static SINKS: OnceLock<RwLock<HashMap<TypeId, Box<SinkAny>>>> = OnceLock::new();

thread_local! {
    /// Typed sink pointers resolved by this thread.
    ///
    /// Installed sinks are immutable and are never removed, so the boxed
    /// allocation behind a cached pointer remains stable for the process
    /// lifetime even if the registry's HashMap reallocates.
    static CACHED_SINKS: RefCell<Vec<(TypeId, *const ())>> = const { RefCell::new(Vec::new()) };
}

/// Hot-path fast check: set to true on the first successful `install_sink`
/// call. Lets `dispatch` short-circuit with a single `Relaxed` atomic load
/// (~1 ns) instead of taking the registry's read lock for every log call.
static ANY_INSTALLED: AtomicBool = AtomicBool::new(false);

fn registry() -> &'static RwLock<HashMap<TypeId, Box<SinkAny>>> {
    SINKS.get_or_init(|| RwLock::new(HashMap::new()))
}

struct SinkSlot<R: RequestFields>(Box<dyn Fn(EdenLog<R>) + Send + Sync + 'static>);

/// Install a sink for the concrete `RequestFields` type `R`. At most one
/// sink may be installed per `R`; subsequent calls return `Err`.
pub fn install_sink<R, F>(sink: F) -> Result<(), &'static str>
where
    R: RequestFields,
    F: Fn(EdenLog<R>) + Send + Sync + 'static,
{
    let mut guard = registry().write().map_err(|_| "eden_logger sink registry poisoned")?;
    let key = TypeId::of::<R>();
    if guard.contains_key(&key) {
        return Err("eden_logger sink already installed");
    }
    guard.insert(key, Box::new(SinkSlot::<R>(Box::new(sink))));
    ANY_INSTALLED.store(true, Ordering::Relaxed);
    Ok(())
}

/// Hot-path dispatch helper.
///
/// `build_log` is only invoked when a sink is actually installed for `R`.
/// The fast path is a single `Relaxed` atomic load — no lock, no map lookup,
/// no log construction — when no sink has ever been installed. Once a thread
/// resolves the sink for `R`, subsequent dispatches on that thread also avoid
/// the global registry lock.
#[inline]
pub(crate) fn dispatch<R>(build_log: impl FnOnce() -> EdenLog<R>)
where
    R: RequestFields,
{
    if !ANY_INSTALLED.load(Ordering::Relaxed) {
        return;
    }
    let Some(slot) = cached_sink::<R>() else {
        return;
    };
    (slot.0)(build_log());
}

#[inline]
fn cached_sink<R: RequestFields>() -> Option<&'static SinkSlot<R>> {
    let key = TypeId::of::<R>();
    if let Some(pointer) = CACHED_SINKS.with_borrow(|cache| cache.iter().find(|(cached, _)| *cached == key).map(|(_, pointer)| *pointer)) {
        // SAFETY: pointers are inserted only after downcasting the process-global
        // boxed sink to SinkSlot<R>. Sinks cannot be replaced or removed, and a
        // Box's allocation stays stable when the registry HashMap moves entries.
        return Some(unsafe { &*pointer.cast::<SinkSlot<R>>() });
    }

    let lock = SINKS.get()?;
    let guard = lock.read().ok()?;
    let slot = guard.get(&key)?;
    let slot = slot.downcast_ref::<SinkSlot<R>>()?;
    let pointer = std::ptr::from_ref(slot).cast::<()>();
    CACHED_SINKS.with_borrow_mut(|cache| cache.push((key, pointer)));

    // SAFETY: same invariant as the cached branch above. Reborrow through the
    // stable allocation so the returned reference is independent of the lock
    // guard's lexical lifetime.
    Some(unsafe { &*pointer.cast::<SinkSlot<R>>() })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::{FieldWriter, LogAudience, LogContext, LogLevel};

    use super::*;

    #[derive(Clone, Default)]
    struct CachedFields;

    impl RequestFields for CachedFields {
        fn write_display(&self, _: &mut dyn FieldWriter) {}
        fn write_json(&self, _: &mut dyn FieldWriter) {}
        fn merge(&mut self, _: Self) {}
    }

    #[test]
    fn caches_the_typed_sink_pointer_per_thread() {
        let calls = Arc::new(AtomicUsize::new(0));
        let sink_calls = Arc::clone(&calls);
        install_sink::<CachedFields, _>(move |_| {
            sink_calls.fetch_add(1, Ordering::Relaxed);
        })
        .expect("install test sink");

        let first = cached_sink::<CachedFields>().expect("first lookup");
        let second = cached_sink::<CachedFields>().expect("cached lookup");
        assert!(std::ptr::eq(first, second));

        dispatch(|| EdenLog::new(LogLevel::Info, "cached", &LogContext::<CachedFields>::new(), LogAudience::Internal));
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        let key = TypeId::of::<CachedFields>();
        CACHED_SINKS.with_borrow(|cache| {
            assert_eq!(cache.iter().filter(|(cached, _)| *cached == key).count(), 1);
        });
    }
}
