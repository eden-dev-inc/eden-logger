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
use arc_swap::ArcSwapOption;
use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

type SinkAny = dyn Any + Send + Sync + 'static;

static SINKS: OnceLock<RwLock<HashMap<TypeId, Box<SinkAny>>>> = OnceLock::new();

thread_local! {
    /// Typed sink pointers resolved by this thread.
    ///
    /// Sink slots are never removed, so the boxed allocation behind a cached
    /// pointer remains stable even when its active callback is replaced.
    static CACHED_SINKS: RefCell<Vec<(TypeId, *const ())>> = const { RefCell::new(Vec::new()) };
}

/// Process-wide active sink count used for the no-sink fast path.
static ACTIVE_SINKS: AtomicUsize = AtomicUsize::new(0);

fn registry() -> &'static RwLock<HashMap<TypeId, Box<SinkAny>>> {
    SINKS.get_or_init(|| RwLock::new(HashMap::new()))
}

struct SinkCallback<R: RequestFields>(Box<dyn Fn(EdenLog<R>) + Send + Sync + 'static>);

struct SinkSlot<R: RequestFields> {
    callback: ArcSwapOption<SinkCallback<R>>,
}

impl<R: RequestFields> SinkSlot<R> {
    fn new() -> Self {
        Self { callback: ArcSwapOption::empty() }
    }
}

/// Managed registration for a replaceable typed sink.
///
/// Dropping or disabling the registration atomically stops future dispatches.
/// The stable slot remains cached, allowing another sink for the same
/// `RequestFields` type to be installed later.
#[must_use = "dropping the registration disables the sink"]
pub struct SinkRegistration<R: RequestFields> {
    slot: &'static SinkSlot<R>,
    callback: Option<Arc<SinkCallback<R>>>,
}

impl<R: RequestFields> SinkRegistration<R> {
    /// Atomically replace the active callback without invalidating thread-local
    /// slot caches.
    pub fn replace<F>(&mut self, sink: F) -> Result<(), &'static str>
    where
        F: Fn(EdenLog<R>) + Send + Sync + 'static,
    {
        let current = self.callback.as_ref().cloned().ok_or("eden_logger sink registration is inactive")?;
        let replacement = Arc::new(SinkCallback(Box::new(sink)));
        let _guard = registry().write().map_err(|_| "eden_logger sink registry poisoned")?;
        let active = self.slot.callback.load_full();
        if !active.as_ref().is_some_and(|active| Arc::ptr_eq(active, &current)) {
            return Err("eden_logger sink registration is stale");
        }
        self.slot.callback.store(Some(Arc::clone(&replacement)));
        self.callback = Some(replacement);
        Ok(())
    }

    /// Disable this callback. Returns whether it was still the active sink.
    pub fn disable(&mut self) -> bool {
        let Some(current) = self.callback.as_ref().cloned() else {
            return false;
        };
        let Ok(_guard) = registry().write() else {
            return false;
        };
        let active = self.slot.callback.load_full();
        if !active.as_ref().is_some_and(|active| Arc::ptr_eq(active, &current)) {
            self.callback = None;
            return false;
        }
        self.slot.callback.store(None);
        self.callback = None;
        ACTIVE_SINKS.fetch_sub(1, Ordering::Release);
        true
    }
}

impl<R: RequestFields> Drop for SinkRegistration<R> {
    fn drop(&mut self) {
        self.disable();
    }
}

/// Register a managed sink for the concrete `RequestFields` type `R`.
///
/// At most one callback is active per type. Unlike [`install_sink`], dropping
/// the returned registration releases the slot for a later installation.
pub fn register_sink<R, F>(sink: F) -> Result<SinkRegistration<R>, &'static str>
where
    R: RequestFields,
    F: Fn(EdenLog<R>) + Send + Sync + 'static,
{
    let callback = Arc::new(SinkCallback(Box::new(sink)));
    let mut guard = registry().write().map_err(|_| "eden_logger sink registry poisoned")?;
    let key = TypeId::of::<R>();
    let slot = guard
        .entry(key)
        .or_insert_with(|| Box::new(SinkSlot::<R>::new()))
        .downcast_ref::<SinkSlot<R>>()
        .ok_or("eden_logger sink registry type mismatch")?;
    if slot.callback.load().is_some() {
        return Err("eden_logger sink already installed");
    }
    slot.callback.store(Some(Arc::clone(&callback)));
    let pointer = std::ptr::from_ref(slot);
    ACTIVE_SINKS.fetch_add(1, Ordering::Release);
    drop(guard);

    // SAFETY: registry entries are never removed or replaced. HashMap growth
    // moves only the Box value, not the SinkSlot allocation it owns.
    let slot = unsafe { &*pointer };
    Ok(SinkRegistration { slot, callback: Some(callback) })
}

/// Install a sink for the concrete `RequestFields` type `R`. At most one
/// sink may be installed per `R`; subsequent calls return `Err`.
///
/// This compatibility API intentionally keeps the callback installed for the
/// process lifetime. New lifecycle-managed integrations should use
/// [`register_sink`].
pub fn install_sink<R, F>(sink: F) -> Result<(), &'static str>
where
    R: RequestFields,
    F: Fn(EdenLog<R>) + Send + Sync + 'static,
{
    let registration = register_sink::<R, F>(sink)?;
    std::mem::forget(registration);
    Ok(())
}

/// Hot-path dispatch helper.
///
/// `build_log` is only invoked when a sink is actually installed for `R`.
/// The fast path is a single `Relaxed` atomic load — no lock, no map lookup,
/// no log construction — when no sink is active. Once a thread
/// resolves the sink for `R`, subsequent dispatches on that thread also avoid
/// the global registry lock.
#[inline]
pub(crate) fn dispatch<R>(build_log: impl FnOnce() -> EdenLog<R>)
where
    R: RequestFields,
{
    if ACTIVE_SINKS.load(Ordering::Relaxed) == 0 {
        return;
    }
    let Some(slot) = cached_sink::<R>() else {
        return;
    };
    let callback = slot.callback.load();
    let Some(callback) = callback.as_ref() else {
        return;
    };
    (callback.0)(build_log());
}

#[inline]
fn cached_sink<R: RequestFields>() -> Option<&'static SinkSlot<R>> {
    let key = TypeId::of::<R>();
    if let Some(pointer) = CACHED_SINKS.with_borrow(|cache| cache.iter().find(|(cached, _)| *cached == key).map(|(_, pointer)| *pointer)) {
        // SAFETY: pointers are inserted only after downcasting the
        // process-global boxed slot to SinkSlot<R>. Slots are never removed, and
        // a Box allocation stays stable when the registry HashMap moves entries.
        return Some(unsafe { &*pointer.cast::<SinkSlot<R>>() });
    }

    let lock = SINKS.get()?;
    let guard = lock.read().ok()?;
    let slot = guard.get(&key)?;
    let slot = slot.downcast_ref::<SinkSlot<R>>()?;
    let pointer = std::ptr::from_ref(slot).cast::<()>();
    CACHED_SINKS.with_borrow_mut(|cache| cache.push((key, pointer)));

    // SAFETY: same invariant as the cached branch above.
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

    #[derive(Clone, Default)]
    struct ManagedFields;

    #[derive(Clone, Default)]
    struct ConcurrentFields;

    impl RequestFields for CachedFields {
        fn write_display(&self, _: &mut dyn FieldWriter) {}
        fn write_json(&self, _: &mut dyn FieldWriter) {}
        fn merge(&mut self, _: Self) {}
    }

    impl RequestFields for ManagedFields {
        fn write_display(&self, _: &mut dyn FieldWriter) {}
        fn write_json(&self, _: &mut dyn FieldWriter) {}
        fn merge(&mut self, _: Self) {}
    }

    impl RequestFields for ConcurrentFields {
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

    #[test]
    fn managed_sink_can_be_replaced_disabled_and_reinstalled() {
        let first_calls = Arc::new(AtomicUsize::new(0));
        let first_sink_calls = Arc::clone(&first_calls);
        let mut registration = register_sink::<ManagedFields, _>(move |_| {
            first_sink_calls.fetch_add(1, Ordering::Relaxed);
        })
        .expect("register managed sink");

        dispatch(|| EdenLog::new(LogLevel::Info, "first", &LogContext::<ManagedFields>::new(), LogAudience::Internal));
        assert_eq!(first_calls.load(Ordering::Relaxed), 1);

        let replacement_calls = Arc::new(AtomicUsize::new(0));
        let replacement_sink_calls = Arc::clone(&replacement_calls);
        registration
            .replace(move |_| {
                replacement_sink_calls.fetch_add(1, Ordering::Relaxed);
            })
            .expect("replace managed sink");
        dispatch(|| EdenLog::new(LogLevel::Info, "replacement", &LogContext::<ManagedFields>::new(), LogAudience::Internal));
        assert_eq!(first_calls.load(Ordering::Relaxed), 1);
        assert_eq!(replacement_calls.load(Ordering::Relaxed), 1);

        assert!(registration.disable());
        let builds = AtomicUsize::new(0);
        dispatch(|| {
            builds.fetch_add(1, Ordering::Relaxed);
            EdenLog::new(LogLevel::Info, "disabled", &LogContext::<ManagedFields>::new(), LogAudience::Internal)
        });
        assert_eq!(builds.load(Ordering::Relaxed), 0);

        let reinstalled = register_sink::<ManagedFields, _>(|_| {}).expect("reinstall managed sink");
        drop(reinstalled);
    }

    #[test]
    fn replacement_is_safe_during_concurrent_dispatch() {
        const THREADS: usize = 4;
        const CALLS: usize = 5_000;
        let callback_calls = Arc::new(AtomicUsize::new(0));
        let initial_calls = Arc::clone(&callback_calls);
        let mut registration = register_sink::<ConcurrentFields, _>(move |_| {
            initial_calls.fetch_add(1, Ordering::Relaxed);
        })
        .expect("register concurrent sink");

        let mut workers = Vec::new();
        for _ in 0..THREADS {
            workers.push(std::thread::spawn(|| {
                for _ in 0..CALLS {
                    dispatch(|| EdenLog::new(LogLevel::Info, "concurrent", &LogContext::<ConcurrentFields>::new(), LogAudience::Internal));
                }
            }));
        }
        for _ in 0..64 {
            let replacement_calls = Arc::clone(&callback_calls);
            registration
                .replace(move |_| {
                    replacement_calls.fetch_add(1, Ordering::Relaxed);
                })
                .expect("replace concurrent sink");
            std::thread::yield_now();
        }
        for worker in workers {
            worker.join().expect("dispatch worker");
        }

        assert_eq!(callback_calls.load(Ordering::Relaxed), THREADS * CALLS);
    }
}
