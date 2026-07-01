//! Optional structured log sink for non-blocking telemetry export.
//!
//! Sinks are keyed by the concrete `RequestFields` type, so each application
//! that uses its own `R` gets its own sink slot. At most one sink may be
//! installed per `R`.
//!
//! This module is only compiled when the `serde` feature is enabled, because
//! the typed `EdenLog<R>` values that sinks receive carry `Serialize` bounds
//! in practice.

use crate::fields::RequestFields;
use crate::schema::EdenLog;
use serde::{Deserialize, Serialize};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{OnceLock, RwLock};

type SinkAny = dyn Any + Send + Sync + 'static;

static SINKS: OnceLock<RwLock<HashMap<TypeId, Box<SinkAny>>>> = OnceLock::new();

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
    R: RequestFields + Serialize + for<'d> Deserialize<'d>,
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
/// no log construction — when no sink has ever been installed.
#[inline]
pub(crate) fn dispatch<R>(build_log: impl FnOnce() -> EdenLog<R>)
where
    R: RequestFields,
{
    if !ANY_INSTALLED.load(Ordering::Relaxed) {
        return;
    }
    let Some(lock) = SINKS.get() else { return };
    let Ok(guard) = lock.read() else { return };
    let Some(slot) = guard.get(&TypeId::of::<R>()) else { return };
    let Some(slot) = slot.downcast_ref::<SinkSlot<R>>() else { return };
    (slot.0)(build_log());
}
