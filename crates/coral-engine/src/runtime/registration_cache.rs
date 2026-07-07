//! Cross-build reuse of backend source registrations.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crate::backends::BackendRegistration;

/// Default entry lifetime before a cached registration is refreshed.
const DEFAULT_TTL: Duration = Duration::from_mins(5);

/// Reuses backend registration artifacts across runtime builds.
///
/// Runtime builds happen per query, so backends whose registration performs
/// expensive work (for example remote catalog discovery) would repeat that
/// work on every query. A long-lived owner (the application layer) holds one
/// cache and passes it through [`crate::EngineExtensions`]; registration then
/// replays cached artifacts instead of re-registering.
///
/// Entries are keyed by source name and validated against the fingerprint the
/// backend reports via `CompiledBackendSource::registration_fingerprint`. Only
/// backends that report a fingerprint participate; every other source
/// re-registers on each runtime build exactly as before.
///
/// The fingerprint only tracks source configuration, so remote schema changes
/// (for example new tables in a database) are invisible to it. Entries
/// therefore also expire after a time-to-live: an expired entry is returned
/// as [`CacheLookup::Stale`] so registration can rebuild it off-lock and swap
/// the entry, while runtime builds that already hold the old artifacts keep
/// working unchanged.
pub struct RegistrationCache {
    ttl: Duration,
    entries: Mutex<HashMap<String, CachedRegistration>>,
}

struct CachedRegistration {
    fingerprint: String,
    registration: BackendRegistration,
    refreshed_at: Instant,
    #[cfg(test)]
    force_stale: bool,
}

impl CachedRegistration {
    fn is_fresh(&self, ttl: Duration) -> bool {
        #[cfg(test)]
        if self.force_stale {
            return false;
        }
        self.refreshed_at.elapsed() < ttl
    }
}

/// Outcome of a cache lookup for a fingerprint-matching entry.
pub(crate) enum CacheLookup {
    /// The entry is within its time-to-live and can be used as is.
    Fresh(BackendRegistration),
    /// The entry outlived its time-to-live. Callers should re-register the
    /// source and fall back to this registration only when that fails.
    Stale(BackendRegistration),
}

impl std::fmt::Debug for RegistrationCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistrationCache")
            .field("ttl", &self.ttl)
            .field("sources", &self.entries().keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl Default for RegistrationCache {
    fn default() -> Self {
        Self::with_ttl(DEFAULT_TTL)
    }
}

impl RegistrationCache {
    /// Creates an empty registration cache with the default time-to-live.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty registration cache whose entries expire after `ttl`.
    #[must_use]
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn entries(&self) -> MutexGuard<'_, HashMap<String, CachedRegistration>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Returns the cached registration for `source_name` when its fingerprint
    /// still matches, marked fresh or stale by the entry's age.
    pub(crate) fn lookup(&self, source_name: &str, fingerprint: &str) -> Option<CacheLookup> {
        let entries = self.entries();
        let entry = entries.get(source_name)?;
        if entry.fingerprint != fingerprint {
            return None;
        }
        let registration = entry.registration.clone();
        Some(if entry.is_fresh(self.ttl) {
            CacheLookup::Fresh(registration)
        } else {
            CacheLookup::Stale(registration)
        })
    }

    /// Stores `registration` for `source_name`, replacing any previous entry
    /// and restarting its time-to-live.
    pub(crate) fn store(
        &self,
        source_name: &str,
        fingerprint: &str,
        registration: &BackendRegistration,
    ) {
        self.entries().insert(
            source_name.to_string(),
            CachedRegistration {
                fingerprint: fingerprint.to_string(),
                registration: registration.clone(),
                refreshed_at: Instant::now(),
                #[cfg(test)]
                force_stale: false,
            },
        );
    }

    /// Restarts the time-to-live of the entry for `source_name`.
    ///
    /// Used after a failed refresh so a stale entry keeps serving queries and
    /// the refresh retries once per time-to-live window instead of on every
    /// runtime build.
    pub(crate) fn touch(&self, source_name: &str) {
        if let Some(entry) = self.entries().get_mut(source_name) {
            entry.refreshed_at = Instant::now();
            #[cfg(test)]
            {
                entry.force_stale = false;
            }
        }
    }

    /// Drops cached entries for sources outside `installed`.
    ///
    /// The long-lived owner calls this with the full set of installed source
    /// names so entries for removed sources release their registration
    /// artifacts. Per-build source selections must not drive eviction: a
    /// build that selects a subset (for example single-source validation)
    /// says nothing about whether the other sources still exist.
    pub fn retain_sources(&self, installed: &HashSet<&str>) {
        self.entries()
            .retain(|source_name, _| installed.contains(source_name.as_str()));
    }

    /// Marks the entry for `source_name` as expired regardless of its age.
    #[cfg(test)]
    pub(crate) fn force_stale(&self, source_name: &str) {
        if let Some(entry) = self.entries().get_mut(source_name) {
            entry.force_stale = true;
        }
    }
}
