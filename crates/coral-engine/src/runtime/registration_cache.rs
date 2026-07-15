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
    refresh_status: RefreshStatus,
    #[cfg(test)]
    force_stale: bool,
}

#[derive(Clone, Copy)]
enum RefreshStatus {
    Idle,
    Refreshing { started_at: Instant },
}

impl CachedRegistration {
    fn new(fingerprint: &str, registration: &BackendRegistration) -> Self {
        Self {
            fingerprint: fingerprint.to_string(),
            registration: registration.clone(),
            refreshed_at: Instant::now(),
            refresh_status: RefreshStatus::Idle,
            #[cfg(test)]
            force_stale: false,
        }
    }

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
    /// The entry outlived its time-to-live and this caller claimed the refresh.
    /// Callers should re-register the source and fall back to this registration
    /// only when that fails.
    Stale {
        registration: BackendRegistration,
        claim: RegistrationRefreshClaim,
    },
    /// Another caller is already refreshing this stale entry. Reuse the cached
    /// registration rather than starting another backend registration.
    Refreshing(BackendRegistration),
}

/// Handle for the refresh attempt that moved an entry into `Refreshing`.
pub(crate) struct RegistrationRefreshClaim {
    source_name: String,
    fingerprint: String,
    started_at: Instant,
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
        let mut entries = self.entries();
        let entry = entries.get_mut(source_name)?;
        if entry.fingerprint != fingerprint {
            return None;
        }
        let registration = entry.registration.clone();
        if entry.is_fresh(self.ttl) {
            return Some(CacheLookup::Fresh(registration));
        }
        match entry.refresh_status {
            RefreshStatus::Idle => {
                let started_at = Instant::now();
                entry.refresh_status = RefreshStatus::Refreshing { started_at };
                Some(CacheLookup::Stale {
                    registration,
                    claim: RegistrationRefreshClaim {
                        source_name: source_name.to_string(),
                        fingerprint: fingerprint.to_string(),
                        started_at,
                    },
                })
            }
            RefreshStatus::Refreshing { .. } => Some(CacheLookup::Refreshing(registration)),
        }
    }

    /// Stores `registration` for `source_name`, replacing any previous entry
    /// and restarting its time-to-live.
    pub(crate) fn store(
        &self,
        source_name: &str,
        fingerprint: &str,
        registration: &BackendRegistration,
    ) {
        let mut entries = self.entries();
        entries.insert(
            source_name.to_string(),
            CachedRegistration::new(fingerprint, registration),
        );
    }

    /// Stores a successful claimed refresh if the claim still owns the entry.
    pub(crate) fn refresh_succeeded(
        &self,
        claim: &RegistrationRefreshClaim,
        registration: &BackendRegistration,
    ) {
        let mut entries = self.entries();
        let Some(entry) = entries.get_mut(&claim.source_name) else {
            return;
        };
        if !entry_matches_claim(entry, claim) {
            return;
        }
        *entry = CachedRegistration::new(&claim.fingerprint, registration);
    }

    /// Finishes a claimed refresh attempt that failed.
    ///
    /// Used after a failed refresh so a stale entry keeps serving queries and
    /// the refresh retries once per time-to-live window instead of on every
    /// runtime build when the caller chooses to defer retry. The claim check
    /// prevents an old failed refresh from touching a newer entry that was
    /// stored while the refresh was in flight.
    pub(crate) fn refresh_failed(&self, claim: &RegistrationRefreshClaim, defer_retry: bool) {
        let mut entries = self.entries();
        let Some(entry) = entries.get_mut(&claim.source_name) else {
            return;
        };
        if !entry_matches_claim(entry, claim) {
            return;
        }
        if defer_retry {
            entry.refreshed_at = Instant::now();
        }
        entry.refresh_status = RefreshStatus::Idle;
        #[cfg(test)]
        {
            if defer_retry {
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

fn entry_matches_claim(entry: &CachedRegistration, claim: &RegistrationRefreshClaim) -> bool {
    entry.fingerprint == claim.fingerprint
        && matches!(
            entry.refresh_status,
            RefreshStatus::Refreshing { started_at } if started_at == claim.started_at
        )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::backends::BackendRegistration;
    use crate::runtime::registration_cache::{CacheLookup, RegistrationCache};

    fn registration() -> BackendRegistration {
        BackendRegistration {
            schemas: Vec::new(),
            catalogs: Vec::new(),
        }
    }

    #[test]
    fn stale_lookup_claims_refresh_and_ignores_late_old_claims() {
        let registration = registration();

        {
            let cache = RegistrationCache::with_ttl(Duration::from_hours(1));
            cache.store("fake", "v1", &registration);
            cache.force_stale("fake");

            let claim = match cache.lookup("fake", "v1").expect("stale entry") {
                CacheLookup::Stale { claim, .. } => claim,
                _ => panic!("stale entry should be claimed by first lookup"),
            };
            assert!(matches!(
                cache.lookup("fake", "v1"),
                Some(CacheLookup::Refreshing(_))
            ));

            cache.store("fake", "v2", &registration);
            cache.force_stale("fake");
            cache.refresh_failed(&claim, true);

            assert!(
                matches!(cache.lookup("fake", "v2"), Some(CacheLookup::Stale { .. })),
                "a failed refresh from v1 must not touch the newer v2 entry"
            );
        }

        {
            let cache = RegistrationCache::with_ttl(Duration::from_hours(1));
            cache.store("fake", "v1", &registration);
            cache.force_stale("fake");

            let claim = match cache.lookup("fake", "v1").expect("stale entry") {
                CacheLookup::Stale { claim, .. } => claim,
                _ => panic!("stale entry should be claimed by first lookup"),
            };

            cache.store("fake", "v2", &registration);
            cache.force_stale("fake");
            cache.refresh_succeeded(&claim, &registration);

            assert!(
                matches!(cache.lookup("fake", "v2"), Some(CacheLookup::Stale { .. })),
                "a successful refresh from v1 must not overwrite the newer v2 entry"
            );
        }
    }
}
