//! Shared in-memory cache primitives for backend-owned response caches.

use std::collections::{HashMap, HashSet, hash_map::DefaultHasher};
use std::hash::{Hash as _, Hasher as _};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use moka::Expiry;
use moka::future::Cache;

pub(crate) const DEFAULT_CACHE_MAX_BYTES: u64 = 256 * 1024 * 1024; // 256 MiB

/// Value stored in a backend cache.
pub(crate) trait CacheValue: Clone + Send + Sync + 'static {
    /// Entry time-to-live.
    fn ttl(&self) -> Duration;

    /// Approximate weighted byte size used for cache admission and eviction.
    fn estimated_bytes(&self) -> usize;
}

/// Stable bucket identity for one backend/source/cache namespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CacheBucketKey {
    namespace: String,
    backend: &'static str,
    source_name: String,
    source_version: String,
}

impl CacheBucketKey {
    pub(crate) fn new(
        namespace: impl Into<String>,
        backend: &'static str,
        source_name: impl Into<String>,
        source_version: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            backend,
            source_name: source_name.into(),
            source_version: source_version.into(),
        }
    }

    fn source_name(&self) -> &str {
        &self.source_name
    }

    fn same_source_family(&self, other: &Self) -> bool {
        self.namespace == other.namespace
            && self.backend == other.backend
            && self.source_name == other.source_name
    }
}

struct EntryExpiry<V>(PhantomData<V>);

impl<V> Default for EntryExpiry<V> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<V> Expiry<String, V> for EntryExpiry<V>
where
    V: CacheValue,
{
    fn expire_after_create(
        &self,
        _key: &String,
        value: &V,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        Some(value.ttl())
    }

    fn expire_after_read(
        &self,
        _key: &String,
        _value: &V,
        _current_time: std::time::Instant,
        current_duration: Option<Duration>,
        _last_modified_at: std::time::Instant,
    ) -> Option<Duration> {
        current_duration
    }

    fn expire_after_update(
        &self,
        _key: &String,
        value: &V,
        _current_time: std::time::Instant,
        _current_duration: Option<Duration>,
    ) -> Option<Duration> {
        Some(value.ttl())
    }
}

/// Shared in-memory cache bucket backed by Moka.
///
/// `Clone` is cheap; all clones share the same underlying cache.
#[derive(Clone)]
pub(crate) struct CacheBucket<V>
where
    V: CacheValue,
{
    inner: Arc<Cache<String, V>>,
    // Moka can report an expired key as a miss while its weight remains pending;
    // this side index lets admission explicitly invalidate those stale entries.
    known_keys: Arc<Mutex<HashSet<String>>>,
    registry: Option<Weak<CacheRegistryInner<V>>>,
}

impl<V> CacheBucket<V>
where
    V: CacheValue,
{
    /// Create a new standalone cache with the default 256 MiB capacity limit.
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::build(DEFAULT_CACHE_MAX_BYTES, None)
    }

    fn build(max_bytes: u64, registry: Option<Weak<CacheRegistryInner<V>>>) -> Self {
        let known_keys = Arc::new(Mutex::new(HashSet::<String>::new()));
        let listener_keys = Arc::clone(&known_keys);
        let inner = Cache::builder()
            .max_capacity(max_bytes)
            .weigher(|_key: &String, value: &V| {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "Saturating at u32::MAX is the correct moka weigher clamp"
                )]
                let weight = value.estimated_bytes().min(u32::MAX as usize) as u32;
                weight
            })
            .expire_after(EntryExpiry::<V>::default())
            .eviction_listener(move |key, _value, _cause| {
                if let Ok(mut guard) = listener_keys.lock() {
                    guard.remove(key.as_str());
                }
            })
            .build();
        Self {
            inner: Arc::new(inner),
            known_keys,
            registry,
        }
    }

    /// Approximate weighted byte size currently stored.
    pub(crate) fn weighted_size(&self) -> u64 {
        self.inner.weighted_size()
    }

    fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }

    fn has_external_refs(&self) -> bool {
        Arc::strong_count(&self.inner) > 1
    }

    async fn remove_expired_known_entries(&self) {
        self.inner.run_pending_tasks().await;

        let known_keys = self.known_keys();
        let mut expired_keys = Vec::new();
        for key in known_keys {
            if !self.inner.contains_key(&key) {
                self.inner.invalidate(&key).await;
                expired_keys.push(key);
            }
        }
        if expired_keys.is_empty() {
            return;
        }

        self.inner.run_pending_tasks().await;
        let mut guard = self.known_keys.lock().expect("cache key mutex poisoned");
        for key in expired_keys {
            guard.remove(&key);
        }
    }

    fn known_keys(&self) -> Vec<String> {
        self.known_keys
            .lock()
            .expect("cache key mutex poisoned")
            .iter()
            .cloned()
            .collect()
    }

    fn remember_key(&self, key: &str) {
        self.known_keys
            .lock()
            .expect("cache key mutex poisoned")
            .insert(key.to_string());
    }

    /// Soft admission check against the parent registry's `total_max_bytes`,
    /// if any. Returns `true` when no registry is attached or no total
    /// ceiling is configured.
    pub(crate) async fn try_admit(&self, incoming_bytes: u64) -> bool {
        let Some(weak) = &self.registry else {
            return true;
        };
        let Some(inner) = weak.upgrade() else {
            return true;
        };
        CacheRegistry { inner }.try_admit(incoming_bytes).await
    }

    /// Return the cached entry for `key`, or `None` on miss or expiry.
    #[cfg(test)]
    pub(crate) async fn get(&self, key: &str) -> Option<V> {
        self.inner.get(key).await
    }

    /// Insert `entry` under `key`.
    #[cfg(test)]
    pub(crate) async fn put(&self, key: String, entry: V) {
        self.inner.insert(key.clone(), entry).await;
        self.inner.run_pending_tasks().await;
        self.remember_key(&key);
    }

    /// Single-flight get-or-fetch. Returns `(entry, is_fresh)` where
    /// `is_fresh` is true when this caller's `init` ran (cache miss).
    pub(crate) async fn try_get_or_insert_with<F, E>(
        &self,
        key: &str,
        init: F,
    ) -> Result<(V, bool), Arc<E>>
    where
        F: std::future::Future<Output = Result<V, E>>,
        E: Send + Sync + 'static,
    {
        let entry = self
            .inner
            .entry_by_ref(key)
            .or_try_insert_with(init)
            .await?;
        let is_fresh = entry.is_fresh();
        let value = entry.into_value();
        self.remember_key(key);
        Ok((value, is_fresh))
    }
}

struct CacheRegistryInner<V>
where
    V: CacheValue,
{
    entries: Mutex<HashMap<CacheBucketKey, CacheBucket<V>>>,
    default_max_bytes: u64,
    total_max_bytes: Option<u64>,
    per_source_max_bytes: HashMap<String, u64>,
}

/// Per-source cache buckets keyed by backend/source identity and namespace.
#[derive(Clone)]
pub(crate) struct CacheRegistry<V>
where
    V: CacheValue,
{
    inner: Arc<CacheRegistryInner<V>>,
}

impl<V> std::fmt::Debug for CacheRegistry<V>
where
    V: CacheValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.inner.entries.lock().map_or(0, |g| g.len());
        f.debug_struct("CacheRegistry")
            .field("entries", &len)
            .field("default_max_bytes", &self.inner.default_max_bytes)
            .field("total_max_bytes", &self.inner.total_max_bytes)
            .finish()
    }
}

impl<V> CacheRegistry<V>
where
    V: CacheValue,
{
    /// Build a registry with default per-source capacity (256 MiB) and no
    /// cross-source ceiling.
    pub(crate) fn new() -> Self {
        Self::with_policy(DEFAULT_CACHE_MAX_BYTES, None, HashMap::new())
    }

    /// Build a registry with explicit policy.
    pub(crate) fn with_policy(
        default_max_bytes: u64,
        total_max_bytes: Option<u64>,
        per_source_max_bytes: HashMap<String, u64>,
    ) -> Self {
        Self {
            inner: Arc::new(CacheRegistryInner {
                entries: Mutex::new(HashMap::new()),
                default_max_bytes,
                total_max_bytes,
                per_source_max_bytes,
            }),
        }
    }

    pub(crate) async fn get_or_create(&self, key: CacheBucketKey) -> CacheBucket<V> {
        {
            let guard = self
                .inner
                .entries
                .lock()
                .expect("cache registry mutex poisoned");
            if let Some(existing) = guard.get(&key) {
                return existing.clone();
            }
        }

        self.prune_empty_source_family_buckets(&key).await;

        let mut guard = self
            .inner
            .entries
            .lock()
            .expect("cache registry mutex poisoned");
        if let Some(existing) = guard.get(&key) {
            return existing.clone();
        }

        let max_bytes = self
            .inner
            .per_source_max_bytes
            .get(key.source_name())
            .copied()
            .unwrap_or(self.inner.default_max_bytes);
        let cache = CacheBucket::build(max_bytes, Some(Arc::downgrade(&self.inner)));
        guard.insert(key, cache.clone());
        cache
    }

    async fn prune_empty_source_family_buckets(&self, keep: &CacheBucketKey) {
        let buckets = {
            let guard = self
                .inner
                .entries
                .lock()
                .expect("cache registry mutex poisoned");
            guard
                .iter()
                .filter(|(key, _bucket)| key.same_source_family(keep))
                .map(|(_key, bucket)| bucket.clone())
                .collect::<Vec<_>>()
        };
        for bucket in &buckets {
            bucket.remove_expired_known_entries().await;
        }
        drop(buckets);

        let mut guard = self
            .inner
            .entries
            .lock()
            .expect("cache registry mutex poisoned");
        guard.retain(|key, bucket| {
            key == keep
                || !key.same_source_family(keep)
                || bucket.has_external_refs()
                || bucket.entry_count() > 0
                || bucket.weighted_size() > 0
        });
    }

    async fn run_pending_tasks_for_all_buckets(&self) {
        let buckets = {
            let guard = self
                .inner
                .entries
                .lock()
                .expect("cache registry mutex poisoned");
            guard.values().cloned().collect::<Vec<_>>()
        };
        for bucket in &buckets {
            bucket.remove_expired_known_entries().await;
        }
    }

    fn prune_empty_buckets(&self) {
        let mut guard = self
            .inner
            .entries
            .lock()
            .expect("cache registry mutex poisoned");
        guard.retain(|_key, bucket| {
            bucket.has_external_refs() || bucket.entry_count() > 0 || bucket.weighted_size() > 0
        });
    }

    /// Soft check: would admitting `incoming_bytes` keep the registry's total
    /// weighted size within `total_max_bytes`? Returns `true` if no ceiling is
    /// configured.
    pub(crate) async fn try_admit(&self, incoming_bytes: u64) -> bool {
        let Some(total) = self.inner.total_max_bytes else {
            return true;
        };
        self.run_pending_tasks_for_all_buckets().await;
        self.prune_empty_buckets();
        let current = {
            let guard = self
                .inner
                .entries
                .lock()
                .expect("cache registry mutex poisoned");
            guard.values().map(CacheBucket::weighted_size).sum::<u64>()
        };
        current.saturating_add(incoming_bytes) <= total
    }

    #[cfg(test)]
    pub(crate) fn bucket_count(&self) -> usize {
        self.inner
            .entries
            .lock()
            .expect("cache registry mutex poisoned")
            .len()
    }
}

impl<V> Default for CacheRegistry<V>
where
    V: CacheValue,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Hash request/cache identity material without storing raw values in keys.
pub(crate) fn hash_cache_bytes(value: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}
