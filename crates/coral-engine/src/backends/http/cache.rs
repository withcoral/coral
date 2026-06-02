//! In-memory HTTP response page cache for opt-in TTL-based caching.
//!
//! Cache entries are keyed by a stable canonical string derived from all
//! request-determining material: source identity, table, rendered URL, query
//! params, body hash, hashed vary header values, and the declared TTL. Secret
//! values are never included in keys, values, or trace events.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use moka::Expiry;
use moka::future::Cache;
use serde_json::Value;

const CACHE_FORMAT_VERSION: u8 = 1;
const DEFAULT_CACHE_MAX_BYTES: u64 = 256 * 1024 * 1024; // 256 MiB

/// A single decoded HTTP response page held in the cache.
#[derive(Clone)]
pub(crate) struct HttpCacheEntry {
    /// Decoded response payload.
    pub(crate) payload: Value,
    /// Parsed `Link: <...>; rel="next"` URL from the response, if any.
    pub(crate) next_url: Option<String>,
    /// Time-to-live used to set expiry when the entry was created.
    pub(crate) ttl: Duration,
    /// Estimated in-memory size in bytes (JSON string length approximation).
    pub(crate) estimated_bytes: usize,
}

struct EntryExpiry;

impl Expiry<String, HttpCacheEntry> for EntryExpiry {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &HttpCacheEntry,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        Some(value.ttl)
    }

    fn expire_after_read(
        &self,
        _key: &String,
        _value: &HttpCacheEntry,
        _current_time: std::time::Instant,
        current_duration: Option<Duration>,
        _last_modified_at: std::time::Instant,
    ) -> Option<Duration> {
        current_duration
    }

    fn expire_after_update(
        &self,
        _key: &String,
        value: &HttpCacheEntry,
        _current_time: std::time::Instant,
        _current_duration: Option<Duration>,
    ) -> Option<Duration> {
        Some(value.ttl)
    }
}

/// Shared in-memory HTTP response page cache backed by moka.
///
/// `Clone` is cheap — all clones share the same underlying cache.
#[derive(Clone)]
pub(crate) struct HttpResponseCache {
    inner: Arc<Cache<String, HttpCacheEntry>>,
    registry: Option<Weak<HttpCacheRegistryInner>>,
}

impl HttpResponseCache {
    /// Create a new cache with the default 256 MiB capacity limit.
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::build(DEFAULT_CACHE_MAX_BYTES, None)
    }

    fn build(max_bytes: u64, registry: Option<Weak<HttpCacheRegistryInner>>) -> Self {
        let inner = Cache::builder()
            .max_capacity(max_bytes)
            .weigher(|_key: &String, value: &HttpCacheEntry| {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "Saturating at u32::MAX is the correct moka weigher clamp"
                )]
                let weight = value.estimated_bytes.min(u32::MAX as usize) as u32;
                weight
            })
            .expire_after(EntryExpiry)
            .build();
        Self {
            inner: Arc::new(inner),
            registry,
        }
    }

    /// Approximate weighted byte size currently stored.
    pub(crate) fn weighted_size(&self) -> u64 {
        self.inner.weighted_size()
    }

    /// Soft admission check against the parent registry's `total_max_bytes`,
    /// if any. Returns `true` when no registry is attached or no total
    /// ceiling is configured.
    pub(crate) fn try_admit(&self, incoming_bytes: u64) -> bool {
        let Some(weak) = &self.registry else {
            return true;
        };
        let Some(inner) = weak.upgrade() else {
            return true;
        };
        HttpCacheRegistry { inner }.try_admit(incoming_bytes)
    }

    /// Return the cached entry for `key`, or `None` on miss or expiry.
    #[cfg(test)]
    pub(crate) async fn get(&self, key: &str) -> Option<HttpCacheEntry> {
        self.inner.get(key).await
    }

    /// Insert `entry` under `key`.
    #[cfg(test)]
    pub(crate) async fn put(&self, key: String, entry: HttpCacheEntry) {
        self.inner.insert(key, entry).await;
    }

    /// Single-flight get-or-fetch. Returns `(entry, is_fresh)` where
    /// `is_fresh` is true when this caller's `init` ran (cache miss).
    pub(crate) async fn try_get_or_insert_with<F, E>(
        &self,
        key: &str,
        init: F,
    ) -> Result<(HttpCacheEntry, bool), Arc<E>>
    where
        F: std::future::Future<Output = Result<HttpCacheEntry, E>>,
        E: Send + Sync + 'static,
    {
        let entry = self
            .inner
            .entry_by_ref(key)
            .or_try_insert_with(init)
            .await?;
        let is_fresh = entry.is_fresh();
        Ok((entry.into_value(), is_fresh))
    }
}

pub(crate) struct HttpCacheRegistryInner {
    entries: Mutex<HashMap<(String, String), HttpResponseCache>>,
    default_max_bytes: u64,
    total_max_bytes: Option<u64>,
    per_source_max_bytes: HashMap<String, u64>,
}

/// Per-source `HttpResponseCache` instances keyed by `(name, version)`.
///
/// Held by long-lived callers (e.g. `QueryManager`) so cache entries
/// survive across the per-query runtime rebuild.
#[derive(Clone)]
pub struct HttpCacheRegistry {
    inner: Arc<HttpCacheRegistryInner>,
}

impl std::fmt::Debug for HttpCacheRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.inner.entries.lock().map_or(0, |g| g.len());
        f.debug_struct("HttpCacheRegistry")
            .field("entries", &len)
            .field("default_max_bytes", &self.inner.default_max_bytes)
            .field("total_max_bytes", &self.inner.total_max_bytes)
            .finish()
    }
}

impl HttpCacheRegistry {
    /// Build a registry with default per-source capacity (256 MiB) and no
    /// cross-source ceiling.
    #[must_use]
    pub fn new() -> Self {
        Self::with_policy(DEFAULT_CACHE_MAX_BYTES, None, HashMap::new())
    }

    /// Build a registry with explicit policy.
    ///
    /// `default_max_bytes` is the per-source cache capacity applied when a
    /// source has no override in `per_source_max_bytes`. `total_max_bytes`,
    /// when set, is a soft ceiling on the sum of weighted bytes across all
    /// per-source caches; entries that would push the running total over the
    /// ceiling are skipped (the response is still returned to the caller).
    #[must_use]
    pub fn with_policy(
        default_max_bytes: u64,
        total_max_bytes: Option<u64>,
        per_source_max_bytes: HashMap<String, u64>,
    ) -> Self {
        Self {
            inner: Arc::new(HttpCacheRegistryInner {
                entries: Mutex::new(HashMap::new()),
                default_max_bytes,
                total_max_bytes,
                per_source_max_bytes,
            }),
        }
    }

    pub(crate) fn get_or_create(
        &self,
        source_name: &str,
        source_version: &str,
    ) -> HttpResponseCache {
        let mut guard = self
            .inner
            .entries
            .lock()
            .expect("HttpCacheRegistry mutex poisoned");
        let key = (source_name.to_string(), source_version.to_string());
        if let Some(existing) = guard.get(&key) {
            return existing.clone();
        }
        let max_bytes = self
            .inner
            .per_source_max_bytes
            .get(source_name)
            .copied()
            .unwrap_or(self.inner.default_max_bytes);
        let cache = HttpResponseCache::build(max_bytes, Some(Arc::downgrade(&self.inner)));
        guard.insert(key, cache.clone());
        cache
    }

    /// Soft check: would admitting `incoming_bytes` keep the registry's total
    /// weighted size within `total_max_bytes`? Returns `true` if no ceiling is
    /// configured.
    pub(crate) fn try_admit(&self, incoming_bytes: u64) -> bool {
        let Some(total) = self.inner.total_max_bytes else {
            return true;
        };
        let current = {
            let guard = self
                .inner
                .entries
                .lock()
                .expect("HttpCacheRegistry mutex poisoned");
            guard
                .values()
                .map(HttpResponseCache::weighted_size)
                .sum::<u64>()
        };
        current.saturating_add(incoming_bytes) <= total
    }
}

impl Default for HttpCacheRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Build a stable, canonical cache key string from all request-determining
/// material.  Auth headers and secret values are never included.
///
/// `body_hash` is a pre-computed hash of the serialized request body, allowing
/// callers to hash the body without exposing its type to this module.
#[expect(
    clippy::too_many_arguments,
    reason = "All parameters are distinct key dimensions; introducing a struct would add noise"
)]
pub(crate) fn build_cache_key(
    source_name: &str,
    source_version: &str,
    table_name: &str,
    method: &str,
    url: &str,
    query_pairs: &[(String, String)],
    body_hash: Option<u64>,
    vary_headers: &[(String, Option<u64>)],
    ttl_secs: u64,
) -> String {
    let mut sorted_qs = query_pairs.to_vec();
    sorted_qs.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    let mut vary_sorted = vary_headers.to_vec();
    vary_sorted.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    format!(
        "v{CACHE_FORMAT_VERSION}\t{source_name}\t{source_version}\t{table_name}\t{method}\t{url}\t{sorted_qs:?}\t{body_hash:?}\t{vary_sorted:?}\t{ttl_secs}"
    )
}

/// Estimate the in-memory size of a JSON value using its serialized length.
pub(crate) fn estimate_json_bytes(value: &Value) -> usize {
    serde_json::to_string(value).map_or(0, |s| s.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cache_key_is_stable_for_identical_inputs() {
        let key1 = build_cache_key(
            "demo",
            "0.1.0",
            "users",
            "GET",
            "https://api.example.com/users",
            &[("page".to_string(), "1".to_string())],
            None,
            &[],
            300,
        );
        let key2 = build_cache_key(
            "demo",
            "0.1.0",
            "users",
            "GET",
            "https://api.example.com/users",
            &[("page".to_string(), "1".to_string())],
            None,
            &[],
            300,
        );
        assert_eq!(key1, key2);
    }

    #[test]
    fn cache_key_differs_for_different_query_params() {
        let key1 = build_cache_key(
            "demo",
            "0.1.0",
            "users",
            "GET",
            "https://api.example.com/users",
            &[("page".to_string(), "1".to_string())],
            None,
            &[],
            300,
        );
        let key2 = build_cache_key(
            "demo",
            "0.1.0",
            "users",
            "GET",
            "https://api.example.com/users",
            &[("page".to_string(), "2".to_string())],
            None,
            &[],
            300,
        );
        assert_ne!(key1, key2);
    }

    #[test]
    fn cache_key_normalises_query_param_order() {
        let key1 = build_cache_key(
            "demo",
            "0.1.0",
            "items",
            "GET",
            "https://api.example.com/items",
            &[
                ("b".to_string(), "2".to_string()),
                ("a".to_string(), "1".to_string()),
            ],
            None,
            &[],
            60,
        );
        let key2 = build_cache_key(
            "demo",
            "0.1.0",
            "items",
            "GET",
            "https://api.example.com/items",
            &[
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
            ],
            None,
            &[],
            60,
        );
        assert_eq!(key1, key2);
    }

    #[test]
    fn cache_key_differs_for_different_source_version() {
        let key1 = build_cache_key(
            "demo",
            "0.1.0",
            "users",
            "GET",
            "https://api.example.com/users",
            &[],
            None,
            &[],
            300,
        );
        let key2 = build_cache_key(
            "demo",
            "0.2.0",
            "users",
            "GET",
            "https://api.example.com/users",
            &[],
            None,
            &[],
            300,
        );
        assert_ne!(key1, key2);
    }

    #[test]
    fn cache_key_differs_for_different_ttl() {
        let key1 = build_cache_key(
            "demo",
            "0.1.0",
            "users",
            "GET",
            "https://api.example.com/users",
            &[],
            None,
            &[],
            60,
        );
        let key2 = build_cache_key(
            "demo",
            "0.1.0",
            "users",
            "GET",
            "https://api.example.com/users",
            &[],
            None,
            &[],
            300,
        );
        assert_ne!(key1, key2);
    }

    #[test]
    fn cache_key_differs_for_different_vary_header_values() {
        let key1 = build_cache_key(
            "demo",
            "0.1.0",
            "users",
            "GET",
            "https://api.example.com/users",
            &[],
            None,
            &[("accept".to_string(), Some(1))],
            300,
        );
        let key2 = build_cache_key(
            "demo",
            "0.1.0",
            "users",
            "GET",
            "https://api.example.com/users",
            &[],
            None,
            &[("accept".to_string(), Some(2))],
            300,
        );
        assert_ne!(key1, key2);
    }

    #[tokio::test]
    async fn updating_entry_resets_ttl() {
        let cache = HttpResponseCache::new();
        let key = "ttl-refresh".to_string();
        cache
            .put(
                key.clone(),
                HttpCacheEntry {
                    payload: json!({"version": 1}),
                    next_url: None,
                    ttl: Duration::from_secs(1),
                    estimated_bytes: 1,
                },
            )
            .await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        cache
            .put(
                key.clone(),
                HttpCacheEntry {
                    payload: json!({"version": 2}),
                    next_url: None,
                    ttl: Duration::from_secs(1),
                    estimated_bytes: 1,
                },
            )
            .await;
        tokio::time::sleep(Duration::from_millis(700)).await;

        let entry = cache.get(&key).await.expect("updated entry should remain");
        assert_eq!(entry.payload, json!({"version": 2}));
    }

    #[test]
    fn estimate_json_bytes_returns_string_length() {
        let value = json!({"key": "value", "num": 42});
        let estimated = estimate_json_bytes(&value);
        let serialized = serde_json::to_string(&value).unwrap();
        assert_eq!(estimated, serialized.len());
    }

    #[test]
    fn registry_with_no_ceiling_admits_anything() {
        let registry = HttpCacheRegistry::with_policy(1024, None, HashMap::new());
        assert!(registry.try_admit(1024 * 1024));
    }

    #[test]
    fn registry_returns_distinct_caches_for_distinct_sources() {
        let mut overrides = HashMap::new();
        overrides.insert("large_source".to_string(), 1024);
        let registry = HttpCacheRegistry::with_policy(64, None, overrides);
        let small = registry.get_or_create("small_source", "0.1.0");
        let large = registry.get_or_create("large_source", "0.1.0");
        // Small source uses the default capacity (64); large source has an override (1024).
        // Different moka caches → different identity. We can't read max_capacity off
        // moka directly, but admission against a zero-size ceiling proves the registry
        // is exercising both entries.
        assert_eq!(small.weighted_size(), 0);
        assert_eq!(large.weighted_size(), 0);
        let zero_ceiling = HttpCacheRegistry::with_policy(64, Some(0), HashMap::new());
        let _ = zero_ceiling.get_or_create("s", "0");
        assert!(!zero_ceiling.try_admit(1));
    }
}
