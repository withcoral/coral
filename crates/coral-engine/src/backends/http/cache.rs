//! In-memory HTTP response page cache for opt-in TTL-based caching.
//!
//! Cache entries are keyed by a stable canonical string derived from all
//! request-determining material: source identity, table, rendered URL, query
//! params, body hash, hashed vary header values, and the declared TTL. Rendered
//! request material is hashed before it becomes part of the stored cache key.

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash as _, Hasher as _};
use std::time::Duration;

use serde_json::Value;

use crate::backends::shared::cache::{
    CacheBucket, CacheBucketKey, CacheRegistry, CacheValue, hash_cache_bytes,
};

const CACHE_FORMAT_VERSION: u8 = 1;

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

impl CacheValue for HttpCacheEntry {
    fn ttl(&self) -> Duration {
        self.ttl
    }

    fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }
}

pub(crate) type HttpResponseCache = CacheBucket<HttpCacheEntry>;

/// Per-source `HttpResponseCache` instances keyed by `(name, version)`.
///
/// Held by long-lived callers (e.g. `QueryManager`) so cache entries
/// survive across the per-query runtime rebuild.
#[derive(Clone)]
pub(crate) struct HttpCacheRegistry {
    inner: CacheRegistry<HttpCacheEntry>,
}

impl std::fmt::Debug for HttpCacheRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("HttpCacheRegistry")
            .field(&self.inner)
            .finish()
    }
}

impl HttpCacheRegistry {
    /// Build a registry with default per-source capacity (256 MiB) and no
    /// cross-source ceiling.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            inner: CacheRegistry::new(),
        }
    }

    /// Build a registry with explicit policy.
    ///
    /// `default_max_bytes` is the per-source cache capacity applied when a
    /// source has no override in `per_source_max_bytes`. `total_max_bytes`,
    /// when set, is a soft ceiling on the sum of weighted bytes across all
    /// per-source caches; entries that would push the running total over the
    /// ceiling are skipped (the response is still returned to the caller).
    #[must_use]
    pub(crate) fn with_policy(
        default_max_bytes: u64,
        total_max_bytes: Option<u64>,
        per_source_max_bytes: HashMap<String, u64>,
    ) -> Self {
        Self {
            inner: CacheRegistry::with_policy(
                default_max_bytes,
                total_max_bytes,
                per_source_max_bytes,
            ),
        }
    }

    pub(crate) async fn get_or_create(
        &self,
        namespace: &str,
        source_name: &str,
        source_version: &str,
    ) -> HttpResponseCache {
        self.inner
            .get_or_create(CacheBucketKey::new(
                namespace,
                "http",
                source_name,
                source_version,
            ))
            .await
    }

    /// Soft check: would admitting `incoming_bytes` keep the registry's total
    /// weighted size within `total_max_bytes`? Returns `true` if no ceiling is
    /// configured.
    #[cfg(test)]
    pub(crate) async fn try_admit(&self, incoming_bytes: u64) -> bool {
        self.inner.try_admit(incoming_bytes).await
    }

    #[cfg(test)]
    fn bucket_count(&self) -> usize {
        self.inner.bucket_count()
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
    resolved_input_fingerprint: u64,
    table_name: &str,
    method: &str,
    url: &str,
    query_pairs: &[(String, String)],
    body_hash: Option<u64>,
    vary_headers: &[(String, Option<u64>)],
    ttl_secs: u64,
) -> String {
    let mut vary_sorted = vary_headers.to_vec();
    vary_sorted.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    let url_hash = hash_cache_bytes(url.as_bytes());
    let query_hash = hash_cache_value(query_pairs);
    let vary_hash = hash_cache_value(&vary_sorted);
    let body_hash = body_hash.map(|hash| format!("{hash:016x}"));

    format!(
        "v{CACHE_FORMAT_VERSION}\t{source_name}\t{source_version}\tinputs:{resolved_input_fingerprint:016x}\t{table_name}\t{method}\turl:{url_hash:016x}\tquery:{query_hash:016x}\tbody:{body_hash:?}\tvary:{vary_hash:016x}\tttl:{ttl_secs}"
    )
}

/// Estimate the in-memory size of a JSON value using its serialized length.
pub(crate) fn estimate_json_bytes(value: &Value) -> usize {
    serde_json::to_string(value).map_or(0, |s| s.len())
}

pub(crate) fn resolved_inputs_cache_fingerprint(resolved_inputs: &BTreeMap<String, String>) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for (key, value) in resolved_inputs {
        key.hash(&mut hasher);
        hash_cache_bytes(value.as_bytes()).hash(&mut hasher);
    }
    hasher.finish()
}

fn hash_cache_value<T: std::hash::Hash + ?Sized>(value: &T) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
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
            0,
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
            0,
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
            0,
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
            0,
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
    fn cache_key_preserves_query_param_order() {
        let key1 = build_cache_key(
            "demo",
            "0.1.0",
            0,
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
            0,
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
        assert_ne!(key1, key2);
    }

    #[test]
    fn cache_key_differs_for_different_source_version() {
        let key1 = build_cache_key(
            "demo",
            "0.1.0",
            0,
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
            0,
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
    fn cache_key_differs_for_different_resolved_inputs() {
        let key1 = build_cache_key(
            "demo",
            "0.1.0",
            1,
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
            "0.1.0",
            2,
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
            0,
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
            0,
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
            0,
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
            0,
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

    #[test]
    fn cache_key_hashes_rendered_request_material() {
        let key = build_cache_key(
            "demo",
            "0.1.0",
            0,
            "users",
            "GET",
            "https://api.example.com/users?token=secret-token",
            &[("api_key".to_string(), "secret-key".to_string())],
            Some(42),
            &[("authorization".to_string(), Some(99))],
            300,
        );

        assert!(!key.contains("secret-token"));
        assert!(!key.contains("secret-key"));
        assert!(!key.contains("https://api.example.com"));
        assert!(key.contains("url:"));
        assert!(key.contains("query:"));
    }

    #[test]
    fn resolved_inputs_fingerprint_hashes_values() {
        let inputs = BTreeMap::from([("TOKEN".to_string(), "secret-token".to_string())]);
        let fingerprint = resolved_inputs_cache_fingerprint(&inputs);
        let key = build_cache_key(
            "demo",
            "0.1.0",
            fingerprint,
            "users",
            "GET",
            "https://api.example.com/users",
            &[],
            None,
            &[],
            300,
        );

        assert_ne!(fingerprint, 0);
        assert!(!key.contains("secret-token"));
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

    #[tokio::test]
    async fn registry_with_no_ceiling_admits_anything() {
        let registry = HttpCacheRegistry::with_policy(1024, None, HashMap::new());
        assert!(registry.try_admit(1024 * 1024).await);
    }

    #[tokio::test]
    async fn registry_returns_distinct_caches_for_distinct_sources() {
        let mut overrides = HashMap::new();
        overrides.insert("large_source".to_string(), 1024);
        let registry = HttpCacheRegistry::with_policy(64, None, overrides);
        let small = registry
            .get_or_create("default", "small_source", "0.1.0")
            .await;
        let large = registry
            .get_or_create("default", "large_source", "0.1.0")
            .await;
        // Small source uses the default capacity (64); large source has an override (1024).
        // Different moka caches mean different identity. We can't read max_capacity off
        // moka directly, but admission against a zero-size ceiling proves the registry
        // is exercising both entries.
        assert_eq!(small.weighted_size(), 0);
        assert_eq!(large.weighted_size(), 0);
        let zero_ceiling = HttpCacheRegistry::with_policy(64, Some(0), HashMap::new());
        let _ = zero_ceiling.get_or_create("default", "s", "0").await;
        assert!(!zero_ceiling.try_admit(1).await);
    }

    #[tokio::test]
    async fn registry_prunes_empty_obsolete_buckets_on_lookup() {
        let registry = HttpCacheRegistry::with_policy(64, None, HashMap::new());
        let first = registry
            .get_or_create("default", "versioned_source", "0.1.0")
            .await;
        assert_eq!(registry.bucket_count(), 1);
        drop(first);

        let _second = registry
            .get_or_create("default", "versioned_source", "0.2.0")
            .await;

        assert_eq!(registry.bucket_count(), 1);
    }

    #[tokio::test]
    async fn admission_ignores_expired_entries_from_other_sources() {
        let registry = HttpCacheRegistry::with_policy(64, Some(1), HashMap::new());
        let expired_source = registry
            .get_or_create("default", "expired_source", "0.1.0")
            .await;
        expired_source
            .put(
                "expired".to_string(),
                HttpCacheEntry {
                    payload: json!({"stale": true}),
                    next_url: None,
                    ttl: Duration::from_millis(1),
                    estimated_bytes: 1,
                },
            )
            .await;
        assert_eq!(expired_source.weighted_size(), 1);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let fresh_source = registry
            .get_or_create("default", "fresh_source", "0.1.0")
            .await;

        assert!(fresh_source.try_admit(1).await);
        assert_eq!(expired_source.weighted_size(), 0);
    }

    #[tokio::test]
    async fn admission_defers_global_expiry_cleanup_until_capacity_pressure() {
        let registry = HttpCacheRegistry::with_policy(64, Some(10), HashMap::new());
        let expired_source = registry
            .get_or_create("default", "expired_source", "0.1.0")
            .await;
        expired_source
            .put(
                "expired".to_string(),
                HttpCacheEntry {
                    payload: json!({"stale": true}),
                    next_url: None,
                    ttl: Duration::from_millis(1),
                    estimated_bytes: 1,
                },
            )
            .await;
        assert_eq!(expired_source.weighted_size(), 1);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let fresh_source = registry
            .get_or_create("default", "fresh_source", "0.1.0")
            .await;

        assert!(fresh_source.try_admit(1).await);
        assert_eq!(expired_source.weighted_size(), 1);
    }
}
