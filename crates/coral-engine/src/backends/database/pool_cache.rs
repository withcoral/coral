//! Cross-query connection pool caching for remote database sources.
//!
//! Query runtimes are rebuilt per query, so without caching every query pays
//! a fresh connection handshake per database source. This module keeps pools
//! alive across runtime builds, keyed by the resolved connection parameters.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use datafusion::error::{DataFusionError, Result as DataFusionResult};
use moka::future::Cache;
use sha2::{Digest as _, Sha256};

/// How long an unused pool stays cached before its connections are released.
const POOL_TIME_TO_IDLE: Duration = Duration::from_mins(10);

/// Upper bound on concurrently cached pools per provider.
const POOL_CAPACITY: u64 = 32;

/// Opaque cache key derived from resolved connection parameters.
///
/// Keys hash the parameters instead of storing them so plaintext credentials
/// never sit in cache state. Changing any parameter (including credentials)
/// produces a new key, so superseded pools idle out without an explicit
/// invalidation API.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct PoolKey([u8; 32]);

impl PoolKey {
    pub(super) fn new(provider: &str, params: &HashMap<String, String>) -> Self {
        let mut sorted = params.iter().collect::<Vec<_>>();
        sorted.sort();
        let mut hasher = Sha256::new();
        hash_component(&mut hasher, provider);
        for (key, value) in sorted {
            hash_component(&mut hasher, key);
            hash_component(&mut hasher, value);
        }
        Self(hasher.finalize().into())
    }
}

/// Length-prefix each component so adjacent values cannot collide
/// (e.g. `{"ab": "c"}` vs `{"a": "bc"}`).
fn hash_component(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value.as_bytes());
}

/// Process-wide cache of connection pools for one database provider.
pub(super) struct PoolCache<P> {
    pools: Cache<PoolKey, Arc<P>>,
}

impl<P: Send + Sync + 'static> PoolCache<P> {
    pub(super) fn new() -> Self {
        Self {
            pools: Cache::builder()
                .max_capacity(POOL_CAPACITY)
                .time_to_idle(POOL_TIME_TO_IDLE)
                .build(),
        }
    }

    /// Runs `operation` against the pool cached under `key`, building the
    /// pool on a miss. Concurrent callers for the same key coalesce into one
    /// build. If `operation` fails against a pool that predates this call
    /// (e.g. the database restarted and its connections are dead), the stale
    /// entry is dropped and the operation retries once against a fresh pool.
    pub(super) async fn run<F, FutP, Op, FutR, R>(
        &self,
        key: PoolKey,
        build: F,
        operation: Op,
    ) -> DataFusionResult<R>
    where
        F: Fn() -> FutP,
        FutP: Future<Output = DataFusionResult<P>>,
        Op: Fn(Arc<P>) -> FutR,
        FutR: Future<Output = DataFusionResult<R>>,
    {
        let (pool, built_here) = self.get_or_build(&key, &build).await?;
        match operation(pool).await {
            Ok(value) => Ok(value),
            Err(error) if !built_here => {
                tracing::debug!(
                    detail = %error,
                    "cached database pool failed; rebuilding connection pool"
                );
                self.pools.invalidate(&key).await;
                let (pool, _) = self.get_or_build(&key, &build).await?;
                operation(pool).await
            }
            Err(error) => Err(error),
        }
    }

    async fn get_or_build<F, FutP>(
        &self,
        key: &PoolKey,
        build: &F,
    ) -> DataFusionResult<(Arc<P>, bool)>
    where
        F: Fn() -> FutP,
        FutP: Future<Output = DataFusionResult<P>>,
    {
        let built_here = AtomicBool::new(false);
        let pool = self
            .pools
            .try_get_with(key.clone(), async {
                built_here.store(true, Ordering::Relaxed);
                build().await.map(Arc::new)
            })
            .await
            .map_err(unwrap_shared_error)?;
        Ok((pool, built_here.load(Ordering::Relaxed)))
    }
}

/// Moka shares one init error across coalesced waiters; unwrap it when this
/// caller is the only holder.
fn unwrap_shared_error(error: Arc<DataFusionError>) -> DataFusionError {
    Arc::try_unwrap(error).unwrap_or_else(DataFusionError::Shared)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    fn params(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn pool_key_ignores_parameter_order() {
        let left = PoolKey::new("postgres", &params(&[("host", "db"), ("user", "coral")]));
        let right = PoolKey::new("postgres", &params(&[("user", "coral"), ("host", "db")]));
        assert_eq!(left, right);
    }

    #[test]
    fn pool_key_distinguishes_providers_and_parameters() {
        let base = PoolKey::new("postgres", &params(&[("host", "db")]));
        assert_ne!(base, PoolKey::new("mysql", &params(&[("host", "db")])));
        assert_ne!(
            base,
            PoolKey::new("postgres", &params(&[("host", "other")]))
        );
        assert_ne!(base, PoolKey::new("postgres", &params(&[("pass", "db")])));
    }

    #[test]
    fn pool_key_distinguishes_component_boundaries() {
        let left = PoolKey::new("postgres", &params(&[("ab", "c")]));
        let right = PoolKey::new("postgres", &params(&[("a", "bc")]));
        assert_ne!(left, right);
    }

    struct FakePool {
        generation: usize,
    }

    fn counting_build(
        builds: &AtomicUsize,
    ) -> impl Fn() -> std::future::Ready<DataFusionResult<FakePool>> + '_ {
        move || {
            std::future::ready(Ok(FakePool {
                generation: builds.fetch_add(1, Ordering::SeqCst) + 1,
            }))
        }
    }

    fn test_key(name: &str) -> PoolKey {
        PoolKey::new("test", &params(&[("name", name)]))
    }

    #[tokio::test]
    async fn run_reuses_cached_pool_across_calls() {
        let cache = PoolCache::<FakePool>::new();
        let builds = AtomicUsize::new(0);
        for _ in 0..3 {
            let generation = cache
                .run(test_key("reuse"), counting_build(&builds), |pool| {
                    std::future::ready(Ok(pool.generation))
                })
                .await
                .expect("cached run succeeds");
            assert_eq!(generation, 1);
        }
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn run_coalesces_concurrent_builds() {
        let cache = Arc::new(PoolCache::<FakePool>::new());
        let builds = Arc::new(AtomicUsize::new(0));
        let tasks = (0..8)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let builds = Arc::clone(&builds);
                tokio::spawn(async move {
                    cache
                        .run(
                            test_key("coalesce"),
                            || {
                                let builds = Arc::clone(&builds);
                                async move {
                                    tokio::time::sleep(Duration::from_millis(10)).await;
                                    Ok(FakePool {
                                        generation: builds.fetch_add(1, Ordering::SeqCst) + 1,
                                    })
                                }
                            },
                            |pool| std::future::ready(Ok(pool.generation)),
                        )
                        .await
                })
            })
            .collect::<Vec<_>>();
        for task in tasks {
            let generation = task.await.expect("join").expect("run succeeds");
            assert_eq!(generation, 1);
        }
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn run_retries_once_against_fresh_pool_when_cached_pool_fails() {
        let cache = PoolCache::<FakePool>::new();
        let builds = AtomicUsize::new(0);

        cache
            .run(test_key("stale"), counting_build(&builds), |_pool| {
                std::future::ready(Ok(()))
            })
            .await
            .expect("first run caches the pool");

        // The cached generation-1 pool now fails; the retry must observe a
        // freshly built generation-2 pool.
        let generation = cache
            .run(test_key("stale"), counting_build(&builds), |pool| {
                std::future::ready(if pool.generation == 1 {
                    Err(DataFusionError::Execution("connection reset".to_string()))
                } else {
                    Ok(pool.generation)
                })
            })
            .await
            .expect("retry succeeds against a fresh pool");
        assert_eq!(generation, 2);
        assert_eq!(builds.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn run_does_not_retry_when_the_pool_was_built_by_this_call() {
        let cache = PoolCache::<FakePool>::new();
        let builds = AtomicUsize::new(0);
        let operations = AtomicUsize::new(0);

        let error = cache
            .run(
                test_key("fresh-failure"),
                counting_build(&builds),
                |_pool| {
                    operations.fetch_add(1, Ordering::SeqCst);
                    std::future::ready(Err::<(), _>(DataFusionError::Execution(
                        "authentication failed".to_string(),
                    )))
                },
            )
            .await
            .expect_err("fresh pool failure propagates");
        assert!(error.to_string().contains("authentication failed"));
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert_eq!(operations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn build_errors_are_not_cached() {
        let cache = PoolCache::<FakePool>::new();
        let attempts = AtomicUsize::new(0);
        let build = || {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            std::future::ready(if attempt == 0 {
                Err(DataFusionError::Execution("dns failure".to_string()))
            } else {
                Ok(FakePool {
                    generation: attempt,
                })
            })
        };

        let error = cache
            .run(test_key("build-error"), build, |pool| {
                std::future::ready(Ok(pool.generation))
            })
            .await
            .expect_err("first build fails");
        assert!(error.to_string().contains("dns failure"));

        let generation = cache
            .run(test_key("build-error"), build, |pool| {
                std::future::ready(Ok(pool.generation))
            })
            .await
            .expect("second build succeeds");
        assert_eq!(generation, 1);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
