//! Implementation of `grpc.health.v1.Health` for the gRPC surface.
//!
//! The service separates the two questions orchestrators ask, because they
//! demand opposite failure responses. The empty service name — the gRPC
//! convention for overall server health, and what off-the-shelf probers query by
//! default — answers process liveness from a constant: a live process must not
//! report `NotServing` just because its engine is degraded, or a liveness prober
//! restarts the container instead of removing it from rotation. Engine readiness
//! is a separate registered service, [`READINESS_SERVICE_NAME`], so a readiness
//! probe can ask for it explicitly.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Mutex as AsyncMutex;

use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_server::Health;
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

use crate::catalog::discovery::CatalogDiscovery;
use crate::query::QueryAttribution;
use crate::query::manager::QueryManager;
use crate::workspaces::WorkspaceName;

/// Health service name reporting whether the engine can answer for its catalog.
///
/// Readiness lives under its own name so the default (empty-name) check stays a
/// constant-time liveness answer. Probes that want engine health ask for this.
pub const READINESS_SERVICE_NAME: &str = "coral.readiness";

/// How long one engine readiness answer is reused.
///
/// The health service is unauthenticated by design, so without this every caller
/// that can reach the port could drive a catalog resolution per request. It also
/// keeps a normal orchestrator poll interval from doing the work every time.
const READINESS_CACHE_TTL: Duration = Duration::from_secs(2);

type ReadinessFuture = Pin<Box<dyn Future<Output = bool> + Send>>;

/// Whether the server can still answer for its catalog.
///
/// The health service is the only unauthenticated RPC on the gRPC surface, which
/// makes it the one place a readiness probe can reach without a bearer token.
/// Answering from the engine here is what keeps an authenticated `/readyz` from
/// degenerating into a port check: the alternative — probing a data-plane RPC
/// from outside — either needs a token or an authentication bypass.
#[derive(Clone)]
pub(super) struct EngineReadiness {
    probe: Arc<dyn Fn() -> ReadinessFuture + Send + Sync>,
    cached: Arc<Mutex<Option<(Instant, bool)>>>,
    /// Serializes probes so concurrent callers share one catalog resolution.
    ///
    /// The TTL alone bounds the steady-state rate but not concurrency: every
    /// caller arriving in the instant the cache expires would otherwise launch its
    /// own resolution, letting unauthenticated health traffic fan out into
    /// arbitrary engine work. Holding this across the await costs a public probe
    /// nothing it would not already wait for.
    probing: Arc<AsyncMutex<()>>,
}

impl EngineReadiness {
    fn new(probe: Arc<dyn Fn() -> ReadinessFuture + Send + Sync>) -> Self {
        Self {
            probe,
            cached: Arc::new(Mutex::new(None)),
            probing: Arc::new(AsyncMutex::new(())),
        }
    }

    /// Reports readiness by resolving the default workspace's catalog, the same
    /// work the auth-disabled readiness probe drives through `ListCatalog`.
    pub(super) fn from_query_manager(queries: QueryManager) -> Self {
        Self::new(Arc::new(move || {
            let catalog = CatalogDiscovery::new(queries.clone());
            Box::pin(async move {
                catalog
                    // Neither the catalog nor the schema is filtered: the probe
                    // asks the same unqualified question `ListCatalog` does.
                    .catalog_info(
                        &WorkspaceName::default(),
                        None,
                        None,
                        &QueryAttribution::new(None),
                    )
                    .await
                    .is_ok()
            })
        }))
    }

    #[cfg(test)]
    pub(super) fn fixed(ready: bool) -> Self {
        Self::new(Arc::new(move || Box::pin(std::future::ready(ready))))
    }

    /// Reads the cached answer while it is fresh, else probes once and caches.
    ///
    /// Exactly one probe runs at a time. Callers that queue behind it re-read the
    /// cache first, so a burst of health traffic resolves the catalog once rather
    /// than once per request.
    async fn is_ready(&self) -> bool {
        if let Some(ready) = self.fresh_answer() {
            return ready;
        }
        let _probing = self.probing.lock().await;
        // The probe that held the lock may have just answered this.
        if let Some(ready) = self.fresh_answer() {
            return ready;
        }
        let ready = (self.probe)().await;
        if let Ok(mut cached) = self.cached.lock() {
            *cached = Some((Instant::now(), ready));
        }
        ready
    }

    fn fresh_answer(&self) -> Option<bool> {
        let cached = self.cached.lock().ok()?;
        cached
            .filter(|(recorded, _)| recorded.elapsed() < READINESS_CACHE_TTL)
            .map(|(_, ready)| ready)
    }
}

pub(super) struct AggregateHealthService {
    readiness: EngineReadiness,
}

impl AggregateHealthService {
    pub(super) fn new(readiness: EngineReadiness) -> Self {
        Self { readiness }
    }
}

impl std::fmt::Debug for AggregateHealthService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AggregateHealthService")
            .finish_non_exhaustive()
    }
}

pub(super) type HealthWatchStream = Pin<
    Box<
        dyn tonic::codegen::tokio_stream::Stream<Item = Result<HealthCheckResponse, tonic::Status>>
            + Send,
    >,
>;

#[tonic::async_trait]
impl Health for AggregateHealthService {
    async fn check(
        &self,
        request: tonic::Request<HealthCheckRequest>,
    ) -> Result<tonic::Response<HealthCheckResponse>, tonic::Status> {
        let status = match request.get_ref().service.as_str() {
            // Process liveness: answered from a constant so a degraded engine
            // never tells a liveness prober to restart the process.
            "" => ServingStatus::Serving,
            READINESS_SERVICE_NAME if self.readiness.is_ready().await => ServingStatus::Serving,
            READINESS_SERVICE_NAME => ServingStatus::NotServing,
            _ => return Err(tonic::Status::not_found("service not registered")),
        };
        Ok(tonic::Response::new(HealthCheckResponse {
            status: status as i32,
        }))
    }

    type WatchStream = HealthWatchStream;

    async fn watch(
        &self,
        _request: tonic::Request<HealthCheckRequest>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "health watch is not supported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tonic_health::pb::HealthCheckRequest;
    use tonic_health::pb::health_check_response::ServingStatus;
    use tonic_health::pb::health_server::Health as _;

    use super::{AggregateHealthService, EngineReadiness, READINESS_SERVICE_NAME};

    async fn check(service: &str, ready: bool) -> Result<i32, tonic::Status> {
        AggregateHealthService::new(EngineReadiness::fixed(ready))
            .check(tonic::Request::new(HealthCheckRequest {
                service: service.to_string(),
            }))
            .await
            .map(|response| response.into_inner().status)
    }

    #[tokio::test]
    async fn liveness_stays_serving_while_the_engine_is_unready() {
        for ready in [true, false] {
            assert_eq!(
                check("", ready).await.expect("liveness check"),
                ServingStatus::Serving as i32,
                "liveness must not follow engine readiness"
            );
        }
    }

    #[tokio::test]
    async fn the_readiness_service_reports_engine_readiness() {
        assert_eq!(
            check(READINESS_SERVICE_NAME, true)
                .await
                .expect("readiness check"),
            ServingStatus::Serving as i32
        );
        assert_eq!(
            check(READINESS_SERVICE_NAME, false)
                .await
                .expect("readiness check"),
            ServingStatus::NotServing as i32
        );
    }

    #[tokio::test]
    async fn other_service_names_are_not_registered() {
        let error = check("coral.v1.QueryService", true)
            .await
            .expect_err("unknown services must not resolve");
        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn readiness_probes_the_engine_once_within_the_cache_window() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        let readiness = EngineReadiness::new(Arc::new(move || {
            counted.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(true))
        }));

        for _ in 0..5 {
            assert!(readiness.is_ready().await);
        }

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the unauthenticated readiness check must not resolve the catalog per request"
        );
    }

    #[tokio::test]
    async fn concurrent_readiness_checks_share_one_probe() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        // Yields inside the probe so every task is queued before the first
        // one finishes: without shared in-flight probing each would run its own.
        let readiness = EngineReadiness::new(Arc::new(move || {
            let counted = Arc::clone(&counted);
            Box::pin(async move {
                counted.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                true
            })
        }));

        let (first, second, third, fourth) = tokio::join!(
            readiness.is_ready(),
            readiness.is_ready(),
            readiness.is_ready(),
            readiness.is_ready(),
        );
        assert!(first && second && third && fourth);

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "concurrent public health traffic must not fan out into parallel engine work"
        );
    }
}
