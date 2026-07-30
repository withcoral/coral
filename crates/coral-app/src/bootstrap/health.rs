//! Engine-backed implementation of `grpc.health.v1.Health`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_server::Health;
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

use crate::catalog::discovery::CatalogDiscovery;
use crate::query::QueryAttribution;
use crate::query::manager::QueryManager;
use crate::workspaces::WorkspaceName;

type ReadinessFuture = Pin<Box<dyn Future<Output = bool> + Send>>;

/// Whether the server can still answer for its catalog.
///
/// The health service is the only unauthenticated RPC on the gRPC surface, which
/// makes it the one place a readiness probe can reach without a bearer token.
/// Answering from the engine here is what keeps an authenticated `/readyz` from
/// degenerating into a port check: the alternative — probing a data-plane RPC
/// from outside — either needs a token or an authentication bypass.
#[derive(Clone)]
pub(super) struct EngineReadiness(Arc<dyn Fn() -> ReadinessFuture + Send + Sync>);

impl EngineReadiness {
    /// Reports readiness by resolving the default workspace's catalog, the same
    /// work the auth-disabled readiness probe drives through `ListCatalog`.
    pub(super) fn from_query_manager(queries: QueryManager) -> Self {
        Self(Arc::new(move || {
            let catalog = CatalogDiscovery::new(queries.clone());
            Box::pin(async move {
                catalog
                    .catalog_info(
                        &WorkspaceName::default(),
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
        Self(Arc::new(move || Box::pin(std::future::ready(ready))))
    }

    async fn is_ready(&self) -> bool {
        (self.0)().await
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
        if !request.get_ref().service.is_empty() {
            return Err(tonic::Status::not_found("service not registered"));
        }

        let status = if self.readiness.is_ready().await {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
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
    use tonic_health::pb::HealthCheckRequest;
    use tonic_health::pb::health_check_response::ServingStatus;
    use tonic_health::pb::health_server::Health as _;

    use super::{AggregateHealthService, EngineReadiness};

    async fn check_status(ready: bool) -> i32 {
        AggregateHealthService::new(EngineReadiness::fixed(ready))
            .check(tonic::Request::new(HealthCheckRequest {
                service: String::new(),
            }))
            .await
            .expect("aggregate health check")
            .into_inner()
            .status
    }

    #[tokio::test]
    async fn aggregate_health_reports_engine_readiness() {
        assert_eq!(check_status(true).await, ServingStatus::Serving as i32);
        assert_eq!(check_status(false).await, ServingStatus::NotServing as i32);
    }
}
