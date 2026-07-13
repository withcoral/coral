//! Transport-free attribution for a query's originating context.

use tonic::codegen::http;

/// Request-scoped attribution threaded from the gRPC service edge into the query
/// manager, so transport concerns (gRPC metadata, OpenTelemetry baggage) stay
/// out of the manager and off the deeper query path.
#[derive(Debug, Clone, Default)]
pub(crate) struct QueryAttribution;

impl QueryAttribution {
    pub(crate) fn from_extensions(extensions: &http::Extensions) -> Self {
        let _ = extensions;
        Self
    }
}
