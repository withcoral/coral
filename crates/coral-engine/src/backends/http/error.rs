//! Error types specific to HTTP-backed source queries.

/// Structured query-time failures for HTTP-backed tables.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ProviderQueryError {
    #[error(
        "{schema}.{table} table requires a constant equality filter: WHERE {field} = <constant>"
    )]
    MissingRequiredFilter {
        schema: String,
        table: String,
        field: String,
    },

    #[error("{source_schema}.{table} API error: {detail}")]
    ApiRequest {
        source_schema: String,
        table: String,
        status: Option<u16>,
        method: Option<String>,
        url: Option<String>,
        detail: String,
    },
}
