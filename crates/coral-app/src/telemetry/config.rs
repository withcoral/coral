//! Telemetry configuration parsing from the bootstrap snapshot.

use serde::Deserialize;

use crate::bootstrap::AppError;

pub(super) const DEFAULT_TRACE_FILTER: &str = "coral_app=trace,coral_grpc=trace,coral_mcp=trace,coral_search=trace,coral_administration=trace,coral.http.body=off,coral.mcp.body=off";
/// Targets whose spans always get trace context, whatever `trace_filter`
/// exports: the listeners' request spans and the MCP tool span. A request's
/// safe errors and records take their trace identifier from these spans.
pub(super) const REQUEST_CONTEXT_FILTER: &str = "coral_app=info,coral_grpc=info,coral_mcp=info";
pub(super) const DEFAULT_LOG_FILTER: &str =
    "coral_app=info,coral_grpc=info,coral_mcp=info,coral_search=info,coral_administration=info";
const DEFAULT_SERVICE_NAME: &str = "coral";

#[derive(Debug, Clone, Default, Deserialize)]
struct TelemetryConfigFile {
    #[serde(default)]
    otel: OtlpConfig,
}

/// External telemetry settings loaded from `config.toml`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TelemetryConfig {
    pub(crate) otel: OtlpConfig,
}

/// External OpenTelemetry export settings loaded from `[otel]`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct OtlpConfig {
    pub(crate) endpoint: Option<String>,
    pub(crate) headers: Option<String>,
    pub(crate) log_filter: Option<String>,
    pub(crate) trace_filter: String,
    pub(crate) service_name: String,
}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            endpoint: None,
            headers: None,
            log_filter: None,
            trace_filter: DEFAULT_TRACE_FILTER.to_string(),
            service_name: DEFAULT_SERVICE_NAME.to_string(),
        }
    }
}

impl TelemetryConfig {
    /// Parses the `[otel]` section from the captured `config.toml` document.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the section cannot be parsed.
    pub(crate) fn from_toml(raw: &str) -> Result<Self, AppError> {
        let file = crate::config_toml::decode::<TelemetryConfigFile>(raw).map_err(|error| {
            AppError::FailedPrecondition(format!("telemetry configuration is invalid: {error}"))
        })?;
        Ok(Self { otel: file.otel })
    }
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::filter::Targets;

    use super::{DEFAULT_LOG_FILTER, DEFAULT_TRACE_FILTER};

    #[test]
    fn default_filters_carry_universal_search_index_errors() {
        // Universal Search logs a failed index write; the Provider workflow
        // logs a publication whose search entries failed to prepare.
        for target in ["coral_search::index", "coral_administration::providers"] {
            for filter in [DEFAULT_TRACE_FILTER, DEFAULT_LOG_FILTER] {
                let targets: Targets = filter.parse().expect("valid default filter");
                assert!(
                    targets.would_enable(target, &tracing::Level::ERROR),
                    "{target} in {filter}"
                );
            }
        }
    }
}
