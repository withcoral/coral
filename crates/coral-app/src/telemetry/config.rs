//! Telemetry configuration parsing from the bootstrap snapshot.

use serde::Deserialize;

use crate::bootstrap::AppError;

pub(super) const DEFAULT_TRACE_FILTER: &str = "coral.request=trace,coral_app=trace,coral_grpc=trace,coral_mcp=trace,coral_run=trace,coral_search=trace,coral_search::index::reconcile=info,coral_upstream=trace,coral_administration=trace,coral.http.body=off,coral.mcp.body=off";
/// Spans that always get trace context, whatever `trace_filter` exports:
/// every Coral span at info or above. A request's safe errors and records
/// take their trace identifier from whichever of them is current, so a span
/// left out would start a new trace.
pub(super) const TRACE_CONTEXT_FILTER: &str = "coral=info";
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
    pub(crate) trace_sample_ratio: TraceSampleRatio,
}

/// The share of the roots Coral creates that it samples, from `0.0` to
/// `1.0`: a request that brings no valid caller context, every Run root,
/// and each search-index write and reconcile. Metrics are never sampled.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(try_from = "f64")]
pub(crate) struct TraceSampleRatio(f64);

impl TraceSampleRatio {
    pub(crate) const fn get(self) -> f64 {
        self.0
    }
}

impl Default for TraceSampleRatio {
    fn default() -> Self {
        Self(1.0)
    }
}

impl TryFrom<f64> for TraceSampleRatio {
    type Error = &'static str;

    fn try_from(ratio: f64) -> Result<Self, Self::Error> {
        if (0.0..=1.0).contains(&ratio) {
            Ok(Self(ratio))
        } else {
            Err("trace_sample_ratio must be a number from 0.0 to 1.0")
        }
    }
}

/// A ratio is never NaN, so equality is total.
impl Eq for TraceSampleRatio {}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            endpoint: None,
            headers: None,
            log_filter: None,
            trace_filter: DEFAULT_TRACE_FILTER.to_string(),
            service_name: DEFAULT_SERVICE_NAME.to_string(),
            trace_sample_ratio: TraceSampleRatio::default(),
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

    use super::{DEFAULT_LOG_FILTER, DEFAULT_TRACE_FILTER, TelemetryConfig};

    #[test]
    fn trace_sample_ratio_admits_only_zero_to_one() {
        for (raw, ratio) in [
            ("", Some(1.0)),
            ("0", Some(0.0)),
            ("0.0", Some(0.0)),
            ("0.25", Some(0.25)),
            ("1.0", Some(1.0)),
            ("1.5", None),
            ("-0.1", None),
            ("nan", None),
        ] {
            let toml = if raw.is_empty() {
                String::new()
            } else {
                format!("[otel]\ntrace_sample_ratio = {raw}\n")
            };
            let parsed = TelemetryConfig::from_toml(&toml)
                .map(|config| config.otel.trace_sample_ratio.get());
            assert_eq!(parsed.ok(), ratio, "{raw}");
        }
    }

    /// Request spans are the root of every request's trace.
    #[test]
    fn the_default_trace_filter_exports_request_spans() {
        let targets: Targets = DEFAULT_TRACE_FILTER.parse().expect("valid default filter");
        assert!(targets.would_enable(coral_telemetry::REQUEST_TARGET, &tracing::Level::INFO));
    }

    /// Search index reconcile passes run every interval and almost never
    /// find work; their debug spans stay out of the default export.
    #[test]
    fn the_default_trace_filter_leaves_out_reconcile_passes() {
        let targets: Targets = DEFAULT_TRACE_FILTER.parse().expect("valid default filter");
        let reconcile = "coral_search::index::reconcile";
        assert!(!targets.would_enable(reconcile, &tracing::Level::DEBUG));
        assert!(targets.would_enable(reconcile, &tracing::Level::INFO));
        assert!(targets.would_enable("coral_search::index", &tracing::Level::DEBUG));
    }

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
