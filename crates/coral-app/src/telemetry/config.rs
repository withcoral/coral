//! Telemetry configuration parsing from the bootstrap snapshot.

use std::ffi::OsString;

use serde::Deserialize;

pub(super) const DEFAULT_TRACE_FILTER: &str = "coral.request=trace,coral_app=trace,coral_grpc=trace,coral_mcp=trace,coral_run=trace,coral_run_executor=info,coral_search=trace,coral_search::index::reconcile=info,coral_upstream=trace,coral_administration=trace,coral.http.body=off,coral.mcp.body=off";
/// Spans that always get trace context, whatever `trace_filter` exports:
/// every Coral span at info or above. A request's safe errors and records
/// take their trace identifier from whichever of them is current, so a span
/// left out would start a new trace.
pub(super) const TRACE_CONTEXT_FILTER: &str = "coral=info";
pub(super) const DEFAULT_LOG_FILTER: &str = "coral_app=info,coral_grpc=info,coral_mcp=info,coral_run_executor=info,coral_search=info,coral_administration=info,coral_provider::account::secret_references=info";
const DEFAULT_SERVICE_NAME: &str = "coral";

/// The standard OpenTelemetry variables that a Run Executor reads at its
/// start, and that Coral writes into a local child's environment (ADR 0027).
const OTEL_EXPORTER_OTLP_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const OTEL_EXPORTER_OTLP_HEADERS: &str = "OTEL_EXPORTER_OTLP_HEADERS";
const OTEL_SERVICE_NAME: &str = "OTEL_SERVICE_NAME";
const OTEL_TRACES_SAMPLER_ARG: &str = "OTEL_TRACES_SAMPLER_ARG";
const OTEL_RESOURCE_ATTRIBUTES: &str = "OTEL_RESOURCE_ATTRIBUTES";

/// External telemetry settings loaded from `config.toml`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TelemetryConfig {
    pub(crate) otel: OtlpConfig,
}

impl TelemetryConfig {
    /// The variables that make a local Run Executor export where Coral
    /// does, under Coral's service name: none while no endpoint is set.
    pub(crate) fn run_executor_variables(&self) -> Vec<(OsString, OsString)> {
        let Some(endpoint) = super::configured_otlp_endpoint(self) else {
            return Vec::new();
        };
        let ratio = self.otel.trace_sample_ratio.get().to_string();
        [
            (OTEL_EXPORTER_OTLP_ENDPOINT, Some(endpoint)),
            (OTEL_EXPORTER_OTLP_HEADERS, self.otel.headers.as_deref()),
            (OTEL_SERVICE_NAME, Some(self.otel.service_name.as_str())),
            (OTEL_TRACES_SAMPLER_ARG, Some(ratio.as_str())),
        ]
        .into_iter()
        .filter_map(|(name, value)| Some((name.into(), value?.into())))
        .collect()
    }
}

/// A Run Executor's telemetry settings, from the standard OpenTelemetry
/// variables. The rest is Coral's default, its filters included.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunExecutorTelemetry {
    pub(crate) config: TelemetryConfig,
    /// `key=value` pairs, below the service name, version and instance id.
    pub(crate) resource_attributes: String,
}

impl RunExecutorTelemetry {
    /// The settings that `variable` gives, which answers each variable's
    /// value, or why it cannot.
    pub(crate) fn from_variables(
        variable: impl Fn(&str) -> Result<Option<String>, String>,
    ) -> Result<Self, String> {
        let mut otel = OtlpConfig {
            endpoint: variable(OTEL_EXPORTER_OTLP_ENDPOINT)?,
            headers: variable(OTEL_EXPORTER_OTLP_HEADERS)?,
            ..OtlpConfig::default()
        };
        // As set, empty included: a local child carries its parent's name.
        if let Some(name) = variable(OTEL_SERVICE_NAME)? {
            otel.service_name = name;
        }
        if let Some(ratio) = variable(OTEL_TRACES_SAMPLER_ARG)? {
            otel.trace_sample_ratio = ratio
                .trim()
                .parse::<f64>()
                .map_err(|_not_a_number| "not a number")
                .and_then(TraceSampleRatio::try_from)
                .map_err(|error| format!("{OTEL_TRACES_SAMPLER_ARG}: {error}"))?;
        }
        Ok(Self {
            config: TelemetryConfig { otel },
            resource_attributes: variable(OTEL_RESOURCE_ATTRIBUTES)?.unwrap_or_default(),
        })
    }
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tracing_subscriber::filter::Targets;

    use super::{DEFAULT_LOG_FILTER, DEFAULT_TRACE_FILTER, OtlpConfig, RunExecutorTelemetry};

    /// A local child reads what Coral writes into its environment: Coral's
    /// endpoint, headers, service name and sample ratio, never its filters,
    /// and nothing while no endpoint is set.
    #[test]
    fn a_local_run_executor_reads_the_settings_that_coral_writes() {
        let endpoint = "[otel]\nendpoint = 'http://collector:4318'\n";
        for (raw, written) in [
            (String::new(), 0),
            ("[otel]\nendpoint = ''\nheaders = 'x-key=1'\n".to_owned(), 0),
            (endpoint.to_owned(), 3),
            (format!("{endpoint}service_name = ''\n"), 3),
            (
                format!(
                    "{endpoint}headers = 'x-key=1'\nservice_name = 'coral-eu'\n\
                     trace_sample_ratio = 0.25\nlog_filter = 'coral=debug'\n\
                     trace_filter = 'coral=debug'\n"
                ),
                4,
            ),
        ] {
            let parent = crate::bootstrap::telemetry(&raw).expect("telemetry config");
            let written_variables: HashMap<String, String> = parent
                .run_executor_variables()
                .into_iter()
                .map(|(name, value)| (name.into_string().unwrap(), value.into_string().unwrap()))
                .collect();
            assert_eq!(written_variables.len(), written, "{raw}");
            let child = RunExecutorTelemetry::from_variables(|name| {
                Ok(written_variables.get(name).cloned())
            })
            .expect("the child reads its variables");
            let expected = if written == 0 {
                OtlpConfig::default()
            } else {
                OtlpConfig {
                    log_filter: None,
                    trace_filter: DEFAULT_TRACE_FILTER.to_owned(),
                    ..parent.otel
                }
            };
            assert_eq!(child.config.otel, expected, "{raw}");
        }
        for ratio in ["1.5", "half"] {
            let refused = RunExecutorTelemetry::from_variables(|name| {
                Ok((name == "OTEL_TRACES_SAMPLER_ARG").then(|| ratio.to_owned()))
            });
            assert!(
                refused.is_err_and(|error| error.starts_with("OTEL_TRACES_SAMPLER_ARG: ")),
                "{ratio}"
            );
        }
    }

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
            let parsed = crate::bootstrap::telemetry(&toml)
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
