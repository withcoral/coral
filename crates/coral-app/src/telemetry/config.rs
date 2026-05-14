//! Telemetry configuration loading from app state.

use std::time::Duration;

use serde::Deserialize;

use crate::bootstrap::AppError;
use crate::state::AppStateLayout;

pub(super) const DEFAULT_TRACE_FILTER: &str = "coral_app=trace,coral_client=trace,coral_mcp=trace,coral_engine=trace,coral_engine::datafusion=off";
pub(super) const DEFAULT_LOCAL_TRACE_FILTER: &str = "coral_app=trace,coral_client=trace,coral_mcp=trace,coral_engine=trace,coral_engine::datafusion=trace";
pub(super) const DEFAULT_LOG_FILTER: &str = "coral_app=info,coral_engine=info";
const DEFAULT_SERVICE_NAME: &str = "coral";
const DEFAULT_TRACE_HISTORY_RETENTION_DAYS: u64 = 7;
const HOURS_PER_DAY: u64 = 24;
const SECONDS_PER_HOUR: u64 = 60 * 60;

#[derive(Debug, Clone, Default, Deserialize)]
struct TelemetryConfigFile {
    #[serde(default)]
    otel: OtlpConfig,
    #[serde(default)]
    trace_history: TraceHistoryConfig,
}

/// Telemetry and trace history settings loaded from `config.toml`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TelemetryConfig {
    pub(crate) otel: OtlpConfig,
    pub(crate) trace_history: TraceHistoryConfig,
}

/// External OpenTelemetry export settings loaded from `[otel]`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
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

/// Product-owned trace history settings loaded from `[trace_history]`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub(crate) struct TraceHistoryConfig {
    pub(crate) enabled: bool,
    pub(crate) retention_days: u64,
}

impl Default for TraceHistoryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            retention_days: DEFAULT_TRACE_HISTORY_RETENTION_DAYS,
        }
    }
}

impl TelemetryConfig {
    /// Load telemetry and trace history sections from `config.toml`.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if `config.toml` exists but cannot be read or parsed.
    pub(crate) fn load(layout: &AppStateLayout) -> Result<Self, AppError> {
        let config = if layout.config_file().exists() {
            let raw = std::fs::read_to_string(layout.config_file())?;
            let file = toml::from_str::<TelemetryConfigFile>(&raw)?;
            Self {
                otel: file.otel,
                trace_history: file.trace_history,
            }
        } else {
            Self::default()
        };

        Ok(config)
    }
}

impl TraceHistoryConfig {
    #[must_use]
    pub(crate) fn retention(&self) -> Duration {
        Duration::from_secs(
            self.retention_days
                .max(1)
                .saturating_mul(HOURS_PER_DAY)
                .saturating_mul(SECONDS_PER_HOUR),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::TempDir;

    use super::{OtlpConfig, TelemetryConfig, TraceHistoryConfig};
    use crate::state::AppStateLayout;

    #[test]
    fn defaults_when_config_file_is_missing() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");

        let config = TelemetryConfig::load(&layout).expect("default telemetry config");

        assert_eq!(config, TelemetryConfig::default());
        assert!(config.trace_history.enabled);
    }

    #[test]
    fn loads_otel_and_trace_history_sections_from_config_file() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("ensure config dir");
        std::fs::write(
            layout.config_file(),
            r#"
version = 1

[otel]
endpoint = "http://localhost:4318"
headers = "from=config"
log_filter = "info"
trace_filter = "coral_app=debug"
service_name = "from-config"

[trace_history]
enabled = true
retention_days = 14
"#,
        )
        .expect("write config");

        let config = TelemetryConfig::load(&layout).expect("telemetry config");

        assert_eq!(
            config.otel.endpoint.as_deref(),
            Some("http://localhost:4318")
        );
        assert_eq!(config.otel.headers.as_deref(), Some("from=config"));
        assert_eq!(config.otel.log_filter.as_deref(), Some("info"));
        assert_eq!(config.otel.trace_filter, "coral_app=debug");
        assert_eq!(config.otel.service_name, "from-config");
        assert!(config.trace_history.enabled);
        assert_eq!(config.trace_history.retention_days, 14);
        assert_eq!(
            config.trace_history.retention(),
            Duration::from_hours(14 * 24)
        );
    }

    #[test]
    fn trace_history_retention_saturates_large_day_values() {
        let config = TraceHistoryConfig {
            retention_days: u64::MAX,
            ..TraceHistoryConfig::default()
        };

        assert_eq!(config.retention(), Duration::from_secs(u64::MAX));
    }

    #[test]
    fn otel_defaults_do_not_include_trace_history_settings() {
        let config = OtlpConfig::default();

        assert_eq!(config.endpoint, None);
        assert_eq!(config.trace_filter, super::DEFAULT_TRACE_FILTER);
    }
}
