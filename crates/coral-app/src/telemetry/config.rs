//! Telemetry configuration loading from app state.

use serde::Deserialize;

use crate::bootstrap::AppError;
use crate::state::AppStateLayout;

pub(super) const DEFAULT_TRACE_FILTER: &str = "coral_app=trace,coral_engine=trace";
const DEFAULT_SERVICE_NAME: &str = "coral";

#[derive(Debug, Clone, Default, Deserialize)]
struct TelemetryConfigFile {
    #[serde(default)]
    telemetry: TelemetryConfig,
}

/// Telemetry settings loaded from `config.toml`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[allow(
    clippy::struct_field_names,
    reason = "field names mirror the TOML config keys for direct serde deserialization"
)]
pub struct TelemetryConfig {
    pub(crate) otel_endpoint: Option<String>,
    pub(crate) otel_headers: Option<String>,
    pub(crate) otel_log_filter: Option<String>,
    pub(crate) otel_trace_filter: String,
    pub(crate) otel_service_name: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            otel_endpoint: None,
            otel_headers: None,
            otel_log_filter: None,
            otel_trace_filter: DEFAULT_TRACE_FILTER.to_string(),
            otel_service_name: DEFAULT_SERVICE_NAME.to_string(),
        }
    }
}

impl TelemetryConfig {
    /// Load the `[telemetry]` section from `config.toml`.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if `config.toml` exists but cannot be read or parsed.
    pub(crate) fn load(layout: &AppStateLayout) -> Result<Self, AppError> {
        let config = if layout.config_file().exists() {
            let raw = std::fs::read_to_string(layout.config_file())?;
            toml::from_str::<TelemetryConfigFile>(&raw)?.telemetry
        } else {
            Self::default()
        };

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::TelemetryConfig;
    use crate::state::AppStateLayout;

    #[test]
    fn defaults_when_config_file_is_missing() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");

        let config = TelemetryConfig::load(&layout).expect("default telemetry config");

        assert_eq!(config, TelemetryConfig::default());
    }

    #[test]
    fn loads_telemetry_section_from_config_file() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("ensure config dir");
        std::fs::write(
            layout.config_file(),
            r#"
version = 1

[telemetry]
otel_endpoint = "http://localhost:4318"
otel_headers = "from=config"
otel_log_filter = "info"
otel_trace_filter = "coral_app=debug"
otel_service_name = "from-config"
"#,
        )
        .expect("write config");

        let config = TelemetryConfig::load(&layout).expect("telemetry config");

        assert_eq!(
            config.otel_endpoint.as_deref(),
            Some("http://localhost:4318")
        );
        assert_eq!(config.otel_headers.as_deref(), Some("from=config"));
        assert_eq!(config.otel_log_filter.as_deref(), Some("info"));
        assert_eq!(config.otel_trace_filter, "coral_app=debug");
        assert_eq!(config.otel_service_name, "from-config");
    }
}
