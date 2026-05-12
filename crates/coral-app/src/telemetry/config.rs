//! Telemetry configuration loading from app state.

use serde::Deserialize;

use crate::bootstrap::AppError;
use crate::state::AppStateLayout;

pub(super) const DEFAULT_TRACE_FILTER: &str = "coral_app=trace,coral_client=trace,coral_mcp=trace,coral_engine=trace,coral_engine::datafusion=off";
pub(super) const DEFAULT_LOG_FILTER: &str = "coral_app=info,coral_engine=info";
const DEFAULT_SERVICE_NAME: &str = "coral";

#[derive(Debug, Clone, Default, Deserialize)]
struct TelemetryConfigFile {
    #[serde(default)]
    otel: TelemetryConfig,
}

/// Telemetry settings loaded from `config.toml`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TelemetryConfig {
    pub(crate) endpoint: Option<String>,
    pub(crate) headers: Option<String>,
    pub(crate) log_filter: Option<String>,
    pub(crate) trace_filter: String,
    pub(crate) service_name: String,
}

impl Default for TelemetryConfig {
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
    /// Load the `[otel]` section from `config.toml`.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if `config.toml` exists but cannot be read or parsed.
    pub(crate) fn load(layout: &AppStateLayout) -> Result<Self, AppError> {
        let config = if layout.config_file().exists() {
            let raw = std::fs::read_to_string(layout.config_file())?;
            toml::from_str::<TelemetryConfigFile>(&raw)?.otel
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
    fn loads_otel_section_from_config_file() {
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
"#,
        )
        .expect("write config");

        let config = TelemetryConfig::load(&layout).expect("telemetry config");

        assert_eq!(config.endpoint.as_deref(), Some("http://localhost:4318"));
        assert_eq!(config.headers.as_deref(), Some("from=config"));
        assert_eq!(config.log_filter.as_deref(), Some("info"));
        assert_eq!(config.trace_filter, "coral_app=debug");
        assert_eq!(config.service_name, "from-config");
    }
}
