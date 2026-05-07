//! Telemetry configuration loading from app state.

use serde::Deserialize;

use crate::bootstrap::AppError;
use crate::state::AppStateLayout;

pub(super) const DEFAULT_TRACE_FILTER: &str = "coral_app=trace,coral_client=trace,coral_mcp=trace,coral_engine=trace,coral_engine::datafusion=off";
pub(super) const DEFAULT_INTERNAL_TRACE_FILTER: &str = "coral_app=trace,coral_client=trace,coral_mcp=trace,coral_engine=trace,coral_engine::datafusion=trace";
pub(super) const DEFAULT_LOG_FILTER: &str = "coral_app=info,coral_engine=info";
const DEFAULT_SERVICE_NAME: &str = "coral";
const DEFAULT_INTERNAL_HTTP_BODY_MAX_BYTES: usize = 64 * 1024;

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
    pub(crate) enable_internal_tracing: bool,
    pub(crate) record_internal_http_bodies: bool,
    pub(crate) internal_http_body_max_bytes: usize,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            endpoint: None,
            headers: None,
            log_filter: None,
            trace_filter: DEFAULT_TRACE_FILTER.to_string(),
            service_name: DEFAULT_SERVICE_NAME.to_string(),
            enable_internal_tracing: false,
            record_internal_http_bodies: false,
            internal_http_body_max_bytes: DEFAULT_INTERNAL_HTTP_BODY_MAX_BYTES,
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

    #[must_use]
    pub(crate) fn internal_http_body_recording_max_bytes(&self) -> Option<usize> {
        (self.enable_internal_tracing && self.record_internal_http_bodies)
            .then_some(self.internal_http_body_max_bytes)
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
enable_internal_tracing = true
record_internal_http_bodies = true
internal_http_body_max_bytes = 42
"#,
        )
        .expect("write config");

        let config = TelemetryConfig::load(&layout).expect("telemetry config");

        assert_eq!(config.endpoint.as_deref(), Some("http://localhost:4318"));
        assert_eq!(config.headers.as_deref(), Some("from=config"));
        assert_eq!(config.log_filter.as_deref(), Some("info"));
        assert_eq!(config.trace_filter, "coral_app=debug");
        assert_eq!(config.service_name, "from-config");
        assert!(config.enable_internal_tracing);
        assert!(config.record_internal_http_bodies);
        assert_eq!(config.internal_http_body_max_bytes, 42);
        assert_eq!(config.internal_http_body_recording_max_bytes(), Some(42));
    }

    #[test]
    fn http_body_recording_requires_internal_tracing() {
        let config = TelemetryConfig {
            record_internal_http_bodies: true,
            internal_http_body_max_bytes: 8,
            ..TelemetryConfig::default()
        };

        assert_eq!(config.internal_http_body_recording_max_bytes(), None);
    }
}
