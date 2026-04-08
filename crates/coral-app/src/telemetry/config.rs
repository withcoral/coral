//! Telemetry configuration loading from app state.

use serde::Deserialize;

use crate::bootstrap::AppError;
use crate::state::AppStateLayout;

const DEFAULT_TRACE_FILTER: &str = "coral_app=trace,coral_engine=trace";
const DEFAULT_SERVICE_NAME: &str = "coral";

#[derive(Debug, Clone, Default, Deserialize)]
struct TelemetryConfigFile {
    #[serde(default)]
    telemetry: TelemetryConfig,
}

/// Telemetry settings loaded from `config.toml` and environment overrides.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TelemetryConfig {
    pub(crate) otel_endpoint: Option<String>,
    pub(crate) otel_headers: Option<String>,
    pub(crate) log_filter: Option<String>,
    pub(crate) trace_filter: String,
    pub(crate) otel_service_name: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            otel_endpoint: None,
            otel_headers: None,
            log_filter: None,
            trace_filter: DEFAULT_TRACE_FILTER.to_string(),
            otel_service_name: DEFAULT_SERVICE_NAME.to_string(),
        }
    }
}

impl TelemetryConfig {
    /// Load the `[telemetry]` section from `config.toml`, then apply env overrides.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if `config.toml` exists but cannot be read or parsed.
    #[allow(
        clippy::disallowed_methods,
        reason = "Telemetry config intentionally supports environment-variable overrides"
    )]
    pub(crate) fn load(layout: &AppStateLayout) -> Result<Self, AppError> {
        let mut config = if layout.config_file().exists() {
            let raw = std::fs::read_to_string(layout.config_file())?;
            toml::from_str::<TelemetryConfigFile>(&raw)?.telemetry
        } else {
            Self::default()
        };

        config.apply_env_overrides(|key| std::env::var(key).ok());

        Ok(config)
    }

    fn apply_env_overrides(&mut self, mut read_env: impl FnMut(&str) -> Option<String>) {
        if let Some(value) = read_env("CORAL_OTEL_ENDPOINT") {
            self.otel_endpoint = normalize_optional(&value);
        }
        if let Some(value) = read_env("CORAL_OTEL_HEADERS") {
            self.otel_headers = normalize_optional(&value);
        }
        if let Some(value) = read_env("CORAL_LOG_FILTER") {
            self.log_filter = normalize_optional(&value);
        }
        if let Some(value) = read_env("CORAL_TRACE_FILTER") {
            self.trace_filter = normalize_non_empty(&value, DEFAULT_TRACE_FILTER);
        }
        if let Some(value) = read_env("CORAL_OTEL_SERVICE_NAME") {
            self.otel_service_name = normalize_non_empty(&value, DEFAULT_SERVICE_NAME);
        }
    }
}

fn normalize_optional(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn normalize_non_empty(value: &str, default: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        default.to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

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
    fn env_overrides_replace_config_file_values() {
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
log_filter = "info"
trace_filter = "coral_app=debug"
otel_service_name = "from-config"
"#,
        )
        .expect("write config");

        let mut config = TelemetryConfig::load(&layout).expect("telemetry config");
        let env = HashMap::from([
            (
                "CORAL_OTEL_ENDPOINT".to_string(),
                "http://collector:4318".to_string(),
            ),
            ("CORAL_OTEL_HEADERS".to_string(), "from=env".to_string()),
            ("CORAL_LOG_FILTER".to_string(), "warn".to_string()),
            (
                "CORAL_TRACE_FILTER".to_string(),
                "coral_engine=trace".to_string(),
            ),
            (
                "CORAL_OTEL_SERVICE_NAME".to_string(),
                "from-env".to_string(),
            ),
        ]);
        config.apply_env_overrides(|key| env.get(key).cloned());

        assert_eq!(
            config.otel_endpoint.as_deref(),
            Some("http://collector:4318")
        );
        assert_eq!(config.otel_headers.as_deref(), Some("from=env"));
        assert_eq!(config.log_filter.as_deref(), Some("warn"));
        assert_eq!(config.trace_filter, "coral_engine=trace");
        assert_eq!(config.otel_service_name, "from-env");
    }
}
