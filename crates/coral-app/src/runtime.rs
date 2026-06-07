//! Runtime exposure policy for app-owned local server surfaces.

use std::str::FromStr;

use coral_exports::{Binding, ExportKind};
use serde::Deserialize;

use crate::bootstrap::AppError;
use crate::state::AppStateLayout;

/// Which generated runtime bindings the local server exposes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeExposureMode {
    /// Expose both TypeScript invocation and SQL projection bindings.
    #[default]
    Both,
    /// Expose only TypeScript invocation bindings.
    #[serde(rename = "typescript")]
    TypeScript,
    /// Expose only SQL projection bindings.
    Sql,
}

impl RuntimeExposureMode {
    /// Stable config and CLI value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Both => "both",
            Self::TypeScript => "typescript",
            Self::Sql => "sql",
        }
    }

    /// Whether TypeScript invocation bindings are visible and executable.
    #[must_use]
    pub const fn exposes_typescript(self) -> bool {
        matches!(self, Self::Both | Self::TypeScript)
    }

    /// Whether SQL projection bindings are visible and executable.
    #[must_use]
    pub const fn exposes_sql(self) -> bool {
        matches!(self, Self::Both | Self::Sql)
    }

    pub(crate) const fn exposes_kind(self, kind: ExportKind) -> bool {
        match kind {
            ExportKind::Typescript => self.exposes_typescript(),
            ExportKind::SqlTable | ExportKind::SqlFunction => self.exposes_sql(),
        }
    }

    pub(crate) fn exposes_binding(self, binding: &Binding) -> bool {
        self.exposes_kind(binding.ref_().kind)
    }
}

impl FromStr for RuntimeExposureMode {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "both" => Ok(Self::Both),
            "typescript" => Ok(Self::TypeScript),
            "sql" => Ok(Self::Sql),
            other => Err(AppError::InvalidInput(format!(
                "unsupported runtime exposure '{other}'. Valid values: both, typescript, sql"
            ))),
        }
    }
}

impl std::fmt::Display for RuntimeExposureMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct RuntimeConfigFile {
    #[serde(default)]
    runtime: RuntimeConfig,
}

/// Runtime settings loaded from `config.toml`.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub(crate) struct RuntimeConfig {
    pub(crate) exposure: RuntimeExposureMode,
}

impl RuntimeConfig {
    /// Load the `[runtime]` section from `config.toml`.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if `config.toml` exists but cannot be read or parsed.
    pub(crate) fn load(layout: &AppStateLayout) -> Result<Self, AppError> {
        if !layout.config_file().exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(layout.config_file())?;
        let file = toml::from_str::<RuntimeConfigFile>(&raw)?;
        Ok(file.runtime)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use tempfile::TempDir;

    use super::{RuntimeConfig, RuntimeExposureMode};
    use crate::state::AppStateLayout;

    #[test]
    fn runtime_exposure_values_parse() {
        assert_eq!(
            RuntimeExposureMode::from_str("both").expect("both"),
            RuntimeExposureMode::Both
        );
        assert_eq!(
            RuntimeExposureMode::from_str("typescript").expect("typescript"),
            RuntimeExposureMode::TypeScript
        );
        assert_eq!(
            RuntimeExposureMode::from_str("sql").expect("sql"),
            RuntimeExposureMode::Sql
        );
        RuntimeExposureMode::from_str("neither").unwrap_err();
    }

    #[test]
    fn runtime_config_defaults_to_both_when_missing() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");

        let config = RuntimeConfig::load(&layout).expect("runtime config");

        assert_eq!(config.exposure, RuntimeExposureMode::Both);
    }

    #[test]
    fn runtime_config_loads_exposure_from_config_file() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("ensure config dir");
        std::fs::write(
            layout.config_file(),
            r#"
version = 1

[runtime]
exposure = "sql"
"#,
        )
        .expect("write config");

        let config = RuntimeConfig::load(&layout).expect("runtime config");

        assert_eq!(config.exposure, RuntimeExposureMode::Sql);
    }
}
