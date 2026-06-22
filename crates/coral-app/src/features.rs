//! Runtime feature registry and effective feature resolution.

use std::collections::BTreeMap;
use std::path::PathBuf;

use tracing::warn;

use crate::bootstrap::{AppError, discover_app_state_layout};
use crate::state::{
    AppStateLayout, RawFeatureContainerState, RawFeatureOverrides, RawFeatureValue,
};

/// Runtime feature keys recognized by Coral.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Feature {
    /// Enables preview DSL v4 source manifests.
    DslV4,
    /// Expose the optional MCP `feedback` tool.
    Feedback,
    /// Experimental trajectory-memory episodes (in progress): exposes the MCP
    /// `open_episode` tool and associates follow-up Coral MCP tool calls with
    /// the intent they served via the `coral-episode-id` metadata key.
    Episodes,
}

impl Feature {
    /// Returns all runtime features recognized by this Coral binary.
    #[must_use]
    pub fn all() -> impl ExactSizeIterator<Item = Self> {
        FEATURE_SPECS.iter().map(|spec| spec.feature)
    }

    /// Returns the stable key used under `[features]` in `config.toml`.
    #[must_use]
    pub fn key(self) -> &'static str {
        spec_for_feature(self).key
    }

    /// Returns whether the feature is enabled when no config override exists.
    #[must_use]
    pub fn default_enabled(self) -> bool {
        spec_for_feature(self).default_enabled
    }

    /// Returns a short human-readable description of the feature.
    #[must_use]
    pub fn description(self) -> &'static str {
        spec_for_feature(self).description
    }

    /// Returns the long flag name for process-local enable overrides.
    #[must_use]
    pub fn enable_flag(self) -> &'static str {
        spec_for_feature(self).enable_flag
    }

    /// Returns the long flag name for process-local disable overrides.
    #[must_use]
    pub fn disable_flag(self) -> &'static str {
        spec_for_feature(self).disable_flag
    }
}

#[derive(Debug, Clone, Copy)]
struct FeatureSpec {
    feature: Feature,
    key: &'static str,
    default_enabled: bool,
    description: &'static str,
    enable_flag: &'static str,
    disable_flag: &'static str,
}

const FEATURE_SPECS: &[FeatureSpec] = &[
    FeatureSpec {
        feature: Feature::DslV4,
        key: "dsl_v4",
        default_enabled: false,
        description: "Enables preview DSL v4 source manifests.",
        enable_flag: "enable-dsl-v4",
        disable_flag: "disable-dsl-v4",
    },
    FeatureSpec {
        feature: Feature::Feedback,
        key: "feedback",
        default_enabled: false,
        description: "Exposes the MCP feedback tool when enabled. Feedback reports are stored locally and anonymous copies may be uploaded to Coral.",
        enable_flag: "enable-feedback",
        disable_flag: "disable-feedback",
    },
    FeatureSpec {
        feature: Feature::Episodes,
        key: "episodes",
        default_enabled: false,
        description: "Experimental trajectory memory (in progress): exposes MCP open_episode and tags follow-up Coral tool calls with episode ids. Off by default.",
        enable_flag: "enable-episodes",
        disable_flag: "disable-episodes",
    },
];

/// How a feature's value is configured in Coral's local config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureConfiguredState {
    /// No override exists, so the feature uses its built-in default.
    Default,
    /// The local config explicitly enables the feature.
    Enabled,
    /// The local config explicitly disables the feature.
    Disabled,
    /// The local config contains a known feature key with a non-boolean value.
    InvalidValue,
    /// The local config uses an unsupported `[features]` container shape.
    InvalidContainer,
}

impl FeatureConfiguredState {
    /// Returns the stable user-facing label for this configured state.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
            Self::InvalidValue => "invalid-value",
            Self::InvalidContainer => "invalid-container",
        }
    }
}

/// A feature's configured and effective runtime state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureStatus {
    /// Feature enum variant.
    pub feature: Feature,
    /// Stable config key.
    pub key: &'static str,
    /// Built-in enabled state when no override exists.
    pub default_enabled: bool,
    /// Current config state.
    pub configured: FeatureConfiguredState,
    /// Effective runtime state after applying config to the default.
    pub enabled: bool,
    /// Short human-readable description.
    pub description: &'static str,
}

/// Effective runtime feature state after applying config overrides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Features {
    enabled: BTreeMap<Feature, bool>,
}

impl Default for Features {
    fn default() -> Self {
        let enabled = FEATURE_SPECS
            .iter()
            .map(|spec| (spec.feature, spec.default_enabled))
            .collect();
        Self { enabled }
    }
}

impl Features {
    /// Returns whether a runtime feature is enabled.
    #[must_use]
    pub fn enabled(&self, feature: Feature) -> bool {
        self.enabled.get(&feature).copied().unwrap_or(false)
    }

    /// Errors unless the preview DSL v4 runtime feature is enabled.
    ///
    /// DSL v4 sources and their identity specs cannot be installed or managed
    /// while `dsl_v4` is off, so source-install and identity-spec management paths
    /// share this single gate and its canonical remediation message.
    pub(crate) fn ensure_dsl_v4_enabled(&self) -> Result<(), AppError> {
        if self.enabled(Feature::DslV4) {
            return Ok(());
        }
        Err(AppError::SourceUnservable(
            "DSL v4 source and identity-spec management requires the 'dsl_v4' runtime feature."
                .to_string(),
        ))
    }

    fn from_raw_overrides(raw: &RawFeatureOverrides) -> Self {
        let mut features = Self::default();
        for (key, value) in raw.iter() {
            let Some(spec) = spec_for_key(key) else {
                warn!(feature = key, "ignoring unknown Coral runtime feature");
                continue;
            };

            match value {
                RawFeatureValue::Bool(enabled) => {
                    features.enabled.insert(spec.feature, enabled);
                }
                RawFeatureValue::UnsupportedType => {
                    warn!(
                        feature = key,
                        "ignoring unsupported Coral runtime feature value; expected boolean"
                    );
                }
            }
        }
        features
    }

    pub(crate) fn apply_overrides(&mut self, overrides: &FeatureOverrides) {
        for (feature, enabled) in overrides.iter() {
            self.enabled.insert(feature, enabled);
        }
    }
}

/// Process-local runtime feature overrides.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FeatureOverrides {
    enabled: BTreeMap<Feature, bool>,
}

impl FeatureOverrides {
    /// Sets a process-local override for a known feature.
    pub fn set(&mut self, feature: Feature, enabled: bool) {
        self.enabled.insert(feature, enabled);
    }

    /// Returns the process-local override for a known feature, when present.
    #[must_use]
    pub fn get(&self, feature: Feature) -> Option<bool> {
        self.enabled.get(&feature).copied()
    }

    /// Adds every override from `defaults` that is not already explicitly set.
    pub fn apply_defaults_from(&mut self, defaults: &Self) {
        for (feature, enabled) in defaults.iter() {
            if self.get(feature).is_none() {
                self.set(feature, enabled);
            }
        }
    }

    fn iter(&self) -> impl Iterator<Item = (Feature, bool)> + '_ {
        self.enabled
            .iter()
            .map(|(feature, enabled)| (*feature, *enabled))
    }
}

/// Loader for runtime features from Coral's local config.
#[derive(Debug, Clone)]
pub struct FeatureStore {
    layout: AppStateLayout,
}

impl FeatureStore {
    /// Builds a feature store for an already-discovered app state layout.
    #[must_use]
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    /// Discovers the Coral app state layout used for runtime feature config.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the platform config directory cannot be discovered.
    pub fn discover(config_dir_override: Option<PathBuf>) -> Result<Self, AppError> {
        Ok(Self {
            layout: discover_app_state_layout(config_dir_override)?,
        })
    }

    /// Loads effective runtime feature state, applying process-local overrides.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if `config.toml` exists but cannot be read or parsed.
    pub fn load_with_overrides(&self, overrides: &FeatureOverrides) -> Result<Features, AppError> {
        let raw = crate::state::load_raw_feature_overrides(&self.layout)?;
        let mut features = Features::from_raw_overrides(&raw);
        features.apply_overrides(overrides);
        Ok(features)
    }

    /// Lists every known feature, applying process-local overrides to effective state.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if `config.toml` exists but cannot be read or parsed.
    pub fn statuses_with_overrides(
        &self,
        overrides: &FeatureOverrides,
    ) -> Result<Vec<FeatureStatus>, AppError> {
        let raw = crate::state::load_raw_feature_overrides(&self.layout)?;
        let mut features = Features::from_raw_overrides(&raw);
        features.apply_overrides(overrides);
        Ok(statuses_from_raw(&raw, &features))
    }

    /// Persists a local opt-in override for a known feature key.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the key is unknown or the local config cannot be updated.
    pub fn enable(&self, key: &str) -> Result<(), AppError> {
        let spec = spec_for_key(key).ok_or_else(|| unknown_feature_error(key))?;
        crate::state::set_raw_feature_override(&self.layout, spec.key, true)
    }

    /// Persists a local opt-out for a known feature key.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the key is unknown or the local config cannot be updated.
    pub fn disable(&self, key: &str) -> Result<(), AppError> {
        let spec = spec_for_key(key).ok_or_else(|| unknown_feature_error(key))?;
        crate::state::set_raw_feature_override(&self.layout, spec.key, false)
    }
}

fn status_from_raw(
    spec: &'static FeatureSpec,
    raw: &RawFeatureOverrides,
    features: &Features,
) -> FeatureStatus {
    let configured = match raw.container() {
        RawFeatureContainerState::Unsupported => FeatureConfiguredState::InvalidContainer,
        RawFeatureContainerState::Missing | RawFeatureContainerState::Table => {
            match raw.get(spec.key) {
                Some(RawFeatureValue::Bool(true)) => FeatureConfiguredState::Enabled,
                Some(RawFeatureValue::Bool(false)) => FeatureConfiguredState::Disabled,
                Some(RawFeatureValue::UnsupportedType) => FeatureConfiguredState::InvalidValue,
                None => FeatureConfiguredState::Default,
            }
        }
    };

    FeatureStatus {
        feature: spec.feature,
        key: spec.key,
        default_enabled: spec.default_enabled,
        configured,
        enabled: features.enabled(spec.feature),
        description: spec.description,
    }
}

fn statuses_from_raw(raw: &RawFeatureOverrides, features: &Features) -> Vec<FeatureStatus> {
    let mut statuses = FEATURE_SPECS
        .iter()
        .map(|spec| status_from_raw(spec, raw, features))
        .collect::<Vec<_>>();
    statuses.sort_by_key(|status| status.key);
    statuses
}

fn unknown_feature_error(key: &str) -> AppError {
    let mut valid = FEATURE_SPECS
        .iter()
        .map(|spec| spec.key)
        .collect::<Vec<_>>();
    valid.sort_unstable();
    let valid = valid.join(", ");
    AppError::InvalidInput(format!("unknown feature '{key}'. Valid features: {valid}"))
}

fn spec_for_feature(feature: Feature) -> &'static FeatureSpec {
    FEATURE_SPECS
        .iter()
        .find(|spec| spec.feature == feature)
        .expect("every Feature variant must have a FeatureSpec")
}

fn spec_for_key(key: &str) -> Option<&'static FeatureSpec> {
    FEATURE_SPECS.iter().find(|spec| spec.key == key)
}

/// Builds a [`Features`] with the preview DSL v4 feature enabled, shared by the
/// crate's source-install and query tests that exercise v4 sources.
#[cfg(test)]
pub(crate) fn dsl_v4_features() -> Features {
    let mut overrides = FeatureOverrides::default();
    overrides.set(Feature::DslV4, true);
    let mut features = Features::default();
    features.apply_overrides(&overrides);
    features
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(
        entries: impl IntoIterator<Item = (&'static str, RawFeatureValue)>,
    ) -> RawFeatureOverrides {
        RawFeatureOverrides::from_entries_for_tests(entries)
    }

    #[test]
    fn defaults_disable_features() {
        let features = Features::default();

        for (label, feature) in [("feedback", Feature::Feedback), ("dsl_v4", Feature::DslV4)] {
            assert!(!features.enabled(feature), "{label} should default off");
        }
    }

    #[test]
    fn process_overrides_enable_default_disabled_feature() {
        let mut overrides = FeatureOverrides::default();
        overrides.set(Feature::Feedback, true);
        let mut features = Features::default();

        features.apply_overrides(&overrides);

        assert!(features.enabled(Feature::Feedback));
    }

    #[test]
    fn process_override_get_reports_only_explicit_overrides() {
        let mut overrides = FeatureOverrides::default();

        assert_eq!(overrides.get(Feature::DslV4), None);

        overrides.set(Feature::DslV4, false);

        assert_eq!(overrides.get(Feature::DslV4), Some(false));
    }

    #[test]
    fn process_override_defaults_fill_missing_features_only() {
        let mut overrides = FeatureOverrides::default();
        overrides.set(Feature::DslV4, false);
        let mut defaults = FeatureOverrides::default();
        defaults.set(Feature::DslV4, true);
        defaults.set(Feature::Feedback, true);

        overrides.apply_defaults_from(&defaults);

        assert_eq!(overrides.get(Feature::DslV4), Some(false));
        assert_eq!(overrides.get(Feature::Feedback), Some(true));
    }

    #[test]
    fn raw_overrides_toggle_only_known_boolean_entries() {
        use RawFeatureValue::{Bool, UnsupportedType};

        for (label, key, value, on) in [
            ("enables feedback", "feedback", Bool(true), true),
            ("disables feedback", "feedback", Bool(false), false),
            ("unknown key ignored", "future_flag", Bool(true), false),
            ("unsupported type", "feedback", UnsupportedType, false),
        ] {
            let features = Features::from_raw_overrides(&raw([(key, value)]));

            assert_eq!(features.enabled(Feature::Feedback), on, "{label}");
        }
    }

    #[test]
    fn process_overrides_disable_config_enabled_feature() {
        let mut overrides = FeatureOverrides::default();
        overrides.set(Feature::Feedback, false);
        let raw = raw([("feedback", RawFeatureValue::Bool(true))]);
        let mut features = Features::from_raw_overrides(&raw);

        features.apply_overrides(&overrides);

        assert!(!features.enabled(Feature::Feedback));
    }

    #[test]
    fn statuses_report_invalid_known_value_without_enabling_feature() {
        let raw = raw([("feedback", RawFeatureValue::UnsupportedType)]);
        let features = Features::from_raw_overrides(&raw);

        let status = FEATURE_SPECS
            .iter()
            .map(|spec| status_from_raw(spec, &raw, &features))
            .find(|status| status.key == "feedback")
            .expect("feedback status");

        assert_eq!(status.key, "feedback");
        assert_eq!(status.configured, FeatureConfiguredState::InvalidValue);
        assert!(!status.enabled);
    }

    #[test]
    fn unknown_feature_error_lists_valid_keys() {
        let error = unknown_feature_error("nope");

        assert!(error.to_string().contains("unknown feature 'nope'"));
        assert!(error.to_string().contains("feedback"));
    }
}
