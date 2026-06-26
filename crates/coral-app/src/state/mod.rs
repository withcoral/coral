//! App-home state layout and persisted config ownership.

mod config;
mod layout;

#[expect(unused_imports, reason = "used in next stack PR")]
pub(crate) use config::{AppConfig, ConfigStore, ConfigWorkspaceStore};
pub(crate) use config::{
    RawFeatureContainerState, RawFeatureOverrides, RawFeatureValue, load_raw_feature_overrides,
    set_raw_feature_override,
};
pub(crate) use layout::AppStateLayout;
