//! App-home state layout and persisted config ownership.

mod config;
mod layout;

pub(crate) use config::{AppConfig, ConfigStore};
pub(crate) use config::{
    RawFeatureContainerState, RawFeatureOverrides, RawFeatureValue, load_raw_feature_overrides,
    set_raw_feature_override,
};
pub(crate) use layout::AppStateLayout;
#[expect(
    unused_imports,
    reason = "re-export consumed by the identity-spec manager in a later PR"
)]
pub(crate) use layout::INSTALLED_IDENTITY_FILE_NAME;
