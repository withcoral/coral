//! App-home state layout and persisted config ownership.

mod config;
mod layout;

pub(crate) use config::ConfigStore;
pub(crate) use config::{
    RawFeatureContainerState, RawFeatureOverrides, RawFeatureValue, load_raw_feature_overrides,
    set_raw_feature_override,
};
pub(crate) use layout::AppStateLayout;
