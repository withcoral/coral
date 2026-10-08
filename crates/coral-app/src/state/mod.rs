//! App-home state layout and persisted config ownership.

mod config;
pub(crate) mod db;
mod layout;

pub(crate) use config::{AppConfig, ConfigStore, RemovedWorkspaceConfig};
pub(crate) use config::{
    RawFeatureContainerState, RawFeatureOverrides, RawFeatureValue, load_raw_feature_overrides,
    set_raw_feature_override,
};
pub(crate) use layout::{
    AppStateLayout, V4OperationMetadataFile, V4OperationMetadataOrigin, V4ProjectionCatalogFile,
    V4ProjectionCatalogOrigin,
};
