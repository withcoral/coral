//! Global identity-spec registry.

mod manager;
mod service;

pub(crate) use manager::{
    IdentitySpecImportInstall, IdentitySpecInputValue, IdentitySpecManager, IdentitySpecRecord,
    identity_spec_fingerprint, validate_identity_spec_name,
};
pub use manager::{
    IdentitySpecInputMaterialStorage, IdentitySpecManifestMetadata, IdentitySpecRegistry,
    IdentitySpecRegistryRecord, IdentitySpecUsageProvider,
    identity_spec_input_material_from_manifest,
    identity_spec_input_material_from_manifest_with_existing, identity_spec_manifest_metadata,
};
pub(crate) use service::IdentitySpecService;
