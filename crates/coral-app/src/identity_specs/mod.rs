//! Global identity-spec registry.

mod manager;
mod service;

#[expect(
    unused_imports,
    reason = "re-exports consumed by the identity and source managers in later PRs"
)]
pub(crate) use manager::{
    IdentitySpecInputValue, IdentitySpecManager, IdentitySpecRecord, IdentitySpecSnapshot,
    identity_spec_fingerprint, validate_identity_spec_name,
};
pub use manager::{
    IdentitySpecManifestMetadata, IdentitySpecRegistry, IdentitySpecRegistryRecord,
    IdentitySpecUsageProvider, identity_spec_input_material_from_manifest,
    identity_spec_input_material_from_manifest_with_existing, identity_spec_manifest_metadata,
};
pub(crate) use service::IdentitySpecService;
