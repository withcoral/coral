//! Stored user-owned provider identity registry and APIs.

mod manager;
mod runtime;
mod service;

pub(crate) use manager::UserOwnedIdentityManager;
pub use manager::{
    CreateFixedTokenIdentityCommand, CreateOAuthIdentityCommand, IdentityCredentialInput,
    IdentityManagementHandle, IdentityOwnerKey, UserOwnedIdentityMaterialGuard,
    UserOwnedIdentityRecord, UserOwnedIdentityStore,
};
pub(crate) use service::IdentityService;
