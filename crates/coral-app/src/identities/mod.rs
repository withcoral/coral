//! Stored provider identity registry and user-owned gRPC APIs.

mod manager;
mod runtime;
mod service;

pub(crate) use manager::IdentityManager;
pub use manager::{
    CreateFixedTokenIdentityCommand, CreateOAuthIdentityCommand, IdentityCredentialInput,
    IdentityManagementHandle, IdentityMaterialGuard, IdentityName, IdentityOwner, IdentityRecord,
    IdentityStore,
};
pub(crate) use service::IdentityService;
