//! Stored user-owned provider identity registry and APIs.

mod manager;
mod service;

pub(crate) use manager::UserOwnedIdentityManager;
pub use manager::{
    CreateFixedTokenIdentityCommand, IdentityManagementHandle, IdentityOwnerKey,
    UserOwnedIdentityMaterialGuard, UserOwnedIdentityName, UserOwnedIdentityRecord,
    UserOwnedIdentityStore,
};
pub(crate) use service::IdentityService;
