//! Stored provider identity registry and user-owned gRPC APIs.

mod manager;
mod service;

pub(crate) use manager::IdentityManager;
pub use manager::{
    CreateFixedTokenIdentityCommand, IdentityManagementHandle, IdentityMaterialGuard, IdentityName,
    IdentityOwner, IdentityRecord, IdentityStore,
};
pub(crate) use service::IdentityService;
