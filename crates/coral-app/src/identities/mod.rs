//! Stored provider identity-instance registry and user-owned gRPC APIs.

mod manager;
mod service;

pub(crate) use manager::IdentityInstanceManager;
pub use manager::{
    CreateFixedTokenIdentityCommand, IdentityInstanceMaterialGuard, IdentityInstanceName,
    IdentityInstanceRecord, IdentityInstanceStore, IdentityManagementHandle, IdentityOwnerKey,
};
pub(crate) use service::IdentityService;
