//! Stored user-owned provider identity storage seams.

mod manager;

pub use manager::{
    IdentityOwnerKey, UserOwnedIdentityMaterialGuard, UserOwnedIdentityName,
    UserOwnedIdentityRecord, UserOwnedIdentityStore,
};
