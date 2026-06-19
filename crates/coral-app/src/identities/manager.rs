//! Storage seam types and traits for user-owned provider identity material.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use crate::bootstrap::AppError;
use crate::identity::parse_path_segment;

/// One stored user-owned identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserOwnedIdentityRecord {
    /// Stable identity name used by source identity bindings.
    pub name: UserOwnedIdentityName,
    /// Installed identity spec used to instantiate this identity.
    pub identity_spec: String,
    /// Fingerprint of the identity spec at creation time.
    pub identity_spec_fingerprint: Option<String>,
    /// Provider or issuer copied from the identity spec.
    pub issuer: String,
    /// Identity type copied from the identity spec.
    pub identity_type: String,
    /// Safe provider metadata, never credential material.
    pub metadata: BTreeMap<String, String>,
}

/// Validated storage name for one user-owned identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UserOwnedIdentityName(String);

impl UserOwnedIdentityName {
    /// Builds an identity name from a storage-safe string.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the name is empty or contains path separators.
    pub fn new(value: impl Into<String>) -> Result<Self, AppError> {
        parse_path_segment("identity", &value.into()).map(Self)
    }

    /// Returns the storage name string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for UserOwnedIdentityName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for UserOwnedIdentityName {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for UserOwnedIdentityName {
    type Error = AppError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// Opaque durable owner key for stored provider-facing identity material.
///
/// OSS Coral only constructs this from the request user principal. Product
/// runtimes can use their own stable owner keys without adding product-specific
/// ownership concepts to OSS identity management.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdentityOwnerKey(String);

impl IdentityOwnerKey {
    /// Builds an owner key from a storage-safe string.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the key is empty or contains path separators.
    pub fn new(value: impl Into<String>) -> Result<Self, AppError> {
        parse_path_segment("identity owner", &value.into()).map(Self)
    }

    /// Returns the storage key string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdentityOwnerKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for IdentityOwnerKey {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for IdentityOwnerKey {
    type Error = AppError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// Locked access to credential material for one user-owned identity.
#[tonic::async_trait]
pub trait UserOwnedIdentityMaterialGuard: Send {
    /// Reads the identity's credential material.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when material cannot be read or decoded.
    async fn read_material(&self) -> Result<BTreeMap<String, String>, AppError>;

    /// Replaces the identity's credential material.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when material cannot be written.
    async fn write_material(&self, material: &BTreeMap<String, String>) -> Result<(), AppError>;
}

/// Durable storage backend for user-owned identities.
#[tonic::async_trait]
pub trait UserOwnedIdentityStore: Send + Sync + std::fmt::Debug + 'static {
    /// Lists identities owned by one opaque owner key.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be read.
    async fn list_identities(
        &self,
        owner: &IdentityOwnerKey,
    ) -> Result<Vec<UserOwnedIdentityRecord>, AppError>;

    /// Loads one identity owned by one opaque owner key.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be read.
    async fn load_identity(
        &self,
        owner: &IdentityOwnerKey,
        identity_name: &UserOwnedIdentityName,
    ) -> Result<Option<UserOwnedIdentityRecord>, AppError>;

    /// Replaces one identity and its credential material atomically.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be written.
    async fn replace_identity(
        &self,
        owner: &IdentityOwnerKey,
        record: &UserOwnedIdentityRecord,
        material: &BTreeMap<String, String>,
    ) -> Result<(), AppError>;

    /// Deletes one identity and its credential material.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be written.
    async fn delete_identity(
        &self,
        owner: &IdentityOwnerKey,
        identity_name: &UserOwnedIdentityName,
    ) -> Result<bool, AppError>;

    /// Returns locked access to one identity's credential material.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the material lock cannot be acquired.
    async fn material_guard(
        &self,
        owner: &IdentityOwnerKey,
        identity_name: &UserOwnedIdentityName,
    ) -> Result<Box<dyn UserOwnedIdentityMaterialGuard>, AppError>;
}

#[cfg(test)]
mod tests {
    use super::{IdentityOwnerKey, UserOwnedIdentityName};

    #[test]
    fn owner_key_rejects_storage_unsafe_values() {
        IdentityOwnerKey::new(" ").unwrap_err();
        IdentityOwnerKey::new("a/b").unwrap_err();
        IdentityOwnerKey::new("a\\b").unwrap_err();
        IdentityOwnerKey::new("..").unwrap_err();
    }

    #[test]
    fn owner_key_round_trips_storage_key() {
        let owner = IdentityOwnerKey::new("member-123").expect("owner key");

        assert_eq!(owner.as_str(), "member-123");
        assert_eq!(owner.to_string(), "member-123");
        assert_eq!(
            "member-123".parse::<IdentityOwnerKey>().expect("parse"),
            owner
        );
    }

    #[test]
    fn identity_name_rejects_storage_unsafe_values() {
        UserOwnedIdentityName::new(" ").unwrap_err();
        UserOwnedIdentityName::new("a/b").unwrap_err();
        UserOwnedIdentityName::new("a\\b").unwrap_err();
        UserOwnedIdentityName::new("..").unwrap_err();
    }

    #[test]
    fn identity_name_round_trips_storage_name() {
        let identity_name = UserOwnedIdentityName::new("github-primary").expect("identity name");

        assert_eq!(identity_name.as_str(), "github-primary");
        assert_eq!(identity_name.to_string(), "github-primary");
        assert_eq!(
            "github-primary"
                .parse::<UserOwnedIdentityName>()
                .expect("parse"),
            identity_name
        );
    }
}
