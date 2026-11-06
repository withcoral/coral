//! Reading Credential Field values from a Secret Store.
//!
//! # Interface
//!
//! [`SecretStore`] is the transport-neutral port: read the value a
//! [`SecretReference`] names, or check that it can be read. It knows nothing
//! about Workspaces or grants. [`SecretStores`] is the name-to-store registry
//! bootstrap builds from deployment config.
//!
//! [`kv_v2`] is the first adapter. A later adapter implements the same port.
//!
//! [`SecretStores::list`] is open to anything holding the registry, so a caller
//! serving people goes through `coral_administration::SecretStoreAdministration`.
//!
//! No error here carries a path, a key, a value or a store's token, so a caller
//! may report any of them to a Program.

use std::collections::BTreeMap;
use std::sync::Arc;

pub use coral_credentials::{Secret, SecretReference};

pub mod kv_v2;

/// Why a store did not give up a value. Each is a different thing to fix.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The store did not answer, is sealed, or is outside the destination policy.
    #[error("the secret store is unreachable")]
    Unreachable,
    /// The store has no entry at the reference's path.
    #[error("the secret store has no such entry")]
    EntryNotFound,
    /// The entry exists and has no such key.
    #[error("the secret store entry has no such key")]
    KeyNotFound,
    /// The store refused Coral's identity, or its policy forbids this read.
    #[error("the secret store denied permission")]
    PermissionDenied,
    /// The store answered, but not with a usable value.
    #[error("the secret store sent a malformed response")]
    MalformedResponse,
}

/// A connection to one secrets system outside Coral.
#[async_trait::async_trait]
pub trait SecretStore: Send + Sync {
    /// Reads the current value the reference names. The caller holds it for
    /// one use and never stores it.
    ///
    /// # Errors
    /// Returns why the store gave up no value.
    async fn read(&self, reference: &SecretReference) -> Result<Secret, Error>;
}

/// Which secrets system a store connects to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecretStoreKind {
    /// `HashiCorp` Vault or `OpenBao` KV version 2.
    KvV2,
}

impl SecretStoreKind {
    /// A bounded name, safe for telemetry.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::KvV2 => "kv_v2",
        }
    }
}

/// A store's stable name: what a grant and a Secret Reference say.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SecretStoreName(String);

impl SecretStoreName {
    /// The longest name.
    pub const MAX_LEN: usize = 64;

    /// Accepts lowercase letters, digits, `-` and `_`, starting with a letter or digit.
    ///
    /// # Errors
    /// Returns [`InvalidSetting`], which does not repeat the rejected value.
    pub fn parse(value: &str) -> Result<Self, InvalidSetting> {
        let allowed = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
        if value.len() <= Self::MAX_LEN
            && value.starts_with(allowed)
            && value.chars().all(|c| allowed(c) || c == '-' || c == '_')
        {
            Ok(Self(value.to_owned()))
        } else {
            Err(InvalidSetting(
                "must be at most 64 lowercase letters, digits, `-` or `_`, starting with a letter or digit",
            ))
        }
    }

    /// The name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::borrow::Borrow<str> for SecretStoreName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

/// Why a store setting is unusable. Never repeats the rejected value, which an
/// operator may have pasted under the wrong key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct InvalidSetting(&'static str);

#[derive(Clone)]
struct DefinedStore {
    kind: SecretStoreKind,
    store: Arc<dyn SecretStore>,
}

/// The Secret Stores this deployment defines, by name.
#[derive(Clone, Default)]
pub struct SecretStores(Arc<BTreeMap<SecretStoreName, DefinedStore>>);

impl SecretStores {
    /// The registry of the given stores.
    ///
    /// # Errors
    /// Returns [`InvalidSetting`] when two stores share a name, so that no read
    /// can reach the wrong store.
    pub fn new(
        stores: impl IntoIterator<Item = (SecretStoreName, SecretStoreKind, Arc<dyn SecretStore>)>,
    ) -> Result<Self, InvalidSetting> {
        let mut defined = BTreeMap::new();
        for (name, kind, store) in stores {
            if defined.insert(name, DefinedStore { kind, store }).is_some() {
                return Err(InvalidSetting("another store has this name"));
            }
        }
        Ok(Self(Arc::new(defined)))
    }

    /// Each defined store's name and kind, ordered by name.
    pub fn list(&self) -> impl Iterator<Item = (&SecretStoreName, SecretStoreKind)> {
        self.0.iter().map(|(name, defined)| (name, defined.kind))
    }

    /// The kind of the store with this name, while config defines it.
    #[must_use]
    pub fn kind(&self, name: &str) -> Option<SecretStoreKind> {
        self.0.get(name).map(|defined| defined.kind)
    }

    /// The store with this name, while config defines it.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<Arc<dyn SecretStore>> {
        self.0.get(name).map(|defined| Arc::clone(&defined.store))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Refusing;

    #[async_trait::async_trait]
    impl SecretStore for Refusing {
        async fn read(&self, _reference: &SecretReference) -> Result<Secret, Error> {
            Err(Error::PermissionDenied)
        }
    }

    #[test]
    fn a_store_name_is_a_short_lowercase_token() {
        let longest = "a".repeat(SecretStoreName::MAX_LEN);
        for name in ["a", "9", "team-a_1", longest.as_str()] {
            assert_eq!(SecretStoreName::parse(name).unwrap().as_str(), name);
        }
        let too_long = "a".repeat(SecretStoreName::MAX_LEN + 1);
        for name in ["", "-a", "_a", "A", "a b", "a/b", "a.b", too_long.as_str()] {
            assert!(SecretStoreName::parse(name).is_err(), "{name}");
        }
    }

    #[tokio::test]
    async fn the_registry_lists_by_name_and_finds_a_store() {
        let named = |name| SecretStoreName::parse(name).unwrap();
        let store = || -> Arc<dyn SecretStore> { Arc::new(Refusing) };
        let stores = SecretStores::new([
            (named("zeta"), SecretStoreKind::KvV2, store()),
            (named("alpha"), SecretStoreKind::KvV2, store()),
        ])
        .unwrap();
        let listed: Vec<_> = stores.list().map(|(name, _)| name.as_str()).collect();
        assert_eq!(listed, ["alpha", "zeta"]);
        assert!(stores.get("missing").is_none());

        let reference = SecretReference::new("zeta".into(), "path".into(), None).unwrap();
        let found = stores.get("zeta").unwrap();
        assert_eq!(
            found.read(&reference).await.unwrap_err(),
            Error::PermissionDenied
        );

        assert!(
            SecretStores::new([
                (named("same"), SecretStoreKind::KvV2, store()),
                (named("same"), SecretStoreKind::KvV2, store()),
            ])
            .is_err(),
            "a duplicate name must not drop a store"
        );
    }
}
