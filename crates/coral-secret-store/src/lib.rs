//! Reading Credential Field values from a Secret Store.
//!
//! # Interface
//!
//! [`SecretStore`] is the transport-neutral trait every store implements:
//! read the value a [`SecretReference`] names, and say whether it can read at
//! all. It knows nothing about Workspaces or grants. [`SecretStores`] is the
//! name-to-store registry bootstrap builds from deployment config.
//!
//! Core has one store kind, [`EnvKind`], which every build registers. A
//! launching binary registers each other kind it has as a
//! [`SecretStoreFactory`] in [`SecretStoreKinds`]; bootstrap hands that factory
//! a store's settings ([`StoreSettings`]) and keeps the store it builds. A
//! store whose kind the build lacks is still defined, and every read of it
//! fails with [`Error::KindMissing`] (ADR 0036).
//!
//! [`SecretStores::list`] is open to anything holding the registry, so a caller
//! serving people goes through `coral_administration::SecretStoreAdministration`.
//!
//! No error here carries a path, a key, a value or a store's token, so a caller
//! may report any of them to a Program.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

pub use coral_credentials::{Secret, SecretReference};

mod env;
mod settings;

pub use env::{EnvKind, EnvReader};

pub use settings::{BuildEnvironment, NamedVariables, SettingError, StoreSettings, env_var_name};

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
    /// The store's kind is an Enterprise Feature and the Edition is Community.
    #[error("the secret store needs the Enterprise Edition")]
    NeedsEnterprise,
    /// This build has no adapter for the store's kind.
    #[error("this build cannot read this kind of secret store")]
    KindMissing,
}

/// Whether a store can read right now. A hint for display: the read decides.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreStatus {
    /// Reads go to the store.
    Available,
    /// Reads fail with [`Error::NeedsEnterprise`] until the Edition is Enterprise.
    NeedsEnterprise,
    /// Reads fail with [`Error::KindMissing`] in this build.
    KindMissing,
}

/// What a Secret Reference to one store holds, so a form asks for exactly
/// that. Labels are what people see; Reef writes any other words.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceForm {
    /// What the reference's path is, such as "Path" or "Variable name".
    pub path_label: Cow<'static, str>,
    /// What every path the store reads starts with, when the store sets that.
    pub path_prefix: Option<String>,
    /// The key, when a reference to the store holds one.
    pub key: Option<KeyField>,
}

/// The key part of a [`ReferenceForm`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyField {
    /// What the key is, such as "Key".
    pub label: Cow<'static, str>,
    /// Whether a reference must hold a key.
    pub required: bool,
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

    /// What a reference to this store holds.
    fn reference_form(&self) -> ReferenceForm;

    /// Whether reads can reach the store right now, asked again each time.
    fn status(&self) -> StoreStatus {
        StoreStatus::Available
    }
}

/// Which secrets system a store connects to: the token config names it by, and
/// a label for people.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretStoreKind {
    name: Cow<'static, str>,
    label: Cow<'static, str>,
}

const TOKEN_RULE: &str =
    "must be at most 64 lowercase letters, digits, `-` or `_`, starting with a letter or digit";

impl SecretStoreKind {
    /// A kind a build has, named by `name`, the token in `[[secret_stores]]`,
    /// and shown as `label`.
    ///
    /// # Errors
    /// Returns [`InvalidSetting`] unless `name` is a short lowercase token.
    pub fn new(name: &'static str, label: &'static str) -> Result<Self, InvalidSetting> {
        if is_token(name, 64) {
            Ok(Self {
                name: Cow::Borrowed(name),
                label: Cow::Borrowed(label),
            })
        } else {
            Err(InvalidSetting::new(TOKEN_RULE))
        }
    }

    /// A kind config names that this build lacks. It shows its token.
    ///
    /// # Errors
    /// Returns [`InvalidSetting`] unless `name` is a short lowercase token.
    pub fn missing(name: &str) -> Result<Self, InvalidSetting> {
        if is_token(name, 64) {
            Ok(Self {
                name: Cow::Owned(name.to_owned()),
                label: Cow::Owned(name.to_owned()),
            })
        } else {
            Err(InvalidSetting::new(TOKEN_RULE))
        }
    }

    /// The token config names the kind by.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// What people call the kind. A kind this build lacks shows its token.
    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    /// A bounded name, safe for telemetry: the kind's token when the build has
    /// it, and `missing` for any kind config names that the build lacks.
    #[must_use]
    pub fn telemetry_name(&self) -> &'static str {
        match self.name {
            Cow::Borrowed(name) => name,
            Cow::Owned(_) => "missing",
        }
    }
}

fn is_token(value: &str, max_len: usize) -> bool {
    let allowed = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    value.len() <= max_len
        && value.starts_with(allowed)
        && value.chars().all(|c| allowed(c) || c == '-' || c == '_')
}

/// Builds the stores of one kind from their settings in deployment config.
pub trait SecretStoreFactory: Send + Sync {
    /// The kind this factory builds.
    fn kind(&self) -> &SecretStoreKind;

    /// Checks one store's settings without reading the environment. `named`
    /// holds the variables the rest of config names, for a kind that reads
    /// the environment to stay clear of.
    ///
    /// # Errors
    /// Returns the field and reason a setting is unusable, never its value.
    fn validate(
        &self,
        settings: &StoreSettings,
        named: &NamedVariables,
    ) -> Result<(), SettingError>;

    /// Builds one store. Sends nothing, so an unreachable store does not stop
    /// Coral from starting.
    ///
    /// # Errors
    /// Returns the field and reason a setting is unusable, never its value.
    fn build(
        &self,
        settings: &StoreSettings,
        environment: &BuildEnvironment<'_>,
    ) -> Result<Arc<dyn SecretStore>, SettingError>;
}

/// The store kinds a build has, by name. The default has none.
#[derive(Clone, Default)]
pub struct SecretStoreKinds(Vec<Arc<dyn SecretStoreFactory>>);

impl SecretStoreKinds {
    /// Adds a kind. A later factory for the same kind replaces the earlier one.
    #[must_use]
    pub fn with(mut self, factory: Arc<dyn SecretStoreFactory>) -> Self {
        self.0
            .retain(|other| other.kind().name() != factory.kind().name());
        self.0.push(factory);
        self
    }

    /// Adds each of `other`'s kinds, in order, as [`Self::with`] does.
    #[must_use]
    pub fn with_all(self, other: &Self) -> Self {
        other
            .0
            .iter()
            .fold(self, |kinds, factory| kinds.with(Arc::clone(factory)))
    }

    /// The factory for the kind config names `name`.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Arc<dyn SecretStoreFactory>> {
        self.0.iter().find(|factory| factory.kind().name() == name)
    }
}

/// Stands in for a store whose kind this build lacks.
pub struct MissingKind;

#[async_trait::async_trait]
impl SecretStore for MissingKind {
    async fn read(&self, _reference: &SecretReference) -> Result<Secret, Error> {
        Err(Error::KindMissing)
    }

    /// Nothing says what the kind's references hold, so a saved one shows
    /// whatever it has.
    fn reference_form(&self) -> ReferenceForm {
        ReferenceForm {
            path_label: "Path".into(),
            path_prefix: None,
            key: Some(KeyField {
                label: "Key".into(),
                required: false,
            }),
        }
    }

    fn status(&self) -> StoreStatus {
        StoreStatus::KindMissing
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
        if is_token(value, Self::MAX_LEN) {
            Ok(Self(value.to_owned()))
        } else {
            Err(InvalidSetting::new(TOKEN_RULE))
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

impl InvalidSetting {
    /// A reason that does not hold the rejected value.
    #[must_use]
    pub const fn new(reason: &'static str) -> Self {
        Self(reason)
    }
}

/// What people may learn of one defined store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreDescription {
    /// The store's kind.
    pub kind: SecretStoreKind,
    /// Whether it can read right now.
    pub status: StoreStatus,
    /// What a reference to it holds.
    pub reference_form: ReferenceForm,
}

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
                return Err(InvalidSetting::new("another store has this name"));
            }
        }
        Ok(Self(Arc::new(defined)))
    }

    /// Each defined store's name, kind and current status, ordered by name.
    pub fn list(&self) -> impl Iterator<Item = (&SecretStoreName, &SecretStoreKind, StoreStatus)> {
        self.0
            .iter()
            .map(|(name, defined)| (name, &defined.kind, defined.store.status()))
    }

    /// The kind, current status and reference form of the store with this
    /// name, while config defines it.
    #[must_use]
    pub fn describe(&self, name: &str) -> Option<StoreDescription> {
        self.0.get(name).map(|defined| StoreDescription {
            kind: defined.kind.clone(),
            status: defined.store.status(),
            reference_form: defined.store.reference_form(),
        })
    }

    /// The kind of the store with this name, while config defines it.
    #[must_use]
    pub fn kind(&self, name: &str) -> Option<&SecretStoreKind> {
        self.0.get(name).map(|defined| &defined.kind)
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

        fn reference_form(&self) -> ReferenceForm {
            MissingKind.reference_form()
        }
    }

    fn kind(name: &'static str) -> SecretStoreKind {
        SecretStoreKind::new(name, "A kind").unwrap()
    }

    #[test]
    fn a_store_name_and_a_kind_are_short_lowercase_tokens() {
        let longest = "a".repeat(SecretStoreName::MAX_LEN);
        for name in ["a", "9", "team-a_1", longest.as_str()] {
            assert_eq!(SecretStoreName::parse(name).unwrap().as_str(), name);
            assert_eq!(SecretStoreKind::missing(name).unwrap().name(), name);
        }
        let too_long = "a".repeat(SecretStoreName::MAX_LEN + 1);
        for name in ["", "-a", "_a", "A", "a b", "a/b", "a.b", too_long.as_str()] {
            assert!(SecretStoreName::parse(name).is_err(), "{name}");
            assert!(SecretStoreKind::missing(name).is_err(), "{name}");
        }
    }

    #[tokio::test]
    async fn the_registry_lists_by_name_with_status_and_finds_a_store() {
        let named = |name| SecretStoreName::parse(name).unwrap();
        let stores = SecretStores::new([
            (named("zeta"), kind("first"), Arc::new(Refusing) as _),
            (named("alpha"), kind("absent"), Arc::new(MissingKind) as _),
        ])
        .unwrap();
        let listed: Vec<_> = stores
            .list()
            .map(|(name, kind, status)| (name.as_str(), kind.name(), status))
            .collect();
        assert_eq!(
            listed,
            [
                ("alpha", "absent", StoreStatus::KindMissing),
                ("zeta", "first", StoreStatus::Available),
            ]
        );
        assert!(stores.get("missing").is_none());
        // A store says what its references hold, and describe passes it on.
        assert_eq!(
            stores.describe("alpha"),
            Some(StoreDescription {
                kind: kind("absent"),
                status: StoreStatus::KindMissing,
                reference_form: ReferenceForm {
                    path_label: "Path".into(),
                    path_prefix: None,
                    key: Some(KeyField {
                        label: "Key".into(),
                        required: false,
                    }),
                },
            })
        );
        assert!(stores.describe("missing").is_none());

        let reference =
            |store: &str| SecretReference::new(store.into(), "path".into(), None).unwrap();
        let found = stores.get("zeta").unwrap();
        assert_eq!(
            found.read(&reference("zeta")).await.unwrap_err(),
            Error::PermissionDenied
        );
        let absent = stores.get("alpha").unwrap();
        assert_eq!(
            absent.read(&reference("alpha")).await.unwrap_err(),
            Error::KindMissing
        );

        assert!(
            SecretStores::new([
                (named("same"), kind("first"), Arc::new(Refusing) as _),
                (named("same"), kind("first"), Arc::new(Refusing) as _),
            ])
            .is_err(),
            "a duplicate name must not drop a store"
        );
    }

    struct Factory(SecretStoreKind);

    impl SecretStoreFactory for Factory {
        fn kind(&self) -> &SecretStoreKind {
            &self.0
        }
        fn validate(
            &self,
            _settings: &StoreSettings,
            _named: &NamedVariables,
        ) -> Result<(), SettingError> {
            Ok(())
        }
        fn build(
            &self,
            _settings: &StoreSettings,
            _environment: &BuildEnvironment<'_>,
        ) -> Result<Arc<dyn SecretStore>, SettingError> {
            Ok(Arc::new(Refusing))
        }
    }

    #[test]
    fn telemetry_names_only_the_kinds_a_build_has() {
        assert_eq!(kind("kv-v2").telemetry_name(), "kv-v2");
        assert_eq!(
            SecretStoreKind::missing("anything")
                .unwrap()
                .telemetry_name(),
            "missing"
        );
    }

    #[test]
    fn a_later_factory_replaces_one_of_the_same_kind() {
        let first = SecretStoreKind::new("same", "First").unwrap();
        let second = SecretStoreKind::new("same", "Second").unwrap();
        let kinds = SecretStoreKinds::default()
            .with(Arc::new(Factory(first)))
            .with(Arc::new(Factory(second)));
        assert_eq!(kinds.get("same").unwrap().kind().label(), "Second");
        assert!(kinds.get("other").is_none());
    }
}
