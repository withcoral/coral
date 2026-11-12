//! The `env` store kind: Credential Field values from environment variables.
//!
//! A store names a `prefix`, and a Secret Reference's path is the name of one
//! variable that starts with it, read on each use. Bootstrap hands the kind its only way to read
//! the environment, so a store reads nothing outside its prefix.

use std::env::VarError;
use std::sync::Arc;

use serde::Deserialize;

use crate::{
    BuildEnvironment, Error, NamedVariables, ReferenceForm, Secret, SecretReference, SecretStore,
    SecretStoreFactory, SecretStoreKind, SettingError, StoreSettings,
};

/// Reads one process environment variable.
pub type EnvReader = Arc<dyn Fn(&str) -> Result<Option<String>, VarError> + Send + Sync>;

/// The `env` store kind.
pub struct EnvKind {
    kind: SecretStoreKind,
    read: EnvReader,
}

impl EnvKind {
    /// Builds stores that read variables through `read`.
    ///
    /// # Panics
    /// Never: the kind's name is a valid token.
    #[must_use]
    pub fn new(read: EnvReader) -> Self {
        Self {
            kind: SecretStoreKind::new("env", "Environment variables").expect("a valid kind name"),
            read,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSettings {
    prefix: String,
}

impl SecretStoreFactory for EnvKind {
    fn kind(&self) -> &SecretStoreKind {
        &self.kind
    }

    fn validate(
        &self,
        settings: &StoreSettings,
        named: &NamedVariables,
    ) -> Result<(), SettingError> {
        prefix(settings, named).map(drop)
    }

    fn build(
        &self,
        settings: &StoreSettings,
        _environment: &BuildEnvironment<'_>,
    ) -> Result<Arc<dyn SecretStore>, SettingError> {
        // Bootstrap validated the prefix against the rest of config; the rules
        // that need no config hold here too.
        let prefix = prefix(settings, &NamedVariables::default())?;
        Ok(Arc::new(EnvStore {
            prefix,
            read: Arc::clone(&self.read),
        }))
    }
}

/// Coral's own variables. No store may read under this prefix.
const CORAL_PREFIX: &str = "CORAL_";

/// The checked prefix. The checks ignore case, as Windows does.
fn prefix(settings: &StoreSettings, named: &NamedVariables) -> Result<String, SettingError> {
    let raw: RawSettings = settings.decode()?;
    let prefix = raw.prefix;
    if prefix.is_empty() {
        return Err(SettingError::new("prefix", "must not be empty"));
    }
    let upper = prefix.to_ascii_uppercase();
    if CORAL_PREFIX.starts_with(&upper) || upper.starts_with(CORAL_PREFIX) {
        return Err(SettingError::new(
            "prefix",
            "must not cover Coral's own `CORAL_` variables",
        ));
    }
    if !is_variable_name(&prefix) {
        return Err(SettingError::new(
            "prefix",
            "must hold only letters, digits and `_`",
        ));
    }
    if let Some((setting, _)) = named
        .iter()
        .find(|(_, variable)| variable.to_ascii_uppercase().starts_with(&upper))
    {
        return Err(SettingError::new(
            "prefix",
            format!("covers the variable that {setting} names"),
        ));
    }
    Ok(prefix)
}

/// ASCII letters, digits and `_`: what a portable variable name holds.
fn is_variable_name(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

struct EnvStore {
    prefix: String,
    read: EnvReader,
}

#[async_trait::async_trait]
impl SecretStore for EnvStore {
    fn reference_form(&self) -> ReferenceForm {
        ReferenceForm {
            path_label: "Variable name".into(),
            path_prefix: Some(self.prefix.clone()),
            key: None,
        }
    }

    async fn read(&self, reference: &SecretReference) -> Result<Secret, Error> {
        // The whole name is a variable name that starts with the prefix and
        // has more after it, or the store reads nothing.
        let name = reference.path();
        let under_prefix = name
            .strip_prefix(&self.prefix)
            .is_some_and(|rest| !rest.is_empty());
        if !under_prefix || !is_variable_name(name) {
            return Err(Error::EntryNotFound);
        }
        if reference.key().is_some() {
            return Err(Error::KeyNotFound);
        }
        let value = (self.read)(name)
            .map_err(|_not_unicode| Error::MalformedResponse)?
            .ok_or(Error::EntryNotFound)?;
        Secret::new(value).map_err(|_unusable| Error::MalformedResponse)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use super::*;

    /// A fake environment that records every name the store asks for.
    struct Environment {
        values: BTreeMap<&'static str, Result<&'static str, ()>>,
        asked: Arc<Mutex<Vec<String>>>,
    }

    impl Environment {
        fn new(values: impl IntoIterator<Item = (&'static str, Result<&'static str, ()>)>) -> Self {
            Self {
                values: values.into_iter().collect(),
                asked: Arc::default(),
            }
        }

        fn kind(&self) -> EnvKind {
            let values = self.values.clone();
            let asked = Arc::clone(&self.asked);
            EnvKind::new(Arc::new(move |name: &str| {
                asked.lock().unwrap().push(name.to_owned());
                match values.get(name) {
                    None => Ok(None),
                    Some(Ok(value)) => Ok(Some((*value).to_owned())),
                    Some(Err(())) => Err(VarError::NotUnicode("\u{fffd}".into())),
                }
            }))
        }

        fn asked(&self) -> Vec<String> {
            self.asked.lock().unwrap().clone()
        }
    }

    fn settings(raw: &str) -> StoreSettings {
        StoreSettings::new(toml::from_str(raw).unwrap())
    }

    fn store(kind: &EnvKind, prefix: &str) -> Arc<dyn SecretStore> {
        let no_env =
            |_: &str| -> Result<Option<String>, VarError> { panic!("build reads no variable") };
        kind.build(
            &settings(&format!("prefix = '{prefix}'")),
            &BuildEnvironment {
                policy: coral_outbound::DestinationPolicy::default(),
                env: &no_env,
            },
        )
        .unwrap()
    }

    fn reference(path: &str, key: Option<&str>) -> SecretReference {
        SecretReference::new("env".into(), path.into(), key.map(str::to_owned)).unwrap()
    }

    #[tokio::test]
    async fn a_reference_reads_the_variable_it_names() {
        let environment = Environment::new([("MYCO_API_KEY", Ok("sk-live-value"))]);
        let store = store(&environment.kind(), "MYCO_");
        let value = store.read(&reference("MYCO_API_KEY", None)).await.unwrap();
        assert_eq!(value.expose(), "sk-live-value");
        assert_eq!(environment.asked(), ["MYCO_API_KEY"]);
    }

    /// A reference names one whole variable, so the form asks for its name
    /// and no key, and says which prefix the name starts with.
    #[test]
    fn a_reference_holds_one_variable_name() {
        let store = store(&Environment::new([]).kind(), "MYCO_");
        assert_eq!(
            store.reference_form(),
            ReferenceForm {
                path_label: "Variable name".into(),
                path_prefix: Some("MYCO_".to_owned()),
                key: None,
            }
        );
    }

    #[tokio::test]
    async fn a_read_fails_with_the_reason_to_fix() {
        let environment = Environment::new([
            ("MYCO_API_KEY", Ok("sk-live-value")),
            ("MYCO_EMPTY", Ok("")),
            ("MYCO_BINARY", Err(())),
        ]);
        let store = store(&environment.kind(), "MYCO_");
        for (path, key, expected) in [
            ("MYCO_UNSET", None, Error::EntryNotFound),
            ("MYCO_API_KEY", Some("token"), Error::KeyNotFound),
            ("MYCO_EMPTY", None, Error::MalformedResponse),
            ("MYCO_BINARY", None, Error::MalformedResponse),
        ] {
            let error = store.read(&reference(path, key)).await.unwrap_err();
            assert_eq!(error, expected, "{path}");
        }
    }

    /// A name the store reads starts with its prefix, exactly, and has more
    /// after it, so no path can reach a variable outside the prefix.
    #[tokio::test]
    async fn a_name_outside_the_prefix_reads_nothing() {
        let environment = Environment::new([
            ("MYCO_API_KEY", Ok("sk-live-value")),
            ("API_KEY", Ok("outside-the-prefix")),
            ("MYCO_", Ok("the-prefix-itself")),
            ("CORAL_CREDENTIALS_KEY", Ok("coral-s-own")),
        ]);
        let store = store(&environment.kind(), "MYCO_");
        for path in [
            "API_KEY",
            "MYCO_",
            "MYCO",
            "myco_API_KEY",
            "XMYCO_API_KEY",
            "CORAL_CREDENTIALS_KEY",
            "MYCO_../API_KEY",
            "MYCO_API_KEY=",
            "MYCO_API KEY",
            "MYCO_API-KEY",
            "MYCO_API_KEY\u{200b}",
            "MYCO_ÄPI",
        ] {
            let error = store.read(&reference(path, None)).await.unwrap_err();
            assert_eq!(error, Error::EntryNotFound, "{path}");
        }
        assert_eq!(environment.asked(), Vec::<String>::new());
    }

    #[test]
    fn a_prefix_must_not_cover_coral_s_variables_or_one_config_names() {
        let kind = Environment::new([]).kind();
        let named = NamedVariables::new([
            ("credentials.key_env".to_owned(), "MYCO_KEY".to_owned()),
            (
                "secret_stores[0] (vault).auth.secret_id_env".to_owned(),
                "VAULT_SECRET_ID".to_owned(),
            ),
        ]);
        let coral = "prefix: must not cover Coral's own `CORAL_` variables";
        for (prefix, expected) in [
            ("", "prefix: must not be empty"),
            ("C", coral),
            ("CO", coral),
            ("CORAL", coral),
            ("CORAL_", coral),
            ("coral_", coral),
            ("CORAL_MYCO_", coral),
            ("MY CO_", "prefix: must hold only letters, digits and `_`"),
            ("MY=CO_", "prefix: must hold only letters, digits and `_`"),
            (
                "MYCO_",
                "prefix: covers the variable that credentials.key_env names",
            ),
            (
                "myco",
                "prefix: covers the variable that credentials.key_env names",
            ),
            (
                "VAULT_",
                "prefix: covers the variable that secret_stores[0] (vault).auth.secret_id_env names",
            ),
        ] {
            let error = kind
                .validate(&settings(&format!("prefix = '{prefix}'")), &named)
                .expect_err(prefix);
            assert_eq!(error.to_string(), expected, "{prefix}");
        }
        for prefix in ["MYCO_API_", "VAULT_SECRET_ID_", "ACME_"] {
            kind.validate(&settings(&format!("prefix = '{prefix}'")), &named)
                .expect(prefix);
        }
    }
}
