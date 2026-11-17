//! The `[[secret_stores]]` section of deployment config.
//!
//! Core reads each entry's `name` and `kind`, and hands the rest to the store
//! kind the launching binary registered under that name. A store whose kind
//! the build lacks is still defined, so a deployment can move between builds:
//! Coral starts, warns, and fails every read of it.
//!
//! Every build has the `env` kind, which reads the environment through the
//! reader bootstrap hands it. A launching binary cannot replace it.
//!
//! Each entry decodes on its own, so a failure can name the store as well as
//! the field. A name is only repeated once it has passed validation, because
//! an operator can paste a secret under the wrong key.

use std::sync::Arc;

use coral_secret_store::{
    BuildEnvironment, EnvKind, EnvReader, MissingKind, NamedVariables, SecretStore,
    SecretStoreFactory, SecretStoreKind, SecretStoreKinds, SecretStoreName, SecretStores,
    SettingError, StoreSettings,
};

use super::AppError;
use coral_telemetry::safe;

/// The name of a store config defines, once it passed validation.
struct DefinedStoreName<'a>(&'a SecretStoreName);

impl std::fmt::Display for DefinedStoreName<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0.as_str())
    }
}

/// A validated store name is a bounded token the operator chose, not a secret.
impl safe::Identifier for DefinedStoreName<'_> {}

/// One entry, with its name and kind checked and the rest left to its kind.
struct Entry<'a> {
    /// Where a failure in this store's settings says it is.
    label: String,
    name: SecretStoreName,
    kind: SecretStoreKind,
    factory: Option<&'a Arc<dyn SecretStoreFactory>>,
    settings: StoreSettings,
}

fn invalid(message: impl std::fmt::Display) -> AppError {
    AppError::FailedPrecondition(format!("server configuration is invalid: {message}"))
}

fn rejected(label: &str, error: &SettingError) -> AppError {
    if error.field().is_empty() {
        invalid(format!("{label}: {}", error.reason()))
    } else {
        invalid(format!("{label}.{error}"))
    }
}

fn entry<'a>(
    index: usize,
    raw: &toml::Value,
    kinds: &'a SecretStoreKinds,
) -> Result<Entry<'a>, AppError> {
    let Some(table) = raw.as_table() else {
        return Err(invalid(format!("secret_stores[{index}]: must be a table")));
    };
    let mut settings = table.clone();
    let name = settings.remove("name");
    let kind = settings.remove("kind");
    // Repeat a name only once it is known not to be a misplaced secret.
    let name = match name.as_ref().map(toml::Value::as_str) {
        None => {
            return Err(invalid(format!(
                "secret_stores[{index}]: missing field `name`"
            )));
        }
        Some(None) => {
            return Err(invalid(format!(
                "secret_stores[{index}].name: unsupported value"
            )));
        }
        Some(Some(name)) => SecretStoreName::parse(name)
            .map_err(|error| invalid(format!("secret_stores[{index}].name: {error}")))?,
    };
    let label = format!("secret_stores[{index}] ({})", name.as_str());
    let kind = match kind.as_ref().map(toml::Value::as_str) {
        None => return Err(invalid(format!("{label}: missing field `kind`"))),
        Some(None) => return Err(invalid(format!("{label}.kind: unsupported value"))),
        Some(Some(kind)) => kind,
    };
    let factory = kinds.get(kind);
    let kind = match factory {
        Some(factory) => factory.kind().clone(),
        None => SecretStoreKind::missing(kind)
            .map_err(|error| invalid(format!("{label}.kind: {error}")))?,
    };
    Ok(Entry {
        label,
        name,
        kind,
        factory,
        settings: StoreSettings::new(settings),
    })
}

/// The launching binary's kinds, then the kinds every build has, which
/// replace any of the same name.
fn with_core_kinds(kinds: &SecretStoreKinds, read: EnvReader) -> SecretStoreKinds {
    SecretStoreKinds::default()
        .with_all(kinds)
        .with(Arc::new(EnvKind::new(read)))
}

/// Every environment variable config names: the value of each key that ends
/// in `_env`, by convention, one string or a list of them.
fn named_variables(config: &str) -> Result<NamedVariables, AppError> {
    fn walk(value: &toml::Value, path: &str, named: &mut Vec<(String, String)>) {
        match value {
            toml::Value::Table(table) => {
                for (key, value) in table {
                    let path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{path}.{key}")
                    };
                    if key.ends_with("_env") {
                        let variables = match value {
                            toml::Value::Array(values) => values.iter().collect(),
                            value => vec![value],
                        };
                        named.extend(
                            variables
                                .into_iter()
                                .filter_map(toml::Value::as_str)
                                .map(|name| (path.clone(), name.trim().to_owned())),
                        );
                    } else {
                        walk(value, &path, named);
                    }
                }
            }
            toml::Value::Array(values) => {
                for (index, value) in values.iter().enumerate() {
                    walk(value, &format!("{path}[{index}]"), named);
                }
            }
            _ => {}
        }
    }
    let config: toml::Table =
        toml::from_str(config).map_err(|_error| invalid("the file is not a TOML table"))?;
    let mut named = Vec::new();
    walk(&toml::Value::Table(config), "", &mut named);
    Ok(NamedVariables::new(named))
}

fn entries<'a>(
    config: &str,
    raw: &[toml::Value],
    kinds: &'a SecretStoreKinds,
) -> Result<Vec<Entry<'a>>, AppError> {
    let named = named_variables(config)?;
    let mut stores: Vec<Entry<'a>> = Vec::with_capacity(raw.len());
    for (index, raw) in raw.iter().enumerate() {
        let store = entry(index, raw, kinds)?;
        if stores.iter().any(|other| other.name == store.name) {
            return Err(invalid(format!(
                "{}.name: another store has this name",
                store.label
            )));
        }
        if let Some(factory) = store.factory {
            factory
                .validate(&store.settings, &named)
                .map_err(|error| rejected(&store.label, &error))?;
        }
        stores.push(store);
    }
    Ok(stores)
}

/// Checks each store's settings without reading the environment. `config`
/// is the whole file, whose `*_env` settings an `env` store stays clear of.
pub(super) fn validate(
    config: &str,
    raw: &[toml::Value],
    kinds: &SecretStoreKinds,
) -> Result<(), AppError> {
    // Validation reads no variable.
    let kinds = with_core_kinds(kinds, Arc::new(|_: &str| Ok(None)));
    entries(config, raw, &kinds).map(drop)
}

/// Builds the stores. No request leaves here, so an unreachable store does
/// not stop Coral from starting; a missing secret does.
pub(super) fn build(
    config: &str,
    raw: &[toml::Value],
    kinds: &SecretStoreKinds,
    policy: coral_outbound::DestinationPolicy,
    env: &EnvReader,
) -> Result<SecretStores, AppError> {
    let kinds = with_core_kinds(kinds, Arc::clone(env));
    let environment = BuildEnvironment {
        policy,
        env: &**env,
    };
    let mut stores = Vec::with_capacity(raw.len());
    for entry in entries(config, raw, &kinds)? {
        let store: Arc<dyn SecretStore> = if let Some(factory) = entry.factory {
            factory
                .build(&entry.settings, &environment)
                .map_err(|error| rejected(&entry.label, &error))?
        } else {
            tracing::warn!(
                coral.secret_store.name = safe::id(&DefinedStoreName(&entry.name)),
                coral.secret_store.kind = safe::kind(entry.kind.telemetry_name()),
                "this build has no adapter for a defined Secret Store's kind, so every read of it fails"
            );
            Arc::new(MissingKind)
        };
        stores.push((entry.name, entry.kind, store));
    }
    SecretStores::new(stores).map_err(invalid)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use coral_outbound::DestinationPolicy;
    use coral_secret_store::{
        BuildEnvironment, Error, NamedVariables, SecretReference, SecretStore, SecretStoreFactory,
        SecretStoreKind, SecretStoreKinds, SecretStores, SettingError, StoreSettings, StoreStatus,
    };
    use coral_test_support::InMemorySecretStore;
    use serde::Deserialize;

    use crate::bootstrap::AppError;
    use crate::bootstrap::server_config::tests::load;

    const SECRET: &str = "sk-live-0123456789abcdef";

    /// A kind with one required setting, `path`, and a secret read from
    /// `secret_env`.
    struct TestKind(SecretStoreKind);

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct TestSettings {
        path: String,
        secret_env: String,
    }

    impl SecretStoreFactory for TestKind {
        fn kind(&self) -> &SecretStoreKind {
            &self.0
        }
        fn validate(
            &self,
            settings: &StoreSettings,
            _named: &NamedVariables,
        ) -> Result<(), SettingError> {
            let settings: TestSettings = settings.decode()?;
            if settings.path.is_empty() {
                return Err(SettingError::new("path", "must not be empty"));
            }
            Ok(())
        }
        fn build(
            &self,
            settings: &StoreSettings,
            environment: &BuildEnvironment<'_>,
        ) -> Result<Arc<dyn SecretStore>, SettingError> {
            let settings: TestSettings = settings.decode()?;
            environment.secret("secret_env", &settings.secret_env)?;
            Ok(Arc::new(InMemorySecretStore::new()))
        }
    }

    fn kinds() -> SecretStoreKinds {
        SecretStoreKinds::default().with(Arc::new(TestKind(
            SecretStoreKind::new("test", "Test store").unwrap(),
        )))
    }

    fn store(name: &str) -> String {
        format!(
            "[[secret_stores]]\nname = '{name}'\nkind = 'test'\n\
             path = 'p'\nsecret_env = 'SECRET'\n"
        )
    }

    fn build(raw: &str, secret: Option<&str>) -> Result<SecretStores, AppError> {
        let secret = secret.map(str::to_owned);
        build_with(raw, &kinds(), move |name| {
            secret.clone().filter(|_| name == "SECRET")
        })
    }

    fn build_with(
        raw: &str,
        kinds: &SecretStoreKinds,
        env: impl Fn(&str) -> Option<String> + Send + Sync + 'static,
    ) -> Result<SecretStores, AppError> {
        let (_temp, config) = load(raw);
        config.secret_stores_from(
            kinds,
            DestinationPolicy::default(),
            &(Arc::new(move |name: &str| Ok(env(name))) as coral_secret_store::EnvReader),
        )
    }

    /// The `env` kind is Community: every build has it, beside what the
    /// launching binary registers.
    #[tokio::test]
    async fn every_build_reads_an_env_store() {
        let raw = "[[secret_stores]]\nname = 'env'\nkind = 'env'\nprefix = 'MYCO_'\n";
        let stores = build_with(raw, &SecretStoreKinds::default(), |name| {
            (name == "MYCO_API_KEY").then(|| "sk-live-value".to_owned())
        })
        .unwrap();
        let listed: Vec<_> = stores
            .list()
            .map(|(name, kind, status)| (name.as_str(), kind.name(), kind.label(), status))
            .collect();
        assert_eq!(
            listed,
            [(
                "env",
                "env",
                "Environment variables",
                StoreStatus::Available
            )]
        );
        let reference = SecretReference::new("env".into(), "MYCO_API_KEY".into(), None).unwrap();
        let value = stores.get("env").unwrap().read(&reference).await.unwrap();
        assert_eq!(value.expose(), "sk-live-value");
    }

    /// An env store's prefix stays clear of every variable another setting
    /// names, and the error names that setting, not the variable.
    #[test]
    fn an_env_prefix_must_not_cover_a_variable_config_names() {
        let env_store = "[[secret_stores]]\nname = 'env'\nkind = 'env'\nprefix = 'MYCO_'\n";
        for (raw, setting) in [
            (
                "[credentials]\nkey_env = ' MYCO_KEY '\n".to_owned(),
                "credentials.key_env",
            ),
            (
                "[database]\nbackend = 'postgres'\nurl_env = 'MYCO_DATABASE_URL'\n".to_owned(),
                "database.url_env",
            ),
            (
                "[listeners.run_executor]\ntoken_env = ['OTHER', 'MYCO_TOKEN']\n".to_owned(),
                "listeners.run_executor.token_env",
            ),
            (
                store("vault").replace("'SECRET'", "'MYCO_SECRET'"),
                "secret_stores[0].secret_env",
            ),
        ] {
            let raw = format!("{raw}{env_store}");
            let index = usize::from(raw.contains("name = 'vault'"));
            let Err(AppError::FailedPrecondition(error)) = validate(&raw) else {
                panic!("{raw}: an env prefix over a named variable must fail validation");
            };
            assert_eq!(
                error,
                format!(
                    "server configuration is invalid: secret_stores[{index}] (env).prefix: \
                     covers the variable that {setting} names"
                ),
                "{raw}"
            );
        }
        validate(&format!(
            "[credentials]\nkey_env = 'OTHER_KEY'\n{}{env_store}",
            store("vault")
        ))
        .expect("no named variable is under the prefix");
    }

    fn validate(raw: &str) -> Result<(), AppError> {
        load(raw).1.validate(&kinds())
    }

    #[test]
    fn defined_stores_are_listed_by_name_with_their_kind() {
        let raw = format!("{}{}", store("zeta"), store("alpha-1"));
        let stores = build(&raw, Some("secret")).unwrap();
        let listed: Vec<_> = stores
            .list()
            .map(|(name, kind, status)| (name.as_str().to_owned(), kind.label().to_owned(), status))
            .collect();
        assert_eq!(
            listed,
            ["alpha-1", "zeta"].map(|name| (
                name.to_owned(),
                "Test store".to_owned(),
                StoreStatus::Available
            ))
        );
        assert_eq!(build("", None).unwrap().list().count(), 0);
    }

    /// ADR 0036: a deployment moves between builds without failing to boot.
    #[tokio::test]
    async fn a_store_of_a_kind_the_build_lacks_is_defined_and_never_read() {
        let raw = "[[secret_stores]]\nname = 'vault'\nkind = 'elsewhere'\n\
                   address = 'https://vault.example.test'\nanything = { nested = 1 }\n";
        validate(raw).expect("a missing kind's settings are its own business");
        let stores = build(raw, None).unwrap();
        let listed: Vec<_> = stores
            .list()
            .map(|(name, kind, status)| (name.as_str(), kind.name(), kind.label(), status))
            .collect();
        assert_eq!(
            listed,
            [("vault", "elsewhere", "elsewhere", StoreStatus::KindMissing)]
        );
        let reference = SecretReference::new("vault".into(), "p".into(), None).unwrap();
        assert_eq!(
            stores
                .get("vault")
                .unwrap()
                .read(&reference)
                .await
                .unwrap_err(),
            Error::KindMissing
        );
    }

    #[test]
    fn a_malformed_store_fails_validation_naming_the_store_and_field() {
        let good = store("team");
        for (raw, expected) in [
            (
                good.replace("kind = 'test'", "kind = 'Not A Kind'"),
                "secret_stores[0] (team).kind: must be",
            ),
            (
                good.replace("kind = 'test'", "kind = 7"),
                "secret_stores[0] (team).kind: unsupported value",
            ),
            (
                good.replace("kind = 'test'\n", ""),
                "secret_stores[0] (team): missing field `kind`",
            ),
            (
                good.replace("name = 'team'\n", ""),
                "secret_stores[0]: missing field `name`",
            ),
            (
                good.replace("name = 'team'", "name = 'Not Valid'"),
                "secret_stores[0].name: must be",
            ),
            (
                good.replace("path = 'p'", "path = ''"),
                "secret_stores[0] (team).path: must not be empty",
            ),
            (
                good.replace("path = 'p'\n", ""),
                "secret_stores[0] (team): missing field `path`",
            ),
            (
                format!("{good}token = 'x'\n"),
                "secret_stores[0] (team).token: unknown field `token`",
            ),
            (
                format!("{good}{}", store("team")),
                "secret_stores[1] (team).name: another store has this name",
            ),
        ] {
            let error = validate(&raw).expect_err(&raw).to_string();
            assert!(
                error.contains("server configuration is invalid: ") && error.contains(expected),
                "{raw}: {error}"
            );
        }
    }

    /// Validation reads no environment, so deploy checks work without secrets,
    /// but a store cannot start without its secret.
    #[test]
    fn a_missing_secret_names_the_store_and_field_only_when_built() {
        let raw = store("team");
        validate(&raw).expect("validation needs no secret");
        let Err(error) = build(&raw, None) else {
            panic!("a store needs its secret");
        };
        assert!(
            error
                .to_string()
                .contains("secret_stores[0] (team).secret_env: environment variable is not set"),
            "{error}"
        );
    }

    /// Operators paste secrets under the wrong key.
    #[test]
    fn a_rejected_value_is_never_repeated() {
        let good = store("team");
        for raw in [
            good.replace("name = 'team'", &format!("name = '{SECRET}!'")),
            good.replace("kind = 'test'", &format!("kind = '{SECRET}!'")),
            good.replace("path = 'p'", "path = 7"),
            good.replace("path = 'p'", &format!("path = ['{SECRET}']")),
        ] {
            let error = validate(&raw).expect_err(&raw).to_string();
            assert!(!error.contains(SECRET), "{raw}: {error}");
        }
    }
}
