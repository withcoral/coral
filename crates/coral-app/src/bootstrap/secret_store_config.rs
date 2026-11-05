//! The `[[secret_stores]]` section of deployment config.
//!
//! Each entry decodes on its own, so a failure can name the store as well as
//! the field. A name is only repeated once it has passed validation, because
//! an operator can paste a secret under the wrong key.

use std::sync::Arc;

use coral_secret_store::kv_v2::{AppRole, KvV2Settings, KvV2Store, MountPath, StoreAddress};
use coral_secret_store::{Secret, SecretStore, SecretStoreKind, SecretStoreName, SecretStores};
use serde::Deserialize;

use super::AppError;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStore {
    name: String,
    kind: RawKind,
    address: String,
    mount: String,
    auth: RawAuth,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RawKind {
    KvV2,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAuth {
    method: RawAuthMethod,
    /// Where the auth method is mounted; `approle` by default.
    mount: Option<String>,
    role_id: String,
    secret_id_env: String,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RawAuthMethod {
    Approle,
}

/// One store's validated settings. Holds no secret: its secret ID is read only
/// when the store is built.
struct Definition {
    /// Where a failure in this store's settings says it is.
    label: String,
    name: SecretStoreName,
    kind: SecretStoreKind,
    address: StoreAddress,
    mount: MountPath,
    auth_mount: MountPath,
    role_id: String,
    secret_id_env: String,
}

fn invalid(message: impl std::fmt::Display) -> AppError {
    AppError::FailedPrecondition(format!("server configuration is invalid: {message}"))
}

fn definition(index: usize, raw: &toml::Value) -> Result<Definition, AppError> {
    // Repeat a name only once it is known not to be a misplaced secret.
    let label = raw
        .get("name")
        .and_then(toml::Value::as_str)
        .and_then(|name| SecretStoreName::parse(name).ok())
        .map_or_else(
            || format!("secret_stores[{index}]"),
            |name| format!("secret_stores[{index}] ({})", name.as_str()),
        );
    let store: RawStore = crate::config_toml::decode_entry(&label, raw.clone()).map_err(invalid)?;
    let name = SecretStoreName::parse(&store.name)
        .map_err(|error| invalid(format!("{label}.name: {error}")))?;
    let address = StoreAddress::parse(&store.address)
        .map_err(|error| invalid(format!("{label}.address: {error}")))?;
    let mount = MountPath::parse(&store.mount)
        .map_err(|error| invalid(format!("{label}.mount: {error}")))?;
    let auth_mount = MountPath::parse(store.auth.mount.as_deref().unwrap_or("approle"))
        .map_err(|error| invalid(format!("{label}.auth.mount: {error}")))?;
    let role_id = store.auth.role_id.trim();
    if role_id.is_empty() || role_id.chars().any(char::is_control) {
        return Err(invalid(format!(
            "{label}.auth.role_id: must not be empty or hold control characters"
        )));
    }
    let secret_id_env = super::env_var_name(
        &format!("{label}.auth.secret_id_env"),
        &store.auth.secret_id_env,
    )
    .map_err(invalid)?;
    let RawKind::KvV2 = store.kind;
    let RawAuthMethod::Approle = store.auth.method;
    Ok(Definition {
        name,
        kind: SecretStoreKind::KvV2,
        address,
        mount,
        auth_mount,
        role_id: role_id.to_owned(),
        secret_id_env,
        label,
    })
}

fn definitions(raw: &[toml::Value]) -> Result<Vec<Definition>, AppError> {
    let mut stores: Vec<Definition> = Vec::with_capacity(raw.len());
    for (index, entry) in raw.iter().enumerate() {
        let store = definition(index, entry)?;
        if stores.iter().any(|other| other.name == store.name) {
            return Err(invalid(format!(
                "{}.name: another store has this name",
                store.label
            )));
        }
        stores.push(store);
    }
    Ok(stores)
}

/// Checks each store's settings without reading its secret ID.
pub(super) fn validate(raw: &[toml::Value]) -> Result<(), AppError> {
    definitions(raw).map(drop)
}

/// Builds the adapters. No request leaves here, so an unreachable store does
/// not stop Coral from starting; a missing secret ID does.
pub(super) fn build(
    raw: &[toml::Value],
    policy: coral_outbound::DestinationPolicy,
    env: &dyn Fn(&str) -> Result<Option<String>, std::env::VarError>,
) -> Result<SecretStores, AppError> {
    let definitions = definitions(raw)?;
    if definitions.is_empty() {
        return Ok(SecretStores::default());
    }
    let client = coral_outbound::Client::new(policy, coral_secret_store::kv_v2::REQUEST_TIMEOUT)
        .map_err(|_error| {
            AppError::FailedPrecondition("secret store client initialization failed".into())
        })?;
    let mut stores = Vec::with_capacity(definitions.len());
    for store in definitions {
        let secret_id = env(&store.secret_id_env)
            .ok()
            .flatten()
            .ok_or_else(|| {
                invalid(format!(
                    "{}.auth.secret_id_env: environment variable `{}` is not set",
                    store.label, store.secret_id_env
                ))
            })
            .and_then(|value| {
                Secret::new(value).map_err(|_error| {
                    invalid(format!(
                        "{}.auth.secret_id_env: environment variable `{}` is not a usable secret",
                        store.label, store.secret_id_env
                    ))
                })
            })?;
        let adapter: Arc<dyn SecretStore> = Arc::new(KvV2Store::new(
            client.clone(),
            KvV2Settings {
                address: store.address,
                mount: store.mount,
                auth: AppRole {
                    mount: store.auth_mount,
                    role_id: store.role_id,
                    secret_id,
                },
            },
        ));
        stores.push((store.name, store.kind, adapter));
    }
    Ok(SecretStores::new(stores))
}

#[cfg(test)]
mod tests {
    use coral_outbound::DestinationPolicy;
    use coral_secret_store::{SecretStoreKind, SecretStores};

    use crate::bootstrap::AppError;
    use crate::bootstrap::server_config::tests::load;

    const SECRET: &str = "sk-live-0123456789abcdef";

    fn store(name: &str) -> String {
        format!(
            "[[secret_stores]]\n\
             name = '{name}'\nkind = 'kv-v2'\n\
             address = 'https://store.example.test:8200'\nmount = 'secret'\n\
             auth = {{ method = 'approle', role_id = 'role', secret_id_env = 'SECRET_ID' }}\n"
        )
    }

    fn build(raw: &str, secret_id: Option<&str>) -> Result<SecretStores, AppError> {
        let (_temp, config) = load(raw);
        config.secret_stores_from(DestinationPolicy::default(), &|name| {
            Ok(secret_id.filter(|_| name == "SECRET_ID").map(str::to_owned))
        })
    }

    #[test]
    fn defined_stores_are_listed_by_name_and_kind() {
        let raw = format!("{}{}", store("zeta"), store("alpha-1"));
        let stores = build(&raw, Some("secret-id")).unwrap();
        let mut listed: Vec<_> = stores
            .list()
            .map(|(name, kind)| (name.as_str().to_owned(), kind))
            .collect();
        listed.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            listed,
            [
                ("alpha-1".to_owned(), SecretStoreKind::KvV2),
                ("zeta".to_owned(), SecretStoreKind::KvV2),
            ]
        );
        assert_eq!(build("", None).unwrap().list().count(), 0);
    }

    #[test]
    fn a_malformed_store_fails_validation_naming_the_store_and_field() {
        let good = store("team");
        let without = |line: &str| good.replace(line, "");
        for (raw, expected) in [
            (
                good.replace("kind = 'kv-v2'", "kind = 'kv-v1'"),
                "secret_stores[0] (team).kind: unsupported value",
            ),
            (
                good.replace("https://store.example.test:8200", "ftp://store.internal"),
                "secret_stores[0] (team).address: must be",
            ),
            (
                good.replace("mount = 'secret'", "mount = 'a/../b'"),
                "secret_stores[0] (team).mount: must be",
            ),
            (
                without("mount = 'secret'\n"),
                "secret_stores[0] (team): missing field `mount`",
            ),
            (
                good.replace("method = 'approle'", "method = 'token'"),
                "secret_stores[0] (team).auth.method: unsupported value",
            ),
            (
                good.replace("role_id = 'role'", "role_id = ' '"),
                "secret_stores[0] (team).auth.role_id: must not be empty",
            ),
            (
                good.replace("'SECRET_ID'", "' '"),
                "secret_stores[0] (team).auth.secret_id_env must not be empty",
            ),
            (
                good.replace("secret_id_env = 'SECRET_ID'", "secret_id = 'x'"),
                "secret_stores[0] (team).auth.secret_id: unknown field `secret_id`",
            ),
            (
                format!("{good}token = 'x'\n"),
                "secret_stores[0] (team).token: unknown field `token`",
            ),
            (
                good.replace("name = 'team'", "name = 'Not Valid'"),
                "secret_stores[0].name: must be",
            ),
            (
                format!("{good}{}", store("team")),
                "secret_stores[1] (team).name: another store has this name",
            ),
        ] {
            let error = load(&raw).1.validate().expect_err(&raw).to_string();
            assert!(
                error.contains("server configuration is invalid: ") && error.contains(expected),
                "{raw}: {error}"
            );
        }
    }

    /// A Docker Compose store is plain HTTP on a private name. Whether Coral may
    /// reach it is the `[outbound]` policy's call, not a reason to refuse to start.
    #[test]
    fn a_plain_http_address_is_left_to_the_outbound_policy() {
        let raw = store("team").replace("https://store.example.test:8200", "http://vault:8200");
        load(&raw).1.validate().expect("valid settings");
    }

    /// Validation reads no environment, so deploy checks work without secrets,
    /// but a store cannot start without its secret ID.
    #[test]
    fn a_missing_secret_id_names_the_store_and_field_only_when_built() {
        let raw = store("team");
        load(&raw).1.validate().expect("validation needs no secret");
        for secret_id in [None, Some("a\nb")] {
            let Err(error) = build(&raw, secret_id) else {
                panic!("a store needs a usable secret ID");
            };
            let error = error.to_string();
            assert!(
                error.contains("secret_stores[0] (team).auth.secret_id_env")
                    && error.contains("`SECRET_ID`"),
                "{error}"
            );
            assert!(!error.contains("a\nb"), "{error}");
        }
    }

    /// Operators paste secrets under the wrong key.
    #[test]
    fn a_rejected_value_is_never_repeated() {
        let good = store("team");
        for raw in [
            good.replace("name = 'team'", &format!("name = '{SECRET}!'")),
            good.replace("kind = 'kv-v2'", &format!("kind = '{SECRET}'")),
            good.replace("https://store.example.test:8200", SECRET),
            good.replace("mount = 'secret'", &format!("mount = '../{SECRET}'")),
            good.replace("method = 'approle'", &format!("method = '{SECRET}'")),
            good.replace("role_id = 'role'", "role_id = 7"),
        ] {
            let error = load(&raw).1.validate().expect_err(&raw).to_string();
            assert!(!error.contains(SECRET), "{raw}: {error}");
        }
    }

    /// Nothing is sent at boot, so Coral starts while a store is down.
    #[tokio::test]
    async fn an_unreachable_store_does_not_stop_coral_from_starting() {
        let temp = tempfile::TempDir::new().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let closed = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        // Any variable the test process already has can stand in for the
        // secret ID, so the test changes no process-wide state.
        let store = store("team")
            .replace("https://store.example.test:8200", &closed)
            .replace("SECRET_ID", "PATH");
        std::fs::write(
            temp.path().join("config.toml"),
            format!("{store}\n[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n"),
        )
        .unwrap();
        let server = crate::ServerBuilder::new()
            .with_config_dir(temp.path())
            .start()
            .await
            .unwrap();
        server.shutdown().await.unwrap();
    }
}
