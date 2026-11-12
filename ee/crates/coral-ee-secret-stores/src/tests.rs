// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use coral_edition::{Edition, EditionCheck, EditionSource};
use coral_outbound::{DestinationPolicy, NetworkAccess};
use coral_secret_store::{
    BuildEnvironment, Error, KeyField, NamedVariables, ReferenceForm, SecretReference, SecretStore,
    SecretStoreFactory as _, SettingError, StoreSettings, StoreStatus,
};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::KvV2Kind;

const SECRET: &str = "sk-live-0123456789abcdef";

fn settings(raw: &str) -> StoreSettings {
    StoreSettings::new(toml::from_str(raw).unwrap())
}

fn validate(kind: &KvV2Kind, raw: &str) -> Result<(), SettingError> {
    kind.validate(&settings(raw), &NamedVariables::default())
}

fn good(address: &str) -> String {
    format!(
        "address = '{address}'\nmount = 'secret'\n\
         auth = {{ method = 'approle', role_id = 'role', secret_id_env = 'SECRET_ID' }}\n"
    )
}

fn kind(edition: Edition) -> KvV2Kind {
    KvV2Kind::new(EditionCheck::new(Arc::new(edition)))
}

fn build(
    kind: &KvV2Kind,
    raw: &str,
    secret_id: Option<&str>,
) -> Result<Arc<dyn SecretStore>, SettingError> {
    let env = |name: &str| Ok(secret_id.filter(|_| name == "SECRET_ID").map(str::to_owned));
    kind.build(
        &settings(raw),
        &BuildEnvironment {
            policy: DestinationPolicy::new(NetworkAccess::HttpAndHttps, NetworkAccess::Deny),
            env: &env,
        },
    )
}

#[test]
fn malformed_settings_name_the_field() {
    let good = good("https://store.example.test:8200");
    let without = |line: &str| good.replace(line, "");
    let kind = kind(Edition::Enterprise);
    for (raw, expected) in [
        (
            good.replace("https://store.example.test:8200", "ftp://store.internal"),
            "address: must be",
        ),
        (
            good.replace("mount = 'secret'", "mount = 'a/../b'"),
            "mount: must be",
        ),
        (without("mount = 'secret'\n"), "missing field `mount`"),
        (
            good.replace("method = 'approle'", "method = 'token'"),
            "auth.method: unsupported value",
        ),
        (
            good.replace("role_id = 'role'", "role_id = ' '"),
            "auth.role_id: must not be empty",
        ),
        (
            good.replace("'SECRET_ID'", "' '"),
            "auth.secret_id_env: must not be empty",
        ),
        (
            good.replace("secret_id_env = 'SECRET_ID'", "secret_id = 'x'"),
            "auth.secret_id: unknown field `secret_id`",
        ),
        (
            format!("{good}token = 'x'\n"),
            "token: unknown field `token`",
        ),
    ] {
        let error = validate(&kind, &raw).expect_err(&raw).to_string();
        assert!(error.contains(expected), "{raw}: {error}");
    }
}

/// A Docker Compose store is plain HTTP on a private name. Whether Coral may
/// reach it is the `[outbound]` policy's call, not a reason to refuse to start.
#[test]
fn a_plain_http_address_is_left_to_the_outbound_policy() {
    validate(&kind(Edition::Enterprise), &good("http://vault:8200")).expect("valid settings");
}

/// Validation reads no environment, so deploy checks work without secrets,
/// but a store cannot be built without its secret ID.
#[test]
fn a_missing_secret_id_fails_only_when_built() {
    let raw = good("https://store.example.test:8200");
    let kind = kind(Edition::Enterprise);
    validate(&kind, &raw).expect("validation needs no secret");
    for secret_id in [None, Some("a\nb")] {
        let Err(error) = build(&kind, &raw, secret_id) else {
            panic!("a store needs a usable secret ID");
        };
        assert_eq!(error.field(), "auth.secret_id_env");
        assert!(!error.to_string().contains("a\nb"), "{error}");
    }
}

/// Operators paste secrets under the wrong key.
#[test]
fn a_rejected_value_is_never_repeated() {
    let good = good("https://store.example.test:8200");
    let kind = kind(Edition::Enterprise);
    for raw in [
        good.replace("https://store.example.test:8200", SECRET),
        good.replace("mount = 'secret'", &format!("mount = '../{SECRET}'")),
        good.replace("method = 'approle'", &format!("method = '{SECRET}'")),
        good.replace("role_id = 'role'", "role_id = 7"),
    ] {
        let error = validate(&kind, &raw).expect_err(&raw).to_string();
        assert!(!error.contains(SECRET), "{raw}: {error}");
    }
    let pasted = good.replace("'SECRET_ID'", &format!("'{SECRET}'"));
    let Err(error) = build(&kind, &pasted, None) else {
        panic!("the variable is not set");
    };
    assert!(!error.to_string().contains(SECRET), "{error}");
}

/// An entry holds named keys, so a reference holds a path and a key, whatever
/// the Edition: the form stays the same while reads are refused.
#[test]
fn a_reference_holds_a_path_and_a_key() {
    let expected = ReferenceForm {
        path_label: "Path".into(),
        path_prefix: None,
        key: Some(KeyField {
            label: "Key".into(),
            required: true,
        }),
    };
    for edition in [Edition::Community, Edition::Enterprise] {
        let store = build(
            &kind(edition),
            &good("https://store.example.test:8200"),
            Some("id"),
        )
        .expect("a store");
        assert_eq!(store.reference_form(), expected, "{edition:?}");
    }
}

/// Nothing is sent when a store is built, so Coral starts while it is down.
#[test]
fn building_a_store_sends_nothing() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let closed = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    build(
        &kind(Edition::Enterprise),
        &good(&closed),
        Some("secret-id"),
    )
    .unwrap();
}

/// An Edition a test changes while the store runs, as a licence can.
struct Switch(AtomicBool);

impl EditionSource for Switch {
    fn edition(&self) -> Edition {
        if self.0.load(Ordering::SeqCst) {
            Edition::Enterprise
        } else {
            Edition::Community
        }
    }
}

/// ADR 0033: an Enterprise Feature refuses while the Edition is Community,
/// checks the Edition on every use, and deletes nothing.
#[tokio::test]
async fn a_store_reads_only_while_the_edition_is_enterprise() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/auth/approle/login"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "auth": { "client_token": "token", "lease_duration": 3600, "renewable": true }
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/secret/data/team/jira"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": { "data": { "token": "value" }, "metadata": { "version": 1 } }
        })))
        .mount(&server)
        .await;
    let switch = Arc::new(Switch(AtomicBool::new(false)));
    let kind = KvV2Kind::new(EditionCheck::new(Arc::clone(&switch) as _));
    let store = build(&kind, &good(&server.uri()), Some("secret-id")).unwrap();
    let reference =
        SecretReference::new("store".into(), "team/jira".into(), Some("token".into())).unwrap();

    assert_eq!(store.status(), StoreStatus::NeedsEnterprise);
    assert_eq!(
        store.read(&reference).await.unwrap_err(),
        Error::NeedsEnterprise
    );
    assert!(
        server.received_requests().await.unwrap().is_empty(),
        "a refused read sends nothing"
    );

    switch.0.store(true, Ordering::SeqCst);
    assert_eq!(store.status(), StoreStatus::Available);
    assert_eq!(store.read(&reference).await.unwrap().expose(), "value");

    switch.0.store(false, Ordering::SeqCst);
    assert_eq!(store.status(), StoreStatus::NeedsEnterprise);
    assert_eq!(
        store.read(&reference).await.unwrap_err(),
        Error::NeedsEnterprise
    );
}
