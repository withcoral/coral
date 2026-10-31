//! The rules `updateProvider`'s arguments meet beyond their schema, the
//! configuration a call confirms, and the arguments its conformance test
//! starts from.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use super::UpdateProviderArguments;
use crate::builtin::add_provider::tests::{LITERAL_REASON, NAMESPACE_REASON, SOURCE_URL_REASON};
use crate::{
    AuthenticationConfiguration, CredentialField, OAuthClientAuthenticationMethod,
    OAuthClientBootstrap, OAuthClientPolicy, OAuthPkcePolicy, OAuthScopeFormat,
    ProviderConfiguration, ServerSelection,
};

/// Arguments that use every part of the input schema, with values that meet
/// the rules a schema cannot state. Each sends two fields beside the
/// namespace, so a call that loses one still changes something.
pub(in crate::builtin) fn valid_arguments() -> Vec<Value> {
    vec![
        json!({
            "namespace": "weather",
            "source": { "kind": "openapi_document", "document": "openapi: 3.1.0" },
            "credentialRoute": "fixed",
            "displayName": "Weather",
            "servers": {
                "#/servers": {
                    "kind": "declared",
                    "url": "https://{region}.weather.example",
                    "variables": { "region": "eu" },
                },
            },
            "authentication": {
                "method": "header_credentials",
                "headers": {
                    "X-Weather-Auth": { "source": "field", "field": "token", "prefix": "Bot " },
                },
            },
            "requestHeaders": { "X-Trace": "on" },
            "credentialFields": {
                "token": { "label": "Bot token", "description": "From settings.", "sensitive": true },
                "region": null,
            },
        }),
        json!({ "namespace": "weather", "refresh": true, "displayName": "Weather" }),
        json!({
            "namespace": "weather",
            "source": { "kind": "mcp_url", "url": "https://weather.example/mcp" },
            "servers": { "#/servers": null },
            "requestHeaders": { "X-Trace": null },
        }),
        json!({
            "namespace": "weather",
            "source": { "kind": "openapi_url", "url": "https://weather.example/openapi.json" },
            "credentialRoute": "none",
        }),
    ]
}

// Catches: a call that changes nothing, or that asks for two sources at once,
// reaching admission; a reason that fails to name the argument; or a shared
// rule of a Provider's input that `updateProvider` skips. `addProvider`'s
// tests own those rules.
#[test]
fn each_rejected_argument_names_what_to_correct() {
    let source = json!({ "kind": "openapi_url", "url": "https://weather.example/openapi.json" });
    for (arguments, expected) in [
        (
            json!({ "namespace": "weather" }),
            vec![("", "send at least one field to change beside `namespace`")],
        ),
        (
            json!({ "namespace": "weather", "refresh": true, "source": source }),
            vec![(
                "refresh",
                "send `refresh` or `source`, not both: `source` replaces the saved source",
            )],
        ),
        (
            json!({ "namespace": "weather", "refresh": false }),
            vec![("refresh", "expected `true`")],
        ),
        (
            // Every problem Coral can find without the Provider, at once.
            json!({
                "namespace": "not-an-identifier",
                "refresh": true,
                "source": { "kind": "mcp_url", "url": "ftp://weather.example" },
                "authentication": {
                    "method": "basic_auth",
                    "username": { "source": "field", "field": "username" },
                    "password": { "source": "literal", "value": "secret" },
                },
            }),
            vec![
                ("namespace", NAMESPACE_REASON),
                ("authentication.password", LITERAL_REASON),
                ("source.url", SOURCE_URL_REASON),
                (
                    "refresh",
                    "send `refresh` or `source`, not both: `source` replaces the saved source",
                ),
            ],
        ),
    ] {
        let problems = UpdateProviderArguments::parse(&arguments)
            .expect_err(&format!("{arguments} is rejected"));
        let found = problems
            .iter()
            .map(|problem| (problem.field(), problem.reason()))
            .collect::<Vec<_>>();
        assert_eq!(found, expected, "{arguments}");
    }
}

/// A saved OpenAPI Provider Configuration with an OAuth application and two
/// of every map entry.
fn saved() -> ProviderConfiguration {
    ProviderConfiguration {
        display_name: "Weather".to_owned(),
        servers: BTreeMap::from([
            (
                "#/servers".to_owned(),
                ServerSelection::Custom {
                    url: "https://weather.example".to_owned(),
                },
            ),
            (
                "#/paths/~1forecast/servers".to_owned(),
                ServerSelection::Custom {
                    url: "https://forecast.weather.example".to_owned(),
                },
            ),
        ]),
        authentication: Some(
            oauth("https://weather.example", &["read"]).with_client_policy(Some(client("saved"))),
        ),
        request_headers: BTreeMap::from([
            ("A".to_owned(), "1".to_owned()),
            ("B".to_owned(), "2".to_owned()),
        ]),
        credential_fields: BTreeMap::from([
            ("token".to_owned(), field("Token")),
            ("region".to_owned(), field("Region")),
        ]),
    }
}

fn field(label: &str) -> CredentialField {
    CredentialField {
        label: label.to_owned(),
        description: String::new(),
        sensitive: true,
    }
}

fn oauth(authorization_url: &str, scopes: &[&str]) -> AuthenticationConfiguration {
    AuthenticationConfiguration::OAuthAuthorizationCode {
        authorization_url: authorization_url.to_owned(),
        token_url: format!("{authorization_url}/token"),
        scopes: scopes.iter().map(|scope| (*scope).to_owned()).collect(),
        scope_format: OAuthScopeFormat::Space,
        resource: Some(format!("{authorization_url}/resource")),
        issuer: Some(authorization_url.to_owned()),
        client: None,
    }
}

fn client(client_id: &str) -> OAuthClientPolicy {
    OAuthClientPolicy {
        bootstrap: OAuthClientBootstrap::Manual,
        client_id: client_id.to_owned(),
        authentication: OAuthClientAuthenticationMethod::None,
        pkce: OAuthPkcePolicy::S256,
        authorization_parameters: BTreeMap::new(),
    }
}

fn arguments(changes: Value) -> UpdateProviderArguments {
    let mut arguments = json!({ "namespace": "weather" });
    if let (Some(arguments), Value::Object(changes)) = (arguments.as_object_mut(), changes) {
        arguments.extend(changes);
    }
    UpdateProviderArguments::parse(&arguments).expect("valid arguments")
}

// Catches: an omitted field reset to the source's suggestion, a sent map that
// drops a saved entry it does not name, a null that keeps its entry, a sent
// value that does not replace its saved value, or an OAuth method sent
// without an application that drops the saved one, though confirmation
// keeps it.
#[test]
fn each_sent_field_replaces_its_saved_value_and_omitted_fields_stay() {
    let sent_oauth = |client: Option<Value>| {
        let mut authentication = json!({
            "method": "oauth_authorization_code",
            "authorization_url": "https://weather.example/authorize",
            "token_url": "https://weather.example/token",
            "scopes": ["write"],
        });
        if let (Some(fields), Some(client)) = (authentication.as_object_mut(), client) {
            fields.insert("client".to_owned(), client);
        }
        json!({ "authentication": authentication })
    };
    let confirmed_oauth = |client_id: &str| ProviderConfiguration {
        authentication: Some(AuthenticationConfiguration::OAuthAuthorizationCode {
            authorization_url: "https://weather.example/authorize".to_owned(),
            token_url: "https://weather.example/token".to_owned(),
            scopes: vec!["write".to_owned()],
            scope_format: OAuthScopeFormat::Space,
            resource: None,
            issuer: None,
            client: Some(client(client_id)),
        }),
        ..saved()
    };
    for (changes, expected) in [
        (
            json!({ "displayName": "Forecasts" }),
            ProviderConfiguration {
                display_name: "Forecasts".to_owned(),
                ..saved()
            },
        ),
        (
            json!({ "requestHeaders": { "A": "3", "B": null, "C": "4" } }),
            ProviderConfiguration {
                request_headers: BTreeMap::from([
                    ("A".to_owned(), "3".to_owned()),
                    ("C".to_owned(), "4".to_owned()),
                ]),
                ..saved()
            },
        ),
        (
            json!({ "servers": {
                "#/servers": { "kind": "custom", "url": "https://eu.weather.example" },
                "#/paths/~1forecast/servers": null,
            } }),
            ProviderConfiguration {
                servers: BTreeMap::from([(
                    "#/servers".to_owned(),
                    ServerSelection::Custom {
                        url: "https://eu.weather.example".to_owned(),
                    },
                )]),
                ..saved()
            },
        ),
        (
            json!({ "credentialFields": { "token": { "label": "Key", "sensitive": false } } }),
            ProviderConfiguration {
                credential_fields: BTreeMap::from([
                    (
                        "token".to_owned(),
                        CredentialField {
                            label: "Key".to_owned(),
                            description: String::new(),
                            sensitive: false,
                        },
                    ),
                    ("region".to_owned(), field("Region")),
                ]),
                ..saved()
            },
        ),
        (
            json!({ "authentication": { "method": "none" } }),
            ProviderConfiguration {
                authentication: Some(AuthenticationConfiguration::None),
                ..saved()
            },
        ),
        (sent_oauth(None), confirmed_oauth("saved")),
        (
            sent_oauth(Some(json!({ "client_id": "sent" }))),
            confirmed_oauth("sent"),
        ),
        // A route or a source changes no configuration field.
        (json!({ "credentialRoute": "caller" }), saved()),
        (json!({ "refresh": true }), saved()),
    ] {
        assert_eq!(
            arguments(changes.clone())
                .choices
                .configuration(saved(), None),
            expected,
            "{changes}"
        );
    }
}

// Catches: an MCP update that keeps a saved server or OAuth endpoint that the
// server no longer reports, so a new MCP URL fails, or one that drops the
// saved scopes and application with them.
#[test]
fn an_mcp_server_sets_the_omitted_server_and_oauth_endpoints() {
    let client = client("weather-app");
    let saved = ProviderConfiguration {
        servers: BTreeMap::from([(
            "#/servers".to_owned(),
            ServerSelection::Custom {
                url: "https://old.weather.example/mcp".to_owned(),
            },
        )]),
        authentication: Some(
            oauth("https://old.weather.example", &["read"])
                .with_client_policy(Some(client.clone())),
        ),
        ..saved()
    };
    let discovered = ProviderConfiguration {
        display_name: "Weather MCP".to_owned(),
        servers: BTreeMap::from([(
            "#/servers".to_owned(),
            ServerSelection::Custom {
                url: "https://new.weather.example/mcp".to_owned(),
            },
        )]),
        authentication: Some(oauth("https://new.weather.example", &[])),
        request_headers: BTreeMap::new(),
        credential_fields: BTreeMap::new(),
    };
    let followed = ProviderConfiguration {
        servers: discovered.servers.clone(),
        authentication: Some(
            oauth("https://new.weather.example", &["read"]).with_client_policy(Some(client)),
        ),
        ..saved.clone()
    };
    for (changes, expected) in [
        (json!({ "displayName": "Weather" }), followed.clone()),
        (
            // A sent value is the Program's, even for an MCP source.
            json!({
                "servers": { "#/servers": { "kind": "custom", "url": "https://other.weather.example/mcp" } },
                "authentication": { "method": "bearer" },
            }),
            ProviderConfiguration {
                servers: BTreeMap::from([(
                    "#/servers".to_owned(),
                    ServerSelection::Custom {
                        url: "https://other.weather.example/mcp".to_owned(),
                    },
                )]),
                authentication: Some(AuthenticationConfiguration::Bearer),
                ..followed.clone()
            },
        ),
    ] {
        assert_eq!(
            arguments(changes.clone())
                .choices
                .configuration(saved.clone(), Some(&discovered)),
            expected,
            "{changes}"
        );
    }
    // A saved method the server no longer offers stays for validation to report.
    let bearer = ProviderConfiguration {
        authentication: Some(AuthenticationConfiguration::Bearer),
        ..saved
    };
    assert_eq!(
        arguments(json!({ "displayName": "Weather" }))
            .choices
            .configuration(bearer.clone(), Some(&discovered))
            .authentication,
        bearer.authentication
    );
}
