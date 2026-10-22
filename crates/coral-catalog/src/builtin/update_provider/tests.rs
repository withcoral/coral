//! The rules `updateProvider`'s arguments meet beyond their schema, the
//! configuration a call confirms, and the arguments its conformance test
//! starts from.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use super::{
    EntryChange, EntryStatus, FieldChange, ProviderChanges, SavedProvider, SourceRead,
    UpdateProviderArguments,
};
use crate::builtin::create_provider::tests::{LITERAL_REASON, NAMESPACE_REASON, SOURCE_URL_REASON};
use crate::builtin::{BuiltinCredentialRoute, ProviderSourceOutput};
use crate::{
    AuthenticationConfiguration, CredentialField, HeaderBinding, OAuthClientAuthenticationMethod,
    OAuthClientBootstrap, OAuthClientPolicy, OAuthPkcePolicy, OAuthScopeFormat,
    ProviderConfiguration, ServerSelection,
};

/// A value that must never reach a reason.
const CANARY: &str = "sk-canary";

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
            },
        }),
        json!({ "namespace": "weather", "refresh": true, "displayName": "Weather" }),
        json!({
            "namespace": "weather",
            "source": { "kind": "mcp_url", "url": "https://weather.example/mcp" },
            "requestHeaders": {},
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
// rule of a Provider's input that `updateProvider` skips. `createProvider`'s
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
            json!({ "displayName": "Weather" }),
            vec![("", "missing field `namespace`")],
        ),
        (
            json!({ "namespace": "weather", "displayname": "Weather" }),
            vec![(
                "displayname",
                "unknown field `displayname`, expected one of `namespace`, `source`, `refresh`, \
                 `credentialRoute`, `displayName`, `servers`, `authentication`, \
                 `requestHeaders`, `credentialFields`",
            )],
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

/// A saved OpenAPI Provider Configuration with two of every map entry.
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
        authentication: Some(AuthenticationConfiguration::Bearer),
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

fn arguments(changes: Value) -> UpdateProviderArguments {
    let mut arguments = json!({ "namespace": "weather" });
    if let (Some(arguments), Value::Object(changes)) = (arguments.as_object_mut(), changes) {
        arguments.extend(changes);
    }
    UpdateProviderArguments::parse(&arguments).expect("valid arguments")
}

// Catches: an omitted field reset to the source's suggestion, a sent map
// merged into the saved one so a removed entry stays, or a sent field that
// does not replace its saved value.
#[test]
fn each_sent_field_replaces_its_saved_value_and_omitted_fields_stay() {
    for (changes, expected) in [
        (
            json!({ "displayName": "Forecasts" }),
            ProviderConfiguration {
                display_name: "Forecasts".to_owned(),
                ..saved()
            },
        ),
        (
            json!({ "requestHeaders": { "A": "3" } }),
            ProviderConfiguration {
                request_headers: BTreeMap::from([("A".to_owned(), "3".to_owned())]),
                ..saved()
            },
        ),
        (
            json!({ "requestHeaders": {} }),
            ProviderConfiguration {
                request_headers: BTreeMap::new(),
                ..saved()
            },
        ),
        (
            json!({ "servers": { "#/servers": { "kind": "custom", "url": "https://eu.weather.example" } } }),
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
                credential_fields: BTreeMap::from([(
                    "token".to_owned(),
                    CredentialField {
                        label: "Key".to_owned(),
                        description: String::new(),
                        sensitive: false,
                    },
                )]),
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
        // A route or a source changes no configuration field.
        (json!({ "credentialRoute": "caller" }), saved()),
        (json!({ "refresh": true }), saved()),
    ] {
        assert_eq!(
            arguments(changes.clone()).configuration(saved(), None),
            expected,
            "{changes}"
        );
    }
}

// Catches: an OAuth method sent without an application that drops the saved
// one, though confirmation keeps it, so the review shows a removal that
// never happens.
#[test]
fn an_oauth_method_sent_without_an_application_keeps_the_saved_one() {
    let client = |client_id: &str| OAuthClientPolicy {
        bootstrap: OAuthClientBootstrap::Manual,
        client_id: client_id.to_owned(),
        authentication: OAuthClientAuthenticationMethod::None,
        pkce: OAuthPkcePolicy::S256,
        authorization_parameters: BTreeMap::new(),
    };
    let saved = ProviderConfiguration {
        authentication: Some(
            oauth("https://weather.example", &["read"]).with_client_policy(Some(client("saved"))),
        ),
        ..saved()
    };
    let method = |client: Option<Value>| {
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
    for (changes, client_id) in [
        (method(None), "saved"),
        (method(Some(json!({ "client_id": "sent" }))), "sent"),
    ] {
        let configured = arguments(changes.clone()).configuration(saved.clone(), None);
        assert_eq!(
            configured
                .authentication
                .as_ref()
                .and_then(AuthenticationConfiguration::oauth_client)
                .map(|client| client.client_id.as_str()),
            Some(client_id),
            "{changes}"
        );
    }
}

// Catches: an MCP update that keeps a saved server or OAuth endpoint that the
// server no longer reports, so a new MCP URL fails, or one that drops the
// saved scopes and application with them.
#[test]
fn an_mcp_server_sets_the_omitted_server_and_oauth_endpoints() {
    let client = OAuthClientPolicy {
        bootstrap: OAuthClientBootstrap::Manual,
        client_id: "weather-app".to_owned(),
        authentication: OAuthClientAuthenticationMethod::None,
        pkce: OAuthPkcePolicy::S256,
        authorization_parameters: BTreeMap::new(),
    };
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
            arguments(changes.clone()).configuration(saved.clone(), Some(&discovered)),
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
            .configuration(bearer.clone(), Some(&discovered))
            .authentication,
        bearer.authentication
    );
}

/// The saved Provider a review reads: an OpenAPI URL source on the fixed
/// route, with a fixed credential value that no change may show.
fn saved_provider() -> SavedProvider {
    SavedProvider {
        source: ProviderSourceOutput::OpenapiUrl {
            url: "https://weather.example/openapi.json".to_owned(),
        },
        credential_route: BuiltinCredentialRoute::Fixed,
        configuration: ProviderConfiguration {
            authentication: Some(AuthenticationConfiguration::HeaderCredentials {
                headers: BTreeMap::from([(
                    "X-Key".to_owned(),
                    HeaderBinding::Literal {
                        value: CANARY.to_owned(),
                    },
                )]),
            }),
            ..saved()
        },
    }
}

fn entry(
    key: &str,
    status: EntryStatus,
    saved: Option<Value>,
    requested: Option<Value>,
) -> EntryChange {
    EntryChange {
        key: key.to_owned(),
        status,
        saved,
        requested,
    }
}

// Catches: a review that shows only what a call sends, so a map entry the
// call drops, or the source it reads again, goes unnoticed; one that shows a
// fixed credential value; or one that hides that a refresh of an uploaded
// document fails.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one table lists the change each field kind shows"
)]
fn each_sent_field_shows_its_saved_value_beside_the_requested_one() {
    let saved_source = || ProviderSourceOutput::OpenapiUrl {
        url: "https://weather.example/openapi.json".to_owned(),
    };
    for (changes, expected) in [
        (
            json!({ "displayName": "Forecasts", "credentialRoute": "caller" }),
            vec![
                FieldChange::Value {
                    field: "credentialRoute",
                    saved: json!("fixed"),
                    requested: json!("caller"),
                },
                FieldChange::Value {
                    field: "displayName",
                    saved: json!("Weather"),
                    requested: json!("Forecasts"),
                },
            ],
        ),
        (
            json!({ "requestHeaders": { "A": "3", "C": "4" } }),
            vec![FieldChange::Entries {
                field: "requestHeaders",
                entries: vec![
                    entry(
                        "A",
                        EntryStatus::Changed,
                        Some(json!("1")),
                        Some(json!("3")),
                    ),
                    entry("B", EntryStatus::Removed, Some(json!("2")), None),
                    entry("C", EntryStatus::Added, None, Some(json!("4"))),
                ],
            }],
        ),
        (
            json!({ "servers": { "#/servers": { "kind": "custom", "url": "https://weather.example" } } }),
            vec![FieldChange::Entries {
                field: "servers",
                entries: vec![
                    entry(
                        "#/paths/~1forecast/servers",
                        EntryStatus::Removed,
                        Some(
                            json!({ "kind": "custom", "url": "https://forecast.weather.example" }),
                        ),
                        None,
                    ),
                    entry(
                        "#/servers",
                        EntryStatus::Kept,
                        Some(json!({ "kind": "custom", "url": "https://weather.example" })),
                        Some(json!({ "kind": "custom", "url": "https://weather.example" })),
                    ),
                ],
            }],
        ),
        (
            json!({ "credentialFields": { "token": { "label": "Key", "sensitive": false } } }),
            vec![FieldChange::Entries {
                field: "credentialFields",
                entries: vec![
                    entry(
                        "region",
                        EntryStatus::Removed,
                        Some(json!({ "label": "Region", "description": "", "sensitive": true })),
                        None,
                    ),
                    entry(
                        "token",
                        EntryStatus::Changed,
                        Some(json!({ "label": "Token", "description": "", "sensitive": true })),
                        Some(json!({ "label": "Key", "description": "", "sensitive": false })),
                    ),
                ],
            }],
        ),
        (
            json!({ "authentication": { "method": "bearer" } }),
            vec![FieldChange::Value {
                field: "authentication",
                // The fixed value stays out, as getProvider leaves it out.
                saved: json!({
                    "method": "header_credentials",
                    "headers": { "X-Key": { "source": "literal" } },
                }),
                requested: json!({ "method": "bearer" }),
            }],
        ),
        (
            json!({ "refresh": true }),
            vec![FieldChange::Source {
                saved: saved_source(),
                requested: SourceRead::Again,
            }],
        ),
        (
            json!({
                "source": { "kind": "openapi_document", "document": "openapi: 3.1.0" },
                "displayName": "Weather",
            }),
            vec![
                // An uploaded document shows without its content.
                FieldChange::Source {
                    saved: saved_source(),
                    requested: SourceRead::New(ProviderSourceOutput::OpenapiDocument),
                },
                FieldChange::Value {
                    field: "displayName",
                    saved: json!("Weather"),
                    requested: json!("Weather"),
                },
            ],
        ),
        (
            json!({ "source": { "kind": "mcp_url", "url": "https://weather.example/mcp" } }),
            vec![FieldChange::Source {
                saved: saved_source(),
                requested: SourceRead::New(ProviderSourceOutput::McpUrl {
                    url: "https://weather.example/mcp".to_owned(),
                }),
            }],
        ),
    ] {
        let found = arguments(changes.clone()).changes(Some(saved_provider()));
        assert_eq!(found, ProviderChanges::Fields(expected), "{changes}");
        assert!(
            !format!("{found:?}").contains(CANARY),
            "{changes}: a change shows a fixed credential value"
        );
    }
    assert_eq!(
        arguments(json!({ "displayName": "Forecasts" })).changes(None),
        ProviderChanges::ProviderMissing
    );
    // An uploaded document has no URL to read again, so the review says the call fails.
    let uploaded = SavedProvider {
        source: ProviderSourceOutput::OpenapiDocument,
        ..saved_provider()
    };
    assert_eq!(
        arguments(json!({ "refresh": true })).changes(Some(uploaded)),
        ProviderChanges::Fields(vec![FieldChange::Source {
            saved: ProviderSourceOutput::OpenapiDocument,
            requested: SourceRead::Unreadable,
        }])
    );
}
