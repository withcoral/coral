//! The Provider Configuration `getProvider` returns: the shapes `addProvider`
//! takes, without a fixed credential value.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use super::{ConfigurationOutput, GetProviderArguments};
use crate::builtin::AddProviderArguments;
use crate::{
    AuthenticationConfiguration as Auth, CredentialSource, HeaderBinding,
    OAuthClientAuthenticationMethod, OAuthClientBootstrap, OAuthClientPolicy, OAuthPkcePolicy,
    OAuthScopeFormat, ProviderConfiguration,
};

/// A fixed value that must never reach a Program.
const CANARY: &str = "sk-canary";

/// The only arguments `getProvider` takes, for its conformance test.
pub(in crate::builtin) fn valid_arguments() -> Vec<Value> {
    vec![json!({ "namespace": "weather" })]
}

fn field(name: &str) -> CredentialSource {
    CredentialSource::Field {
        field: name.to_owned(),
    }
}

fn literal(value: &str) -> CredentialSource {
    CredentialSource::Literal {
        value: value.to_owned(),
    }
}

fn client(
    bootstrap: OAuthClientBootstrap,
    client_id: &str,
    authentication: OAuthClientAuthenticationMethod,
    pkce: OAuthPkcePolicy,
) -> OAuthClientPolicy {
    OAuthClientPolicy {
        bootstrap,
        client_id: client_id.to_owned(),
        authentication,
        pkce,
        authorization_parameters: BTreeMap::new(),
    }
}

fn authorization_code(client: Option<OAuthClientPolicy>) -> Auth {
    Auth::OAuthAuthorizationCode {
        authorization_url: "https://weather.example/authorize".to_owned(),
        token_url: "https://weather.example/token".to_owned(),
        scopes: vec!["read".to_owned()],
        scope_format: OAuthScopeFormat::Space,
        resource: None,
        issuer: None,
        client,
    }
}

/// `authentication` read as `getProvider` returns it.
fn read(authentication: Auth) -> Value {
    let configuration = ConfigurationOutput::new(
        "Weather".to_owned(),
        BTreeMap::new(),
        authentication,
        BTreeMap::new(),
        BTreeMap::new(),
    );
    serde_json::to_value(configuration)
        .expect("a Provider Configuration serializes")
        .get("authentication")
        .cloned()
        .expect("the result has authentication")
}

/// A configured method of each kind, each part in each form a Provider
/// Configuration holds it in, for the output schema test.
pub(in crate::builtin) fn configured_methods() -> Vec<Auth> {
    configured_reads()
        .into_iter()
        .map(|(method, _read)| method)
        .collect()
}

// Catches: a saved fixed value, such as a header constant, returned to a
// Program that stores it in Run evidence; a fact Coral set itself, such as an
// application it registered or an MCP server's issuer, missing from the read;
// or an OAuth application a Program cannot see.
#[test]
fn each_configured_method_reads_without_a_fixed_value() {
    for (configured, expected) in configured_reads() {
        assert_eq!(read(configured.clone()), expected, "{configured:?}");
    }
}

/// Each configured method beside the `authentication` `getProvider` returns
/// for it.
#[expect(
    clippy::too_many_lines,
    reason = "one table lists how each configured method reads"
)]
fn configured_reads() -> Vec<(Auth, Value)> {
    let mut confidential = client(
        OAuthClientBootstrap::Manual,
        "weather-app",
        OAuthClientAuthenticationMethod::ClientSecretPost,
        OAuthPkcePolicy::S256,
    );
    confidential
        .authorization_parameters
        .insert("access_type".to_owned(), "offline".to_owned());
    vec![
        (Auth::None, json!({ "method": "none" })),
        (Auth::Bearer, json!({ "method": "bearer" })),
        (
            Auth::HeaderCredentials {
                headers: BTreeMap::from([
                    (
                        "authorization".to_owned(),
                        HeaderBinding::Field {
                            field: "token".to_owned(),
                            prefix: "Bot ".to_owned(),
                        },
                    ),
                    (
                        "x-tenant".to_owned(),
                        HeaderBinding::Literal {
                            value: CANARY.to_owned(),
                        },
                    ),
                ]),
            },
            json!({
                "method": "header_credentials",
                "headers": {
                    "authorization": { "source": "field", "field": "token", "prefix": "Bot " },
                    "x-tenant": { "source": "literal" },
                },
            }),
        ),
        (
            // A blank part is no value, so it reads as addProvider takes it.
            Auth::BasicAuth {
                username: literal(CANARY),
                password: literal(""),
            },
            json!({
                "method": "basic_auth",
                "username": { "source": "literal" },
                "password": { "source": "literal", "value": "" },
            }),
        ),
        (
            Auth::QueryCredentials {
                parameters: BTreeMap::from([
                    ("key".to_owned(), field("key")),
                    ("tenant".to_owned(), literal(CANARY)),
                ]),
            },
            json!({
                "method": "query_credentials",
                "parameters": {
                    "key": { "source": "field", "field": "key" },
                    "tenant": { "source": "literal" },
                },
            }),
        ),
        (
            // Device authorization takes no PKCE or authorization parameters.
            Auth::OAuthDeviceCode {
                device_authorization_url: "https://weather.example/device".to_owned(),
                token_url: "https://weather.example/token".to_owned(),
                scopes: vec!["read".to_owned()],
                scope_format: OAuthScopeFormat::Comma,
                resource: Some("https://weather.example/mcp".to_owned()),
                issuer: Some("https://auth.weather.example".to_owned()),
                client: Some(client(
                    OAuthClientBootstrap::Manual,
                    "weather-app",
                    OAuthClientAuthenticationMethod::ClientSecretBasic,
                    OAuthPkcePolicy::Disabled,
                )),
            },
            json!({
                "method": "oauth_device_code",
                "device_authorization_url": "https://weather.example/device",
                "token_url": "https://weather.example/token",
                "scopes": ["read"],
                "scope_format": "comma",
                "issuer": "https://auth.weather.example",
                "resource": "https://weather.example/mcp",
                "client": { "client_id": "weather-app", "authentication": "client_secret_basic" },
            }),
        ),
        (
            // A confidential application's secret is stored apart, and an MCP
            // server's issuer and resource read as discovery found them.
            Auth::OAuthAuthorizationCode {
                authorization_url: "https://weather.example/authorize".to_owned(),
                token_url: "https://weather.example/token".to_owned(),
                scopes: vec!["read".to_owned()],
                scope_format: OAuthScopeFormat::Space,
                resource: Some("https://weather.example/mcp".to_owned()),
                issuer: Some("https://auth.weather.example".to_owned()),
                client: Some(confidential),
            },
            json!({
                "method": "oauth_authorization_code",
                "authorization_url": "https://weather.example/authorize",
                "token_url": "https://weather.example/token",
                "scopes": ["read"],
                "scope_format": "space",
                "issuer": "https://auth.weather.example",
                "resource": "https://weather.example/mcp",
                "client": {
                    "client_id": "weather-app",
                    "authentication": "client_secret_post",
                    "pkce": "s256",
                    "authorization_parameters": { "access_type": "offline" },
                },
            }),
        ),
        (
            // Coral registered this application, and the Program reads how.
            authorization_code(Some(OAuthClientPolicy::public(
                OAuthClientBootstrap::DynamicClientRegistration,
                "registered-by-coral".to_owned(),
            ))),
            json!({
                "method": "oauth_authorization_code",
                "authorization_url": "https://weather.example/authorize",
                "token_url": "https://weather.example/token",
                "scopes": ["read"],
                "scope_format": "space",
                "client": {
                    "client_id": "registered-by-coral",
                    "authentication": "none",
                    "pkce": "s256",
                    "authorization_parameters": {},
                    "registration": "dynamic_client_registration",
                },
            }),
        ),
        (
            authorization_code(Some(OAuthClientPolicy::public(
                OAuthClientBootstrap::ClientIdMetadataDocument,
                "https://coral.example/oauth/client.json".to_owned(),
            ))),
            json!({
                "method": "oauth_authorization_code",
                "authorization_url": "https://weather.example/authorize",
                "token_url": "https://weather.example/token",
                "scopes": ["read"],
                "scope_format": "space",
                "client": {
                    "client_id": "https://coral.example/oauth/client.json",
                    "authentication": "none",
                    "pkce": "s256",
                    "authorization_parameters": {},
                    "registration": "client_id_metadata_document",
                },
            }),
        ),
        (
            // Coral registers an application when a person signs in.
            authorization_code(None),
            json!({
                "method": "oauth_authorization_code",
                "authorization_url": "https://weather.example/authorize",
                "token_url": "https://weather.example/token",
                "scopes": ["read"],
                "scope_format": "space",
            }),
        ),
    ]
}

/// The configuration `arguments` give `addProvider`, each field over an
/// empty suggestion.
fn written(arguments: &Value) -> ProviderConfiguration {
    AddProviderArguments::parse(arguments)
        .unwrap_or_else(|problems| panic!("{arguments} is valid: {problems:?}"))
        .choices
        .over(ProviderConfiguration {
            authentication: None,
            request_headers: BTreeMap::new(),
            credential_fields: BTreeMap::new(),
            servers: BTreeMap::new(),
            display_name: "Suggested".to_owned(),
        })
}

// Catches: a read a Program cannot send back to addProvider, or one that
// sends back a different configuration than the one saved.
#[test]
fn a_configuration_a_program_wrote_reads_back_as_the_same_configuration() {
    for arguments in super::super::add_provider::tests::valid_arguments() {
        let saved = written(&arguments);
        let read = serde_json::to_value(ConfigurationOutput::new(
            saved.display_name.clone(),
            saved.servers.clone(),
            saved
                .authentication
                .clone()
                .expect("each valid call names a method"),
            saved.request_headers.clone(),
            saved.credential_fields.clone(),
        ))
        .expect("a Provider Configuration serializes");
        let mut sent_back = read.as_object().expect("an object").clone();
        for kept in ["namespace", "source", "credentialRoute"] {
            let value = arguments.get(kept).cloned().expect("a valid call has it");
            sent_back.insert(kept.to_owned(), value);
        }
        assert_eq!(written(&Value::Object(sent_back)), saved, "{arguments}");
    }
}

// Catches: an application Coral registered, sent back and saved as one a
// person registered, or an MCP server's issuer sent back and dropped.
#[test]
fn a_fact_coral_set_is_rejected_when_sent_back() {
    let registered = authorization_code(Some(OAuthClientPolicy::public(
        OAuthClientBootstrap::DynamicClientRegistration,
        "registered-by-coral".to_owned(),
    )));
    let discovered = Auth::OAuthAuthorizationCode {
        authorization_url: "https://weather.example/authorize".to_owned(),
        token_url: "https://weather.example/token".to_owned(),
        scopes: vec!["read".to_owned()],
        scope_format: OAuthScopeFormat::Space,
        resource: Some("https://weather.example/mcp".to_owned()),
        issuer: Some("https://auth.weather.example".to_owned()),
        client: None,
    };
    for (saved, expected) in [
        (
            registered,
            "unknown field `registration`, expected one of `client_id`, `authentication`, \
             `pkce`, `authorization_parameters`",
        ),
        (
            discovered,
            "unknown field `issuer`, expected one of `authorization_url`, `token_url`, `scopes`, \
             `scope_format`, `client`",
        ),
    ] {
        let arguments = json!({
            "namespace": "weather",
            "source": { "kind": "mcp_url", "url": "https://weather.example/mcp" },
            "credentialRoute": "caller",
            "authentication": read(saved.clone()),
        });
        let problems =
            AddProviderArguments::parse(&arguments).expect_err(&format!("{saved:?} is rejected"));
        let found = problems
            .iter()
            .map(|problem| (problem.field(), problem.reason()))
            .collect::<Vec<_>>();
        assert_eq!(found, [("authentication", expected)], "{saved:?}");
    }
}

// Catches: a namespace no Provider can hold, looked up instead of corrected.
#[test]
fn an_invalid_namespace_names_what_to_correct() {
    for arguments in [
        json!({ "namespace": "not-an-identifier" }),
        json!({ "namespace": "coral.providers.weather" }),
        json!({ "namespace": "" }),
    ] {
        let problems =
            GetProviderArguments::parse(&arguments).expect_err(&format!("{arguments} is rejected"));
        let found = problems
            .iter()
            .map(|problem| (problem.field(), problem.reason()))
            .collect::<Vec<_>>();
        assert_eq!(
            found,
            [(
                "namespace",
                "use a JavaScript identifier of at most 128 bytes, such as `weather`"
            )],
            "{arguments}"
        );
    }
}
