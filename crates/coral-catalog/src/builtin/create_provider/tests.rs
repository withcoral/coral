//! The rules `createProvider`'s arguments meet beyond their schema, and the
//! arguments its conformance test starts from.

use serde_json::{Value, json};

use super::CreateProviderArguments;

/// A value that must never reach a reason.
const CANARY: &str = "sk-canary";

/// Arguments that use every part of the input schema, with values that meet
/// the rules a schema cannot state, such as the namespace syntax.
pub(in crate::builtin) fn valid_arguments() -> Vec<Value> {
    let with = |source: Value, route: &str, authentication: Value| {
        json!({
            "namespace": "weather",
            "source": source,
            "credentialRoute": route,
            "authentication": authentication,
        })
    };
    let url = json!({ "kind": "openapi_url", "url": "https://weather.example/openapi.json" });
    let oauth = |client: Value| {
        json!({
            "method": "oauth_authorization_code",
            "authorization_url": "https://weather.example/authorize",
            "token_url": "https://weather.example/token",
            "scopes": ["read", "write"],
            "scope_format": "comma",
            "client": client,
        })
    };
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
                "#/paths/~1forecast/servers": {
                    "kind": "custom",
                    "url": "https://forecast.weather.example",
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
        with(url.clone(), "none", json!({ "method": "none" })),
        with(url.clone(), "fixed", json!({ "method": "bearer" })),
        with(
            url.clone(),
            "fixed",
            json!({
                "method": "basic_auth",
                "username": { "source": "field", "field": "username" },
                "password": { "source": "literal", "value": "" },
            }),
        ),
        with(
            url.clone(),
            "caller",
            json!({
                "method": "query_credentials",
                "parameters": { "key": { "source": "field", "field": "key" } },
            }),
        ),
        with(
            url.clone(),
            "caller",
            json!({
                "method": "oauth_device_code",
                "device_authorization_url": "https://weather.example/device",
                "token_url": "https://weather.example/token",
                "scopes": ["read"],
                "scope_format": "space",
                "client": {
                    "client_id": "weather-app",
                    "authentication": "client_secret_basic",
                    "pkce": "disabled",
                },
            }),
        ),
        with(
            json!({ "kind": "mcp_url", "url": "https://weather.example/mcp" }),
            "caller",
            oauth(json!({
                "client_id": "weather-app",
                "authentication": "client_secret_post",
                "pkce": "s256",
                "authorization_parameters": { "access_type": "offline" },
            })),
        ),
        with(
            url,
            "fixed",
            oauth(json!({ "client_id": "weather-app", "authentication": "none" })),
        ),
    ]
}

/// The arguments with `extra` over a valid minimal call.
fn valid_with(extra: Value) -> Value {
    let mut arguments = json!({
        "namespace": "weather",
        "source": { "kind": "openapi_url", "url": "https://weather.example/openapi.json" },
        "credentialRoute": "none",
    });
    if let (Some(arguments), Value::Object(extra)) = (arguments.as_object_mut(), extra) {
        arguments.extend(extra);
    }
    arguments
}

fn authenticated(authentication: Value) -> Value {
    let mut arguments = valid_with(json!({ "credentialRoute": "fixed" }));
    if let Some(arguments) = arguments.as_object_mut() {
        arguments.insert("authentication".to_owned(), authentication);
    }
    arguments
}

fn oauth(extra: Value) -> Value {
    let mut authentication = json!({
        "method": "oauth_authorization_code",
        "authorization_url": "https://weather.example/authorize",
        "token_url": "https://weather.example/token",
        "scopes": [],
    });
    if let (Some(authentication), Value::Object(extra)) = (authentication.as_object_mut(), extra) {
        authentication.extend(extra);
    }
    authenticated(authentication)
}

const HEADER_REASON: &str = "this header names a credential; send it through `authentication` \
     so a person enters its value when they sign in";
const LITERAL_REASON: &str = "a literal credential value must be empty; use a `field` source so \
     a person enters the value when they sign in";
const ENDPOINT_REASON: &str = "use an absolute `http` or `https` URL without credentials, a \
     fragment or an OAuth parameter that Coral sets, such as `client_secret`";
const NAMESPACE_REASON: &str =
    "use a JavaScript identifier of at most 128 bytes, such as `weather`";
const SOURCE_URL_REASON: &str =
    "use an absolute `http` or `https` URL without credentials or a fragment";

// Catches: a credential, or a value Coral cannot use, accepted into arguments
// that Coral stores and Owners can read; or a reason that repeats a submitted
// value or fails to name the argument to correct.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one table lists every argument rule and the reason it gives"
)]
fn each_rejected_argument_names_what_to_correct() {
    for (arguments, expected) in [
        (
            json!({ "namespace": "weather", "source": { "kind": "openapi_url", "url": "https://weather.example" } }),
            vec![("credentialRoute", "choose `none`, `fixed` or `caller`")],
        ),
        (
            // serde quotes the value before its own `expected` clause.
            valid_with(json!({ "credentialRoute": format!("expected {CANARY}") })),
            vec![(
                "credentialRoute",
                "expected one of `none`, `fixed`, `caller`",
            )],
        ),
        (
            valid_with(json!({ "displayName": [CANARY] })),
            vec![("displayName", "expected a string")],
        ),
        (
            valid_with(json!({ "displayname": "Forecasts" })),
            vec![(
                "displayname",
                "unknown field `displayname`, expected one of `namespace`, `source`, \
                 `credentialRoute`, `displayName`, `servers`, `authentication`, \
                 `requestHeaders`, `credentialFields`",
            )],
        ),
        (
            valid_with(json!({ "namespace": "not-an-identifier" })),
            vec![("namespace", NAMESPACE_REASON)],
        ),
        (
            valid_with(json!({ "namespace": "" })),
            vec![("namespace", NAMESPACE_REASON)],
        ),
        (
            valid_with(json!({ "namespace": "coral.providers.weather" })),
            vec![("namespace", NAMESPACE_REASON)],
        ),
        (
            valid_with(json!({ "namespace": "then" })),
            vec![("namespace", NAMESPACE_REASON)],
        ),
        (
            valid_with(json!({ "source": { "kind": "openapi_url" } })),
            vec![("source", "missing field `url`")],
        ),
        (
            valid_with(json!({ "source": { "kind": "openapi_url", "url": "not a url" } })),
            vec![("source.url", SOURCE_URL_REASON)],
        ),
        (
            valid_with(
                json!({ "source": { "kind": "openapi_url", "url": format!("ftp://{CANARY}.example") } }),
            ),
            vec![("source.url", SOURCE_URL_REASON)],
        ),
        (
            valid_with(json!({ "source": { "kind": "mcp_url", "url": "file:///etc/passwd" } })),
            vec![("source.url", SOURCE_URL_REASON)],
        ),
        (
            // A Setup takes no credentials or fragment in its source URL.
            valid_with(json!({
                "source": { "kind": "openapi_url", "url": format!("https://user:{CANARY}@weather.example") },
            })),
            vec![("source.url", SOURCE_URL_REASON)],
        ),
        (
            valid_with(
                json!({ "source": { "kind": "mcp_url", "url": "https://weather.example/mcp#tools" } }),
            ),
            vec![("source.url", SOURCE_URL_REASON)],
        ),
        (
            // A method that takes no settings takes no credential beside it.
            authenticated(json!({ "method": "bearer", "token": CANARY })),
            vec![(
                "authentication",
                "unknown field `token`, there are no fields",
            )],
        ),
        (
            authenticated(json!({ "method": "none", "token": CANARY })),
            vec![(
                "authentication",
                "unknown field `token`, there are no fields",
            )],
        ),
        (
            authenticated(json!({
                "method": "header_credentials",
                "headers": { "x-weather-key": { "source": "literal", "value": CANARY } },
            })),
            vec![("authentication", "expected `field`")],
        ),
        (
            authenticated(json!({
                "method": "basic_auth",
                "username": { "source": "field", "field": "username" },
                "password": { "source": "literal", "value": CANARY },
            })),
            vec![("authentication.password", LITERAL_REASON)],
        ),
        (
            authenticated(json!({
                "method": "query_credentials",
                "parameters": { "api_key": { "source": "literal", "value": CANARY } },
            })),
            vec![("authentication.parameters.api_key", LITERAL_REASON)],
        ),
        (
            oauth(
                json!({ "authorization_url": format!("https://user:{CANARY}@weather.example/authorize") }),
            ),
            vec![("authentication.authorization_url", ENDPOINT_REASON)],
        ),
        (
            oauth(
                json!({ "token_url": format!("https://weather.example/token?client_secret={CANARY}") }),
            ),
            vec![("authentication.token_url", ENDPOINT_REASON)],
        ),
        (
            oauth(json!({
                "client": {
                    "client_id": "weather-app",
                    "authorization_parameters": { "client_secret": CANARY },
                },
            })),
            vec![(
                "authentication.client.authorization_parameters.client_secret",
                "Coral sets this OAuth parameter, or a person enters it when they sign in; \
                 remove it",
            )],
        ),
        (
            // Device authorization takes no authorization parameters.
            authenticated(json!({
                "method": "oauth_device_code",
                "device_authorization_url": "https://weather.example/device",
                "token_url": "https://weather.example/token",
                "scopes": [],
                "client": {
                    "client_id": "weather-app",
                    "authorization_parameters": { "audience": "weather" },
                },
            })),
            // serde reads a tagged method whole, so the path stops at it.
            vec![(
                "authentication",
                "unknown field `authorization_parameters`, expected one of `client_id`, \
                 `authentication`, `pkce`",
            )],
        ),
        (
            // A credential field takes no value: a person enters it when they sign in.
            valid_with(json!({
                "credentialFields": { "token": { "label": "Bot token", "sensitive": true, "value": CANARY } },
            })),
            vec![(
                "credentialFields.token.value",
                "unknown field `value`, expected one of `label`, `description`, `sensitive`",
            )],
        ),
        (
            valid_with(json!({ "requestHeaders": { "X-Api-Key": CANARY } })),
            vec![("requestHeaders.X-Api-Key", HEADER_REASON)],
        ),
        (
            // Every problem Coral can find without the source, at once.
            json!({
                "namespace": "not-an-identifier",
                "source": { "kind": "openapi_url", "url": "ftp://weather.example" },
                "requestHeaders": { "X-Api-Key": CANARY, "Authorization": CANARY },
            }),
            vec![
                ("namespace", NAMESPACE_REASON),
                ("requestHeaders.Authorization", HEADER_REASON),
                ("requestHeaders.X-Api-Key", HEADER_REASON),
                ("credentialRoute", "choose `none`, `fixed` or `caller`"),
                ("source.url", SOURCE_URL_REASON),
            ],
        ),
    ] {
        let problems = CreateProviderArguments::parse(&arguments)
            .expect_err(&format!("{arguments} is rejected"));
        let found = problems
            .iter()
            .map(|problem| (problem.field(), problem.reason()))
            .collect::<Vec<_>>();
        assert_eq!(found, expected, "{arguments}");
        assert!(
            problems
                .iter()
                .all(|problem| !problem.reason().contains(CANARY)),
            "{arguments}: a reason repeats a submitted value"
        );
    }
}

// Catches: a header named for a credential carrying its value through the
// Program, or a service header such as `Idempotency-Key` rejected because a
// credential word appears inside its name.
#[test]
fn credential_headers_are_rejected_by_their_exact_name() {
    for (header, rejected) in [
        ("Authorization", true),
        ("proxy-authorization", true),
        ("Cookie", true),
        ("X-API-Key", true),
        ("Api-Key", true),
        ("X-Auth-Token", true),
        ("X-Client-Secret", true),
        ("Idempotency-Key", false),
    ] {
        let problems = CreateProviderArguments::parse(&valid_with(
            json!({ "requestHeaders": { header: "value" } }),
        ))
        .err()
        .unwrap_or_default();
        let found = problems
            .iter()
            .map(|problem| (problem.field(), problem.reason()))
            .collect::<Vec<_>>();
        let field = format!("requestHeaders.{header}");
        let expected = if rejected {
            vec![(field.as_str(), HEADER_REASON)]
        } else {
            Vec::new()
        };
        assert_eq!(found, expected, "{header}");
    }
}

// Catches: a configuration problem Coral finds in the source naming a field
// the Program never wrote, so it cannot tell what to correct.
#[test]
fn a_configuration_field_names_the_argument_to_correct() {
    for (configuration_field, argument) in [
        ("#/servers", "servers.#/servers"),
        ("display_name", "displayName"),
        ("request_headers.Host", "requestHeaders.Host"),
        ("credential_fields.token", "credentialFields.token"),
        ("authentication.fields.token", "credentialFields.token"),
        ("application", "authentication.client"),
        ("server", "servers"),
        ("authentication", "authentication"),
        // Only a whole segment is renamed.
        ("servers_extra", "servers_extra"),
    ] {
        assert_eq!(
            CreateProviderArguments::argument_for(configuration_field),
            argument,
            "{configuration_field}"
        );
    }
}
