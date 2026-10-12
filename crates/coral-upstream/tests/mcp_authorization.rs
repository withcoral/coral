//! MCP authorization discovery through the upstream Adapter's public seam.
#![allow(
    unused_crate_dependencies,
    reason = "integration targets inherit library dependencies"
)]
#![expect(clippy::indexing_slicing, reason = "assert known fixture JSON members")]

use coral_outbound::DestinationPolicy;
use coral_upstream::{
    AuthorizationDiscoveryError, AuthorizationRequirement, BearerChallenge, discover_authorization,
    register_client,
};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn authorization_server(issuer: &str, extra: &Value) -> Value {
    let mut metadata = json!({
        "issuer": issuer,
        "authorization_endpoint": format!("{issuer}/authorize"),
        "token_endpoint": format!("{issuer}/token"),
        "registration_endpoint": format!("{issuer}/register"),
        "code_challenge_methods_supported": ["S256"],
        "client_id_metadata_document_supported": true,
        "scopes_supported": ["read", "write", "openid"]
    });
    if let (Some(base), Some(extra)) = (metadata.as_object_mut(), extra.as_object()) {
        for (key, value) in extra {
            base.insert(key.clone(), value.clone());
        }
    }
    metadata
}

async fn mount_json(server: &MockServer, at: &str, body: &Value) {
    Mock::given(method("GET"))
        .and(path(at))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

fn loopback() -> DestinationPolicy {
    DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps)
}

/// Discovery for a loopback fixture that needs no service headers.
async fn discover(
    mcp: &str,
    challenge: &BearerChallenge,
) -> Result<AuthorizationRequirement, AuthorizationDiscoveryError> {
    discover_authorization(
        loopback(),
        mcp,
        challenge,
        &std::collections::BTreeMap::new(),
    )
    .await
}

fn challenge(resource_metadata: Option<&str>, scopes: &[&str]) -> BearerChallenge {
    BearerChallenge::from_parts(
        resource_metadata.map(str::to_owned),
        scopes.iter().map(|scope| (*scope).to_owned()).collect(),
    )
}

#[tokio::test]
async fn service_headers_reach_only_same_origin_resource_metadata() {
    let service = MockServer::start().await;
    let external = MockServer::start().await;
    let mcp = format!("{}/mcp", service.uri());
    let headers = std::collections::BTreeMap::from([("x-tenant".into(), "team".into())]);
    for issuer in [&service, &external] {
        mount_json(
            issuer,
            "/.well-known/oauth-authorization-server",
            &authorization_server(&issuer.uri(), &json!({})),
        )
        .await;
    }
    // Service headers must never reach issuer metadata, even on the service origin.
    // Each issuer gets its own resource path: mocks on one path shadow later ones.
    for (resource, issuer) in [
        ("/resource/service", &service),
        ("/resource/external", &external),
    ] {
        Mock::given(method("GET"))
            .and(path(resource))
            .and(wiremock::matchers::header("x-tenant", "team"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "resource": mcp, "authorization_servers": [issuer.uri()]
            })))
            .mount(&service)
            .await;
        let requirement = discover_authorization(
            loopback(),
            &mcp,
            &challenge(Some(&format!("{}{resource}", service.uri())), &[]),
            &headers,
        )
        .await
        .expect("header-gated resource metadata");
        assert_eq!(requirement.issuer(), issuer.uri());
    }
    mount_json(
        &external,
        "/resource",
        &json!({
            "resource": mcp, "authorization_servers": [external.uri()]
        }),
    )
    .await;
    let requirement = discover_authorization(
        loopback(),
        &mcp,
        &challenge(Some(&format!("{}/resource", external.uri())), &[]),
        &headers,
    )
    .await
    .expect("external resource metadata without service headers");
    assert_eq!(requirement.issuer(), external.uri());
    for server in [&service, &external] {
        for request in server.received_requests().await.unwrap() {
            if server.uri() == service.uri() && request.url.path().starts_with("/resource/") {
                assert_eq!(request.headers.get("x-tenant").unwrap(), "team");
            } else {
                assert!(!request.headers.contains_key("x-tenant"));
            }
        }
    }
}

#[tokio::test]
async fn challenge_metadata_selects_resource_issuer_endpoints_and_challenge_scopes() {
    let server = MockServer::start().await;
    let mcp = format!("{}/mcp", server.uri());
    mount_json(
        &server,
        "/.well-known/oauth-protected-resource/mcp",
        &json!({"resource": mcp, "authorization_servers": [server.uri()],
                "scopes_supported": ["read", "write"]}),
    )
    .await;
    mount_json(
        &server,
        "/.well-known/oauth-authorization-server",
        &authorization_server(&server.uri(), &json!({})),
    )
    .await;
    let requirement = discover(
        &mcp,
        &challenge(
            Some(&format!(
                "{}/.well-known/oauth-protected-resource/mcp",
                server.uri()
            )),
            &["read"],
        ),
    )
    .await
    .expect("authorization requirement");
    assert_eq!(requirement.resource(), mcp);
    assert_eq!(requirement.issuer(), server.uri());
    assert_eq!(
        requirement.authorization_endpoint(),
        Some(format!("{}/authorize", server.uri()).as_str())
    );
    assert_eq!(
        requirement.token_endpoint(),
        format!("{}/token", server.uri())
    );
    assert_eq!(
        requirement.registration_endpoint(),
        Some(format!("{}/register", server.uri()).as_str())
    );
    assert!(requirement.client_id_metadata_document_supported());
    assert_eq!(requirement.scopes(), ["read"]);
    for request in server.received_requests().await.unwrap() {
        assert!(request.headers.get("authorization").is_none());
    }
}

#[tokio::test]
async fn discovery_exposes_only_advertised_and_permitted_device_endpoints() {
    /// The discovered device and authorization endpoints, or the refusal code.
    type Expected<'a> = Result<(Option<&'a str>, Option<&'a str>), &'a str>;
    let server = MockServer::start().await;
    let mcp = format!("{}/mcp", server.uri());
    let uri = server.uri();
    let device = format!("{uri}/device");
    let authorize = format!("{uri}/authorize");
    let cases: Vec<(Value, Expected)> = vec![
        (
            authorization_server(&uri, &json!({})),
            Ok((None, Some(&authorize))),
        ),
        (
            authorization_server(&uri, &json!({"device_authorization_endpoint": device})),
            Ok((Some(&device), Some(&authorize))),
        ),
        // A device-only server needs neither an authorization endpoint nor PKCE.
        (
            json!({
                "issuer": uri, "token_endpoint": format!("{uri}/token"),
                "device_authorization_endpoint": device
            }),
            Ok((Some(&device), None)),
        ),
        (
            authorization_server(
                &uri,
                &json!({"device_authorization_endpoint": "http://203.0.113.9/device"}),
            ),
            Err("outbound_destination_rejected"),
        ),
    ];
    for (metadata, expected) in cases {
        server.reset().await;
        mount_json(
            &server,
            "/.well-known/oauth-protected-resource/mcp",
            &json!({"resource": mcp, "authorization_servers": [uri]}),
        )
        .await;
        mount_json(
            &server,
            "/.well-known/oauth-authorization-server",
            &metadata,
        )
        .await;
        let outcome = discover(&mcp, &challenge(None, &[])).await;
        match expected {
            Ok((device, authorization)) => {
                let requirement = outcome.expect("authorization requirement");
                assert_eq!(requirement.device_authorization_endpoint(), device);
                assert_eq!(requirement.authorization_endpoint(), authorization);
            }
            Err(code) => assert_eq!(outcome.expect_err(code).code(), code),
        }
    }
}

/// Slack answers `https://mcp.slack.com/mcp` with a host-level document whose
/// `resource` is `https://mcp.slack.com`. The identifier the authorization
/// server knows is the one requested, and a path ancestor covers the same way.
#[tokio::test]
async fn an_ancestor_protected_resource_covers_the_server_and_is_the_resource_requested() {
    let server = MockServer::start().await;
    let cases = [
        (server.uri(), format!("{}/mcp", server.uri())),
        (
            format!("{}/api", server.uri()),
            format!("{}/api/mcp", server.uri()),
        ),
        (
            format!("{}/api/", server.uri()),
            format!("{}/api/mcp", server.uri()),
        ),
        // A server query selects the endpoint, not the resource.
        (
            format!("{}/mcp", server.uri()),
            format!("{}/mcp?tenant=team", server.uri()),
        ),
    ];
    for (declared, mcp) in cases {
        server.reset().await;
        mount_json(
            &server,
            "/.well-known/oauth-protected-resource",
            &json!({"resource": declared, "authorization_servers": [server.uri()],
                    "scopes_supported": ["chat:write"]}),
        )
        .await;
        mount_json(
            &server,
            "/.well-known/oauth-authorization-server",
            &authorization_server(&server.uri(), &json!({})),
        )
        .await;
        let requirement = discover(
            &mcp,
            &challenge(
                Some(&format!(
                    "{}/.well-known/oauth-protected-resource",
                    server.uri()
                )),
                &[],
            ),
        )
        .await
        .expect("authorization requirement");
        assert_eq!(requirement.resource(), declared, "{mcp}");
        assert_eq!(requirement.scopes(), ["chat:write"]);
    }
}

/// Covering stops at a path segment: a sibling by prefix, a descendant, and
/// another origin are not the server's resource.
#[tokio::test]
async fn a_protected_resource_that_is_neither_the_server_nor_an_ancestor_is_refused() {
    let server = MockServer::start().await;
    let mcp = format!("{}/api/mcp", server.uri());
    let declared = [
        format!("{}/ap", server.uri()),
        format!("{}/api/mc", server.uri()),
        format!("{mcp}/deeper"),
        "https://mcp.example".to_owned(),
    ];
    for resource in declared {
        server.reset().await;
        mount_json(
            &server,
            "/.well-known/oauth-protected-resource",
            &json!({"resource": resource, "authorization_servers": [server.uri()]}),
        )
        .await;
        let error = discover(
            &mcp,
            &challenge(
                Some(&format!(
                    "{}/.well-known/oauth-protected-resource",
                    server.uri()
                )),
                &[],
            ),
        )
        .await
        .expect_err(&resource);
        assert_eq!(error.code(), "resource_mismatch", "{resource}");
        // Refused before any authorization server is consulted.
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn well_known_fallbacks_and_protected_resource_scopes_apply_without_challenge_details() {
    let server = MockServer::start().await;
    let mcp = format!("{}/mcp", server.uri());
    // No path-specific document: the root protected-resource document is used.
    mount_json(
        &server,
        "/.well-known/oauth-protected-resource",
        &json!({"resource": mcp, "authorization_servers": [server.uri()],
                "scopes_supported": ["write", "read"]}),
    )
    .await;
    // RFC 8414 is absent; OpenID discovery answers for the issuer.
    mount_json(
        &server,
        "/.well-known/openid-configuration",
        &authorization_server(
            &server.uri(),
            &json!({"registration_endpoint": Value::Null}),
        ),
    )
    .await;
    let requirement = discover(&mcp, &challenge(None, &[]))
        .await
        .expect("fallback discovery");
    assert_eq!(
        requirement.scopes(),
        ["read", "write"],
        "sorted resource scopes"
    );
    assert_eq!(requirement.registration_endpoint(), None);
    let paths: Vec<String> = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|request| request.url.path().to_owned())
        .collect();
    assert_eq!(
        paths,
        [
            "/.well-known/oauth-protected-resource/mcp",
            "/.well-known/oauth-protected-resource",
            "/.well-known/oauth-authorization-server",
            "/.well-known/openid-configuration",
        ]
    );
}

#[tokio::test]
async fn issuer_paths_and_multiple_issuers_follow_the_specification_order() {
    let server = MockServer::start().await;
    let mcp = format!("{}/mcp", server.uri());
    let broken = format!("{}/broken", server.uri());
    let tenant = format!("{}/tenant", server.uri());
    mount_json(
        &server,
        "/.well-known/oauth-protected-resource/mcp",
        &json!({"resource": mcp, "authorization_servers": [broken, tenant]}),
    )
    .await;
    // The broken issuer publishes metadata for a different issuer and is skipped.
    mount_json(
        &server,
        "/.well-known/oauth-authorization-server/broken",
        &authorization_server("https://elsewhere.example", &json!({})),
    )
    .await;
    // The tenant issuer answers only at the OpenID path-suffix location.
    mount_json(
        &server,
        "/tenant/.well-known/openid-configuration",
        &authorization_server(&tenant, &json!({})),
    )
    .await;
    let requirement = discover(&mcp, &challenge(None, &["read"]))
        .await
        .expect("second issuer");
    assert_eq!(requirement.issuer(), tenant);
    let paths: Vec<String> = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|request| request.url.path().to_owned())
        .collect();
    assert_eq!(
        paths,
        [
            "/.well-known/oauth-protected-resource/mcp",
            "/.well-known/oauth-authorization-server/broken",
            "/.well-known/oauth-authorization-server/tenant",
            "/.well-known/openid-configuration/tenant",
            "/tenant/.well-known/openid-configuration",
        ]
    );
}

/// Google advertises `https://accounts.google.com/` for Gmail's MCP server while
/// that server declares `https://accounts.google.com`. The two spell one issuer,
/// so discovery selects it and reports the declared spelling.
#[tokio::test]
async fn an_issuer_advertised_with_a_trailing_slash_is_the_issuer_it_declares() {
    let server = MockServer::start().await;
    let mcp = format!("{}/mcp", server.uri());
    mount_json(
        &server,
        "/.well-known/oauth-protected-resource/mcp",
        &json!({"resource": mcp, "authorization_servers": [format!("{}/", server.uri())],
                "scopes_supported": ["read"]}),
    )
    .await;
    mount_json(
        &server,
        "/.well-known/oauth-authorization-server",
        &authorization_server(&server.uri(), &json!({})),
    )
    .await;
    let requirement = discover(&mcp, &challenge(None, &[]))
        .await
        .expect("authorization requirement");
    assert_eq!(requirement.issuer(), server.uri());
    assert_eq!(
        requirement.token_endpoint(),
        format!("{}/token", server.uri())
    );
}

#[tokio::test]
async fn untrusted_metadata_is_validated_and_never_followed() {
    let server = MockServer::start().await;
    let mcp = format!("{}/mcp", server.uri());
    let prm = format!("{}/.well-known/oauth-protected-resource/mcp", server.uri());
    let cases: Vec<(Value, Value, &str)> = vec![
        (
            json!({"resource": format!("{}/other", server.uri()), "authorization_servers": [server.uri()]}),
            authorization_server(&server.uri(), &json!({})),
            "resource_mismatch",
        ),
        (
            json!({"resource": mcp, "authorization_servers": [server.uri()]}),
            authorization_server(
                &server.uri(),
                &json!({"code_challenge_methods_supported": Value::Null}),
            ),
            "pkce_unsupported",
        ),
        (
            json!({"resource": mcp, "authorization_servers": [server.uri()]}),
            authorization_server(
                &server.uri(),
                &json!({"token_endpoint": "http://203.0.113.9/token"}),
            ),
            "outbound_destination_rejected",
        ),
        (
            json!({"resource": mcp, "authorization_servers": [server.uri()]}),
            authorization_server("https://someone-else.example", &json!({})),
            "issuer_mismatch",
        ),
        (
            json!({"resource": mcp, "authorization_servers": []}),
            authorization_server(&server.uri(), &json!({})),
            "authorization_server_unavailable",
        ),
        (
            json!({"resource": format!("{mcp}/"), "authorization_servers": [server.uri()]}),
            authorization_server(&server.uri(), &json!({})),
            "resource_mismatch",
        ),
        (
            json!({"resource": mcp, "authorization_servers": [server.uri()],
                   "scopes_supported": (0..101).map(|i| format!("scope{i}")).collect::<Vec<_>>()}),
            authorization_server(&server.uri(), &json!({})),
            "resource_metadata_unavailable",
        ),
    ];
    for (protected_resource, metadata, expected) in cases {
        server.reset().await;
        mount_json(
            &server,
            "/.well-known/oauth-protected-resource/mcp",
            &protected_resource,
        )
        .await;
        mount_json(
            &server,
            "/.well-known/oauth-authorization-server",
            &metadata,
        )
        .await;
        let error = discover(&mcp, &challenge(Some(&prm), &[]))
            .await
            .expect_err(expected);
        assert_eq!(error.code(), expected, "{error:?}");
        assert!(!format!("{error:?}").contains("203.0.113.9"));
    }
    // Redirected metadata is unavailable, and a challenge pointing at a
    // non-HTTPS remote host is refused before any request.
    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/.well-known/oauth-protected-resource/mcp"))
        .respond_with(ResponseTemplate::new(302).insert_header("location", "/elsewhere"))
        .mount(&server)
        .await;
    let error = discover(&mcp, &challenge(Some(&prm), &[]))
        .await
        .expect_err("redirect");
    assert_eq!(error.code(), "resource_metadata_unavailable");
    let error: AuthorizationDiscoveryError = discover(
        &mcp,
        &challenge(
            Some("http://203.0.113.9/.well-known/oauth-protected-resource"),
            &[],
        ),
    )
    .await
    .expect_err("insecure metadata location");
    assert_eq!(error.code(), "outbound_destination_rejected");
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn larger_provider_scope_sets_are_preserved_without_trimming() {
    let server = MockServer::start().await;
    let mcp = format!("{}/mcp", server.uri());
    let prm = format!("{}/.well-known/oauth-protected-resource/mcp", server.uri());
    for count in [83, 100] {
        server.reset().await;
        let scopes: Vec<String> = (0..count).map(|index| format!("scope{index:03}")).collect();
        mount_json(
            &server,
            "/.well-known/oauth-protected-resource/mcp",
            &json!({"resource": mcp, "authorization_servers": [server.uri()], "scopes_supported": scopes}),
        )
        .await;
        mount_json(
            &server,
            "/.well-known/oauth-authorization-server",
            &authorization_server(&server.uri(), &json!({})),
        )
        .await;
        let requirement = discover(&mcp, &challenge(Some(&prm), &[]))
            .await
            .expect("bounded provider scope set");
        assert_eq!(requirement.scopes(), scopes);
    }
}

#[tokio::test]
async fn an_invalid_first_issuer_falls_back_to_the_next_and_explains_a_total_failure() {
    let server = MockServer::start().await;
    let mcp = format!("{}/mcp", server.uri());
    let weak = format!("{}/weak", server.uri());
    let strong = format!("{}/strong", server.uri());
    mount_json(
        &server,
        "/.well-known/oauth-protected-resource/mcp",
        &json!({"resource": mcp, "authorization_servers": [weak, strong]}),
    )
    .await;
    mount_json(
        &server,
        "/.well-known/oauth-authorization-server/weak",
        &authorization_server(
            &weak,
            &json!({"code_challenge_methods_supported": ["plain"]}),
        ),
    )
    .await;
    mount_json(
        &server,
        "/.well-known/oauth-authorization-server/strong",
        &authorization_server(&strong, &json!({})),
    )
    .await;
    let requirement = discover(&mcp, &challenge(None, &["read"]))
        .await
        .expect("the second issuer validates");
    assert_eq!(requirement.issuer(), strong);

    server.reset().await;
    mount_json(
        &server,
        "/.well-known/oauth-protected-resource/mcp",
        &json!({"resource": mcp, "authorization_servers": [weak]}),
    )
    .await;
    mount_json(
        &server,
        "/.well-known/oauth-authorization-server/weak",
        &authorization_server(
            &weak,
            &json!({"code_challenge_methods_supported": ["plain"]}),
        ),
    )
    .await;
    let error = discover(&mcp, &challenge(None, &["read"]))
        .await
        .expect_err("no issuer validates");
    assert_eq!(error.code(), "pkce_unsupported");
}

#[tokio::test]
async fn dynamic_registration_yields_a_public_client_only() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/register"))
        .respond_with(|request: &wiremock::Request| {
            let body: Value = request.body_json().expect("registration request");
            assert_eq!(body["token_endpoint_auth_method"], "none");
            assert_eq!(
                body["redirect_uris"],
                json!(["https://reef.example/callback"])
            );
            assert_eq!(
                body["grant_types"],
                json!(["authorization_code", "refresh_token"])
            );
            assert_eq!(body["response_types"], json!(["code"]));
            ResponseTemplate::new(201).set_body_json(json!({
                "client_id": "registered-client", "client_id_issued_at": 1,
                "redirect_uris": ["https://reef.example/callback"],
                "token_endpoint_auth_method": "none"
            }))
        })
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/confidential"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "client_id": "confidential", "client_secret": "secret-canary"
        })))
        .mount(&server)
        .await;
    let client = register_client(
        loopback(),
        &format!("{}/register", server.uri()),
        "https://reef.example/callback",
    )
    .await
    .expect("registration");
    assert_eq!(client.client_id(), "registered-client");
    let error = register_client(
        loopback(),
        &format!("{}/confidential", server.uri()),
        "https://reef.example/callback",
    )
    .await
    .expect_err("confidential client");
    assert_eq!(error.code(), "client_bootstrap_failed");
    assert!(!format!("{error:?}").contains("secret-canary"));
}
