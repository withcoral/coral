//! Bounded MCP discovery for Provider Setup through the upstream Adapter's public seam.
#![allow(
    unused_crate_dependencies,
    reason = "integration targets inherit library dependencies"
)]
#![expect(clippy::indexing_slicing, reason = "assert known fixture JSON members")]

use coral_catalog::AuthenticationConfiguration;
use coral_catalog::{MaterializationOutcome, ProviderCredentialContract, ProviderSetupInput};
use coral_credentials::Secret;
use coral_outbound::DestinationPolicy;
use coral_upstream::{Error, discover_mcp};
use serde_json::{Value, json};
use wiremock::matchers::{body_partial_json, header, method};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const TOKEN: &str = "setup-bearer-canary";

fn loopback() -> DestinationPolicy {
    DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps)
}

/// Discovery against a loopback fixture without service headers.
async fn discover_catalog(
    url: &str,
    token: Option<&str>,
    contract: &ProviderCredentialContract,
) -> Result<coral_catalog::McpDiscoverySnapshot, Error> {
    let token = token.map(|token| Secret::new(token.into()).expect("fixture token"));
    discover_mcp(
        loopback(),
        url,
        token.as_ref(),
        contract,
        &std::collections::BTreeMap::new(),
    )
    .await
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": "2025-11-25",
        "capabilities": {"tools": {}},
        "serverInfo": {"name": "fixture", "title": "Fixture", "version": "1"}
    })
}

fn envelope(request: &Request, result: &Value) -> ResponseTemplate {
    let request: Value = request.body_json().expect("json-rpc request");
    ResponseTemplate::new(200).set_body_json(json!({
        "jsonrpc": "2.0", "id": request["id"], "result": result
    }))
}

async fn authenticated_server() -> MockServer {
    let server = MockServer::start().await;
    let bearer = format!("Bearer {TOKEN}");
    Mock::given(method("POST"))
        .and(header("authorization", bearer.as_str()))
        .and(body_partial_json(json!({"method": "initialize"})))
        .respond_with(|request: &Request| envelope(request, &initialize_result()))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(header("authorization", bearer.as_str()))
        .and(body_partial_json(
            json!({"method": "notifications/initialized"}),
        ))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(header("authorization", bearer.as_str()))
        .and(body_partial_json(json!({"method": "tools/list"})))
        .respond_with(|request: &Request| {
            let body: Value = request.body_json().expect("tools/list");
            let result = if body["params"]["cursor"] == json!("page-2") {
                json!({"tools": [{"name": "second", "inputSchema": {"type": "object"}}]})
            } else {
                json!({
                    "tools": [{"name": "first", "inputSchema": {"type": "object"},
                               "annotations": {"readOnlyHint": true}}],
                    "nextCursor": "page-2"
                })
            };
            envelope(request, &result)
        })
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(401).insert_header(
                "www-authenticate",
                r#"Bearer realm="OAuth", resource_metadata="https://fixture.example/.well-known/oauth-protected-resource/mcp", scope="read write", error="invalid_token""#,
            ),
        )
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn authenticated_discovery_pins_every_page_and_stamps_the_supplied_contract() {
    let server = authenticated_server().await;
    let url = format!("{}/mcp", server.uri());
    let snapshot = discover_catalog(
        &url,
        Some(TOKEN),
        &AuthenticationConfiguration::Bearer.into(),
    )
    .await
    .expect("authenticated discovery");
    let snapshot: Value = serde_json::from_str(snapshot.as_str()).unwrap();
    assert_eq!(snapshot["protocolVersion"], "2025-11-25");
    assert_eq!(snapshot["server"]["url"], url);
    assert_eq!(snapshot["server"]["transport"], "streamable_http");
    assert_eq!(
        snapshot["credentialContract"],
        json!({"authentication": {"method": "bearer"}, "base_urls": []})
    );
    let names: Vec<&str> = snapshot["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["first", "second"]);
    assert_eq!(snapshot["tools"][0]["annotations"]["readOnlyHint"], true);
    assert!(!snapshot.to_string().contains(TOKEN));
    let methods: Vec<String> = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|request| {
            request.body_json::<Value>().unwrap()["method"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert!(!methods.iter().any(|m| m == "tools/call"));
    assert_eq!(methods.iter().filter(|m| *m == "tools/list").count(), 2);
}

fn sse_envelope(request: &Request, result: &Value) -> ResponseTemplate {
    let request: Value = request.body_json().expect("json-rpc request");
    ResponseTemplate::new(200).set_body_raw(
        format!(
            "event: message\ndata: {}\n\n",
            json!({"jsonrpc": "2.0", "id": request["id"], "result": result})
        ),
        "text/event-stream",
    )
}

#[tokio::test]
async fn authenticated_sse_discovery_materializes_large_catalog_without_truncation() {
    let server = authenticated_server().await;
    let bearer = format!("Bearer {TOKEN}");
    Mock::given(method("POST"))
        .and(header("authorization", bearer.as_str()))
        .and(body_partial_json(json!({"method": "initialize"})))
        .respond_with(|request: &Request| sse_envelope(request, &initialize_result()))
        .with_priority(1)
        .mount(&server)
        .await;
    // Above both the former cap and a plausible replacement cap: only the byte
    // budget, not the number of Tools, bounds this small valid catalog.
    let tools: Vec<Value> = (0..1025)
        .map(|i| json!({"name": format!("operation_{i}"), "inputSchema": {"type": "object"}}))
        .collect();
    let result = json!({"tools": tools});
    Mock::given(method("POST"))
        .and(header("authorization", bearer.as_str()))
        .and(body_partial_json(json!({"method": "tools/list"})))
        .respond_with(move |request: &Request| sse_envelope(request, &result))
        .with_priority(1)
        .mount(&server)
        .await;
    let snapshot = discover_catalog(
        &format!("{}/mcp", server.uri()),
        Some(TOKEN),
        &AuthenticationConfiguration::Bearer.into(),
    )
    .await
    .expect("a byte-bounded catalog has no Tool-count cap");
    let pinned: Value = serde_json::from_str(snapshot.as_str()).unwrap();
    assert_eq!(pinned["tools"].as_array().unwrap().len(), 1025);
    assert!(!snapshot.as_str().contains(TOKEN));
    let MaterializationOutcome::Ready(prepared) =
        coral_catalog::materialize(loopback(), &ProviderSetupInput::from(snapshot), &[])
    else {
        panic!("every discovered Tool must also reach the materialized catalog");
    };
    assert_eq!(prepared.tools().len(), 1025);
    for i in 0..1025 {
        assert!(
            prepared
                .tools()
                .iter()
                .any(|operation| { operation.key().as_str() == format!("operation_{i}") })
        );
    }
}

#[tokio::test]
async fn discovery_still_rejects_an_oversized_sse_response() {
    let server = authenticated_server().await;
    let result = json!({"tools": [{
        "name": "oversized", "inputSchema": {"type": "object"},
        "description": "x".repeat(coral_catalog::MCP_DISCOVERY_SNAPSHOT_MAX_BYTES)
    }]});
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"method": "tools/list"})))
        .respond_with(move |request: &Request| sse_envelope(request, &result))
        .with_priority(1)
        .mount(&server)
        .await;
    let error = discover_catalog(
        &format!("{}/mcp", server.uri()),
        Some(TOKEN),
        &AuthenticationConfiguration::Bearer.into(),
    )
    .await
    .expect_err("the response byte budget still applies");
    assert!(matches!(error, Error::ResultTooLarge), "{error:?}");
}

#[tokio::test]
async fn discovery_still_bounds_an_endless_sequence_of_distinct_pages() {
    let server = authenticated_server().await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"method": "tools/list"})))
        .respond_with(|request: &Request| {
            let body: Value = request.body_json().expect("tools/list");
            let page = body["params"]["cursor"]
                .as_str()
                .map_or(0, |cursor| cursor.parse::<u32>().unwrap());
            sse_envelope(request, &json!({
                "tools": [{"name": format!("operation_{page}"), "inputSchema": {"type": "object"}}],
                "nextCursor": (page + 1).to_string()
            }))
        })
        .with_priority(1)
        .mount(&server)
        .await;
    let error = discover_catalog(
        &format!("{}/mcp", server.uri()),
        Some(TOKEN),
        &AuthenticationConfiguration::Bearer.into(),
    )
    .await
    .expect_err("the pagination budget still applies");
    assert!(matches!(error, Error::ResultTooLarge), "{error:?}");
}

#[tokio::test]
async fn unauthenticated_discovery_returns_the_parsed_bearer_challenge() {
    let server = authenticated_server().await;
    let url = format!("{}/mcp", server.uri());
    let error = discover_catalog(&url, None, &ProviderCredentialContract::NONE)
        .await
        .expect_err("challenge");
    let Error::McpUnauthorized(Some(challenge)) = error else {
        panic!("expected a typed Bearer challenge, got {error:?}");
    };
    assert_eq!(
        challenge.resource_metadata(),
        Some("https://fixture.example/.well-known/oauth-protected-resource/mcp")
    );
    assert_eq!(challenge.scopes(), ["read", "write"]);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].headers.get("authorization").is_none());
}

#[tokio::test]
async fn a_challenge_after_initialization_is_still_typed() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"method": "initialize"})))
        .respond_with(|request: &Request| envelope(request, &initialize_result()))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_partial_json(
            json!({"method": "notifications/initialized"}),
        ))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"method": "tools/list"})))
        .respond_with(ResponseTemplate::new(403).insert_header(
            "www-authenticate",
            r#"Bearer error="insufficient_scope", scope="read write admin""#,
        ))
        .mount(&server)
        .await;
    let error = discover_catalog(
        &format!("{}/mcp", server.uri()),
        Some(TOKEN),
        &AuthenticationConfiguration::Bearer.into(),
    )
    .await
    .expect_err("scope challenge");
    let Error::McpUnauthorized(Some(challenge)) = error else {
        panic!("expected a typed Bearer challenge, got {error:?}");
    };
    assert_eq!(challenge.resource_metadata(), None);
    assert_eq!(challenge.scopes(), ["read", "write", "admin"]);
}

#[tokio::test]
async fn an_unauthorized_answer_without_a_bearer_challenge_is_typed_as_unsupported() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(401).insert_header("www-authenticate", "Basic realm=\"x\""),
        )
        .mount(&server)
        .await;
    let error = discover_catalog(
        &format!("{}/mcp", server.uri()),
        None,
        &ProviderCredentialContract::NONE,
    )
    .await
    .expect_err("basic challenge");
    assert!(
        matches!(error, Error::McpAuthenticationUnsupported),
        "{error:?}"
    );
}

#[tokio::test]
async fn credential_free_discovery_stamps_no_credential_and_sends_no_header() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"method": "initialize"})))
        .respond_with(|request: &Request| envelope(request, &initialize_result()))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_partial_json(
            json!({"method": "notifications/initialized"}),
        ))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"method": "tools/list"})))
        .respond_with(|request: &Request| {
            envelope(
                request,
                &json!({"tools": [{"name": "only", "inputSchema": {"type": "object"}}]}),
            )
        })
        .mount(&server)
        .await;
    let snapshot = discover_catalog(
        &format!("{}/mcp", server.uri()),
        None,
        &ProviderCredentialContract::NONE,
    )
    .await
    .unwrap();
    let snapshot: Value = serde_json::from_str(snapshot.as_str()).unwrap();
    assert_eq!(
        snapshot["credentialContract"],
        json!({"authentication": {"method": "none"}, "base_urls": []})
    );
    assert_eq!(snapshot["tools"][0]["name"], "only");
    for request in server.received_requests().await.unwrap() {
        assert!(request.headers.get("authorization").is_none());
    }
}

#[tokio::test]
async fn discovery_sends_configured_service_headers_on_every_catalog_request() {
    let server = authenticated_server().await;
    let headers = std::collections::BTreeMap::from([("x-tenant".into(), "example".into())]);
    discover_mcp(
        loopback(),
        &format!("{}/mcp", server.uri()),
        Some(&Secret::new(TOKEN.into()).unwrap()),
        &AuthenticationConfiguration::Bearer.into(),
        &headers,
    )
    .await
    .unwrap();
    let requests = server.received_requests().await.unwrap();
    assert!(!requests.is_empty());
    for request in requests {
        assert_eq!(request.headers.get("x-tenant").unwrap(), "example");
    }
}
