#![allow(
    clippy::indexing_slicing,
    reason = "test code: assertion-style indexing and fixture buffer slicing are intentional"
)]

use std::collections::BTreeMap;
use std::fs;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64_URL_SAFE_NO_PAD;
use coral_api::v1::{ExecuteSqlRequest, SourceSecret, SourceVariable, ValidateSourceRequest};
use coral_client::{batches_to_json_rows, decode_execute_sql_response, default_workspace};
use serde_json::json;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::Notify;
use tonic::Request;

use crate::harness::{GrpcHarness, source_dir};

#[tokio::test]
async fn query_refreshes_expired_oauth_access_token_before_runtime_registration() {
    let fixture = RefreshingHttpFixture::new().await;
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            oauth_refresh_manifest_yaml(&fixture.base_url, &fixture.token_url),
            vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: fixture.base_url.clone(),
            }],
            vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "expired-token".to_string(),
            }],
        )
        .await;

    let secret_path = source_dir(harness.config_dir(), "refreshed_messages").join("secrets.env");
    fs::write(
        &secret_path,
        format!(
            "\
API_TOKEN=expired-token
__coral_oauth.QVBJX1RPS0VO.method=oauth
__coral_oauth.QVBJX1RPS0VO.access_token_expires_at={}
__coral_oauth.QVBJX1RPS0VO.refresh_token=stored-refresh-token
__coral_oauth.QVBJX1RPS0VO.client_id=stored-client
__coral_oauth.QVBJX1RPS0VO.token_url={}
",
            (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339(),
            fixture.token_url,
        ),
    )
    .expect("seed expired oauth material");

    let rows = harness
        .execute_sql_rows("SELECT id FROM refreshed_messages.messages")
        .await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "ok");
    assert_eq!(
        fixture.message_authorizations(),
        vec!["Bearer refreshed-token".to_string()]
    );
    let forms = fixture.token_forms();
    assert_eq!(forms.len(), 1);
    assert_eq!(
        forms[0].get("grant_type").map(String::as_str),
        Some("refresh_token")
    );
    assert_eq!(
        forms[0].get("refresh_token").map(String::as_str),
        Some("stored-refresh-token")
    );
    assert_eq!(
        forms[0].get("client_id").map(String::as_str),
        Some("stored-client")
    );

    let material = fs::read_to_string(secret_path).expect("read refreshed material");
    assert!(material.contains("API_TOKEN=refreshed-token"), "{material}");
    assert!(
        material.contains("__coral_oauth.QVBJX1RPS0VO.refresh_token=rotated-refresh-token"),
        "{material}"
    );
}

#[tokio::test]
async fn list_catalog_does_not_refresh_expired_oauth_access_token() {
    let fixture = RefreshingHttpFixture::new().await;
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            oauth_refresh_manifest_yaml(&fixture.base_url, &fixture.token_url),
            vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: fixture.base_url.clone(),
            }],
            vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "expired-token".to_string(),
            }],
        )
        .await;

    let secret_path = source_dir(harness.config_dir(), "refreshed_messages").join("secrets.env");
    fs::write(
        &secret_path,
        format!(
            "\
API_TOKEN=expired-token
__coral_oauth.QVBJX1RPS0VO.method=oauth
__coral_oauth.QVBJX1RPS0VO.access_token_expires_at={}
__coral_oauth.QVBJX1RPS0VO.refresh_token=stored-refresh-token
__coral_oauth.QVBJX1RPS0VO.client_id=stored-client
__coral_oauth.QVBJX1RPS0VO.token_url={}
",
            (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339(),
            fixture.token_url,
        ),
    )
    .expect("seed expired oauth material");

    let tables = harness.list_tables().await;

    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].name, "messages");
    assert!(
        fixture.token_forms().is_empty(),
        "passive catalog discovery should not call the token endpoint"
    );
    let material = fs::read_to_string(secret_path).expect("read material");
    assert!(material.contains("API_TOKEN=expired-token"), "{material}");
}

#[tokio::test]
async fn concurrent_queries_share_one_expired_oauth_refresh() {
    let fixture = RefreshingHttpFixture::new().await;
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            oauth_refresh_manifest_yaml(&fixture.base_url, &fixture.token_url),
            vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: fixture.base_url.clone(),
            }],
            vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "expired-token".to_string(),
            }],
        )
        .await;

    let secret_path = source_dir(harness.config_dir(), "refreshed_messages").join("secrets.env");
    fs::write(
        &secret_path,
        format!(
            "\
API_TOKEN=expired-token
__coral_oauth.QVBJX1RPS0VO.method=oauth
__coral_oauth.QVBJX1RPS0VO.access_token_expires_at={}
__coral_oauth.QVBJX1RPS0VO.refresh_token=stored-refresh-token
__coral_oauth.QVBJX1RPS0VO.client_id=stored-client
__coral_oauth.QVBJX1RPS0VO.token_url={}
",
            (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339(),
            fixture.token_url,
        ),
    )
    .expect("seed expired oauth material");

    let (first, second) = tokio::join!(
        harness.execute_sql_rows("SELECT id FROM refreshed_messages.messages"),
        harness.execute_sql_rows("SELECT id FROM refreshed_messages.messages"),
    );

    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    let forms = fixture.token_forms();
    assert_eq!(
        forms.len(),
        1,
        "only one request should spend the rotating refresh token: {forms:?}"
    );
    assert_eq!(
        fixture.message_authorizations(),
        vec![
            "Bearer refreshed-token".to_string(),
            "Bearer refreshed-token".to_string()
        ]
    );
}

#[tokio::test]
async fn refresh_does_not_overwrite_concurrent_manual_credential_replacement() {
    let fixture = RefreshingHttpFixture::new_blocked_token_response().await;
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            oauth_refresh_manifest_yaml(&fixture.base_url, &fixture.token_url),
            vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: fixture.base_url.clone(),
            }],
            vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "expired-token".to_string(),
            }],
        )
        .await;

    let secret_path = source_dir(harness.config_dir(), "refreshed_messages").join("secrets.env");
    fs::write(
        &secret_path,
        format!(
            "\
API_TOKEN=expired-token
__coral_oauth.QVBJX1RPS0VO.method=oauth
__coral_oauth.QVBJX1RPS0VO.access_token_expires_at={}
__coral_oauth.QVBJX1RPS0VO.refresh_token=stored-refresh-token
__coral_oauth.QVBJX1RPS0VO.client_id=stored-client
__coral_oauth.QVBJX1RPS0VO.token_url={}
",
            (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339(),
            fixture.token_url,
        ),
    )
    .expect("seed expired oauth material");

    let mut query_client = harness.query_client();
    let query = tokio::spawn(async move {
        let response = query_client
            .execute_sql(Request::new(ExecuteSqlRequest {
                workspace: Some(default_workspace()),
                sql: "SELECT id FROM refreshed_messages.messages".to_string(),
            }))
            .await
            .expect("execute sql")
            .into_inner();
        let result = decode_execute_sql_response(&response).expect("decode execute response");
        batches_to_json_rows(result.batches()).expect("json rows")
    });

    fixture.wait_for_token_request().await;
    harness
        .import_source(
            oauth_refresh_manifest_yaml(&fixture.base_url, &fixture.token_url),
            vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: fixture.base_url.clone(),
            }],
            vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "manual-token".to_string(),
            }],
        )
        .await;
    fixture.allow_token_response();

    let rows = query.await.expect("query task");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "ok");
    assert_eq!(
        fixture.message_authorizations(),
        vec!["Bearer manual-token".to_string()]
    );
    let material = fs::read_to_string(secret_path).expect("read material");
    assert!(material.contains("API_TOKEN=manual-token"), "{material}");
    assert!(
        !material.contains("API_TOKEN=refreshed-token"),
        "stale refresh should not overwrite manual replacement: {material}"
    );
}

#[tokio::test]
async fn successful_refresh_is_persisted_before_later_oauth_input_failure() {
    let fixture = RefreshingHttpFixture::new().await;
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            two_oauth_inputs_manifest_yaml(&fixture.base_url, &fixture.token_url),
            vec![SourceVariable {
                key: "API_BASE".to_string(),
                value: fixture.base_url.clone(),
            }],
            vec![
                SourceSecret {
                    key: "API_TOKEN".to_string(),
                    value: "expired-primary-token".to_string(),
                },
                SourceSecret {
                    key: "SECOND_TOKEN".to_string(),
                    value: "expired-secondary-token".to_string(),
                },
            ],
        )
        .await;

    let secret_path = source_dir(harness.config_dir(), "multi_oauth_messages").join("secrets.env");
    let primary_prefix = oauth_metadata_prefix("API_TOKEN");
    let secondary_prefix = oauth_metadata_prefix("SECOND_TOKEN");
    fs::write(
        &secret_path,
        format!(
            "\
API_TOKEN=expired-primary-token
SECOND_TOKEN=expired-secondary-token
{primary_prefix}method=oauth
{primary_prefix}access_token_expires_at={}
{primary_prefix}refresh_token=stored-primary-refresh-token
{primary_prefix}client_id=stored-client
{primary_prefix}token_url={}
{secondary_prefix}method=oauth
{secondary_prefix}access_token_expires_at={}
{secondary_prefix}refresh_token=stored-secondary-refresh-token
{secondary_prefix}client_id=stored-client
{secondary_prefix}token_url={}/token-fail
",
            (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339(),
            fixture.token_url,
            (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339(),
            fixture.base_url,
        ),
    )
    .expect("seed expired oauth material");

    harness
        .source_client()
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name: "multi_oauth_messages".to_string(),
        }))
        .await
        .expect_err("second OAuth refresh should fail validation");

    let forms = fixture.token_forms();
    assert!(
        forms.iter().any(|form| {
            form.get("refresh_token").map(String::as_str) == Some("stored-secondary-refresh-token")
        }),
        "second failing refresh should be attempted: {forms:?}"
    );

    let material = fs::read_to_string(secret_path).expect("read partially refreshed material");
    assert!(
        material.contains("API_TOKEN=refreshed-token"),
        "first refresh should be durable even when a later refresh fails: {material}"
    );
    assert!(
        material.contains(&format!(
            "{primary_prefix}refresh_token=rotated-refresh-token"
        )),
        "rotated refresh token should be durable even when a later refresh fails: {material}"
    );
    assert!(
        material.contains("SECOND_TOKEN=expired-secondary-token"),
        "failed second refresh should not overwrite its source secret: {material}"
    );
}

fn oauth_refresh_manifest_yaml(base_url: &str, token_url: &str) -> String {
    serde_yaml::to_string(&json!({
        "name": "refreshed_messages",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "inputs": {
            "API_BASE": { "kind": "variable" },
            "API_TOKEN": {
                "kind": "secret",
                "credential": {
                    "methods": [{
                        "type": "oauth",
                        "oauth": {
                            "flow": {
                                "type": "authorization_code",
                                "pkce": "disabled",
                            },
                            "redirect_uri": "http://127.0.0.1:53682/oauth/callback",
                            "redirect_uri_port_mode": "fixed",
                            "endpoints": {
                                "authorization_url": "https://provider.example.test/oauth/authorize",
                                "token_url": token_url,
                            },
                            "client": {
                                "id": { "default": "manifest-client" },
                            },
                        },
                    }],
                },
            },
        },
        "base_url": base_url,
        "auth": {
            "type": "HeaderAuth",
            "headers": [{
                "name": "Authorization",
                "from": "template",
                "template": "Bearer {{input.API_TOKEN}}",
            }],
        },
        "tables": [{
            "name": "messages",
            "description": "Messages behind an OAuth access token",
            "request": {
                "method": "GET",
                "path": "/messages",
            },
            "response": {
                "rows_path": ["data"],
            },
            "columns": [
                {"name": "id", "type": "Utf8"},
            ],
        }],
    }))
    .expect("serialize manifest")
}

fn two_oauth_inputs_manifest_yaml(base_url: &str, token_url: &str) -> String {
    serde_yaml::to_string(&json!({
        "name": "multi_oauth_messages",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "inputs": {
            "API_BASE": { "kind": "variable" },
            "API_TOKEN": oauth_input(token_url),
            "SECOND_TOKEN": oauth_input(&format!("{base_url}/token-fail")),
        },
        "base_url": base_url,
        "auth": {
            "type": "HeaderAuth",
            "headers": [{
                "name": "Authorization",
                "from": "template",
                "template": "Bearer {{input.API_TOKEN}}",
            }],
        },
        "tables": [{
            "name": "messages",
            "description": "Messages behind multiple OAuth inputs",
            "request": {
                "method": "GET",
                "path": "/messages",
            },
            "response": {
                "rows_path": ["data"],
            },
            "columns": [
                {"name": "id", "type": "Utf8"},
            ],
        }],
    }))
    .expect("serialize manifest")
}

fn oauth_input(token_url: &str) -> serde_json::Value {
    json!({
        "kind": "secret",
        "credential": {
            "methods": [{
                "type": "oauth",
                "oauth": {
                    "flow": {
                        "type": "authorization_code",
                        "pkce": "disabled",
                    },
                    "redirect_uri": "http://127.0.0.1:53682/oauth/callback",
                    "redirect_uri_port_mode": "fixed",
                    "endpoints": {
                        "authorization_url": "https://provider.example.test/oauth/authorize",
                        "token_url": token_url,
                    },
                    "client": {
                        "id": { "default": "manifest-client" },
                    },
                },
            }],
        },
    })
}

fn oauth_metadata_prefix(input_key: &str) -> String {
    format!(
        "__coral_oauth.{}.",
        BASE64_URL_SAFE_NO_PAD.encode(input_key.as_bytes())
    )
}

struct RefreshingHttpFixture {
    base_url: String,
    token_url: String,
    token_forms: Arc<Mutex<Vec<BTreeMap<String, String>>>>,
    message_authorizations: Arc<Mutex<Vec<String>>>,
    token_request_seen: Arc<Notify>,
    token_response_gate: Option<Arc<Notify>>,
    task: tokio::task::JoinHandle<()>,
}

impl RefreshingHttpFixture {
    async fn new() -> Self {
        Self::new_with_token_response_gate(None).await
    }

    async fn new_blocked_token_response() -> Self {
        Self::new_with_token_response_gate(Some(Arc::new(Notify::new()))).await
    }

    async fn new_with_token_response_gate(token_response_gate: Option<Arc<Notify>>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind oauth refresh fixture");
        let addr = listener.local_addr().expect("fixture addr");
        let token_forms = Arc::new(Mutex::new(Vec::new()));
        let message_authorizations = Arc::new(Mutex::new(Vec::new()));
        let token_request_seen = Arc::new(Notify::new());
        let task_token_forms = Arc::clone(&token_forms);
        let task_message_authorizations = Arc::clone(&message_authorizations);
        let task_token_request_seen = Arc::clone(&token_request_seen);
        let task_token_response_gate = token_response_gate.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let token_forms = Arc::clone(&task_token_forms);
                let message_authorizations = Arc::clone(&task_message_authorizations);
                let token_request_seen = Arc::clone(&task_token_request_seen);
                let token_response_gate = task_token_response_gate.clone();
                tokio::spawn(async move {
                    let request = read_http_request(&mut socket).await;
                    match request.path.as_str() {
                        "/token" => {
                            token_forms
                                .lock()
                                .expect("token forms")
                                .push(request.form());
                            token_request_seen.notify_one();
                            if let Some(gate) = token_response_gate {
                                gate.notified().await;
                            }
                            write_json_response(
                                &mut socket,
                                "200 OK",
                                r#"{"access_token":"refreshed-token","refresh_token":"rotated-refresh-token","token_type":"Bearer","expires_in":3600}"#,
                            )
                            .await;
                        }
                        "/token-fail" => {
                            token_forms
                                .lock()
                                .expect("token forms")
                                .push(request.form());
                            write_json_response(
                                &mut socket,
                                "500 Internal Server Error",
                                r#"{"error":"refresh failed"}"#,
                            )
                            .await;
                        }
                        "/messages" => {
                            if let Some(authorization) = request.header("authorization") {
                                message_authorizations
                                    .lock()
                                    .expect("message authorizations")
                                    .push(authorization.to_string());
                            }
                            write_json_response(&mut socket, "200 OK", r#"{"data":[{"id":"ok"}]}"#)
                                .await;
                        }
                        _ => {
                            write_json_response(
                                &mut socket,
                                "404 Not Found",
                                r#"{"error":"not found"}"#,
                            )
                            .await;
                        }
                    }
                });
            }
        });
        Self {
            base_url: format!("http://{addr}"),
            token_url: format!("http://{addr}/token"),
            token_forms,
            message_authorizations,
            token_request_seen,
            token_response_gate,
            task,
        }
    }

    async fn wait_for_token_request(&self) {
        self.token_request_seen.notified().await;
    }

    fn allow_token_response(&self) {
        if let Some(gate) = &self.token_response_gate {
            gate.notify_one();
        }
    }

    fn token_forms(&self) -> Vec<BTreeMap<String, String>> {
        self.token_forms.lock().expect("token forms").clone()
    }

    fn message_authorizations(&self) -> Vec<String> {
        self.message_authorizations
            .lock()
            .expect("message authorizations")
            .clone()
    }
}

impl Drop for RefreshingHttpFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct HttpRequest {
    path: String,
    headers: BTreeMap<String, String>,
    body: String,
}

impl HttpRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }

    fn form(&self) -> BTreeMap<String, String> {
        url::form_urlencoded::parse(self.body.as_bytes())
            .into_owned()
            .collect()
    }
}

async fn read_http_request(socket: &mut tokio::net::TcpStream) -> HttpRequest {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    let header_end = loop {
        let read = socket.read(&mut chunk).await.expect("read fixture request");
        assert!(read > 0, "fixture request closed before headers");
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(index) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers_raw = String::from_utf8_lossy(&buffer[..header_end]).into_owned();
    let content_length = headers_raw
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find_map(|(name, value)| {
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    while buffer.len() < header_end + content_length {
        let read = socket.read(&mut chunk).await.expect("read fixture body");
        assert!(read > 0, "fixture request closed before body");
        buffer.extend_from_slice(&chunk[..read]);
    }
    let mut lines = headers_raw.lines();
    let path = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .expect("fixture request path")
        .to_string();
    let headers = lines
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    let body =
        String::from_utf8_lossy(&buffer[header_end..header_end + content_length]).into_owned();
    HttpRequest {
        path,
        headers,
        body,
    }
}

async fn write_json_response(socket: &mut tokio::net::TcpStream, status: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    socket
        .write_all(response.as_bytes())
        .await
        .expect("write fixture response");
    socket.shutdown().await.expect("shutdown fixture response");
}
