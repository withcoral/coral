//! Bounded setup discovery: initialize and every `tools/list` page, nothing else.
//! Raw tool definitions survive SDK decoding, and an authentication challenge is
//! returned as typed evidence instead of being collapsed into a request failure.

use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};

use rmcp::model::{ClientConfig, PaginatedRequestParams};
use rmcp::service::RunningService;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::common::client_side_sse::NeverRetry;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::{RoleClient, ServiceExt as _};
use serde_json::{Value, json};

use super::transport::{Captured, McpHttpClient};
use crate::Error;

const DISCOVERY_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_DISCOVERY_PAGES: usize = 64;
const MAX_SCOPE_BYTES: usize = 256;
const MAX_RESOURCE_METADATA_BYTES: usize = 2048;

/// Safe facts parsed from one `WWW-Authenticate: Bearer` challenge.
///
/// Only the protected-resource metadata location and the requested scope set are
/// retained; realm, error, and error description never leave the Adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BearerChallenge {
    resource_metadata: Option<String>,
    scopes: Vec<String>,
}

impl BearerChallenge {
    /// Returns the advertised protected-resource metadata URL, if any.
    #[must_use]
    pub fn resource_metadata(&self) -> Option<&str> {
        self.resource_metadata.as_deref()
    }

    /// Returns the challenge's scope set in the order the server sent it.
    #[must_use]
    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }

    pub(super) const fn new(resource_metadata: Option<String>, scopes: Vec<String>) -> Self {
        Self {
            resource_metadata,
            scopes,
        }
    }

    /// Parses the first Bearer challenge from one or more header values.
    pub(super) fn parse(headers: &[String]) -> Option<Self> {
        headers
            .iter()
            .flat_map(|value| split_challenges(value))
            .find_map(|(scheme, params)| {
                scheme
                    .eq_ignore_ascii_case("bearer")
                    .then(|| Self::from_params(&params))
            })
    }

    fn from_params(params: &[(String, String)]) -> Self {
        let resource_metadata = params
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("resource_metadata"))
            .map(|(_, value)| value.clone())
            .filter(|value| {
                value.len() <= MAX_RESOURCE_METADATA_BYTES && url::Url::parse(value).is_ok()
            });
        let scopes = params
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("scope"))
            .map(|(_, value)| {
                value
                    .split(' ')
                    .filter(|scope| !scope.is_empty())
                    .filter(|scope| scope.len() <= MAX_SCOPE_BYTES && is_scope_token(scope))
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        Self {
            resource_metadata,
            scopes,
        }
    }
}

fn is_scope_token(scope: &str) -> bool {
    scope
        .bytes()
        .all(|byte| byte == b'!' || (b'#'..=b'[').contains(&byte) || (b']'..=b'~').contains(&byte))
}

/// Splits one header value into `(scheme, params)` challenges per RFC 9110 §11.6.1.
fn split_challenges(value: &str) -> Vec<(String, Vec<(String, String)>)> {
    let mut challenges: Vec<(String, Vec<(String, String)>)> = Vec::new();
    let mut rest = skip_separators(value);
    while !rest.is_empty() {
        let (scheme, after_scheme) = take_token(rest);
        if scheme.is_empty() {
            // Skip an unparseable character rather than loop forever.
            rest = rest
                .char_indices()
                .nth(1)
                .and_then(|(index, _)| rest.get(index..))
                .unwrap_or("");
            continue;
        }
        if let Some(after_equals) = after_scheme.strip_prefix('=') {
            // A bare `name=value` outside a scheme belongs to the previous challenge.
            let (parsed, remaining) = parse_value(after_equals);
            if let Some((_, params)) = challenges.last_mut() {
                params.push((scheme.to_owned(), parsed));
            }
            rest = skip_separators(remaining);
            continue;
        }
        let mut params = Vec::new();
        rest = after_scheme.trim_start_matches([' ', '\t']);
        loop {
            rest = skip_separators(rest);
            let (name, after_name) = take_token(rest);
            let Some(after_equals) = after_name.strip_prefix('=') else {
                break;
            };
            if name.is_empty() {
                break;
            }
            let (parsed, remaining) = parse_value(after_equals);
            params.push((name.to_owned(), parsed));
            rest = remaining;
        }
        challenges.push((scheme.to_owned(), params));
        rest = skip_separators(rest);
    }
    challenges
}

fn skip_separators(input: &str) -> &str {
    input.trim_start_matches([' ', '\t', ','])
}

fn take_token(input: &str) -> (&str, &str) {
    let end = input
        .find(|character: char| !is_token_character(character))
        .unwrap_or(input.len());
    input.split_at_checked(end).unwrap_or((input, ""))
}

fn parse_value(input: &str) -> (String, &str) {
    if let Some(quoted) = input.strip_prefix('"') {
        let mut value = String::new();
        let mut characters = quoted.char_indices();
        while let Some((index, character)) = characters.next() {
            match character {
                '\\' => {
                    if let Some((_, escaped)) = characters.next() {
                        value.push(escaped);
                    }
                }
                '"' => {
                    return (value, quoted.get(index.saturating_add(1)..).unwrap_or(""));
                }
                other => value.push(other),
            }
        }
        (value, "")
    } else {
        let (token, remaining) = take_token(input);
        (token.to_owned(), remaining)
    }
}

fn is_token_character(character: char) -> bool {
    character.is_ascii_alphanumeric()
        || matches!(
            character,
            '!' | '#'
                | '$'
                | '%'
                | '&'
                | '\''
                | '*'
                | '+'
                | '-'
                | '.'
                | '^'
                | '_'
                | '`'
                | '|'
                | '~'
        )
}

/// Captures one exact bounded Streamable HTTP catalog for Provider Setup.
///
/// The credential, when supplied, is sent only to `url` and never embedded in the
/// snapshot. The caller supplies the secret-free credential contract to stamp.
///
/// # Errors
/// Returns [`Error::McpUnauthorized`] when the server answers with an
/// authentication challenge, and request, protocol, size, or malformed-discovery
/// failures otherwise. Discovery never invokes a tool or reads a resource.
pub async fn discover_mcp(
    policy: coral_outbound::DestinationPolicy,
    url: &str,
    credential: Option<&coral_credentials::Secret>,
    contract: &coral_catalog::ProviderCredentialContract,
    request_headers: &std::collections::BTreeMap<String, String>,
) -> Result<coral_catalog::McpDiscoverySnapshot, Error> {
    tokio::time::timeout(
        DISCOVERY_DEADLINE,
        discover(policy, url, credential, contract, request_headers),
    )
    .await
    .map_err(|_deadline| Error::McpRequest)?
}

async fn discover(
    policy: coral_outbound::DestinationPolicy,
    url: &str,
    credential: Option<&coral_credentials::Secret>,
    contract: &coral_catalog::ProviderCredentialContract,
    request_headers: &std::collections::BTreeMap<String, String>,
) -> Result<coral_catalog::McpDiscoverySnapshot, Error> {
    let limit = coral_catalog::MCP_DISCOVERY_SNAPSHOT_MAX_BYTES;
    let captured: Arc<Mutex<Option<Captured>>> = Arc::new(Mutex::new(None));
    let mut config = StreamableHttpClientTransportConfig::with_uri(url.to_owned())
        .reinit_on_expired_session(false)
        .max_sse_event_size(limit);
    config.retry_config = Arc::new(NeverRetry::default());
    config.auth_header = credential.map(|secret| secret.expose().to_owned());
    let transport = StreamableHttpClientTransport::with_client(
        McpHttpClient {
            request_headers: request_headers.clone(),
            client: crate::http_client(policy)?,
            limit,
            result: captured.clone(),
            // Discovery is not a Provider Tool dispatch: it sends no trace context.
            span: tracing::Span::none(),
        },
        config,
    );
    let mut info = ClientConfig::default();
    info.protocol_version = serde_json::from_value(json!("2025-11-25"))
        .map_err(|_invalid_version| Error::McpProtocolChanged)?;
    let mut client = match info.serve(transport).await {
        Ok(client) => client,
        Err(_initialization) => return Err(failure(&captured)),
    };
    let result = async {
        let initialization = take(&captured)?;
        if !matches!(
            initialization
                .get("protocolVersion")
                .and_then(Value::as_str),
            Some("2025-03-26" | "2025-06-18" | "2025-11-25")
        ) {
            return Err(Error::McpProtocolChanged);
        }
        let mut tools = Vec::new();
        if initialization.pointer("/capabilities/tools").is_some() {
            visit_tool_pages(&client, &captured, |page| {
                tools.extend(page);
                ControlFlow::Continue(())
            })
            .await?;
        }
        snapshot(initialization, tools, url, contract)
    }
    .await;
    let _closed = client.close().await;
    result
}

fn failure(slot: &Mutex<Option<Captured>>) -> Error {
    match slot.lock().ok().and_then(|mut slot| slot.take()) {
        Some(captured) => captured.into_result().err().unwrap_or(Error::McpRequest),
        None => Error::McpRequest,
    }
}

/// Walks every `tools/list` page in order and hands each page's tools to
/// `visit`, which can end the walk early.
pub(super) async fn visit_tool_pages(
    client: &RunningService<RoleClient, ClientConfig>,
    captured: &Mutex<Option<Captured>>,
    mut visit: impl FnMut(Vec<Value>) -> ControlFlow<()>,
) -> Result<(), Error> {
    let mut cursor: Option<String> = None;
    let mut seen = std::collections::HashSet::new();
    loop {
        let params = cursor.take().map(|cursor| {
            let mut params = PaginatedRequestParams::default();
            params.cursor = Some(cursor);
            params
        });
        if client.list_tools(params).await.is_err() {
            return Err(failure(captured));
        }
        let page = take(captured)?;
        let tools = page
            .get("tools")
            .and_then(Value::as_array)
            .ok_or(Error::McpResult)?
            .clone();
        if visit(tools).is_break() {
            return Ok(());
        }
        if seen.len() >= MAX_DISCOVERY_PAGES {
            return Err(Error::ResultTooLarge);
        }
        cursor = match page.get("nextCursor") {
            None => None,
            Some(Value::String(next)) if !next.is_empty() && seen.insert(next.clone()) => {
                Some(next.clone())
            }
            Some(_) => return Err(Error::McpResult),
        };
        if cursor.is_none() {
            return Ok(());
        }
    }
}

fn snapshot(
    mut initialization: Value,
    tools: Vec<Value>,
    url: &str,
    contract: &coral_catalog::ProviderCredentialContract,
) -> Result<coral_catalog::McpDiscoverySnapshot, Error> {
    let root = initialization.as_object_mut().ok_or(Error::McpResult)?;
    root.insert(
        "server".into(),
        json!({"url": url, "transport": "streamable_http"}),
    );
    root.insert("tools".into(), Value::Array(tools));
    root.insert(
        "credentialContract".into(),
        serde_json::to_value(contract).map_err(Error::EncodeJson)?,
    );
    let text = serde_json::to_string(&initialization).map_err(Error::EncodeJson)?;
    coral_catalog::McpDiscoverySnapshot::parse(&text).map_err(|_invalid| Error::ResultTooLarge)
}

fn take(slot: &Mutex<Option<Captured>>) -> Result<Value, Error> {
    slot.lock()
        .map_err(|_poisoned| Error::McpResult)?
        .take()
        .ok_or(Error::McpResult)?
        .into_result()
}

#[cfg(test)]
mod tests {
    use super::BearerChallenge;

    #[test]
    fn parses_quoted_and_bare_parameters_across_several_challenges() {
        let parsed = BearerChallenge::parse(&[
            r#"Basic realm="x", Bearer realm="OAuth", resource_metadata="https://a.example/.well-known/oauth-protected-resource", scope="read write", error="invalid_token""#.to_owned(),
        ])
        .expect("bearer challenge");
        assert_eq!(
            parsed.resource_metadata(),
            Some("https://a.example/.well-known/oauth-protected-resource")
        );
        assert_eq!(parsed.scopes(), ["read", "write"]);
    }

    #[test]
    fn bare_scheme_and_missing_parameters_are_accepted() {
        let parsed = BearerChallenge::parse(&["bearer".to_owned()]).expect("bare bearer");
        assert_eq!(parsed.resource_metadata(), None);
        assert!(parsed.scopes().is_empty());
        assert!(BearerChallenge::parse(&["Digest realm=\"x\"".to_owned()]).is_none());
    }

    #[test]
    fn invalid_metadata_urls_and_scope_tokens_are_dropped() {
        let parsed = BearerChallenge::parse(&[
            "Bearer resource_metadata=\"not a url\", scope=\"ok bad\\\"quote ok2\"".to_owned(),
        ])
        .expect("bearer");
        assert_eq!(parsed.resource_metadata(), None);
        assert_eq!(parsed.scopes(), ["ok", "ok2"]);
    }
}
