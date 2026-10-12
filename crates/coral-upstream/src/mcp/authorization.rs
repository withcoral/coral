//! MCP authorization discovery per the 2025-11-25 authorization specification:
//! protected-resource metadata, RFC 8414 and `OpenID` authorization-server
//! discovery with issuer-path rules, and Dynamic Client Registration. Every
//! answer is untrusted input: relationships are validated, nothing is followed,
//! no credential is sent, and every request is bounded.

use serde::Deserialize;
use serde_json::Value;

use super::BearerChallenge;

const METADATA_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const DISCOVERY_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);
const METADATA_MAX_BYTES: usize = 64 * 1024;
const MAX_METADATA_REQUESTS: usize = 6;
// Match the recipe scope limit and accommodate Zoho CRM's 83 advertised scopes.
const MAX_SCOPES: usize = 100;

/// Safe, typed reason authorization discovery or registration failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationDiscoveryError {
    /// A metadata or registration destination is forbidden by deployment policy.
    DestinationRejected,
    /// Every protected-resource location explicitly reported no metadata.
    ResourceMetadataMissing,
    /// Protected-resource metadata was absent, malformed, redirected, or oversized.
    ResourceMetadataUnavailable,
    /// The protected resource is neither the MCP server URL nor an ancestor of it.
    ResourceMismatch,
    /// No advertised authorization server published valid metadata for its issuer.
    AuthorizationServerUnavailable,
    /// An authorization server's metadata names a different issuer than advertised.
    IssuerMismatch,
    /// The selected authorization server does not advertise S256 PKCE.
    PkceUnsupported,
    /// A metadata location or endpoint is not HTTPS or an explicit loopback.
    InsecureEndpoint,
    /// Dynamic Client Registration failed or issued a confidential client.
    ClientBootstrapFailed,
}

impl AuthorizationDiscoveryError {
    /// Stable `snake_case` code for diagnostics.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::DestinationRejected => "outbound_destination_rejected",
            Self::ResourceMetadataMissing => "resource_metadata_missing",
            Self::ResourceMetadataUnavailable => "resource_metadata_unavailable",
            Self::ResourceMismatch => "resource_mismatch",
            Self::AuthorizationServerUnavailable => "authorization_server_unavailable",
            Self::IssuerMismatch => "issuer_mismatch",
            Self::PkceUnsupported => "pkce_unsupported",
            Self::InsecureEndpoint => "insecure_endpoint",
            Self::ClientBootstrapFailed => "client_bootstrap_failed",
        }
    }
}

/// Authority-bearing authorization facts discovered for one MCP server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationRequirement {
    resource: String,
    issuer: String,
    authorization_endpoint: Option<String>,
    token_endpoint: String,
    device_authorization_endpoint: Option<String>,
    registration_endpoint: Option<String>,
    client_id_metadata_document_supported: bool,
    scopes: Vec<String>,
}

impl AuthorizationRequirement {
    /// Canonical MCP `resource` sent in authorization and token requests.
    #[must_use]
    pub fn resource(&self) -> &str {
        &self.resource
    }

    /// Selected authorization server issuer.
    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Validated authorization endpoint.
    #[must_use]
    pub fn authorization_endpoint(&self) -> Option<&str> {
        self.authorization_endpoint.as_deref()
    }

    /// Validated device authorization endpoint, when advertised.
    #[must_use]
    pub fn device_authorization_endpoint(&self) -> Option<&str> {
        self.device_authorization_endpoint.as_deref()
    }

    /// Validated token endpoint.
    #[must_use]
    pub fn token_endpoint(&self) -> &str {
        &self.token_endpoint
    }

    /// Validated Dynamic Client Registration endpoint, when advertised.
    #[must_use]
    pub fn registration_endpoint(&self) -> Option<&str> {
        self.registration_endpoint.as_deref()
    }

    /// Whether the issuer accepts Client ID Metadata Documents.
    #[must_use]
    pub const fn client_id_metadata_document_supported(&self) -> bool {
        self.client_id_metadata_document_supported
    }

    /// Exact sorted scope request: the challenge's scopes, else the resource's.
    #[must_use]
    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }
}

/// A public client registered dynamically for one Setup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredClient {
    client_id: String,
    client_id_issued_at: Option<u64>,
}

impl RegisteredClient {
    /// Issued client identifier.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Issuance time reported by the authorization server, for diagnostics only.
    #[must_use]
    pub const fn client_id_issued_at(&self) -> Option<u64> {
        self.client_id_issued_at
    }
}

impl BearerChallenge {
    /// Builds a challenge from already-parsed facts, for callers that hold them.
    #[must_use]
    pub fn from_parts(resource_metadata: Option<String>, scopes: Vec<String>) -> Self {
        Self::new(resource_metadata, scopes)
    }
}

/// Discovers the authorization requirement for one MCP server from its challenge.
///
/// # Errors
/// Returns a typed, safe [`AuthorizationDiscoveryError`]; no upstream body,
/// header, or complete URL is carried.
pub async fn discover_authorization(
    policy: coral_outbound::DestinationPolicy,
    mcp_url: &str,
    challenge: &BearerChallenge,
    request_headers: &std::collections::BTreeMap<String, String>,
) -> Result<AuthorizationRequirement, AuthorizationDiscoveryError> {
    tokio::time::timeout(
        DISCOVERY_DEADLINE,
        discover(policy, mcp_url, challenge, request_headers),
    )
    .await
    .map_err(|_deadline| AuthorizationDiscoveryError::ResourceMetadataUnavailable)?
}

async fn discover(
    policy: coral_outbound::DestinationPolicy,
    mcp_url: &str,
    challenge: &BearerChallenge,
    request_headers: &std::collections::BTreeMap<String, String>,
) -> Result<AuthorizationRequirement, AuthorizationDiscoveryError> {
    // A server query selects the endpoint, not the protected resource.
    let mut server =
        url::Url::parse(mcp_url).map_err(|_error| AuthorizationDiscoveryError::ResourceMismatch)?;
    server.set_query(None);
    let server = canonical_resource(server.as_str())?;
    let client = Fetcher::new(policy)?;
    let protected = fetch_protected_resource(&client, &server, challenge, request_headers).await?;
    // The declared identifier is the one the authorization server knows, so
    // it is the one requested.
    let resource = canonical_resource(&protected.resource)?;
    if !covers(&resource, &server) {
        return Err(AuthorizationDiscoveryError::ResourceMismatch);
    }
    // The first advertised issuer whose metadata is fetched and validates is
    // selected; an invalid issuer disqualifies itself and the next one is tried.
    // When none validates, the first failure explains why.
    let mut selected = None;
    let mut first_failure = None;
    for issuer in &protected.authorization_servers {
        match validated_issuer(&client, issuer).await? {
            Ok(metadata) => {
                selected = Some(metadata);
                break;
            }
            Err(failure) => {
                first_failure.get_or_insert(failure);
            }
        }
    }
    let metadata = selected.ok_or(
        first_failure.unwrap_or(AuthorizationDiscoveryError::AuthorizationServerUnavailable),
    )?;
    let mut scopes: Vec<String> = if challenge.scopes().is_empty() {
        protected.scopes_supported.clone()
    } else {
        challenge.scopes().to_vec()
    };
    scopes.sort();
    scopes.dedup();
    // The request is exact: an unbounded advertisement is rejected, never trimmed.
    if scopes.len() > MAX_SCOPES {
        return Err(AuthorizationDiscoveryError::ResourceMetadataUnavailable);
    }
    // Consent navigation is screened by Provider authorization when issued.
    // Only endpoints Coral requests belong to the server-egress policy.
    for endpoint in [
        Some(&metadata.token_endpoint),
        metadata.registration_endpoint.as_ref(),
        metadata.device_authorization_endpoint.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        let url = url::Url::parse(endpoint)
            .map_err(|_error| AuthorizationDiscoveryError::InsecureEndpoint)?;
        policy
            .validate(&url)
            .map_err(|_error| AuthorizationDiscoveryError::DestinationRejected)?;
    }
    Ok(AuthorizationRequirement {
        resource,
        issuer: metadata.issuer,
        authorization_endpoint: metadata.authorization_endpoint,
        token_endpoint: metadata.token_endpoint,
        device_authorization_endpoint: metadata.device_authorization_endpoint,
        registration_endpoint: metadata.registration_endpoint,
        client_id_metadata_document_supported: metadata.client_id_metadata_document_supported,
        scopes,
    })
}

/// Registers a public client for the exact redirect URI.
///
/// # Errors
/// Returns `ClientBootstrapFailed` for transport, shape, or a confidential client.
pub async fn register_client(
    policy: coral_outbound::DestinationPolicy,
    registration_endpoint: &str,
    redirect_uri: &str,
) -> Result<RegisteredClient, AuthorizationDiscoveryError> {
    let endpoint = validated_endpoint(registration_endpoint)?;
    let client = Fetcher::new(policy)?;
    let body = serde_json::json!({
        "client_name": "Coral",
        "redirect_uris": [redirect_uri],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
    });
    let response = tokio::time::timeout(METADATA_TIMEOUT, async {
        let mut response = client
            .client
            .post(&endpoint)
            .map_err(|_error| AuthorizationDiscoveryError::DestinationRejected)?
            .json(&body)
            .send()
            .await
            .map_err(|error| {
                discovery_fetch_error(error, AuthorizationDiscoveryError::ClientBootstrapFailed)
            })?;
        if !response.status().is_success() {
            return Err(AuthorizationDiscoveryError::ClientBootstrapFailed);
        }
        crate::read_bounded_body(&mut response, METADATA_MAX_BYTES, true)
            .await
            .map_err(|_error| AuthorizationDiscoveryError::ClientBootstrapFailed)
    })
    .await
    .map_err(|_deadline| AuthorizationDiscoveryError::ClientBootstrapFailed)??;
    let registration: Registration = serde_json::from_slice(&response)
        .map_err(|_error| AuthorizationDiscoveryError::ClientBootstrapFailed)?;
    drop(response);
    if registration.client_secret.is_some()
        || registration
            .token_endpoint_auth_method
            .as_deref()
            .is_some_and(|method| method != "none")
        || coral_catalog::validate_oauth_client_id(&registration.client_id).is_err()
    {
        return Err(AuthorizationDiscoveryError::ClientBootstrapFailed);
    }
    Ok(RegisteredClient {
        client_id: registration.client_id,
        client_id_issued_at: registration.client_id_issued_at,
    })
}

#[derive(Deserialize)]
struct Registration {
    client_id: String,
    #[serde(default)]
    client_id_issued_at: Option<u64>,
    #[serde(default)]
    client_secret: Option<String>,
    #[serde(default)]
    token_endpoint_auth_method: Option<String>,
}

struct Fetcher {
    client: coral_outbound::Client,
    remaining: std::sync::atomic::AtomicUsize,
}

impl Fetcher {
    fn new(policy: coral_outbound::DestinationPolicy) -> Result<Self, AuthorizationDiscoveryError> {
        Ok(Self {
            client: coral_outbound::Client::new(policy, METADATA_TIMEOUT)
                .map_err(|_error| AuthorizationDiscoveryError::ResourceMetadataUnavailable)?,
            remaining: std::sync::atomic::AtomicUsize::new(MAX_METADATA_REQUESTS),
        })
    }

    /// Fetches one JSON document; `None` means the location answered without one.
    async fn json(&self, location: &str) -> Result<Option<Value>, AuthorizationDiscoveryError> {
        self.json_with_headers(location, &std::collections::BTreeMap::new())
            .await
    }

    async fn json_with_headers(
        &self,
        location: &str,
        headers: &std::collections::BTreeMap<String, String>,
    ) -> Result<Option<Value>, AuthorizationDiscoveryError> {
        let endpoint = validated_endpoint(location)?;
        if self
            .remaining
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |remaining| remaining.checked_sub(1),
            )
            .is_err()
        {
            return Err(AuthorizationDiscoveryError::ResourceMetadataUnavailable);
        }
        let mut request = self
            .client
            .get(&endpoint)
            .map_err(|_error| AuthorizationDiscoveryError::DestinationRejected)?
            .header("accept", "application/json");
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let mut response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                return Err(discovery_fetch_error(
                    error,
                    AuthorizationDiscoveryError::ResourceMetadataUnavailable,
                ));
            }
        };
        if matches!(response.status().as_u16(), 404 | 410) {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(AuthorizationDiscoveryError::ResourceMetadataUnavailable);
        }
        let body = crate::read_bounded_body(&mut response, METADATA_MAX_BYTES, true)
            .await
            .map_err(|_invalid| AuthorizationDiscoveryError::ResourceMetadataUnavailable)?;
        serde_json::from_slice(&body)
            .map(Some)
            .map_err(|_invalid| AuthorizationDiscoveryError::ResourceMetadataUnavailable)
    }
}

#[derive(Deserialize)]
struct ProtectedResource {
    resource: String,
    #[serde(default)]
    authorization_servers: Vec<String>,
    #[serde(default)]
    scopes_supported: Vec<String>,
}

#[derive(Deserialize)]
struct AuthorizationServer {
    issuer: String,
    authorization_endpoint: Option<String>,
    token_endpoint: String,
    device_authorization_endpoint: Option<String>,
    #[serde(default)]
    registration_endpoint: Option<String>,
    #[serde(default)]
    code_challenge_methods_supported: Option<Vec<String>>,
    #[serde(default)]
    client_id_metadata_document_supported: Option<bool>,
}

async fn fetch_protected_resource(
    client: &Fetcher,
    resource: &str,
    challenge: &BearerChallenge,
    request_headers: &std::collections::BTreeMap<String, String>,
) -> Result<ProtectedResource, AuthorizationDiscoveryError> {
    let base = url::Url::parse(resource)
        .map_err(|_error| AuthorizationDiscoveryError::ResourceMetadataUnavailable)?;
    let origin = format!(
        "{}://{}",
        base.scheme(),
        base.host_str()
            .ok_or(AuthorizationDiscoveryError::ResourceMetadataUnavailable)?
    );
    let origin = match base.port() {
        Some(port) => format!("{origin}:{port}"),
        None => origin,
    };
    let locations: Vec<String> = if let Some(location) = challenge.resource_metadata() {
        vec![location.to_owned()]
    } else {
        let path = base.path().trim_end_matches('/');
        let mut locations = Vec::new();
        if !path.is_empty() {
            locations.push(format!(
                "{origin}/.well-known/oauth-protected-resource{path}"
            ));
        }
        locations.push(format!("{origin}/.well-known/oauth-protected-resource"));
        locations
    };
    for location in locations {
        // A challenge can name metadata on another origin. Service headers belong
        // only to this resource, never to an external metadata host or its issuer.
        let same_origin =
            url::Url::parse(&location).is_ok_and(|endpoint| endpoint.origin() == base.origin());
        let document = if same_origin {
            client.json_with_headers(&location, request_headers).await?
        } else {
            client.json(&location).await?
        };
        if let Some(document) = document {
            return serde_json::from_value(document)
                .map_err(|_error| AuthorizationDiscoveryError::ResourceMetadataUnavailable);
        }
    }
    Err(AuthorizationDiscoveryError::ResourceMetadataMissing)
}

enum IssuerAnswer {
    Valid(AuthorizationServer),
    Mismatch,
    Unavailable,
}

/// Validated authorization-server facts for one issuer.
struct ValidatedIssuer {
    /// The identifier the authorization server declares for itself, which is
    /// the canonical spelling when an advertisement writes the same one
    /// differently.
    issuer: String,
    authorization_endpoint: Option<String>,
    token_endpoint: String,
    device_authorization_endpoint: Option<String>,
    registration_endpoint: Option<String>,
    client_id_metadata_document_supported: bool,
}

/// Fetches and validates one issuer; the inner error is that issuer's disqualification.
async fn validated_issuer(
    client: &Fetcher,
    issuer: &str,
) -> Result<Result<ValidatedIssuer, AuthorizationDiscoveryError>, AuthorizationDiscoveryError> {
    let metadata = match fetch_authorization_server(client, issuer).await? {
        IssuerAnswer::Valid(metadata) => metadata,
        IssuerAnswer::Mismatch => return Ok(Err(AuthorizationDiscoveryError::IssuerMismatch)),
        IssuerAnswer::Unavailable => {
            return Ok(Err(
                AuthorizationDiscoveryError::AuthorizationServerUnavailable,
            ));
        }
    };
    if metadata.authorization_endpoint.is_none() && metadata.device_authorization_endpoint.is_none()
    {
        return Ok(Err(
            AuthorizationDiscoveryError::AuthorizationServerUnavailable,
        ));
    }
    // S256 is required only for authorization-code OAuth.
    let pkce = metadata
        .code_challenge_methods_supported
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|method| method == "S256");
    if !pkce && metadata.device_authorization_endpoint.is_none() {
        return Ok(Err(AuthorizationDiscoveryError::PkceUnsupported));
    }
    let validated = (|| {
        Ok(ValidatedIssuer {
            issuer: metadata.issuer.clone(),
            authorization_endpoint: if pkce {
                metadata
                    .authorization_endpoint
                    .as_deref()
                    .map(validated_endpoint)
                    .transpose()?
            } else {
                None
            },
            token_endpoint: validated_endpoint(&metadata.token_endpoint)?,
            device_authorization_endpoint: metadata
                .device_authorization_endpoint
                .as_deref()
                .map(validated_endpoint)
                .transpose()?,
            registration_endpoint: metadata
                .registration_endpoint
                .as_deref()
                .map(validated_endpoint)
                .transpose()?,
            client_id_metadata_document_supported: metadata
                .client_id_metadata_document_supported
                .unwrap_or(false),
        })
    })();
    Ok(validated)
}

async fn fetch_authorization_server(
    client: &Fetcher,
    issuer: &str,
) -> Result<IssuerAnswer, AuthorizationDiscoveryError> {
    let parsed = url::Url::parse(issuer)
        .map_err(|_error| AuthorizationDiscoveryError::AuthorizationServerUnavailable)?;
    validated_endpoint(issuer)?;
    let mut origin = format!(
        "{}://{}",
        parsed.scheme(),
        parsed
            .host_str()
            .ok_or(AuthorizationDiscoveryError::AuthorizationServerUnavailable)?
    );
    if let Some(port) = parsed.port() {
        origin = format!("{origin}:{port}");
    }
    let path = parsed.path().trim_end_matches('/');
    let locations = if path.is_empty() {
        vec![
            format!("{origin}/.well-known/oauth-authorization-server"),
            format!("{origin}/.well-known/openid-configuration"),
        ]
    } else {
        vec![
            format!("{origin}/.well-known/oauth-authorization-server{path}"),
            format!("{origin}/.well-known/openid-configuration{path}"),
            format!("{origin}{path}/.well-known/openid-configuration"),
        ]
    };
    for location in locations {
        let Some(document) = client.json(&location).await? else {
            continue;
        };
        // The first document an issuer publishes is its answer; a malformed or
        // mismatched one disqualifies the issuer rather than inviting more probes.
        let Ok(metadata) = serde_json::from_value::<AuthorizationServer>(document) else {
            return Ok(IssuerAnswer::Unavailable);
        };
        if !same_issuer(issuer, &metadata.issuer) {
            return Ok(IssuerAnswer::Mismatch);
        }
        return Ok(IssuerAnswer::Valid(metadata));
    }
    Ok(IssuerAnswer::Unavailable)
}

/// Whether an advertised issuer and the one its metadata declares are the same
/// identifier. RFC 8414 asks for an exact string match, but advertisements and
/// the metadata they point at differ in spelling alone: Google's
/// protected-resource metadata names `https://accounts.google.com/` while that
/// server declares `https://accounts.google.com`. Both sides are therefore
/// compared after RFC 3986 syntax-based normalization, which forgives spelling
/// and nothing else; a different scheme, host, port, or path is still a
/// mismatch, and an unparsable declaration can never match.
fn same_issuer(advertised: &str, declared: &str) -> bool {
    match (url::Url::parse(advertised), url::Url::parse(declared)) {
        (Ok(advertised), Ok(declared)) => advertised == declared,
        _ => advertised == declared,
    }
}

fn canonical_resource(value: &str) -> Result<String, AuthorizationDiscoveryError> {
    let parsed =
        url::Url::parse(value).map_err(|_error| AuthorizationDiscoveryError::ResourceMismatch)?;
    if parsed.query().is_some()
        || parsed.fragment().is_some()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        return Err(AuthorizationDiscoveryError::ResourceMismatch);
    }
    let host = parsed
        .host_str()
        .ok_or(AuthorizationDiscoveryError::ResourceMismatch)?
        .to_ascii_lowercase();
    let mut canonical = format!("{}://{host}", parsed.scheme().to_ascii_lowercase());
    if let Some(port) = parsed.port() {
        canonical = format!("{canonical}:{port}");
    }
    // The path is exact: `/mcp/` and `/mcp` are different resources.
    let path = parsed.path();
    if path != "/" {
        canonical.push_str(path);
    }
    Ok(canonical)
}

/// Whether a declared protected resource covers the MCP server: it is the
/// server itself, or an ancestor with the same scheme, host, and port whose
/// path the server's path extends at a segment boundary. `https://h` covers
/// `https://h/mcp`, and `https://h/api` covers `https://h/api/mcp`; `/ap`
/// never covers `/api`, and `/mcp/` still does not cover `/mcp`.
fn covers(resource: &str, server: &str) -> bool {
    if server == resource {
        return true;
    }
    let Some(descendant) = server.strip_prefix(resource) else {
        return false;
    };
    resource.ends_with('/') || descendant.starts_with('/')
}

/// Checks endpoint shape; the shared policy owns scheme/category permissions.
fn validated_endpoint(value: &str) -> Result<String, AuthorizationDiscoveryError> {
    let parsed =
        url::Url::parse(value).map_err(|_error| AuthorizationDiscoveryError::InsecureEndpoint)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        return Err(AuthorizationDiscoveryError::InsecureEndpoint);
    }
    Ok(parsed.to_string())
}

fn discovery_fetch_error(
    error: reqwest::Error,
    otherwise: AuthorizationDiscoveryError,
) -> AuthorizationDiscoveryError {
    match coral_outbound::FetchError::from(error) {
        coral_outbound::FetchError::DestinationRejected => {
            AuthorizationDiscoveryError::DestinationRejected
        }
        coral_outbound::FetchError::Http(_) => otherwise,
    }
}
