//! Runtime HTTP injection for stored provider identities.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use coral_engine::{RequestIdentityResolutionContext, RequestIdentityResolverError};
use coral_spec::{
    IdentityManifest, IdentitySpecConfig, IdentitySpecType, ManifestOAuthCredentialSpec,
};
use reqwest::header::{AUTHORIZATION, HeaderName, HeaderValue};
use serde_json::Value;

use crate::bootstrap::AppError;
use crate::credentials::oauth::{OAuthCredentialService, RefreshOAuthCredentialRequest};
use crate::identity::RuntimeSourceIdentity;

pub(super) const OAUTH_ACCESS_TOKEN_MATERIAL_KEY: &str = "ACCESS_TOKEN";
pub(super) const FIXED_TOKEN_MATERIAL_KEY: &str = "TOKEN";

/// Services type-specific identity runtimes may need while preparing material.
pub(super) struct IdentityRuntimeServices<'a> {
    pub(super) oauth_credential_service: &'a OAuthCredentialService,
}

/// Parsed identity spec plus stored secret material for one concrete identity.
pub(super) struct StoredIdentityRuntimeData {
    identity_name: String,
    identity_spec: IdentityManifest,
    identity_inputs: BTreeMap<String, String>,
    material: BTreeMap<String, String>,
}

impl fmt::Debug for StoredIdentityRuntimeData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let identity_input_keys = self.identity_inputs.keys().collect::<Vec<_>>();
        let material_keys = self.material.keys().collect::<Vec<_>>();
        f.debug_struct("StoredIdentityRuntimeData")
            .field("identity_name", &self.identity_name)
            .field("identity_spec", &self.identity_spec.name)
            .field("identity_type", &self.identity_spec.identity_type.label())
            .field("identity_input_keys", &identity_input_keys)
            .field("material_keys", &material_keys)
            .finish()
    }
}

impl StoredIdentityRuntimeData {
    pub(super) fn new(
        identity_name: String,
        identity_spec: IdentityManifest,
        identity_inputs: BTreeMap<String, String>,
        material: BTreeMap<String, String>,
    ) -> Self {
        Self {
            identity_name,
            identity_spec,
            identity_inputs,
            material,
        }
    }

    /// Prepares the runtime identity for this stored identity's type:
    /// OAuth identities refresh stored token material when needed before
    /// injection; fixed-token identities inject their stored token as-is.
    pub(super) async fn prepare(
        mut self,
        services: IdentityRuntimeServices<'_>,
    ) -> Result<PreparedRuntimeIdentity, AppError> {
        let (material_key, label, updated_material) = match self.identity_spec.identity_type {
            IdentitySpecType::OAuth => {
                let oauth = oauth_method(&self.identity_spec)?;
                let refreshed = services
                    .oauth_credential_service
                    .refresh_if_needed(
                        RefreshOAuthCredentialRequest::for_identity(
                            &self.identity_name,
                            OAUTH_ACCESS_TOKEN_MATERIAL_KEY,
                            oauth,
                            &self.identity_inputs,
                        ),
                        &mut self.material,
                    )
                    .await?;
                let updated_material = refreshed.then(|| self.material.clone());
                (
                    OAUTH_ACCESS_TOKEN_MATERIAL_KEY,
                    "OAuth access token",
                    updated_material,
                )
            }
            IdentitySpecType::FixedToken => (FIXED_TOKEN_MATERIAL_KEY, "fixed token", None),
        };
        Ok(PreparedRuntimeIdentity {
            identity: Arc::new(BearerTokenRuntimeIdentity {
                data: self,
                material_key,
                label,
            }),
            updated_material,
        })
    }

    fn identity_spec_id(&self) -> &str {
        &self.identity_spec.name
    }

    fn audience(&self) -> &BTreeMap<String, Value> {
        &self.identity_spec.audience
    }
}

pub(super) struct PreparedRuntimeIdentity {
    pub(super) identity: Arc<dyn RuntimeSourceIdentity>,
    pub(super) updated_material: Option<BTreeMap<String, String>>,
}

/// Runtime identity that injects `Authorization: Bearer <token>` from one
/// stored material key, after enforcing the identity's audience host.
struct BearerTokenRuntimeIdentity {
    data: StoredIdentityRuntimeData,
    material_key: &'static str,
    label: &'static str,
}

impl fmt::Debug for BearerTokenRuntimeIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BearerTokenRuntimeIdentity")
            .field("data", &self.data)
            .field("material_key", &self.material_key)
            .field("label", &self.label)
            .finish()
    }
}

#[tonic::async_trait]
impl RuntimeSourceIdentity for BearerTokenRuntimeIdentity {
    fn identity_spec_id(&self) -> &str {
        self.data.identity_spec_id()
    }

    fn audience(&self) -> &BTreeMap<String, Value> {
        self.data.audience()
    }

    async fn resolve_headers(
        &self,
        identity: &RequestIdentityResolutionContext,
        request: &reqwest::Request,
        _resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityResolverError> {
        ensure_identity_is_accepted(&self.data, identity)?;
        ensure_request_matches_identity_audience(&self.data, request)?;
        let token = required_material(&self.data, self.material_key, self.label)?;
        bearer_authorization_header(&self.data, token, self.label)
    }
}

fn oauth_method(spec: &IdentityManifest) -> Result<&ManifestOAuthCredentialSpec, AppError> {
    let IdentitySpecConfig::OAuth(oauth) = &spec.config else {
        return Err(AppError::FailedPrecondition(format!(
            "identity spec '{}' has type oauth but no OAuth runtime config",
            spec.name
        )));
    };
    Ok(&oauth.method.oauth)
}

fn required_material<'a>(
    data: &'a StoredIdentityRuntimeData,
    key: &str,
    label: &str,
) -> Result<&'a str, RequestIdentityResolverError> {
    data.material
        .get(key)
        .filter(|value| !value.is_empty())
        .map(String::as_str)
        .ok_or_else(|| {
            RequestIdentityResolverError::failed_precondition(format!(
                "identity '{}' is missing {label} material key '{key}'",
                data.identity_name
            ))
        })
}

fn bearer_authorization_header(
    data: &StoredIdentityRuntimeData,
    token: &str,
    label: &str,
) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityResolverError> {
    authorization_header(data, "Bearer", token, label)
}

fn ensure_identity_is_accepted(
    data: &StoredIdentityRuntimeData,
    identity: &RequestIdentityResolutionContext,
) -> Result<(), RequestIdentityResolverError> {
    if identity.accepts_identity(data.identity_spec_id(), data.audience()) {
        return Ok(());
    }
    Err(RequestIdentityResolverError::failed_precondition(format!(
        "identity '{}' with spec '{}' is not accepted by source '{}' surface '{}'",
        data.identity_name,
        data.identity_spec_id(),
        identity.source_name(),
        identity.surface_id()
    )))
}

fn authorization_header(
    data: &StoredIdentityRuntimeData,
    scheme: &str,
    credential: &str,
    label: &str,
) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityResolverError> {
    let value = HeaderValue::from_str(&format!("{scheme} {credential}")).map_err(|error| {
        RequestIdentityResolverError::failed_precondition(format!(
            "identity '{}' {label} material could not be encoded as an Authorization header: {error}",
            data.identity_name
        ))
    })?;
    Ok(vec![(AUTHORIZATION, value)])
}

fn ensure_request_matches_identity_audience(
    data: &StoredIdentityRuntimeData,
    request: &reqwest::Request,
) -> Result<(), RequestIdentityResolverError> {
    let audience_host = required_audience_host(data)?;
    let request_host = request.url().host_str().ok_or_else(|| {
        RequestIdentityResolverError::failed_precondition(format!(
            "identity '{}' cannot inject Authorization headers for URL without a host",
            data.identity_name
        ))
    })?;
    if !host_matches_audience(request_host, audience_host) {
        return Err(RequestIdentityResolverError::failed_precondition(format!(
            "identity '{}' audience host '{}' does not match request host '{}'",
            data.identity_name, audience_host, request_host
        )));
    }
    ensure_request_scheme_matches_identity_audience(data, request.url().scheme())?;
    ensure_request_port_matches_identity_audience(data, request.url().port_or_known_default())?;
    Ok(())
}

fn required_audience_host(
    data: &StoredIdentityRuntimeData,
) -> Result<&str, RequestIdentityResolverError> {
    data.identity_spec
        .audience
        .get("host")
        .and_then(Value::as_str)
        .filter(|host| !host.trim().is_empty())
        .ok_or_else(|| {
            RequestIdentityResolverError::failed_precondition(format!(
                "identity spec '{}' must declare string audience.host before identity '{}' can inject Authorization headers",
                data.identity_spec.name, data.identity_name
            ))
        })
}

fn optional_audience_string_claim<'a>(
    data: &'a StoredIdentityRuntimeData,
    key: &str,
) -> Result<Option<&'a str>, RequestIdentityResolverError> {
    let Some(value) = data.identity_spec.audience.get(key) else {
        return Ok(None);
    };
    value
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .map(Some)
        .ok_or_else(|| {
            RequestIdentityResolverError::failed_precondition(format!(
                "identity spec '{}' audience.{key} must be a non-empty string before identity '{}' can inject Authorization headers",
                data.identity_spec.name, data.identity_name
            ))
        })
}

fn optional_audience_port(
    data: &StoredIdentityRuntimeData,
) -> Result<Option<u16>, RequestIdentityResolverError> {
    let Some(value) = data.identity_spec.audience.get("port") else {
        return Ok(None);
    };
    let port = value.as_u64().and_then(|port| u16::try_from(port).ok());
    port.map(Some).ok_or_else(|| {
        RequestIdentityResolverError::failed_precondition(format!(
            "identity spec '{}' audience.port must be an integer between 0 and 65535 before identity '{}' can inject Authorization headers",
            data.identity_spec.name, data.identity_name
        ))
    })
}

fn ensure_request_scheme_matches_identity_audience(
    data: &StoredIdentityRuntimeData,
    request_scheme: &str,
) -> Result<(), RequestIdentityResolverError> {
    let audience_scheme = optional_audience_string_claim(data, "scheme")?.unwrap_or("https");
    if request_scheme.eq_ignore_ascii_case(audience_scheme) {
        return Ok(());
    }
    Err(RequestIdentityResolverError::failed_precondition(format!(
        "identity '{}' audience scheme '{}' does not match request scheme '{}'",
        data.identity_name, audience_scheme, request_scheme
    )))
}

fn ensure_request_port_matches_identity_audience(
    data: &StoredIdentityRuntimeData,
    request_port: Option<u16>,
) -> Result<(), RequestIdentityResolverError> {
    let Some(audience_port) = optional_audience_port(data)? else {
        return Ok(());
    };
    let Some(request_port) = request_port else {
        return Err(RequestIdentityResolverError::failed_precondition(format!(
            "identity '{}' cannot inject Authorization headers for URL without a known port while audience.port is {}",
            data.identity_name, audience_port
        )));
    };
    if request_port == audience_port {
        return Ok(());
    }
    Err(RequestIdentityResolverError::failed_precondition(format!(
        "identity '{}' audience port '{}' does not match request port '{}'",
        data.identity_name, audience_port, request_port
    )))
}

fn host_matches_audience(request_host: &str, audience_host: &str) -> bool {
    let request_host = request_host
        .trim()
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let audience_host = audience_host
        .trim()
        .trim_end_matches('.')
        .to_ascii_lowercase();
    !audience_host.is_empty() && request_host == audience_host
}

#[cfg(test)]
mod tests {
    use super::*;
    use coral_spec::parse_identity_manifest_yaml;
    use coral_spec::v4::{AcceptedIdentityRequirement, IdentityRequirements};

    async fn prepared_identity(
        identity_spec: IdentityManifest,
        material_key: &str,
        token: &str,
    ) -> PreparedRuntimeIdentity {
        StoredIdentityRuntimeData::new(
            "demo_local".to_string(),
            identity_spec,
            BTreeMap::new(),
            BTreeMap::from([(material_key.to_string(), token.to_string())]),
        )
        .prepare(IdentityRuntimeServices {
            oauth_credential_service: &OAuthCredentialService::new(),
        })
        .await
        .expect("prepare identity")
    }

    async fn prepared_fixed_token_identity(
        identity_spec: IdentityManifest,
    ) -> PreparedRuntimeIdentity {
        prepared_identity(identity_spec, FIXED_TOKEN_MATERIAL_KEY, "fixed-token").await
    }

    async fn resolve_headers(
        prepared: &PreparedRuntimeIdentity,
        host: &str,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityResolverError> {
        resolve_headers_for_url(prepared, &format!("https://{host}/user")).await
    }

    async fn resolve_headers_for_url(
        prepared: &PreparedRuntimeIdentity,
        url: &str,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityResolverError> {
        let requirements = IdentityRequirements {
            accepts: vec![AcceptedIdentityRequirement {
                id: "demo-runtime".to_string(),
                identity_specs: vec![prepared.identity.identity_spec_id().to_string()],
                audience: prepared.identity.audience().clone(),
            }],
        };
        resolve_headers_for_url_with_requirements(prepared, url, requirements).await
    }

    async fn resolve_headers_with_requirements(
        prepared: &PreparedRuntimeIdentity,
        host: &str,
        requirements: IdentityRequirements,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityResolverError> {
        resolve_headers_for_url_with_requirements(
            prepared,
            &format!("https://{host}/user"),
            requirements,
        )
        .await
    }

    async fn resolve_headers_for_url_with_requirements(
        prepared: &PreparedRuntimeIdentity,
        url: &str,
        requirements: IdentityRequirements,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityResolverError> {
        let context = RequestIdentityResolutionContext::new(
            "demo".to_string(),
            "rest".to_string(),
            requirements,
        );
        let request = reqwest::Request::new(reqwest::Method::GET, url.parse().expect("url"));
        prepared
            .identity
            .resolve_headers(&context, &request, &BTreeMap::new())
            .await
    }

    async fn assert_identity_injects_bearer_token(
        identity_spec: IdentityManifest,
        material_key: &str,
        token: &str,
        expected_header: &'static str,
    ) {
        let prepared = prepared_identity(identity_spec, material_key, token).await;

        let headers = resolve_headers(&prepared, "example.test")
            .await
            .expect("headers");

        assert_eq!(
            headers,
            vec![(AUTHORIZATION, HeaderValue::from_static(expected_header))]
        );
        assert!(prepared.updated_material.is_none());
    }

    fn oauth_identity_spec() -> IdentityManifest {
        parse_identity_manifest_yaml(
            r"
kind: identity
spec_version: 1
name: demo_oauth
version: 0.1.0
issuer: demo
type: oauth
audience:
  host: example.test
oauth:
  method:
    flow:
      type: device_code
    endpoints:
      device_authorization_url: https://example.test/device
      token_url: https://example.test/token
    client:
      id:
        default: demo-client
",
        )
        .expect("identity spec")
    }

    fn fixed_token_identity_spec() -> IdentityManifest {
        fixed_token_identity_spec_with_audience("host: example.test")
    }

    fn fixed_token_identity_spec_without_audience_host() -> IdentityManifest {
        fixed_token_identity_spec_with_audience("tenant: demo")
    }

    fn fixed_token_identity_spec_with_origin_audience() -> IdentityManifest {
        fixed_token_identity_spec_with_audience("host: example.test\n  scheme: https\n  port: 443")
    }

    fn fixed_token_identity_spec_with_audience(audience: &str) -> IdentityManifest {
        parse_identity_manifest_yaml(&format!(
            r"
kind: identity
spec_version: 1
name: demo_token
version: 0.1.0
issuer: demo
type: fixed_token
audience:
  {audience}
"
        ))
        .expect("identity spec")
    }

    #[test]
    fn runtime_identity_debug_redacts_inputs_and_material_values() {
        let data = StoredIdentityRuntimeData::new(
            "demo_local".to_string(),
            oauth_identity_spec(),
            BTreeMap::from([("CLIENT_SECRET".to_string(), "secret-input".to_string())]),
            BTreeMap::from([
                (
                    OAUTH_ACCESS_TOKEN_MATERIAL_KEY.to_string(),
                    "access-token".to_string(),
                ),
                (
                    "__coral_oauth.ACCESS_TOKEN.refresh_token".to_string(),
                    "refresh-token".to_string(),
                ),
            ]),
        );

        let data_debug = format!("{data:?}");
        assert!(data_debug.contains("demo_local"));
        assert!(data_debug.contains("CLIENT_SECRET"));
        assert!(data_debug.contains(OAUTH_ACCESS_TOKEN_MATERIAL_KEY));
        assert!(!data_debug.contains("secret-input"));
        assert!(!data_debug.contains("access-token"));
        assert!(!data_debug.contains("refresh-token"));

        let identity = BearerTokenRuntimeIdentity {
            data,
            material_key: OAUTH_ACCESS_TOKEN_MATERIAL_KEY,
            label: "OAuth access token",
        };
        let identity_debug = format!("{identity:?}");
        assert!(identity_debug.contains("BearerTokenRuntimeIdentity"));
        assert!(!identity_debug.contains("secret-input"));
        assert!(!identity_debug.contains("access-token"));
        assert!(!identity_debug.contains("refresh-token"));
    }

    #[tokio::test]
    async fn oauth_identity_injects_bearer_access_token_from_material() {
        assert_identity_injects_bearer_token(
            oauth_identity_spec(),
            OAUTH_ACCESS_TOKEN_MATERIAL_KEY,
            "oauth-token",
            "Bearer oauth-token",
        )
        .await;
    }

    #[tokio::test]
    async fn fixed_token_identity_injects_bearer_token_from_material() {
        assert_identity_injects_bearer_token(
            fixed_token_identity_spec(),
            FIXED_TOKEN_MATERIAL_KEY,
            "fixed-token",
            "Bearer fixed-token",
        )
        .await;
    }

    #[tokio::test]
    async fn identity_headers_reject_identity_not_accepted_by_surface_requirements() {
        let prepared = prepared_fixed_token_identity(fixed_token_identity_spec()).await;
        let requirements = IdentityRequirements {
            accepts: vec![AcceptedIdentityRequirement {
                id: "other-runtime".to_string(),
                identity_specs: vec!["other_identity_spec".to_string()],
                audience: BTreeMap::new(),
            }],
        };

        let error = resolve_headers_with_requirements(&prepared, "api.example.test", requirements)
            .await
            .expect_err("unaccepted identity should not receive identity headers");

        assert!(
            error.to_string().contains("is not accepted"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn identity_headers_reject_subdomain_request_host_outside_audience() {
        let prepared = prepared_fixed_token_identity(fixed_token_identity_spec()).await;

        let error = resolve_headers(&prepared, "api.example.test")
            .await
            .expect_err("mismatched host should not receive identity headers");

        assert!(
            error.to_string().contains("does not match request host"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn identity_headers_enforce_audience_scheme_and_port() {
        let prepared =
            prepared_fixed_token_identity(fixed_token_identity_spec_with_origin_audience()).await;

        resolve_headers_for_url(&prepared, "https://example.test/user")
            .await
            .expect("default https port should satisfy audience.port");

        let scheme_error = resolve_headers_for_url(&prepared, "http://example.test/user")
            .await
            .expect_err("mismatched scheme should not receive identity headers");
        assert!(
            scheme_error
                .to_string()
                .contains("audience scheme 'https' does not match request scheme 'http'"),
            "unexpected error: {scheme_error}"
        );

        let port_error = resolve_headers_for_url(&prepared, "https://example.test:8443/user")
            .await
            .expect_err("mismatched port should not receive identity headers");
        assert!(
            port_error
                .to_string()
                .contains("audience port '443' does not match request port '8443'"),
            "unexpected error: {port_error}"
        );
    }

    #[tokio::test]
    async fn identity_headers_require_https_for_host_only_audience() {
        let prepared = prepared_fixed_token_identity(fixed_token_identity_spec()).await;

        let error = resolve_headers_for_url(&prepared, "http://example.test/user")
            .await
            .expect_err("host-only audience should default to https");

        assert!(
            error
                .to_string()
                .contains("audience scheme 'https' does not match request scheme 'http'"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn identity_headers_allow_explicit_http_audience_scheme() {
        let prepared = prepared_fixed_token_identity(fixed_token_identity_spec_with_audience(
            "host: example.test\n  scheme: http",
        ))
        .await;

        resolve_headers_for_url(&prepared, "http://example.test/user")
            .await
            .expect("explicit http audience should allow http requests");
    }

    #[tokio::test]
    async fn identity_headers_reject_invalid_audience_origin_claims() {
        let invalid_scheme = prepared_fixed_token_identity(
            fixed_token_identity_spec_with_audience("host: example.test\n  scheme: 42"),
        )
        .await;
        let scheme_error = resolve_headers(&invalid_scheme, "example.test")
            .await
            .expect_err("non-string audience scheme should fail closed");
        assert!(
            scheme_error
                .to_string()
                .contains("audience.scheme must be a non-empty string"),
            "unexpected error: {scheme_error}"
        );

        let invalid_port = prepared_fixed_token_identity(fixed_token_identity_spec_with_audience(
            "host: example.test\n  port: bad",
        ))
        .await;
        let port_error = resolve_headers(&invalid_port, "example.test")
            .await
            .expect_err("non-numeric audience port should fail closed");
        assert!(
            port_error
                .to_string()
                .contains("audience.port must be an integer between 0 and 65535"),
            "unexpected error: {port_error}"
        );
    }

    #[tokio::test]
    async fn identity_headers_require_audience_host() {
        let prepared =
            prepared_fixed_token_identity(fixed_token_identity_spec_without_audience_host()).await;

        let error = resolve_headers(&prepared, "api.example.test")
            .await
            .expect_err("missing audience host should fail closed");

        assert!(
            error
                .to_string()
                .contains("must declare string audience.host"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn audience_host_matching_allows_only_exact_hosts() {
        assert!(host_matches_audience("example.test", "example.test"));
        assert!(host_matches_audience("EXAMPLE.TEST", "example.test"));
        assert!(host_matches_audience("example.test.", "example.test."));
        assert!(!host_matches_audience("api.example.test", "example.test"));
        assert!(!host_matches_audience("badexample.test", "example.test"));
        assert!(!host_matches_audience(
            "example.test.attacker.test",
            "example.test"
        ));
        assert!(!host_matches_audience("github.com.evil.test", "github.com"));
        assert!(!host_matches_audience("example.test", ""));
    }
}
