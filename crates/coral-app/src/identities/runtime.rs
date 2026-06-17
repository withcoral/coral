//! Runtime HTTP injection for stored provider identities.

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::sync::Arc;

use coral_engine::{RequestIdentityHttpAuthenticatorError, SelectedRequestIdentity};
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
#[derive(Debug)]
pub(super) struct StoredIdentityRuntimeData {
    identity_name: String,
    identity_spec: IdentityManifest,
    identity_inputs: BTreeMap<String, String>,
    material: BTreeMap<String, String>,
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
/// stored material key after enforcing the identity's audience host.
#[derive(Debug)]
struct BearerTokenRuntimeIdentity {
    data: StoredIdentityRuntimeData,
    material_key: &'static str,
    label: &'static str,
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
        _identity: &SelectedRequestIdentity,
        request: &reqwest::Request,
        _resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityHttpAuthenticatorError> {
        ensure_request_uses_credential_safe_transport(&self.data, request)?;
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
) -> Result<&'a str, RequestIdentityHttpAuthenticatorError> {
    data.material
        .get(key)
        .filter(|value| !value.is_empty())
        .map(String::as_str)
        .ok_or_else(|| {
            RequestIdentityHttpAuthenticatorError::failed_precondition(format!(
                "identity '{}' is missing {label} material key '{key}'",
                data.identity_name
            ))
        })
}

fn bearer_authorization_header(
    data: &StoredIdentityRuntimeData,
    token: &str,
    label: &str,
) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityHttpAuthenticatorError> {
    authorization_header(data, "Bearer", token, label)
}

fn authorization_header(
    data: &StoredIdentityRuntimeData,
    scheme: &str,
    credential: &str,
    label: &str,
) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityHttpAuthenticatorError> {
    let value = HeaderValue::from_str(&format!("{scheme} {credential}")).map_err(|error| {
        RequestIdentityHttpAuthenticatorError::failed_precondition(format!(
            "identity '{}' {label} material could not be encoded as an Authorization header: {error}",
            data.identity_name
        ))
    })?;
    Ok(vec![(AUTHORIZATION, value)])
}

fn ensure_request_matches_identity_audience(
    data: &StoredIdentityRuntimeData,
    request: &reqwest::Request,
) -> Result<(), RequestIdentityHttpAuthenticatorError> {
    let audience_host = data
        .identity_spec
        .audience
        .get("host")
        .and_then(Value::as_str)
        .filter(|host| !host.trim().is_empty())
        .ok_or_else(|| {
            RequestIdentityHttpAuthenticatorError::failed_precondition(format!(
                "identity spec '{}' must declare string audience.host before identity '{}' can inject Authorization headers",
                data.identity_spec.name, data.identity_name
            ))
        })?;
    let request_host = request.url().host_str().ok_or_else(|| {
        RequestIdentityHttpAuthenticatorError::failed_precondition(format!(
            "identity '{}' cannot inject Authorization headers for URL without a host",
            data.identity_name
        ))
    })?;
    if host_matches_audience(request_host, audience_host) {
        return Ok(());
    }
    Err(RequestIdentityHttpAuthenticatorError::failed_precondition(
        format!(
            "identity '{}' audience host '{}' does not match request host '{}'",
            data.identity_name, audience_host, request_host
        ),
    ))
}

fn ensure_request_uses_credential_safe_transport(
    data: &StoredIdentityRuntimeData,
    request: &reqwest::Request,
) -> Result<(), RequestIdentityHttpAuthenticatorError> {
    let url = request.url();
    if url.scheme() == "https" || is_loopback_http_url(url) {
        return Ok(());
    }
    let host = url.host_str().unwrap_or("<missing>");
    Err(RequestIdentityHttpAuthenticatorError::failed_precondition(
        format!(
            "identity '{}' cannot inject Authorization headers over {}://{host}; provider identity requests must use https or loopback http",
            data.identity_name,
            url.scheme()
        ),
    ))
}

fn is_loopback_http_url(url: &reqwest::Url) -> bool {
    url.scheme() == "http"
        && url.host_str().is_some_and(|host| {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        })
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
    !audience_host.is_empty()
        && (request_host == audience_host
            || request_host
                .strip_suffix(&audience_host)
                .is_some_and(|prefix| prefix.ends_with('.')))
}

#[cfg(test)]
mod tests {
    use super::*;
    use coral_spec::parse_identity_manifest_yaml;

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

    async fn resolve_headers(
        prepared: &PreparedRuntimeIdentity,
        host: &str,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityHttpAuthenticatorError> {
        resolve_headers_for_url(prepared, &format!("https://{host}/user")).await
    }

    async fn resolve_headers_for_url(
        prepared: &PreparedRuntimeIdentity,
        url: &str,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityHttpAuthenticatorError> {
        let context = SelectedRequestIdentity::new(
            "demo".to_string(),
            prepared.identity.identity_spec_id().to_string(),
            prepared.identity.audience().clone(),
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

        let headers = resolve_headers(&prepared, "api.example.test")
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
    async fn identity_headers_reject_request_host_outside_audience() {
        let prepared = prepared_identity(
            fixed_token_identity_spec(),
            FIXED_TOKEN_MATERIAL_KEY,
            "token",
        )
        .await;

        let error = resolve_headers(&prepared, "api.example.test.attacker.test")
            .await
            .expect_err("mismatched host should not receive identity headers");

        assert!(
            error.to_string().contains("does not match request host"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn identity_headers_reject_non_https_provider_requests() {
        let prepared = prepared_identity(
            fixed_token_identity_spec(),
            FIXED_TOKEN_MATERIAL_KEY,
            "token",
        )
        .await;

        let error = resolve_headers_for_url(&prepared, "http://api.example.test/user")
            .await
            .expect_err("non-https provider request should not receive identity headers");

        assert!(
            error.to_string().contains("must use https"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn identity_headers_require_audience_host() {
        let prepared = prepared_identity(
            fixed_token_identity_spec_without_audience_host(),
            FIXED_TOKEN_MATERIAL_KEY,
            "token",
        )
        .await;

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
    fn audience_host_matching_allows_only_exact_or_subdomain_hosts() {
        assert!(host_matches_audience("example.test", "example.test"));
        assert!(host_matches_audience("api.example.test", "example.test"));
        assert!(host_matches_audience("API.EXAMPLE.TEST", "example.test"));
        assert!(host_matches_audience("api.example.test.", "example.test."));
        assert!(!host_matches_audience("badexample.test", "example.test"));
        assert!(!host_matches_audience(
            "example.test.attacker.test",
            "example.test"
        ));
        assert!(!host_matches_audience("github.com.evil.test", "github.com"));
        assert!(!host_matches_audience("example.test", ""));
    }
}
