//! Prepared request authentication for coherent stored identities.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "B5e2 prepares the app-owned adapter that B7 wires into query runtimes"
    )
)]

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use coral_engine::{
    BoundRequestIdentityHttpAuthenticator, RequestIdentityHttpAuthenticatorError,
    SelectedRequestIdentity,
};
use coral_spec::IdentitySpecType;
use reqwest::header::{AUTHORIZATION, HeaderValue};
use serde_json::Value;
use url::{Host, Url};

use crate::bootstrap::AppError;
use crate::identities::manager::{
    FIXED_TOKEN_KEY, IdentityManager, OAUTH_ACCESS_TOKEN_KEY, ResolvedIdentityForUse,
};
use crate::identities::model::IdentityOwner;

/// One coherent identity snapshot prepared for the engine's synchronous factory handoff.
pub(crate) struct PreparedBearerIdentity {
    selected: SelectedRequestIdentity,
    resolved: ResolvedIdentityForUse,
    material_key: &'static str,
}

impl PreparedBearerIdentity {
    fn new(resolved: ResolvedIdentityForUse) -> Arc<Self> {
        let manifest = &resolved.identity_spec.spec.manifest;
        let material_key = match manifest.identity_type {
            IdentitySpecType::FixedToken => FIXED_TOKEN_KEY,
            IdentitySpecType::OAuth => OAUTH_ACCESS_TOKEN_KEY,
        };
        let selected = SelectedRequestIdentity::new(
            format!("request-identity-{}", uuid::Uuid::new_v4().simple()),
            manifest.name.clone(),
            manifest.audience.clone(),
        );
        Arc::new(Self {
            selected,
            resolved,
            material_key,
        })
    }

    /// Returns the exact selection B7 will hand to the engine.
    pub(crate) fn selected_identity(&self) -> SelectedRequestIdentity {
        self.selected.clone()
    }

    /// Retains the complete material and revision needed by B5f refresh CAS.
    pub(crate) fn resolved_for_refresh(&self) -> &ResolvedIdentityForUse {
        &self.resolved
    }

    /// Binds the exact engine selection to this prepared coherent snapshot.
    pub(crate) fn bind(
        self: &Arc<Self>,
        selected: &SelectedRequestIdentity,
    ) -> Result<BoundRequestIdentityHttpAuthenticator, RequestIdentityHttpAuthenticatorError> {
        if selected != &self.selected {
            return Err(RequestIdentityHttpAuthenticatorError::failed_precondition(
                "selected identity does not match the prepared identity snapshot",
            ));
        }
        let prepared = Arc::clone(self);
        Ok(Arc::new(move |request, _resolved_inputs| {
            let prepared = Arc::clone(&prepared);
            Box::pin(async move { prepared.headers_for_request(request) })
        }))
    }

    fn headers_for_request(
        &self,
        request: &reqwest::Request,
    ) -> Result<
        Vec<(reqwest::header::HeaderName, HeaderValue)>,
        RequestIdentityHttpAuthenticatorError,
    > {
        validate_request(request, self.selected.audience())?;
        let token = self
            .resolved
            .material()
            .get(self.material_key)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                RequestIdentityHttpAuthenticatorError::failed_precondition(
                    "prepared identity is missing bearer credential material",
                )
            })?;
        Ok(vec![(AUTHORIZATION, bearer_header(token)?)])
    }
}

impl fmt::Debug for PreparedBearerIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedBearerIdentity")
            .field("material_value_count", &self.resolved.material().len())
            .finish_non_exhaustive()
    }
}

impl IdentityManager {
    /// Resolves and prepares one fixed-token or OAuth bearer identity, refreshing when necessary.
    pub(crate) async fn prepare_bearer_for_use(
        &self,
        owner: &IdentityOwner,
        identity_name: &str,
    ) -> Result<Arc<PreparedBearerIdentity>, AppError> {
        Ok(PreparedBearerIdentity::new(
            self.get_for_use(owner, identity_name).await?,
        ))
    }
}

fn validate_request_url(
    url: &Url,
    audience: &BTreeMap<String, Value>,
) -> Result<(), RequestIdentityHttpAuthenticatorError> {
    if !url.username().is_empty() || url.password().is_some() || url_has_userinfo(url) {
        return Err(RequestIdentityHttpAuthenticatorError::failed_precondition(
            "identity authentication requires a request URL without user information",
        ));
    }
    if url.fragment().is_some() {
        return Err(RequestIdentityHttpAuthenticatorError::failed_precondition(
            "identity authentication requires a request URL without a fragment",
        ));
    }
    let request_host = url.host().ok_or_else(|| {
        RequestIdentityHttpAuthenticatorError::failed_precondition(
            "identity authentication requires a request URL with a host",
        )
    })?;
    let transport_is_safe = match (url.scheme(), &request_host) {
        ("https", _) => true,
        ("http", Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        ("http", Host::Ipv4(address)) => address.is_loopback(),
        ("http", Host::Ipv6(address)) => address.is_loopback(),
        _ => false,
    };
    if !transport_is_safe {
        return Err(RequestIdentityHttpAuthenticatorError::failed_precondition(
            "identity authentication requires HTTPS or exact loopback HTTP",
        ));
    }

    let audience_host = audience
        .get("host")
        .and_then(Value::as_str)
        .filter(|host| !host.trim().is_empty())
        .ok_or_else(|| {
            RequestIdentityHttpAuthenticatorError::failed_precondition(
                "identity audience.host must be a non-empty string",
            )
        })?;
    let audience_host = Host::parse(audience_host).map_err(|_error| {
        RequestIdentityHttpAuthenticatorError::failed_precondition(
            "identity audience.host must be a valid typed host",
        )
    })?;
    if audience_host != request_host {
        return Err(RequestIdentityHttpAuthenticatorError::failed_precondition(
            "identity audience host does not match the request host",
        ));
    }

    if let Some(port) = audience.get("port") {
        let expected_port = port
            .as_u64()
            .and_then(|port| u16::try_from(port).ok())
            .filter(|port| *port != 0)
            .ok_or_else(|| {
                RequestIdentityHttpAuthenticatorError::failed_precondition(
                    "identity audience.port must be an integer from 1 through 65535",
                )
            })?;
        if url.port_or_known_default() != Some(expected_port) {
            return Err(RequestIdentityHttpAuthenticatorError::failed_precondition(
                "identity audience port does not match the effective request port",
            ));
        }
    }
    Ok(())
}

fn validate_request(
    request: &reqwest::Request,
    audience: &BTreeMap<String, Value>,
) -> Result<(), RequestIdentityHttpAuthenticatorError> {
    validate_request_url(request.url(), audience)?;
    if request.headers().contains_key(AUTHORIZATION) {
        return Err(RequestIdentityHttpAuthenticatorError::failed_precondition(
            "identity authentication cannot replace an existing Authorization header",
        ));
    }
    Ok(())
}

fn bearer_header(token: &str) -> Result<HeaderValue, RequestIdentityHttpAuthenticatorError> {
    let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_error| {
        RequestIdentityHttpAuthenticatorError::failed_precondition(
            "prepared identity bearer credential cannot be encoded as an HTTP header",
        )
    })?;
    value.set_sensitive(true);
    Ok(value)
}

fn url_has_userinfo(url: &Url) -> bool {
    let Some(authority) = url
        .as_str()
        .split_once("://")
        .map(|(_, remainder)| remainder.split(['/', '?', '#']).next().unwrap_or_default())
    else {
        return false;
    };
    authority.contains('@')
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use reqwest::header::{AUTHORIZATION, HeaderValue};
    use serde_json::{Value, json};

    use super::{bearer_header, validate_request, validate_request_url};

    fn request(url: &str) -> reqwest::Request {
        reqwest::Request::new(reqwest::Method::GET, url.parse().expect("test URL"))
    }

    fn audience(host: Value, port: Option<Value>) -> BTreeMap<String, Value> {
        let mut audience = BTreeMap::from([
            ("host".to_string(), host),
            ("tenant".to_string(), json!({"id": 7})),
        ]);
        if let Some(port) = port {
            audience.insert("port".to_string(), port);
        }
        audience
    }

    #[test]
    fn request_audience_accepts_exact_typed_hosts_and_effective_ports() {
        for (url, host, port) in [
            (
                "https://api.example.test/path",
                json!("api.example.test"),
                None,
            ),
            (
                "https://api.example.test/path",
                json!("api.example.test"),
                Some(json!(443)),
            ),
            (
                "https://api.example.test:443/path",
                json!("api.example.test"),
                Some(json!(443)),
            ),
            ("https://127.0.0.1/path", json!("127.0.0.1"), None),
            ("https://[::1]/path", json!("[::1]"), None),
            ("http://localhost/path", json!("localhost"), Some(json!(80))),
            ("http://127.0.0.1/path", json!("127.0.0.1"), None),
            ("http://[::1]/path", json!("[::1]"), None),
        ] {
            validate_request_url(request(url).url(), &audience(host, port))
                .unwrap_or_else(|error| panic!("{url} should pass: {error}"));
        }
    }

    #[test]
    fn request_audience_rejects_host_and_port_substitution() {
        for (url, host, port) in [
            ("https://api.example.test", json!("example.test"), None),
            ("https://example.test", json!("api.example.test"), None),
            ("https://example.test.attacker", json!("example.test"), None),
            ("https://127.0.0.1", json!("localhost"), None),
            (
                "https://example.test",
                json!("example.test"),
                Some(json!(444)),
            ),
            (
                "https://example.test",
                json!("example.test"),
                Some(json!("443")),
            ),
            (
                "https://example.test",
                json!("example.test"),
                Some(json!(443.0)),
            ),
            (
                "https://example.test",
                json!("example.test"),
                Some(json!(0)),
            ),
            (
                "https://example.test",
                json!("example.test"),
                Some(json!(65536)),
            ),
        ] {
            assert!(
                validate_request_url(request(url).url(), &audience(host, port)).is_err(),
                "{url} should reject the supplied audience"
            );
        }
    }

    #[test]
    fn request_audience_rejects_malformed_host_and_unsafe_urls() {
        let valid = audience(json!("example.test"), None);
        for url in [
            "http://example.test/path",
            "ftp://example.test/path",
            "https://user@example.test/path",
            "https://example.test/path#fragment",
        ] {
            assert!(
                validate_request_url(request(url).url(), &valid).is_err(),
                "{url} should be unsafe"
            );
        }
        for host in [Value::Null, json!(7), json!(""), json!("bad host")] {
            assert!(
                validate_request_url(request("https://example.test").url(), &audience(host, None),)
                    .is_err()
            );
        }
    }

    #[test]
    fn existing_authorization_and_invalid_bearer_values_fail_closed() {
        let mut existing = request("https://example.test");
        existing
            .headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_static("Basic existing"));
        assert!(validate_request(&existing, &audience(json!("example.test"), None)).is_err());

        let canary = "token-canary\r\ninjected: value";
        let error = bearer_header(canary).expect_err("CRLF token must fail");
        assert!(!error.to_string().contains(canary));
    }
}
