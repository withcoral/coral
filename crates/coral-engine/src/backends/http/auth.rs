//! Authentication header resolution for HTTP source manifests.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use datafusion::error::{DataFusionError, Result};
use reqwest::header::{HeaderName, HeaderValue};
use url::Host;

use coral_spec::{AuthSpec, BasicAuthSpec, HeaderAuthSpec};

use crate::RequestAuthenticator;
use crate::backends::shared::template::{
    RenderContext, render_template, resolve_value_source, value_to_string,
};

/// Built-in auth variants resolve their headers from resolved inputs only;
/// they do not need access to the fully built request.
trait BuiltinAuth {
    fn authenticate(
        &self,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<Vec<(HeaderName, HeaderValue)>>;

    fn validate(&self, resolved_inputs: &BTreeMap<String, String>) -> Result<()>;
}

impl BuiltinAuth for BasicAuthSpec {
    fn authenticate(
        &self,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<Vec<(HeaderName, HeaderValue)>> {
        let context = RenderContext::source_scoped(resolved_inputs);
        let username = render_template(&self.username, &context)?;
        let password = render_template(&self.password, &context)?;
        let encoded = BASE64_STANDARD.encode(format!("{username}:{password}"));
        let value =
            HeaderValue::try_from(format!("Basic {encoded}").as_str()).map_err(|error| {
                DataFusionError::Execution(format!("invalid Basic auth header value: {error}"))
            })?;
        Ok(vec![(reqwest::header::AUTHORIZATION, value)])
    }

    fn validate(&self, resolved_inputs: &BTreeMap<String, String>) -> Result<()> {
        let context = RenderContext::source_scoped(resolved_inputs);
        render_template(&self.username, &context)?;
        render_template(&self.password, &context)?;
        Ok(())
    }
}

impl BuiltinAuth for HeaderAuthSpec {
    fn authenticate(
        &self,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<Vec<(HeaderName, HeaderValue)>> {
        let mut out = Vec::with_capacity(self.headers.len());
        let context = RenderContext::source_scoped(resolved_inputs);
        for header in &self.headers {
            let resolved = resolve_value_source(&header.value, &context)?.ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "missing value for auth header '{}'",
                    header.name
                ))
            })?;
            let name = HeaderName::try_from(header.name.as_str()).map_err(|error| {
                DataFusionError::Execution(format!(
                    "invalid auth header name '{}': {error}",
                    header.name
                ))
            })?;
            let value =
                HeaderValue::try_from(value_to_string(&resolved).as_str()).map_err(|error| {
                    DataFusionError::Execution(format!(
                        "invalid auth header value for '{}': {error}",
                        header.name
                    ))
                })?;
            out.push((name, value));
        }
        Ok(out)
    }

    fn validate(&self, resolved_inputs: &BTreeMap<String, String>) -> Result<()> {
        let _ = <Self as BuiltinAuth>::authenticate(self, resolved_inputs)?;
        Ok(())
    }
}

pub(crate) fn validate_auth_inputs(
    auth: &AuthSpec,
    request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    match auth {
        AuthSpec::BasicAuth(spec) => spec.validate(resolved_inputs),
        AuthSpec::HeaderAuth(spec) => spec.validate(resolved_inputs),
        AuthSpec::CustomAuth(spec) => {
            let authenticator = get_custom_authenticator(request_authenticators, spec)?;
            authenticator
                .validate(spec, resolved_inputs)
                .map_err(|error| authenticator_error(&spec.authenticator, &error))
        }
    }
}

pub(crate) fn resolve_auth_headers(
    auth: &AuthSpec,
    request: reqwest::RequestBuilder,
    request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
    resolved_inputs: &BTreeMap<String, String>,
    require_credential_safe_transport: bool,
) -> Result<reqwest::Request> {
    let mut built = request.build().map_err(|error| {
        DataFusionError::Execution(format!("failed to build HTTP request: {error}"))
    })?;
    let headers = match auth {
        AuthSpec::BasicAuth(spec) => spec.authenticate(resolved_inputs),
        AuthSpec::HeaderAuth(spec) => spec.authenticate(resolved_inputs),
        AuthSpec::CustomAuth(spec) => {
            let authenticator = get_custom_authenticator(request_authenticators, spec)?;
            authenticator
                .authenticate(spec, &built, resolved_inputs)
                .map_err(|error| authenticator_error(&spec.authenticator, &error))
        }
    }?;
    if require_credential_safe_transport && !headers.is_empty() {
        ensure_auth_uses_credential_safe_transport(built.url())?;
    }
    for (name, value) in headers {
        built.headers_mut().insert(name, value);
    }
    Ok(built)
}

pub(super) fn ensure_auth_uses_credential_safe_transport(url: &reqwest::Url) -> Result<()> {
    if is_credential_safe_auth_transport(url) {
        return Ok(());
    }
    Err(DataFusionError::Execution(format!(
        "HTTP source auth headers require https or loopback http, got '{}'",
        auth_transport_url_label(url)
    )))
}

pub(super) fn is_credential_safe_auth_transport(url: &reqwest::Url) -> bool {
    url.scheme() == "https" || is_loopback_http_url(url)
}

fn is_loopback_http_url(url: &reqwest::Url) -> bool {
    if url.scheme() != "http" {
        return false;
    }
    match url.host() {
        Some(Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

fn auth_transport_url_label(url: &reqwest::Url) -> String {
    url.origin().ascii_serialization()
}

fn get_custom_authenticator<'a>(
    request_authenticators: &'a HashMap<String, Arc<dyn RequestAuthenticator>>,
    spec: &coral_spec::CustomAuthSpec,
) -> Result<&'a Arc<dyn RequestAuthenticator>> {
    request_authenticators
        .get(&spec.authenticator)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "custom authenticator '{}' is not registered",
                spec.authenticator
            ))
        })
}

fn authenticator_error(name: &str, error: &crate::RequestAuthenticatorError) -> DataFusionError {
    DataFusionError::Execution(format!("custom authenticator '{name}' failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use coral_spec::{HeaderSpec, ParsedTemplate, ValueSourceSpec};

    fn bearer_auth() -> AuthSpec {
        AuthSpec::HeaderAuth(HeaderAuthSpec {
            headers: vec![HeaderSpec {
                name: "Authorization".to_string(),
                value: ValueSourceSpec::Template {
                    template: ParsedTemplate::parse("Bearer {{input.API_TOKEN}}")
                        .expect("auth template"),
                },
            }],
        })
    }

    fn resolve_bearer_auth_for_url(
        url: &str,
        require_credential_safe_transport: bool,
    ) -> Result<reqwest::Request> {
        let http = reqwest::Client::new();
        let request = http.get(url);
        let resolved_inputs = BTreeMap::from([("API_TOKEN".to_string(), "secret".to_string())]);
        resolve_auth_headers(
            &bearer_auth(),
            request,
            &HashMap::new(),
            &resolved_inputs,
            require_credential_safe_transport,
        )
    }

    #[test]
    fn v4_auth_rejects_non_https_non_loopback_provider_urls() {
        for url in [
            "http://api.example.test/items",
            "http://[::2]:8080/items",
            "http://[2001:db8::1]/items",
        ] {
            let error = resolve_bearer_auth_for_url(url, true)
                .expect_err("non-https non-loopback auth request should fail");

            let error = error.to_string();
            assert!(error.contains("require https"), "{error}");
            assert!(
                error.contains(url.split("/items").next().expect("authority")),
                "{error}"
            );
        }
    }

    #[test]
    fn auth_transport_url_label_contains_only_the_origin() {
        let url = reqwest::Url::parse(
            "http://member:secret@api.example.test/items?api_key=hidden#fragment",
        )
        .expect("test URL");

        assert_eq!(auth_transport_url_label(&url), "http://api.example.test");
    }

    #[test]
    fn v4_auth_allows_loopback_http_provider_urls() {
        for url in [
            "http://127.0.0.1:8080/items",
            "http://localhost:8080/items",
            "http://[::1]/items",
            "http://[::1]:8080/items",
        ] {
            let built = resolve_bearer_auth_for_url(url, true)
                .expect("loopback http auth should be allowed");

            assert_eq!(
                built.headers().get(reqwest::header::AUTHORIZATION),
                Some(&HeaderValue::from_static("Bearer secret")),
                "{url}"
            );
        }
    }

    #[test]
    fn v3_auth_can_skip_transport_guard_for_legacy_sources() {
        let built = resolve_bearer_auth_for_url("http://api.example.test/items", false)
            .expect("legacy source auth policy should allow existing http behavior");

        assert_eq!(
            built.headers().get(reqwest::header::AUTHORIZATION),
            Some(&HeaderValue::from_static("Bearer secret"))
        );
    }
}
