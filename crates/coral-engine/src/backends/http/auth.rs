//! Authentication header resolution for HTTP source manifests.

use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use datafusion::error::{DataFusionError, Result};
use reqwest::header::{HeaderName, HeaderValue};

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

fn ensure_auth_uses_credential_safe_transport(url: &reqwest::Url) -> Result<()> {
    if url.scheme() == "https" || is_loopback_http_url(url) {
        return Ok(());
    }
    let host = url.host_str().unwrap_or("<missing>");
    Err(DataFusionError::Execution(format!(
        "HTTP source auth headers require https or loopback http, got {}://{host}",
        url.scheme()
    )))
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

    #[test]
    fn source_auth_rejects_non_https_non_loopback_provider_url() {
        let http = reqwest::Client::new();
        let request = http.get("http://api.example.test/items");
        let resolved_inputs = BTreeMap::from([("API_TOKEN".to_string(), "secret".to_string())]);

        let error = resolve_auth_headers(
            &bearer_auth(),
            request,
            &HashMap::new(),
            &resolved_inputs,
            true,
        )
        .expect_err("non-https non-loopback auth request should fail");

        assert!(
            error.to_string().contains("require https"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn source_auth_allows_loopback_http_provider_url() {
        let http = reqwest::Client::new();
        let request = http.get("http://127.0.0.1:8080/items");
        let resolved_inputs = BTreeMap::from([("API_TOKEN".to_string(), "secret".to_string())]);

        let built = resolve_auth_headers(
            &bearer_auth(),
            request,
            &HashMap::new(),
            &resolved_inputs,
            true,
        )
        .expect("loopback http auth should be allowed");

        assert_eq!(
            built.headers().get(reqwest::header::AUTHORIZATION),
            Some(&HeaderValue::from_static("Bearer secret"))
        );
    }

    #[test]
    fn source_auth_can_skip_transport_guard_for_legacy_sources() {
        let http = reqwest::Client::new();
        let request = http.get("http://api.example.test/items");
        let resolved_inputs = BTreeMap::from([("API_TOKEN".to_string(), "secret".to_string())]);

        let built = resolve_auth_headers(
            &bearer_auth(),
            request,
            &HashMap::new(),
            &resolved_inputs,
            false,
        )
        .expect("legacy source auth policy should allow existing http behavior");

        assert_eq!(
            built.headers().get(reqwest::header::AUTHORIZATION),
            Some(&HeaderValue::from_static("Bearer secret"))
        );
    }
}
