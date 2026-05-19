//! Authentication header resolution for HTTP source manifests.

use std::collections::{BTreeMap, HashMap};
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
    for (name, value) in headers {
        built.headers_mut().insert(name, value);
    }
    Ok(built)
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
