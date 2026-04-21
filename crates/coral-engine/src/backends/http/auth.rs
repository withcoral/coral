//! Authentication header resolution for HTTP source manifests.
//!
//! Each [`AuthSpec`] variant implements [`Authenticator::auth`], returning the
//! list of typed headers to attach to an outbound request. Static variants
//! (Basic, declarative headers) ignore the request-shaped fields on
//! [`AuthContext`]; dynamic variants (AWS `SigV4` and future signers) sign over
//! them.

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use datafusion::error::{DataFusionError, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use coral_spec::{AuthSpec, BasicAuthSpec, CustomAuthSpec, HeaderAuthSpec};

use crate::backends::shared::template::{
    EMPTY_MAP, render_template, resolve_value_source, value_to_string,
};

/// Context passed to [`Authenticator::auth`]. Wraps the fully-built
/// `reqwest::Request` so authenticators can read whatever parts they need
/// (method, URL, headers, body) without the caller pre-extracting each field.
pub(crate) struct AuthContext<'a> {
    pub(crate) request: &'a reqwest::Request,
    pub(crate) resolved_inputs: &'a BTreeMap<String, String>,
}

impl<'a> AuthContext<'a> {
    pub(crate) fn method(&self) -> &reqwest::Method {
        self.request.method()
    }

    pub(crate) fn url(&self) -> &reqwest::Url {
        self.request.url()
    }

    pub(crate) fn headers(&self) -> &HeaderMap {
        self.request.headers()
    }

    pub(crate) fn body(&self) -> Option<&'a [u8]> {
        self.request.body().and_then(|b| b.as_bytes())
    }
}

pub(crate) trait Authenticator {
    fn auth(&self, ctx: &AuthContext<'_>) -> Result<Vec<(HeaderName, HeaderValue)>>;

    /// Registration-time check: every template / value source this
    /// authenticator depends on must resolve from `resolved_inputs` (or a
    /// token-level default). Called before any request is issued so invalid
    /// source configs fail import instead of first fetch.
    fn validate_inputs(&self, resolved_inputs: &BTreeMap<String, String>) -> Result<()>;
}

impl Authenticator for BasicAuthSpec {
    fn auth(&self, ctx: &AuthContext<'_>) -> Result<Vec<(HeaderName, HeaderValue)>> {
        let username =
            render_template(&self.username, &EMPTY_MAP, &EMPTY_MAP, ctx.resolved_inputs)?;
        let password =
            render_template(&self.password, &EMPTY_MAP, &EMPTY_MAP, ctx.resolved_inputs)?;
        let encoded = BASE64_STANDARD.encode(format!("{username}:{password}"));
        let value = HeaderValue::try_from(format!("Basic {encoded}").as_str()).map_err(|e| {
            DataFusionError::Execution(format!("invalid Basic auth header value: {e}"))
        })?;
        Ok(vec![(reqwest::header::AUTHORIZATION, value)])
    }

    fn validate_inputs(&self, resolved_inputs: &BTreeMap<String, String>) -> Result<()> {
        render_template(&self.username, &EMPTY_MAP, &EMPTY_MAP, resolved_inputs)?;
        render_template(&self.password, &EMPTY_MAP, &EMPTY_MAP, resolved_inputs)?;
        Ok(())
    }
}

impl Authenticator for HeaderAuthSpec {
    fn auth(&self, ctx: &AuthContext<'_>) -> Result<Vec<(HeaderName, HeaderValue)>> {
        let mut out = Vec::with_capacity(self.headers.len());
        for header in &self.headers {
            let resolved =
                resolve_value_source(&header.value, &EMPTY_MAP, &EMPTY_MAP, ctx.resolved_inputs)?
                    .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "missing value for auth header '{}'",
                        header.name
                    ))
                })?;
            let name = HeaderName::try_from(header.name.as_str()).map_err(|e| {
                DataFusionError::Execution(format!(
                    "invalid auth header name '{}': {e}",
                    header.name
                ))
            })?;
            let value =
                HeaderValue::try_from(value_to_string(&resolved).as_str()).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "invalid auth header value for '{}': {e}",
                        header.name
                    ))
                })?;
            out.push((name, value));
        }
        Ok(out)
    }

    fn validate_inputs(&self, resolved_inputs: &BTreeMap<String, String>) -> Result<()> {
        for header in &self.headers {
            let resolved =
                resolve_value_source(&header.value, &EMPTY_MAP, &EMPTY_MAP, resolved_inputs)?
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "missing value for auth header '{}'",
                            header.name
                        ))
                    })?;
            HeaderName::try_from(header.name.as_str()).map_err(|e| {
                DataFusionError::Execution(format!(
                    "invalid auth header name '{}': {e}",
                    header.name
                ))
            })?;
            let _ = HeaderValue::try_from(value_to_string(&resolved).as_str()).map_err(|e| {
                DataFusionError::Execution(format!(
                    "invalid auth header value for '{}': {e}",
                    header.name
                ))
            })?;
        }
        Ok(())
    }
}

impl Authenticator for CustomAuthSpec {
    fn auth(&self, ctx: &AuthContext<'_>) -> Result<Vec<(HeaderName, HeaderValue)>> {
        match self {
            CustomAuthSpec::AwsSigV4(spec) => spec.auth(ctx),
        }
    }

    fn validate_inputs(&self, resolved_inputs: &BTreeMap<String, String>) -> Result<()> {
        match self {
            CustomAuthSpec::AwsSigV4(spec) => spec.validate_inputs(resolved_inputs),
        }
    }
}

impl Authenticator for AuthSpec {
    fn auth(&self, ctx: &AuthContext<'_>) -> Result<Vec<(HeaderName, HeaderValue)>> {
        match self {
            AuthSpec::BasicAuth(spec) => spec.auth(ctx),
            AuthSpec::HeaderAuth(spec) => spec.auth(ctx),
            AuthSpec::CustomAuth(spec) => spec.auth(ctx),
        }
    }

    fn validate_inputs(&self, resolved_inputs: &BTreeMap<String, String>) -> Result<()> {
        match self {
            AuthSpec::BasicAuth(spec) => spec.validate_inputs(resolved_inputs),
            AuthSpec::HeaderAuth(spec) => spec.validate_inputs(resolved_inputs),
            AuthSpec::CustomAuth(spec) => spec.validate_inputs(resolved_inputs),
        }
    }
}

/// Materialize the request, run the variant-specific [`Authenticator`] over
/// its final shape, and attach the returned headers. Takes ownership of the
/// builder and returns the ready-to-send [`reqwest::Request`].
pub(crate) fn resolve_auth_headers(
    auth: &AuthSpec,
    request: reqwest::RequestBuilder,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<reqwest::Request> {
    let mut built = request.build().map_err(|error| {
        DataFusionError::Execution(format!("failed to build HTTP request: {error}"))
    })?;
    let headers = auth.auth(&AuthContext {
        request: &built,
        resolved_inputs,
    })?;
    // `insert` (not `append`) so the computed auth value is authoritative —
    // any header already attached to the built request under the same name
    // (from `request_headers:`, per-request headers, or reqwest defaults) is
    // replaced rather than duplicated.
    for (name, value) in headers {
        built.headers_mut().insert(name, value);
    }
    Ok(built)
}
