//! Authentication header resolution for HTTP source manifests.
//!
//! Each [`AuthSpec`] variant implements [`Authenticator::auth`], returning the
//! list of headers to attach to an outbound request. Static variants (API keys,
//! Basic, raw headers) ignore the request-shaped fields on [`AuthContext`];
//! dynamic variants (AWS SigV4 and future signers) sign over them.

use std::collections::{BTreeMap, HashMap};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use datafusion::error::{DataFusionError, Result};

use coral_spec::{
    ApiKeyAuthSpec, AuthSpec, BasicHttpAuthSpec, CustomAuthSpec, CustomHeadersAuthSpec,
};

use crate::backends::shared::template::{render_template, resolve_value_source, value_to_string};

/// Context passed to [`Authenticator::auth`]. Populated just before a request
/// is sent, so dynamic authenticators can sign over the final method, URL,
/// headers, and body.
pub(crate) struct AuthContext<'a> {
    pub(crate) method: &'a reqwest::Method,
    pub(crate) url: &'a str,
    pub(crate) headers: &'a [(String, String)],
    pub(crate) body: Option<&'a [u8]>,
    pub(crate) resolved_inputs: &'a BTreeMap<String, String>,
}

pub(crate) trait Authenticator {
    fn auth(&self, ctx: &AuthContext<'_>) -> Result<Vec<(String, String)>>;
}

// AWS SigV4 impl lives in `super::custom::aws`; referencing the module here
// ensures its `impl Authenticator for AwsSigV4Spec` block is linked into the
// crate even though the module is only used via trait dispatch.
use super::custom::aws as _;

impl Authenticator for ApiKeyAuthSpec {
    fn auth(&self, ctx: &AuthContext<'_>) -> Result<Vec<(String, String)>> {
        let empty_filters: HashMap<String, String> = HashMap::new();
        let empty_state: HashMap<String, String> = HashMap::new();
        let token = render_template(
            &self.api_token,
            &empty_filters,
            &empty_state,
            ctx.resolved_inputs,
        )?;
        Ok(vec![(self.header.clone(), token)])
    }
}

impl Authenticator for BasicHttpAuthSpec {
    fn auth(&self, ctx: &AuthContext<'_>) -> Result<Vec<(String, String)>> {
        let empty_filters: HashMap<String, String> = HashMap::new();
        let empty_state: HashMap<String, String> = HashMap::new();
        let username = render_template(
            &self.username,
            &empty_filters,
            &empty_state,
            ctx.resolved_inputs,
        )?;
        let password = render_template(
            &self.password,
            &empty_filters,
            &empty_state,
            ctx.resolved_inputs,
        )?;
        let encoded = BASE64_STANDARD.encode(format!("{username}:{password}"));
        Ok(vec![(
            "Authorization".to_string(),
            format!("Basic {encoded}"),
        )])
    }
}

impl Authenticator for CustomHeadersAuthSpec {
    fn auth(&self, ctx: &AuthContext<'_>) -> Result<Vec<(String, String)>> {
        let empty_filters: HashMap<String, String> = HashMap::new();
        let empty_state: HashMap<String, String> = HashMap::new();
        let mut out = Vec::with_capacity(self.headers.len());
        for header in &self.headers {
            let resolved = resolve_value_source(
                &header.value,
                &empty_filters,
                &empty_state,
                ctx.resolved_inputs,
            )?
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "missing value for auth header '{}'",
                    header.name
                ))
            })?;
            out.push((header.name.clone(), value_to_string(&resolved)));
        }
        Ok(out)
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
    let header_snapshot: Vec<(String, String)> = built
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                value.to_str().unwrap_or("").to_string(),
            )
        })
        .collect();
    let body_bytes: Option<&[u8]> = built.body().and_then(|b| b.as_bytes());
    let ctx = AuthContext {
        method: built.method(),
        url: built.url().as_str(),
        headers: &header_snapshot,
        body: body_bytes,
        resolved_inputs,
    };

    let headers = match auth {
        AuthSpec::ApiKeyAuth(spec) => spec.auth(&ctx),
        AuthSpec::BasicHttpAuth(spec) => spec.auth(&ctx),
        AuthSpec::CustomHeadersAuth(spec) => spec.auth(&ctx),
        AuthSpec::CustomAuth(custom) => match custom {
            CustomAuthSpec::AwsSigV4(spec) => spec.auth(&ctx),
        },
    }?;

    for (name, value) in headers {
        let header_name = reqwest::header::HeaderName::try_from(name.as_str()).map_err(|e| {
            DataFusionError::Execution(format!("invalid auth header name '{name}': {e}"))
        })?;
        let header_value = reqwest::header::HeaderValue::try_from(value.as_str())
            .map_err(|e| DataFusionError::Execution(format!("invalid auth header value: {e}")))?;
        built.headers_mut().append(header_name, header_value);
    }
    Ok(built)
}
