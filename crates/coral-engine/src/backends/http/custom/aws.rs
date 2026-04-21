//! AWS Signature Version 4 signer.
//!
//! Produces `Authorization`, `X-Amz-Date`, and related headers for outbound
//! requests using the `aws-sigv4` crate. Configured from [`AwsSigV4Spec`]
//! and signs over the live request via [`AuthContext`].

use std::time::SystemTime;

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest, SigningSettings,
    UriPathNormalizationMode, sign as sigv4_sign,
};
use aws_sigv4::sign::v4;
use datafusion::error::{DataFusionError, Result};
use reqwest::header::{HeaderName, HeaderValue};

use coral_spec::AwsSigV4Spec;

use std::collections::BTreeMap;

use crate::backends::http::auth::{AuthContext, Authenticator};
use crate::backends::shared::template::{EMPTY_MAP, render_template, validate_input_dependencies};

/// Per-service `SigV4` settings. Most AWS APIs accept the library defaults,
/// but S3 needs path normalization disabled, single percent-encoding, and the
/// `X-Amz-Content-Sha256` header enabled to avoid `SignatureDoesNotMatch`.
fn signing_settings_for(service: &str) -> SigningSettings {
    let mut settings = SigningSettings::default();
    if matches!(service, "s3" | "s3-outposts") {
        settings.percent_encoding_mode = PercentEncodingMode::Single;
        settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
        settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    }
    settings
}

impl Authenticator for AwsSigV4Spec {
    fn auth(&self, ctx: &AuthContext<'_>) -> Result<Vec<(HeaderName, HeaderValue)>> {
        let region = render_template(&self.region, &EMPTY_MAP, &EMPTY_MAP, ctx.resolved_inputs)?;
        let access_key_id = render_template(
            &self.access_key_id,
            &EMPTY_MAP,
            &EMPTY_MAP,
            ctx.resolved_inputs,
        )?;
        let secret_access_key = render_template(
            &self.secret_access_key,
            &EMPTY_MAP,
            &EMPTY_MAP,
            ctx.resolved_inputs,
        )?;
        let session_token = self
            .session_token
            .as_ref()
            .map(|template| render_template(template, &EMPTY_MAP, &EMPTY_MAP, ctx.resolved_inputs))
            .transpose()?
            .filter(|value| !value.is_empty());

        let credentials = Credentials::new(
            access_key_id,
            secret_access_key,
            session_token,
            None,
            "coral",
        );
        let identity = credentials.into();

        let signing_params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&region)
            .name(&self.service)
            .time(SystemTime::now())
            .settings(signing_settings_for(&self.service))
            .build()
            .map_err(|error| {
                DataFusionError::Execution(format!("failed to build SigV4 signing params: {error}"))
            })?
            .into();

        let headers = ctx.headers();
        let mut header_refs = Vec::with_capacity(headers.len());
        for (name, value) in headers {
            let value_str = value.to_str().map_err(|error| {
                DataFusionError::Execution(format!(
                    "header '{name}' is not valid ASCII, cannot sign with SigV4: {error}"
                ))
            })?;
            header_refs.push((name.as_str(), value_str));
        }
        let body = SignableBody::Bytes(ctx.body().unwrap_or(&[]));
        let signable = SignableRequest::new(
            ctx.method().as_str(),
            ctx.url().as_str(),
            header_refs.iter().copied(),
            body,
        )
        .map_err(|error| {
            DataFusionError::Execution(format!("failed to build SigV4 signable request: {error}"))
        })?;

        let (instructions, _signature) = sigv4_sign(signable, &signing_params)
            .map_err(|error| DataFusionError::Execution(format!("SigV4 signing failed: {error}")))?
            .into_parts();

        let (headers, _params) = instructions.into_parts();
        let mut out = Vec::with_capacity(headers.len());
        for header in headers {
            let name = HeaderName::try_from(header.name()).map_err(|error| {
                DataFusionError::Execution(format!(
                    "SigV4 returned invalid header name '{}': {error}",
                    header.name()
                ))
            })?;
            let value = HeaderValue::try_from(header.value()).map_err(|error| {
                DataFusionError::Execution(format!(
                    "SigV4 returned invalid header value for '{}': {error}",
                    header.name()
                ))
            })?;
            out.push((name, value));
        }
        Ok(out)
    }

    fn validate_inputs(&self, resolved_inputs: &BTreeMap<String, String>) -> Result<()> {
        validate_input_dependencies(&self.region, resolved_inputs)?;
        validate_input_dependencies(&self.access_key_id, resolved_inputs)?;
        validate_input_dependencies(&self.secret_access_key, resolved_inputs)?;
        if let Some(session_token) = &self.session_token {
            validate_input_dependencies(session_token, resolved_inputs)?;
        }
        Ok(())
    }
}
