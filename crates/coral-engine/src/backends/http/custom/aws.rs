//! AWS Signature Version 4 signer.
//!
//! Produces `Authorization`, `X-Amz-Date`, and related headers for outbound
//! requests using the `aws-sigv4` crate. Configured from [`AwsSigV4Spec`]
//! and signs over the live request via [`AuthContext`].

use std::collections::HashMap;
use std::time::SystemTime;

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    SignableBody, SignableRequest, SigningSettings, sign as sigv4_sign,
};
use aws_sigv4::sign::v4;
use datafusion::error::{DataFusionError, Result};

use coral_spec::AwsSigV4Spec;

use crate::backends::http::auth::{AuthContext, Authenticator};
use crate::backends::shared::template::render_template;

impl Authenticator for AwsSigV4Spec {
    fn auth(&self, ctx: &AuthContext<'_>) -> Result<Vec<(String, String)>> {
        let empty_filters: HashMap<String, String> = HashMap::new();
        let empty_state: HashMap<String, String> = HashMap::new();
        let region =
            render_template(&self.region, &empty_filters, &empty_state, ctx.resolved_inputs)?;
        let access_key_id = render_template(
            &self.access_key_id,
            &empty_filters,
            &empty_state,
            ctx.resolved_inputs,
        )?;
        let secret_access_key = render_template(
            &self.secret_access_key,
            &empty_filters,
            &empty_state,
            ctx.resolved_inputs,
        )?;
        let session_token = self
            .session_token
            .as_ref()
            .map(|template| {
                render_template(template, &empty_filters, &empty_state, ctx.resolved_inputs)
            })
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
            .settings(SigningSettings::default())
            .build()
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "failed to build SigV4 signing params: {error}"
                ))
            })?
            .into();

        let header_refs = ctx
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()));
        let body = SignableBody::Bytes(ctx.body.unwrap_or(&[]));
        let signable = SignableRequest::new(ctx.method.as_str(), ctx.url, header_refs, body)
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "failed to build SigV4 signable request: {error}"
                ))
            })?;

        let (instructions, _signature) = sigv4_sign(signable, &signing_params)
            .map_err(|error| {
                DataFusionError::Execution(format!("SigV4 signing failed: {error}"))
            })?
            .into_parts();

        let (headers, _params) = instructions.into_parts();
        Ok(headers
            .into_iter()
            .map(|header| (header.name().to_string(), header.value().to_string()))
            .collect())
    }
}
