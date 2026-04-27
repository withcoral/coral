//! AWS request authenticators for Coral sources.

use std::collections::BTreeMap;
use std::time::SystemTime;

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest, SigningSettings,
    UriPathNormalizationMode, sign as sigv4_sign,
};
use aws_sigv4::sign::v4;
use coral_engine::{RequestAuthenticator, RequestAuthenticatorError};
use coral_spec::{CustomAuthSpec, ParsedTemplate, TemplateNamespace, TemplatePart};
use reqwest::header::{HeaderName, HeaderValue};
use serde::Deserialize;

/// Built-in request authenticator for `auth.authenticator = "aws_sigv4"`.
#[derive(Debug, Default)]
pub struct AwsSigV4Authenticator;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AwsSigV4Config {
    service: String,
    region: ParsedTemplate,
    access_key_id: ParsedTemplate,
    secret_access_key: ParsedTemplate,
    #[serde(default)]
    session_token: Option<ParsedTemplate>,
}

impl RequestAuthenticator for AwsSigV4Authenticator {
    fn name(&self) -> &'static str {
        "aws_sigv4"
    }

    fn authenticate(
        &self,
        auth: &CustomAuthSpec,
        request: &reqwest::Request,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestAuthenticatorError> {
        let config = parse_config(auth)?;
        let region = render_input_template(&config.region, resolved_inputs)?;
        let access_key_id = render_input_template(&config.access_key_id, resolved_inputs)?;
        let secret_access_key = render_input_template(&config.secret_access_key, resolved_inputs)?;
        let session_token = config
            .session_token
            .as_ref()
            .map(|template| render_input_template(template, resolved_inputs))
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
            .name(&config.service)
            .time(SystemTime::now())
            .settings(signing_settings_for(&config.service))
            .build()
            .map_err(|error| {
                RequestAuthenticatorError::failed_precondition(format!(
                    "failed to build SigV4 signing params: {error}"
                ))
            })?
            .into();

        let mut header_refs = Vec::with_capacity(request.headers().len());
        for (name, value) in request.headers() {
            let value_str = value.to_str().map_err(|error| {
                RequestAuthenticatorError::failed_precondition(format!(
                    "header '{name}' is not valid ASCII, cannot sign with SigV4: {error}"
                ))
            })?;
            header_refs.push((name.as_str(), value_str));
        }

        let body = match request.body() {
            Some(request_body) => {
                let bytes = request_body.as_bytes().ok_or_else(|| {
                    RequestAuthenticatorError::failed_precondition(
                        "cannot sign SigV4 request with unbuffered body".to_string(),
                    )
                })?;
                SignableBody::Bytes(bytes)
            }
            None => SignableBody::Bytes(&[]),
        };
        let signable = SignableRequest::new(
            request.method().as_str(),
            request.url().as_str(),
            header_refs.iter().copied(),
            body,
        )
        .map_err(|error| {
            RequestAuthenticatorError::failed_precondition(format!(
                "failed to build SigV4 signable request: {error}"
            ))
        })?;

        let (instructions, _signature) = sigv4_sign(signable, &signing_params)
            .map_err(|error| {
                RequestAuthenticatorError::failed_precondition(format!(
                    "SigV4 signing failed: {error}"
                ))
            })?
            .into_parts();

        let (headers, _params) = instructions.into_parts();
        let mut out = Vec::with_capacity(headers.len());
        for header in headers {
            let name = HeaderName::try_from(header.name()).map_err(|error| {
                RequestAuthenticatorError::failed_precondition(format!(
                    "SigV4 returned invalid header name '{}': {error}",
                    header.name()
                ))
            })?;
            let value = HeaderValue::try_from(header.value()).map_err(|error| {
                RequestAuthenticatorError::failed_precondition(format!(
                    "SigV4 returned invalid header value for '{}': {error}",
                    header.name()
                ))
            })?;
            out.push((name, value));
        }
        Ok(out)
    }

    fn validate(
        &self,
        auth: &CustomAuthSpec,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<(), RequestAuthenticatorError> {
        let config = parse_config(auth)?;
        let _ = render_input_template(&config.region, resolved_inputs)?;
        let _ = render_input_template(&config.access_key_id, resolved_inputs)?;
        let _ = render_input_template(&config.secret_access_key, resolved_inputs)?;
        if let Some(session_token) = &config.session_token {
            let _ = render_input_template(session_token, resolved_inputs)?;
        }
        Ok(())
    }
}

fn parse_config(auth: &CustomAuthSpec) -> Result<AwsSigV4Config, RequestAuthenticatorError> {
    serde_json::from_value(serde_json::Value::Object(auth.config.clone())).map_err(|error| {
        RequestAuthenticatorError::invalid_input(format!(
            "invalid config for custom authenticator '{}': {error}",
            auth.authenticator
        ))
    })
}

fn render_input_template(
    template: &ParsedTemplate,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<String, RequestAuthenticatorError> {
    let mut out = String::with_capacity(template.raw().len());
    for part in template.parts() {
        match part {
            TemplatePart::Literal(part) => out.push_str(part),
            TemplatePart::Token(token) => {
                if token.namespace() == &TemplateNamespace::Input {
                    let value = resolved_inputs
                        .get(token.key())
                        .cloned()
                        .or_else(|| token.default_value().map(ToString::to_string))
                        .ok_or_else(|| {
                            RequestAuthenticatorError::failed_precondition(format!(
                                "missing source input '{}' for template token",
                                token.key()
                            ))
                        })?;
                    out.push_str(&value);
                } else {
                    return Err(RequestAuthenticatorError::invalid_input(format!(
                        "custom authenticator '{}' only supports input template tokens",
                        "aws_sigv4"
                    )));
                }
            }
        }
    }
    Ok(out)
}

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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use reqwest::header::CONTENT_TYPE;
    use serde_json::json;

    use super::*;

    #[test]
    fn aws_sigv4_returns_auth_headers_for_request() {
        let auth: CustomAuthSpec = serde_json::from_value(json!({
            "authenticator": "aws_sigv4",
            "service": "execute-api",
            "region": "{{input.AWS_REGION}}",
            "access_key_id": "{{input.AWS_ACCESS_KEY_ID}}",
            "secret_access_key": "{{input.AWS_SECRET_ACCESS_KEY}}"
        }))
        .expect("custom auth spec should parse");
        let resolved_inputs = BTreeMap::from([
            ("AWS_REGION".to_string(), "us-east-1".to_string()),
            ("AWS_ACCESS_KEY_ID".to_string(), "AKIDEXAMPLE".to_string()),
            (
                "AWS_SECRET_ACCESS_KEY".to_string(),
                "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_string(),
            ),
        ]);
        let request = reqwest::Client::new()
            .post("https://example.execute-api.us-east-1.amazonaws.com/items")
            .header(CONTENT_TYPE, "application/json")
            .body("{\"hello\":\"world\"}")
            .build()
            .expect("request should build");

        let headers = AwsSigV4Authenticator
            .authenticate(&auth, &request, &resolved_inputs)
            .expect("signing should succeed");

        assert!(
            headers
                .iter()
                .any(|(name, _)| name == reqwest::header::AUTHORIZATION)
        );
        assert!(headers.iter().any(|(name, _)| name == "x-amz-date"));
    }
}
