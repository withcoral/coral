// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

//! The AWS Secrets Manager adapter: `GetSecretValue`, signed with `SigV4`.
//!
//! `coral-aws` finds the credentials and checks their account outside the
//! outbound policy (ADR 0041). The read itself goes through
//! `coral-outbound`, because config sets its address.

use std::sync::Arc;

use async_trait::async_trait;
use coral_aws::AwsIdentity;
use coral_secret_store::{Error, KeyField, ReferenceForm, Secret, SecretReference, SecretStore};
use reqwest::StatusCode;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;
use zeroize::Zeroizing;

use crate::store_http::{Fields, StoreAddress, read_body};

const SERVICE: &str = "secretsmanager";
const TARGET: &str = "secretsmanager.GetSecretValue";
const CONTENT: &str = "application/x-amz-json-1.1";
const SECURITY_TOKEN: &str = "x-amz-security-token";

/// The version Coral reads: always the current one.
const CURRENT: &str = "AWSCURRENT";

/// Where and as whom to read one account's secrets in one region.
pub struct SecretsManagerSettings {
    /// The region that holds the secrets, checked by `coral_aws::Settings`.
    pub region: String,
    /// Where to send reads: the `endpoint` setting, or else
    /// [`regional_endpoint`].
    pub endpoint: StoreAddress,
    /// The identity that signs each read.
    pub identity: AwsIdentity,
}

/// One account's secrets in one region, read as one AWS identity.
pub struct SecretsManagerStore {
    client: coral_outbound::Client,
    endpoint: Url,
    region: String,
    identity: AwsIdentity,
    clock: Arc<dyn coral_db::Clock>,
}

impl SecretsManagerStore {
    /// Prepares a store. Nothing is sent until the first read.
    #[must_use]
    pub fn new(
        client: coral_outbound::Client,
        settings: SecretsManagerSettings,
        clock: Arc<dyn coral_db::Clock>,
    ) -> Self {
        let SecretsManagerSettings {
            region,
            endpoint,
            identity,
        } = settings;
        Self {
            client,
            endpoint: endpoint.0,
            region,
            identity,
            clock,
        }
    }

    /// A signed `GetSecretValue` request for the secret called `name`.
    async fn request(&self, name: &str) -> Result<http::Request<Vec<u8>>, Error> {
        #[derive(Serialize)]
        #[serde(rename_all = "PascalCase")]
        struct GetSecretValue<'a> {
            secret_id: &'a str,
            version_stage: &'a str,
        }
        let body = serde_json::to_vec(&GetSecretValue {
            secret_id: name,
            version_stage: CURRENT,
        })
        // Serializing two strings cannot fail.
        .map_err(|_error| Error::Unreachable)?;
        let mut request = http::Request::post(self.endpoint.as_str())
            .header(CONTENT_TYPE, CONTENT)
            .header("x-amz-target", TARGET)
            .body(body)
            .map_err(|_error| Error::Unreachable)?;
        self.identity
            .sign(
                &mut request,
                SERVICE,
                &self.region,
                self.clock.now().to_system_time(),
            )
            .await
            .map_err(|error| {
                if error.is_permission_denied() {
                    Error::PermissionDenied
                } else {
                    Error::Unreachable
                }
            })?;
        for name in [AUTHORIZATION.as_str(), SECURITY_TOKEN] {
            if let Some(value) = request.headers_mut().get_mut(name) {
                value.set_sensitive(true);
            }
        }
        Ok(request)
    }
}

#[async_trait]
impl SecretStore for SecretsManagerStore {
    /// A secret is one string. A reference may name a key in it, when that
    /// string is a JSON object.
    fn reference_form(&self) -> ReferenceForm {
        ReferenceForm {
            path_label: "Secret name".into(),
            path_prefix: None,
            key: Some(KeyField {
                label: "JSON key".into(),
                required: false,
            }),
        }
    }

    async fn read(&self, reference: &SecretReference) -> Result<Secret, Error> {
        let name = reference.path();
        // An ARN names its account, and this store reads only its own. Every
        // ARN holds `:`, and a secret's name never does.
        if name.is_empty() || name.contains(':') {
            return Err(Error::EntryNotFound);
        }
        let (parts, body) = self.request(name).await?.into_parts();
        let response = self
            .client
            .post(self.endpoint.as_str())
            .map_err(|_error| Error::Unreachable)?
            .headers(parts.headers)
            .body(body)
            .send()
            .await
            .map_err(|_error| Error::Unreachable)?;
        let status = response.status();
        if status.is_server_error()
            || status.is_redirection()
            || status == StatusCode::TOO_MANY_REQUESTS
        {
            return Err(Error::Unreachable);
        }
        if status != StatusCode::OK {
            let error_type = response
                .headers()
                .get("x-amzn-errortype")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            // The status alone decides when the body is unreadable.
            let body = read_body(response).await.unwrap_or_default();
            return Err(error(status, error_type.as_deref(), &body));
        }
        let body = read_body(response).await?;
        let answer: Answer =
            serde_json::from_slice(&body).map_err(|_error| Error::MalformedResponse)?;
        // A binary secret has `SecretBinary` instead.
        let text = answer.secret_string.ok_or(Error::MalformedResponse)?;
        let mut value = match reference.key() {
            None => text,
            Some(key) => {
                let fields: Fields =
                    serde_json::from_str(&text).map_err(|_not_an_object| Error::KeyNotFound)?;
                match fields.0.get(key) {
                    Some(Value::String(value)) => Zeroizing::new(value.clone()),
                    Some(_) => return Err(Error::MalformedResponse),
                    None => return Err(Error::KeyNotFound),
                }
            }
        };
        Secret::new(std::mem::take(&mut *value)).map_err(|_error| Error::MalformedResponse)
    }
}

/// AWS's Secrets Manager endpoint for a region `coral_aws::Settings`
/// checked, in the `aws`, `aws-cn` and `aws-us-gov` partitions. `None` for a
/// region of an isolated partition, such as `us-iso-east-1`, whose endpoint
/// has another domain.
#[must_use]
pub fn regional_endpoint(region: &str) -> Option<StoreAddress> {
    let mut parts = region.split('-');
    let (Some(area), Some(location)) = (parts.next(), parts.next()) else {
        return None;
    };
    if location.starts_with("iso") {
        return None;
    }
    let domain = if area == "cn" {
        "amazonaws.com.cn"
    } else {
        "amazonaws.com"
    };
    Url::parse(&format!("https://{SERVICE}.{region}.{domain}/"))
        .ok()
        .map(StoreAddress)
}

/// The port's error for a 4xx answer.
fn error(status: StatusCode, header: Option<&str>, body: &[u8]) -> Error {
    #[derive(Deserialize)]
    struct Failure {
        #[serde(rename = "__type")]
        error_type: Option<String>,
    }
    let from_body = serde_json::from_slice::<Failure>(body)
        .ok()
        .and_then(|failure| failure.error_type);
    let code = header.or(from_body.as_deref()).map(error_code);
    match code {
        Some(
            "ThrottlingException" | "Throttling" | "TooManyRequestsException" | "RequestTimeout",
        ) => Error::Unreachable,
        // `InvalidRequestException` is how AWS answers a secret marked for
        // deletion, and `InvalidParameterException` a name no secret can have.
        Some(
            "ResourceNotFoundException" | "InvalidRequestException" | "InvalidParameterException",
        ) => Error::EntryNotFound,
        // KMS answers `DecryptionFailure` when its key is disabled or Coral may
        // not use it, and `AccessDeniedException` without `kms:Decrypt`.
        Some(
            "AccessDeniedException"
            | "DecryptionFailure"
            | "UnrecognizedClientException"
            | "InvalidClientTokenId"
            | "InvalidSignatureException"
            | "SignatureDoesNotMatch"
            | "IncompleteSignature"
            | "MissingAuthenticationToken"
            | "ExpiredTokenException",
        ) => Error::PermissionDenied,
        _ if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) => {
            Error::PermissionDenied
        }
        _ => Error::MalformedResponse,
    }
}

/// The bare error code in an AWS error type, which may carry a namespace
/// before `#` and a URL after `:`.
fn error_code(error_type: &str) -> &str {
    let code = error_type.rsplit('#').next().unwrap_or(error_type);
    code.split(':').next().unwrap_or(code)
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Answer {
    secret_string: Option<Zeroizing<String>>,
}

#[cfg(test)]
mod tests;
