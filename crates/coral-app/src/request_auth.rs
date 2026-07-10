//! Request authentication shared by served transport surfaces.

use std::fmt;
use std::sync::Arc;

use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq as _;
use tonic::metadata::MetadataMap;

use crate::identity::{UserPrincipal, UserPrincipalProvider, UserPrincipalProviderError};

const AUTHORIZATION_METADATA: &str = "authorization";
const UNAUTHENTICATED_MESSAGE: &str = "authentication required";

/// Synchronous bearer validator for gRPC and HTTP gates, without principal
/// or tenant semantics.
pub trait AuthValidator: Send + Sync + 'static {
    /// Returns whether `token` grants access to the served API.
    fn accepts_bearer(&self, token: &str) -> bool;
}

/// Static bearer validator retaining only a constant-time-compared SHA-256 digest.
#[derive(Clone)]
pub struct StaticTokenValidator {
    expected_digest: [u8; 32],
}

impl StaticTokenValidator {
    /// Builds a validator for a printable ASCII token.
    #[must_use]
    pub fn new(token: &str) -> Option<Self> {
        valid_bearer_token(token).then(|| Self {
            expected_digest: Sha256::digest(token.as_bytes()).into(),
        })
    }
}

impl fmt::Debug for StaticTokenValidator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("StaticTokenValidator").finish()
    }
}

impl AuthValidator for StaticTokenValidator {
    fn accepts_bearer(&self, token: &str) -> bool {
        let actual_digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        self.expected_digest.ct_eq(&actual_digest).into()
    }
}

pub(crate) struct BearerUserPrincipalProvider {
    validator: Arc<dyn AuthValidator>,
    inner: Arc<dyn UserPrincipalProvider>,
}

impl BearerUserPrincipalProvider {
    pub(crate) fn new(
        validator: Arc<dyn AuthValidator>,
        inner: Arc<dyn UserPrincipalProvider>,
    ) -> Self {
        Self { validator, inner }
    }
}

impl fmt::Debug for BearerUserPrincipalProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BearerUserPrincipalProvider")
            .finish_non_exhaustive()
    }
}

#[tonic::async_trait]
impl UserPrincipalProvider for BearerUserPrincipalProvider {
    async fn principal_for_metadata(
        &self,
        metadata: &MetadataMap,
    ) -> Result<UserPrincipal, UserPrincipalProviderError> {
        authenticate_metadata(metadata, self.validator.as_ref())?;
        self.inner.principal_for_metadata(metadata).await
    }
}

fn authenticate_metadata(
    metadata: &MetadataMap,
    validator: &dyn AuthValidator,
) -> Result<(), UserPrincipalProviderError> {
    let token = strict_bearer(metadata)?;
    if !validator.accepts_bearer(token) {
        return Err(unauthenticated());
    }
    Ok(())
}

fn strict_bearer(metadata: &MetadataMap) -> Result<&str, UserPrincipalProviderError> {
    let mut values = metadata.get_all(AUTHORIZATION_METADATA).iter();
    let value = values
        .next()
        .filter(|_| values.next().is_none())
        .and_then(|value| value.to_str().ok())
        .ok_or_else(unauthenticated)?;
    let (scheme, token) = value.split_once(' ').ok_or_else(unauthenticated)?;
    if !scheme.eq_ignore_ascii_case("bearer") || !valid_bearer_token(token) {
        return Err(unauthenticated());
    }
    Ok(token)
}

fn unauthenticated() -> UserPrincipalProviderError {
    UserPrincipalProviderError::unauthenticated(UNAUTHENTICATED_MESSAGE)
}

fn valid_bearer_token(token: &str) -> bool {
    !token.is_empty() && token.bytes().all(|byte| byte.is_ascii_graphic())
}

#[cfg(test)]
mod tests {
    use tonic::Request;
    use tonic::metadata::MetadataValue;

    use super::{AuthValidator as _, StaticTokenValidator, authenticate_metadata};

    #[test]
    fn static_validator_accepts_only_the_exact_token_without_debug_disclosure() {
        let token = "correct-._~+/=:";
        let validator = StaticTokenValidator::new(token).expect("valid token");
        assert!(validator.accepts_bearer(token));
        assert!(!validator.accepts_bearer("wrong-token"));
        assert!(!format!("{validator:?}").contains(token));
        for invalid in ["", "two words", "tökén", "line\nbreak"] {
            assert!(StaticTokenValidator::new(invalid).is_none());
        }
    }

    #[test]
    fn bearer_metadata_is_strict_and_rejections_are_generic() {
        let validator = StaticTokenValidator::new("correct-token").expect("nonempty token");
        for value in [None, Some("Basic correct-token"), Some("Bearer wrong")] {
            let mut request = Request::new(());
            if let Some(value) = value {
                request.metadata_mut().insert(
                    "authorization",
                    MetadataValue::try_from(value).expect("metadata"),
                );
            }
            let error = authenticate_metadata(request.metadata(), &validator)
                .expect_err("invalid authentication must fail");
            assert_eq!(error.client_message(), "authentication required");
        }
    }
}
