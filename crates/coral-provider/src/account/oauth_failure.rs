//! The closed sets of OAuth token-exchange and authorization-discovery
//! failures. Each failure has one stable code. The codes are stored with an
//! authorization and sent to clients, so a code never changes once released.

/// Why a token endpoint or a device-authorization endpoint gave Coral no
/// token. Coral classifies the failure; no upstream description reaches it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenExchangeFailure {
    /// The response could not be read, parsed or accepted.
    ResponseInvalid,
    /// The endpoint did not answer in time.
    Timeout,
    /// The endpoint could not be reached, or answered with a server error or
    /// a redirect.
    Unavailable,
    /// The server rejected the authorization code, device code or refresh
    /// token.
    CodeRejected,
    /// The server does not accept the redirect address Coral sent.
    RedirectMismatch,
    /// The User has not finished the device sign-in yet.
    AuthorizationPending,
    /// The server asked Coral to poll the device sign-in less often.
    SlowDown,
    /// The User or the server denied the request.
    AccessDenied,
    /// The device code expired before the User finished signing in.
    DeviceExpired,
    /// The server rejected the requested scope.
    ScopeRejected,
    /// The server rejected the request for a reason Coral does not classify.
    RequestRejected,
}

impl TokenExchangeFailure {
    /// The stable `snake_case` code that is stored and shown for this failure.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::ResponseInvalid => "oauth_token_response_invalid",
            Self::Timeout => "oauth_token_timeout",
            Self::Unavailable => "oauth_token_unavailable",
            Self::CodeRejected => "oauth_code_rejected",
            Self::RedirectMismatch => "oauth_redirect_mismatch",
            Self::AuthorizationPending => "oauth_authorization_pending",
            Self::SlowDown => "oauth_slow_down",
            Self::AccessDenied => "oauth_access_denied",
            Self::DeviceExpired => "oauth_device_expired",
            Self::ScopeRejected => "oauth_scope_rejected",
            Self::RequestRejected => "oauth_request_rejected",
        }
    }
}

impl std::fmt::Display for TokenExchangeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

/// Why Coral could not learn how to sign in to an MCP server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationDiscoveryFailure {
    /// The server asks for an authentication method Coral cannot use.
    ChallengeUnsupported,
    /// Metadata discovery or client registration failed.
    Discovery(coral_upstream::AuthorizationDiscoveryError),
}

impl AuthorizationDiscoveryFailure {
    /// The stable `snake_case` code that is stored and shown for this failure.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::ChallengeUnsupported => "challenge_unsupported",
            Self::Discovery(error) => error.code(),
        }
    }
}

impl From<coral_upstream::AuthorizationDiscoveryError> for AuthorizationDiscoveryFailure {
    fn from(error: coral_upstream::AuthorizationDiscoveryError) -> Self {
        Self::Discovery(error)
    }
}

impl std::fmt::Display for AuthorizationDiscoveryFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthorizationDiscoveryFailure as Failure, TokenExchangeFailure as Exchange};
    use crate::Error;
    use crate::account::oauth::authorization_failure_code;
    use coral_upstream::AuthorizationDiscoveryError as D;

    /// The rows as `(Error, code, row name)`. The list is both the rows and
    /// an exhaustive match that holds each row's code, so a new variant does
    /// not compile until it has a row with a code.
    macro_rules! pinned {
        ($error:path: $($($variant:ident)::+ $(($($inner:ident)::+))? => $code:literal,)+) => {
            [$($($variant)::+ $(($($inner)::+))?),+].map(|failure| match failure {
                $($($variant)::+ $(($($inner)::+))? => (
                    $error(failure),
                    $code,
                    stringify!($($variant)::+ $(($($inner)::+))?),
                ),)+
            })
        };
    }

    /// Catches a renamed or re-mapped variant that would change a code in a
    /// stored row, a gRPC reason or a Program's error message.
    #[test]
    fn every_failure_keeps_its_stored_and_sent_code() {
        let exchange = pinned!(Error::OAuthTokenExchange:
            Exchange::ResponseInvalid => "oauth_token_response_invalid",
            Exchange::Timeout => "oauth_token_timeout",
            Exchange::Unavailable => "oauth_token_unavailable",
            Exchange::CodeRejected => "oauth_code_rejected",
            Exchange::RedirectMismatch => "oauth_redirect_mismatch",
            Exchange::AuthorizationPending => "oauth_authorization_pending",
            Exchange::SlowDown => "oauth_slow_down",
            Exchange::AccessDenied => "oauth_access_denied",
            Exchange::DeviceExpired => "oauth_device_expired",
            Exchange::ScopeRejected => "oauth_scope_rejected",
            Exchange::RequestRejected => "oauth_request_rejected",
        );
        let discovery = pinned!(Error::AuthorizationDiscovery:
            Failure::ChallengeUnsupported => "challenge_unsupported",
            Failure::Discovery(D::DestinationRejected) => "outbound_destination_rejected",
            Failure::Discovery(D::ResourceMetadataMissing) => "resource_metadata_missing",
            Failure::Discovery(D::ResourceMetadataUnauthorized) => "resource_metadata_unavailable",
            Failure::Discovery(D::ResourceMetadataUnavailable) => "resource_metadata_unavailable",
            Failure::Discovery(D::ResourceMismatch) => "resource_mismatch",
            Failure::Discovery(D::AuthorizationServerUnavailable) => "authorization_server_unavailable",
            Failure::Discovery(D::IssuerMismatch) => "issuer_mismatch",
            Failure::Discovery(D::PkceUnsupported) => "pkce_unsupported",
            Failure::Discovery(D::InsecureEndpoint) => "insecure_endpoint",
            Failure::Discovery(D::ClientBootstrapFailed) => "client_bootstrap_failed",
        );
        for (error, code, row) in exchange.into_iter().chain(discovery) {
            match &error {
                Error::OAuthTokenExchange(failure) => {
                    assert_eq!(failure.code(), code, "{row}");
                    // An exchange failure is stored under its own code. A poll
                    // answer is not: the database accepts neither.
                    let stored = match failure {
                        Exchange::AuthorizationPending | Exchange::SlowDown => {
                            "oauth_request_rejected"
                        }
                        _ => code,
                    };
                    assert_eq!(authorization_failure_code(&error), stored, "{row}");
                    assert_eq!(
                        error.to_string(),
                        format!("OAuth token exchange failed: {code}"),
                        "{row}"
                    );
                }
                Error::AuthorizationDiscovery(failure) => {
                    assert_eq!(failure.code(), code, "{row}");
                    assert_eq!(
                        error.to_string(),
                        format!("MCP authorization discovery failed: {code}"),
                        "{row}"
                    );
                }
                other => panic!("{row} is a failure of another kind: {other:?}"),
            }
        }
    }

    /// Catches a cause that the interface explains losing its stored code,
    /// and an internal fault gaining a code that names it to the person who
    /// signs in.
    #[test]
    fn only_a_cause_the_person_can_act_on_has_its_own_stored_code() {
        for (error, code) in [
            (
                Error::OutboundDestinationRejected,
                "outbound_destination_rejected",
            ),
            (
                Error::OAuthClientConfiguration,
                "oauth_client_configuration",
            ),
            // No discovery runs during an exchange or a device poll.
            (
                Error::AuthorizationDiscovery(Failure::ChallengeUnsupported),
                "authorization_failed",
            ),
            (Error::AccountInput, "authorization_failed"),
            (
                Error::CorruptData("private detail".into()),
                "authorization_failed",
            ),
        ] {
            assert_eq!(authorization_failure_code(&error), code, "{error:?}");
        }
    }
}
