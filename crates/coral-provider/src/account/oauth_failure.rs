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
    use coral_upstream::AuthorizationDiscoveryError as Discovery;

    fn exchange(failure: Exchange) -> Error {
        // A new variant does not compile until it is named here. Add its row
        // to the table too.
        match failure {
            Exchange::ResponseInvalid
            | Exchange::Timeout
            | Exchange::Unavailable
            | Exchange::CodeRejected
            | Exchange::RedirectMismatch
            | Exchange::AuthorizationPending
            | Exchange::SlowDown
            | Exchange::AccessDenied
            | Exchange::DeviceExpired
            | Exchange::ScopeRejected
            | Exchange::RequestRejected => Error::OAuthTokenExchange(failure),
        }
    }

    fn discovery(failure: impl Into<Failure>) -> Error {
        let failure = failure.into();
        // A new variant does not compile until it is named here. Add its row
        // to the table too.
        match failure {
            Failure::ChallengeUnsupported
            | Failure::Discovery(
                Discovery::DestinationRejected
                | Discovery::ResourceMetadataMissing
                | Discovery::ResourceMetadataUnauthorized
                | Discovery::ResourceMetadataUnavailable
                | Discovery::ResourceMismatch
                | Discovery::AuthorizationServerUnavailable
                | Discovery::IssuerMismatch
                | Discovery::PkceUnsupported
                | Discovery::InsecureEndpoint
                | Discovery::ClientBootstrapFailed,
            ) => Error::AuthorizationDiscovery(failure),
        }
    }

    /// Catches a renamed or re-mapped variant that would change a code in a
    /// stored row, a gRPC reason or a Program's error message.
    #[test]
    fn every_failure_keeps_its_stored_and_sent_code() {
        use Discovery as D;
        use Exchange as E;
        let cases = [
            (exchange(E::ResponseInvalid), "oauth_token_response_invalid"),
            (exchange(E::Timeout), "oauth_token_timeout"),
            (exchange(E::Unavailable), "oauth_token_unavailable"),
            (exchange(E::CodeRejected), "oauth_code_rejected"),
            (exchange(E::RedirectMismatch), "oauth_redirect_mismatch"),
            (
                exchange(E::AuthorizationPending),
                "oauth_authorization_pending",
            ),
            (exchange(E::SlowDown), "oauth_slow_down"),
            (exchange(E::AccessDenied), "oauth_access_denied"),
            (exchange(E::DeviceExpired), "oauth_device_expired"),
            (exchange(E::ScopeRejected), "oauth_scope_rejected"),
            (exchange(E::RequestRejected), "oauth_request_rejected"),
            (
                discovery(Failure::ChallengeUnsupported),
                "challenge_unsupported",
            ),
            (
                discovery(D::DestinationRejected),
                "outbound_destination_rejected",
            ),
            (
                discovery(D::ResourceMetadataMissing),
                "resource_metadata_missing",
            ),
            (
                discovery(D::ResourceMetadataUnauthorized),
                "resource_metadata_unavailable",
            ),
            (
                discovery(D::ResourceMetadataUnavailable),
                "resource_metadata_unavailable",
            ),
            (discovery(D::ResourceMismatch), "resource_mismatch"),
            (
                discovery(D::AuthorizationServerUnavailable),
                "authorization_server_unavailable",
            ),
            (discovery(D::IssuerMismatch), "issuer_mismatch"),
            (discovery(D::PkceUnsupported), "pkce_unsupported"),
            (discovery(D::InsecureEndpoint), "insecure_endpoint"),
            (
                discovery(D::ClientBootstrapFailed),
                "client_bootstrap_failed",
            ),
        ];
        for (error, code) in cases {
            match &error {
                Error::OAuthTokenExchange(failure) => {
                    assert_eq!(failure.code(), code, "{error:?}");
                    // The code an authorization stores when its exchange fails.
                    assert_eq!(authorization_failure_code(&error), code, "{error:?}");
                    assert_eq!(
                        error.to_string(),
                        format!("OAuth token exchange failed: {code}")
                    );
                }
                Error::AuthorizationDiscovery(failure) => {
                    assert_eq!(failure.code(), code, "{error:?}");
                    assert_eq!(
                        error.to_string(),
                        format!("MCP authorization discovery failed: {code}")
                    );
                }
                other => panic!("the table holds a failure of another kind: {other:?}"),
            }
        }
    }
}
