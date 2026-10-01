//! The single canonical form of an OAuth resource identifier.
//!
//! One string plays three roles for a served Coral instance: it is minted into
//! access tokens as the `aud` claim, recorded as an authorization resource the
//! authorization server will issue for, and advertised to MCP clients as
//! protected-resource metadata. Those roles are compared byte for byte, so a
//! second copy of this canonicalization that trimmed one URL shape differently
//! would mint an audience that no longer matches the advertised resource and
//! break authentication end to end. Every surface therefore derives its string
//! here.

use url::{Position, Url};

use crate::outbound_url_policy::{EndpointUrl, OutboundUrlPolicyError, ResourceIdentifier};

/// An OAuth resource identifier in its canonical form.
///
/// Holding one is proof that [`CanonicalOauthUrl::parse`] accepted the value,
/// which is why no consumer revalidates it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalOauthUrl {
    url: Url,
    identifier: String,
}

impl CanonicalOauthUrl {
    /// Validates and canonicalizes an operator-configured resource identifier.
    ///
    /// Accepts an absolute HTTPS URL, or an explicit loopback HTTP URL for local
    /// development, carrying no credentials, query, or fragment.
    /// Canonicalization drops a root path — `https://coral.example/` becomes
    /// `https://coral.example` — and is idempotent on its own output.
    ///
    /// # Errors
    ///
    /// Returns the rule the value violates.
    pub fn parse(value: &str) -> Result<Self, OauthUrlError> {
        let url = EndpointUrl::<ResourceIdentifier>::parse(value)
            .map_err(|error| OauthUrlError::from_policy_error(&error))?
            .into_url();
        if url.query().is_some() {
            return Err(OauthUrlError::Query);
        }
        let identifier = match url.path() {
            "/" => url[..Position::BeforePath].to_string(),
            _ => url.to_string(),
        };
        Ok(Self { url, identifier })
    }

    /// Returns the validated URL, for callers deriving paths or hosts from it.
    #[must_use]
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Returns the canonical identifier compared as an audience and a resource.
    #[must_use]
    pub fn identifier(&self) -> &str {
        &self.identifier
    }

    /// Consumes this value and returns its canonical identifier.
    #[must_use]
    pub fn into_identifier(self) -> String {
        self.identifier
    }
}

/// Why a value cannot serve as an OAuth resource identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OauthUrlError {
    /// Not an absolute URL with a host, or it carries credentials or a fragment.
    #[error("must be an absolute URL with a host and no credentials or fragment")]
    Shape,
    /// Neither HTTPS nor an explicit loopback HTTP URL.
    #[error("must use HTTPS or explicit loopback HTTP")]
    Transport,
    /// Carries a query string.
    #[error("must not include a query")]
    Query,
}

impl OauthUrlError {
    fn from_policy_error(error: &OutboundUrlPolicyError) -> Self {
        match error {
            OutboundUrlPolicyError::ResourceIdentifierTransport => Self::Transport,
            _ => Self::Shape,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CanonicalOauthUrl, OauthUrlError};

    #[test]
    fn canonicalization_is_stable_on_sensitive_url_shapes() {
        for (value, expected) in [
            ("https://coral.example/", "https://coral.example"),
            ("https://coral.example", "https://coral.example"),
            ("https://CORAL.Example/", "https://coral.example"),
            ("https://coral.example/mcp", "https://coral.example/mcp"),
            (
                "https://coral.example/base/mcp/",
                "https://coral.example/base/mcp/",
            ),
            ("https://coral.example:8443/", "https://coral.example:8443"),
            ("http://localhost:14556/mcp", "http://localhost:14556/mcp"),
        ] {
            let canonical = CanonicalOauthUrl::parse(value).expect("resource identifier");
            assert_eq!(canonical.identifier(), expected, "value: {value}");
            let again = CanonicalOauthUrl::parse(canonical.identifier()).expect("idempotent");
            assert_eq!(again.identifier(), expected, "value: {value}");
        }
    }

    #[test]
    fn unsafe_resource_identifiers_are_rejected_by_rule() {
        for (value, expected) in [
            ("http://coral.example/mcp", OauthUrlError::Transport),
            ("ftp://coral.example/mcp", OauthUrlError::Transport),
            ("https://coral.example/mcp?tenant=1", OauthUrlError::Query),
            ("https://user:pass@coral.example/mcp", OauthUrlError::Shape),
            ("https://coral.example/mcp#frag", OauthUrlError::Shape),
            ("/mcp", OauthUrlError::Shape),
        ] {
            assert_eq!(
                CanonicalOauthUrl::parse(value).expect_err("must be rejected"),
                expected,
                "value: {value}"
            );
        }
    }
}
