//! Canonical identity for a remote Coral gRPC endpoint.

use thiserror::Error;
use url::{Host, Url};

/// A validated remote endpoint whose value is a canonical URL origin.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalRemoteEndpoint(String);

impl CanonicalRemoteEndpoint {
    /// Parses a root HTTPS endpoint or an explicit loopback HTTP endpoint.
    ///
    /// # Errors
    /// Returns an error for unsafe transports, user information, non-root
    /// paths, queries, fragments, or surrounding whitespace.
    pub fn parse(value: &str) -> Result<Self, RemoteEndpointError> {
        if value.trim() != value || authority_contains_userinfo(value) {
            return Err(RemoteEndpointError);
        }
        let url = Url::parse(value).map_err(|_error| RemoteEndpointError)?;
        if url.host().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
            || !(url.scheme() == "https"
                || url.scheme() == "http" && url.host().as_ref().is_some_and(loopback_host))
        {
            return Err(RemoteEndpointError);
        }
        let origin = url.origin().ascii_serialization();
        (origin != "null")
            .then_some(Self(origin))
            .ok_or(RemoteEndpointError)
    }

    /// Returns the canonical endpoint URI used for both storage and dialing.
    #[must_use]
    pub fn as_uri(&self) -> &str {
        &self.0
    }
}

/// An invalid remote Coral endpoint.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error(
    "remote endpoint must be a root HTTPS URL or explicit loopback HTTP URL without credentials, a query, or a fragment"
)]
pub struct RemoteEndpointError;

fn authority_contains_userinfo(value: &str) -> bool {
    value
        .split_once("://")
        .and_then(|(_scheme, rest)| rest.split(['/', '?', '#']).next())
        .is_some_and(|authority| authority.contains('@'))
}

fn loopback_host(host: &Host<&str>) -> bool {
    match host {
        Host::Domain(host) => host.eq_ignore_ascii_case("localhost"),
        Host::Ipv4(address) => address.is_loopback(),
        Host::Ipv6(address) => address.is_loopback(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalizes_equivalent_endpoint_origins() {
        for (raw, canonical) in [
            ("https://EXAMPLE.com:443/", "https://example.com"),
            ("http://127.1:80/", "http://127.0.0.1"),
            ("http://[::1]:1457/", "http://[::1]:1457"),
        ] {
            assert_eq!(
                CanonicalRemoteEndpoint::parse(raw)
                    .expect("endpoint")
                    .as_uri(),
                canonical
            );
        }
    }

    #[test]
    fn rejects_unsafe_or_ambiguous_endpoints() {
        for raw in [
            "http://example.com",
            "https://user@example.com",
            "https://@example.com",
            "https://example.com/path",
            "https://example.com?",
            "http://localhost.",
            "http://[::ffff:127.0.0.1]",
            " https://example.com",
        ] {
            assert!(CanonicalRemoteEndpoint::parse(raw).is_err(), "{raw}");
        }
    }

    #[test]
    fn keeps_distinct_security_origins_separate() {
        let parse = |raw| CanonicalRemoteEndpoint::parse(raw).expect("endpoint");
        assert_ne!(parse("https://example.com"), parse("https://example.com."));
        assert_ne!(parse("http://localhost"), parse("http://127.0.0.1"));
        assert_ne!(
            parse("https://example.com"),
            parse("https://example.com:444")
        );
    }
}
