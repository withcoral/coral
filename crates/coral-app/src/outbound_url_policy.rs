//! Outbound URL policies for configured endpoints and untrusted metadata.
//!
//! These policies are intentionally separate. Operator-configured endpoints
//! may use HTTPS anywhere or plain HTTP on an explicit loopback host. URLs
//! supplied by an untrusted client must instead identify a public HTTPS
//! resource and use a DNS resolver that rejects non-public answers.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "OAuth, OIDC, and CIMD consumers land later in the serving stack"
    )
)]

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use thiserror::Error;
use url::{Host, Url};

/// Timeout applied to public metadata connections and complete requests.
pub(crate) const PUBLIC_METADATA_TIMEOUT: Duration = Duration::from_secs(5);

/// A configured endpoint that is safe for credential-bearing requests.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ConfiguredEndpointUrl(Url);

impl ConfiguredEndpointUrl {
    /// Parses an HTTPS endpoint or an explicit loopback HTTP endpoint.
    pub(crate) fn parse(value: &str) -> Result<Self, OutboundUrlPolicyError> {
        let url = Url::parse(value).map_err(OutboundUrlPolicyError::InvalidUrl)?;
        if url.host().is_none() {
            return Err(OutboundUrlPolicyError::MissingHost);
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(OutboundUrlPolicyError::CredentialsNotAllowed);
        }
        if url.fragment().is_some() {
            return Err(OutboundUrlPolicyError::FragmentNotAllowed);
        }
        match url.scheme() {
            "https" => Ok(Self(url)),
            "http" if is_explicit_loopback(&url) => Ok(Self(url)),
            _ => Err(OutboundUrlPolicyError::ConfiguredEndpointTransport),
        }
    }

    /// Returns the validated URL.
    pub(crate) fn as_url(&self) -> &Url {
        &self.0
    }

    /// Consumes this wrapper and returns the validated URL.
    pub(crate) fn into_url(self) -> Url {
        self.0
    }
}

impl fmt::Debug for ConfiguredEndpointUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ConfiguredEndpointUrl")
            .field(&RedactedUrl(&self.0))
            .finish()
    }
}

/// An attacker-controlled metadata URL validated for public HTTPS fetching.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct PublicMetadataUrl(Url);

impl PublicMetadataUrl {
    /// Parses a public HTTPS metadata URL with a non-root, traversal-free path.
    pub(crate) fn parse(value: &str) -> Result<Self, OutboundUrlPolicyError> {
        if has_dot_path_segment(value) {
            return Err(OutboundUrlPolicyError::DotPathSegment);
        }
        let url = Url::parse(value).map_err(OutboundUrlPolicyError::InvalidUrl)?;
        if url.scheme() != "https" {
            return Err(OutboundUrlPolicyError::PublicMetadataTransport);
        }
        if url.path().is_empty() || url.path() == "/" {
            return Err(OutboundUrlPolicyError::MetadataPathRequired);
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(OutboundUrlPolicyError::CredentialsNotAllowed);
        }
        if url.fragment().is_some() {
            return Err(OutboundUrlPolicyError::FragmentNotAllowed);
        }
        if public_metadata_host_is_blocked(&url) {
            return Err(OutboundUrlPolicyError::NonPublicHost);
        }
        Ok(Self(url))
    }

    /// Returns the validated URL.
    pub(crate) fn as_url(&self) -> &Url {
        &self.0
    }

    /// Consumes this wrapper and returns the validated URL.
    pub(crate) fn into_url(self) -> Url {
        self.0
    }
}

impl fmt::Debug for PublicMetadataUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("PublicMetadataUrl")
            .field(&RedactedUrl(&self.0))
            .finish()
    }
}

struct RedactedUrl<'a>(&'a Url);

impl fmt::Debug for RedactedUrl<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0.scheme())?;
        formatter.write_str("://")?;
        if let Some(host) = self.0.host_str() {
            formatter.write_str(host)?;
        }
        if let Some(port) = self.0.port() {
            write!(formatter, ":{port}")?;
        }
        formatter.write_str("/<redacted>")
    }
}

/// Errors returned while validating or fetching outbound URLs.
#[derive(Debug, Error)]
pub(crate) enum OutboundUrlPolicyError {
    /// The URL could not be parsed.
    #[error("URL is invalid: {0}")]
    InvalidUrl(url::ParseError),
    /// The URL has no host.
    #[error("URL must include a host")]
    MissingHost,
    /// A configured endpoint used an unsafe transport.
    #[error("configured endpoint must use HTTPS or explicit loopback HTTP")]
    ConfiguredEndpointTransport,
    /// Public metadata did not use HTTPS.
    #[error("public metadata URL must use HTTPS")]
    PublicMetadataTransport,
    /// Public metadata did not identify a document path.
    #[error("public metadata URL must include a non-root path")]
    MetadataPathRequired,
    /// User information was embedded in a URL.
    #[error("URL must not include credentials")]
    CredentialsNotAllowed,
    /// A URL contained a fragment.
    #[error("URL must not include a fragment")]
    FragmentNotAllowed,
    /// An untrusted URL contained a traversal segment.
    #[error("public metadata URL must not include dot path segments")]
    DotPathSegment,
    /// An untrusted URL directly identified a non-public host.
    #[error("public metadata URL host must be public")]
    NonPublicHost,
    /// The hardened HTTP client could not be constructed.
    #[error("failed to build public metadata HTTP client: {0}")]
    ClientBuild(reqwest::Error),
    /// A response body could not be read.
    #[error("failed to read public metadata response: {0}")]
    BodyRead(reqwest::Error),
    /// A response body exceeded the caller's bound.
    #[error("public metadata response exceeded {limit} bytes")]
    BodyTooLarge {
        /// Maximum number of accepted bytes.
        limit: usize,
    },
}

/// Builds an HTTP client for attacker-controlled public metadata URLs.
///
/// The caller must still construct requests from [`PublicMetadataUrl`]. The
/// resolver rejects a hostname if any returned address is non-public, which
/// prevents mixed-answer DNS rebinding from selecting a private destination.
pub(crate) fn public_metadata_http_client() -> Result<reqwest::Client, OutboundUrlPolicyError> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .connect_timeout(PUBLIC_METADATA_TIMEOUT)
        .timeout(PUBLIC_METADATA_TIMEOUT)
        .dns_resolver(PublicMetadataResolver)
        .build()
        .map_err(OutboundUrlPolicyError::ClientBuild)
}

/// Reads a response body without buffering more than `limit` bytes.
pub(crate) async fn read_bounded_body(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, OutboundUrlPolicyError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(OutboundUrlPolicyError::BodyTooLarge { limit });
    }

    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(OutboundUrlPolicyError::BodyRead)?
    {
        append_bounded_chunk(&mut body, &chunk, limit)?;
    }
    Ok(body)
}

#[derive(Clone)]
struct PublicMetadataResolver;

impl reqwest::dns::Resolve for PublicMetadataResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addresses = tokio::net::lookup_host((host.as_str(), 0))
                .await
                .map_err(|error| {
                    std::io::Error::other(format!(
                        "public metadata DNS lookup failed for {host}: {error}"
                    ))
                })?
                .collect::<Vec<_>>();
            validate_public_resolution(&host, &addresses)?;
            Ok(Box::new(addresses.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

fn validate_public_resolution(host: &str, addresses: &[SocketAddr]) -> std::io::Result<()> {
    if addresses.is_empty() {
        return Err(std::io::Error::other(format!(
            "public metadata DNS lookup returned no records for {host}"
        )));
    }
    if let Some(address) = addresses
        .iter()
        .find(|address| public_metadata_ip_is_blocked(address.ip()))
    {
        return Err(std::io::Error::other(format!(
            "public metadata DNS lookup resolved {host} to disallowed address {address}"
        )));
    }
    Ok(())
}

fn is_explicit_loopback(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(host)) => host.trim_end_matches('.').eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => {
            address.is_loopback()
                || address
                    .to_ipv4_mapped()
                    .is_some_and(|address| address.is_loopback())
        }
        None => false,
    }
}

fn public_metadata_host_is_blocked(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(host)) => {
            let host = host.trim_end_matches('.').to_ascii_lowercase();
            host == "localhost" || host.ends_with(".localhost")
        }
        Some(Host::Ipv4(address)) => public_metadata_ipv4_is_blocked(address),
        Some(Host::Ipv6(address)) => public_metadata_ipv6_is_blocked(address),
        None => true,
    }
}

fn public_metadata_ip_is_blocked(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => public_metadata_ipv4_is_blocked(address),
        IpAddr::V6(address) => public_metadata_ipv6_is_blocked(address),
    }
}

fn public_metadata_ipv4_is_blocked(address: Ipv4Addr) -> bool {
    let [a, b, c, _] = address.octets();
    a == 0
        || a == 10
        || a == 127
        || (a == 100 && (64..=127).contains(&b))
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 0 && c == 2)
        || (a == 192 && b == 88 && c == 99)
        || (a == 192 && b == 168)
        || (a == 198 && (b == 18 || b == 19))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
        || a >= 224
}

fn public_metadata_ipv6_is_blocked(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    address.is_loopback()
        || address.is_unspecified()
        || address.is_multicast()
        || address.to_ipv4_mapped().is_some()
        || (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] & 0xffc0) == 0xfe80
        || (segments[0] & 0xffc0) == 0xfec0
        || (segments[0] == 0x0064
            && segments[1] == 0xff9b
            && ((segments[2..6] == [0, 0, 0, 0]) || segments[2] == 1))
        || (segments[0] == 0x0100 && segments[1] == 0 && segments[2] == 0 && segments[3] == 0)
        || (segments[0] == 0x2001 && segments[1] == 0x0002)
        || (segments[0] == 0x2001 && (0x0010..=0x002f).contains(&segments[1]))
        || (segments[0] == 0x2001 && segments[1] == 0x0db8)
}

fn has_dot_path_segment(value: &str) -> bool {
    let Some((_, after_scheme)) = value.split_once("://") else {
        return false;
    };
    let Some(path_start) = after_scheme.find(['/', '\\']) else {
        return false;
    };
    let Some(path) = after_scheme.get(path_start..) else {
        return false;
    };
    let path_end = path.find(['?', '#']).unwrap_or(path.len());
    path.get(..path_end)
        .is_some_and(|path| path.split(['/', '\\']).any(is_dot_path_segment))
}

fn is_dot_path_segment(segment: &str) -> bool {
    matches!(
        segment.to_ascii_lowercase().as_str(),
        "." | ".." | "%2e" | ".%2e" | "%2e." | "%2e%2e"
    )
}

fn append_bounded_chunk(
    body: &mut Vec<u8>,
    chunk: &[u8],
    limit: usize,
) -> Result<(), OutboundUrlPolicyError> {
    if chunk.len() > limit.saturating_sub(body.len()) {
        return Err(OutboundUrlPolicyError::BodyTooLarge { limit });
    }
    body.extend_from_slice(chunk);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{
        ConfiguredEndpointUrl, OutboundUrlPolicyError, PublicMetadataUrl, append_bounded_chunk,
        public_metadata_http_client, public_metadata_ip_is_blocked, read_bounded_body,
        validate_public_resolution,
    };

    #[test]
    fn configured_endpoints_allow_https_and_explicit_loopback_http() {
        for endpoint in [
            "https://service.example.test/oauth",
            "https://10.0.0.8/oauth",
            "http://localhost:14554/callback",
            "http://127.42.0.1:14554/callback",
            "http://[::1]:14554/callback",
            "http://[::ffff:127.0.0.1]:14554/callback",
        ] {
            ConfiguredEndpointUrl::parse(endpoint).expect(endpoint);
        }
    }

    #[test]
    fn configured_endpoints_reject_non_loopback_plaintext() {
        for endpoint in [
            "http://service.example.test/oauth",
            "http://10.0.0.8/oauth",
            "http://localhost.example.test/oauth",
            "ftp://localhost/oauth",
        ] {
            assert!(matches!(
                ConfiguredEndpointUrl::parse(endpoint),
                Err(OutboundUrlPolicyError::ConfiguredEndpointTransport)
            ));
        }

        assert!(matches!(
            ConfiguredEndpointUrl::parse("https://user:password@login.example.test/oauth"),
            Err(OutboundUrlPolicyError::CredentialsNotAllowed)
        ));
    }

    #[test]
    fn validated_wrappers_expose_their_inner_urls() {
        let configured = ConfiguredEndpointUrl::parse("https://login.example.test/oauth")
            .expect("configured endpoint");
        assert_eq!(configured.as_url().host_str(), Some("login.example.test"));
        assert_eq!(configured.into_url().path(), "/oauth");

        let metadata = PublicMetadataUrl::parse("https://client.example.test/oauth/client.json")
            .expect("metadata URL");
        assert_eq!(metadata.as_url().host_str(), Some("client.example.test"));
        assert_eq!(metadata.into_url().path(), "/oauth/client.json");
    }

    #[test]
    fn validated_url_debug_output_redacts_paths_and_queries() {
        let configured = ConfiguredEndpointUrl::parse(
            "https://login.example.test/tenant/secret?client_secret=hidden",
        )
        .expect("configured endpoint");
        let metadata =
            PublicMetadataUrl::parse("https://client.example.test/oauth/client.json?token=hidden")
                .expect("metadata URL");

        for rendered in [format!("{configured:?}"), format!("{metadata:?}")] {
            assert!(rendered.contains("/<redacted>"));
            assert!(!rendered.contains("secret"));
            assert!(!rendered.contains("hidden"));
            assert!(!rendered.contains("client.json"));
        }
    }

    #[test]
    fn public_metadata_requires_document_shaped_https_url() {
        PublicMetadataUrl::parse("https://client.example.test/oauth/client.json?version=1")
            .expect("public metadata URL");

        for metadata_url in [
            "http://client.example.test/oauth/client.json",
            "https://client.example.test",
            "https://user@client.example.test/oauth/client.json",
            "https://user:password@client.example.test/oauth/client.json",
            "https://client.example.test/oauth/client.json#fragment",
            "https://client.example.test/oauth/../client.json",
            "https://client.example.test/oauth/%2e%2e/client.json",
            "https://client.example.test/oauth\\..\\client.json",
        ] {
            PublicMetadataUrl::parse(metadata_url).expect_err(metadata_url);
        }
    }

    #[test]
    fn public_metadata_rejects_localhost_and_non_public_ipv4() {
        for host in [
            "localhost",
            "api.localhost",
            "127.0.0.1",
            "10.0.0.1",
            "100.64.0.1",
            "169.254.169.254",
            "172.16.0.1",
            "192.0.0.1",
            "192.0.2.1",
            "192.88.99.1",
            "192.168.1.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "240.0.0.1",
        ] {
            let url = format!("https://{host}/oauth/client.json");
            PublicMetadataUrl::parse(&url).expect_err(&url);
        }
    }

    #[test]
    fn public_metadata_rejects_non_public_ipv6() {
        for host in [
            "::",
            "::1",
            "::ffff:93.184.216.34",
            "64:ff9b::c000:201",
            "64:ff9b:1::1",
            "100::1",
            "2001:2::1",
            "2001:10::1",
            "2001:20::1",
            "2001:db8::1",
            "fc00::1",
            "fe80::1",
            "fec0::1",
            "ff02::1",
        ] {
            let url = format!("https://[{host}]/oauth/client.json");
            PublicMetadataUrl::parse(&url).expect_err(&url);
        }
    }

    #[test]
    fn public_metadata_accepts_public_ip_literals() {
        for url in [
            "https://93.184.216.34/oauth/client.json",
            "https://[2606:4700:4700::1111]/oauth/client.json",
        ] {
            PublicMetadataUrl::parse(url).expect(url);
        }
    }

    #[test]
    fn public_resolution_rejects_empty_private_and_mixed_answers() {
        validate_public_resolution("empty.test", &[]).expect_err("empty resolution");
        let public: SocketAddr = "93.184.216.34:0".parse().expect("public");
        let private: SocketAddr = "10.0.0.1:0".parse().expect("private");
        validate_public_resolution("public.test", &[public]).expect("public resolution");
        validate_public_resolution("private.test", &[private]).expect_err("private resolution");
        validate_public_resolution("mixed.test", &[public, private]).expect_err("mixed resolution");
    }

    #[test]
    fn ip_classifier_keeps_public_addresses_public() {
        assert!(!public_metadata_ip_is_blocked(
            "93.184.216.34".parse().expect("IPv4")
        ));
        assert!(!public_metadata_ip_is_blocked(
            "2606:4700:4700::1111".parse().expect("IPv6")
        ));
    }

    #[test]
    fn bounded_chunk_append_accepts_exact_limit_and_rejects_overflow() {
        let mut body = Vec::new();
        append_bounded_chunk(&mut body, b"123", 5).expect("first chunk");
        append_bounded_chunk(&mut body, b"45", 5).expect("exact limit");
        assert_eq!(body, b"12345");
        assert!(matches!(
            append_bounded_chunk(&mut body, b"6", 5),
            Err(OutboundUrlPolicyError::BodyTooLarge { limit: 5 })
        ));
    }

    #[tokio::test]
    async fn bounded_body_reads_small_responses_and_rejects_declared_oversize() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/metadata"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello"))
            .mount(&server)
            .await;

        let response = reqwest::get(format!("{}/metadata", server.uri()))
            .await
            .expect("response");
        assert_eq!(
            read_bounded_body(response, 5).await.expect("body"),
            b"hello"
        );

        let response = reqwest::get(format!("{}/metadata", server.uri()))
            .await
            .expect("response");
        assert!(matches!(
            read_bounded_body(response, 4).await,
            Err(OutboundUrlPolicyError::BodyTooLarge { limit: 4 })
        ));
    }

    #[test]
    fn hardened_public_client_builds() {
        public_metadata_http_client().expect("public metadata client");
    }
}
