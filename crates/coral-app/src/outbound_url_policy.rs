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

use crate::bootstrap::is_loopback_ip;

/// Timeout applied to public metadata connections and complete requests.
pub(crate) const PUBLIC_METADATA_TIMEOUT: Duration = Duration::from_secs(5);

/// A configured endpoint that is safe for credential-bearing requests.
///
/// # Trust
///
/// This profile permits plain-HTTP loopback, and HTTPS to any host including a
/// private one, with no check on what the host resolves to. Both are sound only
/// for a value an operator authored — a config file or environment variable.
///
/// Do not build one from a value a remote party controls. That includes the
/// endpoints inside an `OpenID` Connect discovery document, which the provider
/// supplies: a compromised provider answering with
/// `token_endpoint: "http://127.0.0.1:9999/x"` would be handed the client
/// secret, authorization code, and PKCE verifier in cleartext, readable by any
/// local process. Remote-supplied endpoints are a third trust level and need
/// their own profile — one that requires HTTPS unless the configured issuer is
/// itself loopback HTTP.
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
        if has_encoded_path_separator(value) {
            return Err(OutboundUrlPolicyError::EncodedPathSeparator);
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
    /// An untrusted URL percent-encoded a path separator.
    #[error("public metadata URL must not percent-encode path separators")]
    EncodedPathSeparator,
    /// An untrusted URL directly identified a non-public host.
    #[error("public metadata URL host must be public")]
    NonPublicHost,
    /// The hardened HTTP client could not be constructed.
    #[error("failed to build public metadata HTTP client: {0}")]
    ClientBuild(reqwest::Error),
    /// A response body could not be read.
    ///
    /// Shared by both profiles: [`read_bounded_body`] is profile-agnostic, so
    /// this must not name one.
    #[error("failed to read response body: {0}")]
    BodyRead(reqwest::Error),
    /// A response body exceeded the caller's bound.
    #[error("response body exceeded {limit} bytes")]
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

/// Reports whether `url` names the local machine explicitly enough to allow
/// plain HTTP.
///
/// The address arms delegate to [`is_loopback_ip`] so this shares one rule with
/// the bind guards and the auth URL validator: that helper's doc comment calls
/// out `::ffff:127.0.0.1` as the case a divergent copy would get wrong, and a
/// copy here did.
fn is_explicit_loopback(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(host)) => host.trim_end_matches('.').eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(address)) => is_loopback_ip(address.into()),
        Some(Host::Ipv6(address)) => is_loopback_ip(address.into()),
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

/// Reports whether `address` sits outside the public IPv4 internet.
///
/// Every branch is one entry of the IANA IPv4 Special-Purpose Address Registry
/// marked "Globally Reachable: False", so this is that registry as a denylist
/// rather than a hand-picked set. Registry completeness is not reachability
/// completeness, though: an address IANA calls ordinary public unicast can
/// still front something internal — a deployment's own ASN space, or a cloud
/// host-node address such as Azure's `168.63.129.16` — and no arithmetic here
/// can know that. The resolver check in [`validate_public_resolution`] has the
/// same blind spot, so neither is a substitute for an operator deny list.
fn public_metadata_ipv4_is_blocked(address: Ipv4Addr) -> bool {
    let [a, b, c, _] = address.octets();
    a == 0 // 0.0.0.0/8 this network; also 0.0.0.0/32, which connect(2) sends to a local listener
        || a == 10 // 10.0.0.0/8 private
        || a == 127 // 127.0.0.0/8 loopback, the whole /8 and not just 127.0.0.1
        || (a == 100 && (64..=127).contains(&b)) // 100.64.0.0/10 shared address space (CGNAT)
        || (a == 169 && b == 254) // 169.254.0.0/16 link-local, which is where cloud metadata lives
        || (a == 172 && (16..=31).contains(&b)) // 172.16.0.0/12 private
        || (a == 192 && b == 0 && c == 0) // 192.0.0.0/24 IETF protocol assignments
        || (a == 192 && b == 0 && c == 2) // 192.0.2.0/24 documentation (TEST-NET-1)
        || (a == 192 && b == 88 && c == 99) // 192.88.99.0/24 deprecated 6to4 relay anycast
        || (a == 192 && b == 168) // 192.168.0.0/16 private
        || (a == 198 && (b == 18 || b == 19)) // 198.18.0.0/15 benchmarking
        || (a == 198 && b == 51 && c == 100) // 198.51.100.0/24 documentation (TEST-NET-2)
        || (a == 203 && b == 0 && c == 113) // 203.0.113.0/24 documentation (TEST-NET-3)
        // 224.0.0.0/3: 240.0.0.0/4 reserved (with 255.255.255.255 broadcast) plus 224.0.0.0/4
        // multicast, which is wider than the registry entry on purpose — no TCP origin can
        // live on a multicast address.
        || a >= 224
}

/// Reports whether `address` sits outside the public IPv6 internet.
///
/// As with the IPv4 table, each branch is a registry entry that is not globally
/// reachable, plus the transition ranges that carry an IPv4 destination inside
/// an IPv6 address. Being a denylist, a new registry entry is allowed until it
/// is added here — the three most recent ones had to be added after the fact.
fn public_metadata_ipv6_is_blocked(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    // `::1` and `::` are already matched by the `to_ipv4` branch, which covers
    // every address whose top 80 bits are zero. They stay spelled out so that
    // narrowing that branch to `to_ipv4_mapped` — which stops matching them —
    // cannot quietly unblock loopback.
    address.is_loopback() // ::1/128
        || address.is_unspecified() // ::/128
        || address.is_multicast() // ff00::/8
        // ::/96 IPv4-compatible and ::ffff:0:0/96 IPv4-mapped. `to_ipv4` rather than
        // `to_ipv4_mapped` on purpose: it matches both, so neither spelling can smuggle
        // back an IPv4 destination the IPv4 table would have refused.
        || address.to_ipv4().is_some()
        || (segments[0] & 0xfe00) == 0xfc00 // fc00::/7 unique-local
        || (segments[0] & 0xffc0) == 0xfe80 // fe80::/10 link-local
        || (segments[0] & 0xffc0) == 0xfec0 // fec0::/10 deprecated site-local
        || segments[0] == 0x5f00 // 5f00::/16 SRv6 SIDs, routable inside an SRv6 domain
        || (segments[0] == 0x3fff && (segments[1] & 0xf000) == 0) // 3fff::/20 documentation
        // 64:ff9b::/96 NAT64 well-known prefix and 64:ff9b:1::/48 local-use translation
        || (segments[0] == 0x0064
            && segments[1] == 0xff9b
            && ((segments[2..6] == [0, 0, 0, 0]) || segments[2] == 1))
        // 100::/64 discard-only and its neighbour 100:0:0:1::/64 dummy prefix
        || (segments[0] == 0x0100 && segments[1] == 0 && segments[2] == 0 && segments[3] <= 1)
        // 2001::/32 Teredo and 2001:2::/48 benchmarking (matched as the wider /32, which is
        // unallocated IETF protocol space either way)
        || (segments[0] == 0x2001 && (segments[1] == 0 || segments[1] == 0x0002))
        || (segments[0] == 0x2001 && (0x0010..=0x002f).contains(&segments[1])) // 2001:10::/28 ORCHID, 2001:20::/28 ORCHIDv2
        || (segments[0] == 0x2001 && segments[1] == 0x0db8) // 2001:db8::/32 documentation
        || segments[0] == 0x2002 // 2002::/16 6to4, which embeds an arbitrary IPv4 destination
}

/// Reports whether the URL *as supplied* spells out a dot path segment.
///
/// This reads the raw string rather than a parsed [`Url`] because the parser
/// resolves `..` away: once there is a [`Url`], a traversal attempt is
/// indistinguishable from the path it collapsed to.
fn has_dot_path_segment(value: &str) -> bool {
    raw_path(value).is_some_and(|path| path.split(['/', '\\']).any(is_dot_path_segment))
}

/// Reports whether the path percent-encodes a `/` or `\`.
///
/// Unlike a literal dot segment, [`Url::parse`] leaves `%2f` and `%5c` encoded,
/// so they reach the wire verbatim: `/oauth/%2e%2e%2fclient.json` is sent as
/// written. A server that decodes separators before resolving the path then
/// sees the traversal this module promises to have rejected. Nothing legitimate
/// needs an encoded separator in a metadata path, so refusing outright is safer
/// than decoding and re-classifying, which would have to reason about how many
/// rounds of decoding the far end applies.
fn has_encoded_path_separator(value: &str) -> bool {
    raw_path(value).is_some_and(|path| {
        let path = path.to_ascii_lowercase();
        path.contains("%2f") || path.contains("%5c")
    })
}

/// Returns the path portion of a URL string exactly as supplied.
///
/// Shared by the two traversal checks, which both have to read the raw string.
/// It accepts every spelling the parser does — for a special scheme the parser
/// treats any run of `/` and `\` after the scheme's `:` as the authority
/// separator, and strips ASCII tab, CR, and LF anywhere in the input first — so
/// a scan keyed on a literal `://` misses all of those. The query and fragment
/// are excluded: an encoded separator there is data, not structure.
fn raw_path(value: &str) -> Option<String> {
    let value: String = value
        .chars()
        .filter(|character| !matches!(character, '\t' | '\r' | '\n'))
        .collect();
    let (_, after_scheme) = value.split_once(':')?;
    let authority = after_scheme.trim_start_matches(['/', '\\']);
    let path_start = authority.find(['/', '\\'])?;
    let path = authority.get(path_start..)?;
    let path_end = path.find(['?', '#']).unwrap_or(path.len());
    path.get(..path_end).map(str::to_owned)
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
            // Routed through `bootstrap::is_loopback_ip`, so the IPv4-mapped
            // spelling this profile once refused is accepted, matching what the
            // auth URL validator accepts.
            "http://[::ffff:127.0.0.1]:14554/callback",
        ] {
            ConfiguredEndpointUrl::parse(endpoint).expect(endpoint);
        }
    }

    /// `is_explicit_loopback` matches only the exact host `localhost`, while
    /// `public_metadata_host_is_blocked` also treats `*.localhost` as loopback
    /// per RFC 6761. Both refusals below are the fail-safe direction; this pins
    /// the asymmetry so a later change to either side is deliberate.
    #[test]
    fn loopback_name_rules_differ_between_profiles() {
        assert!(matches!(
            ConfiguredEndpointUrl::parse("http://keycloak.localhost:8080/realms/coral"),
            Err(OutboundUrlPolicyError::ConfiguredEndpointTransport)
        ));
        ConfiguredEndpointUrl::parse("https://keycloak.localhost:8443/realms/coral")
            .expect("https reaches any host");

        assert!(matches!(
            PublicMetadataUrl::parse("https://keycloak.localhost/oauth/client.json"),
            Err(OutboundUrlPolicyError::NonPublicHost)
        ));
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

    /// `Url::parse` accepts several spellings of the authority separator and
    /// strips ASCII tab, CR, and LF first. Each of these parses to the same
    /// host and a `/client.json` path, so a scan keyed on a literal `://`
    /// silently canonicalized them instead of rejecting them.
    #[test]
    fn public_metadata_rejects_dot_segments_in_every_parsed_spelling() {
        for metadata_url in [
            "https:/client.example.test/oauth/../client.json",
            "https:client.example.test/oauth/../client.json",
            "https:\\\\client.example.test/oauth/../client.json",
            "https://client.example.test/oauth/.\t./client.json",
            "https://client.example.test/oauth/.\r\n./client.json",
        ] {
            assert!(
                matches!(
                    PublicMetadataUrl::parse(metadata_url),
                    Err(OutboundUrlPolicyError::DotPathSegment)
                ),
                "{metadata_url}"
            );
        }

        PublicMetadataUrl::parse("https://client.example.test/oauth/client.json")
            .expect("a traversal-free path is unaffected");
    }

    /// `Url::parse` keeps `%2f` and `%5c` encoded, so unlike a literal dot
    /// segment these reach the wire as written and only become a traversal at a
    /// server that decodes separators before resolving the path.
    #[test]
    fn public_metadata_rejects_percent_encoded_path_separators() {
        for metadata_url in [
            "https://client.example.test/oauth/%2e%2e%2fclient.json",
            "https://client.example.test/oauth/..%2fclient.json",
            "https://client.example.test/oauth/%2e%2e%5cclient.json",
            "https://client.example.test/oauth/%2E%2E%2Fclient.json",
            "https://client.example.test/oauth%2fclient.json",
        ] {
            assert!(
                matches!(
                    PublicMetadataUrl::parse(metadata_url),
                    Err(OutboundUrlPolicyError::EncodedPathSeparator)
                ),
                "{metadata_url}"
            );
        }

        // An encoded separator past the path delimiter is data, not structure.
        PublicMetadataUrl::parse("https://client.example.test/oauth/client.json?next=%2fhome")
            .expect("encoded separator in the query is unaffected");
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
            "::127.0.0.1",
            "::ffff:93.184.216.34",
            "64:ff9b::c000:201",
            "64:ff9b:1::1",
            "100::1",
            "2001::1",
            "2001:2::1",
            "2001:10::1",
            "2001:20::1",
            "2001:db8::1",
            "2002:7f00:1::",
            "fc00::1",
            "fe80::1",
            "fec0::1",
            "ff02::1",
            // Registry entries newer than the original table.
            "5f00::1",          // 5f00::/16 SRv6 SIDs
            "5f00:ffff::1",     // upper edge of the same /16
            "3fff::1",          // 3fff::/20 documentation
            "3fff:fff:ffff::1", // upper edge of the same /20
            "100:0:0:1::1",     // 100:0:0:1::/64 dummy prefix
        ] {
            let url = format!("https://[{host}]/oauth/client.json");
            PublicMetadataUrl::parse(&url).expect_err(&url);
        }
    }

    /// The ranges immediately outside the three newest branches must stay
    /// reachable, or the added arithmetic is quietly over-blocking.
    #[test]
    fn public_metadata_keeps_neighbours_of_the_newest_ranges_public() {
        for host in [
            "5eff:ffff::1", // just below 5f00::/16
            "5f01::1",      // just above 5f00::/16
            "3ffe::1",      // just below 3fff::/20
            "3fff:1000::1", // just above the /20, still inside 3fff::/16
            "100:0:0:2::1", // just above the dummy prefix
        ] {
            let url = format!("https://[{host}]/oauth/client.json");
            PublicMetadataUrl::parse(&url).expect(&url);
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
        for private in ["[::7f00:1]:0", "[2001::1]:0", "[2002:7f00:1::]:0"] {
            let private = private.parse().expect("transition IPv6 address");
            validate_public_resolution("private.test", &[private])
                .expect_err("transition IPv6 resolution");
        }
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

    /// Refusing redirects is the hardening that matters most for SSRF: without
    /// it a public URL can bounce the client to a private one, after every
    /// static check has already passed. Asserting the builder returns `Ok` does
    /// not pin that, so drive a real 302. The mock listens on a literal IP,
    /// which hyper resolves without consulting the custom resolver.
    #[tokio::test]
    async fn hardened_public_client_does_not_follow_redirects() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/metadata"))
            .respond_with(
                ResponseTemplate::new(302).insert_header("location", "http://169.254.169.254/"),
            )
            .mount(&server)
            .await;

        let response = public_metadata_http_client()
            .expect("public metadata client")
            .get(format!("{}/metadata", server.uri()))
            .send()
            .await
            .expect("response");

        assert_eq!(response.status().as_u16(), 302);
        assert_eq!(
            response
                .headers()
                .get("location")
                .and_then(|location| location.to_str().ok()),
            Some("http://169.254.169.254/")
        );
    }

    /// A server that omits `Content-Length` skips the declared-size early
    /// return, leaving the streaming loop as the only bound. Answer over a raw
    /// socket, since a response body terminated by connection close is exactly
    /// the shape that carries no length.
    #[tokio::test]
    async fn bounded_body_rejects_undeclared_oversize_while_streaming() {
        async fn serve_unsized_body(body: &'static [u8]) -> String {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener");
            let address = listener.local_addr().expect("address");
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};

                let (mut stream, _) = listener.accept().await.expect("connection");
                let mut request = [0_u8; 1024];
                let read = stream.read(&mut request).await.expect("request");
                assert!(read > 0, "expected a request before answering");
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n")
                    .await
                    .expect("headers");
                stream.write_all(body).await.expect("body");
                // Closing the socket is what terminates the body.
                drop(stream);
            });
            format!("http://{address}/metadata")
        }

        let url = serve_unsized_body(b"0123456789").await;
        let response = reqwest::get(&url).await.expect("response");
        assert!(
            response.content_length().is_none(),
            "the fixture must not declare a length, or this exercises the early return"
        );
        assert!(matches!(
            read_bounded_body(response, 4).await,
            Err(OutboundUrlPolicyError::BodyTooLarge { limit: 4 })
        ));

        let url = serve_unsized_body(b"hello").await;
        let response = reqwest::get(&url).await.expect("response");
        assert_eq!(
            read_bounded_body(response, 5).await.expect("body"),
            b"hello"
        );
    }
}
