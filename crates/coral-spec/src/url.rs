//! Shared URL allowlist used for outbound provider URLs.

/// Return `true` when `url` is an HTTPS URL or an HTTP loopback URL.
///
/// This is the canonical SSRF/redirect allowlist used across Coral: HTTPS is
/// always permitted, while plain HTTP is only allowed when the host resolves to
/// loopback (`localhost`, `127.0.0.0/8`, or `::1`) so that local development
/// endpoints work without weakening transport security for real providers.
///
/// Invalid URLs are rejected (`false`).
#[must_use]
pub fn url_is_https_or_loopback(url: &str) -> bool {
    ::url::Url::parse(url).is_ok_and(|url| match url.scheme() {
        "https" => true,
        "http" => match url.host() {
            Some(::url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
            Some(::url::Host::Ipv4(address)) => address.is_loopback(),
            Some(::url::Host::Ipv6(address)) => address.is_loopback(),
            None => false,
        },
        _ => false,
    })
}
