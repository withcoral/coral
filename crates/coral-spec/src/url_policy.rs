//! Shared URL scheme policy for OAuth endpoints and loopback redirects.
//!
//! Coral requires OAuth provider endpoints and remote HTTP surfaces to use
//! `https`, permitting plain `http` only when the host is loopback. The same
//! policy is enforced while validating a manifest (here in `coral-spec`), at
//! OAuth runtime in `coral-app`, and during CLI OAuth handling. Keeping the
//! allowed-scheme set, the loopback definition, and the user-facing messages in
//! one place stops those copies from drifting apart.

use url::Url;

/// Returns `true` when the URL targets a loopback host: `localhost`, an IPv4
/// loopback address (`127.0.0.0/8`), or an IPv6 loopback address (`::1`).
#[must_use]
pub fn is_loopback_url(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(addr)) => addr.is_loopback(),
        Some(url::Host::Ipv6(addr)) => addr.is_loopback(),
        None => false,
    }
}

/// Parses `raw` and enforces the https-or-loopback scheme policy.
///
/// On failure returns the human-readable message (prefixed with `context`) so
/// each caller can wrap it in its own error type.
pub fn validate_https_or_loopback_url(context: &str, raw: &str) -> Result<(), String> {
    let url = Url::parse(raw).map_err(|error| format!("{context} is invalid: {error}"))?;
    validate_https_or_loopback_scheme(context, &url)
}

/// Enforces the https-or-loopback scheme policy on an already-parsed URL.
pub fn validate_https_or_loopback_scheme(context: &str, url: &Url) -> Result<(), String> {
    validate_https_or_loopback_scheme_parts(context, url.scheme(), Some(is_loopback_url(url)))
}

/// Enforces the https-or-loopback scheme policy when a template prefix exposes
/// the scheme but may not expose the final host yet.
///
/// Pass `None` for `host_is_loopback` only when the host is unresolved at
/// manifest-validation time and the fully rendered URL will be validated before
/// use.
pub fn validate_https_or_loopback_scheme_parts(
    context: &str,
    scheme: &str,
    host_is_loopback: Option<bool>,
) -> Result<(), String> {
    match scheme {
        "https" => Ok(()),
        "http" if host_is_loopback.unwrap_or(true) => Ok(()),
        "http" => Err(format!(
            "{context} must use https unless it targets localhost"
        )),
        scheme => Err(format!(
            "{context} has unsupported scheme '{scheme}'; use https unless it targets localhost"
        )),
    }
}
