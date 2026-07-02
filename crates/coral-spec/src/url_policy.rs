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
    validate_https_or_loopback_scheme_name(context, url.scheme(), is_loopback_url(url))
}

/// Enforces the https-or-loopback scheme policy for a known URL scheme and host.
pub fn validate_https_or_loopback_scheme_name(
    context: &str,
    scheme: &str,
    host_is_loopback: bool,
) -> Result<(), String> {
    match scheme.to_ascii_lowercase().as_str() {
        "https" => Ok(()),
        "http" if host_is_loopback => Ok(()),
        "http" => Err(format!(
            "{context} must use https unless it targets localhost"
        )),
        _ => Err(format!(
            "{context} has unsupported scheme '{scheme}'; use https unless it targets localhost"
        )),
    }
}

/// Enforces the provider-endpoint scheme policy when a template prefix exposes
/// only the scheme and the host will come from a required input later.
///
/// The loopback exception cannot be proven until runtime in this shape, so the
/// manifest-time prefix check accepts only `https`. The fully rendered URL is
/// still validated before any request is made.
pub fn validate_https_or_loopback_unresolved_host_scheme(
    context: &str,
    scheme: &str,
) -> Result<(), String> {
    match scheme.to_ascii_lowercase().as_str() {
        "https" => Ok(()),
        "http" => Err(format!(
            "{context} must use https when the host is supplied by a required input"
        )),
        _ => Err(format!(
            "{context} has unsupported scheme '{scheme}'; use https unless it targets localhost"
        )),
    }
}

/// Enforces the https-or-loopback policy for a URL template prefix that ends
/// before a required input value.
///
/// If the prefix has no URL scheme, the required input may supply the whole
/// authority and the fully rendered URL must be checked later. If the prefix
/// exposes a scheme but leaves the authority unresolved, only `https` is
/// accepted at manifest-validation time.
pub(crate) fn validate_https_or_loopback_template_prefix(
    context: &str,
    raw_prefix: &str,
) -> Result<(), String> {
    if let Some(scheme) = raw_prefix.strip_suffix(':')
        && is_url_scheme_name(scheme)
    {
        return validate_https_or_loopback_unresolved_host_scheme(context, scheme);
    }

    if let Some((scheme, authority_prefix)) = raw_prefix.split_once("://") {
        let authority_may_continue = !authority_prefix
            .chars()
            .any(|ch| matches!(ch, '/' | '?' | '#'));
        if authority_may_continue {
            return validate_https_or_loopback_unresolved_host_scheme(context, scheme);
        }
    }

    let parse_prefix = raw_prefix.strip_suffix(':').unwrap_or(raw_prefix);
    if let Ok(url) = Url::parse(parse_prefix) {
        return validate_https_or_loopback_scheme(context, &url);
    }

    let Some((scheme, _rest)) = raw_prefix.split_once("://") else {
        return Ok(());
    };
    validate_https_or_loopback_unresolved_host_scheme(context, scheme)
}

fn is_url_scheme_name(value: &str) -> bool {
    let Some(first) = value.chars().next() else {
        return false;
    };
    first.is_ascii_alphabetic()
        && value
            .chars()
            .skip(1)
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.'))
}
