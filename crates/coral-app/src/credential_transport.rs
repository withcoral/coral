//! Shared transport policy for endpoints that receive credential material.

use url::{Host, Url};

use crate::bootstrap::AppError;

pub(crate) fn validate_credential_endpoint_transport(
    context: &str,
    raw_url: &str,
) -> Result<(), AppError> {
    let normalized = normalize_endpoint_url(raw_url);
    let url = Url::parse(&normalized)
        .map_err(|error| AppError::InvalidInput(format!("{context} is invalid: {error}")))?;
    if url.scheme() == "https" || (url.scheme() == "http" && is_loopback_url(&url)) {
        return Ok(());
    }
    Err(AppError::InvalidInput(format!(
        "{context} must use https or loopback http before credentials can be sent"
    )))
}

fn normalize_endpoint_url(raw_url: &str) -> String {
    let trimmed = raw_url.trim().trim_start_matches("//");
    if trimmed.contains("://") {
        trimmed.to_string()
    } else {
        format!("https://{trimmed}")
    }
}

fn is_loopback_url(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}
