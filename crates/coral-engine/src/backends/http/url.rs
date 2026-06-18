//! HTTP URL normalization and request-path joining.

use datafusion::error::{DataFusionError, Result};

pub(super) fn ensure_credentials_allowed_for_url(url: &str) -> Result<()> {
    let parsed = reqwest::Url::parse(url).map_err(|error| {
        DataFusionError::Execution(format!("invalid HTTP request URL for credentials: {error}"))
    })?;
    match parsed.scheme() {
        "https" => Ok(()),
        "http" if is_loopback_host(&parsed) => Ok(()),
        "http" => Err(DataFusionError::Execution(format!(
            "authenticated HTTP source requests require HTTPS or loopback HTTP; got http://{}",
            parsed.host_str().unwrap_or("<missing-host>")
        ))),
        scheme => Err(DataFusionError::Execution(format!(
            "authenticated HTTP source requests require HTTPS or loopback HTTP; got scheme '{scheme}'"
        ))),
    }
}

pub(super) fn join_url(base: &str, path: &str) -> Result<String> {
    let trimmed = path.trim();
    if reqwest::Url::parse(trimmed).is_ok() || trimmed.starts_with("//") {
        return Err(DataFusionError::Execution(
            "request path must be relative; absolute URLs are not allowed".to_string(),
        ));
    }
    let base = base.trim_end_matches('/');
    if trimmed.starts_with('/') {
        Ok(format!("{base}{trimmed}"))
    } else {
        Ok(format!("{base}/{trimmed}"))
    }
}

pub(super) fn normalize_base_url(base: &str) -> String {
    let trimmed = base.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if starts_with_http_scheme(trimmed) {
        return trimmed.to_string();
    }
    if trimmed.starts_with("//") {
        return format!("https:{trimmed}");
    }
    format!("https://{trimmed}")
}

fn starts_with_http_scheme(value: &str) -> bool {
    value
        .get(..7)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("http://"))
        || value
            .get(..8)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
}

fn is_loopback_host(url: &reqwest::Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    if host.eq_ignore_ascii_case("localhost") || host.to_ascii_lowercase().ends_with(".localhost") {
        return true;
    }
    let ip_host = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);
    ip_host
        .parse::<std::net::IpAddr>()
        .is_ok_and(|address| address.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::{ensure_credentials_allowed_for_url, join_url, normalize_base_url};

    #[test]
    fn credentials_are_allowed_for_https_and_loopback_http() {
        ensure_credentials_allowed_for_url("https://api.example.com/v1").unwrap();
        ensure_credentials_allowed_for_url("http://localhost:8080/v1").unwrap();
        ensure_credentials_allowed_for_url("http://127.0.0.1:8080/v1").unwrap();
        ensure_credentials_allowed_for_url("http://[::1]:8080/v1").unwrap();
        ensure_credentials_allowed_for_url("http://dev.localhost:8080/v1").unwrap();
    }

    #[test]
    fn credentials_are_rejected_for_remote_http() {
        let error = ensure_credentials_allowed_for_url("http://api.example.com/v1").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("authenticated HTTP source requests require HTTPS")
        );
    }

    #[test]
    fn normalize_base_url_adds_https_scheme_for_host_only_values() {
        assert_eq!(
            normalize_base_url("eu.posthog.com"),
            "https://eu.posthog.com"
        );
        assert_eq!(
            normalize_base_url("//api.example.com"),
            "https://api.example.com"
        );
    }

    #[test]
    fn normalize_base_url_preserves_existing_schemes() {
        assert_eq!(
            normalize_base_url("https://api.github.com"),
            "https://api.github.com"
        );
        assert_eq!(
            normalize_base_url("http://localhost:8080"),
            "http://localhost:8080"
        );
        assert_eq!(
            normalize_base_url("HTTPS://api.example.com"),
            "HTTPS://api.example.com"
        );
    }

    #[test]
    fn join_url_handles_relative_paths() {
        assert_eq!(
            join_url("https://api.example.com", "/v1/resources").unwrap(),
            "https://api.example.com/v1/resources"
        );
        assert_eq!(
            join_url("https://api.example.com/", "v1/resources").unwrap(),
            "https://api.example.com/v1/resources"
        );
    }

    #[test]
    fn join_url_rejects_absolute_paths() {
        let err = join_url("https://api.example.com", "https://next.example.com/page").unwrap_err();
        assert!(
            err.to_string()
                .contains("request path must be relative; absolute URLs are not allowed")
        );
    }
}
