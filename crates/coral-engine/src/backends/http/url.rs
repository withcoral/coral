//! HTTP URL normalization and request-path joining.

use datafusion::error::{DataFusionError, Result};

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

#[cfg(test)]
mod tests {
    use super::{join_url, normalize_base_url};

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
