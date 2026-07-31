//! Best-effort suppression of obvious secrets in observed values.
//!
//! This module recognizes credential-like field names and a limited set of
//! obvious secret token shapes. It is defense in depth, not general
//! sensitive-data detection or a data-loss-prevention boundary.

use serde_json::Value;
use url::{Url, form_urlencoded};

use crate::search::content_safety::{
    JsonSanitization, is_sensitive_name, is_sensitive_pair, sanitize_json_value,
};

/// Returns whether a field name resembles a credential-bearing field.
pub(super) fn is_sensitive_column(column_name: &str) -> bool {
    is_sensitive_name(column_name)
}

/// Returns whether a value contains one of the obvious secret shapes we know.
///
/// A `false` result does not mean the value is generally non-sensitive.
pub(super) fn is_sensitive_value(value: &str) -> bool {
    crate::search::content_safety::is_sensitive_value(value)
}

/// Removes recognized secret-bearing fields while preserving safe structured content.
///
/// This is a best-effort heuristic and does not classify arbitrary sensitive data.
pub(super) fn sanitize_observed_value(value: String, max_value_bytes: usize) -> Option<String> {
    let sanitized = match sanitize_json(&value)
        .or_else(|| sanitize_url(&value))
        .or_else(|| sanitize_form(&value))
        .unwrap_or(SanitizedValue::Unchanged)
    {
        SanitizedValue::Unchanged => value,
        SanitizedValue::Changed(value) => value,
        SanitizedValue::Drop => return None,
    };

    if sanitized.len() > max_value_bytes || is_sensitive_value(&sanitized) {
        return None;
    }
    Some(sanitized)
}

enum SanitizedValue {
    Unchanged,
    Changed(String),
    Drop,
}

fn sanitize_json(value: &str) -> Option<SanitizedValue> {
    let trimmed = value.trim_start();
    if !trimmed.starts_with('{') && !trimmed.starts_with('[') {
        return None;
    }

    let mut structured_value = serde_json::from_str::<Value>(value).ok()?;
    match sanitize_json_value(&mut structured_value) {
        JsonSanitization::Unchanged => Some(SanitizedValue::Unchanged),
        JsonSanitization::Changed => Some(
            serde_json::to_string(&structured_value)
                .map_or(SanitizedValue::Drop, SanitizedValue::Changed),
        ),
        JsonSanitization::Drop => Some(SanitizedValue::Drop),
    }
}

fn sanitize_url(value: &str) -> Option<SanitizedValue> {
    let Ok(mut url) = Url::parse(value) else {
        return sanitize_relative_url(value);
    };
    url.query()?;

    let pairs = url.query_pairs().into_owned().collect::<Vec<_>>();
    let retained_pairs = pairs
        .iter()
        .filter(|(key, value)| !is_sensitive_pair(key, value))
        .cloned()
        .collect::<Vec<_>>();
    if retained_pairs.len() == pairs.len() {
        return Some(SanitizedValue::Unchanged);
    }

    url.set_query(None);
    if !retained_pairs.is_empty() {
        url.query_pairs_mut().extend_pairs(
            retained_pairs
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
    }
    Some(SanitizedValue::Changed(url.into()))
}

fn sanitize_relative_url(value: &str) -> Option<SanitizedValue> {
    let (path, query_and_fragment) = value.split_once('?')?;
    let (query, fragment) = query_and_fragment
        .split_once('#')
        .map_or((query_and_fragment, None), |(query, fragment)| {
            (query, Some(fragment))
        });
    let pairs = form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect::<Vec<_>>();
    let retained_pairs = pairs
        .iter()
        .filter(|(key, value)| !is_sensitive_pair(key, value))
        .cloned()
        .collect::<Vec<_>>();
    if retained_pairs.len() == pairs.len() {
        return Some(SanitizedValue::Unchanged);
    }

    let mut sanitized = path.to_string();
    if !retained_pairs.is_empty() {
        sanitized.push('?');
        let mut serializer = form_urlencoded::Serializer::new(String::new());
        serializer.extend_pairs(
            retained_pairs
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
        sanitized.push_str(&serializer.finish());
    }
    if let Some(fragment) = fragment {
        sanitized.push('#');
        sanitized.push_str(fragment);
    }
    if sanitized.is_empty() {
        Some(SanitizedValue::Drop)
    } else {
        Some(SanitizedValue::Changed(sanitized))
    }
}

fn sanitize_form(value: &str) -> Option<SanitizedValue> {
    if !value.contains('=') {
        return None;
    }
    let pairs = form_urlencoded::parse(value.as_bytes())
        .into_owned()
        .collect::<Vec<_>>();
    if pairs.is_empty() {
        return None;
    }
    let retained_pairs = pairs
        .iter()
        .filter(|(key, value)| !is_sensitive_pair(key, value))
        .cloned()
        .collect::<Vec<_>>();
    if retained_pairs.len() == pairs.len() {
        return Some(SanitizedValue::Unchanged);
    }
    if retained_pairs.is_empty() {
        return Some(SanitizedValue::Drop);
    }

    let mut serializer = form_urlencoded::Serializer::new(String::new());
    serializer.extend_pairs(
        retained_pairs
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );
    Some(SanitizedValue::Changed(serializer.finish()))
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use url::Url;

    use super::sanitize_observed_value;

    #[test]
    fn sanitizes_nested_json_and_preserves_benign_siblings() {
        let value = sanitize_observed_value(
            r#"{"name":"Ada","api_key":"literal-secret","nested":{"project":"coral","refresh_token":"other-secret"},"items":[{"label":"safe","password":"hidden"},{"token":"hidden"}]}"#
                .to_string(),
            usize::MAX,
        )
        .expect("safe JSON fields should remain");
        let value: Value = serde_json::from_str(&value).expect("sanitized JSON");

        assert_eq!(
            value,
            json!({
                "name": "Ada",
                "nested": { "project": "coral" },
                "items": [{ "label": "safe" }]
            })
        );
    }

    #[test]
    fn sanitizes_decoded_url_query_keys_and_preserves_the_url() {
        let value = sanitize_observed_value(
            "https://example.test/callback?name=Ada&api%5Fkey=literal-secret&project=coral"
                .to_string(),
            usize::MAX,
        )
        .expect("URL base and safe pairs should remain");
        let url = Url::parse(&value).expect("sanitized URL");
        let pairs = url.query_pairs().into_owned().collect::<Vec<_>>();

        assert_eq!(
            pairs,
            [
                ("name".to_string(), "Ada".to_string()),
                ("project".to_string(), "coral".to_string()),
            ]
        );
        assert!(!value.contains("literal-secret"));
    }

    #[test]
    fn sanitizes_relative_url_query_keys_and_preserves_path_and_fragment() {
        let value = sanitize_observed_value(
            "/callback?name=Ada&api_key=literal-secret#details".to_string(),
            usize::MAX,
        )
        .expect("relative URL content should remain");

        assert_eq!(value, "/callback?name=Ada#details");
    }

    #[test]
    fn sanitizes_decoded_form_keys_and_preserves_safe_pairs() {
        let value = sanitize_observed_value(
            "name=Ada&api%5Fkey=literal-secret&project=coral".to_string(),
            usize::MAX,
        )
        .expect("safe form pairs should remain");

        assert_eq!(value, "name=Ada&project=coral");
    }

    #[test]
    fn drops_empty_structures_but_keeps_a_url_without_sensitive_query_pairs() {
        assert!(
            sanitize_observed_value(r#"{"api_key":"hidden"}"#.to_string(), usize::MAX).is_none()
        );
        assert!(
            sanitize_observed_value("api_key=hidden&token=hidden".to_string(), usize::MAX)
                .is_none()
        );
        assert_eq!(
            sanitize_observed_value(
                "https://example.test/callback?api_key=hidden".to_string(),
                usize::MAX,
            )
            .as_deref(),
            Some("https://example.test/callback")
        );
    }

    #[test]
    fn leaves_benign_structured_values_byte_for_byte_unchanged() {
        for value in [
            r#"{ "name": "Ada" }"#,
            "https://example.test/callback?name=Ada",
            "name=Ada&project=coral",
        ] {
            assert_eq!(
                sanitize_observed_value(value.to_string(), usize::MAX).as_deref(),
                Some(value)
            );
        }
    }

    #[test]
    fn malformed_recognizable_sensitive_assignments_fail_closed() {
        assert!(
            sanitize_observed_value(
                r#"{"name":"Ada","api_key":"literal-secret""#.to_string(),
                usize::MAX,
            )
            .is_none()
        );
    }

    #[test]
    fn rejects_reconstructed_values_that_exceed_the_value_budget() {
        let value = "safe=ééé&token=x".to_string();
        let max_value_bytes = value.len();

        assert!(sanitize_observed_value(value, max_value_bytes).is_none());
    }
}
