//! Sanitisation for provider-authored native search display content.

use serde_json::{Map, Value};
use url::Url;

use crate::search::content_safety::{
    JsonSanitization, is_sensitive_name, is_sensitive_pair, is_sensitive_value, sanitize_json_value,
};

pub(super) const PROVIDER_ID_BYTES: usize = 512;
pub(super) const TITLE_BYTES: usize = 512;
pub(super) const URL_BYTES: usize = 2_048;
pub(super) const SNIPPET_BYTES: usize = 1_024;
pub(super) const ATTRIBUTE_NAME_BYTES: usize = 128;
pub(super) const ATTRIBUTE_VALUE_BYTES: usize = 1_024;

const MAX_PERCENT_DECODE_PASSES: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Sanitized<T> {
    Safe(T),
    SizeLimited(Option<T>),
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OverflowPolicy {
    Omit,
    Truncate,
}

pub(super) fn sanitize_text(
    field_name: &str,
    value: &str,
    max_bytes: usize,
    overflow: OverflowPolicy,
) -> Sanitized<String> {
    if is_sensitive_name(field_name) {
        return Sanitized::Rejected;
    }
    let cleaned = clean_display_text(value);
    if cleaned.trim().is_empty() || is_sensitive_value(&cleaned) {
        return Sanitized::Rejected;
    }
    if cleaned.len() <= max_bytes {
        return Sanitized::Safe(cleaned);
    }
    match overflow {
        OverflowPolicy::Omit => Sanitized::SizeLimited(None),
        OverflowPolicy::Truncate => {
            Sanitized::SizeLimited(Some(truncate_utf8(&cleaned, max_bytes)))
        }
    }
}

pub(super) fn sanitize_attribute_name(field_name: &str) -> Sanitized<String> {
    if is_sensitive_name(field_name) {
        return Sanitized::Rejected;
    }
    let cleaned = clean_display_text(field_name);
    if cleaned.trim().is_empty() || is_sensitive_name(&cleaned) {
        return Sanitized::Rejected;
    }
    if cleaned.len() > ATTRIBUTE_NAME_BYTES {
        Sanitized::SizeLimited(None)
    } else {
        Sanitized::Safe(cleaned)
    }
}

pub(super) fn sanitize_url(field_name: &str, value: &str) -> Sanitized<String> {
    if is_sensitive_name(field_name) {
        return Sanitized::Rejected;
    }
    let cleaned = clean_display_text(value);
    if cleaned.trim().is_empty() {
        return Sanitized::Rejected;
    }
    let Ok(mut url) = Url::parse(&cleaned) else {
        return Sanitized::Rejected;
    };
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || !safe_decoded_path(&url)
    {
        return Sanitized::Rejected;
    }

    url.set_fragment(None);
    let pairs = url.query_pairs().into_owned().collect::<Vec<_>>();
    let retained = pairs
        .into_iter()
        .filter_map(|(name, value)| {
            let decoded_name = fixed_point_percent_decode(&name)?;
            let decoded_value = fixed_point_percent_decode(&value)?;
            let cleaned_name = clean_display_text(&decoded_name);
            let cleaned_value = clean_display_text(&decoded_value);
            if (!name.is_empty() && cleaned_name.is_empty())
                || (!value.is_empty() && cleaned_value.is_empty())
                || is_sensitive_pair(&cleaned_name, &cleaned_value)
            {
                None
            } else {
                Some((cleaned_name, cleaned_value))
            }
        })
        .collect::<Vec<_>>();
    url.set_query(None);
    if !retained.is_empty() {
        url.query_pairs_mut().extend_pairs(
            retained
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
        );
    }

    let sanitized = url.to_string();
    if is_sensitive_value(&sanitized) {
        Sanitized::Rejected
    } else if sanitized.len() > URL_BYTES {
        Sanitized::SizeLimited(None)
    } else {
        Sanitized::Safe(sanitized)
    }
}

pub(super) fn sanitize_canonical_json(field_name: &str, value: &str) -> Sanitized<String> {
    if is_sensitive_name(field_name) {
        return Sanitized::Rejected;
    }
    let Ok(mut value) = serde_json::from_str::<Value>(value) else {
        return Sanitized::Rejected;
    };
    if sanitize_json_value(&mut value) == JsonSanitization::Drop {
        return Sanitized::Rejected;
    }
    if !clean_and_canonicalize_json(&mut value) {
        return Sanitized::Rejected;
    }
    let Ok(serialized) = serde_json::to_string(&value) else {
        return Sanitized::Rejected;
    };
    if serialized.trim().is_empty() || is_sensitive_value(&serialized) {
        return Sanitized::Rejected;
    }
    if serialized.len() > ATTRIBUTE_VALUE_BYTES {
        Sanitized::SizeLimited(None)
    } else {
        Sanitized::Safe(serialized)
    }
}

pub(super) fn clean_display_text(value: &str) -> String {
    let mut cleaned = String::with_capacity(value.len());
    let mut characters = value.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '\u{1b}' => consume_escape_sequence(&mut characters),
            '\u{9b}' => consume_control_sequence(&mut characters),
            '\u{9d}' => consume_operating_system_command(&mut characters),
            character if character.is_control() || is_bidi_control(character) => {}
            character => cleaned.push(character),
        }
    }
    cleaned
}

pub(super) fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut boundary = max_bytes;
    while boundary > 0 && !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.get(..boundary).unwrap_or_default().to_string()
}

fn consume_escape_sequence(characters: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    match characters.next() {
        Some('[') => consume_control_sequence(characters),
        Some(']') => consume_operating_system_command(characters),
        Some(_) | None => {}
    }
}

fn consume_control_sequence(characters: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    for character in characters.by_ref() {
        if ('@'..='~').contains(&character) {
            break;
        }
    }
}

fn consume_operating_system_command(characters: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    while let Some(character) = characters.next() {
        if character == '\u{7}' {
            break;
        }
        if character == '\u{1b}' && characters.next_if_eq(&'\\').is_some() {
            break;
        }
    }
}

fn safe_decoded_path(url: &Url) -> bool {
    let Some(decoded) = fixed_point_percent_decode(url.path()) else {
        return false;
    };
    clean_display_text(&decoded) == decoded && !is_sensitive_value(&decoded)
}

fn fixed_point_percent_decode(value: &str) -> Option<String> {
    let mut decoded = value.to_string();
    for _ in 0..MAX_PERCENT_DECODE_PASSES {
        let next = percent_decode_utf8_once(&decoded)?;
        if next == decoded {
            return Some(decoded);
        }
        decoded = next;
    }

    // Do not classify content while another valid encoded layer remains. This
    // bounds adversarial work without permitting a deeper decoder to reveal a
    // secret or control character after this safety boundary accepted it.
    let next = percent_decode_utf8_once(&decoded)?;
    (next == decoded).then_some(decoded)
}

fn percent_decode_utf8_once(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0_usize;
    while index < bytes.len() {
        if bytes.get(index) == Some(&b'%') {
            let high = bytes
                .get(index.saturating_add(1))
                .copied()
                .and_then(hex_value);
            let low = bytes
                .get(index.saturating_add(2))
                .copied()
                .and_then(hex_value);
            if let (Some(high), Some(low)) = (high, low) {
                decoded.push((high << 4) | low);
                index = index.saturating_add(3);
                continue;
            }
        }
        decoded.push(*bytes.get(index)?);
        index = index.saturating_add(1);
    }
    String::from_utf8(decoded).ok()
}

const fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn is_bidi_control(character: char) -> bool {
    matches!(
        character,
        '\u{061c}'
            | '\u{200e}'
            | '\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2066}'..='\u{2069}'
    )
}

fn clean_and_canonicalize_json(value: &mut Value) -> bool {
    match value {
        Value::Object(fields) => {
            let mut entries = std::mem::take(fields).into_iter().collect::<Vec<_>>();
            entries = entries
                .into_iter()
                .filter_map(|(name, mut value)| {
                    let name = clean_display_text(&name);
                    if name.trim().is_empty()
                        || is_sensitive_name(&name)
                        || !clean_and_canonicalize_json(&mut value)
                    {
                        None
                    } else {
                        Some((name, value))
                    }
                })
                .collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let mut ordered = Map::new();
            for (name, value) in entries {
                if !ordered.contains_key(&name) {
                    ordered.insert(name, value);
                }
            }
            *fields = ordered;
            !fields.is_empty()
        }
        Value::Array(values) => {
            values.retain_mut(clean_and_canonicalize_json);
            !values.is_empty()
        }
        Value::String(text) => {
            *text = clean_display_text(text);
            !text.trim().is_empty() && !is_sensitive_value(text)
        }
        Value::Null => false,
        Value::Bool(_) | Value::Number(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OverflowPolicy, Sanitized, clean_display_text, sanitize_canonical_json, sanitize_text,
        sanitize_url, truncate_utf8,
    };

    #[test]
    fn cleaning_removes_ansi_controls_and_bidi_controls() {
        assert_eq!(
            clean_display_text("a\0\u{1b}[31mred\u{1b}[0m\n\u{202e}b"),
            "aredb"
        );
        assert_eq!(clean_display_text("x\u{1b}]0;title\u{7}y"), "xy");
    }

    #[test]
    fn text_truncation_never_splits_utf8() {
        assert_eq!(truncate_utf8("aéz", 2), "a");
        assert_eq!(
            sanitize_text("title", "aéz", 3, OverflowPolicy::Truncate),
            Sanitized::SizeLimited(Some("aé".to_string()))
        );
    }

    #[test]
    fn strict_urls_drop_userinfo_fragments_and_every_sensitive_pair() {
        assert_eq!(
            sanitize_url(
                "url",
                "HTTPS://Example.TEST/path?token=one&safe=yes&api%5Fkey=two&token=three#secret"
            ),
            Sanitized::Safe("https://example.test/path?safe=yes".to_string())
        );
        assert_eq!(
            sanitize_url("url", "https://user:pass@example.test/path"),
            Sanitized::Rejected
        );
        assert_eq!(sanitize_url("url", "/relative"), Sanitized::Rejected);
        assert_eq!(sanitize_url("url", "file:///tmp/a"), Sanitized::Rejected);
    }

    #[test]
    fn canonical_json_sorts_keys_and_uses_the_shared_secret_policy() {
        assert_eq!(
            sanitize_canonical_json(
                "metadata",
                r#"{"z":2,"api_key":"hidden","a":{"y":2,"x":1}}"#
            ),
            Sanitized::Safe(r#"{"a":{"x":1,"y":2},"z":2}"#.to_string())
        );
    }

    #[test]
    fn canonical_json_cleans_nested_strings_keys_and_emptied_content() {
        assert_eq!(
            sanitize_canonical_json(
                "metadata",
                "{\"b\\u0000\":\"safe\\u202etext\",\"empty\":\"\\u0000\",\"nested\":[\"\\u001b[31mred\\u001b[0m\",null]}"
            ),
            Sanitized::Safe("{\"b\":\"safetext\",\"nested\":[\"red\"]}".to_string())
        );
        assert_eq!(
            sanitize_canonical_json("metadata", "{\"only\":\"\\u0000\"}"),
            Sanitized::Rejected
        );
    }

    #[test]
    fn urls_clean_decoded_query_controls_before_redaction_and_reencoding() {
        assert_eq!(
            sanitize_url(
                "url",
                "https://example.test/path?safe%00name=value%00x&to%E2%80%AEken=hidden&ansi=%1B%5B31mred%1B%5B0m"
            ),
            Sanitized::Safe("https://example.test/path?safename=valuex&ansi=red".to_string())
        );
    }

    #[test]
    fn urls_classify_double_encoded_query_names_values_and_controls() {
        assert_eq!(
            sanitize_url(
                "url",
                "https://example.test/path?to%256ben=hidden&safe=sk%252D12345678901234567890&na%2500me=va%2500lue&ok=yes"
            ),
            Sanitized::Safe("https://example.test/path?name=value&ok=yes".to_string())
        );
    }

    #[test]
    fn urls_drop_query_pairs_when_bounded_decoding_does_not_reach_a_fixed_point() {
        assert_eq!(
            sanitize_url(
                "url",
                "https://example.test/path?to%2525252525256ben=hidden&ok=yes"
            ),
            Sanitized::Safe("https://example.test/path?ok=yes".to_string())
        );
    }

    #[test]
    fn urls_reject_encoded_unsafe_or_secret_shaped_paths() {
        for url in [
            "https://example.test/a%00b",
            "https://example.test/%1B%5B31mred%1B%5B0m",
            "https://example.test/a%E2%80%AEb",
            "https://example.test/sk%2D12345678901234567890",
            "https://example.test/a%2500b",
            "https://example.test/sk%252D12345678901234567890",
            "https://example.test/to%2525252525256ben",
        ] {
            assert_eq!(sanitize_url("url", url), Sanitized::Rejected, "{url}");
        }
        assert_eq!(
            sanitize_url("url", "https://example.test/caf%C3%A9"),
            Sanitized::Safe("https://example.test/caf%C3%A9".to_string())
        );
    }

    #[test]
    fn url_limit_is_exact_and_empty_safe_query_values_are_preserved() {
        let prefix = "https://example.test/";
        let exact = format!("{prefix}{}", "a".repeat(super::URL_BYTES - prefix.len()));
        assert_eq!(sanitize_url("url", &exact), Sanitized::Safe(exact.clone()));
        assert_eq!(
            sanitize_url("url", &format!("{exact}a")),
            Sanitized::SizeLimited(None)
        );
        assert_eq!(
            sanitize_url("url", "https://example.test/path?flag=&safe=yes"),
            Sanitized::Safe("https://example.test/path?flag=&safe=yes".to_string())
        );
    }
}
