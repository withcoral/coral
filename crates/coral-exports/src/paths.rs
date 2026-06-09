//! Export path helpers.

/// Converts arbitrary provider text into a lower camel-ish identifier segment.
#[must_use]
pub fn identifier_segment(raw: &str) -> String {
    let mut out = String::new();
    let mut uppercase_next = false;
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            if out.is_empty() {
                if ch.is_ascii_digit() {
                    out.push('_');
                }
                out.push(ch.to_ascii_lowercase());
            } else if uppercase_next {
                out.push(ch.to_ascii_uppercase());
                uppercase_next = false;
            } else {
                out.push(ch);
            }
        } else {
            uppercase_next = !out.is_empty();
        }
    }
    if out.is_empty() { "_".to_string() } else { out }
}

/// Converts provider text into `PascalCase`.
#[must_use]
pub fn pascal_segment(raw: &str) -> String {
    let lower = identifier_segment(raw);
    let mut chars = lower.chars();
    let Some(first) = chars.next() else {
        return "_".to_string();
    };
    format!(
        "{}{}",
        first.to_ascii_uppercase(),
        chars.collect::<String>()
    )
}

/// Returns true when a URL path segment looks like an API version (`v1`,
/// `2.0`, `v1beta1`). Shared by the TypeScript path builder and the path
/// disambiguation pass so both treat version segments identically.
pub(crate) fn is_version_segment(segment: &str) -> bool {
    let lower = segment.to_ascii_lowercase();
    if let Some(rest) = lower.strip_prefix('v') {
        return is_prefixed_version_body(rest);
    }
    is_version_body(&lower)
}

fn is_prefixed_version_body(value: &str) -> bool {
    !value.is_empty()
        && value.starts_with(|ch: char| ch.is_ascii_digit())
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
}

fn is_version_body(value: &str) -> bool {
    !value.is_empty()
        && value.chars().any(|ch| ch.is_ascii_digit())
        && value
            .chars()
            .all(|ch| ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-'))
}

#[cfg(test)]
mod tests {
    use super::{identifier_segment, pascal_segment};

    #[test]
    fn identifier_segments_are_valid_js_properties() {
        assert_eq!(
            identifier_segment("add-labels_to issue"),
            "addLabelsToIssue"
        );
        assert_eq!(identifier_segment("123"), "_123");
    }

    #[test]
    fn pascal_segments_are_stable() {
        assert_eq!(pascal_segment("list issues"), "ListIssues");
    }
}
